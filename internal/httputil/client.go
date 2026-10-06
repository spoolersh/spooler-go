package httputil

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spoolersh/spooler-go/internal/backoff"
)

const (
	DefaultTimeout = time.Minute
)

type Client struct {
	ClientBuilder ClientBuilder

	// BaseURL is the required absolute URI to use as the base for requests.
	BaseURL string

	Codec Codecs

	Header http.Header

	Backoff backoff.Strategy

	// UserAgent names the calling software; sent as the User-Agent header.
	UserAgent string

	// Hostname identifies the host making the requests; currently carried
	// in the request id prefix.
	Hostname string

	// Error returns an error object the [Client] should try to parse
	// accordingly to the status code. The parsed value will then set as
	// StatusError.Desc.
	//
	// The given status code is never successful (non-2xx).
	Error func(int) any

	once    sync.Once
	base    *url.URL
	http    *http.Client
	backoff backoff.Strategy
	err     error

	reqID       atomic.Uint64
	reqIDPrefix string
}

func (c *Client) init() error {
	c.once.Do(func() {
		c.base, c.err = url.ParseRequestURI(c.BaseURL)
		if c.err != nil {
			return
		}

		c.http = c.ClientBuilder.Build()

		c.backoff = c.Backoff
		if c.backoff == nil {
			c.backoff = &backoff.Exponential{
				Base:   150 * time.Millisecond,
				Factor: 1.2,
				Jitter: 0.2,
				Limit:  5 * time.Second,
			}
		}
		{
			var sb strings.Builder
			if prefix := c.Hostname; prefix != "" {
				sb.WriteString(strings.Trim(prefix, "/"))
				sb.WriteString("/")
			}
			var buf [8]byte
			rand.Read(buf[:])
			sb.WriteString(hex.EncodeToString(buf[:]))
			sb.WriteString("-")
			c.reqIDPrefix = sb.String()
		}
	})
	return c.err
}

type Request struct {
	Header http.Header
	Path   string
	Query  url.Values

	Codec Codecs
	Send  any

	Recv any

	// Attempts is the maximum total number of request attempts. Values < 1 are
	// treated as 1, i.e. no retries.
	Attempts int

	// Retry reports whether a failed attempt should be retried; err is a
	// *StatusError or a *TransportError. A nil Retry retries nothing.
	//
	// However the call ends, its error after any attempt is AttemptErrors,
	// holding every attempt's failure.
	Retry func(err error) bool

	// Error returns an error object the [Client] should try to parse
	// accordingly to the status code. The parsed value will then set as
	// StatusError.Desc.
	// The given status code is never successful (non-2xx).
	Error func(int) any
}

type Response struct {
	Code   int
	Header http.Header
}

func (c *Client) Client() (*http.Client, error) {
	if err := c.init(); err != nil {
		return nil, err
	}
	return c.http, nil
}

func (c *Client) Do(ctx context.Context, method string, r Request) (Response, error) {
	var (
		zero Response
		err  error
	)
	if err := c.init(); err != nil {
		return zero, err
	}

	codec := r.Codec.Merge(c.Codec)
	if r.Send != nil && codec.Send == nil {
		return zero, fmt.Errorf("send codec must be set")
	}
	if r.Recv != nil && codec.Recv == nil {
		return zero, fmt.Errorf("recv codec must be set")
	}

	u := c.base
	if r.Path != "" || len(r.Query) > 0 {
		u = cloneURL(c.base)
		if r.Path != "" {
			u.Path = path.Join(u.Path, r.Path)
		}
		if len(r.Query) > 0 {
			values := u.Query()
			for k, vs := range r.Query {
				for _, v := range vs {
					values.Add(k, v)
				}
			}
			u.RawQuery = values.Encode()
		}
	}
	var codecBody Body
	if v := r.Send; v != nil {
		codecBody, err = codec.Send.Encode(v)
		if err != nil {
			return zero, err
		}
	}
	var (
		errs       AttemptErrors
		retryAfter time.Duration
	)
	for attempt := range max(r.Attempts, 1) {
		if len(errs) > 0 {
			if ctx.Err() != nil || r.Retry == nil || !r.Retry(errs.last()) {
				return zero, errs
			}
		}
		// The body is opened before any wait: one that cannot be opened
		// again ends the retries at once, with the answer already in hand.
		var reqBody io.ReadCloser
		if codecBody != nil {
			switch {
			case codecBody.Size() == 0:
				reqBody = http.NoBody

			default:
				reqBody, err = codecBody.Open()
				if err != nil {
					if len(errs) > 0 {
						return zero, errs
					}
					return zero, err
				}
				reqBody = bodyReader{
					ReadCloser: reqBody,
				}
			}
		}
		backoff := max(
			c.backoff.Delay(attempt),
			retryAfter,
		)
		// A wait the context would not outlive ends the call at once.
		if dl, ok := ctx.Deadline(); ok && len(errs) > 0 && time.Now().Add(backoff).After(dl) {
			if reqBody != nil {
				reqBody.Close()
			}
			return zero, errs
		}
		if backoff > 0 {
			select {
			case <-ctx.Done():
				if reqBody != nil {
					reqBody.Close()
				}
				if len(errs) > 0 {
					return zero, errs
				}
				return zero, ctx.Err()

			case <-time.After(backoff):
				// OK.
			}
		}

		// TODO: add attempt as ctx value for future logging.

		req, err := http.NewRequestWithContext(ctx, method, u.String(), reqBody)
		if err != nil {
			if reqBody != nil {
				err = errors.Join(err, reqBody.Close())
			}
			return zero, err
		}

		// NOTE: set these headers before the c.Header and r.Header loops so
		// the users can override those.
		if ua := c.UserAgent; ua != "" {
			req.Header.Set("User-Agent", ua)
		}
		if codecBody != nil {
			req.Header.Set("Content-Type", codec.Send.MediaType())
			req.ContentLength = codecBody.Size()
		}
		if r.Recv != nil {
			req.Header.Set("Accept", codec.Recv.MediaType())
		}
		for k, vs := range c.Header {
			req.Header.Del(k)
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		for k, vs := range r.Header {
			req.Header.Del(k)
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}

		req.Header.Set("X-Request-ID", c.requestID())

		res, err := c.http.Do(req)
		if err != nil {
			errs = append(errs, &TransportError{
				Err:  err,
				Sent: requestSent(err),
			})
			retryAfter = 0
			continue
		}
		if httpSuccess(res.StatusCode) {
			var err error
			if r.Recv != nil && hasBody(method, res) {
				err = codec.Recv.Decode(r.Recv, res.Body)
			}
			res.Body.Close()
			ret := Response{
				Code:   res.StatusCode,
				Header: res.Header,
			}
			if err != nil {
				errs = append(errs, &BodyError{
					Code: res.StatusCode,
					Err:  err,
				})
				return ret, errs
			}
			return ret, nil
		}

		// A Retry-After that does not parse is no delay: ParseRetryAfter
		// returns zero with its error, and there is nothing else to do
		// with a header the server got wrong.
		retryAfter = 0
		if str := res.Header.Get("Retry-After"); str != "" {
			retryAfter, _ = ParseRetryAfter(time.Now(), str)
		}

		// Keep the start of the body for the error; Close drains the rest
		// (bounded, asynchronously) so that the connection can be reused,
		// which the transport does itself since Go 1.27, this module's
		// floor.
		bts, _ := io.ReadAll(io.LimitReader(res.Body, 1<<12))
		res.Body.Close()

		statusErr := &StatusError{
			Code:   res.StatusCode,
			Header: res.Header,
			Body:   bts,
		}
		if (c.Error != nil || r.Error != nil) && codec.Error != nil {
			var obj any
			if obj == nil && r.Error != nil {
				obj = r.Error(res.StatusCode)
			}
			if obj == nil && c.Error != nil {
				obj = c.Error(res.StatusCode)
			}
			if obj != nil {
				err := codec.Error.Decode(obj, bytes.NewReader(bts))
				if err == nil {
					statusErr.Desc = obj
				}
			}
		}
		errs = append(errs, statusErr)
	}
	if len(errs) == 0 {
		panic("internal error: an attempt must have failed here")
	}
	return zero, errs
}

// AttemptErrors is the failure of a call: each attempt's failure, oldest
// first, the last one ending the call. Each is a *StatusError, a
// *TransportError or a *BodyError.
type AttemptErrors []error

func (e AttemptErrors) Error() string {
	last := e.last()
	if last == nil {
		return "<nil>"
	}
	return last.Error()
}

// Unwrap returns the last failure, so that a match sees the attempt that
// ended the call.
func (e AttemptErrors) Unwrap() error {
	return e.last()
}

// last returns the failure that ended the call, or nil when e is empty.
func (e AttemptErrors) last() error {
	if len(e) == 0 {
		return nil
	}
	return e[len(e)-1]
}

// BodyError is a successful response whose body could not be read or
// decoded.
type BodyError struct {
	// Code is the response's status code.
	Code int

	// Err is the failure.
	Err error
}

func (e *BodyError) Error() string {
	return e.Err.Error()
}

func (e *BodyError) Unwrap() error {
	return e.Err
}

// TransportError is a request that got no response.
type TransportError struct {
	// Err is the failure.
	Err error

	// Sent reports whether the request may have reached the server. It is
	// false only when the connection itself failed: a DNS lookup or a dial.
	Sent bool
}

func (e *TransportError) Error() string {
	return e.Err.Error()
}

func (e *TransportError) Unwrap() error {
	return e.Err
}

// bodyReader marks the request body's own read failures, so that a failure of
// the payload, whatever its error, is never taken for one of the connection.
type bodyReader struct {
	io.ReadCloser
}

func (b bodyReader) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		err = &bodyReadError{
			err: err,
		}
	}
	return n, err
}

// bodyReadError is a failure reading the request body.
type bodyReadError struct {
	err error
}

func (e *bodyReadError) Error() string {
	return e.err.Error()
}

func (e *bodyReadError) Unwrap() error {
	return e.err
}

// requestSent reports whether err leaves open that the request reached the
// server. It errs on the side of yes: only a failed DNS lookup or dial rules
// it out, and never one the request body's own reader returned, since the
// body is read only once the request is under way.
func requestSent(err error) bool {
	if _, ok := errors.AsType[*bodyReadError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return false
	}
	if e, ok := errors.AsType[*net.OpError](err); ok && e.Op == "dial" {
		return false
	}
	return true
}

func (c *Client) requestID() string {
	return c.reqIDPrefix + strconv.FormatUint(c.reqID.Add(1), 10)
}

type StatusError struct {
	Code   int
	Header http.Header
	Body   []byte
	Desc   any // Maybe be an error type to work with error.As().
}

func (s *StatusError) Error() string {
	var sb strings.Builder
	sb.WriteString("httputil: unsuccessful status code: ")
	sb.WriteString(strconv.Itoa(s.Code))
	if e, ok := s.Desc.(error); ok {
		sb.WriteString(": ")
		sb.WriteString(e.Error())
	}
	return sb.String()
}
func (s *StatusError) Unwrap() error {
	e, _ := s.Desc.(error)
	return e
}

func httpSuccess(code int) bool {
	return 200 <= code && code < 300
}
func hasBody(method string, res *http.Response) bool {
	if method == "HEAD" {
		return false
	}
	switch res.StatusCode {
	case
		http.StatusNoContent,
		http.StatusResetContent:
		return false

	default:
		return true
	}
}

func cloneURL(u *url.URL) *url.URL {
	c := *u
	if info := u.User; info != nil {
		x := *info
		c.User = &x
	}
	return &c
}

type ClientBuilder struct {
	Trace RoundTripperTrace

	// If Timeout is zero then the DefaultTimeout is used.
	Timeout time.Duration

	// Transport is the round tripper at the bottom of the stack, the one that
	// reaches the network. If nil, a fresh default transport is used.
	Transport http.RoundTripper

	// RoundTripper is an optional function to stack up more round tripping
	// logic on top of the default one.
	RoundTripper func(next http.RoundTripper) http.RoundTripper
}

func (c *ClientBuilder) Build() *http.Client {
	next := c.Transport
	if next == nil {
		next = httpDefaultTransport()
	}
	var r http.RoundTripper = &TracingRoundTripper{
		Trace: c.Trace,
		Next:  next,
	}
	if f := c.RoundTripper; f != nil {
		r = f(r)
	}
	return &http.Client{
		Timeout:   cmp.Or(c.Timeout, DefaultTimeout),
		Transport: r,
	}
}

func httpDefaultTransport() *http.Transport {
	// Same as http.DefaultTransport, but a fresh instance (not sharing
	// connection pool, etc.).
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// ParseRetryAfter parses a Retry-After header value, either form of RFC 9110:
// delay-seconds or an HTTP date, which is taken relative to now. The result
// is never negative. Delay-seconds is digits only, so a signed number such as
// "-5" is malformed, not a negative delay.
func ParseRetryAfter(now time.Time, str string) (time.Duration, error) {
	sec, err0 := strconv.ParseUint(str, 10, 31)
	if err0 == nil {
		return time.Duration(sec) * time.Second, nil
	}
	t, err1 := http.ParseTime(str)
	if err1 == nil {
		return max(t.Sub(now), 0), nil
	}
	return 0, fmt.Errorf(
		"parse retry-after: %q: %w",
		str, errors.Join(err0, err1),
	)
}
