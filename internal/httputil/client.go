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

	// Retry reports whether a non-2xx response should be retried. A nil Retry
	// retries every non-2xx response until Attempts is reached.
	Retry func(*StatusError) bool

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
		statusErr  *StatusError
		retryAfter time.Duration
	)
	for attempt := range max(r.Attempts, 1) {
		if statusErr != nil && (r.Retry != nil && !r.Retry(statusErr)) {
			return zero, statusErr
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
					if statusErr != nil {
						return zero, statusErr
					}
					return zero, err
				}
			}
		}
		backoff := max(
			c.backoff.Delay(attempt),
			retryAfter,
		)
		if backoff > 0 {
			select {
			case <-ctx.Done():
				if reqBody != nil {
					reqBody.Close()
				}
				if statusErr != nil {
					return zero, statusErr
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
			return zero, err
		}
		if httpSuccess(res.StatusCode) {
			var err error
			if r.Recv != nil && hasBody(method, res) {
				err = codec.Recv.Decode(r.Recv, res.Body)
			}
			res.Body.Close()
			return Response{
				Code:   res.StatusCode,
				Header: res.Header,
			}, err
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

		statusErr = &StatusError{
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
	}
	if statusErr == nil {
		panic("internal error: status error must not be nil here")
	}
	return zero, statusErr
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
