package httputil

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	Trace ClientTrace

	ClientBuilder ClientBuilder

	// BaseURL is the required absolute URI to use as the base for requests.
	BaseURL string

	Header http.Header

	Backoff backoff.Strategy

	// UserAgent names the calling software; sent as the User-Agent header.
	UserAgent string

	// Hostname identifies the host making the requests; currently carried
	// in the request id prefix.
	Hostname string

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
	Send   any
	Recv   any

	// Attempts is the maximum total number of request attempts. Values < 1 are
	// treated as 1, i.e. no retries.
	Attempts int

	// Retry reports whether a non-2xx response should be retried. A nil Retry
	// retries every non-2xx response until Attempts is reached.
	Retry func(*StatusError) bool

	// Error returns an error object the [Client] should try to parse
	// accordingly to the status code. The parsed value will then set as
	// StatusError.Desc.
	Error func(int) any
}

func (c *Client) Client() (*http.Client, error) {
	if err := c.init(); err != nil {
		return nil, err
	}
	return c.http, nil
}

func (c *Client) Do(ctx context.Context, method string, r Request) (err error) {
	if err := c.init(); err != nil {
		return err
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
	var reqBytes []byte
	if send := r.Send; send != nil {
		reqBytes, err = json.Marshal(send)
		if err != nil {
			return err
		}
	}
	var (
		statusErr  *StatusError
		retryAfter time.Duration
	)
	for attempt := range max(r.Attempts, 1) {
		if statusErr != nil && (r.Retry != nil && !r.Retry(statusErr)) {
			return statusErr
		}
		backoff := max(
			c.backoff.Delay(attempt),
			retryAfter,
		)
		if backoff > 0 {
			select {
			case <-ctx.Done():
				if statusErr != nil {
					return statusErr
				}
				return ctx.Err()

			case <-time.After(backoff):
				// OK.
			}
		}

		var reqBody io.Reader
		if reqBytes != nil {
			reqBody = bytes.NewReader(reqBytes)
		}

		// TODO: add attempt as ctx value for future logging.

		req, err := http.NewRequestWithContext(ctx, method, u.String(), reqBody)
		if err != nil {
			return err
		}

		// NOTE: set these headers before the c.Header and r.Header loops so
		// the users can override those.
		if ua := c.UserAgent; ua != "" {
			req.Header.Set("User-Agent", ua)
		}
		if reqBytes != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if r.Recv != nil {
			req.Header.Set("Accept", "application/json")
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
			return err
		}
		if httpSuccess(res.StatusCode) {
			var err error
			if r.Recv != nil {
				err = json.NewDecoder(res.Body).Decode(r.Recv)
			}
			res.Body.Close()
			return err
		}

		retryAfter = 0
		if str := res.Header.Get("Retry-After"); str != "" {
			sec, err := strconv.ParseInt(str, 10, 32)
			if err == nil {
				retryAfter = time.Duration(sec) * time.Second
			} else {
				t, err := time.Parse(time.RFC1123, str)
				if err == nil {
					retryAfter = time.Until(t)
				}
			}
		}

		bts, _ := io.ReadAll(res.Body)
		res.Body.Close()
		statusErr = &StatusError{
			Code: res.StatusCode,
			Body: bts,
		}
		if r.Error == nil {
			continue
		}
		obj := r.Error(res.StatusCode)
		if obj == nil {
			continue
		}
		if err := json.Unmarshal(bts, obj); err == nil {
			statusErr.Desc = obj
		}
	}
	if statusErr == nil {
		panic("internal error: status error must not be nil here")
	}
	return statusErr
}

func (c *Client) requestID() string {
	return c.reqIDPrefix + strconv.FormatUint(c.reqID.Add(1), 10)
}

type StatusError struct {
	Code int
	Body []byte
	Desc any // Maybe be error type to work with error.As().
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

	// RoundTripper is an optional function to stack up more round tripping
	// logic on top of the default one.
	RoundTripper func(next http.RoundTripper) http.RoundTripper
}

func (c *ClientBuilder) Build() *http.Client {
	var r http.RoundTripper = &TracingRoundTripper{
		Trace: c.Trace,
		Next:  httpDefaultTransport(),
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
