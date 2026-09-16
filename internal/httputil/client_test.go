package httputil

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/spoolersh/spooler-go/internal/backoff"
)

// textCodec is the simplest codec that can drive Do: a string is the body.
// It sends a string as is and a oneShot once only; it decodes a body into a
// *string, or into an error body's Kind, and refuses a body that is not text.
type textCodec struct{}

// oneShot is a body that cannot be replayed and has no known size.
type oneShot string

var (
	textEncoder Encoder = textCodec{}
	textDecoder Decoder = textCodec{}
)

func (textCodec) MediaType() string { return "text/plain" }

func (textCodec) Encode(v any) (Body, error) {
	switch x := v.(type) {
	case string:
		return &textBody{
			text: x,
			size: int64(len(x)),
		}, nil
	case oneShot:
		return &textBody{
			text: string(x),
			size: -1,
			once: true,
		}, nil
	default:
		return nil, errBoom
	}
}

func (textCodec) Decode(v any, r io.Reader) error {
	p, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if !utf8.Valid(p) {
		return errBoom
	}
	switch x := v.(type) {
	case *string:
		*x = string(p)
	case *clientErrBody:
		x.Kind = string(p)
	case *requestErrBody:
		x.Kind = string(p)
	default:
		return errBoom
	}
	return nil
}

type textBody struct {
	text string
	size int64
	once bool
	used bool
}

func (b *textBody) Size() int64 { return b.size }

func (b *textBody) Open() (io.ReadCloser, error) {
	if b.used && b.once {
		return nil, errBoom
	}
	b.used = true
	return io.NopCloser(strings.NewReader(b.text)), nil
}

// Error bodies for the decoding cases, one per factory.
type (
	clientErrBody struct {
		Kind string
	}
	requestErrBody struct {
		Kind string
	}
)

var errBoom = errors.New("boom")

func TestDo(t *testing.T) {
	for _, test := range []struct {
		name string

		// The client. The base URL is "https://api.test/v1" unless set;
		// the codecs are the text codec unless noCodec is set.
		baseURL   string
		userAgent string
		hostname  string
		header    http.Header
		noCodec   bool
		withError bool // Set the client's error factory.
		backoff   backoff.Strategy

		// The call. recv says whether Request.Recv points at a string.
		method      string
		req         Request
		recv        bool
		cancelAfter int // Cancel the context once this many requests were made.

		wire []exchange

		exp       Response
		expRecv   string
		expStatus *StatusError // Compared on Code, Body and Desc.
		expErr    error        // Matched with errors.Is.
		err       bool         // Presence only.
		elapsed   time.Duration
	}{
		{
			name:   "a get with a query joins the base path and decodes the result",
			method: "GET",
			req: Request{
				Path: "/spools/a",
				Query: url.Values{
					"limit": {"1"},
				},
			},
			recv: true,
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/spools/a?limit=1",
						header: http.Header{
							"Accept": {"text/plain"},
						},
					},
					res: response{
						status: 200,
						header: http.Header{
							"X-Answer": {"yes"},
						},
						body: "result",
					},
				},
			},
			exp: Response{
				Code: 200,
				Header: http.Header{
					"X-Answer": {"yes"},
				},
			},
			expRecv: "result",
		},
		{
			name:   "no path and no query use the base as is",
			method: "GET",
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1",
					},
					res: response{
						status: 204,
					},
				},
			},
			exp: Response{
				Code: 204,
			},
		},
		{
			name:   "a body is sent with its type and length",
			method: "PUT",
			req: Request{
				Path: "/x",
				Send: "hello",
			},
			wire: []exchange{
				{
					req: request{
						method: "PUT",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"Content-Type": {"text/plain"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
					},
				},
			},
			exp: Response{
				Code: 201,
			},
		},
		{
			name:   "an empty body is sent as no body",
			method: "POST",
			req: Request{
				Path: "/x",
				Send: "",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"Content-Type": {"text/plain"},
						},
					},
					res: response{
						status: 201,
					},
				},
			},
			exp: Response{
				Code: 201,
			},
		},
		{
			name:   "a body of unknown size declares no length",
			method: "POST",
			req: Request{
				Path: "/x",
				Send: oneShot("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"Content-Type": {"text/plain"},
						},
						body:    "hello",
						unsized: true,
					},
					res: response{
						status: 201,
					},
				},
			},
			exp: Response{
				Code: 201,
			},
		},
		{
			name:      "the client's headers come after the user agent, the request's after both",
			userAgent: "sdk/1",
			hostname:  "host-1",
			header: http.Header{
				"Authorization": {"Bearer k"},
				"X-A":           {"client"},
				"X-B":           {"client"},
			},
			method: "GET",
			req: Request{
				Path: "/x",
				Header: http.Header{
					"X-B": {"request"},
				},
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"User-Agent":    {"sdk/1"},
							"Authorization": {"Bearer k"},
							"X-A":           {"client"},
							"X-B":           {"request"},
						},
						idPrefix: "host-1/",
					},
					res: response{
						status: 204,
					},
				},
			},
			exp: Response{
				Code: 204,
			},
		},
		{
			name:      "the client's headers may replace the user agent",
			userAgent: "sdk/1",
			header: http.Header{
				"User-Agent": {"custom"},
			},
			method: "GET",
			req: Request{
				Path: "/x",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"User-Agent": {"custom"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
			exp: Response{
				Code: 204,
			},
		},
		{
			name:   "attempts below one make one",
			method: "GET",
			req: Request{
				Path:     "/x",
				Attempts: 0,
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 500,
					},
				},
			},
			expStatus: &StatusError{
				Code: 500,
			},
		},
		{
			name:    "no retry policy retries every failure until the attempts run out",
			backoff: backoff.Constant(0),
			method:  "GET",
			req: Request{
				Path:     "/x",
				Attempts: 3,
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 500,
						body:   "one",
					},
				},
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 500,
						body:   "two",
					},
				},
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 500,
						body:   "three",
					},
				},
			},
			expStatus: &StatusError{
				Code: 500,
				Body: []byte("three"),
			},
		},
		{
			name:   "a retry policy that says no",
			method: "GET",
			req: Request{
				Path:     "/x",
				Attempts: 2,
				Retry: func(*StatusError) bool {
					return false
				},
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 500,
					},
				},
			},
			expStatus: &StatusError{
				Code: 500,
			},
		},
		{
			name:    "a retry waits the backoff delay",
			backoff: backoff.Constant(time.Second),
			method:  "GET",
			req: Request{
				Path:     "/x",
				Attempts: 2,
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 503,
					},
				},
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 204,
					},
				},
			},
			exp: Response{
				Code: 204,
			},
			elapsed: time.Second,
		},
		{
			name:    "a retry-after delay outweighs the backoff",
			backoff: backoff.Constant(time.Second),
			method:  "GET",
			req: Request{
				Path:     "/x",
				Attempts: 2,
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 429,
						header: http.Header{
							"Retry-After": {"30"},
						},
					},
				},
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 204,
					},
				},
			},
			exp: Response{
				Code: 204,
			},
			elapsed: 30 * time.Second,
		},
		{
			name:   "a retry-after date is taken relative to now",
			method: "GET",
			req: Request{
				Path:     "/x",
				Attempts: 2,
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 503,
						header: http.Header{
							// synctest's clock starts at 2000-01-01 00:00:00 UTC.
							"Retry-After": {"Sat, 01 Jan 2000 00:00:10 GMT"},
						},
					},
				},
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 204,
					},
				},
			},
			exp: Response{
				Code: 204,
			},
			elapsed: 10 * time.Second,
		},
		{
			name:    "cancelled while waiting to retry returns the last answer",
			backoff: backoff.Constant(time.Minute),
			method:  "GET",
			req: Request{
				Path:     "/x",
				Attempts: 2,
			},
			cancelAfter: 1,
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 503,
					},
				},
			},
			expStatus: &StatusError{
				Code: 503,
			},
		},
		{
			name:   "a transport failure is returned as is and not retried",
			method: "GET",
			req: Request{
				Path:     "/x",
				Attempts: 2,
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						err: errBoom,
					},
				},
			},
			expErr: errBoom,
		},
		{
			name:      "an error body is decoded with the client's factory",
			withError: true,
			method:    "GET",
			req: Request{
				Path: "/x",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 404,
						body:   "gone",
					},
				},
			},
			expStatus: &StatusError{
				Code: 404,
				Body: []byte("gone"),
				Desc: &clientErrBody{
					Kind: "gone",
				},
			},
		},
		{
			name:      "the request's factory wins over the client's",
			withError: true,
			method:    "GET",
			req: Request{
				Path: "/x",
				Error: func(int) any {
					return new(requestErrBody)
				},
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 404,
						body:   "gone",
					},
				},
			},
			expStatus: &StatusError{
				Code: 404,
				Body: []byte("gone"),
				Desc: &requestErrBody{
					Kind: "gone",
				},
			},
		},
		{
			name:      "an error body that does not decode keeps its bytes",
			withError: true,
			method:    "GET",
			req: Request{
				Path: "/x",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 502,
						body:   "\xff",
					},
				},
			},
			expStatus: &StatusError{
				Code: 502,
				Body: []byte("\xff"),
			},
		},
		{
			name:   "an error body is capped",
			method: "GET",
			req: Request{
				Path: "/x",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
					},
					res: response{
						status: 502,
						body:   strings.Repeat("x", 5000),
					},
				},
			},
			expStatus: &StatusError{
				Code: 502,
				Body: []byte(strings.Repeat("x", 4096)),
			},
		},
		{
			name:    "a body without a send codec is refused before any request",
			noCodec: true,
			method:  "PUT",
			req: Request{
				Path: "/x",
				Send: "hello",
			},
			err: true,
		},
		{
			name:    "a result without a recv codec is refused before any request",
			noCodec: true,
			method:  "GET",
			req: Request{
				Path: "/x",
			},
			recv: true,
			err:  true,
		},
		{
			name:    "a bad base url fails every call",
			baseURL: "://",
			method:  "GET",
			req: Request{
				Path: "/x",
			},
			err: true,
		},
		{
			name:   "a body that cannot be encoded",
			method: "PUT",
			req: Request{
				Path: "/x",
				Send: 42,
			},
			err: true,
		},
		{
			// The retry is abandoned before the backoff, not after it: with
			// a minute of backoff, no time passes.
			name:    "a one-shot body is not retried",
			backoff: backoff.Constant(time.Minute),
			method:  "POST",
			req: Request{
				Path:     "/x",
				Attempts: 2,
				Send:     oneShot("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"Content-Type": {"text/plain"},
						},
						body:    "hello",
						unsized: true,
					},
					res: response{
						status: 503,
					},
				},
			},
			expStatus: &StatusError{
				Code: 503,
			},
		},
		{
			name:    "a replayable body is sent again",
			backoff: backoff.Constant(0),
			method:  "POST",
			req: Request{
				Path:     "/x",
				Attempts: 2,
				Send:     "hello",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"Content-Type": {"text/plain"},
						},
						body: "hello",
					},
					res: response{
						status: 503,
					},
				},
				{
					req: request{
						method: "POST",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"Content-Type": {"text/plain"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
					},
				},
			},
			exp: Response{
				Code: 201,
			},
		},
		{
			name:   "nothing is decoded from a 204",
			method: "GET",
			req: Request{
				Path: "/x",
			},
			recv: true,
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"Accept": {"text/plain"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
			exp: Response{
				Code: 204,
			},
		},
		{
			name:   "nothing is decoded for a head",
			method: "HEAD",
			req: Request{
				Path: "/x",
			},
			recv: true,
			wire: []exchange{
				{
					req: request{
						method: "HEAD",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"Accept": {"text/plain"},
						},
					},
					res: response{
						status: 200,
						body:   "\xff",
					},
				},
			},
			exp: Response{
				Code: 200,
			},
		},
		{
			name:   "a result that does not decode is an error",
			method: "GET",
			req: Request{
				Path: "/x",
			},
			recv: true,
			wire: []exchange{
				{
					req: request{
						method: "GET",
						url:    "https://api.test/v1/x",
						header: http.Header{
							"Accept": {"text/plain"},
						},
					},
					res: response{
						status: 200,
						body:   "\xff",
					},
				},
			},
			err: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var n int
				rt, done := stubWire(t, test.wire)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				counting := RoundTripperFunc(func(r *http.Request) (*http.Response, error) {
					res, err := rt.RoundTrip(r)
					n++
					if n == test.cancelAfter {
						cancel()
					}
					return res, err
				})

				c := &Client{
					ClientBuilder: ClientBuilder{
						Transport: counting,
					},
					BaseURL:   "https://api.test/v1",
					UserAgent: test.userAgent,
					Hostname:  test.hostname,
					Header:    test.header,
					Backoff:   test.backoff,
				}
				if test.baseURL != "" {
					c.BaseURL = test.baseURL
				}
				if !test.noCodec {
					c.Codec = Codecs{
						Send:  textEncoder,
						Recv:  textDecoder,
						Error: textDecoder,
					}
				}
				if test.withError {
					c.Error = func(int) any {
						return new(clientErrBody)
					}
				}

				req := test.req
				var recv string
				if test.recv {
					req.Recv = &recv
				}

				start := time.Now()
				act, err := c.Do(ctx, test.method, req)
				elapsed := time.Since(start)
				done()

				if act, exp := elapsed, test.elapsed; act != exp {
					t.Errorf(
						"elapsed: %v; want %v",
						act, exp,
					)
				}
				switch {
				case test.expStatus != nil:
					se, ok := errors.AsType[*StatusError](err)
					if !ok {
						t.Fatalf(
							"error: %v; want *StatusError",
							err,
						)
					}
					if diff := cmp.Diff(test.expStatus, se, cmpopts.IgnoreFields(StatusError{}, "Header"), cmpopts.EquateEmpty()); diff != "" {
						t.Errorf(
							"status error mismatch (-want +act):\n%s",
							diff,
						)
					}
				case test.expErr != nil:
					if !errors.Is(err, test.expErr) {
						t.Errorf(
							"error: %v; want %v",
							err, test.expErr,
						)
					}
				case test.err:
					if err == nil {
						t.Errorf("want error; got nothing")
					}
				default:
					if err != nil {
						t.Fatalf(
							"unexpected error: %v",
							err,
						)
					}
					if diff := cmp.Diff(test.exp, act, cmpopts.EquateEmpty()); diff != "" {
						t.Errorf(
							"response mismatch (-want +act):\n%s",
							diff,
						)
					}
				}
				if act, exp := recv, test.expRecv; act != exp {
					t.Errorf(
						"decoded: %q; want %q",
						act, exp,
					)
				}
			})
		})
	}
}
