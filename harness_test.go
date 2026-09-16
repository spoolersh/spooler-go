package spooler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const testKey = "test-key"

// exchange is one request the SDK must make and the response it gets.
type exchange struct {
	req request
	res response
}

// request is what the SDK must send, exactly. Headers every request carries
// (Authorization, User-Agent, X-Request-Id) are checked for all cases and are
// not listed here.
type request struct {
	method string
	path   string
	query  url.Values
	header http.Header
	body   string
}

// response is what the stub answers: a status with headers and a body, or,
// when err is set, a transport failure with no response at all.
type response struct {
	status int
	header http.Header
	body   string
	err    error
}

// opCase is one scenario of one operation. A request the wire does not list
// fails the test, and so does a listed one that is never made; retries are
// more rows in wire.
type opCase[Req, Res any] struct {
	name   string
	req    Req
	wire   []exchange
	exp    Res
	ok     bool  // For a comma-ok operation: whether it reports ok.
	expErr error // Matched with errors.Is.
	err    bool  // Presence only.
}

// none is the result of an operation that returns only an error.
type none = struct{}

// noResult adapts an operation that returns only an error to testOp.
func noResult[Req any](
	op func(*Client, context.Context, Req) error,
) func(*Client, context.Context, Req) (none, error) {
	return func(c *Client, ctx context.Context, req Req) (none, error) {
		return none{}, op(c, ctx, req)
	}
}

type stubRoundTripper struct {
	DoRoundTrip func(*http.Request) (*http.Response, error)
}

var _ http.RoundTripper = (*stubRoundTripper)(nil)

func (s *stubRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if s.DoRoundTrip != nil {
		return s.DoRoundTrip(r)
	}
	return nil, errors.New("stub: RoundTrip is not set")
}

// testOp runs every case of one operation against a stub transport, inside a
// synctest bubble so that backoff and Retry-After waits take no real time.
func testOp[Req, Res any](
	t *testing.T,
	op func(*Client, context.Context, Req) (Res, error),
	cases []opCase[Req, Res],
) {
	t.Helper()
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := new(Client)
				done := stubWire(t, c, test.wire)
				act, err := op(c, t.Context(), test.req)
				done()
				assertError(t, err, test.expErr, test.err)
				if diff := cmp.Diff(test.exp, act, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf(
						"result mismatch (-want +act):\n%s",
						diff,
					)
				}
			})
		})
	}
}

// testOpOK is testOp for an operation with a comma-ok result, checking ok
// against the case as well.
func testOpOK[Req, Res any](
	t *testing.T,
	op func(*Client, context.Context, Req) (Res, bool, error),
	cases []opCase[Req, Res],
) {
	t.Helper()
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := new(Client)
				done := stubWire(t, c, test.wire)
				act, ok, err := op(c, t.Context(), test.req)
				done()
				assertError(t, err, test.expErr, test.err)
				if act, exp := ok, test.ok; act != exp {
					t.Errorf(
						"ok: %t; want %t",
						act, exp,
					)
				}
				if diff := cmp.Diff(test.exp, act, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf(
						"result mismatch (-want +act):\n%s",
						diff,
					)
				}
			})
		})
	}
}

// stubWire makes c talk to a stub transport that replays wire in order. The
// returned func reports the exchanges that were never used.
func stubWire(t *testing.T, c *Client, wire []exchange) (done func()) {
	t.Helper()
	var n int
	c.APIKey = testKey
	c.transport = &stubRoundTripper{
		DoRoundTrip: func(r *http.Request) (*http.Response, error) {
			if n == len(wire) {
				t.Errorf(
					"unexpected request %d: %s %s",
					n+1, r.Method, r.URL,
				)
				return nil, errors.New("unexpected request")
			}
			x := wire[n]
			n++
			assertRequest(t, n, r, x.req)
			return reply(r, x.res)
		},
	}
	return func() {
		t.Helper()
		if act, exp := n, len(wire); act != exp {
			t.Errorf(
				"requests: %d; want %d",
				act, exp,
			)
		}
	}
}

func assertError(t *testing.T, act, exp error, want bool) {
	t.Helper()
	if exp != nil {
		if !errors.Is(act, exp) {
			t.Errorf(
				"error: %v; want %v",
				act, exp,
			)
		}
		return
	}
	if want && act == nil {
		t.Errorf("want error; got nothing")
	}
	if !want && act != nil {
		t.Errorf(
			"unexpected error: %v",
			act,
		)
	}
}

// assertRequest checks request n against what the case lists, exactly.
func assertRequest(t *testing.T, n int, r *http.Request, x request) {
	t.Helper()
	if act, exp := r.Method, x.method; act != exp {
		t.Errorf(
			"request %d: method: %s; want %s",
			n, act, exp,
		)
	}
	if act, exp := r.URL.Path, x.path; act != exp {
		t.Errorf(
			"request %d: path: %s; want %s",
			n, act, exp,
		)
	}
	if diff := cmp.Diff(x.query, r.URL.Query(), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf(
			"request %d: query mismatch (-want +act):\n%s",
			n, diff,
		)
	}

	header := r.Header.Clone()
	if act, exp := header.Get("Authorization"), "Bearer "+testKey; act != exp {
		t.Errorf(
			"request %d: authorization: %q; want %q",
			n, act, exp,
		)
	}
	if act, exp := header.Get("User-Agent"), "spooler-go/"+Version; act != exp {
		t.Errorf(
			"request %d: user agent: %q; want %q",
			n, act, exp,
		)
	}
	if header.Get("X-Request-Id") == "" {
		t.Errorf(
			"request %d: want request id; got nothing",
			n,
		)
	}
	header.Del("Authorization")
	header.Del("User-Agent")
	header.Del("X-Request-Id")
	if diff := cmp.Diff(x.header, header, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf(
			"request %d: header mismatch (-want +act):\n%s",
			n, diff,
		)
	}

	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf(
				"request %d: unexpected body error: %v",
				n, err,
			)
		}
	}
	if act, exp := string(body), x.body; act != exp {
		t.Errorf(
			"request %d: body: %q; want %q",
			n, act, exp,
		)
	}
	// The API refuses a request without a length, so it must always be known.
	if act, exp := r.ContentLength, int64(len(x.body)); act != exp {
		t.Errorf(
			"request %d: content length: %d; want %d",
			n, act, exp,
		)
	}
}

func reply(r *http.Request, x response) (*http.Response, error) {
	if x.err != nil {
		return nil, x.err
	}
	header := x.header.Clone()
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{
		Status:     http.StatusText(x.status),
		StatusCode: x.status,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(x.body)),
		Request:    r,
	}, nil
}
