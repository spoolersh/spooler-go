package httputil

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// exchange is one request the client must make and the response it gets.
type exchange struct {
	req request
	res response
}

// request is what the client must send, exactly. X-Request-ID is checked
// for presence on every request and is not listed here.
type request struct {
	method string
	url    string
	header http.Header
	body   string

	// unsized means the body's length is not declared, as for a reader of
	// unknown size, so the request carries no content length.
	unsized bool

	// idPrefix, when set, is what the request id must begin with.
	idPrefix string
}

// response is what the stub answers: a status with headers and a body, or,
// when err is set, a transport failure with no response at all.
type response struct {
	status int
	header http.Header
	body   string
	err    error
}

// stubWire returns a transport that replays wire in order and a func that
// reports the exchanges never used.
func stubWire(t *testing.T, wire []exchange) (rt http.RoundTripper, done func()) {
	t.Helper()
	var n int
	rt = RoundTripperFunc(func(r *http.Request) (*http.Response, error) {
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
	})
	return rt, func() {
		t.Helper()
		if act, exp := n, len(wire); act != exp {
			t.Errorf(
				"requests: %d; want %d",
				act, exp,
			)
		}
	}
}

func assertRequest(t *testing.T, n int, r *http.Request, x request) {
	t.Helper()
	if act, exp := r.Method, x.method; act != exp {
		t.Errorf(
			"request %d: method: %s; want %s",
			n, act, exp,
		)
	}
	if act, exp := r.URL.String(), x.url; act != exp {
		t.Errorf(
			"request %d: url: %s; want %s",
			n, act, exp,
		)
	}
	header := r.Header.Clone()
	if id := header.Get("X-Request-Id"); id == "" {
		t.Errorf(
			"request %d: want request id; got nothing",
			n,
		)
	} else if !strings.HasPrefix(id, x.idPrefix) {
		t.Errorf(
			"request %d: request id: %q; want prefix %q",
			n, id, x.idPrefix,
		)
	}
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
	length := int64(len(x.body))
	if x.unsized {
		length = -1
	}
	if act, exp := r.ContentLength, length; act != exp {
		t.Errorf(
			"request %d: content length: %d; want %d",
			n, act, exp,
		)
	}
}

func reply(r *http.Request, x response) (*http.Response, error) {
	if x.err != nil {
		return nil, &url.Error{
			Op:  r.Method,
			URL: r.URL.String(),
			Err: x.err,
		}
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
