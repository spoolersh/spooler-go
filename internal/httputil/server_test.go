package httputil

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// arrival is what the server saw of a request on the wire.
type arrival struct {
	method        string
	contentLength string // The header as sent; "" when absent.
	chunked       bool
	body          string
	header        http.Header // The headers the case lists, as received.
}

// TestServer sends real requests to a loopback server, which is the one
// thing a stub transport cannot see: how the body and its length reach the
// wire, and that the connect hook fires. Nothing here waits, so it runs
// outside synctest.
func TestServer(t *testing.T) {
	for _, test := range []struct {
		name   string
		method string
		req    Request
		exp    arrival
	}{
		{
			name:   "a sized stream body arrives with its length and is not chunked",
			method: "POST",
			req: Request{
				Path: "/send",
				Send: strings.NewReader("hello"),
				Codec: Codecs{
					Send: StreamEncoder,
				},
			},
			exp: arrival{
				method:        "POST",
				contentLength: "5",
				body:          "hello",
				header: http.Header{
					"Content-Type":  {"application/octet-stream"},
					"Authorization": {"Bearer k"},
				},
			},
		},
		{
			name:   "an empty stream body arrives with a length of zero",
			method: "POST",
			req: Request{
				Path: "/send",
				Send: strings.NewReader(""),
				Codec: Codecs{
					Send: StreamEncoder,
				},
			},
			exp: arrival{
				method:        "POST",
				contentLength: "0",
				header: http.Header{
					"Content-Type":  {"application/octet-stream"},
					"Authorization": {"Bearer k"},
				},
			},
		},
		{
			name:   "a stream body of unknown size is chunked",
			method: "POST",
			req: Request{
				Path: "/send",
				Send: unsizedReader{
					Reader: strings.NewReader("hello"),
				},
				Codec: Codecs{
					Send: StreamEncoder,
				},
			},
			exp: arrival{
				method:  "POST",
				chunked: true,
				body:    "hello",
				header: http.Header{
					"Content-Type":  {"application/octet-stream"},
					"Authorization": {"Bearer k"},
				},
			},
		},
		{
			name:   "a json body arrives with its length",
			method: "PUT",
			req: Request{
				Path: "/queue",
				Send: map[string]int{
					"a": 1,
				},
			},
			exp: arrival{
				method:        "PUT",
				contentLength: "7",
				body:          `{"a":1}`,
				header: http.Header{
					"Content-Type":  {"application/json"},
					"Authorization": {"Bearer k"},
				},
			},
		},
		{
			name:   "a request without a body",
			method: "GET",
			req: Request{
				Path: "/queue",
			},
			exp: arrival{
				method: "GET",
				header: http.Header{
					"Authorization": {"Bearer k"},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				mu  sync.Mutex
				act arrival
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf(
						"unexpected body error: %v",
						err,
					)
				}
				mu.Lock()
				defer mu.Unlock()
				act = arrival{
					method:        r.Method,
					contentLength: r.Header.Get("Content-Length"),
					body:          string(body),
					header:        make(http.Header),
				}
				for _, enc := range r.TransferEncoding {
					if enc == "chunked" {
						act.chunked = true
					}
				}
				for k := range test.exp.header {
					act.header[k] = r.Header.Values(k)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer srv.Close()

			var (
				connects   int
				roundTrips int
			)
			c := &Client{
				ClientBuilder: ClientBuilder{
					Trace: RoundTripperTrace{
						OnConnect: func(network, addr string) func(error) {
							if act, exp := network, "tcp"; act != exp {
								t.Errorf(
									"network: %q; want %q",
									act, exp,
								)
							}
							if act, exp := addr, srv.Listener.Addr().String(); act != exp {
								t.Errorf(
									"addr: %q; want %q",
									act, exp,
								)
							}
							return func(err error) {
								if err != nil {
									t.Errorf(
										"unexpected connect error: %v",
										err,
									)
								}
								connects++
							}
						},
						OnRoundTrip: func(*http.Request) func(*http.Response, error) {
							return func(res *http.Response, err error) {
								if err != nil {
									t.Errorf(
										"unexpected round trip error: %v",
										err,
									)
								}
								roundTrips++
							}
						},
					},
				},
				BaseURL: srv.URL + "/v1",
				Codec: Codecs{
					Send: JSONEncoder,
				},
				Header: http.Header{
					"Authorization": {"Bearer k"},
				},
			}
			res, err := c.Do(t.Context(), test.method, test.req)
			if err != nil {
				t.Fatalf(
					"unexpected error: %v",
					err,
				)
			}
			if act, exp := res.Code, http.StatusNoContent; act != exp {
				t.Errorf(
					"status: %d; want %d",
					act, exp,
				)
			}
			mu.Lock()
			defer mu.Unlock()
			if diff := cmp.Diff(test.exp, act, cmp.AllowUnexported(arrival{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf(
					"arrival mismatch (-want +act):\n%s",
					diff,
				)
			}
			if act, exp := connects, 1; act != exp {
				t.Errorf(
					"connects: %d; want %d",
					act, exp,
				)
			}
			if act, exp := roundTrips, 1; act != exp {
				t.Errorf(
					"round trips: %d; want %d",
					act, exp,
				)
			}
		})
	}
}
