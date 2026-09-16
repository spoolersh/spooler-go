package spooler

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"
)

// clientConfig is the part of Client a test can hold and compare: Client
// itself carries a sync.Once and must not be copied.
type clientConfig struct {
	Debug      bool
	Host       string
	DisableTLS bool
	APIKey     string
}

func (c clientConfig) client() *Client {
	return &Client{
		InsecureDebug:      c.Debug,
		Host:               c.Host,
		InsecureDisableTLS: c.DisableTLS,
		APIKey:             c.APIKey,
	}
}

// TestClientAttempts checks that Attempts bounds the requests one operation
// makes of an answer that is always transient: the zero value is the
// default, 1 makes no retry, and the error returned is the last answer's.
func TestClientAttempts(t *testing.T) {
	for _, test := range []struct {
		name     string
		attempts int
		exp      int // Requests made.
	}{
		{
			name: "zero is the default",
			exp:  DefaultAttempts,
		},
		{
			name:     "one makes no retry",
			attempts: 1,
			exp:      1,
		},
		{
			name:     "three",
			attempts: 3,
			exp:      3,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var act int
				c := &Client{
					APIKey:   testKey,
					Attempts: test.attempts,
				}
				c.transport = &stubRoundTripper{
					DoRoundTrip: func(r *http.Request) (*http.Response, error) {
						act++
						return reply(r, response{
							status: 503,
						})
					},
				}
				err := c.Ack(t.Context(), AckRequest{
					Spool: "default",
					Lease: "lease-1",
				})
				if !errors.Is(err, ErrUnavailable) {
					t.Errorf(
						"error: %v; want %v",
						err, ErrUnavailable,
					)
				}
				if act, exp := act, test.exp; act != exp {
					t.Errorf(
						"requests: %d; want %d",
						act, exp,
					)
				}
			})
		})
	}
}

// TestClientSetup checks what the client's configuration becomes on the
// wire: the scheme, the host and the version prefix, which the operation
// tables do not look at.
func TestClientSetup(t *testing.T) {
	for _, test := range []struct {
		name   string
		config clientConfig
		exp    string // The URL of a settle on spool "default".
	}{
		{
			name: "defaults",
			exp:  "https://api.spooler.sh/v1/spools/default/ack",
		},
		{
			name: "a host",
			config: clientConfig{
				Host: "example.com",
			},
			exp: "https://example.com/v1/spools/default/ack",
		},
		{
			name: "a host with a port",
			config: clientConfig{
				Host: "example.com:8443",
			},
			exp: "https://example.com:8443/v1/spools/default/ack",
		},
		{
			name: "the local image",
			config: clientConfig{
				Host:       "localhost:8080",
				DisableTLS: true,
			},
			exp: "http://localhost:8080/v1/spools/default/ack",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var act string
				c := test.config.client()
				c.APIKey = testKey
				c.transport = &stubRoundTripper{
					DoRoundTrip: func(r *http.Request) (*http.Response, error) {
						act = r.URL.String()
						return reply(r, response{
							status: 204,
						})
					},
				}
				err := c.Ack(t.Context(), AckRequest{
					Spool: "default",
					Lease: "lease-1",
				})
				if err != nil {
					t.Fatalf(
						"unexpected error: %v",
						err,
					)
				}
				if act, exp := act, test.exp; act != exp {
					t.Errorf(
						"url: %s; want %s",
						act, exp,
					)
				}
			})
		})
	}
}

// TestClientServerMisbehaves checks that a receive whose expiry and retry
// count do not parse still succeeds, with the expiry zero and the count -1,
// and reports each to OnMalformedResponse.
func TestClientServerMisbehaves(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls int
		c := &Client{
			Trace: ClientTrace{
				OnMalformedResponse: func(_ context.Context, rt RequestTrace, err error) {
					calls++
					if act, exp := rt.Op, OpRecv; act != exp {
						t.Errorf(
							"op: %v; want %v",
							act, exp,
						)
					}
					if err == nil {
						t.Errorf("want an error; got nothing")
					}
				},
			},
		}
		done := stubWire(t, c, []exchange{
			{
				req: request{
					method: "POST",
					path:   "/v1/spools/default/queues/jobs/recv",
					header: http.Header{
						"Accept": {"application/octet-stream"},
					},
				},
				res: response{
					status: 200,
					header: http.Header{
						"Spooler-Lease":            {"lease-1"},
						"Spooler-Lease-Expires-At": {"soon"},
						"Spooler-Message-Id":       {"1-1"},
						"Spooler-Message-Retries":  {"many"},
					},
					body: "hello",
				},
			},
		})
		msg, ok, err := c.Recv(t.Context(), RecvRequest{
			Spool: "default",
			Queue: "jobs",
		})
		done()
		if err != nil {
			t.Fatalf(
				"unexpected error: %v",
				err,
			)
		}
		if !ok {
			t.Fatalf("want a message; got nothing")
		}
		exp := RecvResult{
			ID:      "1-1",
			Retries: -1,
			Lease:   "lease-1",
			Data:    []byte("hello"),
		}
		if diff := cmp.Diff(exp, msg); diff != "" {
			t.Errorf(
				"result mismatch (-want +act):\n%s",
				diff,
			)
		}
		if act, exp := calls, 2; act != exp {
			t.Errorf(
				"calls: %d; want %d",
				act, exp,
			)
		}
	})
}

// TestClientTrace checks that OnRequest fires once per operation, with the
// operation's names and the caller's context, and reports the outcome; a
// retry inside the operation is not a second call.
func TestClientTrace(t *testing.T) {
	type key struct{}
	for _, test := range []struct {
		name   string
		wire   []exchange
		exp    RequestTrace
		expErr error
	}{
		{
			name: "a settle",
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/ack",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
			exp: RequestTrace{
				Op:    OpAck,
				Spool: "default",
			},
		},
		{
			name: "a retried settle is one call",
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/ack",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 503,
					},
				},
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/ack",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
			exp: RequestTrace{
				Op:    OpAck,
				Spool: "default",
			},
		},
		{
			name: "a failed settle carries its error",
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/ack",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 410,
						body:   `{"kind":"stale_lease","message":"the lease is stale"}`,
					},
				},
			},
			exp: RequestTrace{
				Op:    OpAck,
				Spool: "default",
			},
			expErr: ErrStaleLease,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var (
					calls  int
					got    RequestTrace
					gotCtx context.Context
					gotErr error
				)
				c := &Client{
					Trace: ClientTrace{
						OnRequest: func(ctx context.Context, rt RequestTrace) func(error) {
							calls++
							got, gotCtx = rt, ctx
							return func(err error) {
								gotErr = err
							}
						},
					},
				}
				done := stubWire(t, c, test.wire)
				ctx := context.WithValue(t.Context(), key{}, "app")
				err := c.Ack(ctx, AckRequest{
					Spool: "default",
					Lease: "lease-1",
				})
				done()
				assertError(t, err, test.expErr, false)

				if act, exp := calls, 1; act != exp {
					t.Errorf(
						"calls: %d; want %d",
						act, exp,
					)
				}
				if diff := cmp.Diff(test.exp, got); diff != "" {
					t.Errorf(
						"trace mismatch (-want +act):\n%s",
						diff,
					)
				}
				if gotCtx == nil || gotCtx.Value(key{}) != "app" {
					t.Errorf("want the caller's context; got %v", gotCtx)
				}
				assertError(t, gotErr, test.expErr, false)
			})
		})
	}
}
