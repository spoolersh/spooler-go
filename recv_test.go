package spooler

import (
	"bytes"
	"net/http"
	"net/url"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestRecv(t *testing.T) {
	testOpOK(t, (*Client).Recv, []opCase[RecvRequest, RecvResult]{
		{
			name: "receives a message",
			req: RecvRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
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
							"Spooler-Lease-Expires-At": {"2026-01-02T03:04:05Z"},
							"Spooler-Message-Id":       {"1-1"},
							"Spooler-Message-Retries":  {"2"},
						},
						body: "hello",
					},
				},
			},
			ok: true,
			exp: RecvResult{
				ID:      "1-1",
				Retries: 2,
				Lease:   "lease-1",
				LeaseMeta: LeaseMeta{
					ExpiresAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
				},
				Data: []byte("hello"),
			},
		},
		{
			name: "no message became visible",
			req: RecvRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/recv",
						header: http.Header{
							"Accept": {"application/octet-stream"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "waits for the given time",
			req: RecvRequest{
				Spool:       "default",
				Queue:       "jobs",
				WaitSeconds: new(20),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/recv",
						query: url.Values{
							"waitSeconds": {"20"},
						},
						header: http.Header{
							"Accept": {"application/octet-stream"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "does not block when asked not to",
			req: RecvRequest{
				Spool:       "default",
				Queue:       "jobs",
				WaitSeconds: new(0),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/recv",
						query: url.Values{
							"waitSeconds": {"0"},
						},
						header: http.Header{
							"Accept": {"application/octet-stream"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "a negative wait is refused",
			req: RecvRequest{
				Spool:       "default",
				Queue:       "jobs",
				WaitSeconds: new(-1),
			},
			expErr: ErrInvalidRequest,
		},
		{
			name: "a lease without an expiry and an unreadable retry count",
			req: RecvRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
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
							"Spooler-Lease":           {"lease-1"},
							"Spooler-Message-Id":      {"1-1"},
							"Spooler-Message-Retries": {"many"},
						},
						body: "hello",
					},
				},
			},
			ok: true,
			exp: RecvResult{
				ID:      "1-1",
				Retries: -1,
				Lease:   "lease-1",
				Data:    []byte("hello"),
			},
		},
		{
			name: "a malformed expiry reads as no expiry",
			req: RecvRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
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
							"Spooler-Message-Retries":  {"0"},
						},
						body: "hello",
					},
				},
			},
			ok: true,
			exp: RecvResult{
				ID:    "1-1",
				Lease: "lease-1",
				Data:  []byte("hello"),
			},
		},
		{
			name: "an empty message is still a message",
			req: RecvRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
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
							"Spooler-Lease":           {"lease-1"},
							"Spooler-Message-Id":      {"1-1"},
							"Spooler-Message-Retries": {"0"},
						},
					},
				},
			},
			ok: true,
			exp: RecvResult{
				ID:    "1-1",
				Lease: "lease-1",
			},
		},
		{
			name: "an unexpected success status is an error",
			req: RecvRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/recv",
						header: http.Header{
							"Accept": {"application/octet-stream"},
						},
					},
					res: response{
						status: 202,
					},
				},
			},
			err: true,
		},
		{
			name: "a missing queue is a verdict",
			req: RecvRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/recv",
						header: http.Header{
							"Accept": {"application/octet-stream"},
						},
					},
					res: response{
						status: 404,
						body:   `{"kind":"queue_not_found","message":"queue jobs does not exist","details":{"queue":"jobs"}}`,
					},
				},
			},
			expErr: ErrQueueNotFound,
		},
		{
			name: "spool is required",
			req: RecvRequest{
				Queue: "jobs",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "queue is required",
			req: RecvRequest{
				Spool: "default",
			},
			expErr: errQueueRequired,
		},
	})
}

func TestPeek(t *testing.T) {
	testOp(t, (*Client).Peek, []opCase[PeekRequest, PeekResult]{
		{
			name: "peeks a failed message",
			req: PeekRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/peek",
						header: http.Header{
							"Accept":         {"application/octet-stream"},
							"Spooler-Handle": {"handle-1"},
						},
					},
					res: response{
						status: 200,
						header: http.Header{
							"Spooler-Message-Id":      {"1-1"},
							"Spooler-Message-State":   {"failed"},
							"Spooler-Message-Retries": {"5"},
						},
						body: "poison",
					},
				},
			},
			exp: PeekResult{
				ID:      "1-1",
				State:   MessageStateFailed,
				Retries: 5,
				Data:    []byte("poison"),
			},
		},
		{
			// A delayed message reads as visible once its delay elapsed; the
			// state is whatever the server says now.
			name: "peeks a visible message",
			req: PeekRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/peek",
						header: http.Header{
							"Accept":         {"application/octet-stream"},
							"Spooler-Handle": {"handle-1"},
						},
					},
					res: response{
						status: 200,
						header: http.Header{
							"Spooler-Message-Id":      {"1-1"},
							"Spooler-Message-State":   {"visible"},
							"Spooler-Message-Retries": {"0"},
						},
					},
				},
			},
			exp: PeekResult{
				ID:    "1-1",
				State: MessageStateVisible,
			},
		},
		{
			// Headers this SDK cannot read do not drop the payload.
			name: "unreadable state and retry count",
			req: PeekRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/peek",
						header: http.Header{
							"Accept":         {"application/octet-stream"},
							"Spooler-Handle": {"handle-1"},
						},
					},
					res: response{
						status: 200,
						header: http.Header{
							"Spooler-Message-Id":      {"1-1"},
							"Spooler-Message-State":   {"frozen"},
							"Spooler-Message-Retries": {"many"},
						},
						body: "poison",
					},
				},
			},
			exp: PeekResult{
				ID:      "1-1",
				Retries: -1,
				Data:    []byte("poison"),
			},
		},
		{
			name: "a stale handle is a verdict",
			req: PeekRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/peek",
						header: http.Header{
							"Accept":         {"application/octet-stream"},
							"Spooler-Handle": {"handle-1"},
						},
					},
					res: response{
						status: 410,
						body:   `{"kind":"stale_lease","message":"the message has moved"}`,
					},
				},
			},
			expErr: ErrStaleLease,
		},
		{
			name: "spool is required",
			req: PeekRequest{
				Handle: "handle-1",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "handle is required",
			req: PeekRequest{
				Spool: "default",
			},
			expErr: errHandleRequired,
		},
	})
}

// TestRecvWriter covers the payload routed to a writer: the generic runner
// compares results only, and here the bytes land outside the result.
func TestRecvWriter(t *testing.T) {
	for _, test := range []struct {
		name       string
		wire       []exchange
		ok         bool
		exp        RecvResult
		expWritten string
	}{
		{
			name: "the payload goes to the writer and not into the result",
			wire: []exchange{
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
							"Spooler-Lease":           {"lease-1"},
							"Spooler-Message-Id":      {"1-1"},
							"Spooler-Message-Retries": {"0"},
						},
						body: "hello",
					},
				},
			},
			ok: true,
			exp: RecvResult{
				ID:    "1-1",
				Lease: "lease-1",
			},
			expWritten: "hello",
		},
		{
			name: "nothing is written when no message became visible",
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/recv",
						header: http.Header{
							"Accept": {"application/octet-stream"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := new(Client)
				done := stubWire(t, c, test.wire)

				var buf bytes.Buffer
				act, ok, err := c.Recv(t.Context(), RecvRequest{
					Spool:  "default",
					Queue:  "jobs",
					Writer: &buf,
				})
				done()
				if err != nil {
					t.Fatalf(
						"unexpected error: %v",
						err,
					)
				}
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
				if act, exp := buf.String(), test.expWritten; act != exp {
					t.Errorf(
						"written: %q; want %q",
						act, exp,
					)
				}
			})
		})
	}
}

// TestPeekWriter is the writer path of Peek, for the same reason.
func TestPeekWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := new(Client)
		done := stubWire(t, c, []exchange{
			{
				req: request{
					method: "POST",
					path:   "/v1/spools/default/peek",
					header: http.Header{
						"Accept":         {"application/octet-stream"},
						"Spooler-Handle": {"handle-1"},
					},
				},
				res: response{
					status: 200,
					header: http.Header{
						"Spooler-Message-Id":      {"1-1"},
						"Spooler-Message-State":   {"failed"},
						"Spooler-Message-Retries": {"5"},
					},
					body: "poison",
				},
			},
		})

		var buf bytes.Buffer
		act, err := c.Peek(t.Context(), PeekRequest{
			Spool:  "default",
			Handle: "handle-1",
			Writer: &buf,
		})
		done()
		if err != nil {
			t.Fatalf(
				"unexpected error: %v",
				err,
			)
		}
		exp := PeekResult{
			ID:      "1-1",
			State:   MessageStateFailed,
			Retries: 5,
		}
		if diff := cmp.Diff(exp, act, cmpopts.EquateEmpty()); diff != "" {
			t.Errorf(
				"result mismatch (-want +act):\n%s",
				diff,
			)
		}
		if act, exp := buf.String(), "poison"; act != exp {
			t.Errorf(
				"written: %q; want %q",
				act, exp,
			)
		}
	})
}
