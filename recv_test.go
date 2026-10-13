package spooler

import (
	"io"
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
				Data:     []byte("hello"),
				DataSize: 5,
			},
		},
		{
			name: "a body cut short leaves the result unknown",
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
							"Spooler-Message-Retries":  {"0"},
						},
						body:    "hel",
						bodyErr: io.ErrUnexpectedEOF,
						size:    5,
					},
				},
			},
			// The message was leased, so the lease comes back for the caller
			// to settle, with what was read before the failure.
			ok: true,
			exp: RecvResult{
				ID:    "1-1",
				Lease: "lease-1",
				LeaseMeta: LeaseMeta{
					ExpiresAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
				},
				Data:     []byte("hel"),
				DataSize: 5,
			},
			expErr: ErrResultUnknown,
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
				ID:       "1-1",
				Retries:  -1,
				Lease:    "lease-1",
				Data:     []byte("hello"),
				DataSize: 5,
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
				ID:       "1-1",
				Lease:    "lease-1",
				Data:     []byte("hello"),
				DataSize: 5,
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

func TestInspect(t *testing.T) {
	testOp(t, (*Client).Inspect, []opCase[InspectRequest, InspectResult]{
		{
			name: "a body cut short returns the result with the error",
			req: InspectRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/inspect",
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
						body:    "poi",
						bodyErr: io.ErrUnexpectedEOF,
						size:    6,
					},
				},
			},
			exp: InspectResult{
				ID:       "1-1",
				State:    MessageStateFailed,
				Retries:  5,
				Data:     []byte("poi"),
				DataSize: 6,
			},
			expErr: ErrResultUnknown,
		},
		{
			name: "inspects a failed message",
			req: InspectRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/inspect",
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
			exp: InspectResult{
				ID:       "1-1",
				State:    MessageStateFailed,
				Retries:  5,
				Data:     []byte("poison"),
				DataSize: 6,
			},
		},
		{
			// A delayed message reads as visible once its delay elapsed; the
			// state is whatever the server says now.
			name: "inspects a visible message",
			req: InspectRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/inspect",
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
			exp: InspectResult{
				ID:    "1-1",
				State: MessageStateVisible,
			},
		},
		{
			// Headers this SDK cannot read do not drop the payload.
			name: "unreadable state and retry count",
			req: InspectRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/inspect",
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
			exp: InspectResult{
				ID:       "1-1",
				Retries:  -1,
				Data:     []byte("poison"),
				DataSize: 6,
			},
		},
		{
			name: "a stale handle is a verdict",
			req: InspectRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/inspect",
						header: http.Header{
							"Accept":         {"application/octet-stream"},
							"Spooler-Handle": {"handle-1"},
						},
					},
					res: response{
						status: 410,
						body:   `{"kind":"stale_token","message":"the message has moved"}`,
					},
				},
			},
			expErr: ErrStaleToken,
		},
		{
			name: "spool is required",
			req: InspectRequest{
				Handle: "handle-1",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "handle is required",
			req: InspectRequest{
				Spool: "default",
			},
			expErr: errHandleRequired,
		},
	})
}

// TestRecvStream covers the payload handed back unread: the generic runner
// compares results only, and here the bytes are read after the result.
func TestRecvStream(t *testing.T) {
	for _, test := range []struct {
		name string
		wire []exchange
		ok   bool
		exp  RecvResult

		// What reading the stream to its end yields; expReadErr is matched
		// with errors.Is.
		expData    string
		expReadErr error
	}{
		{
			name: "the payload comes back as a stream, not in the result",
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
				ID:       "1-1",
				Lease:    "lease-1",
				DataSize: 5,
			},
			expData: "hello",
		},
		{
			name: "a stream cut short reads what arrived and an unexpected EOF",
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
						body:    "hel",
						bodyErr: io.ErrUnexpectedEOF,
						size:    5,
					},
				},
			},
			ok: true,
			exp: RecvResult{
				ID:       "1-1",
				Lease:    "lease-1",
				DataSize: 5,
			},
			expData:    "hel",
			expReadErr: io.ErrUnexpectedEOF,
		},
		{
			name: "a stream of undeclared length has size -1",
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
						size: -1,
					},
				},
			},
			ok: true,
			exp: RecvResult{
				ID:       "1-1",
				Lease:    "lease-1",
				DataSize: -1,
			},
			expData: "hello",
		},
		{
			name: "an empty message is a stream that ends at once",
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
			name: "no stream when no message became visible",
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

				act, ok, err := c.Recv(t.Context(), RecvRequest{
					Spool:  "default",
					Queue:  "jobs",
					Stream: true,
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
				ignore := cmpopts.IgnoreFields(RecvResult{}, "DataStream")
				if diff := cmp.Diff(test.exp, act, cmpopts.EquateEmpty(), ignore); diff != "" {
					t.Errorf(
						"result mismatch (-want +act):\n%s",
						diff,
					)
				}
				if !ok {
					if act.DataStream != nil {
						t.Errorf("want no stream; got one")
					}
					return
				}
				if act.DataStream == nil {
					t.Fatalf("want a stream; got nothing")
				}
				data, err := io.ReadAll(act.DataStream)
				assertError(t, err, test.expReadErr, false)
				if act, exp := string(data), test.expData; act != exp {
					t.Errorf(
						"read: %q; want %q",
						act, exp,
					)
				}
				if err := act.DataStream.Close(); err != nil {
					t.Errorf(
						"close: %v; want nothing",
						err,
					)
				}
			})
		})
	}
}

// TestInspectStream is the stream path of Inspect, for the same reason.
func TestInspectStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := new(Client)
		done := stubWire(t, c, []exchange{
			{
				req: request{
					method: "POST",
					path:   "/v1/spools/default/inspect",
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

		act, err := c.Inspect(t.Context(), InspectRequest{
			Spool:  "default",
			Handle: "handle-1",
			Stream: true,
		})
		done()
		if err != nil {
			t.Fatalf(
				"unexpected error: %v",
				err,
			)
		}
		exp := InspectResult{
			ID:       "1-1",
			State:    MessageStateFailed,
			Retries:  5,
			DataSize: 6,
		}
		ignore := cmpopts.IgnoreFields(InspectResult{}, "DataStream")
		if diff := cmp.Diff(exp, act, cmpopts.EquateEmpty(), ignore); diff != "" {
			t.Errorf(
				"result mismatch (-want +act):\n%s",
				diff,
			)
		}
		if act.DataStream == nil {
			t.Fatalf("want a stream; got nothing")
		}
		data, err := io.ReadAll(act.DataStream)
		if err != nil {
			t.Fatalf(
				"read: %v",
				err,
			)
		}
		if act, exp := string(data), "poison"; act != exp {
			t.Errorf(
				"read: %q; want %q",
				act, exp,
			)
		}
	})
}
