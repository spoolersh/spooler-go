package spooler

import (
	"context"
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

// TestRecvTrace checks that a receive is one operation that ends once, when
// the payload has been read, the stream closed or the body failed, with the
// failure if any.
func TestRecvTrace(t *testing.T) {
	leased := response{
		status: 200,
		header: http.Header{
			"Spooler-Lease":           {"lease-1"},
			"Spooler-Message-Id":      {"1-1"},
			"Spooler-Message-Retries": {"0"},
		},
		body: "hello",
	}
	cut := response{
		status:  200,
		header:  leased.header,
		body:    "hel",
		bodyErr: io.ErrUnexpectedEOF,
		size:    5,
	}
	recv := request{
		method: "POST",
		path:   "/v1/spools/default/queues/jobs/recv",
		header: http.Header{
			"Accept": {"application/octet-stream"},
		},
	}
	for _, test := range []struct {
		name   string
		stream bool
		res    response

		// read is what the caller does with the stream; nil reads nothing.
		read func(*testing.T, RecvResult)

		// ends is each end's error as the hook got it, in order; nil must
		// be nil, the rest is matched with errors.Is.
		ends  []error
		expID string // The id the result must carry, when set.
	}{
		{
			name: "a buffered receive ends after the payload is read",
			res:  leased,
			ends: []error{nil},
		},
		{
			// Buffered, the SDK reads and classifies: the hook sees the
			// error the caller gets, an unknown result.
			name:  "a buffered read cut short ends with the result unknown",
			res:   cut,
			ends:  []error{ErrResultUnknown},
			expID: "1-1",
		},
		{
			name:   "a stream read to its end ends the operation at EOF",
			stream: true,
			res:    leased,
			read: func(t *testing.T, msg RecvResult) {
				if _, err := io.ReadAll(msg.DataStream); err != nil {
					t.Fatalf(
						"read: %v",
						err,
					)
				}
			},
			ends: []error{nil},
		},
		{
			name:   "a stream closed before its end ends the operation cleanly",
			stream: true,
			res:    leased,
			read: func(t *testing.T, msg RecvResult) {
				msg.DataStream.Close()
			},
			ends: []error{nil},
		},
		{
			name:   "a stream cut short ends the operation with an unexpected EOF",
			stream: true,
			res:    cut,
			read: func(t *testing.T, msg RecvResult) {
				io.ReadAll(msg.DataStream)
			},
			ends:  []error{io.ErrUnexpectedEOF},
			expID: "1-1",
		},
		{
			name:   "a close after EOF, and a second close, end nothing again",
			stream: true,
			res:    leased,
			read: func(t *testing.T, msg RecvResult) {
				io.ReadAll(msg.DataStream)
				msg.DataStream.Close()
				msg.DataStream.Close()
			},
			ends: []error{nil},
		},
		{
			name: "no message ends at once",
			res: response{
				status: 204,
			},
			ends: []error{nil},
		},
		{
			name:   "a stream neither read nor closed leaves the operation open",
			stream: true,
			res:    leased,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var (
					started int
					ends    []error
				)
				c := &Client{
					Trace: ClientTrace{
						OnRequest: func(_ context.Context, rt RequestTrace) func(error) {
							if act, exp := rt.Op, OpRecv; act != exp {
								t.Errorf(
									"op: %v; want %v",
									act, exp,
								)
							}
							started++
							return func(err error) {
								ends = append(ends, err)
							}
						},
					},
				}
				done := stubWire(t, c, []exchange{
					{
						req: recv,
						res: test.res,
					},
				})
				msg, _, _ := c.Recv(t.Context(), RecvRequest{
					Spool:  "default",
					Queue:  "jobs",
					Stream: test.stream,
				})
				if test.read != nil {
					test.read(t, msg)
				}
				done()
				if act, exp := msg.ID, test.expID; exp != "" && act != exp {
					t.Errorf(
						"id: %q; want %q",
						act, exp,
					)
				}
				if act, exp := started, 1; act != exp {
					t.Errorf(
						"operations started: %d; want %d",
						act, exp,
					)
				}
				if act, exp := len(ends), len(test.ends); act != exp {
					t.Fatalf(
						"ends: %d; want %d",
						act, exp,
					)
				}
				for i, exp := range test.ends {
					assertError(t, ends[i], exp, false)
				}
				// A stream left open holds the transport's goroutine, which
				// the bubble cannot exit with; the assertions above are what
				// the row is about.
				if msg.DataStream != nil {
					msg.DataStream.Close()
				}
			})
		})
	}
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
