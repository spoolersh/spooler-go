package spooler

import (
	"net/http"
	"testing"
	"time"
)

func TestAck(t *testing.T) {
	testOp(t, noResult((*Client).Ack), []opCase[AckRequest, none]{
		{
			name: "acks",
			req: AckRequest{
				Spool: "default",
				Lease: "lease-1",
			},
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
		},
		{
			name: "retries when unavailable",
			req: AckRequest{
				Spool: "default",
				Lease: "lease-1",
			},
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
		},
		{
			name: "stale lease is a verdict",
			req: AckRequest{
				Spool: "default",
				Lease: "lease-1",
			},
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
			expErr: ErrStaleLease,
		},
		{
			name: "spool is required",
			req: AckRequest{
				Lease: "lease-1",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "lease is required",
			req: AckRequest{
				Spool: "default",
			},
			expErr: errLeaseRequired,
		},
	})
}

func TestNack(t *testing.T) {
	testOp(t, noResult((*Client).Nack), []opCase[NackRequest, none]{
		{
			name: "nacks",
			req: NackRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/nack",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "invalid lease is a verdict",
			req: NackRequest{
				Spool: "default",
				Lease: "garbage",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/nack",
						header: http.Header{
							"Spooler-Lease": {"garbage"},
						},
					},
					res: response{
						status: 400,
						body:   `{"kind":"invalid_lease","message":"the lease is malformed"}`,
					},
				},
			},
			expErr: ErrInvalidLease,
		},
		{
			name: "spool is required",
			req: NackRequest{
				Lease: "lease-1",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "lease is required",
			req: NackRequest{
				Spool: "default",
			},
			expErr: errLeaseRequired,
		},
	})
}

func TestRelease(t *testing.T) {
	testOp(t, noResult((*Client).Release), []opCase[ReleaseRequest, none]{
		{
			name: "releases",
			req: ReleaseRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/release",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "releases with a delay",
			req: ReleaseRequest{
				Spool:        "default",
				Lease:        "lease-1",
				DelaySeconds: 90,
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/release",
						header: http.Header{
							"Spooler-Lease":         {"lease-1"},
							"Spooler-Delay-Seconds": {"90"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "stale lease is a verdict",
			req: ReleaseRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/release",
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
			expErr: ErrStaleLease,
		},
		{
			name: "spool is required",
			req: ReleaseRequest{
				Lease: "lease-1",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "lease is required",
			req: ReleaseRequest{
				Spool: "default",
			},
			expErr: errLeaseRequired,
		},
	})
}

func TestFail(t *testing.T) {
	testOp(t, noResult((*Client).Fail), []opCase[FailRequest, none]{
		{
			name: "fails",
			req: FailRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/fail",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "stale lease is a verdict",
			req: FailRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/fail",
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
			expErr: ErrStaleLease,
		},
		{
			name: "spool is required",
			req: FailRequest{
				Lease: "lease-1",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "lease is required",
			req: FailRequest{
				Spool: "default",
			},
			expErr: errLeaseRequired,
		},
	})
}

func TestDiscard(t *testing.T) {
	testOp(t, noResult((*Client).Discard), []opCase[DiscardRequest, none]{
		{
			name: "discards by lease",
			req: DiscardRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/discard",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "discards by handle",
			req: DiscardRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/discard",
						header: http.Header{
							"Spooler-Handle": {"handle-1"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "stale handle is a verdict",
			req: DiscardRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/discard",
						header: http.Header{
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
			name: "both tokens are refused before any request",
			req: DiscardRequest{
				Spool:  "default",
				Lease:  "lease-1",
				Handle: "handle-1",
			},
			expErr: errLeaseAndHandle,
		},
		{
			name: "a token is required",
			req: DiscardRequest{
				Spool: "default",
			},
			expErr: errLeaseOrHandle,
		},
		{
			name: "spool is required",
			req: DiscardRequest{
				Lease: "lease-1",
			},
			expErr: errSpoolRequired,
		},
	})
}

func TestRecover(t *testing.T) {
	testOp(t, noResult((*Client).Recover), []opCase[RecoverRequest, none]{
		{
			name: "recovers",
			req: RecoverRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/recover",
						header: http.Header{
							"Spooler-Handle": {"handle-1"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "recovers with a delay",
			req: RecoverRequest{
				Spool:        "default",
				Handle:       "handle-1",
				DelaySeconds: 60,
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/recover",
						header: http.Header{
							"Spooler-Handle":        {"handle-1"},
							"Spooler-Delay-Seconds": {"60"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "a message that is not failed is a verdict",
			req: RecoverRequest{
				Spool:  "default",
				Handle: "handle-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/recover",
						header: http.Header{
							"Spooler-Handle": {"handle-1"},
						},
					},
					res: response{
						status: 409,
						body:   `{"kind":"not_failed","message":"the message is not failed"}`,
					},
				},
			},
			expErr: ErrNotFailed,
		},
		{
			name: "spool is required",
			req: RecoverRequest{
				Handle: "handle-1",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "handle is required",
			req: RecoverRequest{
				Spool: "default",
			},
			expErr: errHandleRequired,
		},
	})
}

func TestRenew(t *testing.T) {
	testOp(t, (*Client).Renew, []opCase[RenewRequest, RenewResult]{
		{
			name: "renews and reports the new expiry",
			req: RenewRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/renew",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 204,
						header: http.Header{
							"Spooler-Lease-Expires-At": {"2026-01-02T03:04:05Z"},
						},
					},
				},
			},
			exp: RenewResult{
				LeaseMeta: LeaseMeta{
					ExpiresAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
				},
			},
		},
		{
			name: "no expiry on a queue without a lease timeout",
			req: RenewRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/renew",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 204,
					},
				},
			},
		},
		{
			name: "a malformed expiry reads as no expiry",
			req: RenewRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/renew",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
						},
					},
					res: response{
						status: 204,
						header: http.Header{
							"Spooler-Lease-Expires-At": {"soon"},
						},
					},
				},
			},
		},
		{
			name: "stale lease is a verdict",
			req: RenewRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/renew",
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
			expErr: ErrStaleLease,
		},
		{
			name: "spool is required",
			req: RenewRequest{
				Lease: "lease-1",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "lease is required",
			req: RenewRequest{
				Spool: "default",
			},
			expErr: errLeaseRequired,
		},
	})
}
