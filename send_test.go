package spooler

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestSend(t *testing.T) {
	testOp(t, (*Client).Send, []opCase[SendRequest, SendResult]{
		{
			name: "sends bytes",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID: "1-1",
			},
		},
		{
			name: "sends an empty message",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID: "1-1",
			},
		},
		{
			name: "sends with a delay, a dedup string and a handle",
			req: SendRequest{
				Spool:        "default",
				Queue:        "jobs",
				DelaySeconds: 300,
				Dedup:        DedupString("order-1"),
				Data:         []byte("hello"),
				ReturnHandle: true,
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						query: url.Values{
							"handle": {"true"},
						},
						header: http.Header{
							"Content-Type":          {"application/octet-stream"},
							"Spooler-Delay-Seconds": {"300"},
							"Spooler-Dedup-String":  {"order-1"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
							"Spooler-Handle":     {"handle-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID:     "1-1",
				Handle: "handle-1",
			},
		},
		{
			name: "sends with a dedup hash",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Dedup: DedupHash("00112233445566778899aabbccddeeff"),
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type":       {"application/octet-stream"},
							"Spooler-Dedup-Hash": {"00112233445566778899aabbccddeeff"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID: "1-1",
			},
		},
		{
			name: "a dedup hit reports the original message",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Dedup: DedupString("order-1"),
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type":         {"application/octet-stream"},
							"Spooler-Dedup-String": {"order-1"},
						},
						body: "hello",
					},
					res: response{
						status: 200,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID:        "1-1",
				Duplicate: true,
			},
		},
		{
			name: "sends the declared length of a reader",
			req: SendRequest{
				Spool:      "default",
				Queue:      "jobs",
				DataSource: DataSourceReader(strings.NewReader("hello world"), 5),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID: "1-1",
			},
		},
		{
			name: "sends a read seeker to its end",
			req: SendRequest{
				Spool:      "default",
				Queue:      "jobs",
				DataSource: DataSourceReadSeeker(strings.NewReader("hello")),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID: "1-1",
			},
		},
		{
			name: "an unkeyed send is not retried when the append may have happened",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
						},
						body: "hello",
					},
					res: response{
						status: 500,
						body:   `{"kind":"operation_unconfirmed","message":"durability is unknown"}`,
					},
				},
			},
			expErr: ErrOperationUnconfirmed,
		},
		{
			name: "a keyed send is retried when the append may have happened",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Dedup: DedupString("order-1"),
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type":         {"application/octet-stream"},
							"Spooler-Dedup-String": {"order-1"},
						},
						body: "hello",
					},
					res: response{
						status: 500,
						body:   `{"kind":"operation_unconfirmed","message":"durability is unknown"}`,
					},
				},
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type":         {"application/octet-stream"},
							"Spooler-Dedup-String": {"order-1"},
						},
						body: "hello",
					},
					res: response{
						status: 200,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID:        "1-1",
				Duplicate: true,
			},
		},
		{
			name: "an unkeyed send is retried when nothing was appended",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
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
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID: "1-1",
			},
		},
		{
			name: "an unkeyed send is retried after the rate limit's delay",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
						},
						body: "hello",
					},
					res: response{
						status: 429,
						header: http.Header{
							"Retry-After": {"30"},
						},
						body: `{"kind":"rate_limited","message":"slow down"}`,
					},
				},
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID: "1-1",
			},
		},
		{
			name: "a send whose key is still in flight is retried",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Dedup: DedupString("order-1"),
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type":         {"application/octet-stream"},
							"Spooler-Dedup-String": {"order-1"},
						},
						body: "hello",
					},
					res: response{
						status: 409,
						body:   `{"kind":"dedup_in_flight","message":"the same key has not settled"}`,
					},
				},
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type":         {"application/octet-stream"},
							"Spooler-Dedup-String": {"order-1"},
						},
						body: "hello",
					},
					res: response{
						status: 200,
						header: http.Header{
							"Spooler-Message-Id": {"1-1"},
						},
					},
				},
			},
			exp: SendResult{
				ID:        "1-1",
				Duplicate: true,
			},
		},
		{
			name: "a verdict is not retried",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Dedup: DedupString("order-1"),
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type":         {"application/octet-stream"},
							"Spooler-Dedup-String": {"order-1"},
						},
						body: "hello",
					},
					res: response{
						status: 413,
						body:   `{"kind":"payload_too_large","message":"the message is too large"}`,
					},
				},
			},
			expErr: ErrPayloadTooLarge,
		},
		{
			name: "a one-shot reader cannot be replayed, so the first answer stands",
			req: SendRequest{
				Spool:      "default",
				Queue:      "jobs",
				DataSource: DataSourceReader(strings.NewReader("hello"), 5),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
						},
						body: "hello",
					},
					res: response{
						status: 503,
					},
				},
			},
			expErr: ErrUnavailable,
		},
		{
			name: "no response at all is not retried",
			req: SendRequest{
				Spool: "default",
				Queue: "jobs",
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/queues/jobs/send",
						header: http.Header{
							"Content-Type": {"application/octet-stream"},
						},
						body: "hello",
					},
					res: response{
						err: errors.New("connection reset"),
					},
				},
			},
			err: true,
		},
		{
			name: "data and a data source together are refused before any request",
			req: SendRequest{
				Spool:      "default",
				Queue:      "jobs",
				Data:       []byte("hello"),
				DataSource: DataSourceReadSeeker(strings.NewReader("hello")),
			},
			expErr: errDataAndDataSource,
		},
		{
			name: "spool is required",
			req: SendRequest{
				Queue: "jobs",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "queue is required",
			req: SendRequest{
				Spool: "default",
			},
			expErr: errQueueRequired,
		},
	})
}

func TestAckAndSend(t *testing.T) {
	testOp(t, (*Client).AckAndSend, []opCase[AckAndSendRequest, AckAndSendResult]{
		{
			name: "acks and sends",
			req: AckAndSendRequest{
				Spool: "default",
				Lease: "lease-1",
				Queue: "done",
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/ack-and-send",
						header: http.Header{
							"Content-Type":  {"application/octet-stream"},
							"Spooler-Lease": {"lease-1"},
							"Spooler-Queue": {"done"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-2"},
						},
					},
				},
			},
			exp: AckAndSendResult{
				ID: "1-2",
			},
		},
		{
			name: "acks and sends an empty message",
			req: AckAndSendRequest{
				Spool: "default",
				Lease: "lease-1",
				Queue: "done",
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/ack-and-send",
						header: http.Header{
							"Spooler-Lease": {"lease-1"},
							"Spooler-Queue": {"done"},
						},
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-2"},
						},
					},
				},
			},
			exp: AckAndSendResult{
				ID: "1-2",
			},
		},
		{
			name: "acks and sends with a delay, a dedup string and a handle",
			req: AckAndSendRequest{
				Spool:        "default",
				Lease:        "lease-1",
				Queue:        "done",
				DelaySeconds: 60,
				Dedup:        DedupString("order-1"),
				DataSource:   DataSourceReadSeeker(strings.NewReader("hello")),
				ReturnHandle: true,
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/ack-and-send",
						query: url.Values{
							"handle": {"true"},
						},
						header: http.Header{
							"Content-Type":          {"application/octet-stream"},
							"Spooler-Lease":         {"lease-1"},
							"Spooler-Queue":         {"done"},
							"Spooler-Delay-Seconds": {"60"},
							"Spooler-Dedup-String":  {"order-1"},
						},
						body: "hello",
					},
					res: response{
						status: 201,
						header: http.Header{
							"Spooler-Message-Id": {"1-2"},
							"Spooler-Handle":     {"handle-2"},
						},
					},
				},
			},
			exp: AckAndSendResult{
				ID:     "1-2",
				Handle: "handle-2",
			},
		},
		{
			name: "a claimed dedup key refuses the whole call",
			req: AckAndSendRequest{
				Spool: "default",
				Lease: "lease-1",
				Queue: "done",
				Dedup: DedupString("order-1"),
				Data:  []byte("hello"),
			},
			wire: []exchange{
				{
					req: request{
						method: "POST",
						path:   "/v1/spools/default/ack-and-send",
						header: http.Header{
							"Content-Type":         {"application/octet-stream"},
							"Spooler-Lease":        {"lease-1"},
							"Spooler-Queue":        {"done"},
							"Spooler-Dedup-String": {"order-1"},
						},
						body: "hello",
					},
					res: response{
						status: 409,
						body:   `{"kind":"dedup_claimed","message":"the key is held by 1-1","details":{"id":"1-1"}}`,
					},
				},
			},
			expErr: ErrDedupClaimed,
		},
		{
			name: "data and a data source together are refused before any request",
			req: AckAndSendRequest{
				Spool:      "default",
				Lease:      "lease-1",
				Queue:      "done",
				Data:       []byte("hello"),
				DataSource: DataSourceReadSeeker(strings.NewReader("hello")),
			},
			expErr: errDataAndDataSource,
		},
		{
			name: "spool is required",
			req: AckAndSendRequest{
				Lease: "lease-1",
				Queue: "done",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "queue is required",
			req: AckAndSendRequest{
				Spool: "default",
				Lease: "lease-1",
			},
			expErr: errQueueRequired,
		},
		{
			name: "lease is required",
			req: AckAndSendRequest{
				Spool: "default",
				Queue: "done",
			},
			expErr: errLeaseRequired,
		},
	})
}
