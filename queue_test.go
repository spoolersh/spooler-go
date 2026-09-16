package spooler

import (
	"net/http"
	"net/url"
	"regexp"
	"testing"
	"time"
)

// The queue and the stats every listing case below answers with, and what the
// SDK must make of them.
const (
	testQueueJSON = `{
		"name": "jobs",
		"createdAt": "2026-01-02T03:04:05Z",
		"state": "active",
		"settings": {
			"leaseTimeoutSeconds": 30,
			"maxRetries": 5,
			"dedupWindowSeconds": 60,
			"retentionSeconds": 3600,
			"effectiveRetentionSeconds": 1800,
			"recvRateLimit": {"intervalMicros": 1000, "burst": 10}
		}
	}`
	testStatsJSON = `{
		"total": 14,
		"dataBytes": 1024,
		"delayed": 1,
		"visible": 2,
		"pending": 3,
		"failed": 4,
		"unavailable": 5,
		"addedTotal": 6,
		"ackedTotal": 7,
		"nackedTotal": 8,
		"releasedTotal": 9,
		"failedTotal": 10,
		"discardedTotal": 11,
		"expiredTotal": 12
	}`
)

var (
	testCreatedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	testSettings = QueueSettings{
		LeaseTimeoutSeconds:       30,
		MaxRetries:                5,
		DedupWindowSeconds:        60,
		RetentionSeconds:          3600,
		EffectiveRetentionSeconds: 1800,
		RecvRateLimit: RateLimit{
			IntervalMicros: 1000,
			Burst:          10,
		},
	}
	testQueue = Queue{
		Name:      "jobs",
		CreatedAt: testCreatedAt,
		State:     QueueStateActive,
		Settings:  testSettings,
	}
	testStats = MessageStats{
		Total:          14,
		DataBytes:      1024,
		Delayed:        1,
		Visible:        2,
		Pending:        3,
		Failed:         4,
		Unavailable:    5,
		AddedTotal:     6,
		AckedTotal:     7,
		NackedTotal:    8,
		ReleasedTotal:  9,
		FailedTotal:    10,
		DiscardedTotal: 11,
		ExpiredTotal:   12,
	}
)

func TestQueue(t *testing.T) {
	testOp(t, (*Client).Queue, []opCase[QueueRequest, Queue]{
		{
			name: "describes a queue",
			req: QueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   testQueueJSON,
					},
				},
			},
			exp: testQueue,
		},
		{
			name: "a queue without a rate limit",
			req: QueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"name":"jobs","createdAt":"2026-01-02T03:04:05Z","state":"draining","settings":{}}`,
					},
				},
			},
			exp: Queue{
				Name:      "jobs",
				CreatedAt: testCreatedAt,
				State:     QueueStateDraining,
			},
		},
		{
			name: "a state this SDK does not know fails the read",
			req: QueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"name":"jobs","createdAt":"2026-01-02T03:04:05Z","state":"frozen","settings":{}}`,
					},
				},
			},
			err: true,
		},
		{
			name: "a state that is not a string fails the read",
			req: QueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"name":"jobs","createdAt":"2026-01-02T03:04:05Z","state":1,"settings":{}}`,
					},
				},
			},
			err: true,
		},
		{
			name: "a missing queue is a verdict",
			req: QueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Accept": {"application/json"},
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
			name: "a missing spool is a verdict",
			req: QueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 404,
						body:   `{"kind":"spool_not_found","message":"no spool default on the account"}`,
					},
				},
			},
			expErr: ErrSpoolNotFound,
		},
		{
			name: "a server failure is not retried",
			req: QueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 500,
					},
				},
			},
			err: true,
		},
		{
			name: "a rate limit is retried",
			req: QueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 429,
						header: http.Header{
							"Retry-After": {"2"},
						},
						body: `{"kind":"rate_limited","message":"slow down"}`,
					},
				},
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   testQueueJSON,
					},
				},
			},
			exp: testQueue,
		},
		{
			name: "spool is required",
			req: QueueRequest{
				Queue: "jobs",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "queue is required",
			req: QueueRequest{
				Spool: "default",
			},
			expErr: errQueueRequired,
		},
	})
}

func TestCreateQueue(t *testing.T) {
	testOp(t, noResult((*Client).CreateQueue), []opCase[CreateQueueRequest, none]{
		{
			name: "creates a queue with the server's defaults",
			req: CreateQueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "PUT",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Content-Type": {"application/json"},
						},
						body: `{}`,
					},
					res: response{
						status: 201,
					},
				},
			},
		},
		{
			name: "creates a queue with settings",
			req: CreateQueueRequest{
				Spool: "default",
				Queue: "jobs",
				Settings: QueueSettingsCreate{
					LeaseTimeoutSeconds: new(30),
					MaxRetries:          new(5),
					DedupWindowSeconds:  new(60),
					RetentionSeconds:    new(3600),
					RecvRateLimit: &RateLimit{
						IntervalMicros: 1000,
						Burst:          10,
					},
				},
			},
			wire: []exchange{
				{
					req: request{
						method: "PUT",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Content-Type": {"application/json"},
						},
						body: `{"leaseTimeoutSeconds":30,"maxRetries":5,"dedupWindowSeconds":60,"retentionSeconds":3600,"recvRateLimit":{"intervalMicros":1000,"burst":10}}`,
					},
					res: response{
						status: 201,
					},
				},
			},
		},
		{
			name: "an explicit zero is sent, unlike an unset field",
			req: CreateQueueRequest{
				Spool: "default",
				Queue: "jobs",
				Settings: QueueSettingsCreate{
					LeaseTimeoutSeconds: new(0),
					MaxRetries:          new(0),
				},
			},
			wire: []exchange{
				{
					req: request{
						method: "PUT",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Content-Type": {"application/json"},
						},
						body: `{"leaseTimeoutSeconds":0,"maxRetries":0}`,
					},
					res: response{
						status: 201,
					},
				},
			},
		},
		{
			name: "an existing queue is a verdict",
			req: CreateQueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "PUT",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Content-Type": {"application/json"},
						},
						body: `{}`,
					},
					res: response{
						status: 409,
						body:   `{"kind":"queue_exists","message":"queue jobs exists"}`,
					},
				},
			},
			expErr: ErrQueueExists,
		},
		{
			name: "spool is required",
			req: CreateQueueRequest{
				Queue: "jobs",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "queue is required",
			req: CreateQueueRequest{
				Spool: "default",
			},
			expErr: errQueueRequired,
		},
	})
}

func TestUpdateQueue(t *testing.T) {
	testOp(t, noResult((*Client).UpdateQueue), []opCase[UpdateQueueRequest, none]{
		{
			name: "updates the settings it is given",
			req: UpdateQueueRequest{
				Spool: "default",
				Queue: "jobs",
				Settings: QueueSettingsUpdate{
					MaxRetries: new(9),
				},
			},
			wire: []exchange{
				{
					req: request{
						method: "PATCH",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Content-Type": {"application/json"},
						},
						body: `{"maxRetries":9}`,
					},
					res: response{
						status: 200,
					},
				},
			},
		},
		{
			name: "a lost race is retried",
			req: UpdateQueueRequest{
				Spool: "default",
				Queue: "jobs",
				Settings: QueueSettingsUpdate{
					MaxRetries: new(9),
				},
			},
			wire: []exchange{
				{
					req: request{
						method: "PATCH",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Content-Type": {"application/json"},
						},
						body: `{"maxRetries":9}`,
					},
					res: response{
						status: 409,
						body:   `{"kind":"queue_busy","message":"a concurrent change won"}`,
					},
				},
				{
					req: request{
						method: "PATCH",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Content-Type": {"application/json"},
						},
						body: `{"maxRetries":9}`,
					},
					res: response{
						status: 200,
					},
				},
			},
		},
		{
			name: "a zero rate limit is sent to remove the limit",
			req: UpdateQueueRequest{
				Spool: "default",
				Queue: "jobs",
				Settings: QueueSettingsUpdate{
					RecvRateLimit: &RateLimit{},
				},
			},
			wire: []exchange{
				{
					req: request{
						method: "PATCH",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Content-Type": {"application/json"},
						},
						body: `{"recvRateLimit":{"intervalMicros":0,"burst":0}}`,
					},
					res: response{
						status: 200,
					},
				},
			},
		},
		{
			name: "a retention above the plan's limit is a verdict",
			req: UpdateQueueRequest{
				Spool: "default",
				Queue: "jobs",
				Settings: QueueSettingsUpdate{
					RetentionSeconds: new(999999),
				},
			},
			wire: []exchange{
				{
					req: request{
						method: "PATCH",
						path:   "/v1/spools/default/queues/jobs",
						header: http.Header{
							"Content-Type": {"application/json"},
						},
						body: `{"retentionSeconds":999999}`,
					},
					res: response{
						status: 403,
						body:   `{"kind":"retention_limit","message":"the plan allows 3600 seconds","details":{"maxSeconds":3600}}`,
					},
				},
			},
			expErr: ErrRetentionLimit,
		},
		{
			name: "spool is required",
			req: UpdateQueueRequest{
				Queue: "jobs",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "queue is required",
			req: UpdateQueueRequest{
				Spool: "default",
			},
			expErr: errQueueRequired,
		},
	})
}

func TestDeleteQueue(t *testing.T) {
	testOp(t, noResult((*Client).DeleteQueue), []opCase[DeleteQueueRequest, none]{
		{
			name: "deletes at once",
			req: DeleteQueueRequest{
				Spool: "default",
				Queue: "jobs",
				Mode:  QueueDeleteModeForce,
			},
			wire: []exchange{
				{
					req: request{
						method: "DELETE",
						path:   "/v1/spools/default/queues/jobs",
						query: url.Values{
							"mode": {"force"},
						},
					},
					res: response{
						status: 202,
					},
				},
			},
		},
		{
			name: "deletes once drained",
			req: DeleteQueueRequest{
				Spool: "default",
				Queue: "jobs",
				Mode:  QueueDeleteModeDrain,
			},
			wire: []exchange{
				{
					req: request{
						method: "DELETE",
						path:   "/v1/spools/default/queues/jobs",
						query: url.Values{
							"mode": {"drain"},
						},
					},
					res: response{
						status: 202,
					},
				},
			},
		},
		{
			name: "a queue being deleted is a verdict",
			req: DeleteQueueRequest{
				Spool: "default",
				Queue: "jobs",
				Mode:  QueueDeleteModeForce,
			},
			wire: []exchange{
				{
					req: request{
						method: "DELETE",
						path:   "/v1/spools/default/queues/jobs",
						query: url.Values{
							"mode": {"force"},
						},
					},
					res: response{
						status: 409,
						body:   `{"kind":"queue_deleting","message":"queue jobs is being deleted"}`,
					},
				},
			},
			expErr: ErrQueueDeleting,
		},
		{
			name: "a mode is required",
			req: DeleteQueueRequest{
				Spool: "default",
				Queue: "jobs",
			},
			expErr: ErrInvalidRequest,
		},
		{
			name: "spool is required",
			req: DeleteQueueRequest{
				Queue: "jobs",
				Mode:  QueueDeleteModeForce,
			},
			expErr: errSpoolRequired,
		},
		{
			name: "queue is required",
			req: DeleteQueueRequest{
				Spool: "default",
				Mode:  QueueDeleteModeForce,
			},
			expErr: errQueueRequired,
		},
	})
}

func TestSpoolQueues(t *testing.T) {
	testOp(t, (*Client).SpoolQueues, []opCase[SpoolQueuesRequest, Page[Queue]]{
		{
			name: "lists the last page",
			req: SpoolQueuesRequest{
				Spool: "default",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"items":[` + testQueueJSON + `]}`,
					},
				},
			},
			exp: Page[Queue]{
				Items: []Queue{
					testQueue,
				},
			},
		},
		{
			name: "a page with a cursor",
			req: SpoolQueuesRequest{
				Spool: "default",
				Limit: 1,
				After: "alpha",
				Match: regexp.MustCompile(`^j`),
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues",
						query: url.Values{
							"limit": {"1"},
							"after": {"alpha"},
							"match": {"^j"},
						},
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"items":[` + testQueueJSON + `],"next":"jobs"}`,
					},
				},
			},
			exp: Page[Queue]{
				Items: []Queue{
					testQueue,
				},
				Next: "jobs",
			},
		},
		{
			name: "an empty spool",
			req: SpoolQueuesRequest{
				Spool: "default",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"items":[]}`,
					},
				},
			},
		},
		{
			name: "a suspended account is a verdict",
			req: SpoolQueuesRequest{
				Spool: "default",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 402,
						body:   `{"message":"the account is suspended"}`,
					},
				},
			},
			expErr: ErrSuspended,
		},
		{
			name:   "spool is required",
			req:    SpoolQueuesRequest{},
			expErr: errSpoolRequired,
		},
	})
}

func TestSpoolStats(t *testing.T) {
	testOp(t, (*Client).SpoolStats, []opCase[SpoolStatsRequest, SpoolStats]{
		{
			name: "reads the rollup and the last page of queues",
			req: SpoolStatsRequest{
				Spool: "default",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/stats",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body: `{
							"queueCount": 3,
							"stats": ` + testStatsJSON + `,
							"queues": {
								"items": [{
									"name": "jobs",
									"createdAt": "2026-01-02T03:04:05Z",
									"state": "deleting",
									"settings": {},
									"stats": ` + testStatsJSON + `
								}]
							}
						}`,
					},
				},
			},
			exp: SpoolStats{
				QueueCount: 3,
				Stats:      testStats,
				Queues: Page[QueueStats]{
					Items: []QueueStats{
						{
							Name:      "jobs",
							CreatedAt: testCreatedAt,
							State:     QueueStateDeleting,
							Stats:     testStats,
						},
					},
				},
			},
		},
		{
			name: "reads a selected page with a cursor to the next",
			req: SpoolStatsRequest{
				Spool:  "default",
				Queues: []string{"jobs", "mail"},
				Limit:  1,
				After:  "alpha",
				Match:  regexp.MustCompile(`^j`),
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/stats",
						query: url.Values{
							"queue": {"jobs", "mail"},
							"limit": {"1"},
							"after": {"alpha"},
							"match": {"^j"},
						},
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"queueCount":3,"stats":{},"queues":{"items":[],"next":"jobs"}}`,
					},
				},
			},
			exp: SpoolStats{
				QueueCount: 3,
				Queues: Page[QueueStats]{
					Next: "jobs",
				},
			},
		},
		{
			name: "bad credentials are a verdict",
			req: SpoolStatsRequest{
				Spool: "default",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/stats",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 401,
						body:   `{"message":"bad credentials"}`,
					},
				},
			},
			expErr: ErrUnauthorized,
		},
		{
			name:   "spool is required",
			req:    SpoolStatsRequest{},
			expErr: errSpoolRequired,
		},
	})
}

func TestQueueMessages(t *testing.T) {
	testOp(t, (*Client).QueueMessages, []opCase[QueueMessagesRequest, Page[Message]]{
		{
			name: "lists messages of every state",
			req: QueueMessagesRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs/messages",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body: `{"items":[
							{"id":"1-1","state":"visible","retries":0,"createdAt":"2026-01-02T03:04:05Z"},
							{"id":"1-2","state":"delayed","retries":0,"createdAt":"2026-01-02T03:04:05Z","visibleAt":"2026-01-02T04:00:00Z"},
							{"id":"1-3","state":"pending","retries":1,"createdAt":"2026-01-02T03:04:05Z","leaseExpiresAt":"2026-01-02T03:05:00Z"},
							{"id":"1-4","state":"unavailable","retries":0,"createdAt":"2026-01-02T03:04:05Z","expiresAt":"2026-01-03T03:04:05Z"}
						]}`,
					},
				},
			},
			exp: Page[Message]{
				Items: []Message{
					{
						ID:        "1-1",
						State:     MessageStateVisible,
						CreatedAt: testCreatedAt,
					},
					{
						ID:        "1-2",
						State:     MessageStateDelayed,
						CreatedAt: testCreatedAt,
						VisibleAt: time.Date(2026, 1, 2, 4, 0, 0, 0, time.UTC),
					},
					{
						ID:             "1-3",
						State:          MessageStatePending,
						Retries:        1,
						CreatedAt:      testCreatedAt,
						LeaseExpiresAt: time.Date(2026, 1, 2, 3, 5, 0, 0, time.UTC),
					},
					{
						ID:        "1-4",
						State:     MessageStateUnavailable,
						CreatedAt: testCreatedAt,
						ExpiresAt: time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "lists failed messages with handles and a cursor to the next",
			req: QueueMessagesRequest{
				Spool:        "default",
				Queue:        "jobs",
				State:        MessageStateFailed,
				Limit:        1,
				After:        "1-1",
				ReturnHandle: true,
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs/messages",
						query: url.Values{
							"state":  {"failed"},
							"limit":  {"1"},
							"after":  {"1-1"},
							"handle": {"true"},
						},
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"items":[{"id":"1-2","state":"failed","retries":5,"createdAt":"2026-01-02T03:04:05Z","handle":"handle-1"}],"next":"1-2"}`,
					},
				},
			},
			exp: Page[Message]{
				Items: []Message{
					{
						ID:        "1-2",
						State:     MessageStateFailed,
						Retries:   5,
						CreatedAt: testCreatedAt,
						Handle:    "handle-1",
					},
				},
				Next: "1-2",
			},
		},
		{
			name: "a state this SDK does not know fails the read",
			req: QueueMessagesRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs/messages",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"items":[{"id":"1-1","state":"frozen","retries":0,"createdAt":"2026-01-02T03:04:05Z"}]}`,
					},
				},
			},
			err: true,
		},
		{
			name: "a state that is not a string fails the read",
			req: QueueMessagesRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs/messages",
						header: http.Header{
							"Accept": {"application/json"},
						},
					},
					res: response{
						status: 200,
						body:   `{"items":[{"id":"1-1","state":1,"retries":0,"createdAt":"2026-01-02T03:04:05Z"}]}`,
					},
				},
			},
			err: true,
		},
		{
			name: "an unknown state is refused",
			req: QueueMessagesRequest{
				Spool: "default",
				Queue: "jobs",
				State: MessageState(99),
			},
			expErr: ErrInvalidRequest,
		},
		{
			name: "a missing queue is a verdict",
			req: QueueMessagesRequest{
				Spool: "default",
				Queue: "jobs",
			},
			wire: []exchange{
				{
					req: request{
						method: "GET",
						path:   "/v1/spools/default/queues/jobs/messages",
						header: http.Header{
							"Accept": {"application/json"},
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
			req: QueueMessagesRequest{
				Queue: "jobs",
			},
			expErr: errSpoolRequired,
		},
		{
			name: "queue is required",
			req: QueueMessagesRequest{
				Spool: "default",
			},
			expErr: errQueueRequired,
		},
	})
}
