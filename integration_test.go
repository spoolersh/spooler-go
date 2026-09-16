package spooler

import (
	"context"
	"errors"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// The integration tests run against a real server, the local image in CI,
// when SPOOLER_API and SPOOLER_KEY are set, and skip otherwise. Each test
// works in a queue of its own, created for it and deleted after it, so they
// share nothing and run in parallel.

const integrationSpool = "default"

var integrationReady sync.Once

// integrationClient returns a client for the configured server, or skips.
func integrationClient(t *testing.T) *Client {
	t.Helper()
	api, key := os.Getenv("SPOOLER_API"), os.Getenv("SPOOLER_KEY")
	if api == "" || key == "" {
		t.Skip("SPOOLER_API and SPOOLER_KEY are not set")
	}
	u, err := url.Parse(api)
	if err != nil {
		t.Fatalf(
			"SPOOLER_API: %v",
			err,
		)
	}
	c := &Client{
		Host:               u.Host,
		InsecureDisableTLS: u.Scheme == "http",
		APIKey:             key,
	}
	integrationReady.Do(func() {
		// The docs' readiness check: list queues until it answers.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		for {
			_, err := c.SpoolQueues(ctx, SpoolQueuesRequest{
				Spool: integrationSpool,
			})
			if err == nil {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf(
					"server not ready: %v",
					err,
				)
			case <-time.After(200 * time.Millisecond):
			}
		}
	})
	return c
}

var (
	queueNameUnsafe = regexp.MustCompile(`[^A-Za-z0-9_:-]+`)
	queueSeq        atomic.Uint64
)

// integrationQueue creates a queue for the test and deletes it afterwards.
// The name is the test's, without the common prefix, plus a sequence number
// so a test may own several.
func integrationQueue(t *testing.T, c *Client, settings QueueSettingsCreate) string {
	t.Helper()
	name := strings.TrimPrefix(t.Name(), "TestIntegration")
	name = queueNameUnsafe.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-") + "-" + strconv.FormatUint(queueSeq.Add(1), 10)
	err := c.CreateQueue(t.Context(), CreateQueueRequest{
		Spool:    integrationSpool,
		Queue:    name,
		Settings: settings,
	})
	if err != nil {
		t.Fatalf(
			"create queue: %v",
			err,
		)
	}
	t.Cleanup(func() {
		err := c.DeleteQueue(context.Background(), DeleteQueueRequest{
			Spool: integrationSpool,
			Queue: name,
			Mode:  QueueDeleteModeForce,
		})
		if err != nil {
			t.Errorf(
				"delete queue: %v",
				err,
			)
		}
	})
	return name
}

// recvNow receives without waiting.
func recvNow(t *testing.T, c *Client, queue string) (RecvResult, bool) {
	t.Helper()
	return recvWait(t, c, queue, 0)
}

// recvWait receives with the given wait.
func recvWait(t *testing.T, c *Client, queue string, seconds int) (RecvResult, bool) {
	t.Helper()
	msg, ok, err := c.Recv(t.Context(), RecvRequest{
		Spool:       integrationSpool,
		Queue:       queue,
		WaitSeconds: &seconds,
	})
	if err != nil {
		t.Fatalf(
			"recv: %v",
			err,
		)
	}
	return msg, ok
}

// mustRecv receives without waiting and fails if nothing was there.
func mustRecv(t *testing.T, c *Client, queue string) RecvResult {
	t.Helper()
	msg, ok := recvNow(t, c, queue)
	if !ok {
		t.Fatalf("want a message; got nothing")
	}
	return msg
}

// mustRecvWait is mustRecv with a wait.
func mustRecvWait(t *testing.T, c *Client, queue string, seconds int) RecvResult {
	t.Helper()
	msg, ok := recvWait(t, c, queue, seconds)
	if !ok {
		t.Fatalf("want a message; got nothing")
	}
	return msg
}

// assertEmpty receives without waiting and fails if a message was there.
func assertEmpty(t *testing.T, c *Client, queue, when string) {
	t.Helper()
	if msg, ok := recvNow(t, c, queue); ok {
		t.Errorf(
			"%s: got message %s; want nothing",
			when, msg.ID,
		)
	}
}

func send(t *testing.T, c *Client, queue string, req SendRequest) SendResult {
	t.Helper()
	req.Spool = integrationSpool
	req.Queue = queue
	res, err := c.Send(t.Context(), req)
	if err != nil {
		t.Fatalf(
			"send: %v",
			err,
		)
	}
	return res
}

func TestIntegrationSendRecvAck(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		req  SendRequest
		exp  []byte
	}{
		{
			name: "bytes",
			req: SendRequest{
				Data: []byte("hello"),
			},
			exp: []byte("hello"),
		},
		{
			name: "an empty message",
			req:  SendRequest{},
		},
		{
			name: "a reader",
			req: SendRequest{
				DataSource: DataSourceReader(strings.NewReader("hello world"), 5),
			},
			exp: []byte("hello"),
		},
		{
			name: "a read seeker",
			req: SendRequest{
				DataSource: DataSourceReadSeeker(strings.NewReader("hello")),
			},
			exp: []byte("hello"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := integrationClient(t)
			queue := integrationQueue(t, c, QueueSettingsCreate{})

			sent := send(t, c, queue, test.req)
			if sent.ID == "" {
				t.Errorf("want a message id; got nothing")
			}
			if sent.Duplicate {
				t.Errorf("duplicate: true; want false")
			}

			msg := mustRecv(t, c, queue)
			if act, exp := msg.ID, sent.ID; act != exp {
				t.Errorf(
					"message id: %s; want %s",
					act, exp,
				)
			}
			if act, exp := msg.Retries, 0; act != exp {
				t.Errorf(
					"retry count: %d; want %d",
					act, exp,
				)
			}
			if diff := cmp.Diff(test.exp, msg.Data, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf(
					"payload mismatch (-want +act):\n%s",
					diff,
				)
			}

			err := c.Ack(t.Context(), AckRequest{
				Spool: integrationSpool,
				Lease: msg.Lease,
			})
			if err != nil {
				t.Fatalf(
					"ack: %v",
					err,
				)
			}
			assertEmpty(t, c, queue, "after ack")
		})
	}
}

// TestIntegrationSettle covers what each settle does to a delivery, as the
// docs' delivery page states it.
func TestIntegrationSettle(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		settle      func(*Client, context.Context, string, Lease) error
		redelivered bool
		expRetries  int
		expState    MessageState // When not redelivered: the state listed, if any.
	}{
		{
			name: "ack removes the message",
			settle: func(c *Client, ctx context.Context, spool string, l Lease) error {
				return c.Ack(ctx, AckRequest{Spool: spool, Lease: l})
			},
		},
		{
			name: "nack returns it and counts a retry",
			settle: func(c *Client, ctx context.Context, spool string, l Lease) error {
				return c.Nack(ctx, NackRequest{Spool: spool, Lease: l})
			},
			redelivered: true,
			expRetries:  1,
		},
		{
			name: "release returns it without a retry",
			settle: func(c *Client, ctx context.Context, spool string, l Lease) error {
				return c.Release(ctx, ReleaseRequest{Spool: spool, Lease: l})
			},
			redelivered: true,
		},
		{
			name: "fail marks it failed",
			settle: func(c *Client, ctx context.Context, spool string, l Lease) error {
				return c.Fail(ctx, FailRequest{Spool: spool, Lease: l})
			},
			expState: MessageStateFailed,
		},
		{
			name: "discard removes it",
			settle: func(c *Client, ctx context.Context, spool string, l Lease) error {
				return c.Discard(ctx, DiscardRequest{Spool: spool, Lease: l})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := integrationClient(t)
			queue := integrationQueue(t, c, QueueSettingsCreate{})

			sent := send(t, c, queue, SendRequest{
				Data: []byte("hello"),
			})
			msg := mustRecv(t, c, queue)
			err := test.settle(c, t.Context(), integrationSpool, msg.Lease)
			if err != nil {
				t.Fatalf(
					"settle: %v",
					err,
				)
			}

			// A settled lease settles nothing more.
			err = c.Ack(t.Context(), AckRequest{
				Spool: integrationSpool,
				Lease: msg.Lease,
			})
			if !errors.Is(err, ErrStaleLease) {
				t.Errorf(
					"ack after settle: %v; want %v",
					err, ErrStaleLease,
				)
			}

			again, redelivered := recvNow(t, c, queue)
			if act, exp := redelivered, test.redelivered; act != exp {
				t.Fatalf(
					"redelivered: %t; want %t",
					act, exp,
				)
			}
			if test.redelivered {
				if act, exp := again.ID, sent.ID; act != exp {
					t.Errorf(
						"message id: %s; want %s",
						act, exp,
					)
				}
				if act, exp := again.Retries, test.expRetries; act != exp {
					t.Errorf(
						"retry count: %d; want %d",
						act, exp,
					)
				}
				return
			}

			page, err := c.QueueMessages(t.Context(), QueueMessagesRequest{
				Spool: integrationSpool,
				Queue: queue,
			})
			if err != nil {
				t.Fatalf(
					"list: %v",
					err,
				)
			}
			var states []MessageState
			for _, m := range page.Items {
				states = append(states, m.State)
			}
			var exp []MessageState
			if test.expState != MessageStateUnknown {
				exp = []MessageState{test.expState}
			}
			if diff := cmp.Diff(exp, states, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf(
					"listed states mismatch (-want +act):\n%s",
					diff,
				)
			}
		})
	}
}

func TestIntegrationDedup(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		settings QueueSettingsCreate
		first    Dedup
		second   Dedup
		expErr   error // On the second send.
		expDup   bool  // Whether the second send is a dedup hit.
	}{
		{
			name: "the same string inside the window is a hit",
			settings: QueueSettingsCreate{
				DedupWindowSeconds: new(60),
			},
			first:  DedupString("order-1"),
			second: DedupString("order-1"),
			expDup: true,
		},
		{
			name: "a different string is a new message",
			settings: QueueSettingsCreate{
				DedupWindowSeconds: new(60),
			},
			first:  DedupString("order-1"),
			second: DedupString("order-2"),
		},
		{
			name:   "a key on a queue without a window is refused",
			first:  DedupString("order-1"),
			expErr: ErrDedupDisabled,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := integrationClient(t)
			queue := integrationQueue(t, c, test.settings)

			first, err := c.Send(t.Context(), SendRequest{
				Spool: integrationSpool,
				Queue: queue,
				Dedup: test.first,
				Data:  []byte("hello"),
			})
			if test.expErr != nil {
				if !errors.Is(err, test.expErr) {
					t.Fatalf(
						"first send: %v; want %v",
						err, test.expErr,
					)
				}
				return
			}
			if err != nil {
				t.Fatalf(
					"first send: %v",
					err,
				)
			}
			second, err := c.Send(t.Context(), SendRequest{
				Spool: integrationSpool,
				Queue: queue,
				Dedup: test.second,
				Data:  []byte("hello"),
			})
			if err != nil {
				t.Fatalf(
					"second send: %v",
					err,
				)
			}
			if act, exp := second.Duplicate, test.expDup; act != exp {
				t.Errorf(
					"duplicate: %t; want %t",
					act, exp,
				)
			}
			if act, exp := second.ID == first.ID, test.expDup; act != exp {
				t.Errorf(
					"same id: %t; want %t",
					act, exp,
				)
			}
		})
	}
}

func TestIntegrationDelay(t *testing.T) {
	t.Parallel()
	c := integrationClient(t)
	queue := integrationQueue(t, c, QueueSettingsCreate{})

	sent := send(t, c, queue, SendRequest{
		DelaySeconds: 1,
		Data:         []byte("later"),
	})
	assertEmpty(t, c, queue, "before the delay")
	page, err := c.QueueMessages(t.Context(), QueueMessagesRequest{
		Spool: integrationSpool,
		Queue: queue,
		State: MessageStateDelayed,
	})
	if err != nil {
		t.Fatalf(
			"list: %v",
			err,
		)
	}
	if act, exp := len(page.Items), 1; act != exp {
		t.Fatalf(
			"delayed messages: %d; want %d",
			act, exp,
		)
	}
	if act := page.Items[0].VisibleAt; act.IsZero() {
		t.Errorf("visible at: zero; want a time")
	}

	msg := mustRecvWait(t, c, queue, 5)
	if act, exp := msg.ID, sent.ID; act != exp {
		t.Errorf(
			"after the delay: message id: %s; want %s",
			act, exp,
		)
	}
}

func TestIntegrationAckAndSend(t *testing.T) {
	t.Parallel()
	c := integrationClient(t)
	in := integrationQueue(t, c, QueueSettingsCreate{})
	out := integrationQueue(t, c, QueueSettingsCreate{
		DedupWindowSeconds: new(60),
	})

	send(t, c, in, SendRequest{
		Data: []byte("in"),
	})
	msg := mustRecv(t, c, in)

	res, err := c.AckAndSend(t.Context(), AckAndSendRequest{
		Spool: integrationSpool,
		Lease: msg.Lease,
		Queue: out,
		Dedup: DedupString("out-1"),
		Data:  []byte("out"),
	})
	if err != nil {
		t.Fatalf(
			"ack and send: %v",
			err,
		)
	}
	if res.ID == "" {
		t.Errorf("want a message id; got nothing")
	}

	assertEmpty(t, c, in, "input after ack and send")
	forwarded := mustRecv(t, c, out)
	if act, exp := forwarded.ID, res.ID; act != exp {
		t.Errorf(
			"output message id: %s; want %s",
			act, exp,
		)
	}
	if act, exp := string(forwarded.Data), "out"; act != exp {
		t.Errorf(
			"output payload: %q; want %q",
			act, exp,
		)
	}

	// A committed call retried with the same lease appends nothing.
	_, err = c.AckAndSend(t.Context(), AckAndSendRequest{
		Spool: integrationSpool,
		Lease: msg.Lease,
		Queue: out,
		Dedup: DedupString("out-2"),
		Data:  []byte("out"),
	})
	if !errors.Is(err, ErrStaleLease) {
		t.Errorf(
			"retry with the settled lease: %v; want %v",
			err, ErrStaleLease,
		)
	}
}

// TestIntegrationFailed walks a message into the failed state and back out,
// through the listing's handles.
func TestIntegrationFailed(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		act     func(*Client, context.Context, string, Handle) error
		expLeft int // Messages in the queue afterwards.
	}{
		{
			name: "recover returns it with its retries reset",
			act: func(c *Client, ctx context.Context, spool string, h Handle) error {
				return c.Recover(ctx, RecoverRequest{Spool: spool, Handle: h})
			},
			expLeft: 1,
		},
		{
			name: "discard drops it",
			act: func(c *Client, ctx context.Context, spool string, h Handle) error {
				return c.Discard(ctx, DiscardRequest{Spool: spool, Handle: h})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := integrationClient(t)
			queue := integrationQueue(t, c, QueueSettingsCreate{
				MaxRetries: new(0),
			})

			sent := send(t, c, queue, SendRequest{
				Data: []byte("poison"),
			})
			msg := mustRecv(t, c, queue)
			// With no retries allowed, a nack fails the message.
			err := c.Nack(t.Context(), NackRequest{
				Spool: integrationSpool,
				Lease: msg.Lease,
			})
			if err != nil {
				t.Fatalf(
					"nack: %v",
					err,
				)
			}
			assertEmpty(t, c, queue, "after the last retry")

			page, err := c.QueueMessages(t.Context(), QueueMessagesRequest{
				Spool:        integrationSpool,
				Queue:        queue,
				State:        MessageStateFailed,
				ReturnHandle: true,
			})
			if err != nil {
				t.Fatalf(
					"list: %v",
					err,
				)
			}
			if act, exp := len(page.Items), 1; act != exp {
				t.Fatalf(
					"failed messages: %d; want %d",
					act, exp,
				)
			}
			failed := page.Items[0]
			if act, exp := failed.ID, sent.ID; act != exp {
				t.Errorf(
					"failed message id: %s; want %s",
					act, exp,
				)
			}
			if failed.Handle == "" {
				t.Fatalf("want a handle; got nothing")
			}

			// A peek reads it where it is and leaves the handle valid.
			peeked, err := c.Peek(t.Context(), PeekRequest{
				Spool:  integrationSpool,
				Handle: failed.Handle,
			})
			if err != nil {
				t.Fatalf(
					"peek: %v",
					err,
				)
			}
			if diff := cmp.Diff(PeekResult{
				ID:      sent.ID,
				State:   MessageStateFailed,
				Retries: 0,
				Data:    []byte("poison"),
			}, peeked); diff != "" {
				t.Errorf(
					"peek mismatch (-want +act):\n%s",
					diff,
				)
			}

			err = test.act(c, t.Context(), integrationSpool, failed.Handle)
			if err != nil {
				t.Fatalf(
					"act on the failed message: %v",
					err,
				)
			}
			// The handle went stale with the move, for a peek as for a discard.
			_, err = c.Peek(t.Context(), PeekRequest{
				Spool:  integrationSpool,
				Handle: failed.Handle,
			})
			if !errors.Is(err, ErrStaleLease) {
				t.Errorf(
					"peek with the used handle: %v; want %v",
					err, ErrStaleLease,
				)
			}
			err = c.Discard(t.Context(), DiscardRequest{
				Spool:  integrationSpool,
				Handle: failed.Handle,
			})
			if !errors.Is(err, ErrStaleLease) {
				t.Errorf(
					"discard with the used handle: %v; want %v",
					err, ErrStaleLease,
				)
			}

			page, err = c.QueueMessages(t.Context(), QueueMessagesRequest{
				Spool: integrationSpool,
				Queue: queue,
			})
			if err != nil {
				t.Fatalf(
					"list: %v",
					err,
				)
			}
			if act, exp := len(page.Items), test.expLeft; act != exp {
				t.Fatalf(
					"messages left: %d; want %d",
					act, exp,
				)
			}
			if test.expLeft > 0 {
				left := page.Items[0]
				if act, exp := left.State, MessageStateVisible; act != exp {
					t.Errorf(
						"state: %v; want %v",
						act, exp,
					)
				}
				if act, exp := left.Retries, 0; act != exp {
					t.Errorf(
						"retries: %d; want %d",
						act, exp,
					)
				}
			}
		})
	}
}

func TestIntegrationLeaseExpiry(t *testing.T) {
	t.Parallel()
	c := integrationClient(t)
	queue := integrationQueue(t, c, QueueSettingsCreate{
		LeaseTimeoutSeconds: new(1),
	})

	sent := send(t, c, queue, SendRequest{
		Data: []byte("slow"),
	})
	msg := mustRecv(t, c, queue)
	expires, ok := msg.LeaseMeta.Expires()
	if !ok {
		t.Fatalf("want an expiry; got none")
	}

	// Renewing keeps the lease: the expiry moves and nothing is redelivered.
	renewed, err := c.Renew(t.Context(), RenewRequest{
		Spool: integrationSpool,
		Lease: msg.Lease,
	})
	if err != nil {
		t.Fatalf(
			"renew: %v",
			err,
		)
	}
	if again, ok := renewed.LeaseMeta.Expires(); !ok || again.Before(expires) {
		t.Errorf(
			"renewed expiry: %v; want at or after %v",
			again, expires,
		)
	}

	// Left alone, the lease times out and the message comes back as a
	// retry; the old lease then settles nothing.
	redelivered := mustRecvWait(t, c, queue, 5)
	if act, exp := redelivered.ID, sent.ID; act != exp {
		t.Fatalf(
			"redelivered message id: %s; want %s",
			act, exp,
		)
	}
	if act, exp := redelivered.Retries, 1; act != exp {
		t.Errorf(
			"retry count: %d; want %d",
			act, exp,
		)
	}
	err = c.Ack(t.Context(), AckRequest{
		Spool: integrationSpool,
		Lease: msg.Lease,
	})
	if !errors.Is(err, ErrStaleLease) {
		t.Errorf(
			"ack with the expired lease: %v; want %v",
			err, ErrStaleLease,
		)
	}
}

func TestIntegrationQueues(t *testing.T) {
	t.Parallel()
	c := integrationClient(t)
	queue := integrationQueue(t, c, QueueSettingsCreate{
		MaxRetries: new(3),
	})

	got, err := c.Queue(t.Context(), QueueRequest{
		Spool: integrationSpool,
		Queue: queue,
	})
	if err != nil {
		t.Fatalf(
			"describe: %v",
			err,
		)
	}
	if act, exp := got.Name, queue; act != exp {
		t.Errorf(
			"name: %s; want %s",
			act, exp,
		)
	}
	if act, exp := got.State, QueueStateActive; act != exp {
		t.Errorf(
			"state: %v; want %v",
			act, exp,
		)
	}
	if act, exp := got.Settings.MaxRetries, 3; act != exp {
		t.Errorf(
			"max retries: %d; want %d",
			act, exp,
		)
	}

	err = c.UpdateQueue(t.Context(), UpdateQueueRequest{
		Spool: integrationSpool,
		Queue: queue,
		Settings: QueueSettingsUpdate{
			MaxRetries: new(7),
		},
	})
	if err != nil {
		t.Fatalf(
			"update: %v",
			err,
		)
	}
	got, err = c.Queue(t.Context(), QueueRequest{
		Spool: integrationSpool,
		Queue: queue,
	})
	if err != nil {
		t.Fatalf(
			"describe after update: %v",
			err,
		)
	}
	if act, exp := got.Settings.MaxRetries, 7; act != exp {
		t.Errorf(
			"max retries after update: %d; want %d",
			act, exp,
		)
	}

	err = c.CreateQueue(t.Context(), CreateQueueRequest{
		Spool: integrationSpool,
		Queue: queue,
	})
	if !errors.Is(err, ErrQueueExists) {
		t.Errorf(
			"create again: %v; want %v",
			err, ErrQueueExists,
		)
	}

	send(t, c, queue, SendRequest{
		Data: []byte("counted"),
	})
	stats, err := c.SpoolStats(t.Context(), SpoolStatsRequest{
		Spool:  integrationSpool,
		Queues: []string{queue},
	})
	if err != nil {
		t.Fatalf(
			"stats: %v",
			err,
		)
	}
	if act, exp := len(stats.Queues.Items), 1; act != exp {
		t.Fatalf(
			"queues in stats: %d; want %d",
			act, exp,
		)
	}
	if act, exp := stats.Queues.Items[0].Stats.Visible, int64(1); act != exp {
		t.Errorf(
			"visible: %d; want %d",
			act, exp,
		)
	}

	_, err = c.Queue(t.Context(), QueueRequest{
		Spool: integrationSpool,
		Queue: "no-such-queue",
	})
	if !errors.Is(err, ErrQueueNotFound) {
		t.Errorf(
			"describe a missing queue: %v; want %v",
			err, ErrQueueNotFound,
		)
	}
	_, err = c.Queue(t.Context(), QueueRequest{
		Spool: "no-such-spool",
		Queue: queue,
	})
	if !errors.Is(err, ErrSpoolNotFound) {
		t.Errorf(
			"describe in a missing spool: %v; want %v",
			err, ErrSpoolNotFound,
		)
	}
}
