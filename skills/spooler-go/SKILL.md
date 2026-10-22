---
name: spooler-go
description: Write Go code against the Spooler message queue with the official SDK, module github.com/spoolersh/spooler-go (package spooler). Covers spooler.Client setup, the send, receive, ack loop, the settles, renew, errors.Is sentinels and ErrResultUnknown, dedup keys, delayed sends and handles, failed messages (list, inspect, recover, discard) with the iterators, queue settings, streaming payloads and tests against the spoolersh/memspoold image. Use when Go code imports spooler-go, mentions spooler.Client, Lease, Handle or SendRequest, or a Go service needs a message queue on Spooler.
metadata:
  author: spoolersh
  version: v0.2.0
  docs: https://pkg.go.dev/github.com/spoolersh/spooler-go
---

# Spooler Go SDK

Module `github.com/spoolersh/spooler-go`, package `spooler`, Apache 2.0.

Full API: https://pkg.go.dev/github.com/spoolersh/spooler-go.
The delivery contract is https://docs.spooler.sh/delivery.

The `spooler` skill explains it in full. Never guess server behaviour from
memory.

```sh
go get github.com/spoolersh/spooler-go
```

## Rules

- A receive leases; settle every delivery with Ack, Nack, Release, Fail,
  Discard or AckAndSend, or the lease times out and the message is redelivered.
  Handlers must be idempotent: delivery is at least once.
- Nack is the error path (counts a retry; past the queue's max the message is
  failed). Release is for "not the message's fault" and counts nothing. Fail
  fails the message at once, for one the consumer can tell is poison; it stays
  intact for the listing, inspect, recover and discard.
- `Lease` and `Handle` are opaque credentials; never build or compare them.
  Message ids are for logs; no call takes one.
- The SDK adds no guarantee and hides none: no automatic ack, renew, batching
  or reordering, and no retry of a result it cannot prove safe.
- The zero value of a request field means the server's default; the SDK sends
  nothing for it.

## Client

```go
c := spooler.Client{
	APIKey: os.Getenv("SPOOLER_API_KEY"), // required
	// Host defaults to "api.spooler.sh". For the local image set:
	//
	//	Host:               "127.0.0.1:8080",
	//	InsecureDisableTLS: true,
}
```

- Share one `Client`; it is safe for concurrent use and owns a connection pool.
  Never copy it by value; `c.Clone()` makes a second one.
- `Attempts` (default `DefaultAttempts`, currently 5) bounds retries of what
  the docs' errors table marks transient and changed nothing, and the set
  depends on the operation: every operation retries `ErrRateLimited` (after
  `Retry-After`) and `ErrUnavailable`; a send or ack-and-send also retries
  `ErrDedupInFlight`; every other operation also retries `ErrQueueCreating`
  and `ErrQueueBusy`. Set `Attempts: 1` when the application has its own retry
  policy.
- Put a deadline on the context; it bounds every request.
- Every request type names its `Spool`. There is no client-level default.
- `DebugWriter: os.Stderr` dumps requests and responses with tokens redacted.
- `Trace: spooler.LoggerClientTrace(logger)` logs per-operation hooks.

## The loop

```go
_, err := c.CreateQueue(ctx, spooler.CreateQueueRequest{
	Spool: "default",
	Queue: "jobs",
})
if err != nil && !errors.Is(err, spooler.ErrQueueExists) {
	return err
}

res, err := c.Send(ctx, spooler.SendRequest{
	Spool: "default",
	Queue: "jobs",
	Data:  payload,
})
if err != nil {
	return err
}
log.Println("sent", res.ID) // res.Duplicate reports a dedup hit.

msg, ok, err := c.Recv(ctx, spooler.RecvRequest{
	Spool:       "default",
	Queue:       "jobs",
	WaitSeconds: new(20), // nil = server default (the maximum); 0 = no wait.
})
if err != nil {
	return err
}
if !ok {
	return nil // Nothing became visible within the wait; call again.
}
if err := handle(msg.Data); err != nil {
	return c.Nack(ctx, spooler.NackRequest{
		Spool: "default",
		Lease: msg.Lease,
	})
}
return c.Ack(ctx, spooler.AckRequest{
	Spool: "default",
	Lease: msg.Lease,
})
```

A worker loop is `for { Recv; if !ok { continue }; work; settle }`. Run several
goroutines, each calling `Recv` on the shared client, for concurrency. There is
no callback or channel consumer in the SDK.

## Settles

| Call | Request | Effect |
| --- | --- | --- |
| `Ack` | `AckRequest{Spool, Lease}` | removes the message |
| `Nack` | `NackRequest{Spool, Lease}` | redeliver; counts a retry; past `MaxRetries` the message is failed |
| `Release` | `ReleaseRequest{Spool, Lease, DelaySeconds}` | redeliver without a retry; optional backoff |
| `Fail` | `FailRequest{Spool, Lease}` | failed now, kept intact for the listing, inspect, recover and discard |
| `Discard` | `DiscardRequest{Spool, Lease}` or `{Spool, Handle}` | gone, counted as discarded |
| `AckAndSend` | `AckAndSendRequest{Spool, Lease, Queue, Data, ...}` | ack + append atomically; returns `AckAndSendResult{ID, Handle}` |
| `Renew` | `RenewRequest{Spool, Lease}` | not a settle; resets expiry; returns `RenewResult{LeaseExpiresAt}` |

`Lease` and `Handle` are distinct string types; presenting one where the other
goes does not compile. Both print redacted.

Renew on a ticker while work runs, only when `msg.LeaseExpiresAt` is not zero
(zero means the queue has no lease timeout). Stop on `errors.Is(err,
spooler.ErrStaleToken)`: the message was discarded or the lease is gone, so the
work should stop too.

## Errors

Every API answer with a kind is a `*spooler.Error`. Match the kind with
`errors.Is` against a sentinel; read details with `errors.As`:

```go
switch {
case errors.Is(err, spooler.ErrResultUnknown):
	// The change may have applied; see below. Check it first.
case errors.Is(err, spooler.ErrQueueNotFound):
	var d *spooler.QueueNotFoundError
	if errors.As(err, &d) {
		log.Println("no such queue:", d.Queue)
	}
case errors.Is(err, spooler.ErrRateLimited):
	// The SDK already waited Retry-After and retried, Attempts times.
	var d *spooler.RateLimitedError
	if errors.As(err, &d) {
		time.Sleep(d.RetryAfter)
	}
case errors.Is(err, spooler.ErrSpoolFull):
	// Backpressure: consume and ack, or delete queues, then retry.
case errors.Is(err, spooler.ErrUnavailable):
	// 503 with no kind; the SDK already retried Attempts times.
case errors.Is(err, spooler.ErrInvalidRequest):
	// Refused before sending: a missing field, or both of an either/or set.
}
```

Sentinels, one per kind: `ErrInvalidToken`, `ErrDedupDisabled`,
`ErrUnknownHeader`, `ErrRetentionLimit`, `ErrQueueLimit`, `ErrQueueNotFound`,
`ErrQueueReplaced`, `ErrSpoolNotFound`, `ErrQueueExists`, `ErrQueueCreating`,
`ErrQueueDeleting`, `ErrQueueBusy`, `ErrNotFailed`, `ErrDedupInFlight`,
`ErrDedupClaimed`, `ErrStaleToken`, `ErrPayloadTooLarge`, `ErrRateLimited`,
`ErrOperationUnconfirmed`, `ErrSpoolFull`, `ErrUnauthenticated`,
`ErrAccountSuspended`, `ErrAccountBlocked`. Details types:
`QueueNotFoundError{Queue}`, `QueueLimitError{Max}`, `RetentionLimitError{Max
time.Duration}`, `RateLimitedError{RetryAfter}`, `DedupClaimedError{ID}`,
`QueueReplacedError{UID}`, `SpoolFullError{Limit, Max}`,
`UnknownHeaderError{Header}`.

Errors carry no HTTP status. Act on the kind, *NEVER* on the text.

### Unknown results

`ErrResultUnknown` matches when any attempt may have reached the server and got
no definite answer (a `500`, `502`, `504`, a dropped connection). The SDK does
not retry those unless the request sets `RetryUnknown`:

- `SendRequest{RetryUnknown: true}` may append the message twice (a second id).
  With `Dedup` set, a retry appends nothing new while the queue's dedup window
  still remembers the key and the window was not changed in between; see
  https://docs.spooler.sh/dedup.
- `AckAndSendRequest{RetryUnknown: true}` never appends twice: a retry of a
  committed call fails with `ErrStaleToken` and appends nothing. That error
  also matches `ErrResultUnknown`, since it does not prove the first attempt
  succeeded.

Without `RetryUnknown`, the caller decides. Check `ErrResultUnknown` before
`ErrUnavailable` and `ErrStaleToken`: a later attempt's answer does not clear
an earlier unknown.

## Dedup

```go
res, err := c.Send(ctx, spooler.SendRequest{
	Spool: "default",
	Queue: "orders",
	Dedup: spooler.DedupString("order-42-created"), // or DedupHash(str).
	Data:  data,
})
if res.Duplicate {
	// nothing appended; res.ID is the original's.
}
```

The queue needs `DedupWindowSeconds > 0`, else `ErrDedupDisabled`. Assign the
key before the first attempt and reuse it on retries. The SDK never validates
the key; the server does.

## Delays, handles, cancellation

```go
res, err := c.Send(ctx, spooler.SendRequest{
	Spool:        "default",
	Queue:        "reminders",
	DelaySeconds: 3600, // at most a week
	ReturnHandle: true,
	Data:         data,
})
// Later, if the reminder is no longer needed:
err = c.Discard(ctx, spooler.DiscardRequest{
	Spool:  "default",
	Handle: res.Handle,
})
// ErrStaleToken: a consumer already received it.
```

## Failed messages

```go
seq := spooler.Messages(ctx, &c, spooler.MessagesRequest{
	Spool:        "default",
	Queue:        "orders",
	State:        spooler.MessageStateFailed,
	ReturnHandle: true,
})
for m, err := range seq {
	if err != nil {
		return err
	}
	in, err := c.Inspect(ctx, spooler.InspectRequest{
		Spool:  "default",
		Handle: m.Handle,
	})
	if err != nil {
		return err
	}
	if worthRetrying(in.Data) {
		// Back to the queue with its retries reset; DelaySeconds postpones it.
		err = c.Recover(ctx, spooler.RecoverRequest{
			Spool:  "default",
			Handle: m.Handle,
		})
	} else {
		err = c.Discard(ctx, spooler.DiscardRequest{
			Spool:  "default",
			Handle: m.Handle,
		})
	}
	if errors.Is(err, spooler.ErrStaleToken) {
		continue // The message moved on since the listing; list again later.
	}
	if err != nil {
		return err
	}
}
```

`spooler.Messages` and `spooler.Queues` are `iter.Seq2` walkers over every
page; `c.Messages` and `c.Queues` return one `Page[T]{Items, Next}`, and `Next`
goes back as `After`.

## Queues

```go
q, err := c.CreateQueue(ctx, spooler.CreateQueueRequest{
	Spool: "default",
	Queue: "orders",
	Settings: spooler.QueueSettingsCreate{
		LeaseTimeoutSeconds: new(120),
		MaxRetries:          new(5),
		DedupWindowSeconds:  new(60),
		RetentionSeconds:    new(86400),
		RecvRateLimit: &spooler.RateLimit{
			IntervalMicros: 10000,
			Burst:          100,
		},
	},
})

// q.UID names this queue even if the name is recreated later.

err = c.UpdateQueue(ctx, spooler.UpdateQueueRequest{
	Spool:    "default",
	Queue:    "orders",
	QueueUID: &q.UID, // optional precondition; ErrQueueReplaced if another queue has the name.
	Settings: spooler.QueueSettingsUpdate{
		MaxRetries: new(3),
	},
})

err = c.DeleteQueue(ctx, spooler.DeleteQueueRequest{
	Spool:    "default",
	Queue:    "orders",
	QueueUID: &q.UID,
	Mode:     spooler.QueueDeleteModeDrain, // or QueueDeleteModeForce; required.
})
```

Settings fields are pointers: nil means the server default on create and "keep"
on update. A `RecvRateLimit` pointing at the zero `RateLimit` removes the limit
on update. `c.Queue` and `c.Spool` read a queue and the spool with their
`MessageStats`.

## Streaming payloads

`SendRequest.DataSource` takes `spooler.DataSourceReadSeeker(rs)` (can be
retried) or `spooler.DataSourceReader(r)` (sent once, never retried).
`RecvRequest{Stream: true}` and `InspectRequest{Stream: true}` return the
payload as `DataStream io.ReadCloser`; close it as soon as it is read. A
receive whose payload read fails still returns the lease with `ok` true and the
error: the lease is the caller's to settle.

## Testing against the local image

```sh
docker run --rm -p 127.0.0.1:8080:8080 spoolersh/memspoold
SPOOLER_HOST=localhost:8080 SPOOLER_API_KEY=local go test ./...
```

The image is free for development and testing. It accepts any key, has one
spool `default`, persists nothing and never answers `402`, `429` or `503`;
cover those in unit tests against a fake, not against the image.
