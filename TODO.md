# TODO

Items marked *discuss* need a decision first.

## Iterators

Top-level functions over a `*Client`, built on the single-request primitives,
which stay as they are. Each returns an `iter.Seq2` so that the caller ranges
over it and stops when the context ends or the loop breaks; an error ends the
sequence as its last element, never swallowed. Go 1.23's range-over-func is the
idiom; the primitives remain the bounded calls, as `net.Conn.Read` is under
`io.ReadFull`.

- [ ] `spooler.Consume(ctx, c, req RecvRequest) iter.Seq2[RecvResult, error]`:
  re-polls on an empty receive and yields on a message or an error. The
  blocking, channel-like consumer loop; `Recv` keeps its comma-ok. *discuss*:
  what `WaitSeconds` of zero means here (spin, or refuse), and whether
  `OnRequest` fires per poll (it should: one span per request, as today).

- [ ] `spooler.SpoolQueues(ctx, c, req SpoolQueuesRequest) iter.Seq2[Queue,
  error]`: follows `Next` into `After` until the listing ends, yielding one
  queue at a time.

- [ ] `spooler.SpoolMessages(ctx, c, req QueueMessagesRequest)
  iter.Seq2[Message, error]`: the same over a queue's messages.

- [ ] Names, decided 2026-09-22: `Consume` for the consumer, the message-queue
  word (RabbitMQ, NATS); plural nouns for the listings, the standard library's
  word for what is ranged over (`Lines`, `Fields`, `Keys`), without the scope
  prefix since the request names the scope.

- [ ] Decide the shape once for all three: the page functions take the request
  as the first page's and overwrite `After`; the request's `Limit` is the page
  size, not a cap on the sequence. *discuss*: a `SpoolStats` iterator too, or
  leave it, since its rollup is per call.

- [ ] Examples: a consumer loop with `Consume`, and a listing with `Messages`
  that acts on a handle.

## Retrying an unknown result (decided 2026-10-05)

Adopted: `RetryUnknown bool` on `SendRequest` and `AckAndSendRequest`. The SDK
retries an unknown result only when the request asks, and promises nothing
about safety: a send may append twice (a new message with its own id); an
ack-and-send never does. Neither
the README nor the examples teach a retry loop: any loop leaning on the dedup
window carries a tail risk (a retry reaching the server after the window) the
SDK cannot remove. The plain settles have no field: an unknown settle resolves
through redelivery or lease expiry anyway; add one if users ask. Weighed and
dropped, so that it is not argued afresh:

- A helper (`SendWithRetry`, not `TrySend`: the standard library's `Try*`
  is one non-blocking attempt). Re-implements from outside the loop the
  transport runs, and cannot replay a stream payload.
- A request field, `SendRequest.RetryUnknownFor time.Duration`, built and
  removed: the time could not be stated as a guarantee. A retry can reach
  the server arbitrarily late, so the best contract was a tolerance (a retry
  later than window minus time can append again), which the SDK would co-sign.
- A request field `RetryUnknown func() bool`: the caller decides each retry,
  which left the SDK only payload replay and backoff for a public contract.
- An option on `Client`, or `Send(ctx, req, opts...)`: the window is per
  queue and per key history, not per client; options add a second way to pass
  parameters for one behaviour.

Kept from the work: `ErrResultUnknown` on any 5xx but 503 and 507 and on a
response lost after it may have been sent; `httputil.AttemptErrors`; the early
end of a wait the context's deadline would not outlive.
