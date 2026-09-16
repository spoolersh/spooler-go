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
