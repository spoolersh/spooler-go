# Repository Guidelines

## What this is

`github.com/spoolersh/spooler-go`, package `spooler`: the Go SDK for Spooler, a
hosted message queue with a small API surface and simplicity as its core idea.

The SDK is the thing the tagline is reserved for ("message queues as simple as
Go channels"); the HTTP API is what it speaks, and it is the first transport,
not the only one ever.

Priorities: correctness > ergonomics > features. This is a thin, complete
client of the shipped API; do not invent server behaviour, batching, or retries
the server does not define.

## Where truth lives

- The public docs at https://docs.spooler.sh are the contract: `api` is the
  request shape, `delivery` the guarantees, `dedup` safe retries, `errors` the
  error table, `limits` the bounds, `local` the evaluation image. Prose in
  this repo says what those pages say, in fewer words, and links to them.
- When the docs and the evaluation server disagree, or the docs are silent,
  do not guess: ask the project owner. Never write down behaviour that is not
  documented and observable.
- The evaluation server is the `spoolersh/memspoold` Docker image: the same API
  on `http://localhost:8080/v1`, any bearer value authenticates, nothing
  persists, one spool named `default`. It never answers `401` for a bad key,
  `402`, `429`, or `503`, and never drops a lease by failover; tests that cover
  those run against a fake.

## This repo is public

Everything here documents the contract a client can rely on and nothing about
how the service is built or run. Never let in: hostnames other than the public
API, internal auth, topology, replica or failover mechanics, storage details,
limits typed from memory, pricing, roadmap, or anything from the private repos'
source. Every statement must be checkable with an API key and `curl`.

## The delivery contract the SDK must preserve

- **At least once.** A delivery may repeat; the SDK never hides that and never
  dedups on the consumer's behalf.
- **A receive is a lease, not a removal.** While the lease holds no other
  receiver gets the message. Settle one of five ways: ack removes, nack returns
  it and counts a retry, release returns it without a retry, fail parks it in
  the failure queue now, renew resets the expiry to now plus the queue's
  current lease timeout (a reset, which can shorten). An unsettled lease times
  out and the message is redelivered with the retry count incremented; after
  `maxRetries` it lands in the failure queue.
- **The lease token is opaque.** Never inspect, compare, parse, or construct
  one; the only use is presenting it back. Redact it in logs and traces.
- **A primary change drops in-flight leases.** Their messages are redelivered
  without incrementing the retry count, and a settle with the old lease answers
  410 `stale_lease`. The SDK surfaces this as an error the consumer expects; it
  is not a bug to retry around.
- **Not FIFO.** Oldest visible first, approximately, but order is not a
  contract. No API in the SDK may imply ordering.
- **Ack-and-send** is one atomic call: the input acked and the output appended,
  or neither. A dedup hit on it refuses the whole call with 409 `dedup_claimed`
  and acks nothing.
- **Retries.** Settles and reads are idempotent in effect and may be retried. A
  send is retried only when it carries a dedup key: a dropped connection or 500
  `operation_unconfirmed` means the append may have happened, and a blind retry
  duplicates the message. The SDK must never retry an unkeyed send on its own,
  and its docs say why.
- **429 and 503 are retry-after conditions**, honouring `Retry-After` when
  present. 401, 402, 403, 404 `queue_not_found`, 413 and 507 are verdicts.

## Shape of the SDK (decided upstream, do not relitigate)

- Verbs mirror the operations and are always on the Client struct, i.e.,
  Client.Send().
- `context.Context` first on every call; a long-poll receive is a single
  request bounded by `wait`.
- Every method gets a struct as a parameter if it accepts more than one after
  context: i.e., `SendRequest`.
- Data-related operations should support both `[]byte{}` and streaming
  versions.
- Dedup: the SDK sends `Spooler-Dedup-String` and lets the server hash it.
  `Spooler-Dedup-Hash` exists for callers who computed the fingerprint
  themselves; the recipe is public and frozen, `sha256(key)[:16]`, if a helper
  is ever offered.
- A send handle (`?handle=true`) is opt-in and maps to caller intent (cancel
  before first delivery via discard); it is never requested by default, since
  it is a bearer credential.
- Payloads are `[]byte` or an `io.Reader` with a known length; the SDK
  neither encodes nor frames them.
- Errors: one error type carrying status, kind, message, the optional message
  id, and `Retry-After`; kinds addressable with `errors.Is` through sentinel
  values. Callers must be able to distinguish verdicts from retryable
  conditions without string matching.
- No global state, no logging library, no dependency beyond the standard
  library unless it is `golang.org/x/*` and earns its place.

## Tracing

Observability is the github.com/gobwas/gtrace based, not logging: a
`ClientTrace` struct of `On*` callback fields, a
`LoggerClientTrace(*slog.Logger)` constructor that sets only its own struct's
fields, and generated `*_gtrace.go` files. **The `*_gtrace.go` files are
maintained by the project owner: never read, edit, or generate them.** Hook
conventions: verb-named hooks that carry the error (`OnSend(..., err error)`),
the hook deferred at the top of the function it reports, typed codes not
strings, `.String()` only at log sites, secrets (keys, leases, handles) never
passed to a hook.

## Coding style

Standard Go, `gofmt`, tabs, mixedCaps. Short lowercase file names. In
addition:

- Comments are concise, not memoirs: say what a thing is or does in one line; a
  non-obvious *why* only when the code cannot. One field, one doc comment.
  Plain words, no coined vocabulary; use the spec's names (lease, spool, queue,
  handle, settle).
- Struct literals one field per line, even for one field. `fmt.Errorf` and
  `t.Errorf` broken so the format string and each argument sit on their own
  line.
- Assign a multi-line call on its own line, then `if err != nil`; never inline
  it.
- Method case signals locking: an exported-style name synchronises itself, a
  lowercase one is a lock-held helper.
- Validated types over strings where the API constrains a value (a queue name,
  a dedup key); constructors return errors that are always propagated, never
  ignored.
- Hot paths (send, receive, settle) allocate only for the request and its
  payload; check with `go build -gcflags=-m=1 . 2>&1 | grep escapes` when
  restructuring them.

## Testing

- Table-driven tests with the `testing` package; test names are lowercase
  phrases without underscores. Case structs hold scenario data only; mocks and
  fake servers are assembled in the `t.Run` body from that data.
- Assertions: actual first, then want, never "got": `t.Errorf("status: %d; want
  %d", act, exp)`. Specific errors compare with `errors.Is` against an `expErr
  error` field; presence-only with an `err bool` field and the two-branch idiom
  ("want error; got nothing"). Struct comparison with
  `github.com/google/go-cmp/cmp` and `cmp.Diff(exp, act, opts)`.
- Stubs use the `Do*` function-field pattern with a compile-time interface
  check and a default error naming the method.
- Unit tests run every operation against an `httptest` server that asserts the
  exact request (method, path, headers, body) and returns canned responses,
  including every error kind and the statuses the local image never produces.
- Integration tests run against the local image when `SPOOLER_API` and
  `SPOOLER_KEY` are set and skip otherwise. CI starts `spoolersh/memspoold` for
  them. They exercise the send, receive, settle loop, dedup, delay,
  ack-and-send, the failure queue, and lease expiry.
- `go test ./...` before every hand-off; `go vet ./...` clean.

## Docs and README

Tone is mq_overview(7): terse, complete, factual. Specifics over superlatives.
No "simply", "just", "seamless", "powerful". Honest failure modes are stated
plainly. Never promise unshipped work, never mention pricing, never present the
HTTP API as the differentiator. Examples compile: a README example is a
`Example*` test. The first example is the send, receive, ack loop against the
local image, in under a screen.

## Commits and pull requests

Agents never commit, tag, or push; the working tree is the hand-off.

Whenever asked for summaries, commits follow the Go's standard library
convention "where: imperative what to change", i.e., "all: make all doc
comments complete sentences" -- short imperative commit messages. PRs say what
changed and why in a sentence or two, list material changes, and cite the
observable (an operation, a header, a documented status) for every claim the
SDK newly makes about the API.
