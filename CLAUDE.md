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

- **This file never states what the API does.** No status, kind, header,
  path, parameter, state, limit, guarantee or retry rule is written here, not
  even as a summary, and none is ever moved here. The docs define those, they
  change, and a copy drifts and then gets reasoned from instead of the
  source. This file holds the SDK's own decisions and says where to look for
  the rest. When a task needs a fact about the API, read it from the docs at
  the time, and cite the page.
- The public docs at https://docs.spooler.sh are the contract: `api` is the
  request shape, `delivery` the guarantees, `dedup` safe retries, `errors` the
  error table and the retry rules, `limits` the bounds, `local` the
  evaluation image. The README and doc comments may say what those pages
  say, in fewer words, with a link.
- When the docs and the evaluation server disagree, or the docs are silent,
  do not guess: ask the project owner. Never write down behaviour that is not
  documented and observable.
- The evaluation server is the `spoolersh/memspoold` Docker image. The docs'
  `local` page says what it reproduces and what it does not; tests for what
  it does not run against a fake.
- memspoold is not open source: it ships under the Spooler Local Evaluation
  License (published at docs.spooler.sh/memspoold-license.txt), which permits
  development, testing, and evaluation, including automated test environments.
  Never describe it as "OSS" or "open source" in this repo; say "free for
  development and testing".

## This repo is public

Everything here documents the contract a client can rely on and nothing about
how the service is built or run. Never let in: hostnames other than the public
API, auth, topology, replica or failover mechanics, storage details, limits
typed from memory, pricing, roadmap, or anything from the private repos'
source. Every statement must be checkable with an API key and `curl`.

## What the SDK owes the delivery contract

The contract itself is the docs' `delivery` and `errors` pages; read them, it
is not repeated here. What follows is only the SDK's side of it.

- **The SDK adds no guarantee and hides none.** It never dedups, reorders,
  batches, acks, renews or settles on the consumer's behalf, and never swallows
  an error the docs tell a consumer to expect.
- **Tokens are opaque to the SDK too.** It never inspects, compares, parses or
  constructs one; the only use is presenting it back. It redacts them in logs
  and traces.
- **No name in the SDK implies an ordering.**
- **The SDK retries exactly what the docs declare transient, and nothing
  else.** That a retry is safe does not make a condition transient: the docs
  call some retries harmless without asking any client to make them. The retry
  functions cite the docs' table they implement, and a change there is made by
  re-reading that table, never from memory or from this file.

## Shape of the SDK (decided upstream, do not relitigate)

- Every operation is a method on Client, named for the verb the API
  documents, with context.Context first.
- A long-poll receive is a single request. The zero value of a request field
  means the API's own default, whatever the docs say that is: the SDK sends
  nothing for it, and never turns an absent parameter into an explicit one.
  Departing from the default is an explicit choice by the caller.
- Two layers, never merged. A Client method is one request and mirrors the
  API exactly: a receive answers empty with comma-ok, a listing returns one
  page with its cursor, and no method loops on the caller's behalf.
  Anything that spans requests, such as a consumer that blocks until a
  message arrives or a walk over every page, is a top-level function over a
  `*Client` returning an `iter.Seq2[T, error]`, built only from the
  methods, ending when the context ends, and yielding an error as its last
  element rather than swallowing it. The methods are `net.Conn.Read`, the
  functions are `io.ReadFull`: a caller who wants the loop gets it, one who
  wants the bound keeps it, and a hook still fires per request.
- The two kinds of token are distinct Go types, `Lease` and `Handle`, so
  presenting one where the other goes does not compile.
- Input is a <Op>Request struct whenever there is more than one argument
  after the context. Since every operation names a spool, that is every
  operation.
- A read returns the schema it names. Queue returns Queue, SpoolStats returns
  SpoolStats with its own Next. Do not wrap a resource merely to give it an
  operation-specific name.
- A <Op>Result groups a composite answer: a body plus a header, or a
  resource plus a credential. Send, Recv, Renew, AckAndSend.
- Keep separate results for Send, SendFrom, Recv and RecvTo, even when a
  result only embeds shared metadata. Common fields live in SendMeta and
  RecvMeta; operation-specific fields belong on the outer results. This
  keeps the (Result, error) shape, lets each method gain fields without
  changing its return signature, and keeps a buffered delivery together for
  handlers and channels. The extra types are a deliberate trade-off. Both
  this shape and separate data/metadata returns have Go precedent; this SDK
  follows the operation-specific result approach used by etcd.
- Return values, not pointers, until a type gains mutating methods.
- Cross-cutting extras go on ClientTrace, not into results.
- The SDK converts a value only where it loses nothing. What the caller
  sends in a unit the API fixes is an integer in that unit, with the unit
  in the field's name (`DelaySeconds`, `WaitSeconds`, `IntervalMicros`),
  as Uber's guide prescribes for external systems and as `http.Cookie.MaxAge`
  does: a `time.Duration` there would force the SDK to round or refuse,
  and neither is its call. What the SDK hands back for use gets the time
  types, since those conversions are exact: a `Retry-After` becomes a
  `time.Duration`, a timestamp a `time.Time`.
- One error type. It carries the kind, the server's message, the retry
  delay as a top-level `time.Duration`, and the kind's details as a typed
  error. Every kind matches with `errors.Is` against a sentinel; a kind with
  details also offers its type through `errors.As`. Which kinds exist and
  what each carries is the docs' errors page, not this file.
- The error never carries a status: the SDK is transport-agnostic, and a
  status is HTTP's. A condition the API reports without a kind gets a kind
  of the SDK's own, mapped inside the transport layer, so callers match a
  sentinel like any other. A kind this SDK does not know is treated as no
  kind, so that mapping still applies and code matching the broader sentinel
  keeps working when the server refines it. What neither names is
  ErrorKindUnknown, and only there does the status appear, as prose in the
  message, for a person to read. Callers never parse the message.
- Text is for people, fields are for programs. An error prints as its kind
  followed by the server's message, which already states the facts;
  `Details` is for programmatic access only, through `errors.As`, and is
  not printed beside the message. As a rule of thumb anywhere a value has
  both forms: never make a caller parse text for what could be a field, and
  never repeat in text what a field next to it already says.
- Data-related operations support both []byte and streaming versions.
- Payloads are []byte or an io.Reader with a known length; the SDK neither
  encodes nor frames them.
- Encryption and compression are not client features, decided 2026-09-22.
  Both are client-side only: the server stores opaque bytes and carries no
  per-message metadata, so a consumer can tell a sealed or compressed
  payload from a raw one only by a marker inside the payload, which is
  framing, and a format other language clients would have to share. If
  either is ever wanted, it is a separate package the caller applies
  explicitly to `[]byte` (`Data: p.Seal(...)`, `p.Open(msg.Data)`), never a
  Client switch: a sealed payload cannot be opened until its tag is read, so
  a receive into a Writer would buffer or hand out unverified bytes, and a
  compressed send cannot know its length until it has finished, so a
  streaming send would buffer. Ciphers are the standard library's
  `cipher.AEAD` with a caller-supplied keyring; codecs are `compress/gzip`
  or `compress/flate`, since no zstd exists in the standard library or
  `golang.org/x`. The envelope, if one is ever defined, is a docs page.
- Dedup: the SDK passes the caller's key through and lets the server hash it.
  A fingerprint the caller computed is a separate constructor; the SDK
  computes none.
- A handle is never requested by default, since it is a bearer credential.
  Asking for one is an explicit request field.
- The transport never leaves the package. HTTP is the first transport, not
  the last, and whatever a user can reach cannot be changed or optimised
  later. So nothing from `net/http`, `net/url` or `internal/httputil` appears
  in the public surface, in any form:
  - no exported signature, struct field or embedded type mentions them, and
    no accessor hands out the client, a request, a response or a header;
  - no configuration takes them (`*http.Client`, `http.RoundTripper`,
    `http.Header`); what a user may tune is expressed in the SDK's own terms
    (`Host`, `InsecureDisableTLS`), and a new knob is a new SDK field. A
    knob that turns off a security property carries the `Insecure` prefix,
    as `tls.Config.InsecureSkipVerify` does;
  - `ClientTrace` hooks receive SDK structs of plain values, never a request
    or a response;
  - errors are translated at the boundary into the SDK's error type. A
    transport error (`*url.Error`, the internal status error) is not
    returned and not reachable through `Unwrap`; only `context` errors stay
    matchable with `errors.Is`;
  - names and docs describe intent, not the wire: a field is not named for
    a query parameter or a header, though its comment may cite one.
  `context`, `io`, `time` and `log/slog` types are fine: they are not the
  transport. Check with `go doc -all . | grep -E 'http\.|url\.|httputil\.'`,
  which must print nothing.
- `internal/httputil` and `internal/backoff` are generic libraries that may
  be extracted from this repo one day. They know HTTP and nothing of
  Spooler: no `Spooler-*` header, kind, error shape, lease or queue, and no
  behaviour narrowed to what this API happens to send (a Retry-After parser
  takes both RFC 9110 forms). Everything
  Spooler-specific is configured from package `spooler`, through hooks such
  as the error factory. The reverse holds too: no Spooler rule is enforced
  by leaning on a quirk of the generic code.
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

- When in doubt about a name, a signature or an API shape, look for precedent
  in this order: the standard library first (grep `GOROOT/src`, do not answer
  from memory), then the Uber and Google Go style guides. Report what was
  found, silence included, before recommending anything.
- Every exported symbol has a doc comment, written the way the standard
  library writes them and as Effective Go's Commentary section prescribes
  (https://go.dev/doc/effective_go#commentary): a complete sentence that
  begins with the name it describes ("Send appends a message to the
  queue."), so it reads well in `go doc` and in a grep. One package comment
  in `doc.go` introduces the package and carries the first example. Doc
  comments follow the current Go doc comment syntax
  (https://go.dev/doc/comment): links as `[Name]`, code as an indented
  block, lists only where a list is what is meant. Check with `go vet` and
  by reading `go doc -all .` as a user would.
- Comments are concise, not memoirs: say what a thing is or does in one line; a
  non-obvious *why* only when the code cannot. One field, one doc comment.
- The SDK never coins vocabulary. Every noun and verb of the domain in a
  name, a doc comment or the README is one the public docs already use for
  the same thing: lease, handle, spool, queue, settle, deliver, append,
  discard, and so on. Before using a word that feels like a term of art,
  grep the docs for it; if the docs say it another way, use their way, and
  if they do not say it at all, describe the thing in plain words rather
  than name it. A reader moves between the docs and `go doc` and must never
  have to translate.
- A request type's doc is "<Name>Request describes <what to act on and
  how>": "SendRequest describes a message to append to a queue",
  "AckRequest describes a delivery to ack", "SpoolQueuesRequest describes
  a page of a spool's queues to list". Always "describes": a request does
  nothing itself, so no verb of the operation ("selects", "names", "acks")
  is its verb. It never restates what the operation does, which is the
  method's doc, and never calls the request the thing it carries ("is a
  message"). A result type's doc says what the result is the outcome of.
- Documentation links (`[Name]`) mark the first useful mention of a symbol in
  each doc comment. Later mentions are plain unless a distant paragraph or
  list benefits materially from another link. Each field comment is its own
  doc comment.
- A request field the caller may leave zero says so in its first sentence,
  as the standard library does ("Cancel is an optional channel",
  "ModifyResponse is an optional function"): "DelaySeconds is an optional
  delay until the message becomes visible", "Writer is an optional
  destination for the payload". A required field says what it names and
  nothing about being required; the request is refused without it.
- A field's zero value is documented as the last line of its comment, on a
  line of its own, as a sentence starting with "Zero": "Zero means the
  server's default.", "Zero keeps every queue.", "Zero sends an empty
  message." The word is always "zero", never "nil", "empty" or "absent",
  whatever the type. Where a pointer field's pointed-to 0 means something
  else, that is said in the body as "a value of 0".
- In a struct declaration, a field that carries a doc comment is separated
  from the field before it by a blank line, so that the comment reads as
  the field's own and not as a trailer of the previous one. Fields without
  comments may sit together.
- Struct literals one field per line, even for one field. `fmt.Errorf` and
  `t.Errorf` broken so the format string and each argument sit on their own
  line.
- Assign a multi-line call on its own line, then `if err != nil`; never inline
  it.
- Method case signals locking: an exported-style name synchronises itself, a
  lowercase one is a lock-held helper.
- Validation: check locally only what the server can never see. The test is
  "could the server have caught this exact mistake?" If yes, let it: its
  answer is authoritative and typed, and a mirrored rule drifts. So no name
  patterns, length or size caps, dedup key charsets, or duration bounds in
  the SDK. What the SDK must check is its own doing:
  - its unions and one-of fields (both or neither set), where it would
    otherwise pick one silently;
  - its URL building: `url.PathEscape` every path segment and reject empty,
    `.` and `..`, rather than copying the server's name pattern;
  - its own conversions: none may be lossy, which is why request fields
    take the wire's unit rather than a type that would need rounding;
  - its own sentinels: the empty lease or handle that means "no message" is
    rejected by every operation that presents a token.
  Such errors are returned before any request is made.
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
- Unit tests run every operation through one generic table runner against a
  stub transport, inside a `testing/synctest` bubble so backoff and retry
  delays take no real time. A case lists the exchanges it expects, each an
  exact request (method, path, query, headers, body) and its response or
  transport failure; a request the case does not list fails it, and so does
  a listed one never made, so a retry is one more row. Cases cover every
  error kind the docs list and what the local image cannot produce. Expected
  values come from the docs, never from what the code currently does.
- One real-socket test per package that owns a transport proves what a stub
  cannot see, such as the request's length reaching the wire.
- Integration tests run against the local image when `SPOOLER_API` and
  `SPOOLER_KEY` are set and skip otherwise. CI starts `spoolersh/memspoold` for
  them. They exercise the send, receive, settle loop, dedup, delay,
  ack-and-send, failed messages (list, peek, recover, discard), and lease
  expiry.
- `go test ./...` before every hand-off; `go vet ./...` clean.

## Docs and README

Tone is mq_overview(7): terse, complete, factual. Specifics over superlatives.
No "simply", "just", "seamless", "powerful". Honest failure modes are stated
plainly. Never promise unshipped work, never mention pricing, never present the
HTTP API as the differentiator. Examples compile: a README example is a
`Example*` test. The first example is the send, receive, ack loop against the
local image, in under a screen.

## Editing files

Agents change project files only with the edit tool, one visible edit at a
time. Never with a script: no `sed -i`, no awk, python or perl rewrite, no
heredoc or redirect into a source file, however repetitive the change. The
owner edits the same files concurrently and reviews changes as discrete
edits; a script rewrites a file opaquely and can clobber work in progress.
The only exception is a one-off the owner has blessed beforehand. Scripts
that only read (grep, awk, a python that prints) are fine.

## Commits and pull requests

Agents never commit, tag, or push; the working tree is the hand-off.

Whenever asked for summaries, commits follow the Go's standard library
convention "where: imperative what to change", i.e., "all: make all doc
comments complete sentences" -- short imperative commit messages. PRs say what
changed and why in a sentence or two, list material changes, and cite the
observable (an operation, a header, a documented status) for every claim the
SDK newly makes about the API.
