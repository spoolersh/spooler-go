# spooler-go

[![CI](https://github.com/spoolersh/spooler-go/actions/workflows/ci.yaml/badge.svg)](https://github.com/spoolersh/spooler-go/actions/workflows/ci.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/spoolersh/spooler-go.svg)](https://pkg.go.dev/github.com/spoolersh/spooler-go)

The Go client for [Spooler](https://docs.spooler.sh) message queue.

## Installation

```sh
go get github.com/spoolersh/spooler-go
```

## Usage

Run Spooler [locally](https://docs.spooler.sh/local):

```sh
docker run --rm -p 127.0.0.1:8080:8080 spoolersh/memspoold
```
> The image is an evaluation build: nothing persists, only the `default` spool exists, and the conditions it never produces are listed on [docs.spooler.sh/local](https://docs.spooler.sh/local).

> The example below is in [example_test.go](example_test.go), so it compiles with the package.

#### Create the client

```go
c := spooler.Client{
	InsecureDisableTLS: true,     // The local image runs http only, so no TLS for it.
	Host:       "127.0.0.1:8080", // Address you bound the image to.
	APIKey:     "local",          // Any API key would work with the local image.
}
```

#### Create your first queue

```go
err := c.CreateQueue(ctx, spooler.CreateQueueRequest{
	Spool: "default",
	Queue: "greetings",
})
if err != nil && !errors.Is(err, spooler.ErrQueueExists) {
	// Handle error.
}
```

#### Send your first message

```go
_, err = c.Send(ctx, spooler.SendRequest{
	Spool: "default",
	Queue: "greetings",
	Data:  []byte("Hello, Spooler!"),
})
if err != nil {
	// Handle error.
}

```

#### Receive the message

```go
msg, ok, err := c.Recv(ctx, spooler.RecvRequest{
	Spool: "default",
	Queue: "greetings",
})
if err != nil {
	log.Fatal(err)
}
if !ok {
    // The wait elapsed with nothing to receive (someone else received it!).
}
fmt.Printf("%s\n", msg.Data)
```

#### Acknowledge the message

```go
err = c.Ack(ctx, spooler.AckRequest{
	Spool: "default",
	Lease: msg.Lease,
})
if err != nil {
	// Handle error.
}
```

Against the production API, leave `Host` and `InsecureDisableTLS` at their zero values (`Host` defaults to `"api.spooler.sh"`) and set a real `APIKey`.

## Errors

Every error the API answers is one type, `*spooler.Error`. Match its kind with `errors.Is` against a sentinel, and read the details a kind carries with `errors.As`:

```go
_, err := c.Queue(ctx, spooler.QueueRequest{
    Spool: "default", 
    Queue: "missing",
})
switch {
case errors.Is(err, spooler.ErrQueueNotFound):
	var detail *spooler.QueueNotFoundError
	if errors.As(err, &detail) {
		fmt.Println("no queue named", detail.Queue)
	}

case errors.Is(err, spooler.ErrRateLimited):
	var e *spooler.Error
	if errors.As(err, &e) {
		fmt.Println("retry after", e.RetryAfter)
	}

case err != nil:
    // Handle error.
}
```

The error text is the server's message and is human readable; a program acts on the kind, the details and `RetryAfter`.
The kinds, what each means and which are worth retrying are on [docs.spooler.sh/errors](https://docs.spooler.sh/errors).

A request the client refuses before sending, such as one missing a required field, matches `spooler.ErrInvalidRequest`. 

### Retries

The client retries transient errors on its own, waiting out a `Retry-After` when the server sends one. `Client.Attempts` sets how many tries an operation gets; the default is two. Put a deadline on the context to cap the wait.

A send is retried only when it carries a dedup key, since without one a retry could append the message twice. A request that got no answer at all is never retried; the error is returned and you decide.

Which errors are transient is on [docs.spooler.sh/errors](https://docs.spooler.sh/errors); dedup keys are on [docs.spooler.sh/dedup](https://docs.spooler.sh/dedup).

## Testing

The integration tests run when `SPOOLER_API` and `SPOOLER_KEY` are set and skip otherwise:

```sh
SPOOLER_API=http://localhost:8080/v1 SPOOLER_KEY=local go test ./...
```

## Documentation

- [API reference](https://docs.spooler.sh/api): every operation, its request and its answer.
- [Delivery](https://docs.spooler.sh/delivery): leases, settles, retries and what is guaranteed.
- [Deduplication](https://docs.spooler.sh/dedup): safe retries of a send.
- [Errors](https://docs.spooler.sh/errors): the error kinds and the retry rules.
- [Limits](https://docs.spooler.sh/limits): the bounds on sizes, rates and retention.
- [Running locally](https://docs.spooler.sh/local): the evaluation image.

## License

[Apache 2.0](LICENSE). Copyright 2026 Sergey Kamardin; see [NOTICE](NOTICE).
