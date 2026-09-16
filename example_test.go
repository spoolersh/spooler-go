package spooler_test

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/spoolersh/spooler-go"
)

// The send, receive, ack loop against the local evaluation image, which any
// key authenticates. Run it with:
//
//	docker run --rm -p 127.0.0.1:8080:8080 spoolersh/memspoold
func Example() {
	ctx := context.Background()
	c := spooler.Client{
		Host:               "localhost:8080",
		InsecureDisableTLS: true,
		APIKey:             "local",
	}

	err := c.CreateQueue(ctx, spooler.CreateQueueRequest{
		Spool: "default",
		Queue: "greetings_and_salutations",
	})
	if err != nil && !errors.Is(err, spooler.ErrQueueExists) {
		log.Fatal(err)
	}

	_, err = c.Send(ctx, spooler.SendRequest{
		Spool: "default",
		Queue: "greetings_and_salutations",
		Data:  []byte("hello"),
	})
	if err != nil {
		log.Fatal(err)
	}

	msg, ok, err := c.Recv(ctx, spooler.RecvRequest{
		Spool: "default",
		Queue: "greetings_and_salutations",
	})
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		fmt.Println("nothing to receive")
		return
	}
	fmt.Printf("%s\n", msg.Data)

	err = c.Ack(ctx, spooler.AckRequest{
		Spool: "default",
		Lease: msg.Lease,
	})
	if err != nil {
		log.Fatal(err)
	}
}

// A consumer that keeps a long lease alive while it works: renew on a
// ticker, and stop renewing once the message is settled.
func ExampleClient_Renew() {
	ctx := context.Background()
	c := spooler.Client{APIKey: "..."}

	msg, ok, err := c.Recv(ctx, spooler.RecvRequest{
		Spool:       "default",
		Queue:       "greetings_and_salutations",
		WaitSeconds: new(20),
	})
	if err != nil || !ok {
		return
	}
	if _, expires := msg.LeaseMeta.Expires(); !expires {
		// The queue has no lease timeout: nothing to renew.
		return
	}

	_, err = c.Renew(ctx, spooler.RenewRequest{
		Spool: "default",
		Lease: msg.Lease,
	})
	if err != nil {
		return
	}
}

// Sending with a dedup key makes a retry safe: a repeat inside the queue's
// dedup window returns the original message instead of appending another.
func ExampleClient_Send_dedup() {
	ctx := context.Background()
	c := spooler.Client{APIKey: "..."}

	res, err := c.Send(ctx, spooler.SendRequest{
		Spool: "default",
		Queue: "orders",
		Dedup: spooler.DedupString("order-12345"),
		Data:  []byte(`{"order":12345}`),
	})
	if err != nil {
		return
	}
	if res.Duplicate {
		fmt.Println("already sent as", res.ID)
	}
}

// Every error the API answers is an [spooler.Error]; match its kind with
// errors.Is, and read a kind's details with errors.As.
func ExampleError() {
	ctx := context.Background()
	c := spooler.Client{APIKey: "..."}

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
		fmt.Println(err)
	}
}
