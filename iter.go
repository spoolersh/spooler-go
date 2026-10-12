package spooler

import (
	"context"
	"iter"
)

func list[T any](start string, fn func(after string) (Page[T], error)) iter.Seq2[T, error] {
	var zero T
	return func(yield func(T, error) bool) {
		// Capture given after as in start position: repetitive iteration over
		// sequence would start from the beginning.
		after := start
		for {
			page, err := fn(after)
			if err != nil {
				yield(zero, err)
				return
			}
			for _, item := range page.Items {
				if !yield(item, nil) {
					return
				}
			}
			if page.Next == "" {
				return
			}
			after = page.Next
		}
	}
}

// Queues returns an iterator over a spool's queues across every page, where
// [Client.Queues] returns one.
func Queues(ctx context.Context, c *Client, req QueuesRequest) iter.Seq2[Queue, error] {
	return list(req.After, func(after string) (Page[Queue], error) {
		req.After = after
		return c.Queues(ctx, req)
	})
}

// Messages returns an iterator over a queue's messages across every page,
// where [Client.Messages] returns one.
func Messages(ctx context.Context, c *Client, req MessagesRequest) iter.Seq2[Message, error] {
	return list(req.After, func(after string) (Page[Message], error) {
		req.After = after
		return c.Messages(ctx, req)
	})
}
