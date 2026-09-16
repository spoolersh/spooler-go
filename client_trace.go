package spooler

//go:generate gtrace

import (
	"context"
	"log/slog"
	"time"
)

// ClientTrace is a set of runtime hooks a [Client] calls as it works.
// Every field is optional; a nil hook is skipped. Hooks receive plain values
// and never a token, so anything they log is safe to keep.
//
// Use [ClientTrace.Compose] to compose different hook sets, such as logging,
// metrics, etc.
//
//gtrace:gen
type ClientTrace struct {
	// OnConnect is called when a connection to the host is being opened, and
	// the func it returns when the attempt ends.
	OnConnect func(network, addr string) func(error)

	// OnRequest is called once per operation, before any request is made, and
	// the func it returns when the operation ends.
	//
	// A retry inside the operation is not a second call. The context is the
	// caller's, for logging with it or starting a span under it.
	OnRequest func(context.Context, RequestTrace) func(error)

	// OnMalformedResponse is called when a successful answer carries a value
	// the client cannot read, such as a lease expiry that is not a time. The
	// operation still succeeds, with that value zero, since the answer is
	// otherwise good and a lease it granted must reach the caller; the error
	// says which value and why.
	//
	// Getting it called means a server bug: report it.
	OnMalformedResponse func(context.Context, RequestTrace, error)
}

// RequestTrace describes the operation an OnRequest hook is called for.
type RequestTrace struct {
	// Op is the operation.
	Op Op

	// Spool is the spool the operation names.
	Spool string

	// Queue is the queue the operation names, or empty for a spool-scoped
	// one such as a settle.
	Queue string
}

// LogValue renders the trace as a group of its fields, so that a logger can
// attach it as one attribute.
func (t RequestTrace) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("op", t.Op.String()),
		slog.String("spool", t.Spool),
		slog.String("queue", t.Queue),
	)
}

// Op is an operation of the [Client], for tracing.
type Op uint

// The operations, one per [Client] method.
const (
	OpUnknown Op = iota
	OpSpoolStats
	OpSpoolQueues
	OpQueueMessages
	OpCreateQueue
	OpQueue
	OpUpdateQueue
	OpDeleteQueue
	OpSend
	OpAckAndSend
	OpRecv
	OpRenew
	OpAck
	OpNack
	OpRelease
	OpFail
	OpDiscard
	OpPeek
	OpRecover
)

var opNames = map[Op]string{
	OpSpoolStats:    "SpoolStats",
	OpSpoolQueues:   "SpoolQueues",
	OpQueueMessages: "QueueMessages",
	OpCreateQueue:   "CreateQueue",
	OpQueue:         "Queue",
	OpUpdateQueue:   "UpdateQueue",
	OpDeleteQueue:   "DeleteQueue",
	OpSend:          "Send",
	OpAckAndSend:    "AckAndSend",
	OpRecv:          "Recv",
	OpRenew:         "Renew",
	OpAck:           "Ack",
	OpNack:          "Nack",
	OpRelease:       "Release",
	OpFail:          "Fail",
	OpDiscard:       "Discard",
	OpPeek:          "Peek",
	OpRecover:       "Recover",
}

// String names the operation as the method is named.
func (o Op) String() string { return enumString("Op", opNames, o) }

// LoggerClientTrace builds a [ClientTrace] that logs every hook to the given
// logger.
func LoggerClientTrace(logger *slog.Logger) ClientTrace {
	return ClientTrace{
		OnConnect: func(network, addr string) func(error) {
			logger := logger.With(
				"network", network,
				"addr", addr,
			)
			start := time.Now()
			logger.Debug("connecting to host")
			return func(err error) {
				logger = logger.With(
					"latency", time.Since(start),
				)
				if err != nil {
					logger.Error("failed connecting to host",
						"error", err,
					)
					return
				}
				logger.Debug("connected to host")
			}
		},
		OnRequest: func(ctx context.Context, req RequestTrace) func(error) {
			logger := logger.With(
				"request", req,
			)
			start := time.Now()
			logger.DebugContext(ctx, "doing request")
			return func(err error) {
				logger = logger.With(
					"latency", time.Since(start),
				)
				if err != nil {
					logger.ErrorContext(ctx, "failed doing request",
						"error", err,
					)
					return
				}
				logger.DebugContext(ctx, "done request")
			}
		},
		OnMalformedResponse: func(ctx context.Context, req RequestTrace, err error) {
			logger.ErrorContext(ctx, "malformed server response",
				"request", req,
				"error", err,
			)
		},
	}
}
