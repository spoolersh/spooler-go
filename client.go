package spooler

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spoolersh/spooler-go/internal/httputil"
)

// DefaultHost is the public API's host, used when [Client.Host] is empty.
const DefaultHost = "api.spooler.sh"

// DefaultAttempts is how many times an operation is tried when
// [Client.Attempts] is zero: the first request and one retry.
const DefaultAttempts = 2

const (
	headerAuthorization  = "Authorization"
	headerDedupHash      = "Spooler-Dedup-Hash"
	headerDedupString    = "Spooler-Dedup-String"
	headerDelay          = "Spooler-Delay-Seconds"
	headerHandle         = "Spooler-Handle"
	headerLease          = "Spooler-Lease"
	headerLeaseExpiresAt = "Spooler-Lease-Expires-At"
	headerMessageID      = "Spooler-Message-Id"
	headerMessageRetries = "Spooler-Message-Retries"
	headerMessageState   = "Spooler-Message-State"
	headerQueue          = "Spooler-Queue"
)

// Client implements Spooler API spec.
//
// It is safe for concurrent use and should be shared, since each one holds its
// own connection pool.
//
// Every operation retries the conditions the API documents as transient, up to
// [Client.Attempts] or [DefaultAttempts].
//
// The zero value is ready to use, although for production API the
// [Client.APIKey] is required.
type Client struct {
	// Trace is an optional set of runtime hooks the client calls.
	Trace ClientTrace

	// Host specifies the API host, as "host" or "host:port".
	// Zero means [DefaultHost].
	Host string

	// APIKey authenticates every request made by this Client.
	APIKey string

	// Attempts is how many times at most an operation is tried before its
	// error is returned, the initial request included. Some operations or
	// errors are not retried regardless of the value.
	//
	// Zero means [DefaultAttempts], 1 makes no retries.
	Attempts int

	// InsecureDisableTLS makes Client using insecure connection without TLS,
	// so the API key travels in plain text. For testing only.
	InsecureDisableTLS bool

	// InsecureDebug dumps every request and response to standard error.
	// For testing only.
	InsecureDebug bool

	// transport replaces the network in tests. It is unexported on purpose:
	// the transport never leaves the package.
	transport http.RoundTripper

	once sync.Once
	http *httputil.Client
}

func (c *Client) init() {
	c.once.Do(func() {
		scheme := "https"
		if c.InsecureDisableTLS {
			scheme = "http"
		}
		u := url.URL{
			Scheme: scheme,
			Host:   cmp.Or(c.Host, DefaultHost),
			Path:   "v1",
		}
		b := httputil.ClientBuilder{
			Transport: c.transport,
			Trace: httputil.RoundTripperTrace{
				OnConnect: c.Trace.onConnect,
			},
		}
		if c.InsecureDebug {
			b.Trace = b.Trace.Compose(httputil.DumpRoundTripperTrace(os.Stderr,
				headerAuthorization,
				headerLease,
				headerHandle,
			))
		}
		c.http = &httputil.Client{
			ClientBuilder: b,
			UserAgent:     "spooler-go/" + Version,
			BaseURL:       u.String(),
			Codec: httputil.Codecs{
				Send:  httputil.JSONEncoder,
				Recv:  httputil.JSONDecoder,
				Error: httputil.JSONDecoder,
			},
			Header: http.Header{
				headerAuthorization: []string{"Bearer " + c.APIKey},
			},
			Error: func(int) any {
				return new(errorObj)
			},
		}
	})
}

// attempts returns the effective [Client.Attempts].
func (c *Client) attempts() int {
	return cmp.Or(c.Attempts, DefaultAttempts)
}

// do makes one operation's request, reporting it to the trace as a whole:
// the hook fires once here, however many attempts the transport makes.
func (c *Client) do(ctx context.Context, rt RequestTrace, method string, req httputil.Request) (res httputil.Response, err error) {
	done := c.Trace.onRequest(ctx, rt)
	defer func() {
		done(err)
	}()
	res, err = c.http.Do(ctx, method, req)
	if err != nil {
		err = httpError(err)
	}
	return res, err
}

type clientError struct {
	err error
}

func (e *clientError) Error() string {
	var sb strings.Builder
	sb.WriteString("spooler: ")
	sb.WriteString(e.err.Error())
	return sb.String()
}

// Unwrap exposes only what is contract: the API's [Error], a request the SDK
// refused as [ErrInvalidRequest], and the context errors by their sentinel.
// Anything else in the chain (a *url.Error, say) stays reachable as text only.
func (e *clientError) Unwrap() error {
	if x, ok := e.err.(*Error); ok {
		return x
	}
	switch {
	case errors.Is(e.err, ErrInvalidRequest):
		return e.err
	case errors.Is(e.err, context.Canceled):
		return context.Canceled
	case errors.Is(e.err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	default:
		return nil
	}
}

// ErrInvalidRequest is what every request the SDK refuses before sending.
// Matches with [errors.Is]: a required field missing, or two that exclude each
// other both set. The text says which.
var ErrInvalidRequest = errors.New("invalid request")

// invalidRequest wraps a refusal so that it matches ErrInvalidRequest.
func invalidRequest(msg string) error {
	return resultError(fmt.Errorf("%w: %s", ErrInvalidRequest, msg))
}

var (
	errSpoolRequired     = invalidRequest("spool name is required")
	errQueueRequired     = invalidRequest("queue name is required")
	errDataAndDataSource = invalidRequest("either Data or DataSource is required, not both")
	errLeaseRequired     = invalidRequest("lease is required")
	errLeaseAndHandle    = invalidRequest("either lease or handle is required, not both")
	errLeaseOrHandle     = invalidRequest("either lease or handle is required")
	errHandleRequired    = invalidRequest("handle is required")
)

func resultError(err error) error {
	if err == nil {
		return nil
	}
	return &clientError{
		err: err,
	}
}

// SpoolStatsRequest describes a spool to read, and the pagination of its
// queues.
type SpoolStatsRequest struct {
	// Spool is the spool to read.
	Spool string

	// Queues is an optional filter keeping only these queues on the page.
	// Zero filters nothing.
	Queues []string

	// Match is an optional filter keeping on the page only the queues whose
	// names the expression matches, unanchored.
	// Zero filters nothing.
	Match *regexp.Regexp

	// Limit is an optional cap on the number of queues on the page.
	// Zero means the server's default.
	Limit int

	// After is an optional cursor resuming the page from the previous page's
	// Next.
	// Zero starts from the first page.
	After string
}

// SpoolStats reads a spool's message counts and one page of its queues with
// theirs.
func (c *Client) SpoolStats(ctx context.Context, req SpoolStatsRequest) (SpoolStats, error) {
	c.init()
	if req.Spool == "" {
		return zero[SpoolStats](), errSpoolRequired
	}
	var query url.Values
	if len(req.Queues) > 0 {
		query = sliceMapSet(query, "queue", req.Queues...)
	}
	if req.Limit != 0 {
		query = sliceMapSet(query, "limit", strconv.Itoa(req.Limit))
	}
	if req.After != "" {
		query = sliceMapSet(query, "after", req.After)
	}
	if req.Match != nil {
		query = sliceMapSet(query, "match", req.Match.String())
	}
	var ret spoolStats
	rt := RequestTrace{Op: OpSpoolStats, Spool: req.Spool}
	_, err := c.do(ctx, rt, "GET", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/stats",
		Query:    query,
		Recv:     &ret,
	})
	if err != nil {
		return zero[SpoolStats](), resultError(err)
	}
	return ret.toSpoolStats(), nil
}

// SpoolQueuesRequest describes a spool's queues to list, and their pagination.
type SpoolQueuesRequest struct {
	// Spool is the spool to list.
	Spool string

	// Match is an optional filter keeping on the page only the queues whose
	// names the expression matches, unanchored.
	// Zero filters nothing.
	Match *regexp.Regexp

	// Limit is an optional cap on the queues on the page.
	// Zero means the server's default.
	Limit int

	// After is an optional cursor resuming the page from the previous page's
	// Next.
	// Zero starts from the first page.
	After string
}

// SpoolQueues lists one page of a spool's queues. For their message counts
// use [Client.SpoolStats].
func (c *Client) SpoolQueues(ctx context.Context, req SpoolQueuesRequest) (Page[Queue], error) {
	c.init()
	if req.Spool == "" {
		return zero[Page[Queue]](), errSpoolRequired
	}
	var query url.Values
	if req.Limit != 0 {
		query = sliceMapSet(query, "limit", strconv.Itoa(req.Limit))
	}
	if req.After != "" {
		query = sliceMapSet(query, "after", req.After)
	}
	if req.Match != nil {
		query = sliceMapSet(query, "match", req.Match.String())
	}
	var ret page[queue]
	rt := RequestTrace{Op: OpSpoolQueues, Spool: req.Spool}
	_, err := c.do(ctx, rt, "GET", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues",
		Query:    query,
		Recv:     &ret,
	})
	if err != nil {
		return zero[Page[Queue]](), resultError(err)
	}
	return ret.toPage((*queue).toQueue), nil
}

// QueueMessagesRequest describes a queue's messages to list, and their
// pagination.
type QueueMessagesRequest struct {
	// Spool is the spool the queue is in.
	Spool string

	// Queue is the queue to list.
	Queue string

	// State is an optional filter keeping only messages in this state on
	// the page.
	// Zero keeps messages of every state on the page.
	State MessageState

	// Limit is an optional cap on the messages on the page.
	// Zero means the server's default.
	Limit int

	// After is an optional cursor resuming the page just past this message
	// id, from the previous page's Next.
	// Zero starts from the first page.
	After string

	// ReturnHandle is an optional request for a handle on each message, for
	// [Client.Peek], [Client.Recover] and [Client.Discard].
	//
	// A handle is a credential, so it is off by default.
	ReturnHandle bool
}

// QueueMessages lists one page of a queue's messages, without their payloads.
func (c *Client) QueueMessages(ctx context.Context, req QueueMessagesRequest) (Page[Message], error) {
	c.init()
	if req.Spool == "" {
		return zero[Page[Message]](), errSpoolRequired
	}
	if req.Queue == "" {
		return zero[Page[Message]](), errQueueRequired
	}

	var query url.Values
	if req.State != 0 {
		w, err := req.State.toWire()
		if err != nil {
			return zero[Page[Message]](), invalidRequest(fmt.Sprintf(
				"State: %v", err,
			))
		}
		query = sliceMapSet(query, "state", w)
	}
	if lim := req.Limit; lim != 0 {
		query = sliceMapSet(query, "limit", strconv.Itoa(lim))
	}
	if cur := req.After; cur != "" {
		query = sliceMapSet(query, "after", cur)
	}
	if req.ReturnHandle {
		query = sliceMapSet(query, "handle", "true")
	}
	var ret page[message]
	rt := RequestTrace{Op: OpQueueMessages, Spool: req.Spool, Queue: req.Queue}
	_, err := c.do(ctx, rt, "GET", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue + "/messages",
		Query:    query,
		Recv:     &ret,
	})
	if err != nil {
		return zero[Page[Message]](), resultError(err)
	}
	return ret.toPage((*message).toMessage), nil
}

// CreateQueueRequest describes a queue to create and the settings to create it
// with.
type CreateQueueRequest struct {
	// Spool is the spool to create the queue in.
	Spool string

	// Queue is the queue's name.
	Queue string

	// Settings are optional settings to apply.
	// Zero means the server's defaults.
	Settings QueueSettingsCreate
}

// CreateQueue creates a queue; one that already exists is [ErrQueueExists].
//
//	// Ensure a queue "foo" exists.
//	err := c.CreateQueue(ctx, spooler.CreateQueueRequest{
//		Spool: "default",
//		Queue: "foo",
//	})
//	if errors.Is(err, spooler.ErrQueueExists) {
//		err = nil
//	}
//	if err != nil {
//		// Handle error.
//	}
func (c *Client) CreateQueue(ctx context.Context, req CreateQueueRequest) error {
	c.init()
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Queue == "" {
		return errQueueRequired
	}
	rt := RequestTrace{Op: OpCreateQueue, Spool: req.Spool, Queue: req.Queue}
	_, err := c.do(ctx, rt, "PUT", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue,
		Send:     req.Settings.wire(),
	})
	return resultError(err)
}

// QueueRequest describes a queue to read.
type QueueRequest struct {
	// Spool is the spool the queue is in.
	Spool string

	// Queue is the queue's name.
	Queue string
}

// Queue reads a queue: its state and its settings. For its message counts
// use [Client.SpoolStats].
func (c *Client) Queue(ctx context.Context, req QueueRequest) (Queue, error) {
	c.init()
	if req.Spool == "" {
		return zero[Queue](), errSpoolRequired
	}
	if req.Queue == "" {
		return zero[Queue](), errQueueRequired
	}
	var ret queue
	rt := RequestTrace{Op: OpQueue, Spool: req.Spool, Queue: req.Queue}
	_, err := c.do(ctx, rt, "GET", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue,
		Recv:     &ret,
	})
	if err != nil {
		return zero[Queue](), resultError(err)
	}
	return ret.toQueue(), nil
}

// UpdateQueueRequest describes a queue and the settings to replace on it.
type UpdateQueueRequest struct {
	// Spool is the spool the queue is in.
	Spool string

	// Queue is the queue's name.
	Queue string

	// Settings are the settings to replace.
	// At least one Settings's field must be set.
	Settings QueueSettingsUpdate
}

// UpdateQueue updates queue settings.
func (c *Client) UpdateQueue(ctx context.Context, req UpdateQueueRequest) error {
	c.init()
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Queue == "" {
		return errQueueRequired
	}
	rt := RequestTrace{Op: OpUpdateQueue, Spool: req.Spool, Queue: req.Queue}
	_, err := c.do(ctx, rt, "PATCH", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue,
		Send:     req.Settings.wire(),
	})
	return resultError(err)
}

// DeleteQueueRequest describes a queue to delete and how.
type DeleteQueueRequest struct {
	// Spool is the spool the queue is in.
	Spool string

	// Queue is the queue's name.
	Queue string

	// Mode is how the deletion should proceed.
	Mode QueueDeleteMode
}

// DeleteQueue deletes a queue, per the request's Mode. When the call returns
// the deletion is accepted, not completed: the queue reports
// [QueueStateDeleting] or [QueueStateDraining] until it is gone.
func (c *Client) DeleteQueue(ctx context.Context, req DeleteQueueRequest) error {
	c.init()
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Queue == "" {
		return errQueueRequired
	}
	mode, err := req.Mode.toWire()
	if err != nil {
		return invalidRequest(fmt.Sprintf(
			"Mode: %v", err,
		))
	}
	rt := RequestTrace{Op: OpDeleteQueue, Spool: req.Spool, Queue: req.Queue}
	_, err = c.do(ctx, rt, "DELETE", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue,
		Query: url.Values{
			"mode": []string{mode},
		},
	})
	return resultError(err)
}

// Dedup is a send's deduplication key, from [DedupString] or [DedupHash];
// the rules are on https://docs.spooler.sh/dedup.
//
// Zero means no dedup key.
type Dedup struct {
	str  string
	hash string
}

// IsZero reports whether no key is set.
func (d Dedup) IsZero() bool {
	return d.str == "" && d.hash == ""
}

// DedupString keys a send by a string; the server hashes it.
func DedupString(str string) Dedup {
	return Dedup{
		str: str,
	}
}

// DedupHash keys a send by a hash the caller computed, hex-encoded, per the
// recipe on https://docs.spooler.sh/dedup.
func DedupHash(h string) Dedup {
	return Dedup{
		hash: h,
	}
}

// These wrappers expose only the interfaces the constructor selected.
// The [httputil.StreamEncoder] codec uses every optional method it sees;
// without trimming, passing x as a mere io.Reader could still cause x.Close(),
// x.Seek() or x.Size() to be called.
type (
	readSeeker struct {
		io.ReadSeeker
	}
	readSizer struct {
		io.Reader
		size int64
	}
)

func (r readSizer) Size() int64 { return r.size }

// DataSource is a streamed payload for a send, from [DataSourceReader] or
// [DataSourceReadSeeker]. The zero value is no source.
type DataSource struct {
	r io.Reader
}

// IsZero reports whether no source is set.
func (d DataSource) IsZero() bool {
	return d.r == nil
}

// DataSourceReader sends the first n bytes of r. The reader is read once, so a
// send that fails with an answer that may be retried is not retried.
// Use [DataSourceReadSeeker] for a source that can be sent again.
func DataSourceReader(r io.Reader, n int64) DataSource {
	return DataSource{
		r: readSizer{
			Reader: r,
			size:   n,
		},
	}
}

// DataSourceReadSeeker sends rs from its current offset to its end, and
// rewinds it to send again when a retry is due. To send part of a seekable
// source, pass an [io.SectionReader].
func DataSourceReadSeeker(rs io.ReadSeeker) DataSource {
	return DataSource{
		r: readSeeker{rs},
	}
}

// SendRequest describes a message to append to a queue.
type SendRequest struct {
	// Spool is the spool the queue is in.
	Spool string

	// Queue is the queue to append to.
	Queue string

	// DelaySeconds is an optional delay until the message becomes visible.
	// Zero means visible at once.
	DelaySeconds int

	// Dedup is an optional deduplication key.
	Dedup Dedup

	// Data is an optional payload.
	// Zero sends an empty message.
	Data []byte

	// DataSource is an optional stream to send as the payload instead of Data.
	// Setting both is an error.
	DataSource DataSource

	// ReturnHandle is an optional request for a handle to the new message,
	// which can be used in [Client.Discard] before its first delivery.
	//
	// A handle is a credential, so it is off by default.
	ReturnHandle bool
}

// SendResult is the outcome of a send.
type SendResult struct {
	// ID is the message id. It identifies the message in listings and logs.
	// No operation takes it.
	ID string

	// Duplicate reports a dedup hit: nothing was appended, and ID is the
	// original message's.
	Duplicate bool

	// Handle is the handle to the new message, when the request asked for one.
	// It is absent on a dedup hit.
	Handle Handle
}

func dedupHeader(d Dedup) (name, value string, ok bool) {
	switch {
	case d.str != "":
		return headerDedupString, d.str, true
	case d.hash != "":
		return headerDedupHash, d.hash, true
	default:
		return "", "", false
	}
}

// Send appends a message to a queue.
//
// Message becomes visible to receivers at once, or if set after the request's
// delay.
func (c *Client) Send(ctx context.Context, req SendRequest) (SendResult, error) {
	c.init()

	if req.Spool == "" {
		return zero[SendResult](), errSpoolRequired
	}
	if req.Queue == "" {
		return zero[SendResult](), errQueueRequired
	}
	if req.Data != nil && !req.DataSource.IsZero() {
		return zero[SendResult](), errDataAndDataSource
	}

	var query url.Values
	if req.ReturnHandle {
		query = sliceMapSet(query, "handle", "true")
	}

	var header http.Header
	if d := req.DelaySeconds; d != 0 {
		header = sliceMapSet(header, headerDelay, strconv.Itoa(d))
	}
	if name, value, has := dedupHeader(req.Dedup); has {
		header = sliceMapSet(header, name, value)
	}

	data := req.DataSource.r
	if data == nil && len(req.Data) > 0 {
		data = bytes.NewReader(req.Data)
	}
	rt := RequestTrace{Op: OpSend, Spool: req.Spool, Queue: req.Queue}
	res, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retrySend(!req.Dedup.IsZero()),
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue + "/send",
		Query:    query,
		Header:   header,
		Send:     data,
		Codec: httputil.Codecs{
			Send: httputil.StreamEncoder,
		},
	})
	if err != nil {
		return zero[SendResult](), resultError(err)
	}
	ret := SendResult{
		ID:        res.Header.Get(headerMessageID),
		Duplicate: res.Code == http.StatusOK,
	}
	if h := res.Header.Get(headerHandle); h != "" {
		ret.Handle = Handle(h)
	}
	return ret, nil
}

// AckAndSendRequest describes a delivery to ack and the message to append,
// both in a single atomic operation.
type AckAndSendRequest struct {
	// Spool is the spool both queues are in.
	Spool string

	// Lease is the delivery to ack. It names the queue it came from.
	Lease Lease

	// Queue is the queue to append the new message to.
	Queue string

	// DelaySeconds is an optional delay until the new message becomes
	// visible.
	// Zero means visible at once.
	DelaySeconds int

	// Dedup is an optional deduplication key for the new message.
	Dedup Dedup

	// Data is the new message's optional payload.
	// Zero sends an empty message.
	Data []byte

	// DataSource is an optional stream to send as the payload instead of Data.
	// Setting both is an error.
	DataSource DataSource

	// ReturnHandle is an optional request for a handle to the new message.
	//
	// A handle is a credential, so it is off by default.
	ReturnHandle bool
}

// AckAndSendResult is the outcome of an ack-and-send.
type AckAndSendResult struct {
	// ID is the new message's id.
	ID string

	// Handle is the handle to the new message, when the request asked for one.
	Handle Handle
}

// AckAndSend acks a delivery and appends a message to another queue as one
// atomic step: acknowledge and append together, or neither.
//
// A dedup hit on the new message refuses the whole call with [ErrDedupClaimed]
// and acks nothing.
//
// Unlike [Client.Send], a retry needs no dedup key: a retry of a call that
// committed fails with [ErrStaleLease] and appends nothing.
func (c *Client) AckAndSend(ctx context.Context, req AckAndSendRequest) (AckAndSendResult, error) {
	c.init()
	if req.Spool == "" {
		return zero[AckAndSendResult](), errSpoolRequired
	}
	if req.Queue == "" {
		return zero[AckAndSendResult](), errQueueRequired
	}
	if req.Lease == "" {
		return zero[AckAndSendResult](), errLeaseRequired
	}
	if req.Data != nil && !req.DataSource.IsZero() {
		return zero[AckAndSendResult](), errDataAndDataSource
	}

	header := http.Header{
		headerLease: {string(req.Lease)},
		headerQueue: {req.Queue},
	}
	if d := req.DelaySeconds; d != 0 {
		header.Set(headerDelay, strconv.Itoa(d))
	}
	if name, value, has := dedupHeader(req.Dedup); has {
		header.Set(name, value)
	}

	var query url.Values
	if req.ReturnHandle {
		query = sliceMapSet(query, "handle", "true")
	}

	data := req.DataSource.r
	if data == nil && len(req.Data) > 0 {
		data = bytes.NewReader(req.Data)
	}
	rt := RequestTrace{Op: OpAckAndSend, Spool: req.Spool, Queue: req.Queue}
	res, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retrySend(!req.Dedup.IsZero()),
		Path:     "/spools/" + req.Spool + "/ack-and-send",
		Query:    query,
		Header:   header,
		Send:     data,
		Codec: httputil.Codecs{
			Send: httputil.StreamEncoder,
		},
	})
	if err != nil {
		return zero[AckAndSendResult](), resultError(err)
	}
	ret := AckAndSendResult{
		ID: res.Header.Get(headerMessageID),
	}
	if h := res.Header.Get(headerHandle); h != "" {
		ret.Handle = Handle(h)
	}
	return ret, nil
}

// RecvRequest describes a queue to receive from and how long to wait.
type RecvRequest struct {
	// Spool is the spool the queue is in.
	Spool string

	// Queue is the queue to receive from.
	Queue string

	// WaitSeconds is an optional bound on the long poll; a value of 0
	// returns without waiting.
	//
	// Zero means the server's default.
	WaitSeconds *int

	// Writer is an optional destination for the payload.
	// Zero buffers the payload and returns it in RecvResult.Data instead.
	Writer io.Writer
}

// RecvResult is the outcome of a receive: the message and the lease on it.
type RecvResult struct {
	// ID is the message id. It identifies the message in listings and logs.
	// No operation takes it.
	ID string

	// Retries is the redeliveries so far, or -1 when the server reported a
	// value that did not parse.
	Retries int

	// Lease is the lease on the delivery.
	Lease Lease

	// LeaseMeta is what is known about the lease beyond the token.
	LeaseMeta LeaseMeta

	// Data is the message payload.
	//
	// It is nil when [RecvRequest.Writer] was set. Otherwise, it is nil when
	// the received message was previously sent without payload.
	Data []byte
}

// LeaseMeta is what a receive or a renew reports about a lease.
type LeaseMeta struct {
	// ExpiresAt is when the lease expires.
	// Zero means the lease never expires.
	ExpiresAt time.Time
}

// Expires reports when the lease expires; ok is false when it never does.
func (m LeaseMeta) Expires() (t time.Time, ok bool) {
	if m.ExpiresAt.IsZero() {
		return t, false
	}
	return m.ExpiresAt, true
}

// Lease is exclusive ownership of one delivery, from a receive.
// The token is opaque. It is a credential, and prints redacted.
type Lease string

// Handle is a reference to a message as it is now, from a send that asked for
// one or from a listing. It grants no ownership and has no timeout, and it
// goes stale once anything moves the message.
//
// The token is opaque. It is a credential, and prints redacted.
type Handle string

// A token is a credential, so it prints redacted through fmt.
//
// The SDK converts with string(...) where the value is presented back, and a
// caller may do the same.
const redacted = "REDACTED"

// String returns a redacted form, never the token.
func (Lease) String() string { return redacted }

// String returns a redacted form, never the token.
func (Handle) String() string { return redacted }

// Recv leases the next message of the queue. When none becomes visible
// within the wait, Recv reports ok as false, with a zero result and no
// error.
func (c *Client) Recv(ctx context.Context, req RecvRequest) (msg RecvResult, ok bool, err error) {
	c.init()

	if req.Spool == "" {
		return msg, false, errSpoolRequired
	}
	if req.Queue == "" {
		return msg, false, errQueueRequired
	}

	var buf *bytes.Buffer
	dst := req.Writer
	if dst == nil {
		buf = bytes.NewBuffer(nil)
		dst = buf
	}
	var query url.Values
	if w := req.WaitSeconds; w != nil {
		if *w < 0 {
			return msg, false, invalidRequest(fmt.Sprintf(
				"WaitSeconds: %d is negative", *w,
			))
		}
		query = sliceMapSet(query, "waitSeconds", strconv.Itoa(*w))
	}
	rt := RequestTrace{Op: OpRecv, Spool: req.Spool, Queue: req.Queue}
	res, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue + "/recv",
		Query:    query,
		Recv:     dst,
		Codec: httputil.Codecs{
			Recv: httputil.StreamDecoder,
		},
	})
	if err != nil {
		return msg, false, resultError(err)
	}
	switch res.Code {
	case http.StatusOK:
		expiresAt, err := parseExpiresAt(res.Header)
		if err != nil {
			c.Trace.onMalformedResponse(ctx, rt, err)
		}
		retries, err := parseRetries(res.Header)
		if err != nil {
			c.Trace.onMalformedResponse(ctx, rt, err)
		}
		msg = RecvResult{
			ID:      res.Header.Get(headerMessageID),
			Retries: retries,
			Lease:   Lease(res.Header.Get(headerLease)),
			LeaseMeta: LeaseMeta{
				ExpiresAt: expiresAt,
			},
		}
		if buf != nil {
			msg.Data = buf.Bytes()
		}
		return msg, true, nil

	case http.StatusNoContent:
		return msg, false, nil

	default:
		return msg, false, resultError(fmt.Errorf(
			"unexpected success status: %d",
			res.Code,
		))
	}
}

// RenewRequest describes a lease to renew.
type RenewRequest struct {
	// Spool is the spool the lease is in.
	Spool string

	// Lease is the lease to renew.
	Lease Lease
}

// RenewResult is the outcome of a renew.
type RenewResult struct {
	// LeaseMeta carries the new expiry.
	LeaseMeta LeaseMeta
}

// Renew resets the lease's expiry and reports the new one. The token stays
// the same.
func (c *Client) Renew(ctx context.Context, req RenewRequest) (RenewResult, error) {
	c.init()

	if req.Spool == "" {
		return zero[RenewResult](), errSpoolRequired
	}
	if req.Lease == "" {
		return zero[RenewResult](), errLeaseRequired
	}
	header := http.Header{
		headerLease: {string(req.Lease)},
	}
	rt := RequestTrace{Op: OpRenew, Spool: req.Spool}
	res, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/renew",
		Header:   header,
	})
	if err != nil {
		return zero[RenewResult](), resultError(err)
	}
	expiresAt, err := parseExpiresAt(res.Header)
	if err != nil {
		c.Trace.onMalformedResponse(ctx, rt, err)
	}
	return RenewResult{
		LeaseMeta: LeaseMeta{
			ExpiresAt: expiresAt,
		},
	}, nil
}

// AckRequest describes a delivery to ack.
type AckRequest struct {
	// Spool is the spool the lease is in.
	Spool string

	// Lease is the delivery to ack.
	Lease Lease
}

// Ack acks the delivery: the message is removed.
func (c *Client) Ack(ctx context.Context, req AckRequest) error {
	c.init()
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Lease == "" {
		return errLeaseRequired
	}
	header := http.Header{
		headerLease: {string(req.Lease)},
	}
	rt := RequestTrace{Op: OpAck, Spool: req.Spool}
	_, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/ack",
		Header:   header,
	})
	return resultError(err)
}

// NackRequest describes a delivery to nack.
type NackRequest struct {
	// Spool is the spool the lease is in.
	Spool string

	// Lease is the delivery to nack.
	Lease Lease
}

// Nack returns the message for redelivery, counting a retry. Past the queue's
// max retries the message is failed instead.
func (c *Client) Nack(ctx context.Context, req NackRequest) error {
	c.init()
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Lease == "" {
		return errLeaseRequired
	}
	header := http.Header{
		headerLease: {string(req.Lease)},
	}
	rt := RequestTrace{Op: OpNack, Spool: req.Spool}
	_, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/nack",
		Header:   header,
	})
	return resultError(err)
}

// ReleaseRequest describes a delivery to release.
type ReleaseRequest struct {
	// Spool is the spool the lease is in.
	Spool string

	// Lease is the delivery to release.
	Lease Lease

	// DelaySeconds is an optional delay until the message becomes visible
	// again.
	// Zero means visible at once.
	DelaySeconds int
}

// Release returns the message for redelivery without counting a retry,
// after the request's delay if any.
func (c *Client) Release(ctx context.Context, req ReleaseRequest) error {
	c.init()
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Lease == "" {
		return errLeaseRequired
	}
	header := http.Header{
		headerLease: {string(req.Lease)},
	}
	if d := req.DelaySeconds; d != 0 {
		header.Set(headerDelay, strconv.Itoa(d))
	}
	rt := RequestTrace{Op: OpRelease, Spool: req.Spool}
	_, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/release",
		Header:   header,
	})
	return resultError(err)
}

// FailRequest describes a delivery to fail.
type FailRequest struct {
	// Spool is the spool the lease is in.
	Spool string

	// Lease is the delivery to fail.
	Lease Lease
}

// Fail marks the message failed.
func (c *Client) Fail(ctx context.Context, req FailRequest) error {
	c.init()
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Lease == "" {
		return errLeaseRequired
	}
	header := http.Header{
		headerLease: {string(req.Lease)},
	}
	rt := RequestTrace{Op: OpFail, Spool: req.Spool}
	_, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/fail",
		Header:   header,
	})
	return resultError(err)
}

// DiscardRequest describes a message to discard, by exactly one of its
// tokens.
type DiscardRequest struct {
	// Spool is the spool the message is in.
	Spool string

	// Lease is the delivery to discard, from a receive. Set exactly one of
	// Lease and Handle.
	Lease Lease

	// Handle is the message to discard, from a send or a listing.
	Handle Handle
}

// Discard drops the message, by its lease or its handle, exactly one.
func (c *Client) Discard(ctx context.Context, req DiscardRequest) error {
	c.init()
	if req.Spool == "" {
		return errSpoolRequired
	}
	header := make(http.Header, 1)
	switch {
	case req.Lease != "" && req.Handle != "":
		return errLeaseAndHandle
	case req.Lease != "":
		header.Set(headerLease, string(req.Lease))
	case req.Handle != "":
		header.Set(headerHandle, string(req.Handle))
	default:
		return errLeaseOrHandle
	}
	rt := RequestTrace{Op: OpDiscard, Spool: req.Spool}
	_, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/discard",
		Header:   header,
	})
	return resultError(err)
}

// PeekRequest describes a message to read, by its handle.
type PeekRequest struct {
	// Spool is the spool the message is in.
	Spool string

	// Handle is the message to read, from a listing or a send.
	Handle Handle

	// Writer is an optional destination for the payload.
	// Zero buffers the payload and returns it in PeekResult.Data instead.
	Writer io.Writer
}

// PeekResult is the outcome of a peek: the message as it is now.
type PeekResult struct {
	// ID is the message id. It identifies the message in listings and logs.
	// No operation takes it.
	ID string

	// State is the state the message is in now.
	State MessageState

	// Retries is the redeliveries so far, or -1 when the server reported a
	// value that did not parse.
	Retries int

	// Data is the payload, or nil when the request named a Writer.
	Data []byte
}

// Peek reads the message the handle names without moving it; the handle
// stays valid.
func (c *Client) Peek(ctx context.Context, req PeekRequest) (PeekResult, error) {
	c.init()
	if req.Spool == "" {
		return zero[PeekResult](), errSpoolRequired
	}
	if req.Handle == "" {
		return zero[PeekResult](), errHandleRequired
	}
	var buf *bytes.Buffer
	dst := req.Writer
	if dst == nil {
		buf = bytes.NewBuffer(nil)
		dst = buf
	}
	rt := RequestTrace{Op: OpPeek, Spool: req.Spool}
	res, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/peek",
		Header: http.Header{
			headerHandle: {string(req.Handle)},
		},
		Recv: dst,
		Codec: httputil.Codecs{
			Recv: httputil.StreamDecoder,
		},
	})
	if err != nil {
		return zero[PeekResult](), resultError(err)
	}
	retries, err := parseRetries(res.Header)
	if err != nil {
		c.Trace.onMalformedResponse(ctx, rt, err)
	}
	var state MessageState
	err = state.fromWire(res.Header.Get(headerMessageState))
	if err != nil {
		c.Trace.onMalformedResponse(ctx, rt, err)
	}
	ret := PeekResult{
		ID:      res.Header.Get(headerMessageID),
		State:   state,
		Retries: retries,
	}
	if buf != nil {
		ret.Data = buf.Bytes()
	}
	return ret, nil
}

// RecoverRequest describes a failed message to return to its queue.
type RecoverRequest struct {
	// Spool is the spool the message is in.
	Spool string

	// Handle is the failed message, from a listing.
	Handle Handle

	// DelaySeconds is an optional delay until the recovered message becomes
	// visible.
	// Zero means visible at once.
	DelaySeconds int
}

// Recover returns a failed message to its queue, after the request's delay
// if any.
func (c *Client) Recover(ctx context.Context, req RecoverRequest) error {
	c.init()

	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Handle == "" {
		return errHandleRequired
	}
	header := http.Header{
		headerHandle: {string(req.Handle)},
	}
	if d := req.DelaySeconds; d != 0 {
		header.Set(headerDelay, strconv.Itoa(d))
	}
	rt := RequestTrace{Op: OpRecover, Spool: req.Spool}
	_, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/recover",
		Header:   header,
	})
	return resultError(err)
}

// errorKindOf reports the kind the response named, if any.
func errorKindOf(e *httputil.StatusError) ErrorKind {
	obj, ok := e.Desc.(*errorObj)
	if !ok {
		return ErrorKindUnknown
	}
	return obj.kind()
}

// retry decides for settles and reads. It retries what the errors table at
// https://docs.spooler.sh/errors marks transient, and nothing else: that a
// retry would be harmless does not make a failure transient.
func retry(e *httputil.StatusError) bool {
	if errorKindOf(e) == ErrorKindQueueBusy {
		return true
	}
	switch e.Code {
	case
		http.StatusServiceUnavailable,
		http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}

// retrySend decides for the operations that append, per "Retrying a send" at
// https://docs.spooler.sh/errors.
func retrySend(hasDedup bool) func(*httputil.StatusError) bool {
	return func(e *httputil.StatusError) bool {
		if errorKindOf(e) == ErrorKindDedupInFlight {
			return true
		}
		switch e.Code {
		case
			http.StatusBadGateway,
			http.StatusGatewayTimeout,
			http.StatusInternalServerError:
			// Retry these only when dedup key is set.
			return hasDedup

		case
			http.StatusServiceUnavailable,
			http.StatusTooManyRequests:
			return true

		default:
			return false
		}
	}
}

func zero[T any](...T) (_ T) {
	return
}

// parseExpiresAt reads the lease expiry header. Absent is the zero time; an
// error names the header, for the malformed-response hook.
func parseExpiresAt(h http.Header) (t time.Time, err error) {
	str := h.Get(headerLeaseExpiresAt)
	if str == "" {
		return t, nil
	}
	t, err = time.Parse(time.RFC3339, str)
	if err != nil {
		return t, fmt.Errorf("%s: %w", headerLeaseExpiresAt, err)
	}
	return t, nil
}

// parseRetries reads the retries header. Absent is zero, like the expiry;
// present but unreadable is -1, so it is never mistaken for a count, and the
// error names the header, for the malformed-response hook.
func parseRetries(h http.Header) (int, error) {
	str := h.Get(headerMessageRetries)
	if str == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(str)
	if err != nil {
		return -1, fmt.Errorf("%s: %w", headerMessageRetries, err)
	}
	return n, nil
}

func sliceMapSet[K comparable, V any, M ~map[K][]V](m M, k K, vs ...V) M {
	if m == nil {
		m = make(M)
	}
	m[k] = vs
	return m
}
