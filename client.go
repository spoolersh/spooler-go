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
	"sync"
	"time"

	"github.com/spoolersh/spooler-go/internal/backoff"
	"github.com/spoolersh/spooler-go/internal/httputil"
)

// DefaultHost is the public API's host, used when [Client.Host] is empty.
const DefaultHost = "api.spooler.sh"

// DefaultAttempts is how many times an operation is tried when
// [Client.Attempts] is zero: the first request and four retries.
const DefaultAttempts = 5

const (
	headerAuthorization    = "Authorization"
	headerDedupHash        = "Spooler-Dedup-Hash"
	headerDedupString      = "Spooler-Dedup-String"
	headerDelay            = "Spooler-Delay-Seconds"
	headerHandle           = "Spooler-Handle"
	headerLease            = "Spooler-Lease"
	headerLeaseExpiresAt   = "Spooler-Lease-Expires-At"
	headerMessageCreatedAt = "Spooler-Message-Created-At"
	headerMessageExpiresAt = "Spooler-Message-Expires-At"
	headerMessageID        = "Spooler-Message-Id"
	headerMessageRetries   = "Spooler-Message-Retries"
	headerMessageState     = "Spooler-Message-State"
	headerMessageVisibleAt = "Spooler-Message-Visible-At"
	headerQueue            = "Spooler-Queue"
)

// Client implements Spooler API spec.
//
// It is safe for concurrent use and should be shared, since each one holds its
// own connection pool.
//
// Every operation retries the conditions the API documents as transient, up to
// [Client.Attempts] or [DefaultAttempts].
//
// An operation on a Client without [Client.APIKey] set is refused with
// [ErrInvalidRequest] before any request is made.
type Client struct {
	// Trace is an optional set of runtime hooks the client calls.
	Trace ClientTrace

	// Host specifies the API host, as "host" or "host:port".
	//
	// Zero means [DefaultHost].
	Host string

	// APIKey authenticates every request made by this Client.
	// Required.
	APIKey string

	// Attempts is how many times at most an operation is tried before its
	// error is returned, the initial request included. Only a failure the
	// docs class as transient and as having changed nothing is retried; one
	// that leaves the result unknown is returned at once, matching
	// [ErrResultUnknown], unless the request sets RetryUnknown, as
	// [SendRequest] and [AckAndSendRequest] can. An application with a retry
	// policy of its own can set 1.
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

	once    sync.Once
	initErr error
	http    *httputil.Client
}

// Clone returns a copy of c with its settings and none of its connections;
// the copy opens its own on first use. Use it to run several clients from
// one set of settings, since a Client must not be copied by value.
func (c *Client) Clone() *Client {
	return &Client{
		Trace:              c.Trace,
		Host:               c.Host,
		APIKey:             c.APIKey,
		Attempts:           c.Attempts,
		InsecureDisableTLS: c.InsecureDisableTLS,
		InsecureDebug:      c.InsecureDebug,
		transport:          c.transport,
	}
}

func (c *Client) init() error {
	c.once.Do(func() {
		if c.APIKey == "" {
			// NOTE: "Bearer " is not a valid Bearer credential as per RFC
			// 6750, the token needs to be at least one character.
			c.initErr = errAPIKeyRequired
			return
		}
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
			// With DefaultAttempts the retries wait about 150ms, 450ms,
			// 1.35s and 4.05s, each ±25%: about 6s in all, 4.5s to 7.5s.
			Backoff: &backoff.Exponential{
				Base:   150 * time.Millisecond,
				Factor: 3,
				Jitter: 0.25,
				Limit:  5 * time.Second,
			},
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
	return c.initErr
}

func (c *Client) attempts() int {
	return cmp.Or(c.Attempts, DefaultAttempts)
}

func (c *Client) do(ctx context.Context, rt RequestTrace, method string, req httputil.Request) (res httputil.Response, err error) {
	done := c.Trace.onRequest(ctx, rt)
	defer func() {
		done(err)
	}()
	res, err = c.http.Do(ctx, method, req)
	if err != nil {
		return res, rpcError(err)
	}
	return res, nil
}

func (c *Client) doData(ctx context.Context, rt RequestTrace, method string, req httputil.Request) (res httputil.Response, done func(error), err error) {
	done = c.Trace.onRequest(ctx, rt)
	res, err = c.http.Do(ctx, method, req)
	if err != nil {
		err = rpcError(err)
		done(err)
		return res, nil, err
	}
	return res, done, nil
}

func rpcError(err error) error {
	var conds []error
	if cond := statusCondition(err); cond != nil {
		conds = append(conds, cond)
	}
	if resultUnknown(err) {
		conds = append(conds, ErrResultUnknown)
	}
	return &clientError{
		err:   httpError(err),
		conds: conds,
	}
}

// dataStream is a payload handed back unread. It closes the transport's body
// on EOF, so that a full read returns the connection without a Close. A
// payload cut short reads as io.ErrUnexpectedEOF, as io.ReadFull reports
// one; any other failure is the SDK's error. The first of EOF, a failure or
// Close ends the operation, with the failure if any, or the close's.
type dataStream struct {
	body io.ReadCloser
	done func(error) // Ends the operation.
	once sync.Once
	err  error
}

func (s *dataStream) Read(p []byte) (n int, err error) {
	n, err = s.body.Read(p)
	if err == nil {
		return n, nil
	}
	defer func() {
		s.close(err)
	}()
	switch {
	case errors.Is(err, io.EOF):
		// The literal sentinels: io.ReadAll and its kin compare with ==.
		err = io.EOF
	case errors.Is(err, io.ErrUnexpectedEOF):
		err = io.ErrUnexpectedEOF
	default:
		err = resultError(err)
	}
	return n, err
}

func (s *dataStream) Close() error {
	s.close(nil)
	return s.err
}

func (s *dataStream) close(err error) {
	s.once.Do(func() {
		s.err = resultError(s.body.Close())
		if s.done != nil {
			if err == io.EOF {
				err = nil // A clean end is not a failure.
			}
			if err == nil {
				err = s.err // A close that fails is one.
			}
			s.done(err)
		}
	})
}

// SpoolRequest describes a spool to read.
type SpoolRequest struct {
	Spool string
}

// Spool reads a spool: its queue count and its message counts summed across
// every queue.
func (c *Client) Spool(ctx context.Context, req SpoolRequest) (Spool, error) {
	if err := c.init(); err != nil {
		return zero[Spool](), err
	}
	if req.Spool == "" {
		return zero[Spool](), errSpoolRequired
	}
	var ret spool
	rt := RequestTrace{
		Op:    OpSpool,
		Spool: req.Spool,
	}
	_, err := c.do(ctx, rt, "GET", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool,
		Recv:     &ret,
	})
	if err != nil {
		return zero[Spool](), resultError(err)
	}
	return ret.toSpool(), nil
}

// QueuesRequest describes a spool's queues to list, and their pagination.
type QueuesRequest struct {
	Spool string

	// Match is an optional filter keeping on the page only the queues whose
	// names the expression matches, unanchored.
	//
	// Zero filters nothing.
	Match *regexp.Regexp

	// Limit is an optional cap on the queues on the page.
	//
	// Zero means the server's default.
	Limit int

	// After is an optional cursor resuming the page from the previous page's
	// Next.
	//
	// Zero starts from the first page.
	After string
}

// Queues lists one page of a spool's queues, each with its settings and its
// message counts.
func (c *Client) Queues(ctx context.Context, req QueuesRequest) (Page[Queue], error) {
	if err := c.init(); err != nil {
		return zero[Page[Queue]](), err
	}
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
	rt := RequestTrace{
		Op:    OpQueues,
		Spool: req.Spool,
	}
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

// MessagesRequest describes a queue's messages to list, and their
// pagination.
type MessagesRequest struct {
	Spool string
	Queue string

	// State is an optional filter keeping only messages in this state on
	// the page.
	//
	// Zero keeps messages of every state on the page.
	State MessageState

	// Limit is an optional cap on the messages on the page.
	//
	// Zero means the server's default.
	Limit int

	// After is an optional cursor resuming the page just past this message
	// id, from the previous page's Next.
	//
	// Zero starts from the first page.
	After string

	// ReturnHandle is an optional request for a handle on each message, for
	// [Client.Inspect], [Client.Recover] and [Client.Discard].
	//
	// A handle is a credential, so it is off by default.
	ReturnHandle bool
}

// Messages lists one page of a queue's messages, without their payloads.
func (c *Client) Messages(ctx context.Context, req MessagesRequest) (Page[Message], error) {
	if err := c.init(); err != nil {
		return zero[Page[Message]](), err
	}
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
	rt := RequestTrace{
		Op:    OpMessages,
		Spool: req.Spool,
		Queue: req.Queue,
	}
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
	Spool string
	Queue string

	// Settings are optional settings to apply.
	//
	// Zero means the server's defaults.
	Settings QueueSettingsCreate
}

// CreateQueue creates a queue and returns it as created; one that already
// exists is [ErrQueueExists]. The returned UID names this queue even if
// another is created under the name since.
//
//	// Ensure a queue "foo" exists.
//	_, err := c.CreateQueue(ctx, spooler.CreateQueueRequest{
//		Spool: "default",
//		Queue: "foo",
//	})
//	if errors.Is(err, spooler.ErrQueueExists) {
//		err = nil
//	}
//	if err != nil {
//		// Handle error.
//	}
func (c *Client) CreateQueue(ctx context.Context, req CreateQueueRequest) (Queue, error) {
	if err := c.init(); err != nil {
		return zero[Queue](), err
	}
	if req.Spool == "" {
		return zero[Queue](), errSpoolRequired
	}
	if req.Queue == "" {
		return zero[Queue](), errQueueRequired
	}
	var ret queue
	rt := RequestTrace{
		Op:    OpCreateQueue,
		Spool: req.Spool,
		Queue: req.Queue,
	}
	_, err := c.do(ctx, rt, "PUT", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue,
		Send:     req.Settings.wire(),
		Recv:     &ret,
	})
	if err != nil {
		return zero[Queue](), resultError(err)
	}
	return ret.toQueue(), nil
}

// QueueRequest describes a queue to read.
type QueueRequest struct {
	Spool string
	Queue string
}

// Queue reads a queue: its state, its settings and its message counts.
func (c *Client) Queue(ctx context.Context, req QueueRequest) (Queue, error) {
	if err := c.init(); err != nil {
		return zero[Queue](), err
	}
	if req.Spool == "" {
		return zero[Queue](), errSpoolRequired
	}
	if req.Queue == "" {
		return zero[Queue](), errQueueRequired
	}
	var ret queue
	rt := RequestTrace{
		Op:    OpQueue,
		Spool: req.Spool,
		Queue: req.Queue,
	}
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
	Spool string
	Queue string

	// QueueUID is an optional uid of the queue to update, as [Queue.UID]
	// reports it. Another queue under the name is refused with
	// [ErrQueueReplaced], and nothing is changed.
	//
	// Zero updates whatever queue has the name.
	QueueUID *string

	// Settings are the settings to replace.
	// At least one Settings's field must be set.
	Settings QueueSettingsUpdate
}

// UpdateQueue updates queue settings.
func (c *Client) UpdateQueue(ctx context.Context, req UpdateQueueRequest) error {
	if err := c.init(); err != nil {
		return err
	}
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Queue == "" {
		return errQueueRequired
	}
	var query url.Values
	if uid := req.QueueUID; uid != nil {
		query = sliceMapSet(query, "uid", *uid)
	}
	rt := RequestTrace{
		Op:    OpUpdateQueue,
		Spool: req.Spool,
		Queue: req.Queue,
	}
	_, err := c.do(ctx, rt, "PATCH", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue,
		Query:    query,
		Send:     req.Settings.wire(),
	})
	return resultError(err)
}

// DeleteQueueRequest describes a queue to delete and how.
type DeleteQueueRequest struct {
	Spool string
	Queue string

	// QueueUID is an optional uid of the queue to delete, as [Queue.UID]
	// reports it. Another queue under the name is refused with
	// [ErrQueueReplaced], and nothing is changed, so a retry cannot delete a
	// queue created under the name since.
	//
	// Zero deletes whatever queue has the name.
	QueueUID *string

	Mode QueueDeleteMode
}

// DeleteQueue deletes a queue, per the request's Mode. When the call returns
// the deletion is accepted, not completed: the queue reports
// [QueueStateDeleting] or [QueueStateDraining] until it is gone.
func (c *Client) DeleteQueue(ctx context.Context, req DeleteQueueRequest) error {
	if err := c.init(); err != nil {
		return err
	}
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
	query := url.Values{
		"mode": []string{mode},
	}
	if uid := req.QueueUID; uid != nil {
		query.Set("uid", *uid)
	}
	rt := RequestTrace{
		Op:    OpDeleteQueue,
		Spool: req.Spool,
		Queue: req.Queue,
	}
	_, err = c.do(ctx, rt, "DELETE", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue,
		Query:    query,
	})
	return resultError(err)
}

// Dedup is a send's deduplication key, from [DedupString] or [DedupHash];
// the rules are on https://docs.spooler.sh/dedup.
//
// Zero means no dedup key.
type Dedup struct {
	kind dedupKind
	key  string
}

// dedupKind is which of the two constructors made a Dedup.
type dedupKind uint

const (
	dedupNone dedupKind = iota
	dedupString
	dedupHash
)

// DedupString keys a send by a string; the server hashes it.
func DedupString(str string) Dedup {
	return Dedup{
		kind: dedupString,
		key:  str,
	}
}

// DedupHash keys a send by a hash the caller computed, hex-encoded, per the
// recipe on https://docs.spooler.sh/dedup.
func DedupHash(h string) Dedup {
	return Dedup{
		kind: dedupHash,
		key:  h,
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
	reader struct {
		io.Reader
	}
)

// DataSource is a streamed payload for a send, from [DataSourceReader] or
// [DataSourceReadSeeker]. The zero value is no source.
type DataSource struct {
	r io.Reader
}

// IsZero reports whether no source is set.
func (d DataSource) IsZero() bool {
	return d.r == nil
}

// DataSourceReader sends r to its end, streamed without a length. The reader
// is read once, so a send that fails with an answer that may be retried is not
// retried. Use [DataSourceReadSeeker] for a source that can be sent again.
//
// Only Read is called on r, even if r implements other interfaces.
func DataSourceReader(r io.Reader) DataSource {
	return DataSource{
		r: reader{r},
	}
}

// DataSourceReadSeeker sends rs from its current offset to its end, and
// rewinds it to send again when a retry is due. To send part of a seekable
// source, pass an [io.SectionReader].
//
// Only Read and Seek are called on r, even if rs implements other interfaces.
func DataSourceReadSeeker(rs io.ReadSeeker) DataSource {
	return DataSource{
		r: readSeeker{rs},
	}
}

// SendRequest describes a message to append to a queue.
type SendRequest struct {
	Spool string
	Queue string

	// DelaySeconds is an optional delay until the message becomes visible.
	//
	// Zero means visible at once.
	DelaySeconds int

	// Dedup is an optional deduplication key.
	Dedup Dedup

	// Data is an optional payload.
	//
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

	// RetryUnknown is an optional request to retry an attempt whose result is
	// unknown, within the attempts [Client.Attempts] allows. Such a retry can
	// append the message a second time, as a new message with its own id:
	// consumers whose effect is idempotent absorb it, those that only track
	// handled message ids do not. With Dedup, a retry the queue's dedup window
	// still remembers appends nothing new. A payload from [DataSourceReader] is
	// still sent once.
	//
	// Zero retries no attempt whose result is unknown.
	RetryUnknown bool
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
	switch d.kind {
	case dedupString:
		name = headerDedupString
	case dedupHash:
		name = headerDedupHash
	}
	return name, d.key, d.kind != dedupNone
}

// Send appends a message to a queue.
//
// Message becomes visible to receivers at once, or if set after the request's
// delay.
func (c *Client) Send(ctx context.Context, req SendRequest) (SendResult, error) {
	if err := c.init(); err != nil {
		return zero[SendResult](), err
	}

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
	retry := retrySend
	if req.RetryUnknown {
		retry = retrySendUnknown
	}
	rt := RequestTrace{
		Op:    OpSend,
		Spool: req.Spool,
		Queue: req.Queue,
	}
	res, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
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
	Spool string

	// Lease is the delivery to ack. It names the queue it came from.
	Lease Lease

	// Queue is the queue to append the new message to.
	Queue string

	// DelaySeconds is an optional delay until the new message becomes
	// visible.
	//
	// Zero means visible at once.
	DelaySeconds int

	// Dedup is an optional deduplication key for the new message.
	Dedup Dedup

	// Data is the new message's optional payload.
	//
	// Zero sends an empty message.
	Data []byte

	// DataSource is an optional stream to send as the payload instead of Data.
	// Setting both is an error.
	DataSource DataSource

	// ReturnHandle is an optional request for a handle to the new message.
	//
	// A handle is a credential, so it is off by default.
	ReturnHandle bool

	// RetryUnknown is an optional request to retry an attempt whose result is
	// unknown, within the attempts [Client.Attempts] allows. Unlike
	// [SendRequest.RetryUnknown], such a retry never appends twice, with or
	// without a dedup key: one after a call that committed fails with
	// [ErrStaleToken] and appends nothing. The error then also matches
	// [ErrResultUnknown], since that answer does not prove the first attempt
	// succeeded; check it first. A payload from [DataSourceReader] is still
	// sent once.
	//
	// Zero retries no attempt whose result is unknown.
	RetryUnknown bool
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
// committed fails with [ErrStaleToken] and appends nothing.
func (c *Client) AckAndSend(ctx context.Context, req AckAndSendRequest) (AckAndSendResult, error) {
	if err := c.init(); err != nil {
		return zero[AckAndSendResult](), err
	}
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
	retry := retrySend
	if req.RetryUnknown {
		retry = retrySendUnknown
	}
	rt := RequestTrace{
		Op:    OpAckAndSend,
		Spool: req.Spool,
		Queue: req.Queue,
	}
	res, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
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
	Spool string
	Queue string

	// WaitSeconds is an optional bound on the long poll; a value of 0
	// returns without waiting.
	//
	// Zero means the server's default.
	WaitSeconds *int

	// Stream is an optional request to return the payload unread as
	// [RecvResult.DataStream], for the caller to read.
	//
	// If set, Recv then returns as soon as the message is leased.
	//
	// For [ClientTrace.OnRequest], the operation ends when the stream is read
	// to its end or closed.
	//
	// Zero reads the payload into [RecvResult.Data].
	Stream bool
}

// RecvResult is the outcome of a receive: the message and the lease on it.
type RecvResult struct {
	// ID is the message id. It identifies the message in listings and logs.
	// No operation takes it.
	ID string

	// Retries is the redeliveries so far, or -1 when the server reported a
	// value that did not parse.
	Retries int

	// CreatedAt is when the message was sent.
	CreatedAt time.Time

	Lease Lease

	// LeaseExpiresAt is when the lease expires.
	//
	// Zero means the lease never expires.
	LeaseExpiresAt time.Time

	// Data is the message payload. It is nil when the request set Stream,
	// and when the message was sent without one.
	Data []byte

	// DataStream is the unread payload stream, set when the request had
	// Stream set (empty messages included).
	//
	// Reading is bounded by the receive's context. A payload cut short reads
	// as [io.ErrUnexpectedEOF], and the lease stays the caller's to settle.
	//
	// Users must close it as soon as the data is read.
	DataStream io.ReadCloser

	// DataSize is the payload's size in bytes as the server declared it, or
	// -1 when it did not.
	DataSize int64
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

// Recv leases the next message of the queue. When none becomes visible within
// the wait, Recv reports ok as false, with a zero result and no error.
//
// When a message was leased but its payload could not be read, Recv returns
// the result with the lease and ok as true, and the error: the lease is the
// caller's to settle. Data then holds what was read before the failure.
func (c *Client) Recv(ctx context.Context, req RecvRequest) (ret RecvResult, ok bool, err error) {
	if err := c.init(); err != nil {
		return zero[RecvResult](), false, err
	}

	if req.Spool == "" {
		return ret, false, errSpoolRequired
	}
	if req.Queue == "" {
		return ret, false, errQueueRequired
	}

	var query url.Values
	if w := req.WaitSeconds; w != nil {
		if *w < 0 {
			return ret, false, invalidRequest(fmt.Sprintf(
				"WaitSeconds: %d is negative", *w,
			))
		}
		query = sliceMapSet(query, "waitSeconds", strconv.Itoa(*w))
	}
	// The body is always taken as a stream, so that the operation ends when
	// the payload has been read, buffered or not.
	rq := httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/queues/" + req.Queue + "/recv",
		Query:    query,
		Stream:   true,
		Codec: httputil.Codecs{
			Recv: httputil.StreamDecoder,
		},
	}
	rt := RequestTrace{
		Op:    OpRecv,
		Spool: req.Spool,
		Queue: req.Queue,
	}
	res, done, err := c.doData(ctx, rt, "POST", rq)
	if err != nil {
		return ret, false, err
	}
	if res.Code == http.StatusOK {
		ret = c.parseRecvResult(ctx, rt, res)
		ret.Data, ret.DataStream, err = handleDataOK(res, req.Stream, done)
		return ret, true, err
	}
	res.Body.Close()
	switch res.Code {
	case http.StatusNoContent:
		done(nil)
		return ret, false, nil

	default:
		err := resultError(fmt.Errorf(
			"unexpected success status: %d",
			res.Code,
		))
		done(err)
		return ret, false, err
	}
}

func (c *Client) parseRecvResult(
	ctx context.Context,
	rt RequestTrace,
	res httputil.Response,
) RecvResult {
	expiresAt, err := parseTimeHeader(res.Header, headerLeaseExpiresAt)
	if err != nil {
		c.Trace.onMalformedResponse(ctx, rt, err)
	}
	retries, err := parseRetries(res.Header)
	if err != nil {
		c.Trace.onMalformedResponse(ctx, rt, err)
	}
	createdAt, err := parseTimeHeader(res.Header, headerMessageCreatedAt)
	if err != nil {
		c.Trace.onMalformedResponse(ctx, rt, err)
	}
	return RecvResult{
		ID:             res.Header.Get(headerMessageID),
		Retries:        retries,
		CreatedAt:      createdAt,
		Lease:          Lease(res.Header.Get(headerLease)),
		LeaseExpiresAt: expiresAt,
		DataSize:       res.ContentLength,
	}
}

// RenewRequest describes a lease to renew.
type RenewRequest struct {
	Spool string
	Lease Lease
}

// RenewResult is the outcome of a renew.
type RenewResult struct {
	// LeaseExpiresAt is the new expiry.
	//
	// Zero means the lease never expires.
	LeaseExpiresAt time.Time
}

// Renew resets the lease's expiry and reports the new one. The token stays
// the same.
func (c *Client) Renew(ctx context.Context, req RenewRequest) (RenewResult, error) {
	if err := c.init(); err != nil {
		return zero[RenewResult](), err
	}

	if req.Spool == "" {
		return zero[RenewResult](), errSpoolRequired
	}
	if req.Lease == "" {
		return zero[RenewResult](), errLeaseRequired
	}
	header := http.Header{
		headerLease: {string(req.Lease)},
	}
	rt := RequestTrace{
		Op:    OpRenew,
		Spool: req.Spool,
	}
	res, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/renew",
		Header:   header,
	})
	if err != nil {
		return zero[RenewResult](), resultError(err)
	}
	expiresAt, err := parseTimeHeader(res.Header, headerLeaseExpiresAt)
	if err != nil {
		c.Trace.onMalformedResponse(ctx, rt, err)
	}
	return RenewResult{
		LeaseExpiresAt: expiresAt,
	}, nil
}

// AckRequest describes a delivery to ack.
type AckRequest struct {
	Spool string
	Lease Lease
}

// Ack acks the delivery: the message is removed.
func (c *Client) Ack(ctx context.Context, req AckRequest) error {
	if err := c.init(); err != nil {
		return err
	}
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Lease == "" {
		return errLeaseRequired
	}
	header := http.Header{
		headerLease: {string(req.Lease)},
	}
	rt := RequestTrace{
		Op:    OpAck,
		Spool: req.Spool,
	}
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
	Spool string
	Lease Lease
}

// Nack returns the message for redelivery, counting a retry. Past the queue's
// max retries the message is failed instead.
func (c *Client) Nack(ctx context.Context, req NackRequest) error {
	if err := c.init(); err != nil {
		return err
	}
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Lease == "" {
		return errLeaseRequired
	}
	header := http.Header{
		headerLease: {string(req.Lease)},
	}
	rt := RequestTrace{
		Op:    OpNack,
		Spool: req.Spool,
	}
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
	Spool string
	Lease Lease

	// DelaySeconds is an optional delay until the message becomes visible
	// again.
	//
	// Zero means visible at once.
	DelaySeconds int
}

// Release returns the message for redelivery without counting a retry,
// after the request's delay if any.
func (c *Client) Release(ctx context.Context, req ReleaseRequest) error {
	if err := c.init(); err != nil {
		return err
	}
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
	rt := RequestTrace{
		Op:    OpRelease,
		Spool: req.Spool,
	}
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
	Spool string
	Lease Lease
}

// Fail marks the message failed.
func (c *Client) Fail(ctx context.Context, req FailRequest) error {
	if err := c.init(); err != nil {
		return err
	}
	if req.Spool == "" {
		return errSpoolRequired
	}
	if req.Lease == "" {
		return errLeaseRequired
	}
	header := http.Header{
		headerLease: {string(req.Lease)},
	}
	rt := RequestTrace{
		Op:    OpFail,
		Spool: req.Spool,
	}
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
	Spool string

	// Lease is the delivery to discard, from a receive.
	// Set exactly one of Lease and Handle.
	Lease Lease

	// Handle is the message to discard, from a send or a listing.
	Handle Handle
}

// Discard drops the message, by its lease or its handle, exactly one.
func (c *Client) Discard(ctx context.Context, req DiscardRequest) error {
	if err := c.init(); err != nil {
		return err
	}
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
	rt := RequestTrace{
		Op:    OpDiscard,
		Spool: req.Spool,
	}
	_, err := c.do(ctx, rt, "POST", httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/discard",
		Header:   header,
	})
	return resultError(err)
}

// InspectRequest describes a message to read, by its handle.
type InspectRequest struct {
	Spool string

	// Handle is the message to read, from a listing or a send.
	Handle Handle

	// Stream is an optional request to return the payload unread as
	// [InspectResult.DataStream], for the caller to read.
	//
	// For [ClientTrace.OnRequest], the operation ends when the stream is read
	// to its end or closed.
	//
	// Zero reads the payload into [InspectResult.Data].
	Stream bool
}

// InspectResult is the outcome of an inspect: the message as it is now.
type InspectResult struct {
	// ID is the message id. It identifies the message in listings and logs.
	// No operation takes it.
	ID string

	State MessageState

	// Retries is the redeliveries so far, or -1 when the server reported a
	// value that did not parse.
	Retries int

	// CreatedAt is when the message was sent.
	CreatedAt time.Time

	// VisibleAt is when a delayed message becomes visible.
	//
	// Zero means the message is not delayed.
	VisibleAt time.Time

	// LeaseExpiresAt is when the current lease expires.
	//
	// Zero means no lease, or one that never expires.
	LeaseExpiresAt time.Time

	// ExpiresAt is when retention discards the message.
	//
	// Zero means retention is off.
	ExpiresAt time.Time

	// Data is the payload. It is nil when the request set Stream, and when
	// the message was sent without one.
	Data []byte

	// DataStream is the unread payload stream, set when the request had
	// Stream set (empty messages included).
	//
	// Reading is bounded by the inspect's context. A payload cut short reads
	// as [io.ErrUnexpectedEOF].
	//
	// Users must close it as soon as the data is read.
	DataStream io.ReadCloser

	// DataSize is the payload's size in bytes as the server declared it, or
	// -1 when it did not.
	DataSize int64
}

// Inspect reads the message the handle names without moving it; the handle
// stays valid.
//
// When the message was read but its payload could not be, Inspect returns
// the result and the error. Data then holds what was read before the
// failure.
func (c *Client) Inspect(ctx context.Context, req InspectRequest) (InspectResult, error) {
	if err := c.init(); err != nil {
		return zero[InspectResult](), err
	}
	if req.Spool == "" {
		return zero[InspectResult](), errSpoolRequired
	}
	if req.Handle == "" {
		return zero[InspectResult](), errHandleRequired
	}
	rt := RequestTrace{
		Op:    OpInspect,
		Spool: req.Spool,
	}
	rq := httputil.Request{
		Attempts: c.attempts(),
		Retry:    retry,
		Path:     "/spools/" + req.Spool + "/inspect",
		Header: http.Header{
			headerHandle: {string(req.Handle)},
		},
		Stream: true,
		Codec: httputil.Codecs{
			Recv: httputil.StreamDecoder,
		},
	}
	res, done, err := c.doData(ctx, rt, "POST", rq)
	if err != nil {
		return zero[InspectResult](), err
	}
	if res.Code == http.StatusOK {
		ret := c.parseInspectResult(ctx, rt, res)
		ret.Data, ret.DataStream, err = handleDataOK(res, req.Stream, done)
		return ret, err
	}
	res.Body.Close()
	err = resultError(fmt.Errorf(
		"unexpected success status: %d",
		res.Code,
	))
	done(err)
	return zero[InspectResult](), err
}

func handleDataOK(
	res httputil.Response,
	stream bool,
	done func(error),
) (
	p []byte,
	rc io.ReadCloser,
	err error,
) {
	if stream {
		return nil, &dataStream{
			body: res.Body,
			done: done,
		}, nil
	}
	var buf bytes.Buffer
	_, err = io.Copy(&buf, res.Body)
	if cerr := res.Body.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		// The payload could not be read. The message metadata is still
		// returned: for a receive, the lease is the caller's to settle.
		err = &clientError{
			err: err,
			conds: []error{
				ErrResultUnknown,
			},
		}
	}
	done(err)
	return buf.Bytes(), nil, err
}

func (c *Client) parseInspectResult(
	ctx context.Context,
	rt RequestTrace,
	res httputil.Response,
) InspectResult {
	retries, err := parseRetries(res.Header)
	if err != nil {
		c.Trace.onMalformedResponse(ctx, rt, err)
	}
	var state MessageState
	err = state.fromWire(res.Header.Get(headerMessageState))
	if err != nil {
		c.Trace.onMalformedResponse(ctx, rt, err)
	}
	ret := InspectResult{
		ID:       res.Header.Get(headerMessageID),
		State:    state,
		Retries:  retries,
		DataSize: res.ContentLength,
	}
	for _, h := range []struct {
		name string
		dst  *time.Time
	}{
		{
			name: headerMessageCreatedAt,
			dst:  &ret.CreatedAt,
		},
		{
			name: headerMessageVisibleAt,
			dst:  &ret.VisibleAt,
		},
		{
			name: headerLeaseExpiresAt,
			dst:  &ret.LeaseExpiresAt,
		},
		{
			name: headerMessageExpiresAt,
			dst:  &ret.ExpiresAt,
		},
	} {
		t, err := parseTimeHeader(res.Header, h.name)
		if err != nil {
			c.Trace.onMalformedResponse(ctx, rt, err)
		}
		*h.dst = t
	}
	return ret
}

// RecoverRequest describes a failed message to return to its queue.
type RecoverRequest struct {
	Spool string

	// Handle is the failed message, from a listing.
	Handle Handle

	// DelaySeconds is an optional delay until the recovered message becomes
	// visible.
	//
	// Zero means visible at once.
	DelaySeconds int
}

// Recover returns a failed message to its queue, after the request's delay
// if any.
func (c *Client) Recover(ctx context.Context, req RecoverRequest) error {
	if err := c.init(); err != nil {
		return err
	}

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
	rt := RequestTrace{
		Op:    OpRecover,
		Spool: req.Spool,
	}
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
func retry(err error) bool {
	e, ok := errors.AsType[*httputil.StatusError](err)
	if !ok {
		return false
	}
	switch errorKindOf(e) {
	case
		ErrorKindQueueBusy,
		ErrorKindQueueCreating:

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

// retrySend decides for the operations that append, per "Retrying a change"
// at https://docs.spooler.sh/errors. Like retry, it retries only what changed
// nothing: a result left unknown is the caller's to retry, with its dedup key.
func retrySend(err error) bool {
	e, ok := errors.AsType[*httputil.StatusError](err)
	if !ok {
		return false
	}
	if errorKindOf(e) == ErrorKindDedupInFlight {
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

// retrySendUnknown decides for an append whose request sets RetryUnknown: what
// retrySend retries, and an attempt whose result is unknown.
func retrySendUnknown(err error) bool {
	return retrySend(err) || attemptUnknown(err)
}

// resultUnknown reports whether any attempt of a call leaves open that the
// operation's change happened, per "Retrying a change" at
// https://docs.spooler.sh/errors. Once one did, no later failure settles it;
// only a success does, and a success is no error.
func resultUnknown(err error) bool {
	attempts, ok := errors.AsType[httputil.AttemptErrors](err)
	if !ok {
		return false
	}
	for _, err := range attempts {
		if attemptUnknown(err) {
			return true
		}
	}
	return false
}

// attemptUnknown reports whether one attempt's failure leaves open that the
// change happened.
func attemptUnknown(err error) bool {
	switch e := err.(type) {
	case *httputil.StatusError:
		switch {
		case e.Code < 500:
			return false

		case
			e.Code == http.StatusInsufficientStorage,
			e.Code == http.StatusServiceUnavailable:

			// The docs answer these before the change is attempted.
			return false

		default:
			// Any other 5xx, an unlisted one included, may have changed:
			// an unknown status is read by its class.
			return true
		}

	case *httputil.TransportError:
		return e.Sent

	case *httputil.BodyError:
		// The server answered with success; only the answer was lost.
		return true

	default:
		return false
	}
}

func zero[T any](...T) (_ T) {
	return
}

// parseTimeHeader reads an RFC 3339 header. Absent is the zero time.
// An error names the header it tried to parse and wraps the error.
func parseTimeHeader(h http.Header, name string) (t time.Time, err error) {
	str := h.Get(name)
	if str == "" {
		return t, nil
	}
	t, err = time.Parse(time.RFC3339, str)
	if err != nil {
		return t, fmt.Errorf("%s: %w", name, err)
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
