package spooler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/spoolersh/spooler-go/internal/httputil"
)

// Error is an error the API answered: an answer that names a kind, carries a
// message, or both. Every operation returns one, wrapped, for such an answer.
// An answer that does neither, such as a gateway's, is not the API's and is
// returned as a plain error whose text is its status and body.
//
// You can reach it with [errors.As], or match its kind directly with
// [errors.Is] against the Err sentinels, as errors.Is(err, [ErrQueueNotFound]).
// Its [error.Error] message is for a human only -- a program acts on the kind
// and the details.
type Error struct {
	// Kind is what went wrong, as the API names it. It is ErrorKindUnknown
	// for an answer without a kind and for a kind this SDK does not know.
	Kind ErrorKind

	// Message is the server's text, complete on its own.
	Message string

	// Details is the facts the message states, as a type fixed per kind,
	// such as [QueueNotFoundError]; reach it with [errors.As].
	//
	// Zero means the kind has no details.
	Details error
}

// Error returns the server's message, which is complete on its own and already
// states what Details holds as fields. Without a message it returns the
// details' text, and failing that the kind's words.
func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Details != nil {
		return e.Details.Error()
	}
	if str, has := errorKind2string[e.Kind]; has {
		return str
	}
	return "unknown error"
}

// Is reports whether x is the [KindError] of e's kind, so that
// errors.Is(err, ErrQueueNotFound) matches every queue-not-found answer.
func (e *Error) Is(x error) bool {
	if k, ok := x.(*KindError); ok {
		return e.Kind == k.Kind
	}
	return false
}

// Unwrap returns the details, so that [errors.As] reaches them.
func (e *Error) Unwrap() error {
	return e.Details
}

// ErrResultUnknown matches a failure that leaves the result unknown: an answer
// the docs class as "may have changed", or no answer once the request may have
// reached the server. For an operation that changes state, the change may have
// happened anyway; what a retry then does depends on the operation, see
// "Retrying a change" at https://docs.spooler.sh/errors.
//
// It describes this call, the retries [Client.Attempts] allows included: it
// matches if any attempt left the result unknown, whatever a later attempt
// answered. The client retries such an attempt only when the request sets
// RetryUnknown ([SendRequest], [AckAndSendRequest]); otherwise whether to
// retry is the caller's decision.
var ErrResultUnknown = errors.New("result unknown")

// clientError is what every operation returns on failure: the cause, prefixed
// with the package name, exposing through Unwrap only what is contract.
type clientError struct {
	err error

	// conds are the conditions the failure matches beyond its kind:
	// ErrResultUnknown and ErrUnavailable.
	conds []error
}

func (e *clientError) Error() string {
	var sb strings.Builder
	sb.WriteString("spooler: ")
	for _, c := range e.conds {
		if c == ErrResultUnknown {
			sb.WriteString(c.Error())
			sb.WriteString(": ")
		}
	}
	sb.WriteString(e.err.Error())
	return sb.String()
}

// Unwrap exposes only what is contract: the API's [Error], a request the SDK
// refused as [ErrInvalidRequest], the context errors by their sentinel, and
// the conditions, such as [ErrResultUnknown] and [ErrUnavailable]. Anything
// else in the chain (a *url.Error, say) stays reachable as text only.
func (e *clientError) Unwrap() []error {
	var errs []error
	if x := e.contract(); x != nil {
		errs = append(errs, x)
	}
	return append(errs, e.conds...)
}

// contract returns the contract error the cause carries, or nil.
func (e *clientError) contract() error {
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

// resultError returns an operation's failure as a clientError: one already
// is passes through, anything else is wrapped; nil stays nil.
func resultError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*clientError); ok {
		return err
	}
	return &clientError{
		err: err,
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
	errAPIKeyRequired    = invalidRequest("api key is required")
	errSpoolRequired     = invalidRequest("spool name is required")
	errQueueRequired     = invalidRequest("queue name is required")
	errDataAndDataSource = invalidRequest("either Data or DataSource is required, not both")
	errLeaseRequired     = invalidRequest("lease is required")
	errLeaseAndHandle    = invalidRequest("either lease or handle is required, not both")
	errLeaseOrHandle     = invalidRequest("either lease or handle is required")
	errHandleRequired    = invalidRequest("handle is required")
)

// ErrorKind is what an [Error] reports went wrong: the API's kinds, as
// https://docs.spooler.sh/errors lists them, and nothing else. A 503 with no
// known kind is no kind but a sentinel of its own, [ErrUnavailable].
type ErrorKind uint

const (
	// ErrorKindUnknown is a kind this SDK does not know, or none.
	ErrorKindUnknown ErrorKind = iota
	// ErrorKindInvalidToken is the API's invalid_token kind.
	ErrorKindInvalidToken
	// ErrorKindDedupDisabled is the API's dedup_disabled kind.
	ErrorKindDedupDisabled
	// ErrorKindUnknownHeader is the API's unknown_header kind.
	ErrorKindUnknownHeader
	// ErrorKindRetentionLimit is the API's retention_limit kind.
	ErrorKindRetentionLimit
	// ErrorKindQueueLimit is the API's queue_limit kind.
	ErrorKindQueueLimit
	// ErrorKindQueueNotFound is the API's queue_not_found kind.
	ErrorKindQueueNotFound
	// ErrorKindSpoolNotFound is the API's spool_not_found kind.
	ErrorKindSpoolNotFound
	// ErrorKindQueueExists is the API's queue_exists kind.
	ErrorKindQueueExists
	// ErrorKindQueueCreating is the API's queue_creating kind.
	ErrorKindQueueCreating
	// ErrorKindQueueDeleting is the API's queue_deleting kind.
	ErrorKindQueueDeleting
	// ErrorKindQueueBusy is the API's queue_busy kind.
	ErrorKindQueueBusy
	// ErrorKindNotFailed is the API's not_failed kind.
	ErrorKindNotFailed
	// ErrorKindDedupInFlight is the API's dedup_in_flight kind.
	ErrorKindDedupInFlight
	// ErrorKindDedupClaimed is the API's dedup_claimed kind.
	ErrorKindDedupClaimed
	// ErrorKindStaleToken is the API's stale_token kind.
	ErrorKindStaleToken
	// ErrorKindPayloadTooLarge is the API's payload_too_large kind.
	ErrorKindPayloadTooLarge
	// ErrorKindRateLimited is the API's rate_limited kind.
	ErrorKindRateLimited
	// ErrorKindOperationUnconfirmed is the API's operation_unconfirmed kind.
	ErrorKindOperationUnconfirmed
	// ErrorKindSpoolFull is the API's spool_full kind.
	ErrorKindSpoolFull
	// ErrorKindQueueReplaced is the API's queue_replaced kind.
	ErrorKindQueueReplaced
	// ErrorKindUnauthenticated is the API's unauthenticated kind.
	ErrorKindUnauthenticated
	// ErrorKindAccountSuspended is the API's account_suspended kind.
	ErrorKindAccountSuspended
	// ErrorKindAccountBlocked is the API's account_blocked kind.
	ErrorKindAccountBlocked
)

var (
	errorKind2wire = map[ErrorKind]string{
		ErrorKindAccountBlocked:       "account_blocked",
		ErrorKindAccountSuspended:     "account_suspended",
		ErrorKindDedupClaimed:         "dedup_claimed",
		ErrorKindDedupDisabled:        "dedup_disabled",
		ErrorKindDedupInFlight:        "dedup_in_flight",
		ErrorKindInvalidToken:         "invalid_token",
		ErrorKindNotFailed:            "not_failed",
		ErrorKindOperationUnconfirmed: "operation_unconfirmed",
		ErrorKindPayloadTooLarge:      "payload_too_large",
		ErrorKindQueueBusy:            "queue_busy",
		ErrorKindQueueCreating:        "queue_creating",
		ErrorKindQueueDeleting:        "queue_deleting",
		ErrorKindQueueExists:          "queue_exists",
		ErrorKindQueueLimit:           "queue_limit",
		ErrorKindQueueNotFound:        "queue_not_found",
		ErrorKindQueueReplaced:        "queue_replaced",
		ErrorKindRateLimited:          "rate_limited",
		ErrorKindRetentionLimit:       "retention_limit",
		ErrorKindSpoolFull:            "spool_full",
		ErrorKindSpoolNotFound:        "spool_not_found",
		ErrorKindStaleToken:           "stale_token",
		ErrorKindUnauthenticated:      "unauthenticated",
		ErrorKindUnknownHeader:        "unknown_header",
	}
	errorKind2string = transform(errorKind2wire, func(s string) string {
		return strings.ReplaceAll(s, "_", " ")
	})
	errorKind4wire = reverse(errorKind2wire)
)

func (k ErrorKind) toWire() (string, error)  { return toWire(errorKind2wire, k) }
func (k *ErrorKind) fromWire(w string) error { return fromWire(errorKind4wire, k, w) }

// String names the kind as [Error] prints it: its wire name in words.
func (k ErrorKind) String() string { return enumString("ErrorKind", errorKind2string, k) }

// KindError is a sentinel that every [Error] of its kind matches with
// [errors.Is]. The package declares one per kind, as ErrQueueNotFound; a
// KindError is never returned itself.
type KindError struct {
	Kind ErrorKind
}

// Error returns the kind's words.
func (e *KindError) Error() string {
	s, has := errorKind2string[e.Kind]
	if !has {
		return "unknown error"
	}
	return s
}

// The sentinels, one per [ErrorKind], for [errors.Is]. See the kind for
// what each means.
var (
	ErrInvalidToken         = &KindError{Kind: ErrorKindInvalidToken}
	ErrDedupDisabled        = &KindError{Kind: ErrorKindDedupDisabled}
	ErrUnknownHeader        = &KindError{Kind: ErrorKindUnknownHeader}
	ErrRetentionLimit       = &KindError{Kind: ErrorKindRetentionLimit}
	ErrQueueLimit           = &KindError{Kind: ErrorKindQueueLimit}
	ErrQueueNotFound        = &KindError{Kind: ErrorKindQueueNotFound}
	ErrQueueReplaced        = &KindError{Kind: ErrorKindQueueReplaced}
	ErrSpoolNotFound        = &KindError{Kind: ErrorKindSpoolNotFound}
	ErrQueueExists          = &KindError{Kind: ErrorKindQueueExists}
	ErrQueueCreating        = &KindError{Kind: ErrorKindQueueCreating}
	ErrQueueDeleting        = &KindError{Kind: ErrorKindQueueDeleting}
	ErrQueueBusy            = &KindError{Kind: ErrorKindQueueBusy}
	ErrNotFailed            = &KindError{Kind: ErrorKindNotFailed}
	ErrDedupInFlight        = &KindError{Kind: ErrorKindDedupInFlight}
	ErrDedupClaimed         = &KindError{Kind: ErrorKindDedupClaimed}
	ErrStaleToken           = &KindError{Kind: ErrorKindStaleToken}
	ErrPayloadTooLarge      = &KindError{Kind: ErrorKindPayloadTooLarge}
	ErrRateLimited          = &KindError{Kind: ErrorKindRateLimited}
	ErrOperationUnconfirmed = &KindError{Kind: ErrorKindOperationUnconfirmed}
	ErrSpoolFull            = &KindError{Kind: ErrorKindSpoolFull}
	ErrUnauthenticated      = &KindError{Kind: ErrorKindUnauthenticated}
	ErrAccountSuspended     = &KindError{Kind: ErrorKindAccountSuspended}
	ErrAccountBlocked       = &KindError{Kind: ErrorKindAccountBlocked}
)

// ErrUnavailable matches a spool momentarily unavailable: a 503 that names no
// kind this SDK knows, per https://docs.spooler.sh/errors. It is no kind,
// since a gateway in front of the API can answer it too. The attempt that
// answered it changed nothing; an earlier attempt of the same call may have,
// which [ErrResultUnknown] reports, so check that first.
var ErrUnavailable = errors.New("unavailable")

// UnknownHeaderError is the details of an [ErrorKindUnknownHeader] answer.
type UnknownHeaderError struct {
	// Header is the refused header's name.
	Header string
}

// Error returns the header's name.
func (e *UnknownHeaderError) Error() string {
	return fmt.Sprintf("unknown header: %q", e.Header)
}

// RetentionLimitError is the details of an [ErrorKindRetentionLimit] answer.
type RetentionLimitError struct {
	Max time.Duration
}

// Error returns the limit.
func (e *RetentionLimitError) Error() string {
	return fmt.Sprintf("retention limit: max %s", e.Max)
}

// QueueLimitError is the details of an [ErrorKindQueueLimit] answer.
type QueueLimitError struct {
	Max int64
}

// Error returns the limit.
func (e *QueueLimitError) Error() string {
	return fmt.Sprintf("queue limit: max %d", e.Max)
}

// QueueNotFoundError is the details of an [ErrorKindQueueNotFound] answer.
type QueueNotFoundError struct {
	Queue string
}

// Error returns the queue's name.
func (e *QueueNotFoundError) Error() string {
	return fmt.Sprintf("queue not found: %q", e.Queue)
}

// QueueReplacedError is the details of an [ErrorKindQueueReplaced] answer.
type QueueReplacedError struct {
	// UID is the uid of the queue now under the name.
	UID string
}

// Error returns the current queue's uid.
func (e *QueueReplacedError) Error() string {
	return fmt.Sprintf("queue replaced: current uid %q", e.UID)
}

// DedupClaimedError is the details of an [ErrorKindDedupClaimed] answer.
type DedupClaimedError struct {
	// ID is the message holding the dedup key.
	ID string
}

// Error returns the message's id.
func (e *DedupClaimedError) Error() string {
	return fmt.Sprintf("dedup claimed: %q", e.ID)
}

// RateLimitedError is the details of an [ErrorKindRateLimited] answer.
type RateLimitedError struct {
	// RetryAfter is how long the server asked to wait before retrying.
	RetryAfter time.Duration
}

// Error returns the wait.
func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("rate limited: retry after %s", e.RetryAfter)
}

// SpoolLimit is which of a spool's caps [SpoolFullError] reports reached.
type SpoolLimit uint

const (
	// SpoolLimitUnknown is a cap this SDK does not know, or none.
	SpoolLimitUnknown SpoolLimit = iota
	// SpoolLimitMessages is the cap on stored messages.
	SpoolLimitMessages
	// SpoolLimitDataBytes is the cap on stored payload bytes.
	SpoolLimitDataBytes
)

var (
	spoolLimit2wire = map[SpoolLimit]string{
		SpoolLimitMessages:  "messages",
		SpoolLimitDataBytes: "dataBytes",
	}
	spoolLimit4wire = reverse(spoolLimit2wire)
)

func (l SpoolLimit) toWire() (string, error)  { return toWire(spoolLimit2wire, l) }
func (l *SpoolLimit) fromWire(w string) error { return fromWire(spoolLimit4wire, l, w) }

// String returns the cap's name as the API spells it, or SpoolLimit(n) for
// a value the SDK does not define.
func (l SpoolLimit) String() string { return enumString("SpoolLimit", spoolLimit2wire, l) }

// SpoolFullError is the details of an [ErrorKindSpoolFull] answer.
type SpoolFullError struct {
	// Limit is the cap that was reached, named after the [MessageStats]
	// field it is enforced against.
	Limit SpoolLimit

	Max int64
}

// Error returns the cap and its name.
func (e *SpoolFullError) Error() string {
	return fmt.Sprintf("spool full: max %d %s", e.Max, e.Limit)
}

// errorObj is the wire form of an error. Kind stays a string: one this SDK
// does not know must not fail the decode.
type errorObj struct {
	Kind    string          `json:"kind"`
	Message string          `json:"message"`
	Details json.RawMessage `json:"details"`
}

// toKind is the kind the wire named, or ErrorKindUnknown.
func (e *errorObj) toKind() (k ErrorKind) {
	_ = k.fromWire(e.Kind)
	return k
}

// parseDetails returns the details of an answer of the given kind, read from
// the answer's header or its details object as the kind requires. It never
// fails: details that are absent or do not parse are nil, and the kind and
// message still reach the caller.
func parseDetails(kind ErrorKind, header http.Header, details json.RawMessage) error {
	switch kind {
	case ErrorKindUnknownHeader:
		var d unknownHeaderError
		if err := json.Unmarshal(details, &d); err == nil {
			return d.toError()
		}
	case ErrorKindRetentionLimit:
		var d retentionLimitError
		if err := json.Unmarshal(details, &d); err == nil {
			return d.toError()
		}
	case ErrorKindQueueLimit:
		var d queueLimitError
		if err := json.Unmarshal(details, &d); err == nil {
			return d.toError()
		}
	case ErrorKindQueueNotFound:
		var d queueNotFoundError
		if err := json.Unmarshal(details, &d); err == nil {
			return d.toError()
		}
	case ErrorKindQueueReplaced:
		var d queueReplacedError
		if err := json.Unmarshal(details, &d); err == nil {
			return d.toError()
		}
	case ErrorKindDedupClaimed:
		var d dedupClaimedError
		if err := json.Unmarshal(details, &d); err == nil {
			return d.toError()
		}
	case ErrorKindRateLimited:
		// The delay travels in a header, not in the body.
		d, err := httputil.ParseRetryAfter(time.Now(), header.Get("Retry-After"))
		if err == nil {
			return &RateLimitedError{
				RetryAfter: d,
			}
		}
	case ErrorKindSpoolFull:
		var d spoolFullError
		if err := json.Unmarshal(details, &d); err == nil {
			return d.toError()
		}
	}
	return nil
}

type unknownHeaderError struct {
	Header string `json:"header"`
}

func (e *unknownHeaderError) toError() *UnknownHeaderError {
	return &UnknownHeaderError{
		Header: e.Header,
	}
}

type retentionLimitError struct {
	MaxSeconds int64 `json:"maxSeconds"`
}

func (e *retentionLimitError) toError() *RetentionLimitError {
	return &RetentionLimitError{
		Max: time.Duration(e.MaxSeconds) * time.Second,
	}
}

type queueLimitError struct {
	Max int64 `json:"max"`
}

func (e *queueLimitError) toError() *QueueLimitError {
	return &QueueLimitError{
		Max: e.Max,
	}
}

type queueNotFoundError struct {
	Queue string `json:"queue"`
}

func (e *queueNotFoundError) toError() *QueueNotFoundError {
	return &QueueNotFoundError{
		Queue: e.Queue,
	}
}

type queueReplacedError struct {
	UID string `json:"uid"`
}

func (e *queueReplacedError) toError() *QueueReplacedError {
	return &QueueReplacedError{
		UID: e.UID,
	}
}

type dedupClaimedError struct {
	ID string `json:"id"`
}

func (e *dedupClaimedError) toError() *DedupClaimedError {
	return &DedupClaimedError{
		ID: e.ID,
	}
}

type spoolLimit SpoolLimit

// UnmarshalJSON leaves a limit this SDK does not know as SpoolLimitUnknown,
// so that the cap next to it still reaches the caller.
func (l *spoolLimit) UnmarshalJSON(p []byte) error {
	var s string
	if err := json.Unmarshal(p, &s); err != nil {
		return err
	}
	_ = (*SpoolLimit)(l).fromWire(s)
	return nil
}

type spoolFullError struct {
	Limit spoolLimit `json:"limit"`
	Max   int64      `json:"max"`
}

func (e *spoolFullError) toError() *SpoolFullError {
	return &SpoolFullError{
		Limit: SpoolLimit(e.Limit),
		Max:   e.Max,
	}
}

// httpError turns the status error of an HTTP call into the error the
// operation returns. An answer that names a kind or carries a message is the
// API's and becomes the [Error] it describes, its kind ErrorKindUnknown if
// this SDK does not know it. Any other answer (a gateway's 502, say) is not
// the API's and becomes a plain error of its status and its body, quoted; the
// status never reaches a field, and the conditions a status signals are
// statusCondition's.
//
// Any other error is returned as is, for resultError to wrap.
func httpError(err error) error {
	se, ok := errors.AsType[*httputil.StatusError](err)
	if !ok {
		return err
	}
	desc, ok := se.Desc.(*errorObj)
	if !ok || (desc.Kind == "" && desc.Message == "") {
		if len(se.Body) == 0 {
			return fmt.Errorf(
				"%d %s",
				se.Code, http.StatusText(se.Code),
			)
		}
		return fmt.Errorf(
			"%d %s: %+q",
			se.Code, http.StatusText(se.Code), se.Body,
		)
	}
	kind := desc.toKind()
	return &Error{
		Kind:    kind,
		Message: desc.Message,
		Details: parseDetails(kind, se.Header, desc.Details),
	}
}

// statusCondition returns the sentinel of the condition the status of err's
// last attempt signals when the answer names no kind this SDK knows, or nil.
//
// Today only a status a gateway can answer too gets one; every answer of the
// API itself carries a kind.
func statusCondition(err error) error {
	se, ok := errors.AsType[*httputil.StatusError](err)
	if !ok {
		return nil
	}
	if obj, ok := se.Desc.(*errorObj); ok && obj.toKind() != ErrorKindUnknown {
		// A kind this SDK knows says what the status means.
		return nil
	}
	if se.Code == http.StatusServiceUnavailable {
		return ErrUnavailable
	}
	return nil
}
