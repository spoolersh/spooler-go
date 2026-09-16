package spooler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/spoolersh/spooler-go/internal/httputil"
)

// Error is an error the API answered. Every operation returns one, wrapped,
// for any answer that is an error; reach it with [errors.As], or match its
// kind directly with [errors.Is] against the Err sentinels, as
// errors.Is(err, [ErrQueueNotFound]). The message is for people; a program
// acts on the kind, the details and RetryAfter.
type Error struct {
	// Kind is what went wrong, as the API names it, or a kind of the SDK's
	// own for the conditions the API answers by status alone. It is
	// ErrorKindUnknown for a kind this SDK does not know, whose wire name
	// is then kept in the message.
	Kind ErrorKind

	// Message is the server's text, complete on its own.
	Message string

	// Details is the facts the message states, as a type fixed per kind,
	// such as [QueueNotFoundError]; reach it with [errors.As].
	// Zero means the kind has no details.
	Details error

	// RetryAfter is how long the server asked to wait before retrying, as
	// of the last answer.
	// Zero means the server did not say.
	RetryAfter time.Duration
}

// Error returns the server's message, which is complete on its own and
// already states what Details holds as fields; reach those with [errors.As].
// Without a message it returns the details' text, and failing that the
// kind's words.
func (e *Error) Error() string {
	switch {
	case e.Message != "":
		return e.Message
	case e.Details != nil:
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

// ErrorKind is what an [Error] reports went wrong: the API's kinds, as
// https://docs.spooler.sh/errors lists them, plus the SDK's own for the
// conditions the API answers by status alone.
type ErrorKind uint

const (
	// ErrorKindUnknown is a kind this SDK does not know, or none.
	ErrorKindUnknown ErrorKind = iota
	// ErrorKindInvalidLease is the API's invalid_lease kind.
	ErrorKindInvalidLease
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
	// ErrorKindStaleLease is the API's stale_lease kind.
	ErrorKindStaleLease
	// ErrorKindPayloadTooLarge is the API's payload_too_large kind.
	ErrorKindPayloadTooLarge
	// ErrorKindRateLimited is the API's rate_limited kind.
	ErrorKindRateLimited
	// ErrorKindOperationUnconfirmed is the API's operation_unconfirmed kind.
	ErrorKindOperationUnconfirmed
	// ErrorKindSpoolFull is the API's spool_full kind.
	ErrorKindSpoolFull

	// The kinds below are the SDK's own. The API answers these conditions
	// without a kind, so they have no wire name; the transport maps them.

	// ErrorKindUnauthorized is an API key that is missing or not
	// recognized.
	ErrorKindUnauthorized
	// ErrorKindSuspended is a suspended account.
	ErrorKindSuspended
	// ErrorKindForbidden is a blocked API key.
	ErrorKindForbidden
	// ErrorKindUnavailable is a spool momentarily unavailable.
	ErrorKindUnavailable
)

var (
	errorKind2wire = map[ErrorKind]string{
		ErrorKindDedupClaimed:         "dedup_claimed",
		ErrorKindDedupDisabled:        "dedup_disabled",
		ErrorKindDedupInFlight:        "dedup_in_flight",
		ErrorKindInvalidLease:         "invalid_lease",
		ErrorKindNotFailed:            "not_failed",
		ErrorKindOperationUnconfirmed: "operation_unconfirmed",
		ErrorKindPayloadTooLarge:      "payload_too_large",
		ErrorKindQueueBusy:            "queue_busy",
		ErrorKindQueueDeleting:        "queue_deleting",
		ErrorKindQueueExists:          "queue_exists",
		ErrorKindQueueLimit:           "queue_limit",
		ErrorKindQueueNotFound:        "queue_not_found",
		ErrorKindRateLimited:          "rate_limited",
		ErrorKindRetentionLimit:       "retention_limit",
		ErrorKindSpoolFull:            "spool_full",
		ErrorKindSpoolNotFound:        "spool_not_found",
		ErrorKindStaleLease:           "stale_lease",
		ErrorKindUnknownHeader:        "unknown_header",
	}
	errorKind2string = merge(
		transform(errorKind2wire, func(s string) string {
			return strings.ReplaceAll(s, "_", " ")
		}),
		map[ErrorKind]string{
			ErrorKindUnauthorized: "unauthorized",
			ErrorKindSuspended:    "suspended",
			ErrorKindForbidden:    "forbidden",
			ErrorKindUnavailable:  "unavailable",
		},
	)
	errorKind4wire = reverse(errorKind2wire)
)

func (k ErrorKind) toWire() (string, error)  { return toWire(errorKind2wire, k) }
func (k *ErrorKind) fromWire(w string) error { return fromWire(errorKind4wire, k, w) }

// String names the kind as [Error] prints it, which unlike the wire name also
// covers the kinds of the SDK's own.
func (k ErrorKind) String() string { return enumString("ErrorKind", errorKind2string, k) }

// KindError is a sentinel that every [Error] of its kind matches with
// [errors.Is]. The package declares one per kind, as ErrQueueNotFound; a
// KindError is never returned itself.
type KindError struct {
	// Kind is the kind the sentinel stands for.
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
	ErrInvalidLease         = &KindError{Kind: ErrorKindInvalidLease}
	ErrDedupDisabled        = &KindError{Kind: ErrorKindDedupDisabled}
	ErrUnknownHeader        = &KindError{Kind: ErrorKindUnknownHeader}
	ErrRetentionLimit       = &KindError{Kind: ErrorKindRetentionLimit}
	ErrQueueLimit           = &KindError{Kind: ErrorKindQueueLimit}
	ErrQueueNotFound        = &KindError{Kind: ErrorKindQueueNotFound}
	ErrSpoolNotFound        = &KindError{Kind: ErrorKindSpoolNotFound}
	ErrQueueExists          = &KindError{Kind: ErrorKindQueueExists}
	ErrQueueDeleting        = &KindError{Kind: ErrorKindQueueDeleting}
	ErrQueueBusy            = &KindError{Kind: ErrorKindQueueBusy}
	ErrNotFailed            = &KindError{Kind: ErrorKindNotFailed}
	ErrDedupInFlight        = &KindError{Kind: ErrorKindDedupInFlight}
	ErrDedupClaimed         = &KindError{Kind: ErrorKindDedupClaimed}
	ErrStaleLease           = &KindError{Kind: ErrorKindStaleLease}
	ErrPayloadTooLarge      = &KindError{Kind: ErrorKindPayloadTooLarge}
	ErrRateLimited          = &KindError{Kind: ErrorKindRateLimited}
	ErrOperationUnconfirmed = &KindError{Kind: ErrorKindOperationUnconfirmed}
	ErrSpoolFull            = &KindError{Kind: ErrorKindSpoolFull}
	ErrUnauthorized         = &KindError{Kind: ErrorKindUnauthorized}
	ErrSuspended            = &KindError{Kind: ErrorKindSuspended}
	ErrForbidden            = &KindError{Kind: ErrorKindForbidden}
	ErrUnavailable          = &KindError{Kind: ErrorKindUnavailable}
)

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
	// Max is the retention limit.
	Max time.Duration
}

// Error returns the limit.
func (e *RetentionLimitError) Error() string {
	return fmt.Sprintf("retention limit: max %s", e.Max)
}

// QueueLimitError is the details of an [ErrorKindQueueLimit] answer.
type QueueLimitError struct {
	// Max is the queue limit.
	Max int64
}

// Error returns the limit.
func (e *QueueLimitError) Error() string {
	return fmt.Sprintf("queue limit: max %d", e.Max)
}

// QueueNotFoundError is the details of an [ErrorKindQueueNotFound] answer.
type QueueNotFoundError struct {
	// Queue is the queue name that was addressed.
	Queue string
}

// Error returns the queue's name.
func (e *QueueNotFoundError) Error() string {
	return fmt.Sprintf("queue not found: %q", e.Queue)
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

	// Max is the cap.
	Max int64
}

// Error returns the cap and its name.
func (e *SpoolFullError) Error() string {
	return fmt.Sprintf("spool full: max %d %s", e.Max, e.Limit)
}

// errorObj is the wire form of an error. Kind stays a string: one this SDK
// does not know must not fail the decode, and its name is kept for the
// message so that a caller can still see which kind arrived.
type errorObj struct {
	Kind    string          `json:"kind"`
	Message string          `json:"message"`
	Details json.RawMessage `json:"details"`
}

// kind is the kind the wire named, or ErrorKindUnknown.
func (e *errorObj) kind() (k ErrorKind) {
	_ = k.fromWire(e.Kind)
	return k
}

// toError never fails: details that are absent or do not parse leave
// Error.Details nil, and the kind and message still reach the caller.
func (e *errorObj) toError() *Error {
	ret := &Error{
		Kind:    e.kind(),
		Message: e.Message,
	}
	switch ret.Kind {
	case ErrorKindUnknownHeader:
		var d unknownHeaderError
		if err := json.Unmarshal(e.Details, &d); err == nil {
			ret.Details = d.toError()
		}
	case ErrorKindRetentionLimit:
		var d retentionLimitError
		if err := json.Unmarshal(e.Details, &d); err == nil {
			ret.Details = d.toError()
		}
	case ErrorKindQueueLimit:
		var d queueLimitError
		if err := json.Unmarshal(e.Details, &d); err == nil {
			ret.Details = d.toError()
		}
	case ErrorKindQueueNotFound:
		var d queueNotFoundError
		if err := json.Unmarshal(e.Details, &d); err == nil {
			ret.Details = d.toError()
		}
	case ErrorKindDedupClaimed:
		var d dedupClaimedError
		if err := json.Unmarshal(e.Details, &d); err == nil {
			ret.Details = d.toError()
		}
	case ErrorKindSpoolFull:
		var d spoolFullError
		if err := json.Unmarshal(e.Details, &d); err == nil {
			ret.Details = d.toError()
		}
	}
	return ret
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

// httpError turns the status error of an HTTP call into an [Error]. A
// response that carried an error body becomes the Error it describes; one
// that did not (a gateway's 502, say) carries its raw body as the message.
//
// This is the one place a status is read. Where the response names no kind
// this SDK knows, statusKind maps the status to a kind of the SDK's own. If
// that finds nothing either, the kind stays unknown and the status goes into
// the message as prose, never into a field.
//
// Any other error is returned as is, for resultError to wrap.
func httpError(err error) error {
	se, ok := errors.AsType[*httputil.StatusError](err)
	if !ok {
		return err
	}
	e := new(Error)
	var wireKind string
	if obj, ok := se.Desc.(*errorObj); ok {
		e = obj.toError()
		wireKind = obj.Kind
	} else if len(se.Body) > 0 {
		e.Message = strconv.QuoteToASCII(string(se.Body))
	}
	if s := se.Header.Get("Retry-After"); s != "" {
		e.RetryAfter, _ = httputil.ParseRetryAfter(time.Now(), s)
	}
	if e.Kind == ErrorKindUnknown {
		e.Kind = statusKind(se.Code)
	}
	if e.Kind == ErrorKindUnknown {
		e.Message = statusProse(se.Code, wireKind, e.Message)
	}
	return e
}

// statusKind maps a status the API answers without a kind to the SDK's own
// kind for it, or to ErrorKindUnknown. A 411 is not here: the SDK always
// sends a length.
func statusKind(code int) ErrorKind {
	switch code {
	case http.StatusUnauthorized:
		return ErrorKindUnauthorized
	case http.StatusPaymentRequired:
		return ErrorKindSuspended
	case http.StatusForbidden:
		return ErrorKindForbidden
	case http.StatusServiceUnavailable:
		return ErrorKindUnavailable
	default:
		return ErrorKindUnknown
	}
}

// statusProse words an HTTP status for a message, with the wire's kind if it
// named one this SDK does not know, followed by text if any.
func statusProse(code int, kind, text string) string {
	s := strconv.Itoa(code) + " " + http.StatusText(code)
	if kind != "" {
		s += " (kind " + kind + ")"
	}
	if text != "" {
		s += ": " + text
	}
	return s
}
