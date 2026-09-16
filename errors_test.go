package spooler

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
)

// ackWire is the cheapest operation, answered with res. Conditions the docs
// mark transient are retried, so those cases list the answer twice.
func ackWire(res response, times int) []exchange {
	wire := make([]exchange, times)
	for i := range wire {
		wire[i] = exchange{
			req: request{
				method: "POST",
				path:   "/v1/spools/default/ack",
				header: http.Header{
					"Spooler-Lease": {"lease-1"},
				},
			},
			res: res,
		}
	}
	return wire
}

// TestErrorResponse runs every error the docs' errors page lists, plus the
// answers a server or a gateway can give outside that page, through an
// operation and checks what the caller can match, read and print.
func TestErrorResponse(t *testing.T) {
	for _, test := range []struct {
		name  string
		res   response
		times int    // Answers before the SDK gives up; 1 unless transient.
		exp   *Error // What errors.As yields.
		is    error  // The sentinel errors.Is must match, if any.
	}{
		{
			name: "invalid lease",
			res: response{
				status: 400,
				body:   `{"kind":"invalid_lease","message":"the token is malformed"}`,
			},
			exp: &Error{
				Kind:    ErrorKindInvalidLease,
				Message: "the token is malformed",
			},
			is: ErrInvalidLease,
		},
		{
			name: "dedup disabled",
			res: response{
				status: 400,
				body:   `{"kind":"dedup_disabled","message":"queue jobs has no dedup window"}`,
			},
			exp: &Error{
				Kind:    ErrorKindDedupDisabled,
				Message: "queue jobs has no dedup window",
			},
			is: ErrDedupDisabled,
		},
		{
			name: "unknown header, with the header as a detail",
			res: response{
				status: 400,
				body:   `{"kind":"unknown_header","message":"Spooler-Queue is not accepted here","details":{"header":"Spooler-Queue"}}`,
			},
			exp: &Error{
				Kind:    ErrorKindUnknownHeader,
				Message: "Spooler-Queue is not accepted here",
				Details: &UnknownHeaderError{
					Header: "Spooler-Queue",
				},
			},
			is: ErrUnknownHeader,
		},
		{
			name: "retention limit, with the limit as a duration",
			res: response{
				status: 403,
				body:   `{"kind":"retention_limit","message":"the plan allows 3600 seconds","details":{"maxSeconds":3600}}`,
			},
			exp: &Error{
				Kind:    ErrorKindRetentionLimit,
				Message: "the plan allows 3600 seconds",
				Details: &RetentionLimitError{
					Max: time.Hour,
				},
			},
			is: ErrRetentionLimit,
		},
		{
			name: "queue limit, with the cap as a detail",
			res: response{
				status: 403,
				body:   `{"kind":"queue_limit","message":"the plan allows 10 queues","details":{"max":10}}`,
			},
			exp: &Error{
				Kind:    ErrorKindQueueLimit,
				Message: "the plan allows 10 queues",
				Details: &QueueLimitError{
					Max: 10,
				},
			},
			is: ErrQueueLimit,
		},
		{
			name: "queue not found, with the name as a detail",
			res: response{
				status: 404,
				body:   `{"kind":"queue_not_found","message":"queue jobs does not exist","details":{"queue":"jobs"}}`,
			},
			exp: &Error{
				Kind:    ErrorKindQueueNotFound,
				Message: "queue jobs does not exist",
				Details: &QueueNotFoundError{
					Queue: "jobs",
				},
			},
			is: ErrQueueNotFound,
		},
		{
			name: "spool not found",
			res: response{
				status: 404,
				body:   `{"kind":"spool_not_found","message":"no spool default on the account"}`,
			},
			exp: &Error{
				Kind:    ErrorKindSpoolNotFound,
				Message: "no spool default on the account",
			},
			is: ErrSpoolNotFound,
		},
		{
			name: "queue exists",
			res: response{
				status: 409,
				body:   `{"kind":"queue_exists","message":"queue jobs exists"}`,
			},
			exp: &Error{
				Kind:    ErrorKindQueueExists,
				Message: "queue jobs exists",
			},
			is: ErrQueueExists,
		},
		{
			name: "queue deleting",
			res: response{
				status: 409,
				body:   `{"kind":"queue_deleting","message":"queue jobs is being deleted"}`,
			},
			exp: &Error{
				Kind:    ErrorKindQueueDeleting,
				Message: "queue jobs is being deleted",
			},
			is: ErrQueueDeleting,
		},
		{
			name: "queue busy",
			res: response{
				status: 409,
				body:   `{"kind":"queue_busy","message":"a concurrent change won"}`,
			},
			times: 2,
			exp: &Error{
				Kind:    ErrorKindQueueBusy,
				Message: "a concurrent change won",
			},
			is: ErrQueueBusy,
		},
		{
			name: "not failed",
			res: response{
				status: 409,
				body:   `{"kind":"not_failed","message":"the message is not failed"}`,
			},
			exp: &Error{
				Kind:    ErrorKindNotFailed,
				Message: "the message is not failed",
			},
			is: ErrNotFailed,
		},
		{
			name: "dedup in flight",
			res: response{
				status: 409,
				body:   `{"kind":"dedup_in_flight","message":"the same key has not settled"}`,
			},
			exp: &Error{
				Kind:    ErrorKindDedupInFlight,
				Message: "the same key has not settled",
			},
			is: ErrDedupInFlight,
		},
		{
			name: "dedup claimed, with the holder as a detail",
			res: response{
				status: 409,
				body:   `{"kind":"dedup_claimed","message":"the key is held by 1-1","details":{"id":"1-1"}}`,
			},
			exp: &Error{
				Kind:    ErrorKindDedupClaimed,
				Message: "the key is held by 1-1",
				Details: &DedupClaimedError{
					ID: "1-1",
				},
			},
			is: ErrDedupClaimed,
		},
		{
			name: "stale lease",
			res: response{
				status: 410,
				body:   `{"kind":"stale_lease","message":"the lease is stale"}`,
			},
			exp: &Error{
				Kind:    ErrorKindStaleLease,
				Message: "the lease is stale",
			},
			is: ErrStaleLease,
		},
		{
			name: "payload too large",
			res: response{
				status: 413,
				body:   `{"kind":"payload_too_large","message":"the message exceeds the size limit"}`,
			},
			exp: &Error{
				Kind:    ErrorKindPayloadTooLarge,
				Message: "the message exceeds the size limit",
			},
			is: ErrPayloadTooLarge,
		},
		{
			name: "rate limited, with the delay",
			res: response{
				status: 429,
				header: http.Header{
					"Retry-After": {"30"},
				},
				body: `{"kind":"rate_limited","message":"slow down"}`,
			},
			times: 2,
			exp: &Error{
				Kind:       ErrorKindRateLimited,
				Message:    "slow down",
				RetryAfter: 30 * time.Second,
			},
			is: ErrRateLimited,
		},
		{
			name: "operation unconfirmed",
			res: response{
				status: 500,
				body:   `{"kind":"operation_unconfirmed","message":"durability is unknown"}`,
			},
			exp: &Error{
				Kind:    ErrorKindOperationUnconfirmed,
				Message: "durability is unknown",
			},
			is: ErrOperationUnconfirmed,
		},
		{
			name: "spool full of messages, with the cap as a detail",
			res: response{
				status: 507,
				body:   `{"kind":"spool_full","message":"the spool holds 1000 messages","details":{"limit":"messages","max":1000}}`,
			},
			exp: &Error{
				Kind:    ErrorKindSpoolFull,
				Message: "the spool holds 1000 messages",
				Details: &SpoolFullError{
					Limit: SpoolLimitMessages,
					Max:   1000,
				},
			},
			is: ErrSpoolFull,
		},
		{
			name: "spool full of bytes",
			res: response{
				status: 507,
				body:   `{"kind":"spool_full","message":"the spool holds 65536 bytes","details":{"limit":"dataBytes","max":65536}}`,
			},
			exp: &Error{
				Kind:    ErrorKindSpoolFull,
				Message: "the spool holds 65536 bytes",
				Details: &SpoolFullError{
					Limit: SpoolLimitDataBytes,
					Max:   65536,
				},
			},
			is: ErrSpoolFull,
		},
		{
			name: "spool full of something this SDK does not know keeps the cap",
			res: response{
				status: 507,
				body:   `{"kind":"spool_full","message":"the spool holds 7 things","details":{"limit":"things","max":7}}`,
			},
			exp: &Error{
				Kind:    ErrorKindSpoolFull,
				Message: "the spool holds 7 things",
				Details: &SpoolFullError{
					Max: 7,
				},
			},
			is: ErrSpoolFull,
		},
		{
			name: "bad credentials have no kind and get the SDK's own",
			res: response{
				status: 401,
				body:   `{"message":"bad credentials"}`,
			},
			exp: &Error{
				Kind:    ErrorKindUnauthorized,
				Message: "bad credentials",
			},
			is: ErrUnauthorized,
		},
		{
			name: "a suspended account has no kind and gets the SDK's own",
			res: response{
				status: 402,
				body:   `{"message":"the account is suspended"}`,
			},
			exp: &Error{
				Kind:    ErrorKindSuspended,
				Message: "the account is suspended",
			},
			is: ErrSuspended,
		},
		{
			name: "an unavailable spool has no kind and gets the SDK's own, with the delay",
			res: response{
				status: 503,
				header: http.Header{
					"Retry-After": {"5"},
				},
				body: `{"message":"the spool is momentarily unavailable"}`,
			},
			times: 2,
			exp: &Error{
				Kind:       ErrorKindUnavailable,
				Message:    "the spool is momentarily unavailable",
				RetryAfter: 5 * time.Second,
			},
			is: ErrUnavailable,
		},
		{
			name: "a kind this SDK does not know on a mapped status still matches the broader sentinel",
			res: response{
				status: 503,
				body:   `{"kind":"maintenance","message":"back in five"}`,
			},
			times: 2,
			exp: &Error{
				Kind:    ErrorKindUnavailable,
				Message: "back in five",
			},
			is: ErrUnavailable,
		},
		{
			name: "a kind this SDK does not know on any other status names the status and the kind in prose",
			res: response{
				status: 404,
				body:   `{"kind":"teapot_not_found","message":"no teapot"}`,
			},
			exp: &Error{
				Message: "404 Not Found (kind teapot_not_found): no teapot",
			},
		},
		{
			name: "a kind that is not a string is not the error shape, so the body is quoted",
			res: response{
				status: 404,
				body:   `{"kind":1,"message":"no teapot"}`,
			},
			exp: &Error{
				Message: `404 Not Found: "{\"kind\":1,\"message\":\"no teapot\"}"`,
			},
		},
		{
			name: "a blocked key has no kind and gets the SDK's own",
			res: response{
				status: 403,
				body:   `{"message":"the key is blocked"}`,
			},
			exp: &Error{
				Kind:    ErrorKindForbidden,
				Message: "the key is blocked",
			},
			is: ErrForbidden,
		},
		{
			name: "a gateway answer with no body names the status in prose",
			res: response{
				status: 502,
			},
			exp: &Error{
				Message: "502 Bad Gateway",
			},
		},
		{
			name: "a gateway answer with a body quotes it",
			res: response{
				status: 502,
				body:   "<html>upstream down</html>",
			},
			exp: &Error{
				Message: `502 Bad Gateway: "<html>upstream down</html>"`,
			},
		},
		{
			name: "a body that is not the error shape is quoted",
			res: response{
				status: 400,
				body:   "garbage",
			},
			exp: &Error{
				Message: `400 Bad Request: "garbage"`,
			},
		},
		{
			name: "details that do not parse leave the kind and message intact",
			res: response{
				status: 404,
				body:   `{"kind":"queue_not_found","message":"queue jobs does not exist","details":"oops"}`,
			},
			exp: &Error{
				Kind:    ErrorKindQueueNotFound,
				Message: "queue jobs does not exist",
			},
			is: ErrQueueNotFound,
		},
		{
			name: "a malformed delay reads as none",
			res: response{
				status: 429,
				header: http.Header{
					"Retry-After": {"-5"},
				},
				body: `{"kind":"rate_limited","message":"slow down"}`,
			},
			times: 2,
			exp: &Error{
				Kind:    ErrorKindRateLimited,
				Message: "slow down",
			},
			is: ErrRateLimited,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := new(Client)
				done := stubWire(t, c, ackWire(test.res, max(test.times, 1)))
				err := c.Ack(t.Context(), AckRequest{
					Spool: "default",
					Lease: "lease-1",
				})
				done()
				if err == nil {
					t.Fatalf("want error; got nothing")
				}

				act, ok := errors.AsType[*Error](err)
				if !ok {
					t.Fatalf(
						"error: %T; want *Error",
						err,
					)
				}
				if diff := cmp.Diff(test.exp, act); diff != "" {
					t.Errorf(
						"error mismatch (-want +act):\n%s",
						diff,
					)
				}
				// Every response carries a message, and the message is the
				// text; the fallbacks are TestErrorText's.
				if act, exp := act.Error(), test.exp.Message; act != exp {
					t.Errorf(
						"text: %q; want %q",
						act, exp,
					)
				}
				if test.is != nil && !errors.Is(err, test.is) {
					t.Errorf(
						"errors.Is(%v, %v) is false; want true",
						err, test.is,
					)
				}
				if errors.Is(err, ErrStaleLease) && test.is != ErrStaleLease {
					t.Errorf(
						"errors.Is(%v, %v) is true; want false",
						err, ErrStaleLease,
					)
				}
				if test.exp.Details != nil && !errors.As(err, ptrTo(test.exp.Details)) {
					t.Errorf(
						"errors.As(%v, %T) is false; want true",
						err, test.exp.Details,
					)
				}
			})
		})
	}
}

// ptrTo returns a pointer to a fresh copy of v's dynamic type, for errors.As.
func ptrTo(v error) any {
	switch v.(type) {
	case *UnknownHeaderError:
		return new(*UnknownHeaderError)
	case *RetentionLimitError:
		return new(*RetentionLimitError)
	case *QueueLimitError:
		return new(*QueueLimitError)
	case *QueueNotFoundError:
		return new(*QueueNotFoundError)
	case *DedupClaimedError:
		return new(*DedupClaimedError)
	case *SpoolFullError:
		return new(*SpoolFullError)
	default:
		panic("unknown detail type")
	}
}

func TestErrorText(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		exp  string
	}{
		{
			// The server's message is complete; the kind is not prefixed.
			name: "kind and message",
			err: &Error{
				Kind:    ErrorKindStaleLease,
				Message: "the lease is stale",
			},
			exp: "the lease is stale",
		},
		{
			name: "kind alone",
			err: &Error{
				Kind: ErrorKindStaleLease,
			},
			exp: "stale lease",
		},
		{
			name: "details stand in for a missing message",
			err: &Error{
				Kind: ErrorKindQueueNotFound,
				Details: &QueueNotFoundError{
					Queue: "jobs",
				},
			},
			exp: `queue not found: "jobs"`,
		},
		{
			name: "details are not printed beside a message",
			err: &Error{
				Kind:    ErrorKindQueueNotFound,
				Message: "queue jobs does not exist",
				Details: &QueueNotFoundError{
					Queue: "jobs",
				},
			},
			exp: "queue jobs does not exist",
		},
		{
			name: "unknown kind with a message",
			err: &Error{
				Message: "502 Bad Gateway",
			},
			exp: "502 Bad Gateway",
		},
		{
			name: "unknown kind alone",
			err:  &Error{},
			exp:  "unknown error",
		},
		{
			name: "a sentinel prints its kind",
			err:  ErrSpoolFull,
			exp:  "spool full",
		},
		{
			name: "a sentinel of an unknown kind",
			err: &KindError{
				Kind: ErrorKind(999),
			},
			exp: "unknown error",
		},
		{
			name: "unknown header detail",
			err: &UnknownHeaderError{
				Header: "Spooler-Queue",
			},
			exp: `unknown header: "Spooler-Queue"`,
		},
		{
			name: "retention limit detail",
			err: &RetentionLimitError{
				Max: time.Hour,
			},
			exp: "retention limit: max 1h0m0s",
		},
		{
			name: "queue limit detail",
			err: &QueueLimitError{
				Max: 10,
			},
			exp: "queue limit: max 10",
		},
		{
			name: "queue not found detail",
			err: &QueueNotFoundError{
				Queue: "jobs",
			},
			exp: `queue not found: "jobs"`,
		},
		{
			name: "dedup claimed detail",
			err: &DedupClaimedError{
				ID: "1-1",
			},
			exp: `dedup claimed: "1-1"`,
		},
		{
			name: "spool full detail",
			err: &SpoolFullError{
				Limit: SpoolLimitDataBytes,
				Max:   65536,
			},
			exp: "spool full: max 65536 dataBytes",
		},
		{
			name: "spool full detail of an unknown limit",
			err: &SpoolFullError{
				Limit: SpoolLimit(7),
				Max:   7,
			},
			exp: "spool full: max 7 SpoolLimit(7)",
		},
		{
			name: "the client's prefix",
			err:  resultError(errors.New("boom")),
			exp:  "spooler: boom",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if act, exp := test.err.Error(), test.exp; act != exp {
				t.Errorf(
					"text: %q; want %q",
					act, exp,
				)
			}
		})
	}
}

// TestClientError checks what the wrapper every method returns lets through
// to errors.Is and errors.As: the API's error and the context sentinels, and
// nothing of the transport.
func TestClientError(t *testing.T) {
	var (
		errBoom = errors.New("boom")
		apiErr  = &Error{
			Kind: ErrorKindStaleLease,
		}
		urlErr = &url.Error{
			Op:  "Post",
			URL: "https://api.spooler.sh/v1/spools/default/ack",
			Err: errBoom,
		}
	)
	for _, test := range []struct {
		name  string
		err   error
		is    []error // Must match.
		isNot []error // Must not match.
		text  string
	}{
		{
			name: "nothing wraps to nothing",
			err:  resultError(nil),
		},
		{
			name: "the API's error is matched by its sentinel and nothing else",
			err:  resultError(apiErr),
			is:   []error{apiErr, ErrStaleLease},
			isNot: []error{
				ErrQueueBusy,
				errBoom,
			},
			text: "spooler: stale lease",
		},
		{
			// A refused request matches the sentinel and keeps its reason.
			name: "a refused request",
			err:  errSpoolRequired,
			is:   []error{ErrInvalidRequest},
			text: "spooler: invalid request: spool name is required",
		},
		{
			name: "a cancelled context is matched through the transport's wrapping",
			err: resultError(&url.Error{
				Op:  "Post",
				URL: "https://api.spooler.sh/v1/spools/default/ack",
				Err: context.Canceled,
			}),
			is:   []error{context.Canceled},
			text: `spooler: Post "https://api.spooler.sh/v1/spools/default/ack": context canceled`,
		},
		{
			name: "an expired context is matched through the transport's wrapping",
			err: resultError(&url.Error{
				Op:  "Post",
				URL: "https://api.spooler.sh/v1/spools/default/ack",
				Err: context.DeadlineExceeded,
			}),
			is:   []error{context.DeadlineExceeded},
			text: `spooler: Post "https://api.spooler.sh/v1/spools/default/ack": context deadline exceeded`,
		},
		{
			name:  "a transport error is text only",
			err:   resultError(urlErr),
			isNot: []error{urlErr, errBoom},
			text:  `spooler: Post "https://api.spooler.sh/v1/spools/default/ack": boom`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.err == nil {
				if test.text != "" || len(test.is) > 0 {
					t.Fatalf("want error; got nothing")
				}
				return
			}
			for _, target := range test.is {
				if !errors.Is(test.err, target) {
					t.Errorf(
						"errors.Is(%v, %v) is false; want true",
						test.err, target,
					)
				}
			}
			for _, target := range test.isNot {
				if errors.Is(test.err, target) {
					t.Errorf(
						"errors.Is(%v, %v) is true; want false",
						test.err, target,
					)
				}
			}
			if _, ok := errors.AsType[*url.Error](test.err); ok {
				t.Errorf(
					"errors.As(%v, *url.Error) is true; want false",
					test.err,
				)
			}
			if act, exp := test.err.Error(), test.text; act != exp {
				t.Errorf(
					"text: %q; want %q",
					act, exp,
				)
			}
		})
	}
}
