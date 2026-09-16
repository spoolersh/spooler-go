package spooler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLeaseMetaExpires(t *testing.T) {
	for _, test := range []struct {
		name string
		meta LeaseMeta
		exp  time.Time
		ok   bool
	}{
		{
			name: "no lease timeout",
			meta: LeaseMeta{},
		},
		{
			name: "an expiry",
			meta: LeaseMeta{
				ExpiresAt: testCreatedAt,
			},
			exp: testCreatedAt,
			ok:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			act, ok := test.meta.Expires()
			if act, exp := ok, test.ok; act != exp {
				t.Errorf(
					"ok: %t; want %t",
					act, exp,
				)
			}
			if act, exp := act, test.exp; !act.Equal(exp) {
				t.Errorf(
					"expiry: %v; want %v",
					act, exp,
				)
			}
		})
	}
}

// TestEnumString covers every enum's String: a defined value prints as its
// name, and one the SDK does not define prints as Type(value), the form the
// standard library's stringers use.
// TestTokenRedaction checks that a token never prints in clear by accident,
// through fmt or slog, while an explicit conversion still yields the value.
func TestTokenRedaction(t *testing.T) {
	for _, test := range []struct {
		name  string
		token any
		value string
	}{
		{
			name:  "lease",
			token: Lease("lease-1"),
			value: "lease-1",
		},
		{
			name:  "handle",
			token: Handle("handle-1"),
			value: "handle-1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, verb := range []string{"%v", "%s", "%q"} {
				if act := fmt.Sprintf(verb, test.token); strings.Contains(act, test.value) {
					t.Errorf(
						"%s printed %q; want it redacted",
						verb, act,
					)
				}
			}
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			logger.Info("token", "t", test.token)
			if act := buf.String(); strings.Contains(act, test.value) {
				t.Errorf(
					"slog wrote %q; want it redacted",
					act,
				)
			}
			var act string
			switch v := test.token.(type) {
			case Lease:
				act = string(v)
			case Handle:
				act = string(v)
			}
			if act, exp := act, test.value; act != exp {
				t.Errorf(
					"string(token): %q; want %q",
					act, exp,
				)
			}
		})
	}
}

func TestEnumString(t *testing.T) {
	for _, test := range []struct {
		name string
		v    fmt.Stringer
		exp  string
	}{
		{
			name: "queue state",
			v:    QueueStateDraining,
			exp:  "draining",
		},
		{
			name: "unknown queue state",
			v:    QueueState(7),
			exp:  "QueueState(7)",
		},
		{
			name: "message state",
			v:    MessageStateFailed,
			exp:  "failed",
		},
		{
			name: "unknown message state",
			v:    MessageState(7),
			exp:  "MessageState(7)",
		},
		{
			name: "queue delete mode",
			v:    QueueDeleteModeDrain,
			exp:  "drain",
		},
		{
			name: "unknown queue delete mode",
			v:    QueueDeleteMode(7),
			exp:  "QueueDeleteMode(7)",
		},
		{
			name: "error kind",
			v:    ErrorKindStaleLease,
			exp:  "stale lease",
		},
		{
			name: "an SDK-own error kind",
			v:    ErrorKindUnavailable,
			exp:  "unavailable",
		},
		{
			name: "unknown error kind",
			v:    ErrorKind(99),
			exp:  "ErrorKind(99)",
		},
		{
			name: "spool limit",
			v:    SpoolLimitDataBytes,
			exp:  "dataBytes",
		},
		{
			name: "unknown spool limit",
			v:    SpoolLimit(7),
			exp:  "SpoolLimit(7)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if act, exp := test.v.String(), test.exp; act != exp {
				t.Errorf(
					"string: %q; want %q",
					act, exp,
				)
			}
		})
	}
}

// TestToWire covers the enum-to-wire direction, which only a value this SDK
// does not define can fail.
func TestToWire(t *testing.T) {
	for _, test := range []struct {
		name string
		wire func() (string, error)
		exp  string
		err  bool
	}{
		{
			name: "queue state",
			wire: QueueStateDraining.toWire,
			exp:  "draining",
		},
		{
			name: "unknown queue state",
			wire: QueueState(7).toWire,
			err:  true,
		},
		{
			name: "message state",
			wire: MessageStateFailed.toWire,
			exp:  "failed",
		},
		{
			name: "unknown message state",
			wire: MessageState(7).toWire,
			err:  true,
		},
		{
			name: "queue delete mode",
			wire: QueueDeleteModeDrain.toWire,
			exp:  "drain",
		},
		{
			name: "unknown queue delete mode",
			wire: QueueDeleteMode(7).toWire,
			err:  true,
		},
		{
			name: "error kind",
			wire: ErrorKindStaleLease.toWire,
			exp:  "stale_lease",
		},
		{
			name: "an SDK-own error kind has no wire name",
			wire: ErrorKindUnavailable.toWire,
			err:  true,
		},
		{
			name: "spool limit",
			wire: SpoolLimitDataBytes.toWire,
			exp:  "dataBytes",
		},
		{
			name: "unknown spool limit",
			wire: SpoolLimit(7).toWire,
			err:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			act, err := test.wire()
			if test.err && err == nil {
				t.Errorf("want error; got nothing")
			}
			if !test.err && err != nil {
				t.Errorf(
					"unexpected error: %v",
					err,
				)
			}
			if act, exp := act, test.exp; act != exp {
				t.Errorf(
					"wire: %q; want %q",
					act, exp,
				)
			}
		})
	}
}

// TestFromJSON covers the wire-to-enum direction through the JSON twins,
// including what each does with a value it does not know: a state fails
// the read, while a spool limit degrades to unknown so the rest of the
// error still reaches the caller. Error kinds are covered by the
// error-response table, where an unknown one keeps its name in the
// message.
func TestFromJSON(t *testing.T) {
	for _, test := range []struct {
		name string
		json string
		into any // A pointer to the twin.
		exp  any // The public value expected in it.
		err  bool
	}{
		{
			name: "queue state",
			json: `"deleting"`,
			into: new(queueState),
			exp:  QueueStateDeleting,
		},
		{
			name: "unknown queue state fails",
			json: `"frozen"`,
			into: new(queueState),
			err:  true,
		},
		{
			name: "queue state that is not a string fails",
			json: `1`,
			into: new(queueState),
			err:  true,
		},
		{
			name: "message state",
			json: `"pending"`,
			into: new(messageState),
			exp:  MessageStatePending,
		},
		{
			name: "unknown message state fails",
			json: `"frozen"`,
			into: new(messageState),
			err:  true,
		},
		{
			name: "message state that is not a string fails",
			json: `1`,
			into: new(messageState),
			err:  true,
		},
		{
			name: "spool limit",
			json: `"messages"`,
			into: new(spoolLimit),
			exp:  SpoolLimitMessages,
		},
		{
			name: "unknown spool limit degrades to unknown",
			json: `"things"`,
			into: new(spoolLimit),
			exp:  SpoolLimitUnknown,
		},
		{
			name: "spool limit that is not a string fails",
			json: `1`,
			into: new(spoolLimit),
			err:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := json.Unmarshal([]byte(test.json), test.into)
			if test.err {
				if err == nil {
					t.Errorf("want error; got nothing")
				}
				return
			}
			if err != nil {
				t.Fatalf(
					"unexpected error: %v",
					err,
				)
			}
			var act any
			switch v := test.into.(type) {
			case *queueState:
				act = QueueState(*v)
			case *messageState:
				act = MessageState(*v)
			case *spoolLimit:
				act = SpoolLimit(*v)
			}
			if act, exp := act, test.exp; act != exp {
				t.Errorf(
					"value: %v; want %v",
					act, exp,
				)
			}
		})
	}
}
