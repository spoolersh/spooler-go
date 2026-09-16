package spooler

import (
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"time"
)

type (
	// MessageStats counts messages by state, and cumulative totals since the
	// server last restarted.
	MessageStats struct {
		// Total is the stored messages, every state counted.
		Total int64

		// DataBytes is the payload bytes of the counted messages.
		DataBytes int64

		// Delayed is the messages delayed.
		Delayed int64

		// Visible is the messages visible.
		Visible int64

		// Pending is the messages leased.
		Pending int64

		// Failed is the messages failed.
		Failed int64

		// Unavailable is the messages whose payload the service could not
		// read.
		Unavailable int64

		// AddedTotal is the messages sent, duplicates excluded.
		AddedTotal int64

		// AckedTotal is the messages acked.
		AckedTotal int64

		// NackedTotal is the messages nacked.
		NackedTotal int64

		// ReleasedTotal is the deliveries released.
		ReleasedTotal int64

		// FailedTotal is the messages failed.
		FailedTotal int64

		// DiscardedTotal is the messages discarded.
		DiscardedTotal int64

		// ExpiredTotal is the messages discarded by retention.
		ExpiredTotal int64
	}
	messageStats struct {
		Total          int64 `json:"total"`
		DataBytes      int64 `json:"dataBytes"`
		Delayed        int64 `json:"delayed"`
		Visible        int64 `json:"visible"`
		Pending        int64 `json:"pending"`
		Failed         int64 `json:"failed"`
		Unavailable    int64 `json:"unavailable"`
		AddedTotal     int64 `json:"addedTotal"`
		AckedTotal     int64 `json:"ackedTotal"`
		NackedTotal    int64 `json:"nackedTotal"`
		ReleasedTotal  int64 `json:"releasedTotal"`
		FailedTotal    int64 `json:"failedTotal"`
		DiscardedTotal int64 `json:"discardedTotal"`
		ExpiredTotal   int64 `json:"expiredTotal"`
	}
)

func (m *messageStats) toMessageStats() MessageStats {
	return MessageStats{
		Total:          m.Total,
		DataBytes:      m.DataBytes,
		Delayed:        m.Delayed,
		Visible:        m.Visible,
		Pending:        m.Pending,
		Failed:         m.Failed,
		Unavailable:    m.Unavailable,
		AddedTotal:     m.AddedTotal,
		AckedTotal:     m.AckedTotal,
		NackedTotal:    m.NackedTotal,
		ReleasedTotal:  m.ReleasedTotal,
		FailedTotal:    m.FailedTotal,
		DiscardedTotal: m.DiscardedTotal,
		ExpiredTotal:   m.ExpiredTotal,
	}
}

type (
	// RateLimit is a token bucket rate limit.
	// Zero means no limit.
	RateLimit struct {
		// IntervalMicros is the refill interval in microseconds.
		IntervalMicros int64

		// Burst is the bucket size.
		Burst int
	}
	rateLimit struct {
		IntervalMicros int64 `json:"intervalMicros"`
		Burst          int   `json:"burst"`
	}
)

func (r *RateLimit) wire() rateLimit {
	return rateLimit(*r)
}
func (r *rateLimit) toRateLimit() RateLimit {
	if r == nil {
		return RateLimit{}
	}
	return RateLimit(*r)
}

// IsZero reports whether r sets no limit. A limit needs both fields.
func (r RateLimit) IsZero() bool {
	return r.IntervalMicros == 0 || r.Burst == 0
}

type (
	// QueueSettings is a queue's settings as applied, every field present.
	QueueSettings struct {
		// LeaseTimeoutSeconds is the lease timeout in seconds.
		// Zero means leases never expire.
		LeaseTimeoutSeconds int

		// MaxRetries is the redeliveries a message gets before it is failed.
		// Zero means none.
		MaxRetries int

		// DedupWindowSeconds is the dedup window in seconds.
		// Zero means dedup is off.
		DedupWindowSeconds int

		// RetentionSeconds is the retention in seconds, as set.
		RetentionSeconds int

		// EffectiveRetentionSeconds is the retention in seconds, as applied.
		EffectiveRetentionSeconds int

		// RecvRateLimit is the rate limit on receives.
		// Zero means no limit.
		RecvRateLimit RateLimit
	}
	queueSettings struct {
		LeaseTimeoutSeconds       int        `json:"leaseTimeoutSeconds"`
		MaxRetries                int        `json:"maxRetries"`
		DedupWindowSeconds        int        `json:"dedupWindowSeconds"`
		RetentionSeconds          int        `json:"retentionSeconds"`
		EffectiveRetentionSeconds int        `json:"effectiveRetentionSeconds"`
		RecvRateLimit             *rateLimit `json:"recvRateLimit"`
	}
)

func (s *queueSettings) toQueueSettings() QueueSettings {
	return QueueSettings{
		LeaseTimeoutSeconds:       s.LeaseTimeoutSeconds,
		MaxRetries:                s.MaxRetries,
		DedupWindowSeconds:        s.DedupWindowSeconds,
		RetentionSeconds:          s.RetentionSeconds,
		EffectiveRetentionSeconds: s.EffectiveRetentionSeconds,
		RecvRateLimit:             s.RecvRateLimit.toRateLimit(),
	}
}

type (
	// QueueSettingsCreate is the settings a queue is created with. A nil
	// field takes the server's default.
	QueueSettingsCreate struct {
		// LeaseTimeoutSeconds is the lease timeout in seconds; a value of 0
		// disables it.
		// Zero means the server's default.
		LeaseTimeoutSeconds *int

		// MaxRetries is the redeliveries a message gets before it is failed;
		// a value of 0 means none.
		// Zero means the server's default.
		MaxRetries *int

		// DedupWindowSeconds is the dedup window in seconds; a value of 0
		// disables dedup.
		// Zero means the server's default.
		DedupWindowSeconds *int

		// RetentionSeconds is the retention in seconds.
		// Zero means the server's default.
		RetentionSeconds *int

		// RecvRateLimit is the rate limit on receives.
		// Zero means no limit.
		RecvRateLimit *RateLimit
	}

	// QueueSettingsUpdate is the settings to replace on a queue. A nil
	// field keeps its current value; at least one must be set.
	QueueSettingsUpdate struct {
		// LeaseTimeoutSeconds is the lease timeout in seconds; a value of 0
		// disables it.
		// Zero keeps the current value.
		LeaseTimeoutSeconds *int

		// MaxRetries is the redeliveries a message gets before it is failed;
		// a value of 0 means none.
		// Zero keeps the current value.
		MaxRetries *int

		// DedupWindowSeconds is the dedup window in seconds; a value of 0
		// disables dedup.
		// Zero keeps the current value.
		DedupWindowSeconds *int

		// RetentionSeconds is the retention in seconds.
		// Zero keeps the current value.
		RetentionSeconds *int

		// RecvRateLimit is the rate limit on receives; a pointer to the zero
		// RateLimit removes it.
		// Zero keeps the current value.
		RecvRateLimit *RateLimit
	}

	// queueSettingsWrite is the wire form of both: absent fields are absent.
	queueSettingsWrite struct {
		LeaseTimeoutSeconds *int       `json:"leaseTimeoutSeconds,omitempty"`
		MaxRetries          *int       `json:"maxRetries,omitempty"`
		DedupWindowSeconds  *int       `json:"dedupWindowSeconds,omitempty"`
		RetentionSeconds    *int       `json:"retentionSeconds,omitempty"`
		RecvRateLimit       *rateLimit `json:"recvRateLimit,omitempty"`
	}
)

func (s *QueueSettingsCreate) wire() queueSettingsWrite {
	return settingsWire(s.LeaseTimeoutSeconds, s.MaxRetries, s.DedupWindowSeconds, s.RetentionSeconds, s.RecvRateLimit)
}

func (s *QueueSettingsUpdate) wire() queueSettingsWrite {
	return settingsWire(s.LeaseTimeoutSeconds, s.MaxRetries, s.DedupWindowSeconds, s.RetentionSeconds, s.RecvRateLimit)
}

func settingsWire(lease, retries, dedup, retention *int, limit *RateLimit) queueSettingsWrite {
	ret := queueSettingsWrite{
		LeaseTimeoutSeconds: lease,
		MaxRetries:          retries,
		DedupWindowSeconds:  dedup,
		RetentionSeconds:    retention,
	}
	if limit != nil {
		r := limit.wire()
		ret.RecvRateLimit = &r
	}
	return ret
}

// QueueDeleteMode is how [Client.DeleteQueue] deletes a queue.
type QueueDeleteMode uint

const (
	// QueueDeleteModeUnknown is the zero value and no mode; a request
	// carrying it is refused before sending.
	QueueDeleteModeUnknown QueueDeleteMode = iota
	// QueueDeleteModeForce deletes the queue and its messages at once.
	QueueDeleteModeForce
	// QueueDeleteModeDrain deletes the queue once consumers have emptied it.
	QueueDeleteModeDrain
)

var (
	queueDeleteMode2wire = map[QueueDeleteMode]string{
		QueueDeleteModeForce: "force",
		QueueDeleteModeDrain: "drain",
	}
	queueDeleteMode4wire = reverse(queueDeleteMode2wire)
)

func (q QueueDeleteMode) toWire() (string, error)  { return toWire(queueDeleteMode2wire, q) }
func (q *QueueDeleteMode) fromWire(s string) error { return fromWire(queueDeleteMode4wire, q, s) }

// String returns the mode's name as the API spells it, or QueueDeleteMode(n)
// for a value the SDK does not define.
func (q QueueDeleteMode) String() string {
	return enumString("QueueDeleteMode", queueDeleteMode2wire, q)
}

// QueueState is the state a queue is in.
type QueueState uint

const (
	// QueueStateUnknown is the zero value and no state.
	QueueStateUnknown QueueState = iota
	// QueueStateActive is a queue in service.
	QueueStateActive
	// QueueStateDeleting is a queue being deleted.
	QueueStateDeleting
	// QueueStateDraining is a queue being emptied before its deletion.
	QueueStateDraining
)

var (
	queueState2wire = map[QueueState]string{
		QueueStateActive:   "active",
		QueueStateDeleting: "deleting",
		QueueStateDraining: "draining",
	}
	queueState4wire = reverse(queueState2wire)
)

func (s QueueState) toWire() (string, error)  { return toWire(queueState2wire, s) }
func (s *QueueState) fromWire(w string) error { return fromWire(queueState4wire, s, w) }

// String returns the state's name as the API spells it, or QueueState(n) for
// a value the SDK does not define.
func (s QueueState) String() string { return enumString("QueueState", queueState2wire, s) }

type queueState QueueState

func (q *queueState) UnmarshalJSON(p []byte) error {
	var s string
	if err := json.Unmarshal(p, &s); err != nil {
		return err
	}
	return (*QueueState)(q).fromWire(s)
}

type (
	// QueueStats is a queue with its message counts, as
	// [Client.SpoolStats] lists them.
	QueueStats struct {
		// Name is the queue's name.
		Name string

		// CreatedAt is when the queue was created.
		CreatedAt time.Time

		// State is the state the queue is in.
		State QueueState

		// Settings is the queue's settings, as applied.
		Settings QueueSettings

		// Stats is the queue's message counts.
		Stats MessageStats
	}
	queueStats struct {
		Name      string        `json:"name"`
		CreatedAt time.Time     `json:"createdAt"`
		State     queueState    `json:"state"`
		Settings  queueSettings `json:"settings"`
		Stats     messageStats  `json:"stats"`
	}
)

func (q *queueStats) toQueueStats() QueueStats {
	return QueueStats{
		Name:      q.Name,
		CreatedAt: q.CreatedAt,
		State:     QueueState(q.State),
		Settings:  q.Settings.toQueueSettings(),
		Stats:     q.Stats.toMessageStats(),
	}
}

type (
	// Queue is a queue as [Client.Queue] and [Client.SpoolQueues] describe
	// it.
	Queue struct {
		// Name is the queue's name.
		Name string

		// CreatedAt is when the queue was created.
		CreatedAt time.Time

		// State is the state the queue is in.
		State QueueState

		// Settings is the queue's settings, as applied.
		Settings QueueSettings
	}
	queue struct {
		Name      string        `json:"name"`
		CreatedAt time.Time     `json:"createdAt"`
		State     queueState    `json:"state"`
		Settings  queueSettings `json:"settings"`
	}
)

func (q *queue) toQueue() Queue {
	return Queue{
		Name:      q.Name,
		CreatedAt: q.CreatedAt,
		State:     QueueState(q.State),
		Settings:  q.Settings.toQueueSettings(),
	}
}

type (
	// Page is one page of a listing. Every listing is paged the same way:
	// pass Next as the next request's After until it comes back empty.
	Page[T any] struct {
		// Items is the page.
		Items []T
		// Next is the cursor of the following page; empty when this page
		// ends the listing.
		Next string
	}
	page[T any] struct {
		Items []T     `json:"items"`
		Next  *string `json:"next,omitempty"`
	}
)

func (p *page[A]) toPage[B any](conv func(*A) B) Page[B] {
	bs := make([]B, len(p.Items))
	for i := range p.Items {
		bs[i] = conv(&p.Items[i])
	}
	ret := Page[B]{
		Items: bs,
	}
	if next := p.Next; next != nil {
		ret.Next = *next
	}
	return ret
}

type (
	// SpoolStats is the live view of a spool that [Client.SpoolStats] reads.
	SpoolStats struct {
		// QueueCount is the number of queues on the spool, whatever the
		// page selects.
		QueueCount int64

		// Stats is the message counts rolled up across every queue.
		Stats MessageStats

		// Queues is one page of the queues, each with its counts.
		Queues Page[QueueStats]
	}
	spoolStats struct {
		QueueCount int64            `json:"queueCount"`
		Stats      messageStats     `json:"stats"`
		Queues     page[queueStats] `json:"queues"`
	}
)

func (s *spoolStats) toSpoolStats() (ret SpoolStats) {
	return SpoolStats{
		QueueCount: s.QueueCount,
		Stats:      s.Stats.toMessageStats(),
		Queues:     s.Queues.toPage((*queueStats).toQueueStats),
	}
}

// MessageState is the state a message is in.
type MessageState uint

const (
	// MessageStateUnknown is the zero value and no state.
	MessageStateUnknown MessageState = iota
	// MessageStateDelayed is a message not yet visible.
	MessageStateDelayed
	// MessageStateVisible is a message waiting for a receive.
	MessageStateVisible
	// MessageStatePending is a message leased to a receiver.
	MessageStatePending
	// MessageStateFailed is a message failed, waiting for a recover or a
	// discard.
	MessageStateFailed
	// MessageStateUnavailable is a message whose payload the service could
	// not read.
	MessageStateUnavailable
)

var (
	messageState2wire = map[MessageState]string{
		MessageStateDelayed:     "delayed",
		MessageStateVisible:     "visible",
		MessageStatePending:     "pending",
		MessageStateFailed:      "failed",
		MessageStateUnavailable: "unavailable",
	}
	messageState4wire = reverse(messageState2wire)
)

func (s MessageState) toWire() (string, error)  { return toWire(messageState2wire, s) }
func (s *MessageState) fromWire(w string) error { return fromWire(messageState4wire, s, w) }

// String returns the state's name as the API spells it, or MessageState(n)
// for a value the SDK does not define.
func (s MessageState) String() string { return enumString("MessageState", messageState2wire, s) }

type messageState MessageState

func (q *messageState) UnmarshalJSON(p []byte) error {
	var s string
	if err := json.Unmarshal(p, &s); err != nil {
		return err
	}
	return (*MessageState)(q).fromWire(s)
}

type (
	// Message is a message as [Client.QueueMessages] lists it.
	Message struct {
		// ID is the message id.
		ID string

		// State is the state the message is in.
		State MessageState

		// Retries is the redeliveries so far.
		Retries int

		// CreatedAt is when the message was sent.
		CreatedAt time.Time

		// VisibleAt is when a delayed message becomes visible.
		// Zero means the message is not delayed.
		VisibleAt time.Time

		// LeaseExpiresAt is when the current lease expires.
		// Zero means no lease, or one that never expires.
		LeaseExpiresAt time.Time

		// ExpiresAt is when retention discards the message.
		// Zero means retention is off.
		ExpiresAt time.Time

		// Handle is the message's handle.
		// Zero means the listing did not ask for one with ReturnHandle.
		Handle Handle
	}
	message struct {
		ID             string       `json:"id"`
		State          messageState `json:"state"`
		Retries        int          `json:"retries"`
		CreatedAt      time.Time    `json:"createdAt"`
		VisibleAt      *time.Time   `json:"visibleAt,omitempty"`
		LeaseExpiresAt *time.Time   `json:"leaseExpiresAt,omitempty"`
		ExpiresAt      *time.Time   `json:"expiresAt,omitempty"`
		Handle         *string      `json:"handle,omitempty"`
	}
)

func (m *message) toMessage() Message {
	ret := Message{
		ID:        m.ID,
		State:     MessageState(m.State),
		Retries:   m.Retries,
		CreatedAt: m.CreatedAt,
	}
	if t := m.VisibleAt; t != nil {
		ret.VisibleAt = *t
	}
	if t := m.LeaseExpiresAt; t != nil {
		ret.LeaseExpiresAt = *t
	}
	if t := m.ExpiresAt; t != nil {
		ret.ExpiresAt = *t
	}
	if s := m.Handle; s != nil {
		ret.Handle = Handle(*s)
	}
	return ret
}

// enumString names an enum value as the wire does, or, for one the SDK does
// not define, as the standard library's stringers do: Type(value).
func enumString[T ~uint](typ string, m map[T]string, v T) string {
	if s, has := m[v]; has {
		return s
	}
	return typ + "(" + strconv.FormatUint(uint64(v), 10) + ")"
}

func reverse[K, V comparable](m map[K]V) map[V]K {
	ret := make(map[V]K, len(m))
	for k, v := range m {
		ret[v] = k
	}
	return ret
}
func transform[K comparable, A, B any](m map[K]A, f func(A) B) map[K]B {
	ret := make(map[K]B, len(m))
	for k, a := range m {
		ret[k] = f(a)
	}
	return ret
}
func merge[K comparable, V any](ms ...map[K]V) map[K]V {
	var n int
	for _, m := range ms {
		n += len(m)
	}
	ret := make(map[K]V, n)
	for _, m := range ms {
		maps.Copy(ret, m)
	}
	return ret
}

func toWire[T, W comparable](m map[T]W, v T) (w W, err error) {
	ret, has := m[v]
	if !has {
		return w, fmt.Errorf("unknown %T", v)
	}
	return ret, nil
}
func fromWire[T, W comparable](m map[W]T, v *T, w W) error {
	ret, has := m[w]
	if !has {
		var t T
		return fmt.Errorf("unexpected %T: %v", t, w)
	}
	*v = ret
	return nil
}
