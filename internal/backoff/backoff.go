package backoff

import (
	"cmp"
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

type Strategy interface {
	// Delay returns a delay duration which caller should wait before
	// performing i-th attempt.
	//
	// Argument is an attempt number.
	// Implementations must respect 0 (which usually return 0 delay).
	Delay(int) time.Duration
}

type Constant time.Duration

func (c Constant) Delay(i int) time.Duration {
	if i == 0 {
		return 0
	}
	return time.Duration(c)
}

const (
	DefaultExponentialBase   = 250 * time.Millisecond
	DefaultExponentialFactor = 1.5
)

type Exponential struct {
	Base   time.Duration
	Factor float64
	Jitter float64 // [0, 1)
	Limit  time.Duration
}

func (e *Exponential) Delay(i int) time.Duration {
	if i == 0 {
		return 0
	}
	base := cmp.Or(max(0, e.Base), DefaultExponentialBase)
	limit := cmp.Or(max(0, e.Limit), math.MaxInt64)
	f := cmp.Or(max(0, e.Factor), DefaultExponentialFactor)
	x := float64(base) * math.Pow(f, float64(i-1))
	if j := e.Jitter; 0 < j && j < 1 {
		x += x * j * 2 * (rand.Float64() - 0.5)
	}
	d := time.Duration(x)
	if d > limit {
		return limit
	}
	return d
}

type StopError struct {
	Err error
}

func Stop(err error) *StopError {
	return &StopError{
		Err: err,
	}
}
func (e *StopError) Error() string {
	return "stop backoff: " + e.Err.Error()
}
func (s *StopError) Unwrap() error {
	return s.Err
}

func Wait(ctx context.Context, s Strategy, i int) error {
	d := s.Delay(i)
	if d == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// Doer runs a retry loop over fn with delays from Strategy. [Do] and [Try]
// are conveniences for the common configurations.
type Doer struct {
	Strategy Strategy

	// MaxAttempts bounds the total number of fn calls (including the initial
	// one); zero means unbounded.
	MaxAttempts int

	// ResetAfter makes an attempt that ran at least this long reset the delay
	// schedule: the next retry happens immediately, like a first attempt.
	// The MaxAttempts bound is not affected.
	// Zero never resets.
	ResetAfter time.Duration
}

// Do calls fn and retries it on error until fn succeeds, ctx is canceled or
// the Attempts bound is exhausted.
//
// On ctx cancellation Do returns the last error fn produced, not ctx.Err().
//
// If fn itself respects ctx, that error usually already carries the
// cancellation reason.
func (d *Doer) Do(ctx context.Context, fn func() error) error {
	var (
		err0  error
		sched int
	)
	for i := 0; d.MaxAttempts <= 0 || i < d.MaxAttempts; i++ {
		if err1 := Wait(ctx, d.Strategy, sched); err1 != nil {
			return cmp.Or(err0, err1)
		}
		start := time.Now()
		if err0 = fn(); err0 == nil {
			return nil
		}
		if stop, ok := errors.AsType[*StopError](err0); ok {
			return stop.Err
		}
		if err1 := ctx.Err(); err1 != nil {
			return cmp.Or(err0, err1)
		}
		if d.ResetAfter > 0 && time.Since(start) >= d.ResetAfter {
			sched = 0
		} else {
			sched++
		}
	}
	return cmp.Or(err0, ErrExhausted)
}

var ErrExhausted = errors.New("exhausted")

// Do calls fn and retries it on error until either fn succeeds or ctx is
// canceled. The wait between attempts is determined by s.Delay(i) where i is
// the attempt number. For bounded retry loop use [Try]; for schedule resets
// on long-running attempts use [Doer].
//
// On ctx cancellation Do returns the last error fn produced, not ctx.Err().
//
// If fn itself respects ctx, that error usually already carries the
// cancellation reason.
func Do(ctx context.Context, s Strategy, fn func() error) error {
	d := Doer{
		Strategy: s,
	}
	return d.Do(ctx, fn)
}

// Try is like [Do] but bounds the total number of attempts to n (including
// the initial call). After n failed attempts Try returns the last error fn
// produced. For unbounded retry loop use [Do].
func Try(ctx context.Context, s Strategy, n int, fn func() error) error {
	if n <= 0 {
		panic("n must be > 0")
	}
	d := Doer{
		Strategy:    s,
		MaxAttempts: n,
	}
	return d.Do(ctx, fn)
}
