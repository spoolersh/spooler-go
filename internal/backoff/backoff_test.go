package backoff

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestConstantDelay(t *testing.T) {
	for _, test := range []struct {
		name string
		c    Constant
		i    int
		exp  time.Duration
	}{
		{
			name: "zero attempt returns zero",
			c:    Constant(100 * time.Millisecond),
			i:    0,
			exp:  0,
		},
		{
			name: "zero value",
			c:    Constant(0),
			i:    1,
			exp:  0,
		},
		{
			name: "small",
			c:    Constant(100 * time.Millisecond),
			i:    1,
			exp:  100 * time.Millisecond,
		},
		{
			name: "constant across attempts",
			c:    Constant(500 * time.Millisecond),
			i:    42,
			exp:  500 * time.Millisecond,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			act := test.c.Delay(test.i)
			if exp := test.exp; act != exp {
				t.Errorf("delay: %v; want %v", act, exp)
			}
		})
	}
}

func TestExponentialDelay(t *testing.T) {
	for _, test := range []struct {
		name string
		e    Exponential
		i    int
		exp  time.Duration
	}{
		{
			name: "zero attempt returns zero",
			e:    Exponential{},
			i:    0,
			exp:  0,
		},
		{
			name: "defaults at attempt 1",
			e:    Exponential{},
			i:    1,
			exp:  250 * time.Millisecond, // 250ms * 1.5^0 = base
		},
		{
			name: "defaults at attempt 2",
			e:    Exponential{},
			i:    2,
			exp:  375 * time.Millisecond, // 250ms * 1.5^1
		},
		{
			name: "defaults at attempt 3",
			e:    Exponential{},
			i:    3,
			exp:  562_500 * time.Microsecond, // 250ms * 1.5^2
		},
		{
			name: "custom base and factor",
			e: Exponential{
				Base:   100 * time.Millisecond,
				Factor: 2.0,
			},
			i:   3,
			exp: 400 * time.Millisecond, // 100ms * 2^2
		},
		{
			name: "negative base falls back to default",
			e: Exponential{
				Base: -1 * time.Second,
			},
			i:   1,
			exp: 250 * time.Millisecond, // default base, factor^0
		},
		{
			name: "negative factor falls back to default",
			e: Exponential{
				Base:   100 * time.Millisecond,
				Factor: -2.0,
			},
			i:   1,
			exp: 100 * time.Millisecond, // base * default^0 = base
		},
		{
			name: "factor of 1 yields base each time",
			e: Exponential{
				Base:   100 * time.Millisecond,
				Factor: 1.0,
			},
			i:   5,
			exp: 100 * time.Millisecond,
		},
		{
			name: "limit caps the delay",
			e: Exponential{
				Base:   100 * time.Millisecond,
				Factor: 2.0,
				Limit:  300 * time.Millisecond,
			},
			i:   4,
			exp: 300 * time.Millisecond, // unclamped: 100ms * 2^3 = 800ms
		},
		{
			name: "limit not reached returns scaled value",
			e: Exponential{
				Base:   100 * time.Millisecond,
				Factor: 2.0,
				Limit:  1 * time.Second,
			},
			i:   3,
			exp: 400 * time.Millisecond, // 100ms * 2^2
		},
		{
			name: "base above limit clamps to limit",
			e: Exponential{
				Base:  500 * time.Millisecond,
				Limit: 100 * time.Millisecond,
			},
			i:   1,
			exp: 100 * time.Millisecond, // clamped down from 500ms
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			act := test.e.Delay(test.i)
			if exp := test.exp; act != exp {
				t.Errorf("delay: %v; want %v", act, exp)
			}
		})
	}
}

func TestExponentialJitterInRange(t *testing.T) {
	// Base*Factor^(i-1) with i=3 => 100ms*2^2 = 400ms; Jitter=0.5 => ±50% range.
	e := Exponential{
		Base:   100 * time.Millisecond,
		Factor: 2.0,
		Jitter: 0.5,
	}
	const (
		minD = 200 * time.Millisecond // nominal * (1 - jitter)
		maxD = 600 * time.Millisecond // nominal * (1 + jitter)
	)

	for range 1000 {
		act := e.Delay(3)
		if act < minD || act > maxD {
			t.Errorf("delay %v outside [%v, %v]", act, minD, maxD)
		}
	}
}

func TestExponentialJitterDisabled(t *testing.T) {
	// Out-of-range jitter values should be ignored (no randomness applied).
	for _, test := range []struct {
		name   string
		jitter float64
	}{
		{
			name:   "zero",
			jitter: 0,
		},
		{
			name:   "negative",
			jitter: -0.5,
		},
		{
			name:   "one",
			jitter: 1.0,
		},
		{
			name:   "above one",
			jitter: 2.0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := Exponential{
				Base:   100 * time.Millisecond,
				Factor: 2.0,
				Jitter: test.jitter,
			}
			exp := 400 * time.Millisecond // 100ms * 2^2
			for range 50 {
				if act := e.Delay(3); act != exp {
					t.Errorf("delay: %v; want %v (jitter should be inactive)", act, exp)
				}
			}
		})
	}
}

func TestTryPanicsOnNonPositiveN(t *testing.T) {
	for _, test := range []struct {
		name string
		n    int
	}{
		{
			name: "zero",
			n:    0,
		},
		{
			name: "negative",
			n:    -3,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("want panic; got nothing")
				}
			}()
			_ = Try(context.Background(), Constant(0), test.n, func() error {
				return nil
			})
		})
	}
}

func TestDoSuccessOnFirstCall(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Constant(time.Microsecond), func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if act, exp := calls, 1; act != exp {
		t.Errorf("calls: %d; want %d", act, exp)
	}
}

func TestDoSuccessAfterRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		err := Do(t.Context(), Constant(10*time.Millisecond), func() error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if act, exp := calls, 3; act != exp {
			t.Errorf("calls: %d; want %d", act, exp)
		}
	})
}

func TestDoCtxCancelReturnsLastErr(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		wantErr := errors.New("persistent")
		done := make(chan error, 1)
		calls := 0
		go func() {
			done <- Do(ctx, Constant(time.Hour), func() error {
				calls++
				return wantErr
			})
		}()
		// Let Do call fn once and reach the sleep.
		synctest.Wait()
		cancel()
		err := <-done
		if !errors.Is(err, wantErr) {
			t.Errorf("err: %v; want %v", err, wantErr)
		}
		if act, exp := calls, 1; act != exp {
			t.Errorf("calls: %d; want %d", act, exp)
		}
	})
}

func TestTrySuccessWithinLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		err := Try(t.Context(), Constant(10*time.Millisecond), 5, func() error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if act, exp := calls, 3; act != exp {
			t.Errorf("calls: %d; want %d", act, exp)
		}
	})
}

func TestTryExhaustedReturnsLastErr(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wantErr := errors.New("persistent")
		calls := 0
		err := Try(t.Context(), Constant(10*time.Millisecond), 3, func() error {
			calls++
			return wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Errorf("err: %v; want %v", err, wantErr)
		}
		if act, exp := calls, 3; act != exp {
			t.Errorf("calls: %d; want %d", act, exp)
		}
	})
}

func TestTryCtxCancelReturnsLastErr(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		wantErr := errors.New("persistent")
		done := make(chan error, 1)
		calls := 0
		go func() {
			done <- Try(ctx, Constant(time.Hour), 10, func() error {
				calls++
				return wantErr
			})
		}()
		synctest.Wait()
		cancel()
		err := <-done
		if !errors.Is(err, wantErr) {
			t.Errorf("err: %v; want %v", err, wantErr)
		}
		if act, exp := calls, 1; act != exp {
			t.Errorf("calls: %d; want %d", act, exp)
		}
	})
}

func TestStrategyReceivesIncrementingAttempt(t *testing.T) {
	// Verify Strategy.Delay is called with i=0, 1, ..., n-1 once per attempt:
	// 0 precedes the initial call, and strategies map it to no delay.
	var got []int
	s := strategyRecorder{
		fn: func(i int) time.Duration {
			got = append(got, i)
			return 0
		},
	}
	calls := 0
	err := Try(context.Background(), &s, 4, func() error {
		calls++
		return errors.New("always")
	})
	if err == nil {
		t.Errorf("want error; got nothing")
	}
	if act, exp := calls, 4; act != exp {
		t.Errorf("calls: %d; want %d", act, exp)
	}
	for k, v := range got {
		if exp := k; v != exp {
			t.Errorf("Delay arg[%d]: %d; want %d", k, v, exp)
		}
	}
	if act, exp := len(got), 4; act != exp {
		t.Errorf("Delay calls: %d; want %d", act, exp)
	}
}

type strategyRecorder struct {
	fn func(int) time.Duration
}

func (s *strategyRecorder) Delay(i int) time.Duration {
	return s.fn(i)
}

func TestDoerResetAfter(t *testing.T) {
	// Every fn call fails after running for the given duration; the recorded
	// Delay arguments show whether the schedule was reset.
	for _, test := range []struct {
		name      string
		attempts  int
		reset     time.Duration
		runs      []time.Duration
		expDelays []int
	}{
		{
			name:     "no reset",
			attempts: 4,
			runs: []time.Duration{
				0,
				0,
				0,
				0,
			},
			expDelays: []int{0, 1, 2, 3},
		},
		{
			name:     "reset on long run",
			attempts: 6,
			reset:    10 * time.Millisecond,
			runs: []time.Duration{
				0,
				0,
				15 * time.Millisecond,
				0,
				0,
				0,
			},
			expDelays: []int{0, 1, 2, 0, 1, 2},
		},
		{
			name:     "reset does not extend attempts",
			attempts: 3,
			reset:    10 * time.Millisecond,
			runs: []time.Duration{
				15 * time.Millisecond,
				15 * time.Millisecond,
				15 * time.Millisecond,
			},
			expDelays: []int{0, 0, 0},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var delays []int
				d := Doer{
					Strategy: &strategyRecorder{
						fn: func(i int) time.Duration {
							delays = append(delays, i)
							return 0
						},
					},
					MaxAttempts: test.attempts,
					ResetAfter:  test.reset,
				}
				calls := 0
				errAlways := errors.New("always")
				err := d.Do(t.Context(), func() error {
					if run := test.runs[calls]; run > 0 {
						time.Sleep(run)
					}
					calls++
					return errAlways
				})
				// Exhaustion returns the last fn error.
				if expErr := errAlways; !errors.Is(err, expErr) {
					t.Errorf(
						"err: %v; want %v",
						err, expErr,
					)
				}
				if act, exp := calls, test.attempts; act != exp {
					t.Errorf(
						"calls: %d; want %d",
						act, exp,
					)
				}
				if act, exp := delays, test.expDelays; !slices.Equal(act, exp) {
					t.Errorf(
						"delays: %v; want %v",
						act, exp,
					)
				}
			})
		})
	}
}
