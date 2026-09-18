package pipeline

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

type cancelingClock struct {
	now    time.Time
	cancel context.CancelFunc
}

func (c cancelingClock) Now() time.Time { return c.now }

func (c cancelingClock) Sleep(ctx context.Context, _ time.Duration) error {
	c.cancel()
	return ctx.Err()
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Sleep(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleeps = append(c.sleeps, duration)
	c.now = c.now.Add(duration)
	return nil
}

func (c *manualClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

func (c *manualClock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

func TestDynamicDurationsAreNormalized(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "negative", in: -time.Second, want: 0},
		{name: "zero", in: 0, want: 0},
		{name: "positive", in: 3 * time.Second, want: 3 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("retry_after", func(t *testing.T) {
				err := RetryAfter(tt.in, nil)
				retryAfter, ok := errors.AsType[*ErrRetryAfter](err)
				if !ok || retryAfter.Duration != tt.want {
					t.Fatalf("RetryAfter duration = %v, want %v", retryAfter, tt.want)
				}
			})

			t.Run("poll", func(t *testing.T) {
				clock := newManualClock()
				startedAt := clock.Now()
				p := &Pipeline{Name: "poll", Steps: []Step{
					Poll("wait", func(context.Context, DataAccessor) (bool, time.Duration, error) {
						return false, tt.in, nil
					}),
				}}
				state, err := NewExecutor(WithClock(clock)).Run(t.Context(), p, RunState{})
				snooze, ok := errors.AsType[ErrSnooze](err)
				if !ok || snooze.Duration != tt.want {
					t.Fatalf("poll error = %#v, want duration %s", err, tt.want)
				}
				next := state.StepDiagnostics[StepPathKey([]string{"wait"})].NextRunAt
				if next == nil || !next.Equal(startedAt.Add(tt.want)) {
					t.Fatalf("NextRunAt = %v, want %v", next, startedAt.Add(tt.want))
				}
			})

			t.Run("repeat", func(t *testing.T) {
				clock := newManualClock()
				startedAt := clock.Now()
				p := &Pipeline{Name: "repeat", Steps: []Step{
					Repeat("loop", nil, func(context.Context, DataAccessor, int) (bool, time.Duration, error) {
						return false, tt.in, nil
					}),
				}}
				state, err := NewExecutor(WithClock(clock)).Run(t.Context(), p, RunState{})
				retryAfter, ok := errors.AsType[*ErrRetryAfter](err)
				if !ok || retryAfter.Duration != tt.want {
					t.Fatalf("repeat error = %#v, want duration %s", err, tt.want)
				}
				next := state.StepDiagnostics[StepPathKey([]string{"loop"})].NextRunAt
				if next == nil || !next.Equal(startedAt.Add(tt.want)) {
					t.Fatalf("NextRunAt = %v, want %v", next, startedAt.Add(tt.want))
				}
			})
		})
	}
}

func TestNegativeSnoozeCallbackErrorIsNormalized(t *testing.T) {
	clock := newManualClock()
	startedAt := clock.Now()
	p := &Pipeline{Name: "legacy_snooze", Steps: []Step{
		Poll("wait", func(context.Context, DataAccessor) (bool, time.Duration, error) {
			return false, 0, ErrSnooze{Duration: -time.Second}
		}),
	}}

	state, err := NewExecutor(WithClock(clock)).Run(t.Context(), p, RunState{})
	snooze, ok := errors.AsType[ErrSnooze](err)
	if !ok || snooze.Duration != 0 {
		t.Fatalf("Run() error = %#v, want zero-duration ErrSnooze", err)
	}
	next := state.StepDiagnostics[StepPathKey([]string{"wait"})].NextRunAt
	if next == nil || !next.Equal(startedAt) {
		t.Fatalf("NextRunAt = %v, want %v", next, startedAt)
	}
}

func TestClockControlsRetryWaitsAndDiagnostics(t *testing.T) {
	clock := newManualClock()
	startedAt := clock.Now()
	attempts := 0
	p := &Pipeline{Name: "retry", Steps: []Step{
		Action("call", func(context.Context, DataAccessor) error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary")
			}
			return nil
		}, WithRetry(3, 2*time.Second, true)),
	}}

	state, err := NewExecutor(WithClock(clock)).Run(t.Context(), p, RunState{})
	if err != nil || state.Status != RunStatusCompleted {
		t.Fatalf("Run() = status %q, err %v", state.Status, err)
	}
	if got, want := clock.Sleeps(), []time.Duration{2 * time.Second, 4 * time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleeps = %v, want %v", got, want)
	}
	diagnostic := state.StepDiagnostics[StepPathKey([]string{"call"})]
	if diagnostic.Attempts != 3 || diagnostic.FirstStartedAt == nil || !diagnostic.FirstStartedAt.Equal(startedAt) {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
	if diagnostic.CompletedAt == nil || !diagnostic.CompletedAt.Equal(startedAt.Add(6*time.Second)) {
		t.Fatalf("CompletedAt = %v, want %v", diagnostic.CompletedAt, startedAt.Add(6*time.Second))
	}
}

func TestNegativeStaticDurationsFailValidation(t *testing.T) {
	tests := []struct {
		name string
		step Step
	}{
		{name: "retry", step: Action("action", func(context.Context, DataAccessor) error { return nil }, WithRetry(2, -time.Second, false))},
		{name: "poll", step: Poll("poll", func(context.Context, DataAccessor) (bool, time.Duration, error) { return true, 0, nil }, WithMaxPollDuration(-time.Second))},
		{name: "repeat", step: Repeat("repeat", nil, func(context.Context, DataAccessor, int) (bool, time.Duration, error) { return true, 0, nil }, WithMaxRepeatDuration(-time.Second))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			if tt.step.Action != nil {
				tt.step.Action.Do = func(context.Context, DataAccessor) error { called = true; return nil }
			}
			_, err := NewExecutor().Run(t.Context(), &Pipeline{Name: "invalid", Steps: []Step{tt.step}}, RunState{})
			if err == nil || called {
				t.Fatalf("Run() err = %v, callback called = %v", err, called)
			}
		})
	}
}
