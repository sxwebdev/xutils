package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRepeatTimeoutSurvivesRestartAndCompensates(t *testing.T) {
	clock := newManualClock()
	childCalls := 0
	untilCalls := 0
	compensations := 0
	onEnterCalls := 0
	p := &Pipeline{Name: "timeout", Version: 1, Steps: []Step{
		Action("prepare", func(context.Context, DataAccessor) error { return nil }, WithCompensate(func(context.Context, DataAccessor) error {
			compensations++
			return nil
		})),
		Repeat("loop", []Step{
			Action("child", func(context.Context, DataAccessor) error { childCalls++; return nil }),
		}, func(context.Context, DataAccessor, int) (bool, time.Duration, error) {
			untilCalls++
			return false, time.Second, nil
		}, WithMaxRepeatDuration(10*time.Second), WithOnEnter(func(context.Context, DataAccessor) error {
			onEnterCalls++
			return nil
		})),
	}}

	executor := NewExecutor(WithClock(clock))
	state, err := executor.Run(t.Context(), p, RunState{})
	if _, ok := errors.AsType[*ErrRetryAfter](err); !ok {
		t.Fatalf("first Run() error = %v", err)
	}
	startedAt := state.RepeatStates[StepPathKey([]string{"loop"})].StartedAt
	if startedAt == nil {
		t.Fatal("repeat start was not persisted")
	}

	clock.Advance(9 * time.Second)
	state, err = NewExecutor(WithClock(clock)).Run(t.Context(), p, state)
	if _, ok := errors.AsType[*ErrRetryAfter](err); !ok {
		t.Fatalf("second Run() error = %v", err)
	}
	if got := state.RepeatStates[StepPathKey([]string{"loop"})].StartedAt; got == nil || !got.Equal(*startedAt) {
		t.Fatalf("StartedAt reset across restart: got %v, want %v", got, startedAt)
	}

	clock.Advance(time.Second)
	state, err = NewExecutor(WithClock(clock)).Run(t.Context(), p, state)
	timeoutErr, ok := errors.AsType[*ErrRepeatTimeout](err)
	if !ok || timeoutErr.StepName != "loop" || timeoutErr.MaxDuration != 10*time.Second {
		t.Fatalf("timeout error = %#v", err)
	}
	if state.Status != RunStatusFailed || childCalls != 2 || untilCalls != 2 || onEnterCalls != 2 || compensations != 1 {
		t.Fatalf("state=%q child=%d until=%d on_enter=%d compensation=%d", state.Status, childCalls, untilCalls, onEnterCalls, compensations)
	}
}

func TestRepeatTimeoutCheckedBeforeUntil(t *testing.T) {
	clock := newManualClock()
	untilCalls := 0
	compensations := 0
	p := &Pipeline{Name: "timeout_during_iteration", Steps: []Step{
		Repeat("loop", []Step{
			Action("slow", func(context.Context, DataAccessor) error {
				clock.Advance(5 * time.Second)
				return nil
			}, WithCompensate(func(context.Context, DataAccessor) error {
				compensations++
				return nil
			})),
		}, func(context.Context, DataAccessor, int) (bool, time.Duration, error) {
			untilCalls++
			return true, 0, nil
		}, WithMaxRepeatDuration(5*time.Second)),
	}}

	state, err := NewExecutor(WithClock(clock)).Run(t.Context(), p, RunState{})
	if _, ok := errors.AsType[*ErrRepeatTimeout](err); !ok {
		t.Fatalf("Run() error = %v", err)
	}
	if untilCalls != 0 || compensations != 1 || state.Status != RunStatusFailed {
		t.Fatalf("until=%d compensation=%d status=%q", untilCalls, compensations, state.Status)
	}
}

func TestRepeatRetryAfterDoesNotResetTimeout(t *testing.T) {
	clock := newManualClock()
	untilCalls := 0
	p := &Pipeline{Name: "retry_after_timeout", Steps: []Step{
		Repeat("loop", nil, func(context.Context, DataAccessor, int) (bool, time.Duration, error) {
			untilCalls++
			return false, 0, RetryAfter(time.Second, errors.New("not ready"))
		}, WithMaxRepeatDuration(5*time.Second), WithRepeatHistory(RepeatHistoryCompact)),
	}}

	state, err := NewExecutor(WithClock(clock)).Run(t.Context(), p, RunState{})
	if _, ok := errors.AsType[*ErrRetryAfter](err); !ok {
		t.Fatalf("first Run() error = %v", err)
	}
	clock.Advance(5 * time.Second)
	state, err = NewExecutor(WithClock(clock)).Run(t.Context(), p, state)
	if _, ok := errors.AsType[*ErrRepeatTimeout](err); !ok {
		t.Fatalf("second Run() error = %v", err)
	}
	if untilCalls != 1 || state.Status != RunStatusFailed {
		t.Fatalf("until calls=%d status=%q", untilCalls, state.Status)
	}
}

func TestRepeatTransientErrorDoesNotResetTimeout(t *testing.T) {
	clock := newManualClock()
	untilCalls := 0
	p := &Pipeline{Name: "transient_timeout", Steps: []Step{
		Repeat("loop", nil, func(context.Context, DataAccessor, int) (bool, time.Duration, error) {
			untilCalls++
			return false, 0, NoCompensate(errors.New("temporary"))
		}, WithMaxRepeatDuration(5*time.Second)),
	}}

	state, err := NewExecutor(WithClock(clock)).Run(t.Context(), p, RunState{})
	if !errors.Is(err, ErrNoCompensate) || state.Status != RunStatusRunning {
		t.Fatalf("first Run() = status %q, err %v", state.Status, err)
	}
	clock.Advance(5 * time.Second)
	state, err = NewExecutor(WithClock(clock)).Run(t.Context(), p, state)
	if _, ok := errors.AsType[*ErrRepeatTimeout](err); !ok || state.Status != RunStatusFailed || untilCalls != 1 {
		t.Fatalf("second Run() = status %q, err %v, until calls %d", state.Status, err, untilCalls)
	}
}

func TestRepeatCompletionClearsTimeoutState(t *testing.T) {
	clock := newManualClock()
	p := &Pipeline{Name: "complete", Steps: []Step{
		Repeat("loop", nil, func(context.Context, DataAccessor, int) (bool, time.Duration, error) {
			return true, 0, nil
		}, WithMaxRepeatDuration(time.Hour)),
	}}
	state, err := NewExecutor(WithClock(clock)).Run(t.Context(), p, RunState{})
	if err != nil {
		t.Fatal(err)
	}
	repeatState := state.RepeatStates[StepPathKey([]string{"loop"})]
	if !repeatState.Completed || repeatState.StartedAt != nil {
		t.Fatalf("repeat state = %#v", repeatState)
	}
}

func TestRepeatStartSnapshotFailurePreventsChild(t *testing.T) {
	cause := errors.New("storage unavailable")
	childCalls := 0
	onEnterCalls := 0
	p := &Pipeline{Name: "snapshot", Steps: []Step{
		Repeat("loop", []Step{
			Action("child", func(context.Context, DataAccessor) error { childCalls++; return nil }),
		}, func(context.Context, DataAccessor, int) (bool, time.Duration, error) { return true, 0, nil },
			WithMaxRepeatDuration(time.Hour),
			WithOnEnter(func(context.Context, DataAccessor) error { onEnterCalls++; return nil })),
	}}
	executor := NewExecutor(WithSnapshotFn(func(_ context.Context, state RunState) error {
		if state.RepeatStates[StepPathKey([]string{"loop"})].StartedAt != nil {
			return cause
		}
		return nil
	}))
	_, err := executor.Run(t.Context(), p, RunState{})
	if _, ok := errors.AsType[*ErrSnapshotFailed](err); !ok || !errors.Is(err, cause) || childCalls != 0 || onEnterCalls != 0 {
		t.Fatalf("err=%v on_enter calls=%d child calls=%d", err, onEnterCalls, childCalls)
	}
}
