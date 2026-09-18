package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSystemClockSleepCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := (systemClock{}).Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep() error = %v", err)
	}
}

func TestCompactRepeatIterationHelperEdges(t *testing.T) {
	earlier := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Hour)
	state := RunState{
		CompletedSteps: []CompletedStep{
			{Path: []string{"setup"}},
			{Path: []string{"loop", "7", "child"}},
		},
		StepDiagnostics: map[string]StepDiagnostics{
			StepPathKey([]string{"setup"}): {Attempts: 1},
			StepPathKey([]string{"loop", "child"}): {
				Attempts:       2,
				FirstStartedAt: &later,
				LastStartedAt:  &later,
				CompletedAt:    &later,
			},
			StepPathKey([]string{"loop", "7", "child"}): {
				Attempts:       3,
				FirstStartedAt: &earlier,
				LastStartedAt:  &earlier,
				CompletedAt:    &earlier,
			},
		},
		RepeatStates: map[string]RepeatState{
			StepPathKey([]string{"loop"}):               {Iteration: 7},
			StepPathKey([]string{"loop", "7", "inner"}): {Completed: true},
		},
	}

	compactRepeatIteration(&state, []string{"loop"}, 7)
	if len(state.CompletedSteps) != 1 || !reflect.DeepEqual(state.CompletedSteps[0].Path, []string{"setup"}) {
		t.Fatalf("completed steps = %#v", state.CompletedSteps)
	}
	if _, ok := state.RepeatStates[StepPathKey([]string{"loop", "7", "inner"})]; ok {
		t.Fatal("nested RepeatState was retained")
	}
	diagnostic := state.StepDiagnostics[StepPathKey([]string{"loop", "child"})]
	if diagnostic.Attempts != 5 || diagnostic.FirstStartedAt == nil || !diagnostic.FirstStartedAt.Equal(earlier) {
		t.Fatalf("merged diagnostic = %#v", diagnostic)
	}
	if path := stepPathFromKey(""); path != nil {
		t.Fatalf("empty key path = %v", path)
	}
	escaped := []string{"a/b", "c~d"}
	if path := stepPathFromKey(StepPathKey(escaped)); !reflect.DeepEqual(path, escaped) {
		t.Fatalf("decoded path = %v", path)
	}
	if got := earlierTime(&earlier, nil); got == nil || !got.Equal(earlier) {
		t.Fatalf("earlierTime(left, nil) = %v", got)
	}
	nextRun := later.Add(time.Minute)
	merged := mergeDiagnostics(
		StepDiagnostics{Attempts: 2, FirstStartedAt: &earlier, LastStartedAt: &earlier, CompletedAt: &earlier, LastError: "old"},
		StepDiagnostics{Attempts: 3, FirstStartedAt: &later, LastStartedAt: &later, CompletedAt: &later, LastError: "new", NextRunAt: &nextRun},
	)
	if merged.Attempts != 5 || !merged.FirstStartedAt.Equal(earlier) || !merged.LastStartedAt.Equal(later) ||
		!merged.CompletedAt.Equal(later) || merged.LastError != "new" || !merged.NextRunAt.Equal(nextRun) {
		t.Fatalf("latest diagnostic merge = %#v", merged)
	}
}

func TestCloneRunStateDeepCopiesMutableFields(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	state := RunState{
		CurrentPath:     []string{"step"},
		FailedStepPath:  []string{"failed"},
		ErrorContext:    json.RawMessage(`{"code":1}`),
		PollStartedAt:   &now,
		RepeatTimeout:   &RepeatTimeoutState{StepName: "loop", MaxDuration: time.Hour},
		CompletedSteps:  []CompletedStep{{Path: []string{"done"}}},
		Data:            map[string]json.RawMessage{"value": json.RawMessage(`{"n":1}`)},
		StepDiagnostics: map[string]StepDiagnostics{"/step": {FirstStartedAt: &now, LastStartedAt: &now, CompletedAt: &now, NextRunAt: &now}},
		RepeatStates:    map[string]RepeatState{"/loop": {StartedAt: &now}},
	}
	cloned := cloneRunState(state)
	cloned.CurrentPath[0] = "changed"
	cloned.FailedStepPath[0] = "changed"
	cloned.ErrorContext[0] = '['
	cloned.CompletedSteps[0].Path[0] = "changed"
	cloned.Data["value"][0] = '['
	delete(cloned.StepDiagnostics, "/step")
	delete(cloned.RepeatStates, "/loop")
	cloned.RepeatTimeout.StepName = "changed"
	if state.CurrentPath[0] != "step" || state.FailedStepPath[0] != "failed" || state.ErrorContext[0] != '{' ||
		state.CompletedSteps[0].Path[0] != "done" || state.Data["value"][0] != '{' ||
		state.StepDiagnostics["/step"].FirstStartedAt == nil || state.RepeatStates["/loop"].StartedAt == nil ||
		state.RepeatTimeout.StepName != "loop" {
		t.Fatalf("clone mutated source: %#v", state)
	}
}

func TestMigrationErrorMessage(t *testing.T) {
	err := (&ErrStateMigrationFailed{PipelineName: "orders", FromVersion: 1, ToVersion: 3, Err: errors.New("bad data")}).Error()
	want := `pipeline "orders": state migration from version 1 to 3 failed: bad data`
	if err != want {
		t.Fatalf("Error() = %q, want %q", err, want)
	}
}

func TestMigrationCancellationBeforeCallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	called := false
	p := &Pipeline{Name: "cancel_migration", Version: 2, Steps: []Step{
		Action("action", func(context.Context, DataAccessor) error { called = true; return nil }),
	}}
	executor := NewExecutor(WithStateMigrator(func(_ context.Context, _ string, _, _ int, state RunState) (RunState, error) {
		cancel()
		state.CurrentPath = []string{"action"}
		return state, nil
	}))
	_, err := executor.Run(ctx, p, RunState{Version: 1, Status: RunStatusRunning})
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("Run() error=%v callback called=%v", err, called)
	}
}

func TestResumeCompensationReturnsPersistedRepeatTimeout(t *testing.T) {
	p := &Pipeline{Name: "resume_timeout", Steps: []Step{
		Action("action", func(context.Context, DataAccessor) error { return nil }),
	}}
	state := RunState{
		Status:            RunStatusCompensating,
		CompensationIndex: -1,
		Error:             "timed out",
		RepeatTimeout:     &RepeatTimeoutState{StepName: "loop", MaxDuration: time.Minute},
	}
	state, err := NewExecutor().Run(t.Context(), p, state)
	timeoutErr, ok := errors.AsType[*ErrRepeatTimeout](err)
	if !ok || timeoutErr.StepName != "loop" || state.Status != RunStatusFailed {
		t.Fatalf("Run() = status %q, err %#v", state.Status, err)
	}
}

func TestMarshalFailuresAtNewCallbackBoundaries(t *testing.T) {
	badValue := make(chan int)
	tests := []struct {
		name string
		step Step
	}{
		{
			name: "branch decision",
			step: Branch("branch", func(_ context.Context, data DataAccessor) (string, error) {
				data.Set("bad", badValue)
				return "path", nil
			}, map[string][]Step{"path": {Action("child", func(context.Context, DataAccessor) error { return nil })}}),
		},
		{
			name: "repeat start",
			step: Repeat("repeat", nil, func(context.Context, DataAccessor, int) (bool, time.Duration, error) {
				return true, 0, nil
			}, WithMaxRepeatDuration(time.Hour), WithOnEnter(func(_ context.Context, data DataAccessor) error {
				data.Set("bad", badValue)
				return nil
			})),
		},
		{
			name: "repeat completion",
			step: Repeat("repeat", nil, func(_ context.Context, data DataAccessor, _ int) (bool, time.Duration, error) {
				data.Set("bad", badValue)
				return true, 0, nil
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewExecutor().Run(t.Context(), &Pipeline{Name: "marshal", Steps: []Step{tt.step}}, RunState{})
			if err == nil {
				t.Fatal("Run() succeeded with non-JSON data")
			}
		})
	}
}

func TestHelperEdgeCases(t *testing.T) {
	executor := NewExecutor(WithClock(newManualClock()))
	if duration := executor.activeStepDuration(RunState{}, []string{"missing"}); duration != 0 {
		t.Fatalf("active duration = %s", duration)
	}
	if duration := completedStepDuration(RunState{}, []string{"missing"}); duration != 0 {
		t.Fatalf("completed duration = %s", duration)
	}
	if got := nextBackoffDelay(time.Duration(1<<62 + 1)); got != time.Duration(1<<63-1) {
		t.Fatalf("saturated delay = %v", got)
	}
	steps := []Step{Repeat("loop", nil, func(context.Context, DataAccessor, int) (bool, time.Duration, error) { return true, 0, nil })}
	if iteration := repeatIterationForPath(steps, []string{"loop", "invalid"}); iteration != nil {
		t.Fatalf("invalid iteration = %v", *iteration)
	}
	if iteration := repeatIterationForPath(steps, []string{"loop", "4"}); iteration == nil || *iteration != 4 {
		t.Fatalf("top-level iteration = %v", iteration)
	}
	nestedSteps := []Step{
		Branch("branch", func(context.Context, DataAccessor) (string, error) { return "path", nil }, map[string][]Step{
			"path": {
				Repeat("loop", []Step{Action("child", func(context.Context, DataAccessor) error { return nil })}, func(context.Context, DataAccessor, int) (bool, time.Duration, error) { return true, 0, nil }),
			},
		}),
	}
	if iteration := repeatIterationForPath(nestedSteps, []string{"branch", "path", "loop", "4", "child"}); iteration == nil || *iteration != 4 {
		t.Fatalf("nested iteration = %v", iteration)
	}
	if iteration := repeatIterationForPath(nestedSteps, []string{"missing"}); iteration != nil {
		t.Fatalf("missing step iteration = %v", iteration)
	}
	if iteration := repeatIterationForPath(nestedSteps, []string{"branch"}); iteration != nil {
		t.Fatalf("branch-only iteration = %v", iteration)
	}
	invalidHistory := RepeatHistoryCompact + 1
	_, err := NewExecutor().Run(t.Context(), &Pipeline{Name: "invalid", Steps: []Step{
		Repeat("loop", nil, func(context.Context, DataAccessor, int) (bool, time.Duration, error) { return true, 0, nil }, WithRepeatHistory(invalidHistory)),
	}}, RunState{})
	if err == nil {
		t.Fatal("invalid history mode was accepted")
	}
}

func TestOnEnterAttemptAccountingForPoll(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		p := &Pipeline{Name: "poll_on_enter", Steps: []Step{
			Poll("poll", func(context.Context, DataAccessor) (bool, time.Duration, error) { return true, 0, nil },
				WithOnEnter(func(context.Context, DataAccessor) error { return nil })),
		}}
		state, err := NewExecutor().Run(t.Context(), p, RunState{})
		if err != nil || state.StepDiagnostics[StepPathKey([]string{"poll"})].Attempts != 1 {
			t.Fatalf("Run() = state %#v, err %v", state, err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		clock := newManualClock()
		startedAt := clock.Now().Add(-time.Minute)
		p := &Pipeline{Name: "poll_on_enter_timeout", Steps: []Step{
			Poll("poll", func(context.Context, DataAccessor) (bool, time.Duration, error) {
				t.Fatal("poll callback ran after timeout")
				return false, 0, nil
			}, WithMaxPollDuration(time.Second), WithOnEnter(func(context.Context, DataAccessor) error { return nil })),
		}}
		state, err := NewExecutor(WithClock(clock)).Run(t.Context(), p, RunState{
			Status:        RunStatusPolling,
			CurrentPath:   []string{"poll"},
			PollStartedAt: &startedAt,
		})
		if err != nil || state.Status != RunStatusFailed || state.StepDiagnostics[StepPathKey([]string{"poll"})].Attempts != 1 {
			t.Fatalf("Run() = state %#v, err %v", state, err)
		}
	})
}

func TestRepeatTimeoutAfterOnEnterAndChildren(t *testing.T) {
	tests := []struct {
		name           string
		advanceOnEnter bool
	}{
		{name: "after on_enter", advanceOnEnter: true},
		{name: "after children"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newManualClock()
			observer := ObserverFunc(func(context.Context, Event) {})
			untilCalls := 0
			p := &Pipeline{Name: "repeat_on_enter_timeout", Steps: []Step{
				Repeat("loop", []Step{
					Action("child", func(context.Context, DataAccessor) error {
						if !tt.advanceOnEnter {
							clock.Advance(time.Second)
						}
						return nil
					}),
				}, func(context.Context, DataAccessor, int) (bool, time.Duration, error) {
					untilCalls++
					return true, 0, nil
				}, WithMaxRepeatDuration(time.Second), WithOnEnter(func(context.Context, DataAccessor) error {
					if tt.advanceOnEnter {
						clock.Advance(time.Second)
					}
					return nil
				})),
			}}
			state, err := NewExecutor(WithClock(clock), WithObserver(observer)).Run(t.Context(), p, RunState{})
			if _, ok := errors.AsType[*ErrRepeatTimeout](err); !ok || state.Status != RunStatusFailed || untilCalls != 0 {
				t.Fatalf("Run() = status %q, err %v, until calls %d", state.Status, err, untilCalls)
			}
		})
	}
}

func TestObserverReceivesCopiedRepeatIteration(t *testing.T) {
	var iterations []*int
	observer := ObserverFunc(func(_ context.Context, event Event) {
		if event.RepeatIteration != nil {
			iterations = append(iterations, event.RepeatIteration)
			*event.RepeatIteration = 99
		}
	})
	p := &Pipeline{Name: "observer_repeat", Steps: []Step{
		Repeat("loop", nil, func(context.Context, DataAccessor, int) (bool, time.Duration, error) { return true, 0, nil }),
	}}
	state, err := NewExecutor(WithObserver(observer)).Run(t.Context(), p, RunState{})
	if err != nil || state.RepeatStates[StepPathKey([]string{"loop"})].Iteration != 0 || len(iterations) == 0 {
		t.Fatalf("Run() = state %#v, err %v, iterations %d", state, err, len(iterations))
	}
}

func TestCloneObserverCauseDetachesPipelineErrors(t *testing.T) {
	sentinel := errors.New("cause")
	tests := []struct {
		name  string
		cause error
	}{
		{name: "snooze", cause: ErrSnooze{Duration: time.Second}},
		{name: "retry after", cause: &ErrRetryAfter{Duration: time.Second, Cause: sentinel}},
		{name: "snapshot", cause: &ErrSnapshotFailed{Operation: "save", Err: sentinel}},
		{name: "repeat limit", cause: &ErrRepeatLimit{StepName: "loop", MaxIterations: 2}},
		{name: "repeat timeout", cause: &ErrRepeatTimeout{StepName: "loop", MaxDuration: time.Minute}},
		{name: "compact compensation", cause: &ErrCompactRepeatCompensation{StepName: "loop", CompensatorPath: []string{"loop", "undo"}}},
		{name: "migration", cause: &ErrStateMigrationFailed{PipelineName: "orders", FromVersion: 1, ToVersion: 2, Err: sentinel}},
		{name: "compensation", cause: &ErrCompensationFailed{Original: sentinel, Compensation: errors.New("undo")}},
		{name: "step", cause: &ErrStepFailed{StepName: "call", Path: []string{"call"}, Err: sentinel}},
		{name: "poll timeout", cause: &ErrPollTimeout{StepName: "wait", MaxDuration: time.Minute}},
		{name: "version mismatch", cause: &ErrVersionMismatch{PipelineName: "orders", StateVersion: 1, PipelineVersion: 2, MinResumeVersion: 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cloned := cloneObserverCause(tt.cause)
			if cloned == nil || cloned.Error() != tt.cause.Error() {
				t.Fatalf("clone = %v, want equivalent to %v", cloned, tt.cause)
			}
			original := reflect.ValueOf(tt.cause)
			copy := reflect.ValueOf(cloned)
			if original.Kind() == reflect.Pointer && original.Pointer() == copy.Pointer() {
				t.Fatal("pipeline error pointer was shared with observer")
			}
		})
	}

	compact := &ErrCompactRepeatCompensation{CompensatorPath: []string{"loop", "undo"}}
	compactClone := cloneObserverCause(compact).(*ErrCompactRepeatCompensation)
	compactClone.CompensatorPath[0] = "changed"
	if compact.CompensatorPath[0] != "loop" {
		t.Fatalf("compact path mutated source: %v", compact.CompensatorPath)
	}

	step := &ErrStepFailed{Path: []string{"call"}, Err: &ErrRepeatTimeout{StepName: "loop", MaxDuration: time.Minute}}
	stepClone := cloneObserverCause(step).(*ErrStepFailed)
	stepClone.Path[0] = "changed"
	stepClone.Err.(*ErrRepeatTimeout).StepName = "changed"
	if step.Path[0] != "call" || step.Err.(*ErrRepeatTimeout).StepName != "loop" {
		t.Fatalf("nested step error mutated source: %#v", step)
	}

	if cloneObserverCause(nil) != nil {
		t.Fatal("nil cause did not remain nil")
	}
	if got := cloneObserverCause(sentinel); got != sentinel {
		t.Fatalf("caller-owned error = %v, want original", got)
	}
}
