package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sxwebdev/xutils/pipeline"
)

func TestCompactRepeatStateRemainsBoundedAcrossTenThousandIterations(t *testing.T) {
	const iterations = 10_000
	p := &pipeline.Pipeline{Name: "bounded", Version: 2, Steps: []pipeline.Step{
		pipeline.Repeat("loop", []pipeline.Step{
			pipeline.Action("work", func(context.Context, pipeline.DataAccessor) error { return nil }),
		}, func(_ context.Context, _ pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
			return iteration == iterations-1, 0, nil
		}, pipeline.WithRepeatHistory(pipeline.RepeatHistoryCompact)),
	}}

	state := pipeline.RunState{}
	executor := pipeline.NewExecutor()
	maxSize := 0
	for run := range iterations {
		var err error
		state, err = executor.Run(t.Context(), p, state)
		if run < iterations-1 {
			if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok {
				t.Fatalf("run %d error = %v", run, err)
			}
		} else if err != nil {
			t.Fatalf("final run error = %v", err)
		}
		encoded, marshalErr := json.Marshal(state)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		maxSize = max(maxSize, len(encoded))
	}

	if state.Status != pipeline.RunStatusCompleted {
		t.Fatalf("status = %q", state.Status)
	}
	if maxSize > 1_500 {
		t.Fatalf("serialized RunState grew to %d bytes", maxSize)
	}
	if got := state.StepDiagnostics[pipeline.StepPathKey([]string{"loop", "work"})].Attempts; got != iterations {
		t.Fatalf("aggregated attempts = %d, want %d", got, iterations)
	}
	if got := state.RepeatStates[pipeline.StepPathKey([]string{"loop"})].Iteration; got != iterations-1 {
		t.Fatalf("repeat iteration = %d, want %d", got, iterations-1)
	}
	for key := range state.StepDiagnostics {
		if strings.Contains(key, "/9999/") || strings.Contains(key, "/0/") {
			t.Fatalf("iteration-specific diagnostic retained: %s", key)
		}
	}
	if len(state.CompletedSteps) != 1 || len(state.CompletedSteps[0].Path) != 1 {
		t.Fatalf("completed steps = %#v", state.CompletedSteps)
	}
}

func TestFullRepeatHistoryRemainsDefault(t *testing.T) {
	p := &pipeline.Pipeline{Name: "full", Steps: []pipeline.Step{
		pipeline.Repeat("loop", []pipeline.Step{
			pipeline.Action("work", func(context.Context, pipeline.DataAccessor) error { return nil }),
		}, func(_ context.Context, _ pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
			return iteration == 2, 0, nil
		}),
	}}
	state := runRepeatToCompletion(t, pipeline.NewExecutor(), p)
	if len(state.CompletedSteps) != 4 {
		t.Fatalf("completed steps = %d, want three children and repeat", len(state.CompletedSteps))
	}
	for iteration := range 3 {
		key := pipeline.StepPathKey([]string{"loop", strconv.Itoa(iteration), "work"})
		if state.StepDiagnostics[key].Attempts != 1 {
			t.Fatalf("missing full diagnostic for iteration %d", iteration)
		}
	}
}

func TestCompactRepeatKeepsIncompleteIterationUntilConditionSucceeds(t *testing.T) {
	deferred := true
	p := &pipeline.Pipeline{Name: "incomplete", Version: 2, Steps: []pipeline.Step{
		pipeline.Repeat("loop", []pipeline.Step{
			pipeline.Action("first", func(context.Context, pipeline.DataAccessor) error { return nil }),
			pipeline.Action("second", func(context.Context, pipeline.DataAccessor) error {
				if deferred {
					deferred = false
					return pipeline.RetryAfter(-time.Second, nil)
				}
				return nil
			}),
		}, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) {
			return false, 0, nil
		}, pipeline.WithRepeatHistory(pipeline.RepeatHistoryCompact)),
	}}
	executor := pipeline.NewExecutor()
	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	retryAfter, ok := errors.AsType[*pipeline.ErrRetryAfter](err)
	if !ok || retryAfter.Duration != 0 {
		t.Fatalf("first Run() error = %v", err)
	}
	if !containsCompletedPath(state, []string{"loop", "0", "first"}) {
		t.Fatalf("incomplete iteration journal was compacted: %#v", state.CompletedSteps)
	}
	if state.StepDiagnostics[pipeline.StepPathKey([]string{"loop", "0", "second"})].Attempts != 1 {
		t.Fatal("incomplete iteration diagnostics were not retained")
	}

	state, err = executor.Run(t.Context(), p, state)
	if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok {
		t.Fatalf("second Run() error = %v", err)
	}
	if containsCompletedPath(state, []string{"loop", "0", "first"}) {
		t.Fatalf("completed iteration journal was not compacted: %#v", state.CompletedSteps)
	}
	if got := state.StepDiagnostics[pipeline.StepPathKey([]string{"loop", "second"})].Attempts; got != 2 {
		t.Fatalf("aggregated second attempts = %d, want 2", got)
	}
}

func TestCompactRepeatRejectsNestedCompensators(t *testing.T) {
	tests := []struct {
		name  string
		steps []pipeline.Step
	}{
		{
			name: "direct",
			steps: []pipeline.Step{pipeline.Action("undoable", func(context.Context, pipeline.DataAccessor) error { return nil },
				pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error { return nil }))},
		},
		{
			name: "branch",
			steps: []pipeline.Step{pipeline.Branch("branch", func(context.Context, pipeline.DataAccessor) (string, error) { return "yes", nil }, map[string][]pipeline.Step{
				"yes": {pipeline.Action("undoable", func(context.Context, pipeline.DataAccessor) error { return nil }, pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error { return nil }))},
			})},
		},
		{
			name: "nested_repeat",
			steps: []pipeline.Step{pipeline.Repeat("inner", []pipeline.Step{
				pipeline.Action("undoable", func(context.Context, pipeline.DataAccessor) error { return nil }, pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error { return nil })),
			}, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) { return true, 0, nil })},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &pipeline.Pipeline{Name: "invalid", Steps: []pipeline.Step{
				pipeline.Repeat("outer", tt.steps, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) { return true, 0, nil }, pipeline.WithRepeatHistory(pipeline.RepeatHistoryCompact)),
			}}
			_, err := pipeline.NewExecutor().Run(t.Context(), p, pipeline.RunState{})
			compactErr, ok := errors.AsType[*pipeline.ErrCompactRepeatCompensation](err)
			if !ok || compactErr.StepName != "outer" || !strings.Contains(compactErr.Error(), "undoable") {
				t.Fatalf("validation error = %#v", err)
			}
		})
	}
}

func TestCompactSnapshotFailureStopsBeforeNextIteration(t *testing.T) {
	cause := errors.New("compact snapshot failed")
	actions := 0
	failedOnce := false
	var durable pipeline.RunState
	p := &pipeline.Pipeline{Name: "fail_stop", Version: 2, Steps: []pipeline.Step{
		pipeline.Repeat("loop", []pipeline.Step{
			pipeline.Action("work", func(context.Context, pipeline.DataAccessor) error { actions++; return nil }),
		}, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) { return false, 0, nil }, pipeline.WithRepeatHistory(pipeline.RepeatHistoryCompact)),
	}}
	executor := pipeline.NewExecutor(pipeline.WithSnapshotFn(func(_ context.Context, state pipeline.RunState) error {
		if len(state.CurrentPath) == 2 && state.CurrentPath[0] == "loop" && state.CurrentPath[1] == "1" && !failedOnce {
			failedOnce = true
			return cause
		}
		encoded, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return json.Unmarshal(encoded, &durable)
	}))

	_, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if _, ok := errors.AsType[*pipeline.ErrSnapshotFailed](err); !ok || !errors.Is(err, cause) {
		t.Fatalf("Run() error = %v", err)
	}
	if actions != 1 {
		t.Fatalf("actions = %d, next iteration ran after failed compaction", actions)
	}

	state, err := executor.Run(t.Context(), p, durable)
	if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok {
		t.Fatalf("resume error = %v", err)
	}
	if actions != 1 {
		t.Fatalf("durable completed child was rerun: actions=%d", actions)
	}
	_, err = executor.Run(t.Context(), p, state)
	if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok || actions != 2 {
		t.Fatalf("next iteration: err=%v actions=%d", err, actions)
	}
}

func runRepeatToCompletion(t *testing.T, executor *pipeline.Executor, p *pipeline.Pipeline) pipeline.RunState {
	t.Helper()
	state := pipeline.RunState{}
	for {
		var err error
		state, err = executor.Run(t.Context(), p, state)
		if err == nil {
			return state
		}
		if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok {
			t.Fatalf("Run() error = %v", err)
		}
	}
}

func containsCompletedPath(state pipeline.RunState, path []string) bool {
	for _, completed := range state.CompletedSteps {
		if len(completed.Path) != len(path) {
			continue
		}
		equal := true
		for i := range path {
			if completed.Path[i] != path[i] {
				equal = false
				break
			}
		}
		if equal {
			return true
		}
	}
	return false
}
