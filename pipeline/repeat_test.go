package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sxwebdev/xutils/pipeline"
)

func TestRepeatYieldsAndResumesAtNextIteration(t *testing.T) {
	var actionRuns, afterRuns int
	p := &pipeline.Pipeline{
		Name: "repeat",
		Steps: []pipeline.Step{
			pipeline.Repeat("items", []pipeline.Step{
				pipeline.Action("work", func(context.Context, pipeline.DataAccessor) error {
					actionRuns++
					return nil
				}),
			}, func(_ context.Context, _ pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
				return iteration == 1, 0, nil
			}),
			pipeline.Action("after", func(context.Context, pipeline.DataAccessor) error {
				afterRuns++
				return nil
			}),
		},
	}

	executor := pipeline.NewExecutor()
	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	retryAfter, ok := errors.AsType[*pipeline.ErrRetryAfter](err)
	if !ok {
		t.Fatalf("first run error = %T %v, want *ErrRetryAfter", err, err)
	}
	if retryAfter.Duration != 0 {
		t.Fatalf("retry duration = %s, want 0", retryAfter.Duration)
	}
	if got, want := state.CurrentPath, []string{"items", "1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current path = %v, want %v", got, want)
	}
	if actionRuns != 1 || afterRuns != 0 {
		t.Fatalf("runs after yield = (%d, %d), want (1, 0)", actionRuns, afterRuns)
	}

	state, err = executor.Run(t.Context(), p, state)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if state.Status != pipeline.RunStatusCompleted {
		t.Fatalf("status = %q, want completed", state.Status)
	}
	if actionRuns != 2 || afterRuns != 1 {
		t.Fatalf("final runs = (%d, %d), want (2, 1)", actionRuns, afterRuns)
	}

	wantPaths := [][]string{{"items", "0", "work"}, {"items", "1", "work"}, {"items"}, {"after"}}
	gotPaths := make([][]string, 0, len(state.CompletedSteps))
	for _, completed := range state.CompletedSteps {
		gotPaths = append(gotPaths, completed.Path)
	}
	if !reflect.DeepEqual(gotPaths, wantPaths) {
		t.Fatalf("completed paths = %v, want %v", gotPaths, wantPaths)
	}
}

func TestRepeatResumeDoesNotRerunCompletedAction(t *testing.T) {
	pauseCause := errors.New("dependency not ready")
	var firstRuns, secondRuns int
	p := &pipeline.Pipeline{
		Name: "repeat_resume",
		Steps: []pipeline.Step{
			pipeline.Repeat("loop", []pipeline.Step{
				pipeline.Action("first", func(context.Context, pipeline.DataAccessor) error {
					firstRuns++
					return nil
				}),
				pipeline.Action("second", func(context.Context, pipeline.DataAccessor) error {
					secondRuns++
					if secondRuns == 1 {
						return pipeline.RetryAfter(time.Minute, pauseCause)
					}
					return nil
				}),
			}, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) {
				return true, 0, nil
			}),
		},
	}

	executor := pipeline.NewExecutor()
	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok || !errors.Is(err, pauseCause) {
		t.Fatalf("error = %T %v, want retry signal wrapping cause", err, err)
	}
	if firstRuns != 1 || secondRuns != 1 {
		t.Fatalf("runs before resume = (%d, %d), want (1, 1)", firstRuns, secondRuns)
	}
	if state.StepDiagnostics[pipeline.StepPathKey([]string{"loop", "0", "second"})].NextRunAt == nil {
		t.Fatal("deferred step has no next-run diagnostic")
	}

	state, err = executor.Run(t.Context(), p, state)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if firstRuns != 1 || secondRuns != 2 {
		t.Fatalf("runs after resume = (%d, %d), want (1, 2)", firstRuns, secondRuns)
	}
}

func TestNestedRepeatAndBranchResume(t *testing.T) {
	var actions int
	inner := pipeline.Repeat("inner", []pipeline.Step{
		pipeline.Branch("choice", func(context.Context, pipeline.DataAccessor) (string, error) {
			return "selected", nil
		}, map[string][]pipeline.Step{
			"selected": {
				pipeline.Action("work", func(context.Context, pipeline.DataAccessor) error {
					actions++
					return nil
				}),
			},
		}),
	}, func(_ context.Context, _ pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
		return iteration == 1, 0, nil
	})
	p := &pipeline.Pipeline{
		Name: "nested_repeat",
		Steps: []pipeline.Step{
			pipeline.Repeat("outer", []pipeline.Step{inner}, func(_ context.Context, _ pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
				return iteration == 1, 0, nil
			}),
		},
	}

	state := pipeline.RunState{}
	executor := pipeline.NewExecutor()
	for run := range 10 {
		var err error
		state, err = executor.Run(t.Context(), p, state)
		if err == nil {
			break
		}
		if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok {
			t.Fatalf("run %d: %T %v, want retry signal", run, err, err)
		}
	}
	if state.Status != pipeline.RunStatusCompleted {
		t.Fatalf("status = %q, want completed", state.Status)
	}
	if actions != 4 {
		t.Fatalf("actions = %d, want 4", actions)
	}
}

func TestRepeatCompensatesEveryIterationInReverseOrder(t *testing.T) {
	var compensated []string
	p := &pipeline.Pipeline{
		Name: "repeat_compensation",
		Steps: []pipeline.Step{
			pipeline.Repeat("loop", []pipeline.Step{
				pipeline.Action("a", func(context.Context, pipeline.DataAccessor) error { return nil },
					pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error {
						compensated = append(compensated, "a")
						return nil
					})),
				pipeline.Action("b", func(context.Context, pipeline.DataAccessor) error { return nil },
					pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error {
						compensated = append(compensated, "b")
						return nil
					})),
			}, func(_ context.Context, _ pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
				return iteration == 1, 0, nil
			}),
			pipeline.Action("fail", func(context.Context, pipeline.DataAccessor) error {
				return errors.New("boom")
			}),
		},
	}

	executor := pipeline.NewExecutor()
	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok {
		t.Fatalf("first run error = %v, want retry signal", err)
	}
	state, err = executor.Run(t.Context(), p, state)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if state.Status != pipeline.RunStatusFailed {
		t.Fatalf("status = %q, want failed", state.Status)
	}
	if want := []string{"b", "a", "b", "a"}; !reflect.DeepEqual(compensated, want) {
		t.Fatalf("compensation order = %v, want %v", compensated, want)
	}
}

func TestRepeatMaxIterationsStartsCompensation(t *testing.T) {
	var compensated int
	p := &pipeline.Pipeline{
		Name: "repeat_limit",
		Steps: []pipeline.Step{
			pipeline.Repeat("loop", []pipeline.Step{
				pipeline.Action("work", func(context.Context, pipeline.DataAccessor) error { return nil },
					pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error { compensated++; return nil })),
			}, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) {
				return false, 0, nil
			}, pipeline.WithMaxIterations(1)),
		},
	}

	state, err := pipeline.NewExecutor().Run(t.Context(), p, pipeline.RunState{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if state.Status != pipeline.RunStatusFailed || compensated != 1 {
		t.Fatalf("status/compensations = (%q, %d), want (failed, 1)", state.Status, compensated)
	}
	if state.Error == "" {
		t.Fatal("repeat limit failure was not recorded")
	}
}

func TestRunStateOldJSONRemainsCompatible(t *testing.T) {
	old := []byte(`{"status":"running","current_path":["a"],"completed_steps":[],"data":{"x":1}}`)
	var state pipeline.RunState
	if err := json.Unmarshal(old, &state); err != nil {
		t.Fatalf("unmarshal old state: %v", err)
	}
	if state.Revision != 0 || state.StepDiagnostics != nil || state.RepeatStates != nil {
		t.Fatalf("new optional fields are not zero-valued: %+v", state)
	}

	p := &pipeline.Pipeline{Name: "old", Steps: []pipeline.Step{
		pipeline.Action("a", func(context.Context, pipeline.DataAccessor) error { return nil }),
	}}
	state, err := pipeline.NewExecutor().Run(t.Context(), p, state)
	if err != nil {
		t.Fatalf("resume old state: %v", err)
	}
	if state.Status != pipeline.RunStatusCompleted {
		t.Fatalf("status = %q, want completed", state.Status)
	}
}

func TestStepDiagnosticsAggregateRetries(t *testing.T) {
	var attempts int
	p := &pipeline.Pipeline{Name: "diagnostics", Steps: []pipeline.Step{
		pipeline.Action("unstable", func(context.Context, pipeline.DataAccessor) error {
			attempts++
			if attempts == 1 {
				return errors.New("first failure")
			}
			return nil
		}, pipeline.WithRetry(2, time.Nanosecond, false)),
	}}

	state, err := pipeline.NewExecutor().Run(t.Context(), p, pipeline.RunState{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	diagnostic := state.StepDiagnostics[pipeline.StepPathKey([]string{"unstable"})]
	if diagnostic.Attempts != 2 {
		t.Fatalf("diagnostic attempts = %d, want 2", diagnostic.Attempts)
	}
	if diagnostic.FirstStartedAt == nil || diagnostic.LastStartedAt == nil || diagnostic.CompletedAt == nil {
		t.Fatalf("missing diagnostic timestamps: %+v", diagnostic)
	}
	if diagnostic.LastError != "first failure" {
		t.Fatalf("last error = %q, want first failure", diagnostic.LastError)
	}
	if diagnostic.NextRunAt != nil {
		t.Fatalf("completed step next run = %v, want nil", diagnostic.NextRunAt)
	}
}

func TestStepPathKeyEscapesNames(t *testing.T) {
	if got, want := pipeline.StepPathKey([]string{"a/b", "c~d"}), "/a~1b/c~0d"; got != want {
		t.Fatalf("path key = %q, want %q", got, want)
	}
}

func TestStepPathKeyEmpty(t *testing.T) {
	if got := pipeline.StepPathKey(nil); got != "" {
		t.Fatalf("empty path key = %q, want empty", got)
	}
}

func TestRetryAfterFromEveryForwardCallback(t *testing.T) {
	cause := errors.New("wait")
	tests := []struct {
		name string
		step pipeline.Step
	}{
		{
			name: "on enter",
			step: pipeline.Action("step", func(context.Context, pipeline.DataAccessor) error { return nil },
				pipeline.WithOnEnter(func(context.Context, pipeline.DataAccessor) error {
					return pipeline.RetryAfter(time.Second, cause)
				})),
		},
		{
			name: "poll",
			step: pipeline.Poll("step", func(context.Context, pipeline.DataAccessor) (bool, time.Duration, error) {
				return false, 0, pipeline.RetryAfter(time.Second, cause)
			}),
		},
		{
			name: "branch",
			step: pipeline.Branch("step", func(context.Context, pipeline.DataAccessor) (string, error) {
				return "", pipeline.RetryAfter(time.Second, cause)
			}, map[string][]pipeline.Step{"path": {}}),
		},
		{
			name: "repeat condition",
			step: pipeline.Repeat("step", nil, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) {
				return false, 0, pipeline.RetryAfter(time.Second, cause)
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := &pipeline.Pipeline{Name: test.name, Steps: []pipeline.Step{test.step}}
			state, err := pipeline.NewExecutor().Run(t.Context(), p, pipeline.RunState{})
			if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok || !errors.Is(err, cause) {
				t.Fatalf("error = %T %v, want delayed continuation", err, err)
			}
			if state.Status == pipeline.RunStatusFailed || state.Status == pipeline.RunStatusCompensating {
				t.Fatalf("delayed continuation triggered compensation: %q", state.Status)
			}
		})
	}
}

func TestRepeatRestoresIterationFromRepeatState(t *testing.T) {
	var gotIteration int
	p := &pipeline.Pipeline{Name: "repeat_state", Steps: []pipeline.Step{
		pipeline.Repeat("loop", nil, func(_ context.Context, _ pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
			gotIteration = iteration
			return true, 0, nil
		}),
	}}
	state := pipeline.RunState{
		Status:      pipeline.RunStatusRunning,
		CurrentPath: []string{"loop"},
		RepeatStates: map[string]pipeline.RepeatState{
			pipeline.StepPathKey([]string{"loop"}): {Iteration: 3},
		},
	}

	state, err := pipeline.NewExecutor().Run(t.Context(), p, state)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if gotIteration != 3 || state.Status != pipeline.RunStatusCompleted {
		t.Fatalf("iteration/status = (%d, %q), want (3, completed)", gotIteration, state.Status)
	}
}

func TestRepeatRejectsInvalidSavedIteration(t *testing.T) {
	p := &pipeline.Pipeline{Name: "invalid_repeat_state", Steps: []pipeline.Step{
		pipeline.Repeat("loop", nil, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) {
			return true, 0, nil
		}),
	}}
	state := pipeline.RunState{Status: pipeline.RunStatusRunning, CurrentPath: []string{"loop", "invalid"}}

	state, err := pipeline.NewExecutor().Run(t.Context(), p, state)
	if err != nil {
		t.Fatalf("run engine error: %v", err)
	}
	if state.Status != pipeline.RunStatusFailed || state.Error == "" {
		t.Fatalf("state = status %q/error %q, want failed diagnostic", state.Status, state.Error)
	}
}

func TestRepeatConditionCancellationResumesWithoutRerunningChildren(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var actions, conditions int
	p := &pipeline.Pipeline{Name: "cancel_condition", Steps: []pipeline.Step{
		pipeline.Repeat("loop", []pipeline.Step{
			pipeline.Action("effect", func(context.Context, pipeline.DataAccessor) error { actions++; return nil }),
		}, func(ctx context.Context, _ pipeline.DataAccessor, _ int) (bool, time.Duration, error) {
			conditions++
			if conditions == 1 {
				cancel()
				return false, 0, ctx.Err()
			}
			return true, 0, nil
		}),
	}}

	state, err := pipeline.NewExecutor().Run(ctx, p, pipeline.RunState{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first run error = %v, want context canceled", err)
	}
	repeatState := state.RepeatStates[pipeline.StepPathKey([]string{"loop"})]
	if !repeatState.AwaitingCondition {
		t.Fatalf("repeat state = %+v, want awaiting condition", repeatState)
	}

	state, err = pipeline.NewExecutor().Run(t.Context(), p, state)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if state.Status != pipeline.RunStatusCompleted || actions != 1 || conditions != 2 {
		t.Fatalf("status/actions/conditions = (%q, %d, %d), want (completed, 1, 2)", state.Status, actions, conditions)
	}
}

func TestPollCancellationPreservesTimeoutWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var checks int
	p := &pipeline.Pipeline{Name: "cancel_poll", Steps: []pipeline.Step{
		pipeline.Poll("wait", func(ctx context.Context, _ pipeline.DataAccessor) (bool, time.Duration, error) {
			checks++
			if checks == 1 {
				cancel()
				return false, 0, ctx.Err()
			}
			return true, 0, nil
		}, pipeline.WithMaxPollDuration(time.Hour)),
	}}

	state, err := pipeline.NewExecutor().Run(ctx, p, pipeline.RunState{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first run error = %v, want context canceled", err)
	}
	if state.PollStartedAt == nil {
		t.Fatal("poll start time was cleared by cancellation")
	}
	startedAt := *state.PollStartedAt

	state, err = pipeline.NewExecutor().Run(t.Context(), p, state)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if state.Status != pipeline.RunStatusCompleted || checks != 2 {
		t.Fatalf("status/checks = (%q, %d), want (completed, 2)", state.Status, checks)
	}
	if startedAt.IsZero() {
		t.Fatal("saved poll start time is zero")
	}
}

func TestRepeatValidation(t *testing.T) {
	tests := []pipeline.Step{
		{Name: "nil", Repeat: &pipeline.RepeatStep{}},
		pipeline.Repeat("negative", nil, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) {
			return true, 0, nil
		}, pipeline.WithMaxIterations(-1)),
	}
	for _, step := range tests {
		p := &pipeline.Pipeline{Name: "invalid", Steps: []pipeline.Step{step}}
		if _, err := pipeline.NewExecutor().Run(t.Context(), p, pipeline.RunState{}); err == nil {
			t.Fatalf("step %q unexpectedly validated", step.Name)
		}
	}
}
