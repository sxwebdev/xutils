package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/workflow"
)

func okStep(name string) *workflow.Step {
	s, _ := workflow.NewStep(name, func(_ *workflow.StepContext) error { return nil })
	return s
}

// --- init validation ---

func TestInit_Errors(t *testing.T) {
	cases := []struct {
		name   string
		build  func() *workflow.Workflow
		errSub string
	}{
		{"nil stage", func() *workflow.Workflow {
			wf := workflow.New()
			wf.Stages = []*workflow.Stage{nil}
			return wf
		}, "is nil"},
		{"duplicate stage", func() *workflow.Workflow {
			wf := workflow.New()
			wf.Stages = []*workflow.Stage{
				{Name: "dup", Steps: []*workflow.Step{okStep("a")}},
				{Name: "dup", Steps: []*workflow.Step{okStep("b")}},
			}
			return wf
		}, "not unique"},
		{"nil step", func() *workflow.Workflow {
			wf := workflow.New()
			wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{nil}}}
			return wf
		}, "is nil"},
		{"duplicate step", func() *workflow.Workflow {
			wf := workflow.New()
			wf.Stages = []*workflow.Stage{
				{Name: "s1", Steps: []*workflow.Step{okStep("dup")}},
				{Name: "s2", Steps: []*workflow.Step{okStep("dup")}},
			}
			return wf
		}, "not unique"},
		{"step without func", func() *workflow.Workflow {
			wf := workflow.New()
			wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{{Name: "nofunc"}}}}
			return wf
		}, "no function"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.build().Run(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errSub)
		})
	}
}

// --- control flow ---

func TestControlFlow_SkipStep(t *testing.T) {
	var ran []string
	mk := func(name string, fn workflow.StepFunc) *workflow.Step {
		s, _ := workflow.NewStep(name, fn)
		return s
	}
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{
		mk("skip", func(_ *workflow.StepContext) error { return workflow.ErrSkipStep }),
		mk("after", func(_ *workflow.StepContext) error { ran = append(ran, "after"); return nil }),
	}}}
	require.NoError(t, wf.Run(t.Context()))
	assert.Equal(t, []string{"after"}, ran, "skip step continues to the next step")
}

func TestControlFlow_SkipStage(t *testing.T) {
	var ran []string
	mk := func(name string, fn workflow.StepFunc) *workflow.Step {
		s, _ := workflow.NewStep(name, fn)
		return s
	}
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{
		{Name: "s1", Steps: []*workflow.Step{
			mk("skipper", func(_ *workflow.StepContext) error { return workflow.ErrSkipStage }),
			mk("same-stage", func(_ *workflow.StepContext) error { ran = append(ran, "same-stage"); return nil }),
		}},
		{Name: "s2", Steps: []*workflow.Step{
			mk("next-stage", func(_ *workflow.StepContext) error { ran = append(ran, "next-stage"); return nil }),
		}},
	}
	require.NoError(t, wf.Run(t.Context()))
	assert.Equal(t, []string{"next-stage"}, ran, "skip stage jumps to the next stage")
}

func TestControlFlow_BreakStages(t *testing.T) {
	var ran []string
	mk := func(name string, fn workflow.StepFunc) *workflow.Step {
		s, _ := workflow.NewStep(name, fn)
		return s
	}
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{
		{Name: "s1", Steps: []*workflow.Step{
			mk("breaker", func(_ *workflow.StepContext) error { return workflow.ErrBreakStages }),
		}},
		{Name: "s2", Steps: []*workflow.Step{
			mk("never", func(_ *workflow.StepContext) error { ran = append(ran, "never"); return nil }),
		}},
	}
	require.NoError(t, wf.Run(t.Context()))
	assert.Empty(t, ran, "break stages stops the workflow")
	assert.True(t, wf.State.IsCompleted)
}

func TestControlFlow_EmptyStageSkipped(t *testing.T) {
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{
		{Name: "empty"}, // no steps
		{Name: "s", Steps: []*workflow.Step{okStep("x")}},
	}
	require.NoError(t, wf.Run(t.Context()))
	assert.True(t, wf.State.IsCompleted)
}

// --- navigation ---

func TestNavigation_GoToStage(t *testing.T) {
	var ran []string
	_, stage3 := workflow.NewStage("Stage 3")
	mk := func(name string, fn workflow.StepFunc) *workflow.Step {
		s, _ := workflow.NewStep(name, fn)
		return s
	}
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{
		{Name: "Stage 1", Steps: []*workflow.Step{
			mk("jump", func(sc *workflow.StepContext) error { sc.Workflow.State.GoToStage(stage3); return nil }),
		}},
		{Name: "Stage 2", Steps: []*workflow.Step{
			mk("skipped", func(_ *workflow.StepContext) error { ran = append(ran, "skipped"); return nil }),
		}},
		{Name: "Stage 3", Steps: []*workflow.Step{
			mk("target", func(_ *workflow.StepContext) error { ran = append(ran, "target"); return nil }),
		}},
	}
	require.NoError(t, wf.Run(t.Context()))
	assert.Equal(t, []string{"target"}, ran, "GoToStage skips the intermediate stage")
}

// --- suspend / resume status ---

func TestSuspendedStepBreaks(t *testing.T) {
	var ran []string
	mk := func(name string, fn workflow.StepFunc) *workflow.Step {
		s, _ := workflow.NewStep(name, fn)
		return s
	}
	suspended := mk("suspended", func(_ *workflow.StepContext) error { ran = append(ran, "suspended"); return nil })
	suspended.State = workflow.NewStepState()
	suspended.State.SetStatus(workflow.StepStatusSuspended)

	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{
		suspended,
		mk("after", func(_ *workflow.StepContext) error { ran = append(ran, "after"); return nil }),
	}}}
	require.NoError(t, wf.Run(t.Context()))
	assert.Empty(t, ran, "a suspended step halts execution without running")
}

func TestCompletedStepSkippedOnResume(t *testing.T) {
	var ran int
	step := okStep("once")
	step.Func = func(_ *workflow.StepContext) error { ran++; return nil }
	step.State = workflow.NewStepState()
	step.State.SetStatus(workflow.StepStatusCompleted)

	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}
	require.NoError(t, wf.Run(t.Context()))
	assert.Equal(t, 0, ran, "already-completed step is skipped on resume")
}

// --- checkStatus short-circuits ---

func TestCheckStatus_TerminalStatesSkip(t *testing.T) {
	for _, st := range []workflow.WorkflowState{
		{IsCompleted: true},
		{IsSuspended: true},
		{IsFailed: true},
	} {
		var ran bool
		step := okStep("x")
		step.Func = func(_ *workflow.StepContext) error { ran = true; return nil }
		wf := workflow.New(workflow.WithState(st))
		wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}
		require.NoError(t, wf.Run(t.Context()))
		assert.False(t, ran, "terminal workflow state must not execute steps")
	}
}

// --- hooks and their error paths ---

func TestHooks_AllLevelsRun(t *testing.T) {
	var order []string
	add := func(s string) { order = append(order, s) }

	step, _ := workflow.NewStep("s1", func(_ *workflow.StepContext) error { add("step"); return nil },
		workflow.WithStepBeforeFn(func(_ context.Context, _ *workflow.Step) error { add("step-before"); return nil }),
		workflow.WithStepAfterFn(func(_ context.Context, _ *workflow.Step) error { add("step-after"); return nil }),
	)
	stage, _ := workflow.NewStage("stage",
		workflow.WithStageSteps([]*workflow.Step{step}),
		workflow.WithStageBeforeFn(func(_ context.Context, _ *workflow.Stage) error { add("stage-before"); return nil }),
		workflow.WithStageAfterFn(func(_ context.Context, _ *workflow.Stage) error { add("stage-after"); return nil }),
	)

	wf := workflow.New(
		workflow.WithBeforeFn(func(_ context.Context, _ *workflow.Workflow) error { add("wf-before"); return nil }),
		workflow.WithAfterFn(func(_ context.Context, _ *workflow.Workflow) error { add("wf-after"); return nil }),
		workflow.WithBeforeAllStepsFn(func(_ context.Context, _ *workflow.Workflow, _ *workflow.Stage, _ *workflow.Step) error {
			add("all-before")
			return nil
		}),
		workflow.WithAfterAllStepsFn(func(_ context.Context, _ *workflow.Workflow, _ *workflow.Stage, _ *workflow.Step) error {
			add("all-after")
			return nil
		}),
	)
	wf.Stages = []*workflow.Stage{stage}

	require.NoError(t, wf.Run(t.Context()))
	assert.Equal(t, []string{
		"wf-before", "stage-before", "all-before", "step-before",
		"step", "step-after", "all-after", "stage-after", "wf-after",
	}, order)
}

func TestHooks_ErrorPaths(t *testing.T) {
	boom := errors.New("boom")
	mkStep := func() *workflow.Step { return okStep("s1") }
	mkWF := func(opt workflow.WorkflowOption, stageOpts ...workflow.StageOption) *workflow.Workflow {
		stage, _ := workflow.NewStage("stage", append([]workflow.StageOption{workflow.WithStageSteps([]*workflow.Step{mkStep()})}, stageOpts...)...)
		wf := workflow.New(opt)
		wf.Stages = []*workflow.Stage{stage}
		return wf
	}

	t.Run("workflow before", func(t *testing.T) {
		wf := mkWF(workflow.WithBeforeFn(func(_ context.Context, _ *workflow.Workflow) error { return boom }))
		require.Error(t, wf.Run(t.Context()))
	})
	t.Run("stage before", func(t *testing.T) {
		wf := mkWF(workflow.WithName("x"), workflow.WithStageBeforeFn(func(_ context.Context, _ *workflow.Stage) error { return boom }))
		require.Error(t, wf.Run(t.Context()))
	})
	t.Run("stage after", func(t *testing.T) {
		wf := mkWF(workflow.WithName("x"), workflow.WithStageAfterFn(func(_ context.Context, _ *workflow.Stage) error { return boom }))
		require.Error(t, wf.Run(t.Context()))
	})
	t.Run("before all steps", func(t *testing.T) {
		wf := mkWF(workflow.WithBeforeAllStepsFn(func(_ context.Context, _ *workflow.Workflow, _ *workflow.Stage, _ *workflow.Step) error { return boom }))
		require.Error(t, wf.Run(t.Context()))
	})
	t.Run("after all steps", func(t *testing.T) {
		wf := mkWF(workflow.WithAfterAllStepsFn(func(_ context.Context, _ *workflow.Workflow, _ *workflow.Stage, _ *workflow.Step) error { return boom }))
		require.Error(t, wf.Run(t.Context()))
	})

	t.Run("step before", func(t *testing.T) {
		step, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error { return nil },
			workflow.WithStepBeforeFn(func(_ context.Context, _ *workflow.Step) error { return boom }))
		require.Error(t, runSingleStep(t, step))
	})
	t.Run("step after", func(t *testing.T) {
		step, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error { return nil },
			workflow.WithStepAfterFn(func(_ context.Context, _ *workflow.Step) error { return boom }))
		require.Error(t, runSingleStep(t, step))
	})

	t.Run("workflow after error is logged not returned", func(t *testing.T) {
		wf := mkWF(workflow.WithAfterFn(func(_ context.Context, _ *workflow.Workflow) error { return boom }))
		require.NoError(t, wf.Run(t.Context()), "AfterFn error is logged, not returned")
		assert.True(t, wf.State.IsCompleted)
	})
}

func TestOnFailureFn_CalledAndErrorLogged(t *testing.T) {
	var called bool
	step, _ := workflow.NewStep("fail", func(_ *workflow.StepContext) error { return errors.New("nope") })
	wf := workflow.New(
		workflow.WithOnFailureFn(func(_ context.Context, _ *workflow.Workflow, _ error) error {
			called = true
			return errors.New("failure handler also errors") // must be logged, not crash
		}),
	)
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}

	require.Error(t, wf.Run(t.Context()))
	assert.True(t, called)
}

// --- compensation full routing ---

func TestCompensation_RoutesAndSuppressesError(t *testing.T) {
	failing, _ := workflow.NewStep("fail", func(_ *workflow.StepContext) error { return errors.New("boom") })
	rollbackStep, rollbackRef := workflow.NewStep("Rollback", func(_ *workflow.StepContext) error { return nil })
	compStage, compStageRef := workflow.NewStage("Comp", workflow.WithStageSteps([]*workflow.Step{rollbackStep}))

	wf := workflow.New(workflow.WithCompensationStage(compStageRef, rollbackRef))
	wf.Stages = []*workflow.Stage{
		{Name: "Main", Steps: []*workflow.Step{failing}},
		compStage,
	}

	require.NoError(t, wf.Run(t.Context()), "compensation suppresses the error")
	assert.NotEmpty(t, wf.State.Error)
	assert.Equal(t, "Comp", wf.State.NextStage)
	assert.Equal(t, "Rollback", wf.State.NextStep)
}

// Regression: a setup/validation error (here, a compensation ref to a missing
// stage) must surface from Run, not be swallowed by the compensation routing.
func TestCompensation_InitErrorNotSuppressed(t *testing.T) {
	_, missingStage := workflow.NewStage("ghost")
	rollbackStep, rollbackRef := workflow.NewStep("Rollback", func(_ *workflow.StepContext) error { return nil })

	wf := workflow.New(workflow.WithCompensationStage(missingStage, rollbackRef))
	wf.Stages = []*workflow.Stage{{Name: "Main", Steps: []*workflow.Step{rollbackStep}}}

	err := wf.Run(t.Context())
	require.Error(t, err, "a bad compensation config must not be silently suppressed")
	assert.Contains(t, err.Error(), "compensation stage")
	assert.NotEqual(t, "ghost", wf.State.NextStage, "must not route to the invalid stage")
}

// --- context cancellation and shutdown ---

func TestContextCancellation_StopsWorkflow(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	step, _ := workflow.NewStep("blocker", func(sc *workflow.StepContext) error {
		<-sc.Done()
		return sc.Err()
	})
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := wf.Run(ctx)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestShutdownTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	step, _ := workflow.NewStep("ignores-ctx", func(_ *workflow.StepContext) error {
		time.Sleep(200 * time.Millisecond) // ignores cancellation
		return nil
	})
	wf := workflow.New(workflow.WithShutdownTimeout(40 * time.Millisecond))
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := wf.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown")
}
