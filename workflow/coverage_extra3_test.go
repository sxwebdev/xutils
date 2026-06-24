package workflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loggerutil"
	"github.com/sxwebdev/xutils/workflow"
)

// Suspend on the last step of a stage so the NEXT stage's entry check short-circuits.
func TestSuspend_ShortCircuitsNextStageEntry(t *testing.T) {
	mk := func(name string, fn workflow.StepFunc) *workflow.Step {
		s, _ := workflow.NewStep(name, fn)
		return s
	}
	var s2ran bool
	wf := workflow.New(workflow.WithDebug(true), workflow.WithLogger(loggerutil.NewTestLogger()))
	wf.Stages = []*workflow.Stage{
		{Name: "s1", Steps: []*workflow.Step{
			mk("suspend", func(sc *workflow.StepContext) error { sc.Workflow.State.SetSuspended(true); return nil }),
		}},
		{Name: "s2", Steps: []*workflow.Step{
			mk("never", func(_ *workflow.StepContext) error { s2ran = true; return nil }),
		}},
	}
	require.NoError(t, wf.Run(t.Context()))
	require.False(t, s2ran)
}

// Debug logging while the workflow state is terminal exercises the completed/failed
// common-log-field branches.
func TestDebugLogging_TerminalStates(t *testing.T) {
	logger := loggerutil.NewTestLogger()
	for _, st := range []workflow.WorkflowState{{IsCompleted: true}, {IsFailed: true}} {
		wf := workflow.New(workflow.WithDebug(true), workflow.WithLogger(logger), workflow.WithState(st))
		wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{okStep("x")}}}
		require.NoError(t, wf.Run(t.Context()))
	}
}

func TestContextCancel_StopsBetweenSteps(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	mk := func(name string, fn workflow.StepFunc) *workflow.Step {
		s, _ := workflow.NewStep(name, fn)
		return s
	}
	var secondRan bool
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{
		mk("canceler", func(_ *workflow.StepContext) error {
			cancel()
			time.Sleep(60 * time.Millisecond) // let Run observe cancellation and set the stop flag
			return nil
		}),
		mk("second", func(_ *workflow.StepContext) error { secondRan = true; return nil }),
	}}}
	require.Error(t, wf.Run(ctx))
	require.False(t, secondRan, "no further step runs once the workflow is stopped")
}

func TestContextCancel_StopsBetweenStages(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	mk := func(name string, fn workflow.StepFunc) *workflow.Step {
		s, _ := workflow.NewStep(name, fn)
		return s
	}
	var stage2Ran bool
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{
		{Name: "s1", Steps: []*workflow.Step{
			mk("canceler", func(_ *workflow.StepContext) error {
				cancel()
				time.Sleep(60 * time.Millisecond)
				return nil
			}),
		}},
		{Name: "s2", Steps: []*workflow.Step{
			mk("second-stage", func(_ *workflow.StepContext) error { stage2Ran = true; return nil }),
		}},
	}
	require.Error(t, wf.Run(ctx))
	require.False(t, stage2Ran, "no further stage runs once the workflow is stopped")
}
