package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loggerutil"
	"github.com/sxwebdev/xutils/workflow"
)

func TestRetry_BackoffContextCancelDuringSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	step, _ := workflow.NewStep("flaky", func(_ *workflow.StepContext) error {
		return errors.New("always fails")
	}, workflow.WithStepRetryPolicy(workflow.RunWithBackoff), workflow.WithStepMaxRetries(10), workflow.WithStepTimeout(time.Second))

	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}

	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	require.Error(t, wf.Run(ctx))
}

func TestOnFailureFn_WithLoggerLogsHandlerError(t *testing.T) {
	step, _ := workflow.NewStep("fail", func(_ *workflow.StepContext) error { return errors.New("boom") })
	wf := workflow.New(
		workflow.WithLogger(loggerutil.NewTestLogger()),
		workflow.WithOnFailureFn(func(_ context.Context, _ *workflow.Workflow, _ error) error {
			return errors.New("handler error") // logged via the configured logger
		}),
	)
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}
	require.Error(t, wf.Run(t.Context()))
}

func TestCompensation_WithSnapshotFn(t *testing.T) {
	failing, _ := workflow.NewStep("fail", func(_ *workflow.StepContext) error { return errors.New("boom") })
	rollbackStep, rollbackRef := workflow.NewStep("Rollback", func(_ *workflow.StepContext) error { return nil })
	compStage, compStageRef := workflow.NewStage("Comp", workflow.WithStageSteps([]*workflow.Step{rollbackStep}))

	var snapshots int
	wf := workflow.New(
		workflow.WithLogger(loggerutil.NewTestLogger()),
		workflow.WithCompensationStage(compStageRef, rollbackRef),
		workflow.WithSnapshotFn(func(_ context.Context, _ *workflow.Workflow, _ workflow.Snapshot) error {
			snapshots++
			return errors.New("snap fail") // exercised on the compensation-routing path
		}),
	)
	wf.Stages = []*workflow.Stage{
		{Name: "Main", Steps: []*workflow.Step{failing}},
		compStage,
	}
	require.NoError(t, wf.Run(t.Context()))
	assert.Positive(t, snapshots)
}

func TestCompensation_MissingStepRefNotSuppressed(t *testing.T) {
	_, realStage := workflow.NewStage("Main")
	_, missingStep := workflow.NewStep("ghoststep", func(_ *workflow.StepContext) error { return nil })

	wf := workflow.New(workflow.WithCompensationStage(realStage, missingStep))
	wf.Stages = []*workflow.Stage{{Name: "Main", Steps: []*workflow.Step{okStep("x")}}}

	err := wf.Run(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compensation step")
}

func TestSuspendMidWorkflow_ExitsAndSkips(t *testing.T) {
	var ran []string
	mk := func(name string, fn workflow.StepFunc) *workflow.Step {
		s, _ := workflow.NewStep(name, fn)
		return s
	}
	wf := workflow.New(workflow.WithDebug(true), workflow.WithLogger(loggerutil.NewTestLogger()))
	wf.Stages = []*workflow.Stage{
		{Name: "s1", Steps: []*workflow.Step{
			mk("suspender", func(sc *workflow.StepContext) error {
				sc.Workflow.State.SetSuspended(true)
				ran = append(ran, "suspender")
				return nil
			}),
			mk("same-stage-after", func(_ *workflow.StepContext) error { ran = append(ran, "same-stage-after"); return nil }),
		}},
		{Name: "s2", Steps: []*workflow.Step{
			mk("next-stage", func(_ *workflow.StepContext) error { ran = append(ran, "next-stage"); return nil }),
		}},
	}
	require.NoError(t, wf.Run(t.Context()))
	assert.Equal(t, []string{"suspender"}, ran,
		"after suspend, no further step (same or next stage) runs")
}

func TestStepStates_BeforeRunSkipsNilState(t *testing.T) {
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{okStep("x")}}}
	// Steps have no State yet (init has not run); StepStates must skip them safely.
	assert.Empty(t, wf.StepStates())
}

func TestCurrentStep_NoValidStatusReturnsFirst(t *testing.T) {
	step := okStep("x")
	step.State = workflow.NewStepState() // empty (invalid) status

	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}

	cur := wf.CurrentStep()
	require.NotNil(t, cur)
	assert.Equal(t, "x", cur.Name)
}
