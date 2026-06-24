package workflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loggerutil"
	"github.com/sxwebdev/xutils/workflow"
)

func TestStepStatus_Enum(t *testing.T) {
	assert.Equal(t, "completed", workflow.StepStatusCompleted.String())
	assert.True(t, workflow.StepStatusFailed.Valid())
	assert.False(t, workflow.StepStatus("bogus").Valid())
	assert.Len(t, workflow.AllStepStatuses(), 6)
}

func TestRefs_Name(t *testing.T) {
	_, stepRef := workflow.NewStep("the-step", func(_ *workflow.StepContext) error { return nil })
	_, stageRef := workflow.NewStage("the-stage")
	assert.Equal(t, "the-step", stepRef.Name())
	assert.Equal(t, "the-stage", stageRef.Name())
}

func TestStep_Setters(t *testing.T) {
	s, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error { return nil })

	fn := func(_ *workflow.StepContext) error { return nil }
	before := func(_ context.Context, _ *workflow.Step) error { return nil }
	after := func(_ context.Context, _ *workflow.Step) error { return nil }

	got := s.SetFunc(fn).
		SetTimeout(5 * time.Second).
		SetMaxRetries(3).
		SetRetryPolicy(workflow.RunWithBackoff).
		SetBeforeFn(before).
		SetAfterFn(after).
		SetStepFinishWorkflow(true)

	assert.Same(t, s, got, "setters are chainable")
	assert.Equal(t, 5*time.Second, s.Timeout)
	assert.Equal(t, 3, s.MaxRetries)
	assert.True(t, s.FinishWorkflow)
	assert.NotNil(t, s.RetryPolicy)
	assert.NotNil(t, s.BeforeFn)
	assert.NotNil(t, s.AfterFn)
}

func TestStepOptions_ArgsAndKind(t *testing.T) {
	s, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error { return nil },
		workflow.WithStepArgs(map[string]int{"a": 1}),
		workflow.WithStepKind("http"),
	)
	assert.Equal(t, "http", s.Kind)
	assert.NotNil(t, s.Args)
}

func TestWorkflow_Setters(t *testing.T) {
	wf := workflow.New()
	got := wf.
		SetName("wf").
		SetState(workflow.WorkflowState{}).
		SetStages(nil).
		SetLogger(loggerutil.NewTestLogger()).
		SetDebug(true).
		SetBeforeFn(func(_ context.Context, _ *workflow.Workflow) error { return nil }).
		SetBeforeAllStepsFn(func(_ context.Context, _ *workflow.Workflow, _ *workflow.Stage, _ *workflow.Step) error { return nil }).
		SetAfterFn(func(_ context.Context, _ *workflow.Workflow) error { return nil }).
		SetOnFailureFn(func(_ context.Context, _ *workflow.Workflow, _ error) error { return nil }).
		SetSkipError(true).
		SetSnapshotFn(func(_ context.Context, _ *workflow.Workflow, _ workflow.Snapshot) error { return nil })

	assert.Same(t, wf, got)
	assert.Equal(t, "wf", wf.Name)
	assert.True(t, wf.Debug)
	assert.NotNil(t, wf.Logger())
}

func TestWorkflowState_Setters(t *testing.T) {
	st := &workflow.WorkflowState{}
	st.SetSuspended(true).SetCompleted(true).SetFailed(true).SetErrorMsg("msg")
	assert.True(t, st.IsSuspended)
	assert.True(t, st.IsCompleted)
	assert.True(t, st.IsFailed)
	assert.Equal(t, "msg", st.Error)

	st.SetCustomError([]byte(`{"code":1}`))
	assert.JSONEq(t, `{"code":1}`, string(st.GetCustomError()))

	// SetError ignores nil, records non-nil.
	st.SetErrorMsg("")
	st.SetError(nil)
	assert.Empty(t, st.Error)
	st.SetError(assertAnError{})
	assert.Equal(t, "an error", st.Error)
}

type assertAnError struct{}

func (assertAnError) Error() string { return "an error" }

func TestInfof_NoLoggerIsSafe(t *testing.T) {
	wf := workflow.New() // no logger
	require.NotPanics(t, func() { wf.Infof("hello %s", "world") })

	wf.SetLogger(loggerutil.NewTestLogger())
	require.NotPanics(t, func() { wf.Infof("hello %s", "world") })
}

// Covers StepState.SetNextStage/SetNextStep and clone's pointer-field branches.
func TestStepState_NextPointersClonedInSnapshot(t *testing.T) {
	step, _ := workflow.NewStep("producer", func(sc *workflow.StepContext) error {
		sc.Step.State.SetNextStage("future-stage").SetNextStep("future-step")
		sc.Step.State.SetArgs(map[string]any{"k": "v"})
		return nil
	})
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}
	require.NoError(t, wf.Run(t.Context()))

	states := wf.StepStates()
	require.Len(t, states, 1)
	require.NotNil(t, states[0].NextStage)
	assert.Equal(t, "future-stage", *states[0].NextStage)
	require.NotNil(t, states[0].NextStep)
	assert.Equal(t, "future-step", *states[0].NextStep)
}
