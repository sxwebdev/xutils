package workflow_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/workflow"
)

// Regression: a step that returns ErrExitWorkflow performs a clean exit
// (Run returns nil) and must not be recorded as failed.
func TestExitWorkflow_StepNotMarkedFailed(t *testing.T) {
	exitStep, _ := workflow.NewStep("exiter", func(_ *workflow.StepContext) error {
		return workflow.ErrExitWorkflow
	})
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "S", Steps: []*workflow.Step{exitStep}}}

	require.NoError(t, wf.Run(t.Context()))
	require.NotEqual(t, workflow.StepStatusFailed, exitStep.State.Status,
		"clean exit must not mark the step failed")
	require.Empty(t, exitStep.State.Error)
}

// Regression: a FinishWorkflow step that ran successfully must be recorded as
// completed, not skipped.
func TestFinishWorkflow_StepCompleted(t *testing.T) {
	finStep, _ := workflow.NewStep("finisher", func(_ *workflow.StepContext) error {
		return nil
	})
	finStep.FinishWorkflow = true

	nextStep, _ := workflow.NewStep("next", func(_ *workflow.StepContext) error {
		t.Error("step after FinishWorkflow must not run")
		return nil
	})

	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "S", Steps: []*workflow.Step{finStep, nextStep}}}

	require.NoError(t, wf.Run(t.Context()))
	require.Equal(t, workflow.StepStatusCompleted, finStep.State.Status,
		"a FinishWorkflow step that executed successfully must be completed, not skipped")
}

// Regression: reading snapshots concurrently with a running workflow must be
// race-free. GetSnapshot used to expose the live vars map and step-state
// pointers, so a concurrent read raced (and could fatally panic on the map).
func TestSnapshot_ConcurrentReadIsRaceFree(t *testing.T) {
	start := make(chan struct{})
	stopReader := make(chan struct{})
	readerStopped := make(chan struct{})

	step, _ := workflow.NewStep("producer", func(sc *workflow.StepContext) error {
		close(start)
		for i := range 3000 {
			workflow.SetVar(sc.Workflow, "i", i)
		}
		close(stopReader)
		<-readerStopped // ensure the reader fully stops before the step returns
		return nil
	})

	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "S", Steps: []*workflow.Step{step}}}

	go func() {
		defer close(readerStopped)
		<-start
		for {
			select {
			case <-stopReader:
				return
			default:
				_ = wf.GetJSONSnapshot()
			}
		}
	}()

	require.NoError(t, wf.Run(t.Context()))
}
