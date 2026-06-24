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

// Snapshot errors (before/after a step, and on failure) must be logged, never fatal.
func TestSnapshotFn_ErrorsLoggedNotFatal(t *testing.T) {
	failing := func(_ context.Context, _ *workflow.Workflow, _ workflow.Snapshot) error {
		return errors.New("snap fail")
	}

	t.Run("success path", func(t *testing.T) {
		wf := workflow.New(workflow.WithSnapshotFn(failing), workflow.WithLogger(loggerutil.NewTestLogger()))
		wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{okStep("x")}}}
		require.NoError(t, wf.Run(t.Context()))
	})

	t.Run("failure path", func(t *testing.T) {
		step, _ := workflow.NewStep("fail", func(_ *workflow.StepContext) error { return errors.New("boom") })
		wf := workflow.New(workflow.WithSnapshotFn(failing), workflow.WithLogger(loggerutil.NewTestLogger()))
		wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}
		require.Error(t, wf.Run(t.Context()))
	})
}

// Cancelling the context during a retry sleep must abort the wait promptly.
func TestRetry_ContextCancelDuringSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	step, _ := workflow.NewStep("flaky", func(_ *workflow.StepContext) error {
		return errors.New("always fails")
	}, workflow.WithStepMaxRetries(10), workflow.WithStepTimeout(2*time.Second))

	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}

	go func() {
		time.Sleep(30 * time.Millisecond) // cancel while the step is sleeping between retries
		cancel()
	}()

	start := time.Now()
	err := wf.Run(ctx)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second, "cancellation must abort the retry sleep")
}

// Debug logging with navigation populates the common log fields (next stage/step).
func TestDebugLoggingWithNavigation(t *testing.T) {
	_, target := workflow.NewStep("target", func(_ *workflow.StepContext) error { return nil })
	jump, _ := workflow.NewStep("jump", func(sc *workflow.StepContext) error {
		sc.Workflow.State.GoToStep(target)
		return nil
	})
	tgt, _ := workflow.NewStep("target", func(_ *workflow.StepContext) error { return nil })

	wf := workflow.New(workflow.WithDebug(true), workflow.WithLogger(loggerutil.NewTestLogger()))
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{jump, okStep("middle"), tgt}}}

	require.NoError(t, wf.Run(t.Context()))
}
