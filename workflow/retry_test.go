package workflow_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/workflow"
)

func runSingleStep(t *testing.T, step *workflow.Step) error {
	t.Helper()
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "S", Steps: []*workflow.Step{step}}}
	return wf.Run(t.Context())
}

// Regression: RunWithBackoff must honor the step Timeout as its initial delay
// (it used to hardcode 1s and ignore Timeout).
func TestRetry_BackoffHonorsTimeout(t *testing.T) {
	var attempts int
	step, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error {
		attempts++
		if attempts < 2 {
			return errors.New("transient")
		}
		return nil
	}, workflow.WithStepRetryPolicy(workflow.RunWithBackoff), workflow.WithStepMaxRetries(3), workflow.WithStepTimeout(40*time.Millisecond))

	start := time.Now()
	require.NoError(t, runSingleStep(t, step))
	elapsed := time.Since(start)

	require.Equal(t, 2, attempts)
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond, "first backoff delay must equal the step timeout")
	assert.Less(t, elapsed, time.Second, "and must not fall back to the old hardcoded 1s")
}

// Calling RunWithBackoff directly with a zero Timeout must fall back to a sane
// default delay rather than busy-looping.
func TestRetry_BackoffZeroTimeoutFallback(t *testing.T) {
	step, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error { return nil },
		workflow.WithStepMaxRetries(1)) // Timeout left at 0
	sc := &workflow.StepContext{Context: t.Context(), Workflow: workflow.New(), Step: step}
	require.NoError(t, workflow.RunWithBackoff(sc))
}

func TestRetry_Policies(t *testing.T) {
	policies := map[string]workflow.RetryPolicyFn{
		"linear":  workflow.RunWithLinear,
		"backoff": workflow.RunWithBackoff,
	}

	for name, policy := range policies {
		t.Run(name+" succeeds after failures", func(t *testing.T) {
			var attempts int
			step, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error {
				attempts++
				if attempts < 3 {
					return errors.New("transient")
				}
				return nil
			}, workflow.WithStepRetryPolicy(policy), workflow.WithStepMaxRetries(5), workflow.WithStepTimeout(time.Millisecond))

			require.NoError(t, runSingleStep(t, step))
			require.Equal(t, 3, attempts)
			require.Equal(t, workflow.StepStatusCompleted, step.State.Status)
		})

		t.Run(name+" exhausts retries", func(t *testing.T) {
			var attempts int
			step, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error {
				attempts++
				return errors.New("always fails")
			}, workflow.WithStepRetryPolicy(policy), workflow.WithStepMaxRetries(3), workflow.WithStepTimeout(time.Millisecond))

			err := runSingleStep(t, step)
			require.Error(t, err)
			require.Equal(t, 3, attempts, "must try exactly MaxRetries times")
			require.Equal(t, workflow.StepStatusFailed, step.State.Status)
		})

		t.Run(name+" does not retry control-flow errors", func(t *testing.T) {
			var attempts int
			step, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error {
				attempts++
				return workflow.ErrSkipStep
			}, workflow.WithStepRetryPolicy(policy), workflow.WithStepMaxRetries(5), workflow.WithStepTimeout(time.Millisecond))

			require.NoError(t, runSingleStep(t, step))
			require.Equal(t, 1, attempts, "control-flow errors must not be retried")
		})

		t.Run(name+" infinite retries until success", func(t *testing.T) {
			var attempts int
			step, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error {
				attempts++
				if attempts < 4 {
					return workflow.SilentError(fmt.Errorf("not ready"))
				}
				return nil
			}, workflow.WithStepRetryPolicy(policy), workflow.WithStepMaxRetries(-1), workflow.WithStepTimeout(time.Millisecond))

			require.NoError(t, runSingleStep(t, step))
			require.Equal(t, 4, attempts)
		})
	}
}
