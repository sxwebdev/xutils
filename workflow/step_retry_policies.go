package workflow

import (
	"context"
	"errors"
	"time"
)

// RetryPolicyFn defines the function signature for a retry policy.
type RetryPolicyFn func(sc *StepContext) error

// isControlFlowError returns true if the error is a sentinel control flow error
// that should not be retried.
func isControlFlowError(err error) bool {
	return errors.Is(err, ErrSkipStep) ||
		errors.Is(err, ErrSkipStage) ||
		errors.Is(err, ErrBreakStages) ||
		errors.Is(err, ErrExitWorkflow)
}

// sleepWithContext sleeps for the given duration but returns early if context is cancelled.
func sleepWithContext(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RunWithLinear runs a step function with a simple linear retry mechanism.
func RunWithLinear(sc *StepContext) error {
	var err error

	for retries := 0; retries < sc.Step.MaxRetries || sc.Step.MaxRetries < 0; retries++ {
		err = sc.Step.Func(sc)
		if err == nil {
			return nil
		}

		// do not retry control flow errors
		if isControlFlowError(err) {
			return err
		}

		if !errors.Is(err, ErrSilent) {
			sc.Workflow.Errorf("step [%s] failed with error: %s", sc.Step.Name, err)
		}

		if retries+1 < sc.Step.MaxRetries || sc.Step.MaxRetries < 0 {
			sc.Workflow.Errorf("retrying in %s", sc.Step.Timeout)
			if err := sleepWithContext(sc, sc.Step.Timeout); err != nil {
				return err
			}
		}
	}

	return err
}

// RunWithBackoff runs a step function with a simple backoff retry mechanism.
func RunWithBackoff(sc *StepContext) error {
	var err error

	backoff := 1 * time.Second
	for retries := 0; retries < sc.Step.MaxRetries || sc.Step.MaxRetries < 0; retries++ {
		err = sc.Step.Func(sc)
		if err == nil {
			return nil
		}

		// do not retry control flow errors
		if isControlFlowError(err) {
			return err
		}

		if !errors.Is(err, ErrSilent) {
			sc.Workflow.Errorf("step [%s] failed with error: %s", sc.Step.Name, err)
		}

		if retries+1 < sc.Step.MaxRetries || sc.Step.MaxRetries < 0 {
			sc.Workflow.Errorf("retrying in %s", backoff)
			if err := sleepWithContext(sc, backoff); err != nil {
				return err
			}
			backoff *= 2
		}
	}

	return err
}
