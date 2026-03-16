package workflow

import (
	"context"
	"errors"
	"time"
)

type RetryPolicyFn func(context.Context, *Workflow, *Stage, *Step) error

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
func RunWithLinear(ctx context.Context, w *Workflow, stage *Stage, step *Step) error {
	var err error

	for retries := 0; retries < step.MaxRetries || step.MaxRetries < 0; retries++ {
		err = step.Func(ctx, w, stage, step)
		if err == nil {
			return nil
		}

		// do not retry control flow errors
		if isControlFlowError(err) {
			return err
		}

		if !errors.Is(err, ErrNoConsole) {
			w.Errorf("step [%s] failed with error: %s", step.Name, err)
		}

		if retries+1 < step.MaxRetries || step.MaxRetries < 0 {
			w.Errorf("retrying in %s", step.Timeout)
			if err := sleepWithContext(ctx, step.Timeout); err != nil {
				return err
			}
		}
	}

	return err
}

// RunWithBackoff runs a step function with a simple backoff retry mechanism.
func RunWithBackoff(ctx context.Context, w *Workflow, stage *Stage, step *Step) error {
	var err error

	backoff := 1 * time.Second
	for retries := 0; retries < step.MaxRetries || step.MaxRetries < 0; retries++ {
		err = step.Func(ctx, w, stage, step)
		if err == nil {
			return nil
		}

		// do not retry control flow errors
		if isControlFlowError(err) {
			return err
		}

		if !errors.Is(err, ErrNoConsole) {
			w.Errorf("step [%s] failed with error: %s", step.Name, err)
		}

		if retries+1 < step.MaxRetries || step.MaxRetries < 0 {
			w.Errorf("retrying in %s", backoff)
			if err := sleepWithContext(ctx, backoff); err != nil {
				return err
			}
			backoff *= 2
		}
	}

	return err
}
