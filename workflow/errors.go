package workflow

import (
	"errors"
	"fmt"
)

var (
	// ErrSkipStep is used to skip the current step.
	ErrSkipStep = errors.New("skip step")
	// ErrSkipStage is used to skip the current stage.
	ErrSkipStage = errors.New("skip stage")
	// ErrBreakStages is used to break the root loop.
	ErrBreakStages = errors.New("break stages")
	// ErrExitWorkflow is used to exit the workflow.
	ErrExitWorkflow = errors.New("exit")
	// ErrSilent suppresses error logging in retry loops.
	ErrSilent = errors.New("silent")
	// ErrNotFound indicates a value was not found.
	ErrNotFound = errors.New("not found")
)

// SilentError wraps an error to suppress logging in retry loops.
func SilentError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%w: %w", ErrSilent, err)
}
