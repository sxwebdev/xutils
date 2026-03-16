package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrNoCompensate wraps an error to indicate it should NOT trigger compensation.
// The error will be propagated to the caller as-is.
// Use this for transient/retryable errors where the caller (e.g. job queue)
// should retry the entire pipeline from the current position.
var ErrNoCompensate = errors.New("no compensate")

// NoCompensate wraps an error to prevent compensation on failure.
func NoCompensate(err error) error {
	return fmt.Errorf("%w: %w", ErrNoCompensate, err)
}

// ErrSnooze is returned when a poll step is waiting for a condition.
// The caller should save the state and re-invoke Run after Duration.
type ErrSnooze struct {
	Duration time.Duration
}

func (e ErrSnooze) Error() string {
	return fmt.Sprintf("pipeline: snooze for %s", e.Duration)
}

// ErrCompensationFailed indicates that compensation itself failed.
type ErrCompensationFailed struct {
	// Original is the error that triggered compensation.
	Original error
	// Compensation is the error that occurred during compensation.
	Compensation error
}

func (e *ErrCompensationFailed) Error() string {
	return fmt.Sprintf("pipeline: compensation failed: %v (original error: %v)", e.Compensation, e.Original)
}

func (e *ErrCompensationFailed) Unwrap() []error {
	return []error{e.Original, e.Compensation}
}

// ErrStepFailed indicates that a specific step failed.
type ErrStepFailed struct {
	// StepName is the name of the failed step.
	StepName string
	// Path is the full path to the step.
	Path []string
	// Err is the underlying error.
	Err error
}

func (e *ErrStepFailed) Error() string {
	return fmt.Sprintf("pipeline: step %q (path: %s) failed: %v", e.StepName, strings.Join(e.Path, "/"), e.Err)
}

func (e *ErrStepFailed) Unwrap() error {
	return e.Err
}

// ErrPollTimeout indicates that a poll step exceeded its MaxDuration.
type ErrPollTimeout struct {
	StepName    string
	MaxDuration time.Duration
}

func (e *ErrPollTimeout) Error() string {
	return fmt.Sprintf("pipeline: poll step %q exceeded max duration %s", e.StepName, e.MaxDuration)
}

// ErrVersionMismatch is returned when the pipeline definition version
// is incompatible with the state's version.
type ErrVersionMismatch struct {
	// PipelineName is the name of the pipeline.
	PipelineName string
	// StateVersion is the version stored in the RunState.
	StateVersion int
	// PipelineVersion is the current pipeline definition version.
	PipelineVersion int
	// MinResumeVersion is the minimum state version the pipeline accepts.
	MinResumeVersion int
}

func (e *ErrVersionMismatch) Error() string {
	return fmt.Sprintf(
		"pipeline %q: version mismatch: state version %d not in allowed range [%d, %d]",
		e.PipelineName, e.StateVersion, e.MinResumeVersion, e.PipelineVersion,
	)
}
