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

// ErrRetryAfter is a scheduler-neutral delayed-continuation signal. It means
// the current callback is not complete, compensation must not start, and the
// pipeline should be resumed after Duration. Cause is retained for diagnostics.
type ErrRetryAfter struct {
	Duration time.Duration
	Cause    error
}

func (e *ErrRetryAfter) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("pipeline: retry after %s", e.Duration)
	}
	return fmt.Sprintf("pipeline: retry after %s: %v", e.Duration, e.Cause)
}

func (e *ErrRetryAfter) Unwrap() error { return e.Cause }

// RetryAfter returns a scheduler-neutral delayed-continuation signal.
func RetryAfter(duration time.Duration, cause error) error {
	return &ErrRetryAfter{Duration: normalizeDuration(duration), Cause: cause}
}

// ErrSnapshotFailed reports that updated RunState could not be persisted. The
// executor stops immediately; callers should reload the last durable state
// before retrying because the returned state may contain uncommitted progress.
type ErrSnapshotFailed struct {
	Operation string
	Err       error
}

func (e *ErrSnapshotFailed) Error() string {
	return fmt.Sprintf("pipeline: snapshot failed during %s: %v", e.Operation, e.Err)
}

func (e *ErrSnapshotFailed) Unwrap() error { return e.Err }

// ErrRepeatLimit indicates a Repeat reached its configured iteration limit.
type ErrRepeatLimit struct {
	StepName      string
	MaxIterations int
}

// ErrRepeatTimeout indicates a Repeat reached its configured wall-clock limit.
type ErrRepeatTimeout struct {
	StepName    string
	MaxDuration time.Duration
}

func (e *ErrRepeatTimeout) Error() string {
	return fmt.Sprintf("pipeline: repeat step %q exceeded max duration %s", e.StepName, e.MaxDuration)
}

// ErrCompactRepeatCompensation reports a definition that could discard a
// compensation journal while compacting Repeat history.
type ErrCompactRepeatCompensation struct {
	StepName        string
	CompensatorPath []string
}

func (e *ErrCompactRepeatCompensation) Error() string {
	return fmt.Sprintf(
		"pipeline: compact repeat step %q contains compensating action at path %s; place an aggregating compensator before the repeat or use full history",
		e.StepName,
		strings.Join(e.CompensatorPath, "/"),
	)
}

// ErrStateMigrationFailed reports an unsuccessful version migration.
type ErrStateMigrationFailed struct {
	PipelineName string
	FromVersion  int
	ToVersion    int
	Err          error
}

func (e *ErrStateMigrationFailed) Error() string {
	return fmt.Sprintf(
		"pipeline %q: state migration from version %d to %d failed: %v",
		e.PipelineName,
		e.FromVersion,
		e.ToVersion,
		e.Err,
	)
}

func (e *ErrStateMigrationFailed) Unwrap() error { return e.Err }

func (e *ErrRepeatLimit) Error() string {
	return fmt.Sprintf("pipeline: repeat step %q reached maximum of %d iterations", e.StepName, e.MaxIterations)
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
