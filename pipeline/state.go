package pipeline

import (
	"encoding/json"
	"time"
)

// RunStatus represents the current status of a pipeline execution.
type RunStatus string

const (
	// RunStatusNew indicates the pipeline has not started yet.
	RunStatusNew RunStatus = ""
	// RunStatusRunning indicates the pipeline is actively executing.
	RunStatusRunning RunStatus = "running"
	// RunStatusPolling indicates the pipeline is waiting for a poll condition.
	RunStatusPolling RunStatus = "polling"
	// RunStatusCompleted indicates the pipeline finished successfully.
	RunStatusCompleted RunStatus = "completed"
	// RunStatusCompensating indicates the pipeline is rolling back completed steps.
	RunStatusCompensating RunStatus = "compensating"
	// RunStatusFailed indicates the pipeline failed (after compensation if applicable).
	RunStatusFailed RunStatus = "failed"
)

// RunState is the complete persistent state of a pipeline execution.
// It is serialized to JSON and stored in the database.
// On restart, the executor loads RunState and resumes from where it left off.
type RunState struct {
	// Version is the pipeline definition version that created this state.
	// 0 means legacy (created before versioning was added).
	Version int `json:"version,omitempty"`

	// Status is the current pipeline status.
	Status RunStatus `json:"status"`

	// CurrentPath is the position in the step tree.
	// For linear steps: ["step_name"]
	// For steps inside a branch: ["branch_name", "path_key", "step_name"]
	// For steps inside a repeat: ["repeat_name", "iteration", "step_name"]
	CurrentPath []string `json:"current_path"`

	// CompletedSteps tracks which steps finished successfully (for compensation walk-back).
	CompletedSteps []CompletedStep `json:"completed_steps"`

	// Data holds shared data between steps, serialized as JSON.
	Data map[string]json.RawMessage `json:"data,omitempty"`

	// Error holds the error message if status is failed or compensating.
	Error string `json:"error,omitempty"`

	// FailedStepPath is the full path of the step that caused the failure.
	FailedStepPath []string `json:"failed_step_path,omitempty"`

	// ErrorContext stores structured error data (JSON).
	// Can be used by compensation steps to make decisions.
	ErrorContext json.RawMessage `json:"error_context,omitempty"`

	// PollStartedAt records when the current poll step began (for MaxDuration).
	PollStartedAt *time.Time `json:"poll_started_at,omitempty"`

	// CompensationIndex is the next completed-step index to compensate (walked in
	// reverse). -1 means every completed step has already been compensated and
	// only finalization remains. The executor stamps it to len(CompletedSteps)-1
	// when compensation begins.
	CompensationIndex int `json:"compensation_index,omitempty"`

	// Revision is advanced after every successfully persisted snapshot. Storage
	// implementations may use it for optimistic concurrency control.
	Revision uint64 `json:"revision,omitempty"`

	// StepDiagnostics contains optional aggregate execution metadata keyed by a
	// stable JSON-pointer-like full step path (see StepPathKey).
	StepDiagnostics map[string]StepDiagnostics `json:"step_diagnostics,omitempty"`

	// RepeatStates records the current zero-based iteration for active and
	// completed Repeat steps, keyed by the Repeat step's full path.
	RepeatStates map[string]RepeatState `json:"repeat_states,omitempty"`
}

// StepDiagnostics is aggregate diagnostic metadata for a step. It is
// informational only and never controls pipeline execution.
type StepDiagnostics struct {
	Attempts       int        `json:"attempts,omitempty"`
	FirstStartedAt *time.Time `json:"first_started_at,omitempty"`
	LastStartedAt  *time.Time `json:"last_started_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	NextRunAt      *time.Time `json:"next_run_at,omitempty"`
}

// RepeatState is resumable state for a Repeat step.
type RepeatState struct {
	// Iteration is the current zero-based iteration.
	Iteration int `json:"iteration,omitempty"`
	// AwaitingCondition means nested steps completed and Until is next.
	AwaitingCondition bool `json:"awaiting_condition,omitempty"`
	// Completed indicates the Repeat itself completed.
	Completed bool `json:"completed,omitempty"`
}

// CompletedStep records a step that finished successfully.
type CompletedStep struct {
	// Path is the full path to the step in the pipeline tree.
	Path []string `json:"path"`
	// HasCompensator indicates whether this step has a Compensate function.
	HasCompensator bool `json:"has_compensator"`
}

// IsTerminal returns true if the pipeline is in a terminal state.
func (s RunState) IsTerminal() bool {
	return s.Status == RunStatusCompleted || s.Status == RunStatusFailed
}

// ForceTerminate marks the pipeline as failed without compensation.
// Use when a pipeline is stuck and cannot be resumed (e.g., version drain timeout,
// all old instances are gone). The caller must persist the state after calling this.
func (s *RunState) ForceTerminate(reason string) {
	s.Status = RunStatusFailed
	s.Error = reason
}
