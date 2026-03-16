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
	// Status is the current pipeline status.
	Status RunStatus `json:"status"`

	// CurrentPath is the position in the step tree.
	// For linear steps: ["step_name"]
	// For steps inside a branch: ["branch_name", "path_key", "step_name"]
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

	// CompensationIndex tracks which completed step we're compensating next.
	// -1 means compensation hasn't started iterating yet.
	CompensationIndex int `json:"compensation_index,omitempty"`
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
