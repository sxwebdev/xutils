package workflow

import (
	"fmt"
)

// Snapshot represents a snapshot of the workflow.
type Snapshot struct {
	StepsStates   []*StepState   `json:"steps_states"`
	WorkflowState WorkflowState  `json:"workflow_state"`
	Vars          map[string]any `json:"vars,omitempty"`
}

// GetArg retrieves a typed argument from a step's state by step name and key.
func GetArg[T any](sh Snapshot, stepName, key string) (T, error) {
	var arg T

	if stepName == "" {
		return arg, fmt.Errorf("step is empty")
	}

	if key == "" {
		return arg, fmt.Errorf("key is empty")
	}

	for _, state := range sh.StepsStates {
		if state.CurrentStep != stepName {
			continue
		}

		state.mu.RLock()
		val, ok := state.Args[key]
		state.mu.RUnlock()
		if !ok {
			return arg, fmt.Errorf("value not found for key %s: %w", key, ErrNotFound)
		}

		arg, ok = val.(T)
		if !ok {
			return arg, fmt.Errorf("type assertion failed")
		}

		return arg, nil
	}

	return arg, fmt.Errorf("step not found")
}

// GetArgByRef retrieves a typed argument from a step's state using a typed step reference.
func GetArgByRef[T any](sh Snapshot, ref StepRef, key string) (T, error) {
	return GetArg[T](sh, ref.name, key)
}
