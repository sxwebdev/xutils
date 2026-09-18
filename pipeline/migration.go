package pipeline

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
)

// StateMigrator atomically transforms a non-terminal RunState from one pipeline
// version to the current version. The Executor stamps the returned state with
// toVersion and snapshots it before running any pipeline callback.
type StateMigrator func(
	ctx context.Context,
	pipelineName string,
	fromVersion int,
	toVersion int,
	state RunState,
) (RunState, error)

func cloneRunState(state RunState) RunState {
	cloned := state
	cloned.CurrentPath = slices.Clone(state.CurrentPath)
	cloned.FailedStepPath = slices.Clone(state.FailedStepPath)
	cloned.ErrorContext = slices.Clone(state.ErrorContext)
	cloned.PollStartedAt = cloneTime(state.PollStartedAt)
	if state.RepeatTimeout != nil {
		timeout := *state.RepeatTimeout
		cloned.RepeatTimeout = &timeout
	}

	cloned.CompletedSteps = make([]CompletedStep, len(state.CompletedSteps))
	for i, step := range state.CompletedSteps {
		cloned.CompletedSteps[i] = step
		cloned.CompletedSteps[i].Path = slices.Clone(step.Path)
	}

	if state.Data != nil {
		cloned.Data = make(map[string]json.RawMessage, len(state.Data))
		for key, value := range state.Data {
			cloned.Data[key] = slices.Clone(value)
		}
	}
	if state.StepDiagnostics != nil {
		cloned.StepDiagnostics = make(map[string]StepDiagnostics, len(state.StepDiagnostics))
		for key, diagnostic := range state.StepDiagnostics {
			diagnostic.FirstStartedAt = cloneTime(diagnostic.FirstStartedAt)
			diagnostic.LastStartedAt = cloneTime(diagnostic.LastStartedAt)
			diagnostic.CompletedAt = cloneTime(diagnostic.CompletedAt)
			diagnostic.NextRunAt = cloneTime(diagnostic.NextRunAt)
			cloned.StepDiagnostics[key] = diagnostic
		}
	}
	if state.RepeatStates != nil {
		cloned.RepeatStates = maps.Clone(state.RepeatStates)
		for key, repeatState := range cloned.RepeatStates {
			repeatState.StartedAt = cloneTime(repeatState.StartedAt)
			cloned.RepeatStates[key] = repeatState
		}
	}
	return cloned
}
