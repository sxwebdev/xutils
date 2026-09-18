package pipeline

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

func compactRepeatIteration(state *RunState, repeatPath []string, iteration int) {
	iterationPath := append(slices.Clone(repeatPath), strconv.Itoa(iteration))

	completed := state.CompletedSteps[:0]
	for _, step := range state.CompletedSteps {
		if hasPathPrefix(step.Path, iterationPath) {
			continue
		}
		completed = append(completed, step)
	}
	state.CompletedSteps = completed

	for key, diagnostic := range state.StepDiagnostics {
		path := stepPathFromKey(key)
		if !hasPathPrefix(path, iterationPath) {
			continue
		}
		delete(state.StepDiagnostics, key)
		aggregatePath := append(slices.Clone(repeatPath), path[len(iterationPath):]...)
		aggregateKey := StepPathKey(aggregatePath)
		state.StepDiagnostics[aggregateKey] = mergeDiagnostics(state.StepDiagnostics[aggregateKey], diagnostic)
	}

	for key := range state.RepeatStates {
		path := stepPathFromKey(key)
		if hasPathPrefix(path, iterationPath) {
			delete(state.RepeatStates, key)
		}
	}
}

func hasPathPrefix(path, prefix []string) bool {
	return len(path) >= len(prefix) && slices.Equal(path[:len(prefix)], prefix)
}

func stepPathFromKey(key string) []string {
	if key == "" {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(key, "/"), "/")
	for i := range parts {
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(parts[i], "~1", "/"), "~0", "~")
	}
	return parts
}

func mergeDiagnostics(aggregate, current StepDiagnostics) StepDiagnostics {
	aggregate.Attempts += current.Attempts
	aggregate.FirstStartedAt = earlierTime(aggregate.FirstStartedAt, current.FirstStartedAt)

	if laterTime(current.LastStartedAt, aggregate.LastStartedAt) {
		aggregate.LastStartedAt = cloneTime(current.LastStartedAt)
		aggregate.NextRunAt = cloneTime(current.NextRunAt)
		if current.LastError != "" {
			aggregate.LastError = current.LastError
		}
	}
	if laterTime(current.CompletedAt, aggregate.CompletedAt) {
		aggregate.CompletedAt = cloneTime(current.CompletedAt)
	}
	return aggregate
}

func earlierTime(left, right *time.Time) *time.Time {
	switch {
	case left == nil:
		return cloneTime(right)
	case right == nil || left.Before(*right):
		return cloneTime(left)
	default:
		return cloneTime(right)
	}
}

func laterTime(left, right *time.Time) bool {
	return left != nil && (right == nil || left.After(*right) || left.Equal(*right))
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
