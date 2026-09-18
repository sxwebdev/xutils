package pipeline

import (
	"context"
	"slices"
	"time"
)

// EventType identifies an Executor lifecycle event.
type EventType string

const (
	EventPipelineStarted          EventType = "pipeline_started"
	EventPipelineCompleted        EventType = "pipeline_completed"
	EventStepStarted              EventType = "step_started"
	EventStepCompleted            EventType = "step_completed"
	EventStepFailed               EventType = "step_failed"
	EventDelayedContinuation      EventType = "delayed_continuation"
	EventRepeatIterationStarted   EventType = "repeat_iteration_started"
	EventRepeatIterationCompleted EventType = "repeat_iteration_completed"
	EventCompensationStarted      EventType = "compensation_started"
	EventCompensationSucceeded    EventType = "compensation_succeeded"
	EventCompensationFailed       EventType = "compensation_failed"
	EventSnapshotSucceeded        EventType = "snapshot_succeeded"
	EventSnapshotFailed           EventType = "snapshot_failed"
	EventStateMigrationSucceeded  EventType = "state_migration_succeeded"
	EventStateMigrationFailed     EventType = "state_migration_failed"
	EventVersionMismatch          EventType = "version_mismatch"
	EventContextCanceled          EventType = "context_canceled"
)

// Event is an immutable-by-convention description of one Executor lifecycle
// transition. StepPath, RepeatIteration, and pipeline-owned Cause values are
// copied before delivery, and no RunState pointer is exposed. RepeatIteration
// is nil outside a Repeat.
type Event struct {
	Type            EventType
	PipelineName    string
	PipelineVersion int
	RunStatus       RunStatus
	StepPath        []string
	Attempt         int
	RepeatIteration *int
	Duration        time.Duration
	Operation       string
	Cause           error
}

// Observer receives synchronous lifecycle events. Observe must return quickly;
// applications that perform slow I/O should enqueue events for asynchronous
// processing. Observer panics are recovered and logged without affecting the
// pipeline result.
type Observer interface {
	Observe(ctx context.Context, event Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(ctx context.Context, event Event)

// Observe implements Observer.
func (fn ObserverFunc) Observe(ctx context.Context, event Event) { fn(ctx, event) }

func (e *Executor) observe(ctx context.Context, p *Pipeline, state RunState, event Event) {
	if e.observer == nil {
		return
	}

	event.PipelineName = p.Name
	event.PipelineVersion = p.Version
	event.RunStatus = state.Status
	event.StepPath = slices.Clone(event.StepPath)
	if event.RepeatIteration == nil {
		event.RepeatIteration = repeatIterationForPath(p.Steps, event.StepPath)
	}
	if event.RepeatIteration != nil {
		iteration := *event.RepeatIteration
		event.RepeatIteration = &iteration
	}
	event.Cause = cloneObserverCause(event.Cause)

	defer func() {
		if recovered := recover(); recovered != nil {
			e.Errorf("observer panic during %s: %v", event.Type, recovered)
		}
	}()
	e.observer.Observe(ctx, event)
}

// cloneObserverCause detaches mutable pipeline error values from the copies
// used by Executor control flow. Opaque callback errors are passed through so
// errors.Is/errors.AsType keep their original semantics.
func cloneObserverCause(err error) error {
	switch cause := err.(type) {
	case nil:
		return nil
	case ErrSnooze:
		return cause
	case *ErrRetryAfter:
		cloned := *cause
		cloned.Cause = cloneObserverCause(cause.Cause)
		return &cloned
	case *ErrSnapshotFailed:
		cloned := *cause
		cloned.Err = cloneObserverCause(cause.Err)
		return &cloned
	case *ErrRepeatLimit:
		cloned := *cause
		return &cloned
	case *ErrRepeatTimeout:
		cloned := *cause
		return &cloned
	case *ErrCompactRepeatCompensation:
		cloned := *cause
		cloned.CompensatorPath = slices.Clone(cause.CompensatorPath)
		return &cloned
	case *ErrStateMigrationFailed:
		cloned := *cause
		cloned.Err = cloneObserverCause(cause.Err)
		return &cloned
	case *ErrCompensationFailed:
		cloned := *cause
		cloned.Original = cloneObserverCause(cause.Original)
		cloned.Compensation = cloneObserverCause(cause.Compensation)
		return &cloned
	case *ErrStepFailed:
		cloned := *cause
		cloned.Path = slices.Clone(cause.Path)
		cloned.Err = cloneObserverCause(cause.Err)
		return &cloned
	case *ErrPollTimeout:
		cloned := *cause
		return &cloned
	case *ErrVersionMismatch:
		cloned := *cause
		return &cloned
	default:
		return err
	}
}
