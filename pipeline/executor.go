package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sxwebdev/xutils/loggerutil"
)

// Executor runs a Pipeline with persistence and compensation support.
// It is stateless — all state is passed in and returned via RunState.
type Executor struct {
	logger        loggerutil.Logger
	debug         bool
	snapshotFn    SnapshotFunc
	casSnapshotFn CASSnapshotFunc
	clock         Clock
	stateMigrator StateMigrator
	observer      Observer
}

// NewExecutor creates a new Executor with the given options.
func NewExecutor(opts ...ExecutorOption) *Executor {
	e := &Executor{clock: systemClock{}}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Run executes (or resumes) the pipeline.
//
// Parameters:
//   - ctx: context for cancellation
//   - p: the pipeline definition (immutable)
//   - state: current state (empty RunState{} for new, or loaded from DB for resume)
//
// Returns:
//   - Updated RunState (always valid, should be persisted)
//   - nil if pipeline completed or failed (check state.Status)
//   - ErrRetryAfter if any callback requested a scheduler-neutral continuation
//   - ErrSnooze if a poll step is waiting (legacy-compatible scheduling signal)
//   - ErrSnapshotFailed if persistence failed; no subsequent callback was run
//   - ErrRepeatTimeout if a Repeat reached WithMaxRepeatDuration; ordinary
//     compensation has completed (or ErrCompensationFailed takes precedence)
//   - ctx.Err() if the context is cancelled; the run is left resumable (not
//     compensated), so persisting and re-invoking later continues from the
//     interrupted step
//   - Other errors indicate engine failures
func (e *Executor) Run(ctx context.Context, p *Pipeline, state RunState) (RunState, error) {
	// Validate pipeline definition.
	if err := p.validate(); err != nil {
		return state, err
	}

	// Terminal states — nothing to do.
	if state.IsTerminal() {
		return state, nil
	}

	e.observe(ctx, p, state, Event{Type: EventPipelineStarted, Operation: "run"})
	if ctx.Err() != nil {
		e.observe(ctx, p, state, Event{Type: EventContextCanceled, Operation: "run", Cause: ctx.Err()})
		return state, ctx.Err()
	}

	// Stamp version on new executions.
	if state.Status == RunStatusNew {
		state.Version = p.Version
	} else {
		if state.Version > p.Version {
			versionErr := versionMismatchError(p, state)
			e.observe(ctx, p, state, Event{Type: EventVersionMismatch, Operation: "downgrade", Cause: versionErr})
			return state, versionErr
		}
		if state.Version != p.Version && e.stateMigrator != nil {
			fromVersion := state.Version
			startedAt := e.clock.Now()
			migrated, err := e.stateMigrator(ctx, p.Name, fromVersion, p.Version, cloneRunState(state))
			if err != nil {
				migrationErr := &ErrStateMigrationFailed{
					PipelineName: p.Name,
					FromVersion:  fromVersion,
					ToVersion:    p.Version,
					Err:          err,
				}
				e.observe(ctx, p, state, Event{
					Type:      EventStateMigrationFailed,
					Duration:  e.clock.Now().Sub(startedAt),
					Operation: "state migration",
					Cause:     migrationErr,
				})
				return state, migrationErr
			}
			migrated.Version = p.Version
			state = migrated
			if snapshotErr := e.snapshot(ctx, p, &state, "state migration"); snapshotErr != nil {
				return state, snapshotErr
			}
			e.observe(ctx, p, state, Event{
				Type:      EventStateMigrationSucceeded,
				Duration:  e.clock.Now().Sub(startedAt),
				Operation: "state migration",
			})
		}

		// Version check for resume (non-new, non-terminal), after migration.
		if err := checkVersion(p, state); err != nil {
			e.observe(ctx, p, state, Event{Type: EventVersionMismatch, Operation: "version check", Cause: err})
			return state, err
		}
		if state.IsTerminal() {
			e.observe(ctx, p, state, Event{Type: EventPipelineCompleted, Operation: "state migration"})
			return state, nil
		}
	}

	// Initialize data store.
	ds := newDataStore()
	if state.Data != nil {
		ds.restoreData(state.Data)
	}
	if ctx.Err() != nil {
		e.observe(ctx, p, state, Event{Type: EventContextCanceled, Operation: "run", Cause: ctx.Err()})
		return state, ctx.Err()
	}

	// If compensating, continue compensation.
	if state.Status == RunStatusCompensating {
		state.Data, _ = ds.marshalData() // restored RawMessage values cannot fail
		if snapshotErr := e.snapshot(ctx, p, &state, "resume compensation"); snapshotErr != nil {
			return state, snapshotErr
		}
		compensatedState, err := e.runCompensation(ctx, p, state, ds)
		if err != nil {
			return compensatedState, err
		}
		if compensatedState.RepeatTimeout != nil {
			return compensatedState, &ErrRepeatTimeout{
				StepName:    compensatedState.RepeatTimeout.StepName,
				MaxDuration: compensatedState.RepeatTimeout.MaxDuration,
			}
		}
		return compensatedState, nil
	}

	// Persist status transitions before invoking any callback. This ensures a
	// failed initial snapshot cannot be followed by an external side effect.
	previousStatus := state.Status
	state.Status = RunStatusRunning
	state.Data, _ = ds.marshalData() // restored RawMessage values cannot fail
	operation := "resume execution"
	if previousStatus != RunStatusRunning {
		operation = "transition to running"
	}
	if snapshotErr := e.snapshot(ctx, p, &state, operation); snapshotErr != nil {
		return state, snapshotErr
	}

	// Find the starting index at the top level.
	startIdx := e.findTopLevelIndex(p.Steps, state.CurrentPath)

	e.Debugf("pipeline %q: starting from index %d in path %v", p.Name, startIdx, state.CurrentPath)

	// Execute steps from the current position.
	var err error
	state, err = e.executeSteps(ctx, p, state, ds, p.Steps, startIdx, nil)
	if err != nil {
		return state, err
	}

	// All steps completed successfully.
	if state.Status == RunStatusRunning {
		state.Status = RunStatusCompleted
		state.Data, _ = ds.marshalData() // the final step already validated Data

		if snapshotErr := e.snapshot(ctx, p, &state, "pipeline completion"); snapshotErr != nil {
			return state, snapshotErr
		}

		e.Infof("pipeline %q: completed successfully", p.Name)
		e.observe(ctx, p, state, Event{Type: EventPipelineCompleted, Operation: "run"})
	}

	return state, nil
}

// executeSteps runs a slice of steps starting from startIdx.
// parentPath is the path prefix for steps in a branch.
func (e *Executor) executeSteps(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	ds *dataStore,
	steps []Step,
	startIdx int,
	parentPath []string,
) (RunState, error) {
	// Save the original resume path so branches can detect they're being resumed.
	resumePath := slices.Clone(state.CurrentPath)

	for i := startIdx; i < len(steps); i++ {
		step := &steps[i]
		stepPath := append(slices.Clone(parentPath), step.Name)

		// Resume: the per-step snapshot records CurrentPath as the step that just
		// finished. If we are resuming exactly at that step and it is already in
		// CompletedSteps, skip it — re-executing would run its side effects again
		// and append a duplicate completion (later causing double compensation).
		// Branches are not skipped here: their resume path is deeper than stepPath,
		// so slices.Equal is false and executeBranch handles entering them.
		if i == startIdx && slices.Equal(stepPath, resumePath) && isStepCompleted(state, stepPath) {
			continue
		}

		// Check context cancellation.
		if ctx.Err() != nil {
			state.Data, _ = ds.marshalData() // prior step completion already validated Data
			state.CurrentPath = stepPath
			e.observe(ctx, p, state, Event{Type: EventContextCanceled, StepPath: stepPath, Operation: "step", Cause: ctx.Err()})
			return state, ctx.Err()
		}

		// For branches: preserve the full resume path on the first iteration
		// so executeBranch can detect which inner step to resume at.
		// After the first step, clear it — subsequent steps are fresh.
		if i == startIdx && isPathPrefix(stepPath, resumePath) {
			state.CurrentPath = resumePath
		} else {
			state.CurrentPath = stepPath
		}

		var err error
		state, err = e.executeStep(ctx, p, state, ds, step, stepPath)
		if err != nil {
			// Persistence failures are engine failures, not step failures. Never
			// compensate or run another callback after one occurs.
			if _, ok := errors.AsType[*ErrSnapshotFailed](err); ok {
				return state, err
			}

			// Delayed continuation has already been snapshotted by the callback
			// handler that owns the exact step path. Propagate it unchanged.
			if isDeferred(err) {
				return state, err
			}

			// Context cancellation (e.g. graceful shutdown) is not a step failure:
			// return it cleanly without compensating, leaving the run resumable —
			// mirroring the top-of-loop check. Otherwise a cancel mid-retry (or an
			// action that returns ctx.Err()) would roll back all progress and end
			// the pipeline terminally.
			if ctx.Err() != nil {
				data, marshalErr := ds.marshalData()
				if marshalErr != nil {
					return state, marshalErr
				}
				state.Data = data
				state.CurrentPath = stepPath
				e.observe(ctx, p, state, Event{Type: EventContextCanceled, StepPath: stepPath, Operation: "step", Cause: ctx.Err()})
				return state, ctx.Err()
			}

			// If this error originated inside a nested branch sub-path, that scope
			// already handled and recorded it (set FailedStepPath, snapshotted, and
			// either entered compensation or chose to skip it). Re-processing it here
			// would re-attribute the failure to this branch step and redo that work:
			//   - ErrCompensationFailed (nested compensation failed): the run is left
			//     non-Running; re-entering the compensation block below would reset
			//     CompensationIndex to the top and re-run already-completed
			//     compensators (double compensation).
			//   - a nested ErrNoCompensate step: the run stays Running, but the error
			//     carries the real (deeper) step path; the ErrNoCompensate block below
			//     would overwrite FailedStepPath with this branch step and snapshot
			//     again, once per parent level.
			// Propagate such errors unchanged. A genuine fresh failure at THIS level
			// (action/poll/branch-decide/OnEnter) carries stepPath as its own path and
			// leaves the run Running, so it is not caught here and still triggers
			// compensation below.
			if state.Status != RunStatusRunning {
				return state, err
			}
			if nestedFail, ok := errors.AsType[*ErrStepFailed](err); ok && len(nestedFail.Path) > len(stepPath) {
				return state, err
			}

			// Check if error should skip compensation (retryable error).
			if errors.Is(err, ErrNoCompensate) {
				e.Warnf("pipeline %q: step %q failed (no compensate): %v", p.Name, step.Name, err)
				data, marshalErr := ds.marshalData()
				if marshalErr != nil {
					return state, marshalErr
				}
				state.Data = data
				state.FailedStepPath = stepPath

				if snapshotErr := e.snapshot(ctx, p, &state, "step error without compensation"); snapshotErr != nil {
					return state, snapshotErr
				}

				return state, err
			}

			// Step failed — start compensation.
			e.Errorf("pipeline %q: step %q failed: %v", p.Name, step.Name, err)
			state.Error = err.Error()
			state.FailedStepPath = stepPath
			state.Status = RunStatusCompensating
			state.CompensationIndex = len(state.CompletedSteps) - 1
			if timeoutErr, ok := errors.AsType[*ErrRepeatTimeout](err); ok {
				state.RepeatTimeout = &RepeatTimeoutState{
					StepName:    timeoutErr.StepName,
					MaxDuration: timeoutErr.MaxDuration,
				}
			}

			data, marshalErr := ds.marshalData()
			if marshalErr != nil {
				return state, marshalErr
			}
			state.Data = data

			if snapshotErr := e.snapshot(ctx, p, &state, "step failure before compensation"); snapshotErr != nil {
				return state, snapshotErr
			}

			compensatedState, compensationErr := e.runCompensation(ctx, p, state, ds)
			if compensationErr != nil {
				return compensatedState, compensationErr
			}
			if _, ok := errors.AsType[*ErrRepeatTimeout](err); ok {
				return compensatedState, err
			}
			return compensatedState, nil
		}

		// A step nested in a branch may have failed and fully compensated,
		// returning nil with a terminal/non-running status. In that case the run
		// is already over, so stop instead of executing the remaining steps in
		// this (parent) scope. The top-level Run guards this via a status check
		// too, but nested scopes only see the nil error bubbling up.
		if state.Status != RunStatusRunning {
			return state, nil
		}
	}

	// All steps in this scope completed.
	return state, nil
}

// executeStep dispatches to the appropriate handler based on step type.
func (e *Executor) executeStep(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	ds *dataStore,
	step *Step,
	stepPath []string,
) (RunState, error) {
	attemptRecorded := false

	if step.Repeat != nil && step.Repeat.MaxDuration > 0 {
		var err error
		state, err = e.ensureRepeatStarted(ctx, p, state, stepPath)
		if err != nil {
			return state, err
		}
		repeatState := state.RepeatStates[StepPathKey(stepPath)]
		iteration := repeatState.Iteration
		if len(state.CurrentPath) > len(stepPath) {
			if parsed, parseErr := strconv.Atoi(state.CurrentPath[len(stepPath)]); parseErr == nil {
				iteration = parsed
			}
		}
		if e.repeatTimedOut(step, repeatState) {
			e.recordAttempt(ctx, p, &state, stepPath, &iteration)
			return e.repeatTimeout(ctx, p, state, step, stepPath, iteration)
		}
	}

	// Run OnEnter hook.
	if step.OnEnter != nil {
		if e.observer != nil {
			repeatIteration := repeatIterationForPath(p.Steps, state.CurrentPath)
			if repeatIteration == nil && step.Repeat != nil {
				iteration := state.RepeatStates[StepPathKey(stepPath)].Iteration
				repeatIteration = &iteration
			}
			e.observe(ctx, p, state, Event{
				Type:            EventStepStarted,
				StepPath:        stepPath,
				Attempt:         diagnosticAttempts(state, stepPath) + 1,
				RepeatIteration: repeatIteration,
				Operation:       "on_enter",
			})
		}
		attemptRecorded = true
		if err := step.OnEnter(ctx, ds); err != nil {
			e.recordAttemptData(&state, stepPath)
			e.recordError(&state, stepPath, err)
			stepErr := &ErrStepFailed{StepName: step.Name, Path: stepPath, Err: fmt.Errorf("on_enter: %w", err)}
			if isDeferred(stepErr) {
				return e.persistDeferred(ctx, p, state, ds, stepPath, stepErr, false, "delayed continuation", nil)
			}
			e.observe(ctx, p, state, Event{Type: EventStepFailed, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Operation: "on_enter", Cause: stepErr})
			return state, stepErr
		}
		if step.Repeat != nil && step.Repeat.MaxDuration > 0 {
			repeatState := state.RepeatStates[StepPathKey(stepPath)]
			if e.repeatTimedOut(step, repeatState) {
				iteration := repeatState.Iteration
				e.recordAttemptData(&state, stepPath)
				return e.repeatTimeout(ctx, p, state, step, stepPath, iteration)
			}
		}
	}

	switch {
	case step.Action != nil:
		return e.executeAction(ctx, p, state, ds, step, stepPath, attemptRecorded)
	case step.Poll != nil:
		return e.executePoll(ctx, p, state, ds, step, stepPath, attemptRecorded)
	case step.Branch != nil:
		return e.executeBranch(ctx, p, state, ds, step, stepPath, attemptRecorded)
	case step.Repeat != nil:
		return e.executeRepeat(ctx, p, state, ds, step, stepPath, attemptRecorded)
	default:
		return state, fmt.Errorf("pipeline: step %q has no action, poll, branch, or repeat", step.Name)
	}
}

// executeAction runs an action step with optional retry.
func (e *Executor) executeAction(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	ds *dataStore,
	step *Step,
	stepPath []string,
	attemptRecorded bool,
) (RunState, error) {
	e.Debugf("action step: %s", step.Name)

	firstAttempt := true
	err := e.runWithRetry(ctx, step, func() error {
		if attemptRecorded && firstAttempt {
			e.recordAttemptData(&state, stepPath)
		} else {
			e.recordAttempt(ctx, p, &state, stepPath, nil)
		}
		firstAttempt = false
		err := step.Action.Do(ctx, ds)
		if err != nil {
			e.recordError(&state, stepPath, err)
		}
		return err
	})
	if err != nil {
		stepErr := &ErrStepFailed{StepName: step.Name, Path: stepPath, Err: err}
		if isDeferred(stepErr) {
			return e.persistDeferred(ctx, p, state, ds, stepPath, stepErr, false, "delayed continuation", nil)
		}
		e.observe(ctx, p, state, Event{Type: EventStepFailed, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Duration: e.activeStepDuration(state, stepPath), Operation: "action", Cause: stepErr})
		return state, stepErr
	}

	// Record completion.
	state.CompletedSteps = append(state.CompletedSteps, CompletedStep{
		Path:           slices.Clone(stepPath),
		HasCompensator: step.Action.Compensate != nil,
	})

	data, marshalErr := ds.marshalData()
	if marshalErr != nil {
		return state, marshalErr
	}
	state.Data = data
	e.recordCompleted(&state, stepPath)
	e.Infof("step %q completed", step.Name)

	if err := e.snapshot(ctx, p, &state, "action completion"); err != nil {
		return state, err
	}
	e.observe(ctx, p, state, Event{Type: EventStepCompleted, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Duration: completedStepDuration(state, stepPath), Operation: "action"})

	return state, nil
}

// executePoll runs a poll step, returning ErrSnooze if not done.
func (e *Executor) executePoll(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	ds *dataStore,
	step *Step,
	stepPath []string,
	attemptRecorded bool,
) (RunState, error) {
	e.Debugf("poll step: %s", step.Name)

	// Track poll start time for MaxDuration.
	now := e.clock.Now().UTC()
	if state.PollStartedAt == nil {
		state.PollStartedAt = &now
	}

	// Check MaxDuration.
	if step.Poll.MaxDuration > 0 {
		elapsed := now.Sub(*state.PollStartedAt)
		if elapsed >= step.Poll.MaxDuration {
			state.PollStartedAt = nil
			if attemptRecorded {
				e.recordAttemptData(&state, stepPath)
			} else {
				e.recordAttempt(ctx, p, &state, stepPath, nil)
			}
			timeoutErr := &ErrPollTimeout{StepName: step.Name, MaxDuration: step.Poll.MaxDuration}
			e.recordError(&state, stepPath, timeoutErr)
			stepErr := &ErrStepFailed{
				StepName: step.Name,
				Path:     stepPath,
				Err:      timeoutErr,
			}
			e.observe(ctx, p, state, Event{Type: EventStepFailed, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Operation: "poll timeout", Cause: stepErr})
			return state, stepErr
		}
	}

	if attemptRecorded {
		e.recordAttemptData(&state, stepPath)
	} else {
		e.recordAttempt(ctx, p, &state, stepPath, nil)
	}
	done, retryAfter, err := step.Poll.Check(ctx, ds)
	if err != nil {
		e.recordError(&state, stepPath, err)
		stepErr := &ErrStepFailed{StepName: step.Name, Path: stepPath, Err: err}
		if isDeferred(stepErr) {
			return e.persistDeferred(ctx, p, state, ds, stepPath, stepErr, true, "delayed continuation", nil)
		}
		if ctx.Err() == nil {
			state.PollStartedAt = nil
		}
		e.observe(ctx, p, state, Event{Type: EventStepFailed, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Duration: e.activeStepDuration(state, stepPath), Operation: "poll", Cause: stepErr})
		return state, stepErr
	}

	if !done {
		// Save state and return snooze.
		state.Status = RunStatusPolling

		return e.persistDeferred(ctx, p, state, ds, stepPath, ErrSnooze{Duration: normalizeDuration(retryAfter)}, true, "delayed continuation", nil)
	}

	// Poll completed.
	state.PollStartedAt = nil
	state.Status = RunStatusRunning

	state.CompletedSteps = append(state.CompletedSteps, CompletedStep{
		Path:           slices.Clone(stepPath),
		HasCompensator: false,
	})

	data, marshalErr := ds.marshalData()
	if marshalErr != nil {
		return state, marshalErr
	}
	state.Data = data
	e.recordCompleted(&state, stepPath)
	e.Infof("poll step %q completed", step.Name)

	if err := e.snapshot(ctx, p, &state, "poll completion"); err != nil {
		return state, err
	}
	e.observe(ctx, p, state, Event{Type: EventStepCompleted, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Duration: completedStepDuration(state, stepPath), Operation: "poll"})

	return state, nil
}

// executeBranch evaluates a condition and enters the chosen path.
func (e *Executor) executeBranch(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	ds *dataStore,
	step *Step,
	stepPath []string,
	attemptRecorded bool,
) (RunState, error) {
	// Check if we're resuming inside a branch (already decided).
	var chosenPath string
	var childStartIdx int

	if len(state.CurrentPath) > len(stepPath) {
		// We're resuming inside this branch.
		// CurrentPath is like ["branch_name", "path_key", "child_step_name"]
		// stepPath is ["branch_name"]
		// So the path key is at index len(stepPath).
		chosenPath = state.CurrentPath[len(stepPath)]
		childStepName := ""
		if len(state.CurrentPath) > len(stepPath)+1 {
			childStepName = state.CurrentPath[len(stepPath)+1]
		}

		pathSteps, ok := step.Branch.Paths[chosenPath]
		if !ok {
			return state, fmt.Errorf("pipeline: branch %q: saved path %q not found", step.Name, chosenPath)
		}

		// Find the child step to resume from.
		childStartIdx = 0
		if childStepName != "" {
			for j, cs := range pathSteps {
				if cs.Name == childStepName {
					childStartIdx = j
					break
				}
			}
		}

		childPath := append(slices.Clone(stepPath), chosenPath)
		return e.executeSteps(ctx, p, state, ds, pathSteps, childStartIdx, childPath)
	}

	// First time entering this branch — decide.
	e.Debugf("branch step: %s (deciding)", step.Name)

	var err error
	if attemptRecorded {
		e.recordAttemptData(&state, stepPath)
	} else {
		e.recordAttempt(ctx, p, &state, stepPath, nil)
	}
	chosenPath, err = step.Branch.Decide(ctx, ds)
	if err != nil {
		e.recordError(&state, stepPath, err)
		stepErr := &ErrStepFailed{StepName: step.Name, Path: stepPath, Err: fmt.Errorf("decide: %w", err)}
		if isDeferred(stepErr) {
			return e.persistDeferred(ctx, p, state, ds, stepPath, stepErr, false, "delayed continuation", nil)
		}
		e.observe(ctx, p, state, Event{Type: EventStepFailed, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Duration: e.activeStepDuration(state, stepPath), Operation: "branch decision", Cause: stepErr})
		return state, stepErr
	}

	pathSteps, ok := step.Branch.Paths[chosenPath]
	if !ok {
		stepErr := &ErrStepFailed{
			StepName: step.Name,
			Path:     stepPath,
			Err:      fmt.Errorf("branch returned unknown path %q (available: %v)", chosenPath, branchPathNames(step.Branch)),
		}
		e.recordError(&state, stepPath, stepErr.Err)
		e.observe(ctx, p, state, Event{Type: EventStepFailed, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Operation: "branch decision", Cause: stepErr})
		return state, stepErr
	}

	e.Infof("branch %q chose path %q", step.Name, chosenPath)

	// Record branch decision as a completed step (no compensator).
	state.CompletedSteps = append(state.CompletedSteps, CompletedStep{
		Path:           slices.Clone(stepPath),
		HasCompensator: false,
	})
	e.recordCompleted(&state, stepPath)

	if len(pathSteps) == 0 {
		state.CurrentPath = slices.Clone(stepPath)
		data, marshalErr := ds.marshalData()
		if marshalErr != nil {
			return state, marshalErr
		}
		state.Data = data
		if err := e.snapshot(ctx, p, &state, "empty branch completion"); err != nil {
			return state, err
		}
		e.observe(ctx, p, state, Event{Type: EventStepCompleted, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Duration: completedStepDuration(state, stepPath), Operation: "branch"})
		return state, nil
	}

	childPath := append(slices.Clone(stepPath), chosenPath)
	state.CurrentPath = slices.Clone(childPath)
	data, marshalErr := ds.marshalData()
	if marshalErr != nil {
		return state, marshalErr
	}
	state.Data = data
	if err := e.snapshot(ctx, p, &state, "branch decision"); err != nil {
		return state, err
	}
	e.observe(ctx, p, state, Event{Type: EventStepCompleted, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), Duration: completedStepDuration(state, stepPath), Operation: "branch"})
	return e.executeSteps(ctx, p, state, ds, pathSteps, 0, childPath)
}

// executeRepeat executes or resumes one iteration. An unfinished repeat always
// yields after its Until callback, even for a zero delay, which makes unbounded
// repeats scheduler-neutral and prevents synchronous runaway loops.
func (e *Executor) executeRepeat(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	ds *dataStore,
	step *Step,
	stepPath []string,
	attemptRecorded bool,
) (RunState, error) {
	repeatKey := StepPathKey(stepPath)
	iteration := 0
	childStartIdx := 0
	repeatState := state.RepeatStates[repeatKey]
	awaitingCondition := repeatState.AwaitingCondition

	if len(state.CurrentPath) > len(stepPath) {
		savedIteration := state.CurrentPath[len(stepPath)]
		parsed, err := strconv.Atoi(savedIteration)
		if err != nil || parsed < 0 {
			return state, &ErrStepFailed{
				StepName: step.Name,
				Path:     slices.Clone(stepPath),
				Err:      fmt.Errorf("invalid saved repeat iteration %q", savedIteration),
			}
		}
		iteration = parsed
		if len(state.CurrentPath) > len(stepPath)+1 {
			childName := state.CurrentPath[len(stepPath)+1]
			for i := range step.Repeat.Steps {
				if step.Repeat.Steps[i].Name == childName {
					childStartIdx = i
					break
				}
			}
		}
	} else if _, ok := state.RepeatStates[repeatKey]; ok {
		iteration = repeatState.Iteration
	}

	if state.RepeatStates == nil {
		state.RepeatStates = make(map[string]RepeatState)
	}
	iterationPath := append(slices.Clone(stepPath), strconv.Itoa(iteration))
	repeatState.Iteration = iteration
	repeatState.AwaitingCondition = awaitingCondition
	repeatState.Completed = false

	state.RepeatStates[repeatKey] = repeatState

	var iterationStartedAt time.Time
	if e.observer != nil {
		iterationStartedAt = e.clock.Now()
		e.observe(ctx, p, state, Event{
			Type:            EventRepeatIterationStarted,
			StepPath:        stepPath,
			RepeatIteration: &iteration,
			Operation:       "repeat iteration",
		})
	}

	if !awaitingCondition {
		var err error
		state, err = e.executeSteps(ctx, p, state, ds, step.Repeat.Steps, childStartIdx, iterationPath)
		if err != nil || state.Status != RunStatusRunning {
			return state, err
		}
		repeatState.AwaitingCondition = true
		state.RepeatStates[repeatKey] = repeatState
	}

	if e.repeatTimedOut(step, repeatState) {
		if attemptRecorded {
			e.recordAttemptData(&state, stepPath)
		} else {
			e.recordAttempt(ctx, p, &state, stepPath, &iteration)
		}
		return e.repeatTimeout(ctx, p, state, step, stepPath, iteration)
	}

	if attemptRecorded {
		e.recordAttemptData(&state, stepPath)
	} else {
		e.recordAttempt(ctx, p, &state, stepPath, &iteration)
	}
	done, retryAfter, err := step.Repeat.Until(ctx, ds, iteration)
	if err != nil {
		e.recordError(&state, stepPath, err)
		stepErr := &ErrStepFailed{StepName: step.Name, Path: slices.Clone(stepPath), Err: fmt.Errorf("until: %w", err)}
		if isDeferred(stepErr) {
			return e.persistDeferred(ctx, p, state, ds, stepPath, stepErr, false, "delayed continuation", &iteration)
		}
		e.observe(ctx, p, state, Event{Type: EventStepFailed, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), RepeatIteration: &iteration, Duration: e.activeStepDuration(state, stepPath), Operation: "repeat condition", Cause: stepErr})
		return state, stepErr
	}
	if e.observer != nil {
		e.observe(ctx, p, state, Event{
			Type:            EventRepeatIterationCompleted,
			StepPath:        stepPath,
			Attempt:         diagnosticAttempts(state, stepPath),
			RepeatIteration: &iteration,
			Duration:        e.clock.Now().Sub(iterationStartedAt),
			Operation:       "repeat iteration",
		})
	}

	if step.Repeat.History == RepeatHistoryCompact {
		compactRepeatIteration(&state, stepPath, iteration)
	}

	if done {
		repeatState.Iteration = iteration
		repeatState.AwaitingCondition = false
		repeatState.Completed = true
		repeatState.StartedAt = nil
		state.RepeatStates[repeatKey] = repeatState
		state.CurrentPath = slices.Clone(stepPath)
		state.CompletedSteps = append(state.CompletedSteps, CompletedStep{Path: slices.Clone(stepPath)})
		e.recordCompleted(&state, stepPath)

		data, marshalErr := ds.marshalData()
		if marshalErr != nil {
			return state, marshalErr
		}
		state.Data = data
		if snapshotErr := e.snapshot(ctx, p, &state, "repeat completion"); snapshotErr != nil {
			return state, snapshotErr
		}
		e.observe(ctx, p, state, Event{Type: EventStepCompleted, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), RepeatIteration: &iteration, Duration: completedStepDuration(state, stepPath), Operation: "repeat"})
		return state, nil
	}

	if step.Repeat.MaxIterations > 0 && iteration+1 >= step.Repeat.MaxIterations {
		limitErr := &ErrRepeatLimit{StepName: step.Name, MaxIterations: step.Repeat.MaxIterations}
		e.recordError(&state, stepPath, limitErr)
		stepErr := &ErrStepFailed{StepName: step.Name, Path: slices.Clone(stepPath), Err: limitErr}
		e.observe(ctx, p, state, Event{Type: EventStepFailed, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), RepeatIteration: &iteration, Operation: "repeat limit", Cause: stepErr})
		return state, stepErr
	}

	nextIteration := iteration + 1
	repeatState.Iteration = nextIteration
	repeatState.AwaitingCondition = false
	state.RepeatStates[repeatKey] = repeatState
	state.CurrentPath = append(slices.Clone(stepPath), strconv.Itoa(nextIteration))
	operation := "delayed continuation"
	if step.Repeat.History == RepeatHistoryCompact {
		operation = "repeat compaction"
	}
	return e.persistDeferred(ctx, p, state, ds, stepPath, RetryAfter(retryAfter, nil), false, operation, &iteration)
}

func (e *Executor) ensureRepeatStarted(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	stepPath []string,
) (RunState, error) {
	repeatKey := StepPathKey(stepPath)
	repeatState := state.RepeatStates[repeatKey]
	if repeatState.StartedAt != nil {
		return state, nil
	}
	if state.RepeatStates == nil {
		state.RepeatStates = make(map[string]RepeatState)
	}
	startedAt := e.clock.Now().UTC()
	repeatState.StartedAt = &startedAt
	state.RepeatStates[repeatKey] = repeatState
	if snapshotErr := e.snapshot(ctx, p, &state, "repeat start"); snapshotErr != nil {
		return state, snapshotErr
	}
	return state, nil
}

func (e *Executor) repeatTimedOut(step *Step, repeatState RepeatState) bool {
	return step.Repeat.MaxDuration > 0 && repeatState.StartedAt != nil &&
		e.clock.Now().Sub(*repeatState.StartedAt) >= step.Repeat.MaxDuration
}

func (e *Executor) repeatTimeout(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	step *Step,
	stepPath []string,
	iteration int,
) (RunState, error) {
	timeoutErr := &ErrRepeatTimeout{StepName: step.Name, MaxDuration: step.Repeat.MaxDuration}
	e.recordError(&state, stepPath, timeoutErr)
	stepErr := &ErrStepFailed{StepName: step.Name, Path: slices.Clone(stepPath), Err: timeoutErr}
	e.observe(ctx, p, state, Event{Type: EventStepFailed, StepPath: stepPath, Attempt: diagnosticAttempts(state, stepPath), RepeatIteration: &iteration, Operation: "repeat timeout", Cause: stepErr})
	return state, stepErr
}

// runCompensation walks completed steps in reverse, calling Compensate on each.
func (e *Executor) runCompensation(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	ds *dataStore,
) (RunState, error) {
	e.Infof("pipeline %q: starting compensation", p.Name)
	e.observe(ctx, p, state, Event{Type: EventCompensationStarted, Operation: "compensation"})

	originalError := state.Error

	// CompensationIndex is the next completed-step index to compensate. It is
	// always set by the caller before entering compensation: executeSteps stamps
	// it to len(CompletedSteps)-1 on failure, and a resumed state carries the
	// persisted value. A value of -1 means everything was already compensated
	// and only finalization remains, so the loop below must not run — never
	// reset the index here, or a crash-recovered compensation would re-run
	// already-compensated steps (double compensation).
	for i := state.CompensationIndex; i >= 0; i-- {
		cs := state.CompletedSteps[i]
		if !cs.HasCompensator {
			state.CompensationIndex = i - 1
			continue
		}
		if ctx.Err() != nil {
			e.observe(ctx, p, state, Event{Type: EventContextCanceled, StepPath: cs.Path, Operation: "compensation", Cause: ctx.Err()})
			return state, ctx.Err()
		}

		// Find the step in the pipeline definition.
		step := findStepByPath(p.Steps, cs.Path)
		if step == nil || step.Action == nil || step.Action.Compensate == nil {
			e.Warnf("compensation: step at path %v not found or has no compensator", cs.Path)
			state.CompensationIndex = i - 1
			continue
		}

		e.Infof("compensating step %q", step.Name)
		startedAt := e.clock.Now()
		e.observe(ctx, p, state, Event{Type: EventCompensationStarted, StepPath: cs.Path, Operation: "compensation step"})

		if err := step.Action.Compensate(ctx, ds); err != nil {
			state.CompensationIndex = i
			e.recordError(&state, cs.Path, err)
			if ctx.Err() != nil {
				e.observe(ctx, p, state, Event{Type: EventContextCanceled, StepPath: cs.Path, Duration: e.clock.Now().Sub(startedAt), Operation: "compensation", Cause: ctx.Err()})
				return state, ctx.Err()
			}
			if isDeferred(err) {
				return e.persistDeferred(ctx, p, state, ds, cs.Path, err, false, "delayed continuation", nil)
			}
			e.observe(ctx, p, state, Event{Type: EventCompensationFailed, StepPath: cs.Path, Duration: e.clock.Now().Sub(startedAt), Operation: "compensation step", Cause: err})

			data, marshalErr := ds.marshalData()
			if marshalErr != nil {
				return state, marshalErr
			}
			state.Data = data

			if snapshotErr := e.snapshot(ctx, p, &state, "compensation failure"); snapshotErr != nil {
				return state, snapshotErr
			}

			return state, &ErrCompensationFailed{
				Original:     fmt.Errorf("%s", originalError),
				Compensation: fmt.Errorf("step %q: %w", step.Name, err),
			}
		}

		// Mark this compensator as done by updating the index.
		state.CompensationIndex = i - 1

		data, marshalErr := ds.marshalData()
		if marshalErr != nil {
			return state, marshalErr
		}
		state.Data = data

		if err := e.snapshot(ctx, p, &state, "compensation step completion"); err != nil {
			return state, err
		}

		e.Infof("compensated step %q", step.Name)
		e.observe(ctx, p, state, Event{Type: EventCompensationSucceeded, StepPath: cs.Path, Duration: e.clock.Now().Sub(startedAt), Operation: "compensation step"})
	}

	// All compensation done.
	state.Status = RunStatusFailed
	state.CompensationIndex = -1

	data, marshalErr := ds.marshalData()
	if marshalErr != nil {
		return state, marshalErr
	}
	state.Data = data

	if err := e.snapshot(ctx, p, &state, "compensation completion"); err != nil {
		return state, err
	}

	e.Infof("pipeline %q: compensation complete, status=failed", p.Name)
	e.observe(ctx, p, state, Event{Type: EventCompensationSucceeded, Operation: "compensation"})
	e.observe(ctx, p, state, Event{Type: EventPipelineCompleted, Operation: "run", Cause: fmt.Errorf("%s", originalError)})

	return state, nil
}

// findTopLevelIndex finds the index of the first step to execute at the top level.
// CurrentPath[0] is always a top-level step name. If it's a branch with deeper
// path elements, executeBranch will handle resuming inside the branch.
func (e *Executor) findTopLevelIndex(steps []Step, path []string) int {
	if len(path) == 0 {
		return 0
	}

	stepName := path[0]
	for i, step := range steps {
		if step.Name == stepName {
			return i
		}
	}

	e.Warnf("step %q not found in pipeline, starting from beginning", stepName)
	return 0
}

// findStepByPath locates a step in the pipeline tree by its full path.
func findStepByPath(steps []Step, path []string) *Step {
	if len(path) == 0 {
		return nil
	}

	for i := range steps {
		if steps[i].Name != path[0] {
			continue
		}

		if len(path) == 1 {
			return &steps[i]
		}

		// Recurse into branch.
		if steps[i].Branch != nil && len(path) >= 3 {
			pathKey := path[1]
			pathSteps, ok := steps[i].Branch.Paths[pathKey]
			if !ok {
				return nil
			}
			return findStepByPath(pathSteps, path[2:])
		}

		// Recurse into a repeat. path[1] is the decimal iteration number.
		if steps[i].Repeat != nil && len(path) >= 3 {
			if _, err := strconv.Atoi(path[1]); err != nil {
				return nil
			}
			return findStepByPath(steps[i].Repeat.Steps, path[2:])
		}

		return nil
	}

	return nil
}

// runWithRetry executes fn with optional retry based on the step's RetryConfig.
func (e *Executor) runWithRetry(ctx context.Context, step *Step, fn func() error) error {
	maxAttempts := 1
	delay := time.Second
	backoff := false

	if step.Retry != nil {
		if step.Retry.MaxAttempts > 0 {
			maxAttempts = step.Retry.MaxAttempts
		}
		if step.Retry.InitialDelay > 0 {
			delay = step.Retry.InitialDelay
		}
		backoff = step.Retry.Backoff
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if isDeferred(lastErr) {
			return lastErr
		}

		if attempt+1 < maxAttempts {
			e.Warnf("step %q attempt %d failed: %v, retrying in %s", step.Name, attempt+1, lastErr, delay)

			if err := e.clock.Sleep(ctx, delay); err != nil {
				return err
			}

			if backoff {
				delay = nextBackoffDelay(delay)
			}
		}
	}

	return lastErr
}

// snapshot persists a revisioned copy of state. Revision is committed to the
// returned state only after the callback succeeds.
func (e *Executor) snapshot(ctx context.Context, p *Pipeline, state *RunState, operation string) error {
	if e.snapshotFn == nil && e.casSnapshotFn == nil {
		return nil
	}

	var startedAt time.Time
	if e.observer != nil {
		startedAt = e.clock.Now()
	}
	expectedRevision := state.Revision
	candidate := *state
	candidate.Revision = expectedRevision + 1

	var err error
	if e.casSnapshotFn != nil {
		err = e.casSnapshotFn(ctx, expectedRevision, candidate)
	} else {
		err = e.snapshotFn(ctx, candidate)
	}
	if err != nil {
		snapshotErr := &ErrSnapshotFailed{Operation: operation, Err: err}
		if e.observer != nil {
			e.observe(ctx, p, *state, Event{
				Type:            EventSnapshotFailed,
				StepPath:        state.CurrentPath,
				Attempt:         diagnosticAttempts(*state, state.CurrentPath),
				RepeatIteration: repeatIterationForPath(p.Steps, state.CurrentPath),
				Duration:        e.clock.Now().Sub(startedAt),
				Operation:       operation,
				Cause:           snapshotErr,
			})
		}
		return snapshotErr
	}

	state.Revision = candidate.Revision
	if e.observer != nil {
		e.observe(ctx, p, *state, Event{
			Type:            EventSnapshotSucceeded,
			StepPath:        state.CurrentPath,
			Attempt:         diagnosticAttempts(*state, state.CurrentPath),
			RepeatIteration: repeatIterationForPath(p.Steps, state.CurrentPath),
			Duration:        e.clock.Now().Sub(startedAt),
			Operation:       operation,
		})
	}
	return nil
}

// StepPathKey returns the stable key used by RunState.StepDiagnostics and
// RunState.RepeatStates. It uses JSON Pointer escaping, so names containing '/'
// or '~' remain unambiguous.
func StepPathKey(path []string) string {
	if len(path) == 0 {
		return ""
	}
	escaped := make([]string, len(path))
	for i, part := range path {
		part = strings.ReplaceAll(part, "~", "~0")
		escaped[i] = strings.ReplaceAll(part, "/", "~1")
	}
	return "/" + strings.Join(escaped, "/")
}

func (e *Executor) recordAttempt(
	ctx context.Context,
	p *Pipeline,
	state *RunState,
	path []string,
	repeatIteration *int,
) {
	attempt := e.recordAttemptData(state, path)
	if e.observer == nil {
		return
	}
	if repeatIteration == nil {
		repeatIteration = repeatIterationForPath(p.Steps, path)
	}
	e.observe(ctx, p, *state, Event{
		Type:            EventStepStarted,
		StepPath:        path,
		Attempt:         attempt,
		RepeatIteration: repeatIteration,
		Operation:       "step",
	})
}

func (e *Executor) recordAttemptData(state *RunState, path []string) int {
	if state.StepDiagnostics == nil {
		state.StepDiagnostics = make(map[string]StepDiagnostics)
	}
	key := StepPathKey(path)
	diagnostic := state.StepDiagnostics[key]
	now := e.clock.Now().UTC()
	if diagnostic.FirstStartedAt == nil {
		first := now
		diagnostic.FirstStartedAt = &first
	}
	last := now
	diagnostic.LastStartedAt = &last
	diagnostic.NextRunAt = nil
	diagnostic.Attempts++
	state.StepDiagnostics[key] = diagnostic
	return diagnostic.Attempts
}

func (e *Executor) recordError(state *RunState, path []string, err error) {
	if state.StepDiagnostics == nil {
		state.StepDiagnostics = make(map[string]StepDiagnostics)
	}
	key := StepPathKey(path)
	diagnostic := state.StepDiagnostics[key]
	if err != nil {
		diagnostic.LastError = err.Error()
	}
	state.StepDiagnostics[key] = diagnostic
}

func (e *Executor) recordCompleted(state *RunState, path []string) {
	key := StepPathKey(path)
	diagnostic := state.StepDiagnostics[key]
	now := e.clock.Now().UTC()
	diagnostic.CompletedAt = &now
	diagnostic.NextRunAt = nil
	state.StepDiagnostics[key] = diagnostic
}

func (e *Executor) recordNextRun(state *RunState, path []string, delay time.Duration, cause error) {
	key := StepPathKey(path)
	diagnostic := state.StepDiagnostics[key]
	next := e.clock.Now().UTC().Add(normalizeDuration(delay))
	diagnostic.NextRunAt = &next
	if cause != nil {
		diagnostic.LastError = cause.Error()
	}
	state.StepDiagnostics[key] = diagnostic
}

func (e *Executor) persistDeferred(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	ds *dataStore,
	path []string,
	err error,
	polling bool,
	operation string,
	repeatIteration *int,
) (RunState, error) {
	delay, cause, normalizedErr, _ := deferredDetails(err)
	if polling {
		state.Status = RunStatusPolling
	}
	e.recordNextRun(&state, path, delay, cause)
	data, marshalErr := ds.marshalData()
	if marshalErr != nil {
		return state, marshalErr
	}
	state.Data = data
	if snapshotErr := e.snapshot(ctx, p, &state, operation); snapshotErr != nil {
		return state, snapshotErr
	}
	if e.observer != nil {
		if repeatIteration == nil {
			repeatIteration = repeatIterationForPath(p.Steps, path)
		}
		e.observe(ctx, p, state, Event{
			Type:            EventDelayedContinuation,
			StepPath:        path,
			Attempt:         diagnosticAttempts(state, path),
			RepeatIteration: repeatIteration,
			Duration:        delay,
			Operation:       operation,
			Cause:           cause,
		})
	}
	return state, normalizedErr
}

func isDeferred(err error) bool {
	_, _, _, ok := deferredDetails(err)
	return ok
}

func deferredDetails(err error) (time.Duration, error, error, bool) {
	if retryAfter, ok := errors.AsType[*ErrRetryAfter](err); ok {
		retryAfter.Duration = normalizeDuration(retryAfter.Duration)
		return retryAfter.Duration, retryAfter.Cause, err, true
	}
	if snooze, ok := errors.AsType[ErrSnooze](err); ok {
		snooze.Duration = normalizeDuration(snooze.Duration)
		return snooze.Duration, nil, snooze, true
	}
	return 0, nil, err, false
}

func diagnosticAttempts(state RunState, path []string) int {
	return state.StepDiagnostics[StepPathKey(path)].Attempts
}

func (e *Executor) activeStepDuration(state RunState, path []string) time.Duration {
	diagnostic := state.StepDiagnostics[StepPathKey(path)]
	if diagnostic.LastStartedAt == nil {
		return 0
	}
	return normalizeDuration(e.clock.Now().UTC().Sub(*diagnostic.LastStartedAt))
}

func completedStepDuration(state RunState, path []string) time.Duration {
	diagnostic := state.StepDiagnostics[StepPathKey(path)]
	if diagnostic.LastStartedAt == nil || diagnostic.CompletedAt == nil {
		return 0
	}
	return normalizeDuration(diagnostic.CompletedAt.Sub(*diagnostic.LastStartedAt))
}

func repeatIterationForPath(steps []Step, path []string) *int {
	var result *int
	for len(path) > 0 {
		var step *Step
		for i := range steps {
			if steps[i].Name == path[0] {
				step = &steps[i]
				break
			}
		}
		if step == nil {
			return result
		}
		path = path[1:]
		switch {
		case step.Repeat != nil && len(path) > 0:
			iteration, err := strconv.Atoi(path[0])
			if err != nil {
				return result
			}
			result = &iteration
			path = path[1:]
			steps = step.Repeat.Steps
		case step.Branch != nil && len(path) > 0:
			steps = step.Branch.Paths[path[0]]
			path = path[1:]
		default:
			return result
		}
	}
	return result
}

func nextBackoffDelay(delay time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if delay > maxDuration/2 {
		return maxDuration
	}
	return normalizeDuration(delay * 2)
}

// checkVersion verifies the pipeline definition can resume the given state.
func checkVersion(p *Pipeline, state RunState) error {
	minVersion := p.effectiveMinResumeVersion()
	if state.Version < minVersion || state.Version > p.Version {
		return versionMismatchError(p, state)
	}
	return nil
}

func versionMismatchError(p *Pipeline, state RunState) *ErrVersionMismatch {
	return &ErrVersionMismatch{
		PipelineName:     p.Name,
		StateVersion:     state.Version,
		PipelineVersion:  p.Version,
		MinResumeVersion: p.effectiveMinResumeVersion(),
	}
}

// isStepCompleted reports whether a step at the exact path already finished.
func isStepCompleted(state RunState, path []string) bool {
	for _, cs := range state.CompletedSteps {
		if slices.Equal(cs.Path, path) {
			return true
		}
	}
	return false
}

// isPathPrefix checks if prefix is a prefix of path and path is longer.
func isPathPrefix(prefix, path []string) bool {
	if len(path) <= len(prefix) {
		return false
	}
	for i, p := range prefix {
		if path[i] != p {
			return false
		}
	}
	return true
}

// branchPathNames returns the available path names for error messages.
func branchPathNames(b *BranchStep) []string {
	names := make([]string, 0, len(b.Paths))
	for k := range b.Paths {
		names = append(names, k)
	}
	return names
}
