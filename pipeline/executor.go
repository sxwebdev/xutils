package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/sxwebdev/xutils/loggerutil"
)

// Executor runs a Pipeline with persistence and compensation support.
// It is stateless — all state is passed in and returned via RunState.
type Executor struct {
	logger     loggerutil.Logger
	debug      bool
	snapshotFn SnapshotFunc
}

// NewExecutor creates a new Executor with the given options.
func NewExecutor(opts ...ExecutorOption) *Executor {
	e := &Executor{}
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
//   - ErrSnooze if a poll step is waiting (caller should re-invoke after Duration)
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

	// Stamp version on new executions.
	if state.Status == RunStatusNew {
		state.Version = p.Version
	} else {
		// Version check for resume (non-new, non-terminal).
		if err := checkVersion(p, state); err != nil {
			return state, err
		}
	}

	// Initialize data store.
	ds := newDataStore()
	if state.Data != nil {
		ds.restoreData(state.Data)
	}

	// If compensating, continue compensation.
	if state.Status == RunStatusCompensating {
		return e.runCompensation(ctx, p, state, ds)
	}

	// Set status to running.
	state.Status = RunStatusRunning

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

		data, marshalErr := ds.marshalData()
		if marshalErr != nil {
			return state, marshalErr
		}
		state.Data = data

		if snapshotErr := e.snapshot(ctx, state); snapshotErr != nil {
			e.Errorf("pipeline %q: snapshot on completion: %v", p.Name, snapshotErr)
		}

		e.Infof("pipeline %q: completed successfully", p.Name)
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

		// Check context cancellation.
		if ctx.Err() != nil {
			data, marshalErr := ds.marshalData()
			if marshalErr != nil {
				return state, marshalErr
			}
			state.Data = data
			state.CurrentPath = stepPath
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
			// ErrSnooze — poll step waiting, return to caller.
			var snoozeErr ErrSnooze
			if errors.As(err, &snoozeErr) {
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

				if snapshotErr := e.snapshot(ctx, state); snapshotErr != nil {
					e.Errorf("pipeline %q: snapshot on retryable error: %v", p.Name, snapshotErr)
				}

				return state, err
			}

			// Step failed — start compensation.
			e.Errorf("pipeline %q: step %q failed: %v", p.Name, step.Name, err)
			state.Error = err.Error()
			state.FailedStepPath = stepPath
			state.Status = RunStatusCompensating
			state.CompensationIndex = len(state.CompletedSteps) - 1

			data, marshalErr := ds.marshalData()
			if marshalErr != nil {
				return state, marshalErr
			}
			state.Data = data

			if snapshotErr := e.snapshot(ctx, state); snapshotErr != nil {
				e.Errorf("pipeline %q: snapshot on failure: %v", p.Name, snapshotErr)
			}

			return e.runCompensation(ctx, p, state, ds)
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
	// Run OnEnter hook.
	if step.OnEnter != nil {
		if err := step.OnEnter(ctx, ds); err != nil {
			return state, &ErrStepFailed{StepName: step.Name, Path: stepPath, Err: fmt.Errorf("on_enter: %w", err)}
		}
	}

	switch {
	case step.Action != nil:
		return e.executeAction(ctx, p, state, ds, step, stepPath)
	case step.Poll != nil:
		return e.executePoll(ctx, p, state, ds, step, stepPath)
	case step.Branch != nil:
		return e.executeBranch(ctx, p, state, ds, step, stepPath)
	default:
		return state, fmt.Errorf("pipeline: step %q has no action, poll, or branch", step.Name)
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
) (RunState, error) {
	e.Debugf("action step: %s", step.Name)

	err := e.runWithRetry(ctx, step, func() error {
		return step.Action.Do(ctx, ds)
	})
	if err != nil {
		return state, &ErrStepFailed{StepName: step.Name, Path: stepPath, Err: err}
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
	e.Infof("step %q completed", step.Name)

	if err := e.snapshot(ctx, state); err != nil {
		e.Errorf("pipeline %q: snapshot after action: %v", p.Name, err)
	}

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
) (RunState, error) {
	e.Debugf("poll step: %s", step.Name)

	// Track poll start time for MaxDuration.
	now := time.Now()
	if state.PollStartedAt == nil {
		state.PollStartedAt = &now
	}

	// Check MaxDuration.
	if step.Poll.MaxDuration > 0 {
		elapsed := now.Sub(*state.PollStartedAt)
		if elapsed >= step.Poll.MaxDuration {
			state.PollStartedAt = nil
			return state, &ErrStepFailed{
				StepName: step.Name,
				Path:     stepPath,
				Err:      &ErrPollTimeout{StepName: step.Name, MaxDuration: step.Poll.MaxDuration},
			}
		}
	}

	done, retryAfter, err := step.Poll.Check(ctx, ds)
	if err != nil {
		state.PollStartedAt = nil
		return state, &ErrStepFailed{StepName: step.Name, Path: stepPath, Err: err}
	}

	if !done {
		// Save state and return snooze.
		state.Status = RunStatusPolling

		data, marshalErr := ds.marshalData()
		if marshalErr != nil {
			return state, marshalErr
		}
		state.Data = data

		if err := e.snapshot(ctx, state); err != nil {
			e.Errorf("pipeline %q: snapshot on poll snooze: %v", p.Name, err)
		}

		return state, ErrSnooze{Duration: retryAfter}
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
	e.Infof("poll step %q completed", step.Name)

	if err := e.snapshot(ctx, state); err != nil {
		e.Errorf("pipeline %q: snapshot after poll: %v", p.Name, err)
	}

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
	chosenPath, err = step.Branch.Decide(ctx, ds)
	if err != nil {
		return state, &ErrStepFailed{StepName: step.Name, Path: stepPath, Err: fmt.Errorf("decide: %w", err)}
	}

	pathSteps, ok := step.Branch.Paths[chosenPath]
	if !ok {
		return state, &ErrStepFailed{
			StepName: step.Name,
			Path:     stepPath,
			Err:      fmt.Errorf("branch returned unknown path %q (available: %v)", chosenPath, branchPathNames(step.Branch)),
		}
	}

	e.Infof("branch %q chose path %q", step.Name, chosenPath)

	// Record branch decision as a completed step (no compensator).
	state.CompletedSteps = append(state.CompletedSteps, CompletedStep{
		Path:           slices.Clone(stepPath),
		HasCompensator: false,
	})

	if len(pathSteps) == 0 {
		// Empty path — skip.
		return state, nil
	}

	childPath := append(slices.Clone(stepPath), chosenPath)
	return e.executeSteps(ctx, p, state, ds, pathSteps, 0, childPath)
}

// runCompensation walks completed steps in reverse, calling Compensate on each.
func (e *Executor) runCompensation(
	ctx context.Context,
	p *Pipeline,
	state RunState,
	ds *dataStore,
) (RunState, error) {
	e.Infof("pipeline %q: starting compensation", p.Name)

	originalError := state.Error

	// Start from CompensationIndex (or last step if not set).
	if state.CompensationIndex <= 0 {
		state.CompensationIndex = len(state.CompletedSteps) - 1
	}

	for i := state.CompensationIndex; i >= 0; i-- {
		cs := state.CompletedSteps[i]
		if !cs.HasCompensator {
			continue
		}

		// Find the step in the pipeline definition.
		step := findStepByPath(p.Steps, cs.Path)
		if step == nil || step.Action == nil || step.Action.Compensate == nil {
			e.Warnf("compensation: step at path %v not found or has no compensator", cs.Path)
			continue
		}

		e.Infof("compensating step %q", step.Name)

		if err := step.Action.Compensate(ctx, ds); err != nil {
			state.CompensationIndex = i

			data, marshalErr := ds.marshalData()
			if marshalErr != nil {
				return state, marshalErr
			}
			state.Data = data

			if err := e.snapshot(ctx, state); err != nil {
				e.Errorf("pipeline %q: snapshot on compensation failure: %v", p.Name, err)
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

		if err := e.snapshot(ctx, state); err != nil {
			e.Errorf("pipeline %q: snapshot after compensation step: %v", p.Name, err)
		}

		e.Infof("compensated step %q", step.Name)
	}

	// All compensation done.
	state.Status = RunStatusFailed
	state.CompensationIndex = 0

	data, marshalErr := ds.marshalData()
	if marshalErr != nil {
		return state, marshalErr
	}
	state.Data = data

	if err := e.snapshot(ctx, state); err != nil {
		e.Errorf("pipeline %q: snapshot after compensation complete: %v", p.Name, err)
	}

	e.Infof("pipeline %q: compensation complete, status=failed", p.Name)

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

		if attempt+1 < maxAttempts {
			e.Warnf("step %q attempt %d failed: %v, retrying in %s", step.Name, attempt+1, lastErr, delay)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}

			if backoff {
				delay *= 2
			}
		}
	}

	return lastErr
}

// snapshot calls the snapshot function if configured.
func (e *Executor) snapshot(ctx context.Context, state RunState) error {
	if e.snapshotFn == nil {
		return nil
	}
	return e.snapshotFn(ctx, state)
}

// checkVersion verifies the pipeline definition can resume the given state.
func checkVersion(p *Pipeline, state RunState) error {
	minVersion := p.effectiveMinResumeVersion()
	if state.Version < minVersion || state.Version > p.Version {
		return &ErrVersionMismatch{
			PipelineName:     p.Name,
			StateVersion:     state.Version,
			PipelineVersion:  p.Version,
			MinResumeVersion: minVersion,
		}
	}
	return nil
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
