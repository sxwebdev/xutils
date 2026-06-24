package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sxwebdev/xutils/loggerutil"
)

// Workflow represents the entire workflow composed of multiple stages.
type Workflow struct {
	// Name is the name of the workflow.
	Name string
	// State is the state of the workflow.
	State WorkflowState
	// Stages is the list of stages for the workflow.
	Stages []*Stage
	// BeforeFn is the before start function for the workflow.
	BeforeFn func(context.Context, *Workflow) error
	// AfterFn is the after complete function for the workflow.
	AfterFn func(context.Context, *Workflow) error
	// OnFailureFn is the on failure function for the workflow.
	OnFailureFn func(context.Context, *Workflow, error) error

	// BeforeAllStepsFn is the before all steps function for the workflow.
	BeforeAllStepsFn func(context.Context, *Workflow, *Stage, *Step) error
	// AfterAllStepsFn is the after all steps function for the workflow.
	AfterAllStepsFn func(context.Context, *Workflow, *Stage, *Step) error

	// SnapshotFn is called automatically before and after each step,
	// and on failure, to persist workflow state.
	SnapshotFn func(ctx context.Context, w *Workflow, snapshot Snapshot) error

	// CompensationStage configures automatic routing to a compensation stage on failure.
	compensationStageRef *StageRef
	compensationStepRef  *StepRef

	// Debug allows to enable debug mode.
	Debug bool

	logger loggerutil.Logger

	isStopped atomic.Bool
	// executionStarted is set once init and the before hooks pass, i.e. step
	// execution actually began. Compensation routing is gated on it so a
	// setup/validation failure is not swallowed by the compensation mechanism.
	executionStarted atomic.Bool
	prevStep         *Step
	shutdownTimeout  time.Duration

	isSkipError bool

	// varsMu protects the vars map.
	varsMu sync.RWMutex
	// vars is a shared data store for passing data between steps.
	vars map[string]any

	// indices built during init() for fast lookups.
	stageIndex map[string]*Stage
	stepIndex  map[string]*Step
}

func New(opts ...WorkflowOption) *Workflow {
	wf := &Workflow{}

	for _, opt := range opts {
		opt(wf)
	}

	return wf
}

// Run executes the workflow.
func (w *Workflow) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.execute(ctx)
	}()

	select {
	case err := <-errCh:
		return w.finish(ctx, err)
	case <-ctx.Done():
		w.isStopped.Store(true)
	}

	shutdownTimeout := w.shutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = 10 * time.Second
	}

	select {
	case <-time.After(shutdownTimeout):
		// execute() exceeded the grace period and is still running in its
		// goroutine, which still owns the workflow state. Return the timeout
		// without running the failure pipeline or reading the state: doing so
		// here would race the live goroutine (and mis-route the navigation it is
		// concurrently writing). The run is abandoned, not failed.
		return fmt.Errorf("workflow shutdown execution timeout")
	case err := <-errCh:
		return w.finish(ctx, err)
	}
}

// finish runs the failure-handling pipeline — failure snapshot, OnFailureFn, and
// automatic compensation routing — after execute() has returned. It must be
// called only once the execute goroutine has finished, so it can read and mutate
// workflow state without racing it. It returns the error Run should surface: nil
// when compensation routing or SetSkipError suppresses it.
func (w *Workflow) finish(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	// save snapshot on failure
	if w.SnapshotFn != nil {
		if snapshotErr := w.SnapshotFn(ctx, w, w.GetSnapshot()); snapshotErr != nil {
			w.Errorf("snapshot on failure: %s", snapshotErr)
		}
	}

	// call user's OnFailureFn first (for custom error serialization, etc.)
	if w.OnFailureFn != nil {
		if failureErr := w.OnFailureFn(ctx, w, err); failureErr != nil {
			w.Errorf("workflow on failure: %s", failureErr)
		}
	}

	// apply automatic compensation if configured — but only for failures that
	// occurred during step execution, not setup/validation errors.
	if w.compensationStageRef != nil && w.compensationStepRef != nil && w.executionStarted.Load() {
		w.State.SetError(err)
		w.State.GoToStage(*w.compensationStageRef)
		w.State.GoToStep(*w.compensationStepRef)

		// save snapshot after compensation navigation
		if w.SnapshotFn != nil {
			if snapshotErr := w.SnapshotFn(ctx, w, w.GetSnapshot()); snapshotErr != nil {
				w.Errorf("snapshot after compensation: %s", snapshotErr)
			}
		}

		w.isSkipError = true
	}

	if w.isSkipError {
		return nil
	}

	return err
}

func (w *Workflow) execute(ctx context.Context) (err error) {
	// Reset per-run so a failed setup on a later Run is not gated by a prior run.
	w.executionStarted.Store(false)

	// initialize the workflow
	if err := w.init(); err != nil {
		return fmt.Errorf("workflow init: %w", err)
	}

	// check if workflow is already completed, suspended or failed
	if w.checkStatus() {
		return nil
	}

	if w.BeforeFn != nil {
		if err := w.BeforeFn(ctx, w); err != nil {
			return fmt.Errorf("workflow before: %w", err)
		}
	}

	// Setup succeeded; from here failures route to compensation when configured.
	w.executionStarted.Store(true)

	var stepHandlerError error
	for _, stage := range w.Stages {
		if w.isStopped.Load() {
			return ctx.Err()
		}

		if err := w.handleStage(ctx, stage); err != nil {
			// skip stage
			if errors.Is(err, ErrSkipStage) {
				continue
			}

			// skip all stages
			if errors.Is(err, ErrBreakStages) {
				break
			}

			// exit from workflow
			if errors.Is(err, ErrExitWorkflow) {
				return nil
			}

			stepHandlerError = err

			break
		}
	}

	if stepHandlerError == nil {
		w.State.SetCompleted(true)
	}

	if w.AfterFn != nil {
		if err := w.AfterFn(ctx, w); err != nil {
			w.Errorf("workflow after: %s", err)
		}
	}

	return stepHandlerError
}

func (w *Workflow) init() error {
	// build indices and check for unique names
	w.stageIndex = make(map[string]*Stage, len(w.Stages))
	w.stepIndex = make(map[string]*Step)

	for stageIdx, stage := range w.Stages {
		if stage == nil {
			return fmt.Errorf("stage at index [%d] is nil", stageIdx)
		}

		if _, ok := w.stageIndex[stage.Name]; ok {
			return fmt.Errorf("stage [%s] is not unique", stage.Name)
		}
		w.stageIndex[stage.Name] = stage

		for stepIdx, step := range stage.Steps {
			if step == nil {
				return fmt.Errorf("step at index [%d] in stage [%s] is nil", stepIdx, stage.Name)
			}

			if _, ok := w.stepIndex[step.Name]; ok {
				return fmt.Errorf("step [%s] is not unique (in stage [%s])", step.Name, stage.Name)
			}
			w.stepIndex[step.Name] = step

			if step.State == nil {
				step.State = NewStepState()
			}

			step.setDefaultValues()

			if step.Func == nil {
				return fmt.Errorf("step [%s] in stage [%s] has no function", step.Name, stage.Name)
			}

			// Defensive: setDefaultValues above always assigns a retry policy, so
			// this is unreachable in practice — kept as a guard against future
			// reordering of init.
			if step.RetryPolicy == nil {
				return fmt.Errorf("step [%s] in stage [%s] has no retry policy", step.Name, stage.Name)
			}

			// set current stage and step
			step.State.SetCurrentStage(stage.Name)
			step.State.SetCurrentStep(step.Name)
		}
	}

	// validate compensation refs
	if w.compensationStageRef != nil {
		if _, ok := w.stageIndex[w.compensationStageRef.name]; !ok {
			return fmt.Errorf("compensation stage [%s] not found", w.compensationStageRef.name)
		}
	}
	if w.compensationStepRef != nil {
		if _, ok := w.stepIndex[w.compensationStepRef.name]; !ok {
			return fmt.Errorf("compensation step [%s] not found", w.compensationStepRef.name)
		}
	}

	w.Debugf("start workflow: %s", w.Name)
	w.Debugf("stages count: %d", len(w.Stages))

	return nil
}

// handleStage handles the stage.
func (w *Workflow) handleStage(ctx context.Context, stage *Stage) error {
	nextStageName := w.State.NextStage

	w.Debugf("start stage: %s", stage.Name)

	// check if workflow is already completed or suspended
	if w.checkStatus() {
		return ErrExitWorkflow
	}

	if nextStageName != "" {
		if nextStageName != stage.Name {
			w.Debugf("skipping stage: %s (next stage)", stage.Name)
			return ErrSkipStage
		}
		w.State.NextStage = ""
	}

	if len(stage.Steps) == 0 {
		w.Warnf("skipping stage: %s (no steps)", stage.Name)
		return ErrSkipStage
	}

	if stage.BeforeFn != nil {
		if err := stage.BeforeFn(ctx, stage); err != nil {
			return err
		}
	}

	for _, step := range stage.Steps {
		if w.isStopped.Load() {
			return ctx.Err()
		}

		if err := w.handleStep(ctx, stage, step); err != nil {
			if errors.Is(err, ErrBreakStages) {
				return ErrBreakStages
			}

			if errors.Is(err, ErrSkipStep) {
				continue
			}

			if errors.Is(err, ErrExitWorkflow) {
				return ErrExitWorkflow
			}

			return err
		}
	}

	if stage.AfterFn != nil {
		if err := stage.AfterFn(ctx, stage); err != nil {
			return err
		}
	}

	return nil
}

// handleStep handles the step.
func (w *Workflow) handleStep(ctx context.Context, stage *Stage, step *Step) (err error) {
	nextStepName := w.State.NextStep
	if nextStepName != "" && nextStepName == step.Name {
		w.State.NextStep = ""
	}

	w.Debugf("executing step: %s / %s", stage.Name, step.Name)

	// check if workflow is already completed or suspended
	if w.checkStatus() {
		return ErrExitWorkflow
	}

	if step.State.Status == StepStatusCompleted {
		w.Debugf("skipping step: %s (already completed)", step.Name)
		return ErrSkipStep
	}

	if step.State.Status == StepStatusSkipped {
		w.Debugf("skipping step: %s (skipped)", step.Name)
		return ErrSkipStep
	}

	if step.State.Status == StepStatusSuspended {
		w.Debugf("skipping step: %s (suspended)", step.Name)
		return ErrBreakStages
	}

	if nextStepName != "" {
		if nextStepName != step.Name {
			w.Debugf("skipping step: %s (next step %s)", step.Name, nextStepName)
			step.State.SetStatus(StepStatusSkipped)
			return ErrSkipStep
		}
		w.State.NextStep = ""
	}

	// stepSucceeded records that the step's own work (and its hooks) finished
	// successfully, even if we then return a control-flow signal (FinishWorkflow
	// returns ErrBreakStages but the step itself completed).
	var stepSucceeded bool

	step.State.SetStartTime(time.Now())
	defer func() {
		step.State.SetEndTime(time.Now())
		switch {
		case err == nil || stepSucceeded:
			step.State.SetError(nil)
			step.State.SetStatus(StepStatusCompleted)
		case isControlFlowError(err):
			// The step returned a control-flow sentinel (skip/break/exit)
			// instead of doing work — it was not a failure.
			step.State.SetStatus(StepStatusSkipped)
		default:
			step.State.SetStatus(StepStatusFailed)
			step.State.SetError(err)
		}

		// save snapshot after step completes (success or failure)
		if w.SnapshotFn != nil {
			if snapshotErr := w.SnapshotFn(ctx, w, w.GetSnapshot()); snapshotErr != nil {
				w.Errorf("snapshot after step: %s", snapshotErr)
			}
		}
	}()

	step.State.SetStatus(StepStatusPending)

	if w.prevStep != nil && w.prevStep.State != nil {
		step.State.SetPreviousStage(w.prevStep.State.CurrentStage)
		step.State.SetPreviousStep(w.prevStep.State.CurrentStep)
	}

	w.prevStep = step

	step.State.SetStatus(StepStatusProcessing)

	// save snapshot before step execution
	if w.SnapshotFn != nil {
		if snapshotErr := w.SnapshotFn(ctx, w, w.GetSnapshot()); snapshotErr != nil {
			w.Errorf("snapshot before step: %s", snapshotErr)
		}
	}

	// build step context
	sc := &StepContext{
		Context:  ctx,
		Workflow: w,
		Stage:    stage,
		Step:     step,
	}

	// run before all steps function
	if w.BeforeAllStepsFn != nil {
		if err := w.BeforeAllStepsFn(ctx, w, stage, step); err != nil {
			return err
		}
	}

	// run before function
	if step.BeforeFn != nil {
		if err := step.BeforeFn(ctx, step); err != nil {
			return err
		}
	}

	// run step function via retry policy
	err = step.RetryPolicy(sc)

	// run after function
	if step.AfterFn != nil {
		if afterErr := step.AfterFn(ctx, step); afterErr != nil {
			return afterErr
		}
	}

	// run after all steps function
	if w.AfterAllStepsFn != nil {
		if afterErr := w.AfterAllStepsFn(ctx, w, stage, step); afterErr != nil {
			return afterErr
		}
	}

	if err != nil {
		return fmt.Errorf("stage [%s] failed on step [%s]: %w", stage.Name, step.Name, err)
	}

	// The step's own work succeeded; mark it so the deferred status logic records
	// Completed even when FinishWorkflow makes us return a control-flow break.
	stepSucceeded = true

	if step.FinishWorkflow {
		return ErrBreakStages
	}

	return nil
}
