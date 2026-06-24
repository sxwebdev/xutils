// Package workflow provides an imperative, stage-based workflow engine with
// two-level hierarchy (Stage → Step), typed navigation, snapshot persistence,
// and automatic compensation.
//
// # Core Concepts
//
// A [Workflow] contains an ordered list of [Stage] objects, each holding one or more
// [Step] objects. Steps execute sequentially. Every step receives a [StepContext]
// with full access to the parent workflow, current stage, and step metadata.
//
// Navigation between stages and steps is controlled through typed references
// ([StageRef] and [StepRef]) returned by constructors, enabling compile-time safe jumps.
//
// # Quick Start
//
//	// Define steps.
//	fetchStep, fetchRef := workflow.NewStep("fetch_data", func(sc *workflow.StepContext) error {
//		workflow.SetVar(sc.Workflow, "result", "hello")
//		return nil
//	})
//
//	processStep, _ := workflow.NewStep("process_data", func(sc *workflow.StepContext) error {
//		result, _ := workflow.GetVar[string](sc.Workflow, "result")
//		fmt.Println(result) // "hello"
//		return nil
//	})
//
//	// Define stages.
//	mainStage, _ := workflow.NewStage("main",
//		workflow.WithStageSteps([]*workflow.Step{fetchStep, processStep}),
//	)
//
//	// Build and run.
//	wf := workflow.New(
//		workflow.WithName("my_workflow"),
//	)
//	wf.Stages = []*workflow.Stage{mainStage}
//
//	err := wf.Run(context.Background())
//
// # Stages and Steps
//
// Stages group related steps and provide their own before/after hooks.
// Use [NewStage] with [WithStageSteps], [WithStageBeforeFn], and [WithStageAfterFn]:
//
//	stage, stageRef := workflow.NewStage("provisioning",
//		workflow.WithStageSteps([]*workflow.Step{step1, step2, step3}),
//		workflow.WithStageBeforeFn(func(ctx context.Context, s *workflow.Stage) error {
//			log.Printf("starting stage: %s", s.Name)
//			return nil
//		}),
//	)
//
// Steps are the atomic units of work. Use [NewStep] with options:
//
//	step, stepRef := workflow.NewStep("call_api", myStepFunc,
//		workflow.WithStepTimeout(5*time.Second),
//		workflow.WithStepMaxRetries(3),
//		workflow.WithStepRetryPolicy(workflow.RunWithBackoff),
//	)
//
// # StepContext
//
// Every step function receives a [StepContext] that embeds [context.Context]
// and provides access to the full execution environment:
//
//	func myStep(sc *workflow.StepContext) error {
//		// Access parent workflow.
//		sc.Workflow.Name
//
//		// Access current stage and step.
//		sc.Stage.Name
//		sc.Step.Name
//
//		// Use as context.Context (deadline, cancellation).
//		sc.Deadline()
//
//		// Share data between steps.
//		workflow.SetVar(sc.Workflow, "key", "value")
//
//		return nil
//	}
//
// # Typed Navigation (GoTo)
//
// [StepRef] and [StageRef] enable compile-time safe jumps between stages and steps.
// References are returned by [NewStep] and [NewStage]:
//
//	step1, step1Ref := workflow.NewStep("step_1", step1Fn)
//	step3, step3Ref := workflow.NewStep("step_3", step3Fn)
//	compensationStage, compStageRef := workflow.NewStage("compensation", ...)
//
// Inside a step function, use [WorkflowState.GoToStage] and [WorkflowState.GoToStep]
// to jump. Intermediate steps are marked as skipped:
//
//	func decideNext(sc *workflow.StepContext) error {
//		if needsCompensation {
//			sc.Workflow.State.GoToStage(compStageRef)
//			sc.Workflow.State.GoToStep(compStepRef)
//		} else {
//			sc.Workflow.State.GoToStep(step3Ref) // skip step_2
//		}
//		return nil
//	}
//
// # Data Passing Between Steps
//
// Use the generic [SetVar] and [GetVar] functions to share typed data
// between steps. The variable store is thread-safe:
//
//	// In producer step:
//	workflow.SetVar(sc.Workflow, "order_id", "ord-123")
//	workflow.SetVar(sc.Workflow, "amount", 99.50)
//
//	// In consumer step:
//	orderID, ok := workflow.GetVar[string](sc.Workflow, "order_id")
//	amount, ok := workflow.GetVar[float64](sc.Workflow, "amount")
//
// Steps also have per-step arguments via [StepState.SetArg] and [StepState.Args]:
//
//	sc.Step.State.SetArg("tx_hash", "0xabc...")
//
// # Retry Policies
//
// Each step has a configurable retry policy. Built-in policies:
//
//   - [RunWithLinear] — fixed delay (the step Timeout) between retries (default)
//   - [RunWithBackoff] — exponential backoff starting from the step Timeout
//     and doubling each retry (e.g. with a 1s timeout: 1s, 2s, 4s, ...)
//
// Configure via options or chainable setters:
//
//	step, _ := workflow.NewStep("flaky_api", callAPI,
//		workflow.WithStepMaxRetries(5),             // 5 attempts
//		workflow.WithStepTimeout(2*time.Second),     // 2s between retries
//		workflow.WithStepRetryPolicy(workflow.RunWithBackoff),
//	)
//
// Use MaxRetries = -1 for unlimited retries (until context cancellation):
//
//	step, _ := workflow.NewStep("wait_forever", pollFn,
//		workflow.WithStepMaxRetries(-1),
//		workflow.WithStepTimeout(10*time.Second),
//	)
//
// Custom retry policies implement the [RetryPolicyFn] signature:
//
//	func myRetryPolicy(sc *workflow.StepContext) error {
//		// Custom logic, access sc.Step.MaxRetries, sc.Step.Timeout, etc.
//		return sc.Step.Func(sc)
//	}
//
// # Control Flow Errors
//
// Sentinel errors control execution flow without propagating as failures:
//
//   - [ErrSkipStep] — skip the current step, continue to the next
//   - [ErrSkipStage] — skip remaining steps in the current stage
//   - [ErrBreakStages] — stop all stage execution (workflow completes)
//   - [ErrExitWorkflow] — exit the workflow immediately (no error)
//
// Example:
//
//	func conditionalStep(sc *workflow.StepContext) error {
//		if alreadyDone {
//			return workflow.ErrSkipStep
//		}
//		return doWork(sc)
//	}
//
// # Silent Errors
//
// [SilentError] wraps an error with [ErrSilent] to suppress logging in retry loops.
// Useful for expected transient failures that would otherwise flood logs:
//
//	func pollExternalService(sc *workflow.StepContext) error {
//		result, err := client.Check(sc)
//		if err != nil {
//			return workflow.SilentError(err) // retries without logging each failure
//		}
//		return nil
//	}
//
// # FinishWorkflow Flag
//
// Set [Step.FinishWorkflow] to true to terminate the workflow after the step completes
// successfully, skipping all remaining stages:
//
//	step.FinishWorkflow = true
//	// or
//	step.SetStepFinishWorkflow(true)
//
// # Hooks
//
// Hooks are available at three levels:
//
// Workflow level:
//   - [WithBeforeFn] — called once before execution starts
//   - [WithAfterFn] — called once after execution completes
//   - [WithOnFailureFn] — called on any step failure (for custom error handling)
//   - [WithBeforeAllStepsFn] — called before every step (logging, metrics)
//   - [WithAfterAllStepsFn] — called after every step
//
// Stage level:
//   - [WithStageBeforeFn] — called before the stage starts
//   - [WithStageAfterFn] — called after the stage completes
//
// Step level:
//   - [WithStepBeforeFn] — called before the step executes
//   - [WithStepAfterFn] — called after the step completes
//
// Example with workflow-level hooks for observability:
//
//	wf := workflow.New(
//		workflow.WithName("order_processing"),
//		workflow.WithBeforeAllStepsFn(func(ctx context.Context, w *workflow.Workflow, st *workflow.Stage, s *workflow.Step) error {
//			metrics.StepStarted(w.Name, st.Name, s.Name)
//			return nil
//		}),
//		workflow.WithAfterAllStepsFn(func(ctx context.Context, w *workflow.Workflow, st *workflow.Stage, s *workflow.Step) error {
//			metrics.StepCompleted(w.Name, st.Name, s.Name, s.State.Status.String())
//			return nil
//		}),
//	)
//
// # Snapshots and Persistence
//
// A [Snapshot] captures the complete workflow state: step states, workflow state,
// and shared variables. Snapshots are automatically created before and after
// each step, and on failure.
//
// Register a persistence callback via [WithSnapshotFn]:
//
//	wf := workflow.New(
//		workflow.WithSnapshotFn(func(ctx context.Context, w *workflow.Workflow, snap workflow.Snapshot) error {
//			data, _ := json.Marshal(snap)
//			return db.Save(ctx, w.Name, data)
//		}),
//	)
//
// Retrieve snapshots programmatically:
//
//	snap := wf.GetSnapshot()          // as Snapshot struct
//	jsonStr := wf.GetJSONSnapshot()   // as JSON string
//
// Restore from a snapshot to resume execution:
//
//	wf.SetSnapshot(snap)              // from Snapshot struct
//	wf.SetJSONSnapshot(jsonStr)       // from JSON string
//	err := wf.Run(ctx)                // resumes from where it left off
//
// Already-completed steps are skipped automatically on resume.
//
// Query step arguments from a snapshot using generics:
//
//	txHash, err := workflow.GetArg[string](snap, "send_tx", "tx_hash")
//	txHash, err := workflow.GetArgByRef[string](snap, sendTxRef, "tx_hash")
//
// # Compensation
//
// Use [WithCompensationStage] to configure automatic routing to a compensation
// stage when any step fails. The workflow sets NextStage/NextStep to the
// compensation targets, saves a snapshot, and suppresses the error:
//
//	compensationStage, compStageRef := workflow.NewStage("compensation",
//		workflow.WithStageSteps([]*workflow.Step{rollbackStep}),
//	)
//	rollbackStep, rollbackRef := workflow.NewStep("rollback", rollbackFn)
//
//	wf := workflow.New(
//		workflow.WithName("order"),
//		workflow.WithCompensationStage(compStageRef, rollbackRef),
//	)
//	wf.Stages = []*workflow.Stage{mainStage, compensationStage}
//
// On failure, the next call to [Workflow.Run] (after restoring from snapshot)
// will execute from the compensation stage. Only step-execution failures route
// to compensation; setup/validation errors (init, before hooks) surface from
// Run unchanged.
//
// # Graceful Shutdown
//
// The workflow respects context cancellation. When the context is cancelled,
// the workflow sets an internal stop flag and waits up to 10 seconds for the
// current step to finish. Retry sleeps also respond to cancellation:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//
//	err := wf.Run(ctx) // stops gracefully on timeout
//
// # Step Status
//
// Each step tracks its execution status via [StepStatus]:
//
//   - [StepStatusPending] — step is about to execute
//   - [StepStatusProcessing] — step is currently running
//   - [StepStatusCompleted] — step finished successfully
//   - [StepStatusFailed] — step failed
//   - [StepStatusSuspended] — step is suspended (paused)
//   - [StepStatusSkipped] — step was skipped (via GoTo or sentinel error)
//
// # Workflow Options
//
//   - [WithName] — set the workflow name
//   - [WithDebug] — enable debug logging
//   - [WithState] — set initial workflow state
//   - [WithLogger] — set a structured logger (loggerutil.Logger)
//   - [WithBeforeFn] — before execution hook
//   - [WithAfterFn] — after execution hook
//   - [WithBeforeAllStepsFn] — before every step hook
//   - [WithAfterAllStepsFn] — after every step hook
//   - [WithOnFailureFn] — failure handler
//   - [WithSnapshotFn] — state persistence callback
//   - [WithCompensationStage] — automatic compensation routing
//
// # Step Options
//
//   - [WithStepBeforeFn] — pre-execution hook
//   - [WithStepAfterFn] — post-execution hook
//   - [WithStepTimeout] — retry delay duration
//   - [WithStepMaxRetries] — maximum retry attempts (-1 for unlimited)
//   - [WithStepRetryPolicy] — custom retry policy function
//   - [WithStepArgs] — arbitrary step arguments
//   - [WithStepKind] — step categorization string
//
// # Stage Options
//
//   - [WithStageBeforeFn] — pre-stage hook
//   - [WithStageAfterFn] — post-stage hook
//   - [WithStageSteps] — set steps for the stage
//
// # Execution Lifecycle
//
// A typical execution flow:
//
//  1. Create steps with [NewStep] and stages with [NewStage]
//  2. Build a workflow with [New] and assign stages
//  3. Call [Workflow.Run] with a context
//  4. The workflow initializes (validates uniqueness, builds indices)
//  5. Stages execute in order; within each stage, steps execute in order
//  6. Each step runs through its retry policy with before/after hooks
//  7. Snapshots are saved before and after each step
//  8. On failure: snapshot is saved, OnFailureFn is called, compensation is applied
//  9. On success: State.IsCompleted is set to true
//
// The workflow automatically skips completed, skipped, or suspended steps on resume.
package workflow
