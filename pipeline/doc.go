// Package pipeline provides a declarative, resumable, and persistent workflow engine.
//
// Pipelines survive process restarts, support automatic rollback (compensation),
// and handle long-running asynchronous operations through a poll-and-resume model.
//
// # Core Concepts
//
// A [Pipeline] is a named sequence of steps. Each step is exactly one of three types:
//
//   - [Action] — executes once, optionally defines a compensating (rollback) action
//   - [Poll] — checks a condition repeatedly, pausing between attempts via [ErrSnooze]
//   - [Branch] — evaluates a condition and picks one of several sub-pipelines to execute
//
// The [Executor] runs a pipeline given a [RunState]. The executor is stateless — all
// mutable state lives in RunState, which can be serialized to JSON and stored in a database.
// On restart, pass the saved RunState back to [Executor.Run] to resume from where it left off.
//
// Steps share data through a [DataAccessor] interface. Values are JSON-serialized
// automatically and survive restarts.
//
// # Quick Start
//
//	p := &pipeline.Pipeline{
//		Name: "my_pipeline",
//		Steps: []pipeline.Step{
//			pipeline.Action("fetch_data", func(ctx context.Context, data pipeline.DataAccessor) error {
//				data.Set("result", "hello")
//				return nil
//			}),
//			pipeline.Action("process_data", func(ctx context.Context, data pipeline.DataAccessor) error {
//				result, _ := pipeline.GetData[string](data, "result")
//				fmt.Println(result) // "hello"
//				return nil
//			}),
//		},
//	}
//
//	executor := pipeline.NewExecutor()
//	state, err := executor.Run(context.Background(), p, pipeline.RunState{})
//	// state.Status == RunStatusCompleted
//
// # Step Types
//
// Action steps execute a function once. Use [WithCompensate] to define rollback logic:
//
//	pipeline.Action("provision_server", provisionFn,
//		pipeline.WithCompensate(destroyServerFn),
//	)
//
// Poll steps check a condition and return whether it is met. When the condition is not
// met, the executor returns [ErrSnooze] with a duration hint. The caller should persist
// the state and re-invoke [Executor.Run] after that duration:
//
//	pipeline.Poll("wait_for_deploy", func(ctx context.Context, data pipeline.DataAccessor) (bool, time.Duration, error) {
//		ready, err := checkDeployStatus(ctx)
//		if err != nil {
//			return false, 0, err
//		}
//		if !ready {
//			return false, 30 * time.Second, nil // check again in 30s
//		}
//		return true, 0, nil
//	}, pipeline.WithMaxPollDuration(10*time.Minute))
//
// Branch steps pick a path based on runtime data. Each path is a sub-pipeline
// that can contain any step types, including nested branches:
//
//	pipeline.Branch("select_env", decideEnvFn, map[string][]pipeline.Step{
//		"production": {
//			pipeline.Action("deploy_prod", deployProdFn),
//		},
//		"staging": {
//			pipeline.Action("deploy_staging", deployStagingFn),
//		},
//	})
//
// # Data Passing
//
// Steps communicate through a [DataAccessor], available as the second argument
// to every step function. Values must be JSON-serializable:
//
//	// Producer step
//	data.Set("order_id", "ord-123")
//	data.Set("amount", 99.50)
//
//	// Consumer step
//	orderID, err := pipeline.GetData[string](data, "order_id")
//	amount, err := pipeline.GetData[float64](data, "amount")
//
// [GetData] handles both in-memory values and JSON-deserialized values (after a restart),
// so it works transparently whether the pipeline was resumed or running fresh.
//
// # Persistence and Resume
//
// Pipeline state is captured in [RunState], a fully JSON-serializable struct that tracks:
//   - Current position in the step tree ([RunState.CurrentPath])
//   - Completed steps for compensation ([RunState.CompletedSteps])
//   - Shared data between steps ([RunState.Data])
//   - Execution status ([RunState.Status])
//
// To enable persistence, provide a [SnapshotFunc] via [WithSnapshotFn]:
//
//	executor := pipeline.NewExecutor(
//		pipeline.WithSnapshotFn(func(ctx context.Context, state pipeline.RunState) error {
//			data, _ := json.Marshal(state)
//			return db.Save(ctx, pipelineID, data)
//		}),
//	)
//
// The snapshot function is called after each completed step, on status changes,
// and before returning [ErrSnooze]. To resume a pipeline after a restart:
//
//	var state pipeline.RunState
//	data, _ := db.Load(ctx, pipelineID)
//	json.Unmarshal(data, &state)
//
//	state, err := executor.Run(ctx, p, state)
//
// Already-completed steps are skipped automatically. The pipeline resumes from the
// exact position it was at, including inside nested branches.
//
// # Compensation (Saga Pattern)
//
// When a step fails, the executor walks backward through completed steps,
// calling each step's Compensate function (if defined) in reverse order.
// This implements the Saga pattern for distributed transactions:
//
//	Steps: []pipeline.Step{
//		pipeline.Action("create_order", createOrderFn,
//			pipeline.WithCompensate(cancelOrderFn)),
//		pipeline.Action("charge_payment", chargePaymentFn,
//			pipeline.WithCompensate(refundPaymentFn)),
//		pipeline.Action("ship_item", shipItemFn), // fails here
//	}
//
// If "ship_item" fails, the executor calls refundPaymentFn, then cancelOrderFn.
// Compensation also works inside branches — only steps that actually executed
// are compensated.
//
// If compensation itself fails, [ErrCompensationFailed] is returned with both
// the original and the compensation errors.
//
// # Skipping Compensation
//
// For transient errors where the caller should retry the pipeline instead of
// rolling back, wrap the error with [NoCompensate]:
//
//	if err := callExternalAPI(ctx); err != nil {
//		return pipeline.NoCompensate(fmt.Errorf("transient: %w", err))
//	}
//
// The pipeline will fail without running compensation. The caller can then
// re-invoke [Executor.Run] with the same state to retry from the failed step.
//
// # Error Types
//
//   - [ErrSnooze] — poll step is waiting; contains Duration hint for retry
//   - [ErrStepFailed] — step execution failed; contains StepName, Path, and underlying Err
//   - [ErrPollTimeout] — poll step exceeded its [WithMaxPollDuration] limit
//   - [ErrCompensationFailed] — compensation failed; contains Original and Compensation errors
//   - [ErrNoCompensate] — sentinel used by [NoCompensate] to skip compensation
//
// # Retry
//
// Action steps can be configured with automatic retry using [WithRetry]:
//
//	pipeline.Action("call_api", callApiFn,
//		pipeline.WithRetry(3, time.Second, true), // 3 attempts, 1s initial delay, exponential backoff
//	)
//
// With backoff enabled, delays double on each attempt (1s, 2s, 4s, ...).
// Retries respect context cancellation.
//
// # Step Options
//
//   - [WithCompensate] — attach a rollback function to an action step
//   - [WithOnEnter] — pre-execution hook (logging, webhooks, metrics)
//   - [WithRetry] — automatic retry with configurable attempts, delay, and backoff
//   - [WithMaxPollDuration] — maximum total time a poll step can run before timing out
//
// # Executor Options
//
//   - [WithLogger] — set a structured logger (implements loggerutil.Logger)
//   - [WithDebug] — enable verbose debug logging
//   - [WithSnapshotFn] — register a persistence callback for state snapshots
//
// # Execution Lifecycle
//
// A typical execution flow:
//
//  1. Create a [Pipeline] definition (immutable, reusable)
//  2. Create an [Executor] with desired options
//  3. Call [Executor.Run] with an empty [RunState] for new execution
//  4. If [ErrSnooze] is returned, persist state and schedule a retry
//  5. On retry, load state and call [Executor.Run] again
//  6. Repeat until [RunState.Status] is [RunStatusCompleted] or [RunStatusFailed]
//
// Terminal states ([RunStatusCompleted], [RunStatusFailed]) are idempotent —
// calling Run on a terminal state returns immediately with no side effects.
//
// # Status Values
//
//   - [RunStatusNew] — pipeline has not started
//   - [RunStatusRunning] — pipeline is actively executing steps
//   - [RunStatusPolling] — pipeline is waiting for a poll condition (snooze)
//   - [RunStatusCompensating] — pipeline is rolling back after a failure
//   - [RunStatusCompleted] — pipeline finished successfully
//   - [RunStatusFailed] — pipeline failed (after compensation if applicable)
package pipeline
