# Orchestration Guide

## pipeline — Declarative resumable workflow engine

Stateless executor + persistent state. Four step types: Action, Poll, Branch,
Repeat. Built-in saga compensation, diagnostics, fail-stop snapshots, and
versioning.

### Defining a pipeline

```go
import "github.com/sxwebdev/xutils/pipeline"

p := &pipeline.Pipeline{
    Name:    "deploy_app",
    Version: 1,
    Steps: []pipeline.Step{
        pipeline.Action("validate", func(ctx context.Context, data pipeline.DataAccessor) error {
            data.Set("app_version", "v2.4.1")
            return nil
        }),

        pipeline.Action("provision_infra", provisionFn,
            pipeline.WithCompensate(destroyInfraFn), // saga rollback
            pipeline.WithRetry(3, time.Second, true), // 3 attempts, 1s delay, backoff
        ),

        pipeline.Poll("wait_ready", func(ctx context.Context, data pipeline.DataAccessor) (bool, time.Duration, error) {
            ready, err := checkStatus(ctx)
            if err != nil {
                return false, 0, err
            }
            if ready {
                return true, 0, nil // done
            }
            return false, 5 * time.Second, nil // retry in 5s
        }, pipeline.WithMaxPollDuration(10*time.Minute)),

        pipeline.Branch("select_env", decideEnvFn, map[string][]pipeline.Step{
            "production": {
                pipeline.Action("deploy_prod", deployProdFn),
            },
            "staging": {
                pipeline.Action("deploy_staging", deployStagingFn),
            },
        }),

        pipeline.Repeat("batches", []pipeline.Step{
            pipeline.Action("process_batch", processBatch),
        }, func(ctx context.Context, data pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
            done, err := allBatchesProcessed(ctx, iteration)
            return done, 5 * time.Second, err
        },
            pipeline.WithMaxIterations(1_000),
            pipeline.WithMaxRepeatDuration(24*time.Hour),
            pipeline.WithRepeatHistory(pipeline.RepeatHistoryCompact),
        ),

        pipeline.Action("notify", notifyFn),
    },
}
```

### Running a pipeline

```go
executor := pipeline.NewExecutor(
    pipeline.WithLogger(logger),
    pipeline.WithDebug(true),
    pipeline.WithSnapshotFn(func(ctx context.Context, state pipeline.RunState) error {
        // persist state to DB
        return db.SavePipelineState(ctx, state)
    }),
)

// First run
state, err := executor.Run(ctx, p, pipeline.RunState{})
if retryAfter, ok := errors.AsType[*pipeline.ErrRetryAfter](err); ok {
    // State was snapshotted; ask the external scheduler to resume it later.
    log.Printf("retry after: %v", retryAfter.Duration)
} else if snooze, ok := errors.AsType[pipeline.ErrSnooze](err); ok {
    // Legacy Poll scheduling signal remains supported.
    log.Printf("snoozing for: %v", snooze.Duration)
} else if snapshotErr, ok := errors.AsType[*pipeline.ErrSnapshotFailed](err); ok {
    // Stop. Reload the last durable state before retrying.
    log.Printf("snapshot failed during %s: %v", snapshotErr.Operation, snapshotErr)
}

// Resume from saved state
savedState, _ := db.LoadPipelineState(ctx)
state, err = executor.Run(ctx, p, savedState)
```

### Passing data between steps

```go
// In step A:
data.Set("order_id", "ord-123")

// In step B:
orderID, err := pipeline.GetData[string](data, "order_id")
```

### Repeat and exact resume

`Repeat` executes its nested steps once per zero-based iteration and calls its
condition afterward. If the condition returns `done=false`, the executor
snapshots and yields with `*ErrRetryAfter`. It yields even for duration zero, so
an unlimited repeat cannot spin synchronously. `CurrentPath` includes repeat
iteration segments; completed children are not re-run after restart. Nested
branches and repeats are supported.

Use `WithMaxIterations` when a business limit exists. The default zero is
unlimited but still scheduler-yielding.

`RepeatHistoryFull` is the default and retains per-iteration completion and
compensation journals. `RepeatHistoryCompact` removes each successful
iteration's child completions and aggregates diagnostics without the outer
iteration number; the current incomplete iteration remains exact. Compact mode
recursively rejects compensators inside nested Branch/Repeat. Put an aggregating
compensatable Action before the Repeat when rollback is needed, and bump the
pipeline version when switching an existing definition between history modes.

`WithMaxRepeatDuration` persists its start timestamp across delays and restarts.
The limit is checked before starting/resuming an iteration and before `Until`;
timeout follows normal compensation and remains discoverable as
`*ErrRepeatTimeout`.

### Scheduler-neutral delayed continuation

Any callback, including Action, OnEnter, Poll, Branch decision, Repeat
condition, or Compensate, may return:

```go
return pipeline.RetryAfter(30*time.Second, cause)
```

This does not start compensation and does not sleep in the executor. Match the
signal with `errors.AsType` and the cause with `errors.Is`/`errors.AsType`. Keep
`WithRetry` for bounded retries inside one invocation; use `RetryAfter` when the
external scheduler should own the delay. `ErrSnooze` remains compatible for
existing Poll callbacks, and `NoCompensate` remains compatible for caller-owned
retry policies.

Negative dynamic delays are normalized to zero consistently in returned typed
errors and `StepDiagnostics.NextRunAt`. Zero still yields to the external
scheduler. Negative static retry/Poll/Repeat durations fail validation;
`WithRetry(..., 0, ...)` keeps the historical one-second default.

### Snapshot contract

Every non-terminal `Run` checkpoints before invoking a resumed callback. Every
status transition, completed step, delayed continuation, terminal step error,
and compensation stage is also snapshotted before execution proceeds. A
snapshot callback error returns `*ErrSnapshotFailed` and stops immediately; do
not run the next step or compensator. Reload the last durable state rather than
reusing the possibly uncommitted returned state.

Actions, compensators, and side-effecting hooks must remain idempotent because
an external effect may finish immediately before its completion snapshot fails.

### Diagnostics and concurrency

`RunState.StepDiagnostics` contains aggregate attempt/timestamp/error/next-run
metadata keyed by `pipeline.StepPathKey(fullPath)`. It is diagnostic only and
must not drive workflow decisions. `RepeatStates` and `CurrentPath` drive repeat
resume; new fields are optional and old JSON remains readable.

`RunState.Revision` advances after successful configured snapshot callbacks.
`WithCASSnapshotFn` enables an atomic expected-revision check, while
`WithSnapshotFn` remains compatible. CAS detects stale state but is not a lease:
applications still need single-owner execution (for example a storage lease or
single-consumer queue) and idempotency keys for external effects.

`WithClock` controls diagnostic timestamps, next-run hints, Poll/Repeat timeout
checks, and cancellable `WithRetry` sleeps. Use a fake implementation of
`Clock.Now` and `Clock.Sleep` instead of real sleeps in tests.

`WithObserver` delivers ordered synchronous lifecycle `Event` values without a
mutable RunState pointer. Observer panics are recovered and logged; observers
must enqueue their own slow I/O. Events are vendor-neutral and cover steps,
Repeat iterations, delays, compensation, snapshots, migrations, version
mismatches, and cancellation.

### Error handling

```go
// Stop compensation (saga rollback) for this error:
return pipeline.NoCompensate(err)

// Error types (match with errors.AsType, not errors.Is):
// pipeline.ErrRetryAfter        — scheduler-neutral delayed continuation
// pipeline.ErrSnooze             — poll step is waiting (carries Duration)
// pipeline.ErrSnapshotFailed     — persistence failed; execution stopped
// pipeline.ErrStepFailed         — step execution failed
// pipeline.ErrCompensationFailed — compensation action failed
// pipeline.ErrPollTimeout        — poll exceeded MaxDuration
// pipeline.ErrRepeatLimit        — repeat exceeded WithMaxIterations
// pipeline.ErrRepeatTimeout      — repeat exceeded WithMaxRepeatDuration
// pipeline.ErrCompactRepeatCompensation — compact repeat contains a compensator
// pipeline.ErrStateMigrationFailed — state migration callback failed
// pipeline.ErrVersionMismatch    — state version incompatible with pipeline
//
// Sentinel (match with errors.Is):
// pipeline.ErrNoCompensate       — wrapped by NoCompensate to skip rollback
```

### Versioning

```go
minV := 1
p := &pipeline.Pipeline{
    Name:             "deploy",
    Version:          2,
    MinResumeVersion: &minV, // can resume states from version 1+
    Steps:            steps,
}
```

Use `WithStateMigrator` for one atomic `from → current` transformation before
ordinary compatibility checks and callbacks. The executor stamps and snapshots
the migrated state fail-stop before continuing. The migrator may update Data,
paths, completions, Repeat state, and diagnostics. Downgrades are rejected;
terminal input states are returned unchanged. Without a migrator, existing
`MinResumeVersion`/`ErrVersionMismatch` behavior is unchanged.

---

## workflow — Stage-based workflow engine

Imperative, multi-stage orchestration with per-step retry, shared variables, snapshots, and lifecycle hooks.

### Defining a workflow

```go
import "github.com/sxwebdev/xutils/workflow"

// Create stages and steps (returns typed refs for safe navigation)
stage1, stage1Ref := workflow.NewStage("initialization",
    workflow.WithStageBeforeFn(func(ctx context.Context, s *workflow.Stage) error {
        fmt.Println("starting:", s.Name)
        return nil
    }),
)

step1, _ := workflow.NewStep("load_config", func(sc *workflow.StepContext) error {
    cfg, err := loadConfig(sc.Context)
    if err != nil {
        return err
    }
    workflow.SetVar(sc.Workflow, "config", cfg)
    return nil
}, workflow.WithStepMaxRetries(3))

step2, _ := workflow.NewStep("connect_db", func(sc *workflow.StepContext) error {
    cfg, _ := workflow.GetVar[Config](sc.Workflow, "config")
    return connectDB(sc.Context, cfg.DSN)
}, workflow.WithStepTimeout(10*time.Second),
   workflow.WithStepRetryPolicy(workflow.RunWithBackoff), // initial delay = step Timeout
)

stage1.Steps = []*workflow.Step{step1, step2}

// Compensation stage (runs on failure)
compensationStage, compStageRef := workflow.NewStage("rollback")
compStep, compStepRef := workflow.NewStep("cleanup", func(sc *workflow.StepContext) error {
    return cleanup(sc.Context)
})
compensationStage.Steps = []*workflow.Step{compStep}

wf := workflow.New(
    workflow.WithName("setup_service"),
    workflow.WithLogger(logger),
    workflow.WithShutdownTimeout(30*time.Second),
    workflow.WithCompensationStage(compStageRef, compStepRef),
    workflow.WithSnapshotFn(func(ctx context.Context, w *workflow.Workflow, snap workflow.Snapshot) error {
        return db.SaveSnapshot(ctx, snap)
    }),
)

wf.Stages = []*workflow.Stage{stage1, compensationStage}
```

### Running a workflow

```go
// Fresh run
err := wf.Run(ctx)

// Resume from saved state (snap is a workflow.Snapshot)
snap, _ := db.LoadSnapshot(ctx)
wf.SetSnapshot(snap) // or wf.SetJSONSnapshot(jsonString) from a stored JSON string
err = wf.Run(ctx)
```

### Shared variables between steps

```go
// Set (thread-safe, JSON-serializable)
workflow.SetVar(sc.Workflow, "user_id", 42)

// Get (typed)
userID, ok := workflow.GetVar[int](sc.Workflow, "user_id")
```

### Step arguments (persisted in snapshot)

```go
// Set in step function
sc.Step.State.SetArg("result", resultValue)

// Retrieve from snapshot
result, err := workflow.GetArg[ResultType](snapshot, "step_name", "result")
// or by ref:
result, err := workflow.GetArgByRef[ResultType](snapshot, stepRef, "result")
```

### Control flow

```go
// In a step function:
return workflow.ErrSkipStep      // skip this step
return workflow.ErrSkipStage     // skip remaining steps in current stage
return workflow.ErrBreakStages   // stop executing stages (but run AfterFn)
return workflow.ErrExitWorkflow  // exit immediately

// Suppress error logging during retries:
return workflow.SilentError(err)
```

### Step status lifecycle

```text
pending → processing → completed
                     → failed
                     → skipped
                     → suspended
```

### Retry policies

```go
// RunWithLinear / RunWithBackoff are retry policy functions passed directly
// (not constructors). The delay comes from the step's Timeout.

// Linear: fixed delay (= step Timeout) between retries (default)
workflow.WithStepRetryPolicy(workflow.RunWithLinear)

// Backoff: delay starts at the step Timeout and doubles each retry
workflow.WithStepRetryPolicy(workflow.RunWithBackoff)
```
