# Orchestration Guide

## pipeline — Declarative resumable workflow engine

Stateless executor + persistent state. Three step types: Action, Poll, Branch. Built-in saga compensation and versioning.

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
if snooze, ok := errors.AsType[pipeline.ErrSnooze](err); ok {
    // Poll step is waiting — save state and retry after snooze.Duration
    log.Printf("snoozing for: %v", snooze.Duration)
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

### Error handling

```go
// Stop compensation (saga rollback) for this error:
return pipeline.NoCompensate(err)

// Error types (match with errors.As / errors.AsType, not errors.Is):
// pipeline.ErrSnooze             — poll step is waiting (carries Duration)
// pipeline.ErrStepFailed         — step execution failed
// pipeline.ErrCompensationFailed — compensation action failed
// pipeline.ErrPollTimeout        — poll exceeded MaxDuration
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
