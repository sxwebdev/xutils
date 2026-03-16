# workflow

Stage-based workflow engine with persistence, retry policies, compensation, and shared variables.

## Why

When you need to orchestrate a multi-step process that:

- **Survives restarts** — state is snapshotted after every step and can be restored from DB/file
- **Resumes from where it stopped** — completed and skipped steps are never re-executed
- **Supports compensation** — on failure the workflow can automatically jump to a rollback stage
- **Passes data between steps** — typed shared variables (`SetVar` / `GetVar`) and per-step args
- **Retries with policies** — linear or exponential backoff, configurable per step
- **Provides lifecycle hooks** — before/after callbacks at workflow, stage, and step levels

Use `workflow` when your process is long-running, stateful, and needs reliable step-by-step execution with observability. Typical use cases: deployment pipelines, order processing, data migrations, multi-service orchestration.

## Installation

```bash
go get github.com/sxwebdev/xutils/workflow
```

## Quick Start

```go
// 1. Define stages and steps (NewStage/NewStep return typed refs for compile-time safety)
provisionStage, provisionRef := workflow.NewStage("provision")
cleanupStage, cleanupRef := workflow.NewStage("cleanup")

validateStep, _ := workflow.NewStep("validate", validateFn)
createStep, _ := workflow.NewStep("create_resource", createFn,
    workflow.WithStepMaxRetries(3),
    workflow.WithStepTimeout(5*time.Second),
    workflow.WithStepRetryPolicy(workflow.RunWithBackoff),
)
notifyStep, _ := workflow.NewStep("notify", notifyFn)

rollbackStep, rollbackStepRef := workflow.NewStep("rollback", rollbackFn)

provisionStage.Steps = []*workflow.Step{validateStep, createStep, notifyStep}
cleanupStage.Steps = []*workflow.Step{rollbackStep}

// 2. Build the workflow
wf := workflow.New(
    workflow.WithName("deploy"),
    workflow.WithLogger(logger),
    workflow.WithSnapshotFn(func(ctx context.Context, w *workflow.Workflow, snap workflow.Snapshot) error {
        return db.SaveSnapshot(ctx, jobID, snap)
    }),
    workflow.WithCompensationStage(cleanupRef, rollbackStepRef),
)
wf.Stages = []*workflow.Stage{provisionStage, cleanupStage}

// 3. Run (or resume from a saved snapshot)
if saved, ok := db.LoadSnapshot(jobID); ok {
    wf.SetJSONSnapshot(saved)
}
err := wf.Run(ctx)
```

## Core Concepts

### Workflow → Stages → Steps

A **Workflow** contains an ordered list of **Stages**. Each Stage contains an ordered list of **Steps**. Execution is sequential: stages run top-to-bottom, steps within each stage run top-to-bottom.

```text
Workflow
├── Stage "provision"
│   ├── Step "validate"
│   ├── Step "create_resource"
│   └── Step "notify"
└── Stage "cleanup"
    └── Step "rollback"
```

### Typed References

`NewStage` and `NewStep` return typed references (`StageRef`, `StepRef`) alongside the struct. Use these for compile-time safe navigation:

```go
stage, stageRef := workflow.NewStage("my_stage")
step, stepRef := workflow.NewStep("my_step", myFn)

// Navigate to a specific stage/step on resume
wf.State.GoToStage(stageRef)
wf.State.GoToStep(stepRef)
```

## Step Function

Every step receives a `*StepContext` that provides access to the workflow, current stage, step, and the context:

```go
func myStep(sc *workflow.StepContext) error {
    // Access the parent workflow
    wf := sc.Workflow

    // Access the current stage/step
    stage := sc.Stage
    step := sc.Step

    // Use as context.Context (StepContext embeds it)
    resp, err := http.Get(sc, "https://example.com")

    // Store result in step args (persisted in snapshots)
    step.State.SetArg("result_id", "abc-123")

    return nil
}
```

## Retry Policies

Each step has a configurable retry policy. Two built-in policies are provided:

### Linear (default)

Retries with a fixed delay between attempts:

```go
workflow.NewStep("fetch_data", fetchFn,
    workflow.WithStepMaxRetries(5),       // 5 attempts
    workflow.WithStepTimeout(2*time.Second), // 2s between retries
    // RunWithLinear is the default, no need to set explicitly
)
```

### Exponential Backoff

Retries with doubling delay (1s, 2s, 4s, 8s, ...):

```go
workflow.NewStep("call_api", callApiFn,
    workflow.WithStepMaxRetries(5),
    workflow.WithStepRetryPolicy(workflow.RunWithBackoff),
)
```

Set `MaxRetries` to `-1` for infinite retries (respects context cancellation).

## Shared Variables

Steps can pass typed data to each other via the workflow's shared variable store:

```go
// Producer step
func produce(sc *workflow.StepContext) error {
    workflow.SetVar(sc.Workflow, "order_id", "ord-456")
    return nil
}

// Consumer step
func consume(sc *workflow.StepContext) error {
    orderID, ok := workflow.GetVar[string](sc.Workflow, "order_id")
    if !ok {
        return fmt.Errorf("order_id not found")
    }
    // use orderID
    return nil
}
```

Variables are included in snapshots and restored on resume.

## Persistence & Resume

The workflow is **stateful** — all state lives inside the `Workflow` struct and is captured via `Snapshot`.

```go
// Configure snapshot persistence
wf := workflow.New(
    workflow.WithSnapshotFn(func(ctx context.Context, w *workflow.Workflow, snap workflow.Snapshot) error {
        data, _ := json.Marshal(snap)
        return db.Save(ctx, jobID, data)
    }),
)

// Resume from a saved snapshot
wf.SetJSONSnapshot(savedJSON)
err := wf.Run(ctx)
```

Snapshots are taken automatically:

- **Before** each step execution
- **After** each step completion (success or failure)
- **On workflow failure** (before compensation)

### Snapshot Structure

```go
type Snapshot struct {
    StepsStates   []*StepState   // state of each step
    WorkflowState WorkflowState  // workflow-level flags and navigation
    Vars          map[string]any // shared variables
}
```

You can also read typed arguments from a snapshot:

```go
val, err := workflow.GetArg[string](snapshot, "my_step", "result_id")
// or using a typed reference:
val, err := workflow.GetArgByRef[string](snapshot, stepRef, "result_id")
```

## Compensation

When a step fails, the workflow can automatically redirect to a compensation stage:

```go
cleanupStage, cleanupRef := workflow.NewStage("cleanup")
rollbackStep, rollbackRef := workflow.NewStep("rollback", rollbackFn)
cleanupStage.Steps = []*workflow.Step{rollbackStep}

wf := workflow.New(
    workflow.WithCompensationStage(cleanupRef, rollbackRef),
)
```

On failure the workflow sets `NextStage`/`NextStep` to the compensation targets, saves a snapshot, and suppresses the error. The next `Run()` call continues from the compensation stage.

## Lifecycle Hooks

Hooks are available at every level:

### Workflow Level

```go
workflow.New(
    workflow.WithBeforeFn(func(ctx context.Context, w *workflow.Workflow) error {
        // called before the first stage
        return nil
    }),
    workflow.WithAfterFn(func(ctx context.Context, w *workflow.Workflow) error {
        // called after all stages complete
        return nil
    }),
    workflow.WithOnFailureFn(func(ctx context.Context, w *workflow.Workflow, err error) error {
        // called when a step fails
        return nil
    }),
    workflow.WithBeforeAllStepsFn(func(ctx context.Context, w *workflow.Workflow, s *workflow.Stage, st *workflow.Step) error {
        // called before every step in the workflow
        return nil
    }),
    workflow.WithAfterAllStepsFn(func(ctx context.Context, w *workflow.Workflow, s *workflow.Stage, st *workflow.Step) error {
        // called after every step in the workflow
        return nil
    }),
)
```

### Stage Level

```go
workflow.NewStage("my_stage",
    workflow.WithStageBeforeFn(func(ctx context.Context, s *workflow.Stage) error {
        return nil
    }),
    workflow.WithStageAfterFn(func(ctx context.Context, s *workflow.Stage) error {
        return nil
    }),
)
```

### Step Level

```go
workflow.NewStep("my_step", myFn,
    workflow.WithStepBeforeFn(func(ctx context.Context, s *workflow.Step) error {
        return nil
    }),
    workflow.WithStepAfterFn(func(ctx context.Context, s *workflow.Step) error {
        return nil
    }),
)
```

## Control Flow

Steps can return sentinel errors to control execution:

| Error              | Effect                                        |
| ------------------ | --------------------------------------------- |
| `ErrSkipStep`      | Skip the current step, continue to the next   |
| `ErrSkipStage`     | Skip the remaining steps in the current stage |
| `ErrBreakStages`   | Stop executing stages (finish the workflow)   |
| `ErrExitWorkflow`  | Exit the workflow immediately                 |
| `SilentError(err)` | Suppress error logging during retries         |

```go
func conditionalStep(sc *workflow.StepContext) error {
    if !needsProcessing() {
        return workflow.ErrSkipStep
    }
    return process(sc)
}
```

## Step Statuses

| Status         | Meaning                     |
| -------------- | --------------------------- |
| `"pending"`    | Step is about to execute    |
| `"processing"` | Step is currently executing |
| `"completed"`  | Step finished successfully  |
| `"failed"`     | Step failed                 |
| `"suspended"`  | Step is suspended (paused)  |
| `"skipped"`    | Step was skipped            |

## Workflow States

| Field         | Meaning                                       |
| ------------- | --------------------------------------------- |
| `IsSuspended` | Workflow is paused, will skip on next `Run()` |
| `IsCompleted` | Workflow finished all stages                  |
| `IsFailed`    | Workflow failed                               |
| `NextStage`   | Jump to this stage on next `Run()`            |
| `NextStep`    | Jump to this step on next `Run()`             |

## Graceful Shutdown

When the context is cancelled, the workflow waits for the current step to finish (default 10s timeout):

```go
wf := workflow.New(
    workflow.WithShutdownTimeout(30 * time.Second),
)
```

## Validation

The workflow validates on each `Run()`:

- All stages and steps must be non-nil
- Stage and step names must be unique across the entire workflow
- Every step must have a non-nil `Func` and `RetryPolicy`
- Compensation stage/step references must point to existing stages/steps
