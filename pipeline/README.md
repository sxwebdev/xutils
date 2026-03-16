# pipeline

Declarative pipeline engine with persistence, compensation, and typed steps.

## Features

- **Persistence** — state saved via callback after each step, survives restarts
- **Idempotency** — completed steps are never re-executed on resume
- **Compensation (Saga)** — automatic rollback of completed steps on failure
- **Typed steps** — Action (one-shot), Poll (repeated check), Branch (conditional)
- **Declarative** — entire flow visible in one place

## Installation

```bash
go get github.com/sxwebdev/xutils/pipeline
```

## Quick Start

```go
p := &pipeline.Pipeline{
    Name: "deploy_service",
    Steps: []pipeline.Step{
        pipeline.Action("validate_config", validateConfig),
        pipeline.Action("provision_infra", provisionInfra,
            pipeline.WithCompensate(destroyInfra)), // rollback if later steps fail
        pipeline.Poll("wait_ready", waitInfraReady),
        pipeline.Action("deploy_app", deployApp),
        pipeline.Action("notify", sendNotification),
    },
}

executor := pipeline.NewExecutor(
    pipeline.WithLogger(logger),
    pipeline.WithSnapshotFn(func(ctx context.Context, state pipeline.RunState) error {
        return db.SaveState(ctx, jobID, state)
    }),
)

state, err := executor.Run(ctx, p, pipeline.RunState{})
```

## Step Types

### Action

Executes once. Optionally defines a compensating action for rollback.

```go
pipeline.Action("create_resource", createFn,
    pipeline.WithCompensate(deleteFn),  // called on rollback
    pipeline.WithRetry(3, time.Second, true), // 3 attempts, backoff
    pipeline.WithOnEnter(webhookFn),    // called before step
)
```

### Poll

Checks a condition repeatedly. Returns `ErrSnooze` when not done — the caller decides when to re-invoke.

```go
pipeline.Poll("wait_ready", func(ctx context.Context, data pipeline.DataAccessor) (bool, time.Duration, error) {
    ready, err := checkStatus(ctx)
    if err != nil {
        return false, 0, err
    }
    if !ready {
        return false, 10 * time.Second, nil // retry after 10s
    }
    return true, 0, nil // done
}, pipeline.WithMaxPollDuration(30 * time.Minute))
```

### Branch

Picks a sub-pipeline based on a condition.

```go
pipeline.Branch("env_check", decideEnv, map[string][]pipeline.Step{
    "production": {
        pipeline.Action("blue_green_deploy", blueGreenDeploy),
        pipeline.Poll("wait_health", waitHealthCheck),
    },
    "staging": {
        pipeline.Action("direct_deploy", directDeploy),
    },
})
```

## Persistence & Resume

The executor is **stateless** — all state lives in `RunState` which is passed in and returned.

```go
// First run (or resume from DB).
state := loadFromDB(jobID) // RunState{} for new jobs

state, err := executor.Run(ctx, p, state)
if err == nil {
    // Done. Check state.Status: "completed" or "failed".
} else if snooze, ok := errors.AsType[pipeline.ErrSnooze](err); ok {
    // Poll step waiting. Save state, re-invoke after snooze.Duration.
    saveState(jobID, state)
    scheduleRetry(jobID, snooze.Duration)
} else {
    // Engine error.
}
```

## Compensation

When a step fails, the executor walks completed steps **in reverse** and calls each `Compensate` function.

```go
pipeline.Action("allocate", allocateFn,
    pipeline.WithCompensate(deallocateFn)), // called if any later step fails
```

Only steps with `WithCompensate` are rolled back. Steps without it are skipped during compensation.

## Data Passing

Steps share data via `DataAccessor`. Data is JSON-serialized in snapshots.

```go
// Producer step
func produce(ctx context.Context, data pipeline.DataAccessor) error {
    data.Set("job_id", "abc-123")
    return nil
}

// Consumer step
func consume(ctx context.Context, data pipeline.DataAccessor) error {
    jobID, err := pipeline.GetData[string](data, "job_id")
    if err != nil {
        return err
    }
    // use jobID
    return nil
}
```

## Run States

| Status           | Meaning                     |
| ---------------- | --------------------------- |
| `""`             | New, not started            |
| `"running"`      | Executing steps             |
| `"polling"`      | Waiting for poll condition  |
| `"completed"`    | All steps finished          |
| `"compensating"` | Rolling back after failure  |
| `"failed"`       | Failed (after compensation) |

## Retry

Action steps support retry with optional exponential backoff:

```go
pipeline.WithRetry(
    5,              // max attempts
    time.Second,    // initial delay
    true,           // exponential backoff (1s, 2s, 4s, 8s, 16s)
)
```

Retries respect context cancellation.

## Versioning

Pipeline definitions support versioning to prevent resuming a state created by an incompatible pipeline version.

```go
p := &pipeline.Pipeline{
    Name:    "transfer",
    Version: 2,
    Steps:   []pipeline.Step{ /* ... */ },
}
```

When `Run` is called on a new `RunState`, the pipeline's `Version` is stamped into the state. On resume, the executor checks that the state version falls within the pipeline's allowed range. If not, `ErrVersionMismatch` is returned.

### Backward compatibility

By default, only exact version match is accepted. To allow resuming states from older versions, set `MinResumeVersion`:

```go
minV := 1
p := &pipeline.Pipeline{
    Name:             "transfer",
    Version:          2,
    MinResumeVersion: &minV, // accept states from v1 and v2
    Steps:            []pipeline.Step{ /* ... */ },
}
```

To accept legacy unversioned states (version 0):

```go
p := &pipeline.Pipeline{
    Name:             "transfer",
    Version:          1,
    MinResumeVersion: new(int), // pointer to 0
    Steps:            []pipeline.Step{ /* ... */ },
}
```

### Handling version mismatch

```go
state, err := executor.Run(ctx, p, state)
if vErr, ok := errors.AsType[*pipeline.ErrVersionMismatch](err); ok {
    // vErr.StateVersion, vErr.PipelineVersion, vErr.MinResumeVersion
    // Application decides: retry later, discard, force-compensate, migrate state.
}
```

### Rolling deployments

When deploying a new app version with changed pipeline definitions:

1. Old instances finish active pipelines with old version
2. New instances reject old-version states with `ErrVersionMismatch` — return the job to the queue
3. After all old pipelines complete, only new-version pipelines remain

The library provides the mechanism; the application handles the policy (retry, drain, migrate).

### Stuck pipelines

If old instances are gone and old-version pipelines remain in non-terminal state, there are three strategies:

**Pipeline registry** — new code carries old definitions, routes by state version:

```go
var pipelines = map[int]*pipeline.Pipeline{
    1: transferV1(), // old definition for drain/compensation
    2: transferV2(), // current
}

func handleJob(ctx context.Context, state pipeline.RunState) {
    p := pipelines[state.Version]
    if p == nil {
        state.ForceTerminate("unsupported pipeline version")
        saveState(state)
        return
    }
    state, err := executor.Run(ctx, p, state)
    // ...
}
```

**Force terminate** — mark stuck pipelines as failed without compensation:

```go
state.ForceTerminate("version drain timeout")
saveState(state)
```

**Compensation-only definitions** — keep old pipeline structure with only `Compensate` functions to safely rollback stuck pipelines before discarding them.

## Validation

The executor validates the pipeline definition on each `Run`:

- All steps must have unique names within their scope
- Each step must have exactly one type (Action, Poll, or Branch)
- Action/Poll must have non-nil functions
- Branch must have a Decide function and at least one path
