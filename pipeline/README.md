# pipeline

Declarative pipeline engine with persistence, compensation, and typed steps.

## Features

- **Fail-stop persistence** — every state transition is saved before execution continues
- **Idempotency** — completed steps are never re-executed on resume
- **Compensation (Saga)** — automatic rollback of completed steps on failure
- **Typed steps** — Action, Poll, Branch, and resumable Repeat
- **Scheduler-neutral waits** — callbacks can return `RetryAfter` without blocking a goroutine
- **Bounded Repeat history** — optional compact history for long-running loops
- **Version migrations** — optional fail-stop transformation of persisted state and data
- **Clock injection** — deterministic timeout and retry behavior without external dependencies
- **Observer events** — vendor-neutral lifecycle events for metrics and tracing adapters
- **Diagnostics** — aggregate attempts, timestamps, errors, and next-run hints per stable step path
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

### Repeat

Runs a nested sub-pipeline per iteration. The condition runs after the nested
steps and returns the same `(done, retryAfter, error)` shape as `Poll`:

```go
pipeline.Repeat("pages", []pipeline.Step{
		pipeline.Action("fetch", fetchPage),
		pipeline.Branch("classify", classifyPage, pagePaths),
}, func(ctx context.Context, data pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
		done, err := noMorePages(ctx, iteration)
		return done, 5 * time.Second, err
},
		pipeline.WithMaxIterations(1_000),
		pipeline.WithMaxRepeatDuration(24*time.Hour),
		pipeline.WithRepeatHistory(pipeline.RepeatHistoryCompact),
)
```

When `done` is false, the executor snapshots the next iteration and returns
`*ErrRetryAfter`. It yields even when `retryAfter == 0`, so an unlimited repeat
cannot spin synchronously. `CurrentPath` includes the iteration number and
nested branches/repeats resume at the exact child step.

Repeat history has two explicit modes:

- `RepeatHistoryFull` is the default and preserves the existing behavior:
  completed children and per-iteration diagnostics are retained, and
  compensation walks actions from every iteration in reverse completion order.
- `RepeatHistoryCompact` removes completed child journals after a successful
  `Until` call and merges diagnostics into paths without the outer iteration
  number. The current incomplete iteration remains exact and fully resumable.
  Attempts are summed; first/last starts, latest completion, last error, and the
  current next-run hint are retained. A compacting snapshot is fail-stop, so the
  next iteration cannot begin until compact state is durable.

Compact Repeat recursively rejects any nested `Action` with `WithCompensate`,
including actions inside Branch or nested Repeat. When rollback is required,
place one aggregating compensatable Action before the Repeat and store aggregate
rollback data in `Data`. Changing an existing Repeat between Full and Compact
changes the persisted-state contract and therefore requires a new pipeline
version (and, for active runs, an explicit state migration).

`WithMaxRepeatDuration` starts its durable timer when the Repeat is first
entered. The executor checks the limit before starting or resuming each
iteration and again after its children, immediately before `Until`. Equality is
timed out (`elapsed >= max`). Scheduler delays, `RetryAfter`, callback failures,
and restarts do not reset the timer. Successful Repeat completion clears
`RepeatState.StartedAt`; timeout follows normal compensation and is returned as
`*ErrRepeatTimeout`.

## Persistence & Resume

The executor is **stateless** — all state lives in `RunState` which is passed in and returned.

```go
// First run (or resume from DB).
state := loadFromDB(jobID) // RunState{} for new jobs

state, err := executor.Run(ctx, p, state)
if err == nil {
    // Done. Check state.Status: "completed" or "failed".
} else if retry, ok := errors.AsType[*pipeline.ErrRetryAfter](err); ok {
    // State is already snapshotted. Ask any scheduler to resume it later.
    scheduleRetry(jobID, retry.Duration)
} else if snooze, ok := errors.AsType[pipeline.ErrSnooze](err); ok {
    // Legacy Poll signal; still supported.
    scheduleRetry(jobID, snooze.Duration)
} else if snapshotErr, ok := errors.AsType[*pipeline.ErrSnapshotFailed](err); ok {
    // Stop. Reload the last durable state before retrying.
    log.Printf("snapshot failed during %s: %v", snapshotErr.Operation, snapshotErr)
} else if repeatTimeout, ok := errors.AsType[*pipeline.ErrRepeatTimeout](err); ok {
    // Ordinary compensation has completed; state is failed.
    log.Printf("repeat %s exceeded %s", repeatTimeout.StepName, repeatTimeout.MaxDuration)
} else {
    // Engine error.
}
```

### Snapshot failure contract

Snapshot errors are never logged-and-ignored. `Run` immediately returns the
updated state and `*ErrSnapshotFailed`, which unwraps the storage error. No next
step or compensator is invoked. The returned state can contain progress that was
not persisted; reload the durable state before retrying.

Every non-terminal `Run` checkpoints before invoking a resumed callback. This
lets a CAS callback reject stale state before an Action or Compensate function
runs; status changes and callback results are checkpointed again afterward.

An external effect can succeed immediately before its completion snapshot
fails. Consequently every `Action`, `Compensate`, and side-effecting hook must
be idempotent. Snapshot storage should be atomic and the callback itself should
be safe to retry.

## Compensation

When a step fails, the executor walks completed steps **in reverse** and calls each `Compensate` function.

```go
pipeline.Action("allocate", allocateFn,
    pipeline.WithCompensate(deallocateFn)), // called if any later step fails
```

Only steps with `WithCompensate` are rolled back. Steps without it are skipped during compensation.

If compensation itself fails, `Run` returns `*ErrCompensationFailed` (wrapping both the original and the rollback error) and the run stays `compensating` so it can be resumed.

## Skip compensation (retryable errors)

For transient errors where the caller should retry the pipeline instead of rolling back, wrap the error with `NoCompensate`:

```go
func callAPI(ctx context.Context, data pipeline.DataAccessor) error {
    if err := externalAPI(ctx); err != nil {
        return pipeline.NoCompensate(fmt.Errorf("transient: %w", err))
    }
    return nil
}
```

Compensation is skipped and the error (matchable with `errors.Is(err, pipeline.ErrNoCompensate)`) is returned to the caller. The run is left non-terminal — status stays `running` and `state.FailedStepPath` records the failed step — so re-invoking `Run` with the same state retries from that step.

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
    true,           // exponential backoff
)
```

With backoff enabled, the delay doubles between attempts. There is no delay
after the final attempt, so 5 attempts wait `1s, 2s, 4s, 8s` (four gaps).
Retries respect context cancellation — a cancel during the wait aborts
immediately and returns `context.Canceled` without rolling back.

For a delay that must not hold a goroutine, any callback can return:

```go
return pipeline.RetryAfter(30*time.Second, err)
```

The executor does not compensate, snapshots the incomplete step, and returns a
`*ErrRetryAfter` discoverable with `errors.AsType`; its cause remains discoverable
with `errors.Is`/`errors.AsType`. This is distinct from `WithRetry`, whose bounded
short retries happen within the current invocation, and from a permanent error,
which starts compensation. `ErrSnooze` remains supported for existing Poll code.

Dynamic negative delays from `RetryAfter`, Poll `done=false`, and Repeat
`done=false` are normalized to zero before either the typed scheduling error or
`StepDiagnostics.NextRunAt` leaves the executor. Zero still returns control to
the external scheduler; Repeat never starts another iteration synchronously.
Negative static retry, Poll timeout, and Repeat timeout durations fail pipeline
validation. `WithRetry(..., 0, ...)` retains its historical one-second default.

## Diagnostics

`RunState.StepDiagnostics` is keyed by `StepPathKey(fullPath)` and stores only
aggregate metadata: attempt count, first/last start, completion, last error, and
next scheduled run. Repeat iteration numbers are part of child paths in Full
mode. Compact mode aggregates completed children without the outer iteration
number. The metadata is observational and never drives execution decisions.

## Clock

`WithClock` replaces all executor reads and internal waits with a minimal clock:

```go
type Clock interface {
    Now() time.Time
    Sleep(ctx context.Context, duration time.Duration) error
}
```

It controls diagnostic timestamps, `NextRunAt`, Poll and Repeat duration
limits, and cancellable `WithRetry` waits. The default uses standard `time`. A
fake clock can advance deterministically in tests; `Sleep` implementations must
return `ctx.Err()` on cancellation.

## Concurrent execution

The executor is stateless and does not provide a process lock or lease. The
caller must ensure a single active owner for each run. `WithCASSnapshotFn` is an
optional optimistic-concurrency hook: it receives the expected durable revision
and a state whose `Revision` is one higher. A CAS conflict becomes
`*ErrSnapshotFailed` and stops execution.

CAS protects state ordering, but cannot undo an external effect performed before
a completion CAS loses a race. Use a storage-backed lease/single-consumer policy
as the primary guard and idempotency keys for external actions. The original
`WithSnapshotFn` remains available and also advances `RunState.Revision` after
successful snapshots.

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

Without a migrator, the existing exact/range compatibility behavior remains
unchanged. `WithStateMigrator` enables one atomic `from → current` conversion:

```go
executor := pipeline.NewExecutor(pipeline.WithStateMigrator(
    func(ctx context.Context, name string, from, to int, state pipeline.RunState) (pipeline.RunState, error) {
        state.CurrentPath = renamePath(state.CurrentPath, "charge", "capture_payment")
        state.Data["payment"] = migratePaymentJSON(state.Data["payment"])
        return state, nil
    },
))
```

Migration runs before ordinary compatibility checks and before every pipeline
callback. The executor stamps the target version and snapshots the result
fail-stop before continuing. Errors are returned as
`*ErrStateMigrationFailed`; snapshot errors remain `*ErrSnapshotFailed`.
Downgrades are rejected, while input terminal states are returned unchanged and
never migrated or resumed. The migrator may update `Data`, paths, completed
steps, Repeat state, diagnostics, and other compatible fields. Multi-hop schema
changes are the application's responsibility inside this single atomic call.

The library provides the mechanism; the application still chooses whether to
migrate, drain, or retain old definitions.

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

## Observer

`WithObserver` registers a vendor-neutral synchronous `Observer`. Events cover
pipeline and step lifecycle, delayed continuation, Repeat iterations,
compensation, snapshots, migrations, version mismatches, and cancellation. Each
event contains pipeline identity/version, current status, a cloned full step
path, attempt/iteration where applicable, duration, operation, and cause. No
mutable `RunState` pointer is exposed.

Observer callbacks never affect control flow: panics are recovered and reported
through the existing logger. Callbacks run synchronously and in event order;
observers doing network or disk I/O should enqueue work to their own bounded
asynchronous worker. When no observer is configured, no background machinery is
created.

## Validation

The executor validates the pipeline definition on each `Run`:

- All steps must have unique names within their scope
- Each step must have exactly one type (Action, Poll, Branch, or Repeat)
- Action/Poll must have non-nil functions
- Branch must have a Decide function and at least one path
- Repeat must have an Until function and a non-negative iteration limit
- Static retry/Poll/Repeat durations must be non-negative
- Compact Repeat must not contain a compensator at any recursive depth
