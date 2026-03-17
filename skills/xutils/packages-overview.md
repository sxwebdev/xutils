# Packages Overview

## Quick reference

| Package      | Import                                        | Use when                                                    |
| ------------ | --------------------------------------------- | ----------------------------------------------------------- |
| `broker`     | `github.com/sxwebdev/xutils/broker`           | Need in-process pub/sub messaging between goroutines        |
| `cacheutil`  | `github.com/sxwebdev/xutils/cacheutil`        | Need a cache interface (Redis, in-memory, etc.)             |
| `dbutil`     | `github.com/sxwebdev/xutils/dbutil`           | Need pagination, JSON/Duration columns, tx wrapper          |
| `loggerutil` | `github.com/sxwebdev/xutils/loggerutil`       | Need a logger interface or no-op/test logger                |
| `loopper`    | `github.com/sxwebdev/xutils/loopper`          | Need periodic task execution with overlap prevention        |
| `pipeline`   | `github.com/sxwebdev/xutils/pipeline`         | Need declarative resumable workflow (action/poll/branch)    |
| `randutil`   | `github.com/sxwebdev/xutils/randutil`         | Need crypto-secure random strings or numbers                |
| `retry`      | `github.com/sxwebdev/xutils/retry`            | Need retry with linear/backoff/infinite policies            |
| `strutil`    | `github.com/sxwebdev/xutils/strutil`          | Need UTF-8 cleanup, number formatting, duration formatting  |
| `syncutil`   | `github.com/sxwebdev/xutils/syncutil`         | Need thread-safe Map, Slice, or Locker                      |
| `testutil`   | `github.com/sxwebdev/xutils/testutil`         | Need pretty-print JSON in tests                             |
| `timeutil`   | `github.com/sxwebdev/xutils/timeutil`         | Need to measure function execution time                     |
| `workflow`   | `github.com/sxwebdev/xutils/workflow`          | Need stage-based workflow with retry, snapshots, lifecycle  |

## Package dependencies

```text
loggerutil ← broker, loopper, retry, pipeline, workflow
```

All other packages are independent.

## Choosing between pipeline and workflow

| Criteria                  | `pipeline`                          | `workflow`                              |
| ------------------------- | ----------------------------------- | --------------------------------------- |
| Step types                | Action, Poll, Branch                | Single step function                    |
| Structure                 | Flat list with branch nesting       | Stages → Steps hierarchy                |
| State persistence         | RunState JSON (external)            | Snapshot JSON (external)                |
| Resume mechanism          | Pass RunState to executor           | SetJSONSnapshot before Run              |
| Compensation              | Per-step Compensate function (saga) | CompensationStage (redirect on failure) |
| Retry                     | Per-step RetryConfig                | Per-step RetryPolicy (linear/backoff)   |
| Shared data               | DataAccessor (Set/Get/All)          | SetVar/GetVar (typed, mutex-protected)  |
| Versioning                | Built-in version + MinResumeVersion | Not built-in                            |
| Lifecycle hooks           | OnEnter per step                    | Before/After at workflow, stage, step   |
| Graceful shutdown         | Context cancellation                | ShutdownTimeout + context cancellation  |
| Best for                  | Declarative, stateless executors    | Complex multi-stage, imperative control |
