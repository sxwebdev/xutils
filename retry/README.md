# retry

Flexible retry mechanism with linear, exponential backoff, and infinite retry policies.

## Features

- **Three policies** — linear, exponential backoff, infinite
- **Early exit** — return `ErrExit` to stop retries on non-retryable errors
- **Error inspection** — failures are wrapped in `*retry.Error` carrying policy, attempts and the original cause
- **Callbacks** — `OnFailedFn` and `OnSuccessFn` for observability
- **Context support** — cancellation interrupts the wait between attempts for any policy (required for infinite)
- **Chainable API** — functional options and setter methods

## Installation

```bash
go get github.com/sxwebdev/xutils/retry
```

## Quick Start

```go
r := retry.New(
    retry.WithMaxAttempts(3),
    retry.WithPolicy(retry.PolicyBackoff),
    retry.WithDelay(time.Second),
)

err := r.Do(func() error {
    return callExternalAPI()
})
```

## Policies

### Linear

Constant delay between attempts:

```go
retry.New(
    retry.WithPolicy(retry.PolicyLinear),
    retry.WithDelay(2*time.Second),
    retry.WithMaxAttempts(5),
)
// delays: 2s, 2s, 2s, 2s, 2s
```

### Backoff (default)

Exponential backoff — delay doubles each attempt (`delay * 2^(attempt-1)`):

```go
retry.New(
    retry.WithPolicy(retry.PolicyBackoff),
    retry.WithDelay(time.Second),
    retry.WithMaxAttempts(5),
)
// delays: 1s, 2s, 4s, 8s, 16s
```

Use `WithMaxDelay` to cap the exponential growth:

```go
retry.New(
    retry.WithPolicy(retry.PolicyBackoff),
    retry.WithDelay(time.Second),
    retry.WithMaxDelay(10*time.Second),
    retry.WithMaxAttempts(5),
)
// delays: 1s, 2s, 4s, 8s, 10s (capped)
```

### Infinite

Retries forever until context cancellation or `ErrExit`:

```go
retry.New(
    retry.WithPolicy(retry.PolicyInfinite),
    retry.WithDelay(5*time.Second),
    retry.WithContext(ctx),
)
```

## Early Exit

Return `ErrExit` to stop retries immediately on non-retryable errors:

```go
err := r.Do(func() error {
    resp, err := client.Do(req)
    if err != nil {
        return err // will retry
    }
    if resp.StatusCode == http.StatusForbidden {
        return retry.ErrExit // stop immediately
    }
    return nil
})
```

## Error Handling

When all attempts are exhausted (linear and backoff policies), `Do` returns an
`*retry.Error` that wraps the last error from the function. The original error
stays reachable through the standard helpers:

```go
err := r.Do(doWork)

// match the original cause
if errors.Is(err, sql.ErrNoRows) {
    // ...
}

// read retry metadata (policy, attempts) and the cause
if rerr, ok := errors.AsType[*retry.Error](err); ok {
    log.Printf("policy=%s attempts=%d cause=%v", rerr.Policy, rerr.Attempts, rerr.Err)
}
```

`retry.Error` fields:

| Field      | Description                                 |
| ---------- | ------------------------------------------- |
| `Policy`   | Policy used for the run                     |
| `Attempts` | Number of attempts performed                |
| `Err`      | Last error returned by the retried function |

Notes:

- The infinite policy never returns `*retry.Error` — it returns `nil`, the context error on cancellation, or the `ErrExit`-wrapping error.
- An error wrapping `ErrExit` is returned unchanged by all policies, so `errors.Is(err, retry.ErrExit)` holds.
- `ErrRetry` is a convenience sentinel for an ordinary retryable failure; it has no special handling — returning your own error works the same way.

## Callbacks

```go
retry.New(
    retry.WithOnFailedFn(func() {
        metrics.RetryFailed.Inc()
    }),
    retry.WithOnSuccessFn(func() {
        metrics.RetrySucceeded.Inc()
    }),
)
```

## Chainable Setters

All options are also available as chainable setter methods:

```go
r := retry.New().
    SetMaxAttempts(3).
    SetPolicy(retry.PolicyBackoff).
    SetDelay(time.Second)
```

## Defaults

| Option        | Default         |
| ------------- | --------------- |
| `MaxAttempts` | 5               |
| `Policy`      | `PolicyBackoff` |
| `Delay`       | 1s              |
| `MaxDelay`    | 0 (no cap)      |

## Options

| Option            | Description                                 |
| ----------------- | ------------------------------------------- |
| `WithContext`     | Set context (required for `PolicyInfinite`) |
| `WithLogger`      | Set a logger for retry progress messages    |
| `WithMaxAttempts` | Maximum number of attempts (must be >= 1)   |
| `WithPolicy`      | Retry strategy                              |
| `WithDelay`       | Initial delay between attempts              |
| `WithMaxDelay`    | Upper bound for the delay (0 = no cap)      |
| `WithOnFailedFn`  | Callback on each failed attempt             |
| `WithOnSuccessFn` | Callback on success                         |
