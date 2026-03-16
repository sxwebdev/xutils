# retry

Flexible retry mechanism with linear, exponential backoff, and infinite retry policies.

## Features

- **Three policies** — linear, exponential backoff, infinite
- **Early exit** — return `ErrExit` to stop retries on non-retryable errors
- **Callbacks** — `OnFailedFn` and `OnSuccessFn` for observability
- **Context support** — required for infinite policy, respects cancellation
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

| Option          | Default        |
| --------------- | -------------- |
| `MaxAttempts`   | 5              |
| `Policy`        | `PolicyBackoff` |
| `Delay`         | 1s             |

## Options

| Option           | Description                                   |
| ---------------- | --------------------------------------------- |
| `WithContext`    | Set context (required for `PolicyInfinite`)    |
| `WithLogger`     | Set a structured logger                       |
| `WithMaxAttempts` | Maximum number of attempts                   |
| `WithPolicy`     | Retry strategy                                |
| `WithDelay`      | Initial delay between attempts                |
| `WithOnFailedFn` | Callback on each failed attempt               |
| `WithOnSuccessFn` | Callback on success                          |
