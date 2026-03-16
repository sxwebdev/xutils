# loopper

Periodic task runner with context timeout, panic recovery, and manual trigger.

## Features

- **Fixed interval** — runs a function on a configurable tick period
- **Context timeout** — each execution gets its own context with deadline
- **Overlap prevention** — skips tick if the previous execution is still running
- **Panic recovery** — panics are caught, the loop continues on the next tick
- **Leading mode** — optionally execute immediately on start
- **Manual trigger** — force an immediate execution outside the schedule

## Installation

```bash
go get github.com/sxwebdev/xutils/loopper
```

## Quick Start

```go
l := loopper.New(
    func(ctx context.Context) {
        resp, err := http.Get("https://example.com/health")
        if err != nil {
            log.Printf("health check failed: %v", err)
            return
        }
        resp.Body.Close()
        log.Printf("health check: %d", resp.StatusCode)
    },
    loopper.WithPeriod(10*time.Second),
    loopper.WithContextTimeout(5*time.Second),
)

l.Start(ctx)
defer l.Stop()
defer l.Wait()
```

## Leading Mode

Execute the task immediately on start, then continue on the regular interval:

```go
l := loopper.New(taskFn,
    loopper.WithLeading(),
    loopper.WithPeriod(30*time.Second),
)
```

## Manual Trigger

Force an immediate execution outside the regular schedule:

```go
started := l.Trigger(ctx) // true if started, false if already running
```

## Graceful Shutdown

```go
l.Stop()  // stop scheduling new ticks
l.Wait()  // wait for current execution to finish
```

## Defaults

| Option             | Default |
| ------------------ | ------- |
| `WithPeriod`       | 60s     |
| `WithContextTimeout` | 30s  |
| `WithLeading`      | false   |

## Options

| Option               | Description                                    |
| -------------------- | ---------------------------------------------- |
| `WithPeriod`         | Tick interval between executions               |
| `WithContextTimeout` | Per-execution context deadline                 |
| `WithLeading`        | Execute immediately on Start                   |
| `WithLogger`         | Set a structured logger for debug output       |
