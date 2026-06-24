# Resilience Patterns

## retry — Retry mechanism

Flexible retry with three policies: Linear, Backoff, Infinite.

### Basic usage

```go
import "github.com/sxwebdev/xutils/retry"

// Default: 5 attempts, backoff policy, 1s initial delay
r := retry.New()
err := r.Do(func() error {
    return callExternalAPI()
})
```

### Policies

```go
// Linear: constant delay between attempts
r := retry.New(
    retry.WithPolicy(retry.PolicyLinear),
    retry.WithDelay(500 * time.Millisecond),
    retry.WithMaxAttempts(3),
)

// Backoff: exponential delay (delay * 2^(attempt-1))
// delays: 1s, 2s, 4s, 8s (4 waits between 5 attempts)
r := retry.New(
    retry.WithPolicy(retry.PolicyBackoff),
    retry.WithDelay(time.Second),
    retry.WithMaxAttempts(5),
)

// Infinite: retries forever until success, ErrExit, or context cancellation
r := retry.New(
    retry.WithPolicy(retry.PolicyInfinite),
    retry.WithContext(ctx),
    retry.WithDelay(2 * time.Second),
)
```

### Callbacks

```go
r := retry.New(
    retry.WithLogger(logger),
    retry.WithOnFailedFn(func() {
        metrics.IncrRetryFailed()
    }),
    retry.WithOnSuccessFn(func() {
        metrics.IncrRetrySuccess()
    }),
)
```

### Early exit

```go
err := r.Do(func() error {
    resp, err := http.Get(url)
    if err != nil {
        return err // will retry
    }
    if resp.StatusCode == 404 {
        return retry.ErrExit // stop immediately, no more retries
    }
    return nil
})
```

### Dynamic configuration

```go
r := retry.New()
r.SetMaxAttempts(10)
r.SetPolicy(retry.PolicyLinear)
r.SetDelay(2 * time.Second)
```

## Utility packages

### randutil — Cryptographically secure random

```go
import "github.com/sxwebdev/xutils/randutil"

// Random string (crypto/rand, rejection sampling to avoid modulo bias)
token, err := randutil.GenerateRandomString(32)
// uses default alphabet: a-z, A-Z, 0-9, _-@#$%

// Custom alphabet (ASCII, byte-indexed, max 256 bytes)
code, err := randutil.GenerateRandomString(6, randutil.WithAlphabet("0123456789"))

// Random number with exact digit count (up to 19 digits)
num, err := randutil.GenerateRandomNumber(6) // e.g., 482917
```

### strutil — String utilities

```go
import "github.com/sxwebdev/xutils/strutil"

// Remove invalid UTF-8 characters
clean := strutil.ClearUTF8String(rawInput)

// Remove null bytes
clean := strutil.RemoveNullBytes(rawInput)

// Format number with decimal precision
strutil.FormatNumberWithPrecision("12345", 2) // "123.45"

// Human-readable duration
strutil.FormatDuration(500 * time.Millisecond) // "500ms"
strutil.FormatDuration(90 * time.Second)       // "1m 30s"
strutil.FormatDuration(25 * time.Hour)         // "1d 1h"
```

### timeutil — Execution time measurement

```go
import "github.com/sxwebdev/xutils/timeutil"

duration, err := timeutil.TimeIt(func() error {
    return processData()
})
fmt.Printf("took %s\n", duration) // "took 1.234s"
```

### testutil — Test helpers

```go
import "github.com/sxwebdev/xutils/testutil"

// Pretty-print any value as indented JSON (for debugging in tests)
testutil.PrintJSON(myStruct)
```

### loggerutil — Logger interface

```go
import "github.com/sxwebdev/xutils/loggerutil"

// Interface methods: Debugf, Debugw, Infof, Infow, Warnf, Warnw, Errorf, Errorw

// No-op logger (default in most packages)
logger := &loggerutil.EmptyLogger{}

// Test logger (prints to stdout via fmt)
logger := loggerutil.NewTestLogger()
```
