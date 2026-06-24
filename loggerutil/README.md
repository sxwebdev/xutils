# loggerutil

Minimal logging interface and no-op / test implementations.

## Overview

`loggerutil` defines the `Logger` interface used across xutils so packages can emit diagnostics without
depending on a concrete logging backend. Applications supply a thin adapter around their real logger.

## Installation

```bash
go get github.com/sxwebdev/xutils/loggerutil
```

## Interface

```go
type Logger interface {
    Debugf(format string, args ...any)
    Debugw(format string, args ...any)
    Infof(format string, args ...any)
    Infow(format string, args ...any)
    Warnf(format string, args ...any)
    Warnw(format string, args ...any)
    Errorf(format string, args ...any)
    Errorw(format string, args ...any)
}
```

- `…f` methods take a format string + args (like `Printf`).
- `…w` methods are intended for structured key-value logging.

## Built-in Implementations

| Type          | Behavior                                          |
| ------------- | ------------------------------------------------- |
| `EmptyLogger` | Discards all output — the safe default when unset |
| `TestLogger`  | Prints to stdout with a level prefix (for tests)  |

```go
func New(log loggerutil.Logger) *Service {
    if log == nil {
        log = &loggerutil.EmptyLogger{} // avoid nil checks at every call site
    }
    return &Service{log: log}
}

svc := New(loggerutil.NewTestLogger())
```
