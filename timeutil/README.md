# timeutil

Small time-related helpers.

## Installation

```bash
go get github.com/sxwebdev/xutils/timeutil
```

## Usage

`TimeIt` runs a function and reports how long it took alongside the function's error:

```go
d, err := timeutil.TimeIt(func() error {
    return doWork(ctx)
})
log.Printf("doWork took %s (err=%v)", d, err)
```

## API

| Function     | Description                                 |
| ------------ | ------------------------------------------- |
| `TimeIt(fn)` | Run `fn`, return its elapsed time and error |
