# xutils

[![Go Reference](https://pkg.go.dev/badge/github.com/sxwebdev/xutils.svg)](https://pkg.go.dev/github.com/sxwebdev/xutils)
[![Go Version](https://img.shields.io/badge/go-1.25-blue)](https://go.dev/)
[![Go Report Card](https://goreportcard.com/badge/github.com/sxwebdev/xutils)](https://goreportcard.com/report/github.com/sxwebdev/xutils)
[![License](https://img.shields.io/github/license/sxwebdev/xutils)](LICENSE)

A collection of Go utility packages.

```text
go get github.com/sxwebdev/xutils
```

## Packages

| Package      | Description                                                                                                     |
| ------------ | --------------------------------------------------------------------------------------------------------------- |
| `broker`     | Generic thread-safe pub/sub message broker with goroutine-based broadcasting                                    |
| `cacheutil`  | Cache interface for key-value storage with JSON and TTL support                                                 |
| `dbutil`     | Pagination helper: calculates limit/offset from page and pageSize                                               |
| `loggerutil` | Minimal logger interface with a no-op implementation                                                            |
| `loopper`    | Periodic task runner with context timeout, panic recovery and manual trigger                                    |
| `randutil`   | Cryptographically secure random string and number generation                                                    |
| `retry`      | Retry mechanism with linear, backoff and infinite policies                                                      |
| `strutil`    | String helpers: UTF-8 cleanup, null byte removal, number formatting, duration formatting                        |
| `syncutil`   | Thread-safe generic containers: `Map`, `Slice`, and `Locker` (mutex-wrapped value)                              |
| `testutil`   | Test helpers: pretty-print any value as indented JSON                                                           |
| `timeutil`   | Function execution time measurement                                                                             |
| `pipeline`   | Declarative resumable workflow engine with action/poll/branch steps, saga compensation and snapshot persistence |
| `workflow`   | Stage-based workflow engine with steps, retry policies and lifecycle hooks                                      |
