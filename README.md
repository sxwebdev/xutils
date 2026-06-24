# xutils

[![Go Reference](https://pkg.go.dev/badge/github.com/sxwebdev/xutils.svg)](https://pkg.go.dev/github.com/sxwebdev/xutils)
[![Go Version](https://img.shields.io/badge/go-1.26-blue)](https://go.dev/)
[![Go Report Card](https://goreportcard.com/badge/github.com/sxwebdev/xutils)](https://goreportcard.com/report/github.com/sxwebdev/xutils)
[![License](https://img.shields.io/github/license/sxwebdev/xutils)](LICENSE)

A collection of Go utility packages.

```text
go get github.com/sxwebdev/xutils
```

## Packages

| Package      | Description                                                                                                                     |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `broker`     | Generic thread-safe pub/sub message broker with goroutine-based broadcasting                                                    |
| `cacheutil`  | Cache interface for key-value storage with JSON and TTL support                                                                 |
| `dbutil`     | Database helpers: pagination (limit/offset), JSON column type, duration column type, transaction wrapper, generic find response |
| `loggerutil` | Minimal logger interface with a no-op implementation                                                                            |
| `loopper`    | Periodic task runner with context timeout, panic recovery and manual trigger                                                    |
| `randutil`   | Cryptographically secure random string and number generation                                                                    |
| `retry`      | Retry mechanism with linear, backoff and infinite policies                                                                      |
| `strutil`    | String helpers: UTF-8 cleanup, null byte removal, number formatting, duration formatting                                        |
| `syncutil`   | Thread-safe generic containers: `Map`, `Slice`, and `Locker` (mutex-wrapped value)                                              |
| `testutil`   | Test helpers: pretty-print any value as indented JSON                                                                           |
| `timeutil`   | Function execution time measurement                                                                                             |
| `pipeline`   | Declarative resumable workflow engine with action/poll/branch steps, saga compensation and snapshot persistence                 |
| `workflow`   | Stage-based workflow engine with steps, retry policies and lifecycle hooks                                                      |

## AI Agent Skills

This repository includes [AI agent skills](https://github.com/sxwebdev/skills) with documentation and usage examples for all packages. Install them with the [skills](https://github.com/sxwebdev/skills) CLI:

```bash
go install github.com/sxwebdev/skills/cmd/skills@latest
skills add sxwebdev/xutils
```

## Project Structure

```text
├── /skills/xutils # AI agent skills for Claude Code and other agents
├── broker/        # Generic thread-safe pub/sub message broker
├── cacheutil/     # Cache interface for key-value storage with JSON and TTL support
├── dbutil/        # Database helpers: pagination, JSON column, duration column, tx wrapper
├── loggerutil/    # Minimal logger interface with no-op and test implementations
├── loopper/       # Periodic task runner with context timeout and panic recovery
├── pipeline/      # Declarative resumable workflow engine (action/poll/branch, saga, snapshots)
├── randutil/      # Cryptographically secure random string and number generation
├── retry/         # Retry mechanism with linear, backoff and infinite policies
├── strutil/       # String helpers: UTF-8 cleanup, number and duration formatting
├── syncutil/      # Thread-safe generic containers: Map, Slice, Locker
├── testutil/      # Test helpers: pretty-print as indented JSON
├── timeutil/      # Function execution time measurement
└── workflow/      # Stage-based workflow engine with retry, snapshots and lifecycle hooks
```
