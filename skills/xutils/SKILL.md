---
name: xutils
description: Go utility packages collection (github.com/sxwebdev/xutils) — pub/sub broker, syncutil containers, retry, pipeline/workflow engines, dbutil, cacheutil, and more. Triggers when code imports xutils packages.
user-invocable: true
---

# xutils

Go utility packages for concurrency, orchestration, resilience, and data handling.

## When to use

- Code imports `github.com/sxwebdev/xutils` or any sub-package
- User is building Go services that need pub/sub, retry, periodic tasks, or workflow orchestration
- User asks about thread-safe collections, pagination, or random generation in Go

## How to proceed

1. Read `packages-overview.md` to pick the right package for the task
2. For concurrency (broker, syncutil, loopper) — read `concurrency-patterns.md`
3. For pipeline or workflow engines — read `orchestration-guide.md`
4. For database/cache utilities — read `database-and-cache.md`
5. For retry and error handling — read `resilience-patterns.md`

## Key principles

- All packages use Go generics where applicable (Go 1.18+)
- Functional options pattern for configuration: `New(opts ...Option)`
- `loggerutil.Logger` interface is used across broker, retry, loopper, pipeline, workflow
- Zero external runtime dependencies (only `testify` for tests)
- Pipeline and workflow engines support snapshot persistence for resumability
