# xutils

A collection of Go utility packages.

```text
go get github.com/sxwebdev/xutils
```

## Packages

| Package      | Description                                                                              |
| ------------ | ---------------------------------------------------------------------------------------- |
| `broker`     | Generic thread-safe pub/sub message broker with goroutine-based broadcasting             |
| `dbutil`     | Pagination helper: calculates limit/offset from page and pageSize                        |
| `loggerutil` | Minimal logger interface with a no-op implementation                                     |
| `loopper`    | Periodic task runner with context timeout, panic recovery and manual trigger             |
| `randutil`   | Cryptographically secure random string and number generation                             |
| `strutil`    | String helpers: UTF-8 cleanup, null byte removal, number formatting, duration formatting |
| `syncutil`   | Thread-safe generic containers: `Map`, `Slice`, and `Locker` (mutex-wrapped value)       |
| `testutil`   | Test helpers: pretty-print any value as indented JSON                                    |
| `timeutil`   | Function execution time measurement                                                      |
