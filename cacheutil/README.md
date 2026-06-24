# cacheutil

Backend-agnostic cache interface.

## Overview

`cacheutil` declares a single interface, `ICache`, that abstracts a byte-oriented key-value cache with
per-entry expiration, prefix scans, JSON helpers, and existence checks. Depend on the interface instead
of a concrete store so the backend (Redis, in-memory, Pebble, …) can be swapped or mocked in tests.

The package contains only the contract — implementations live in their backend packages.

## Installation

```bash
go get github.com/sxwebdev/xutils/cacheutil
```

## Interface

```go
type ICache interface {
    Get(ctx context.Context, key []byte) ([]byte, error)
    Set(ctx context.Context, key, value []byte, expiration time.Duration) error
    Delete(ctx context.Context, key []byte) error
    Keys(ctx context.Context, prefix []byte) ([]string, error)
    KeysAndValues(ctx context.Context, prefix []byte) (map[string][]byte, error)
    GetFromJSON(ctx context.Context, key []byte, dst any) error
    SetJSON(ctx context.Context, key []byte, value any, expiration time.Duration) error
    Exists(ctx context.Context, key []byte) (bool, error)
}
```

## Usage

```go
func NewService(cache cacheutil.ICache) *Service {
    return &Service{cache: cache}
}
```
