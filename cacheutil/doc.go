// Package cacheutil defines a backend-agnostic cache interface.
//
// [ICache] abstracts a byte-oriented key-value cache with per-entry expiration,
// prefix scans, JSON convenience helpers, and existence checks. It lets code
// depend on the cache contract rather than a concrete store (Redis, in-memory,
// Pebble, …), so the implementation can be swapped or mocked in tests.
//
// The package intentionally contains only the interface; implementations live
// in their respective backend packages.
//
//	type ICache interface {
//		Get(ctx context.Context, key []byte) ([]byte, error)
//		Set(ctx context.Context, key, value []byte, expiration time.Duration) error
//		Delete(ctx context.Context, key []byte) error
//		Keys(ctx context.Context, prefix []byte) ([]string, error)
//		KeysAndValues(ctx context.Context, prefix []byte) (map[string][]byte, error)
//		GetFromJSON(ctx context.Context, key []byte, dst any) error
//		SetJSON(ctx context.Context, key []byte, value any, expiration time.Duration) error
//		Exists(ctx context.Context, key []byte) (bool, error)
//	}
package cacheutil
