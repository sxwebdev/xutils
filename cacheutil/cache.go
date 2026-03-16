package cacheutil

import (
	"context"
	"time"
)

type ICache interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte, expiration time.Duration) error
	Delete(ctx context.Context, key []byte) error
	Keys(ctx context.Context, prefix []byte) ([]string, error)
	KeysAndValues(ctx context.Context, prefix []byte) (map[string][]byte, error)
	GetFromJSON(ctx context.Context, key []byte, dst any) error
	SetJSON(ctx context.Context, key []byte, value any, expiration time.Duration) error
	Exists(ctx context.Context, key []byte) (bool, error)
}
