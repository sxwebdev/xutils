package cacheutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/cacheutil"
)

// mockCache is a minimal in-memory implementation used to lock the ICache
// contract at compile time (and catch accidental signature drift). cacheutil
// itself declares only the interface, so there is no package code to cover.
type mockCache struct {
	data map[string][]byte
}

func newMockCache() *mockCache { return &mockCache{data: map[string][]byte{}} }

func (m *mockCache) Get(_ context.Context, key []byte) ([]byte, error) {
	v, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	return v, nil
}

func (m *mockCache) Set(_ context.Context, key, value []byte, _ time.Duration) error {
	m.data[string(key)] = value
	return nil
}

func (m *mockCache) Delete(_ context.Context, key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *mockCache) Keys(_ context.Context, _ []byte) ([]string, error) { return nil, nil }
func (m *mockCache) KeysAndValues(_ context.Context, _ []byte) (map[string][]byte, error) {
	return nil, nil
}
func (m *mockCache) GetFromJSON(_ context.Context, _ []byte, _ any) error              { return nil }
func (m *mockCache) SetJSON(_ context.Context, _ []byte, _ any, _ time.Duration) error { return nil }

func (m *mockCache) Exists(_ context.Context, key []byte) (bool, error) {
	_, ok := m.data[string(key)]
	return ok, nil
}

// Compile-time assertion that the contract is implementable.
var _ cacheutil.ICache = (*mockCache)(nil)

func TestICache_ContractIsUsable(t *testing.T) {
	var c cacheutil.ICache = newMockCache()
	ctx := t.Context()

	require.NoError(t, c.Set(ctx, []byte("k"), []byte("v"), time.Minute))

	got, err := c.Get(ctx, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)

	ok, err := c.Exists(ctx, []byte("k"))
	require.NoError(t, err)
	assert.True(t, ok)

	require.NoError(t, c.Delete(ctx, []byte("k")))
	ok, err = c.Exists(ctx, []byte("k"))
	require.NoError(t, err)
	assert.False(t, ok)
}
