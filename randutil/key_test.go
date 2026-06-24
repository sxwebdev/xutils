package randutil_test

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/randutil"
)

func TestGenerateKey_DecodesToRequestedBytes(t *testing.T) {
	for _, n := range []int{randutil.MinKeySize, randutil.RecommendedKeySize, 64, 100} {
		key, err := randutil.GenerateKey(n)
		require.NoError(t, err)

		// Encoded length is deterministic for a given byte count.
		require.Equal(t, base64.RawURLEncoding.EncodedLen(n), len(key))

		raw, err := base64.RawURLEncoding.DecodeString(key)
		require.NoError(t, err, "key must be valid raw-url base64")
		assert.Len(t, raw, n, "decoded key must carry exactly the requested entropy")
	}
}

func TestGenerateKeyBytes_ReturnsRequestedEntropy(t *testing.T) {
	b, err := randutil.GenerateKeyBytes(randutil.RecommendedKeySize)
	require.NoError(t, err)
	assert.Len(t, b, randutil.RecommendedKeySize)

	// Two draws must differ (sanity check that it is not constant).
	b2, err := randutil.GenerateKeyBytes(randutil.RecommendedKeySize)
	require.NoError(t, err)
	assert.NotEqual(t, b, b2)
}

func TestGenerateKey_URLSafeAndUnpadded(t *testing.T) {
	key, err := randutil.GenerateKey(48)
	require.NoError(t, err)
	assert.NotContains(t, key, "+", "must be URL-safe")
	assert.NotContains(t, key, "/", "must be URL-safe")
	assert.NotContains(t, key, "=", "must be unpadded")
}

func TestGenerateKey_Unique(t *testing.T) {
	const iterations = 2000
	seen := make(map[string]struct{}, iterations)
	for range iterations {
		key, err := randutil.GenerateKey(32)
		require.NoError(t, err)
		_, dup := seen[key]
		require.False(t, dup, "GenerateKey must not repeat a 32-byte key")
		seen[key] = struct{}{}
	}
}

func TestGenerateKey_RejectsWeakSizes(t *testing.T) {
	// Anything below the 128-bit minimum (including non-positive) must be
	// rejected so a weak key cannot be created by accident.
	for _, n := range []int{-100, -1, 0, 1, 8, randutil.MinKeySize - 1} {
		_, errStr := randutil.GenerateKey(n)
		require.Error(t, errStr, "GenerateKey(%d) must be rejected", n)

		_, errBytes := randutil.GenerateKeyBytes(n)
		require.Error(t, errBytes, "GenerateKeyBytes(%d) must be rejected", n)
	}

	// Exactly the minimum is accepted.
	_, err := randutil.GenerateKey(randutil.MinKeySize)
	require.NoError(t, err)
}
