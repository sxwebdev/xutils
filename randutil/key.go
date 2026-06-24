package randutil

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

const (
	// MinKeySize is the smallest key length (128-bit) considered safe for
	// cryptographic use. Smaller keys are rejected.
	MinKeySize = 16
	// RecommendedKeySize is the recommended key length (256-bit) for secrets,
	// API keys, and symmetric keys with a long lifetime.
	RecommendedKeySize = 32
)

// GenerateKeyBytes returns nBytes of cryptographically secure random key
// material from crypto/rand. nBytes must be at least [MinKeySize] (128-bit);
// [RecommendedKeySize] (256-bit) is recommended. Use this when you need the raw
// key (e.g. for HMAC/AES) and want to control its lifetime in memory.
func GenerateKeyBytes(nBytes int) ([]byte, error) {
	if nBytes < MinKeySize {
		return nil, fmt.Errorf("randutil: key size must be >= %d bytes (128-bit), got %d", MinKeySize, nBytes)
	}

	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		// Defensive: on Go 1.24+ a crypto/rand reader failure is fatal, not an
		// error return, so this path is unreachable in practice.
		return nil, err
	}

	return b, nil
}

// GenerateKey returns a cryptographically secure random key encoded as a
// URL-safe, unpadded base64 string (suitable for API keys, tokens, and
// secrets). nBytes is the amount of entropy and must satisfy the [MinKeySize]
// requirement of [GenerateKeyBytes]; decode the result with
// base64.RawURLEncoding to recover the raw bytes.
func GenerateKey(nBytes int) (string, error) {
	b, err := GenerateKeyBytes(nBytes)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
