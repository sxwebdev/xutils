// Package randutil provides cryptographically secure random generators for
// strings, fixed-length numbers, and keys.
//
// # Core Concepts
//
// Every generator draws entropy from crypto/rand (the operating system CSPRNG),
// never from math/rand. String generation uses rejection sampling so characters
// are uniformly distributed with no modulo bias, and key generation enforces a
// minimum size so a weak key cannot be produced by accident.
//
// # Random Strings
//
// [GenerateRandomString] returns a random string of length n. By default it
// uses a built-in alphabet; override it with [WithAlphabet]:
//
//	s, err := randutil.GenerateRandomString(32)                              // default alphabet
//	s, err := randutil.GenerateRandomString(16, randutil.WithAlphabet("ab")) // custom alphabet
//
// A non-positive length yields an empty string. An empty alphabet passed to
// [WithAlphabet] is ignored, keeping the default.
//
// # Random Numbers
//
// [GenerateRandomNumber] returns a random int64 with an exact number of decimal
// digits and no leading zero:
//
//	n, err := randutil.GenerateRandomNumber(6) // e.g. 482913, always 6 digits
//
// Length must be between 1 and 19. At length 19 the upper bound is clamped to
// math.MaxInt64, since 10^19 - 1 does not fit in an int64.
//
// # Keys
//
// [GenerateKey] returns a URL-safe, unpadded base64 string suitable for API
// keys, tokens, and secrets. [GenerateKeyBytes] returns the raw bytes for
// direct cryptographic use (HMAC, AES, …):
//
//	key, err := randutil.GenerateKey(randutil.RecommendedKeySize)      // 256-bit base64url string
//	raw, err := randutil.GenerateKeyBytes(randutil.RecommendedKeySize) // 32 raw bytes
//
// Both reject sizes below [MinKeySize] (128-bit). Use [RecommendedKeySize]
// (256-bit) for secrets and long-lived keys.
//
// # Security
//
// The entropy source is crypto/rand, which is the strongest randomness
// available in Go. On Go 1.24+ a reader failure aborts the process rather than
// silently falling back to weak randomness, so the generators never return
// low-entropy output. The strength of a key is therefore determined solely by
// its size — prefer [RecommendedKeySize].
package randutil
