package randutil

import (
	"crypto/rand"
	"errors"
	"math"
	"math/big"
)

// GenerateRandomNumber generates a random number with the specified number of
// decimal digits (no leading zero), in the range [10^(length-1), 10^length - 1].
//
// For length 19 the upper bound is clamped to math.MaxInt64
// (9223372036854775807), since 10^19 - 1 does not fit in an int64; values above
// MaxInt64 are therefore not produced. Use a string-based generator if the full
// 19-digit range is required.
func GenerateRandomNumber(length uint) (int64, error) {
	if length == 0 {
		return 0, errors.New("length must be >= 1")
	}
	// int64 holds at most 19 decimal digits (9223372036854775807).
	if length > 19 {
		return 0, errors.New("length too large for int64; use string variant")
	}

	// Compute the bounds with big.Int: 10^19 - 1 overflows int64, so doing this
	// in int64 arithmetic would wrap and yield out-of-range (even negative)
	// results for length 19.
	ten := big.NewInt(10)
	min := new(big.Int).Exp(ten, big.NewInt(int64(length-1)), nil) // 10^(length-1)
	max := new(big.Int).Exp(ten, big.NewInt(int64(length)), nil)
	max.Sub(max, big.NewInt(1)) // 10^length - 1

	// Clamp to MaxInt64 so the result always fits an int64 (relevant at length 19).
	if maxInt64 := big.NewInt(math.MaxInt64); max.Cmp(maxInt64) > 0 {
		max = maxInt64
	}

	// Uniform value in [min, max] inclusive.
	span := new(big.Int).Sub(max, min)
	span.Add(span, big.NewInt(1))

	n, err := rand.Int(rand.Reader, span)
	if err != nil {
		// Defensive: on Go 1.24+ a crypto/rand reader failure is fatal, not an
		// error return, so this path is unreachable in practice.
		return 0, err
	}
	n.Add(n, min)
	return n.Int64(), nil
}
