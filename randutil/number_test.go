package randutil_test

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/sxwebdev/xutils/randutil"
)

// pow10 returns 10^p for small p without floating point.
func pow10(p uint) int64 {
	r := int64(1)
	for range p {
		r *= 10
	}
	return r
}

func TestGenerateRandomNumber(t *testing.T) {
	for _, length := range []uint{1, 2, 5, 10, 18} {
		t.Run(fmt.Sprintf("length %d in [10^(n-1), 10^n-1]", length), func(t *testing.T) {
			lo := pow10(length - 1) // smallest n-digit number (no leading zero)
			hi := pow10(length) - 1 // largest n-digit number
			// Hammer it so both bounds and the no-leading-zero property are
			// exercised across many draws, not just once.
			for range 2000 {
				n, err := randutil.GenerateRandomNumber(length)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if n < lo {
					t.Fatalf("value %d below lower bound %d (length %d)", n, lo, length)
				}
				if n > hi {
					t.Fatalf("value %d above upper bound %d (length %d)", n, hi, length)
				}
				if s := strconv.FormatInt(n, 10); len(s) != int(length) {
					t.Fatalf("expected %d digits, got %d (%s)", length, len(s), s)
				}
			}
		})
	}
}

// length 1 has a tiny domain [1,9]; verify both ends are reachable and no 0
// ever appears (which would mean a leading-zero / off-by-one on the lower
// bound). Reaching both 1 and 9 guards against the inclusive range being
// computed as exclusive on either end.
func TestGenerateRandomNumber_Length1CoversDomain(t *testing.T) {
	seen := map[int64]bool{}
	for range 5000 {
		n, err := randutil.GenerateRandomNumber(1)
		if err != nil {
			t.Fatal(err)
		}
		if n < 1 || n > 9 {
			t.Fatalf("length-1 value out of [1,9]: %d", n)
		}
		seen[n] = true
	}
	if !seen[1] {
		t.Error("lower bound 1 never produced (inclusive lower bound broken?)")
	}
	if !seen[9] {
		t.Error("upper bound 9 never produced (inclusive upper bound broken?)")
	}
}

// Regression: length 19 used to overflow int64 (10^19-1 does not fit),
// occasionally yielding negative / out-of-range values. The upper bound is
// clamped to math.MaxInt64. Assert BOTH bounds: value is a valid 19-digit
// number >= 10^18 and never exceeds MaxInt64 (it cannot, by type, but we also
// confirm it never goes negative, which is the actual historical symptom).
func TestGenerateRandomNumber_Length19(t *testing.T) {
	const lo = int64(1_000_000_000_000_000_000) // 10^18
	for range 5000 {
		n, err := randutil.GenerateRandomNumber(19)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n < lo {
			t.Fatalf("value below 10^18 (negative or too small): %d", n)
		}
		if n > math.MaxInt64 {
			t.Fatalf("value above MaxInt64: %d", n) // unreachable by int64 type, asserts intent
		}
		if s := strconv.FormatInt(n, 10); len(s) != 19 {
			t.Fatalf("expected 19 digits, got %d (%s)", len(s), s)
		}
	}
}

// Distinct successive draws of a wide range must differ (randomness contract).
func TestGenerateRandomNumber_DistinctDraws(t *testing.T) {
	a, err := randutil.GenerateRandomNumber(18)
	if err != nil {
		t.Fatal(err)
	}
	b, err := randutil.GenerateRandomNumber(18)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("two 18-digit draws identical: %d", a)
	}
}

func TestGenerateRandomNumber_InvalidLength(t *testing.T) {
	for _, length := range []uint{0, 20, 100} {
		if _, err := randutil.GenerateRandomNumber(length); err == nil {
			t.Errorf("length %d: expected error, got nil", length)
		}
	}
}
