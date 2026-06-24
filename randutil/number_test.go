package randutil_test

import (
	"fmt"
	"testing"

	"github.com/sxwebdev/xutils/randutil"
)

func TestGenerateRandomNumber(t *testing.T) {
	tests := []struct {
		name   string
		length uint
	}{
		{"length 1", 1},
		{"length 5", 5},
		{"length 10", 10},
		{"length 19", 19},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := randutil.GenerateRandomNumber(tt.length)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			s := fmt.Sprintf("%d", n)
			if len(s) != int(tt.length) {
				t.Errorf("expected length %d, got %d", tt.length, len(s))
			}
			if s[0] == '0' {
				t.Errorf("number starts with zero: %s", s)
			}
		})
	}

	// Regression: length 19 used to overflow int64 (10^19-1 does not fit),
	// occasionally yielding negative / out-of-range values. Hammer it so the
	// bug is caught deterministically rather than ~8% of the time.
	t.Run("length 19 is always a valid 19-digit int64", func(t *testing.T) {
		const minInt64Len19 = int64(1000000000000000000) // 10^18
		for range 5000 {
			n, err := randutil.GenerateRandomNumber(19)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if n < minInt64Len19 {
				t.Fatalf("value below 10^18 (negative or too small): %d", n)
			}
			if s := fmt.Sprintf("%d", n); len(s) != 19 {
				t.Fatalf("expected 19 digits, got %d (%s)", len(s), s)
			}
		}
	})

	t.Run("length 0", func(t *testing.T) {
		_, err := randutil.GenerateRandomNumber(0)
		if err == nil {
			t.Error("expected error for length 0, got nil")
		}
	})
	t.Run("length too large", func(t *testing.T) {
		_, err := randutil.GenerateRandomNumber(20)
		if err == nil {
			t.Error("expected error for length > 19, got nil")
		}
	})
}
