package randutil_test

import (
	"strings"
	"testing"

	"github.com/sxwebdev/xutils/randutil"
)

const defaultAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-@#$%"

func TestGenerateRandomString(t *testing.T) {
	tests := []struct {
		name     string
		n        int
		alphabet string
	}{
		{"length 0", 0, ""},
		{"length 1 default alphabet", 1, ""},
		{"length 10 default alphabet", 10, ""},
		{"length 100 default alphabet", 100, ""},
		{"length 1000 default alphabet", 1000, ""},
		{"length 100 custom alphabet abc", 100, "abc"},
		{"length 100 custom alphabet digits", 100, "0123456789"},
		{"single-element alphabet", 50, "Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []randutil.Option
			if tt.alphabet != "" {
				opts = append(opts, randutil.WithAlphabet(tt.alphabet))
			}
			s, err := randutil.GenerateRandomString(tt.n, opts...)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(s) != tt.n {
				t.Errorf("expected length %d, got %d", tt.n, len(s))
			}
			want := tt.alphabet
			if want == "" {
				want = defaultAlphabet
			}
			for i := range len(s) {
				if !strings.ContainsRune(want, rune(s[i])) {
					t.Errorf("byte %q at %d not in alphabet %q", s[i], i, want)
				}
			}
		})
	}
}

// A single-character alphabet must reproduce that character n times. This also
// pins the rejection-sampling math at alLen==1 (accept==256: every byte usable).
func TestGenerateRandomString_SingleCharAlphabet(t *testing.T) {
	s, err := randutil.GenerateRandomString(64, randutil.WithAlphabet("x"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != strings.Repeat("x", 64) {
		t.Fatalf("expected 64 x's, got %q", s)
	}
}

// Non-positive lengths yield an empty string with no error (documented).
func TestGenerateRandomString_NonPositiveLength(t *testing.T) {
	for _, n := range []int{0, -1, -1000} {
		s, err := randutil.GenerateRandomString(n)
		if err != nil {
			t.Errorf("n=%d: unexpected error: %v", n, err)
		}
		if s != "" {
			t.Errorf("n=%d: expected empty string, got %q", n, s)
		}
	}
}

// Every character of the alphabet must be reachable, and the output must use
// more than one distinct character (proves it is not constant). With 10k draws
// from a 16-char alphabet, missing any character would be astronomically
// unlikely unless a character is genuinely unreachable (e.g. a modulo/rejection
// bug skipping the tail of the alphabet).
func TestGenerateRandomString_CoversWholeAlphabet(t *testing.T) {
	const alphabet = "0123456789abcdef"
	s, err := randutil.GenerateRandomString(10000, randutil.WithAlphabet(alphabet))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	seen := map[byte]bool{}
	for i := range len(s) {
		seen[s[i]] = true
	}
	for i := range len(alphabet) {
		if !seen[alphabet[i]] {
			t.Errorf("alphabet character %q never appeared", alphabet[i])
		}
	}
	if len(seen) < 2 {
		t.Fatalf("output is effectively constant: only %d distinct chars", len(seen))
	}
}

// Two independent draws of a long string must differ. The contract promises
// randomness, so identical output would indicate a broken/constant generator.
func TestGenerateRandomString_DistinctDraws(t *testing.T) {
	a, err := randutil.GenerateRandomString(64)
	if err != nil {
		t.Fatal(err)
	}
	b, err := randutil.GenerateRandomString(64)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("two 64-char draws were identical: %q", a)
	}
}

// Regression: a 256-byte alphabet used to panic with "integer divide by zero"
// because byte(256) == 0 in the rejection-sampling math. It must now produce a
// correct 256-length-alphabet string instead. Breaking the fix (reverting to
// byte(len(alphabet))) makes this test panic.
func TestGenerateRandomString_256ByteAlphabet(t *testing.T) {
	// 256 distinct bytes, one per index, so we can verify membership precisely.
	var sb strings.Builder
	for i := range 256 {
		sb.WriteByte(byte(i))
	}
	alphabet := sb.String()

	s, err := randutil.GenerateRandomString(5000, randutil.WithAlphabet(alphabet))
	if err != nil {
		t.Fatalf("256-byte alphabet must not error: %v", err)
	}
	if len(s) != 5000 {
		t.Fatalf("expected length 5000, got %d", len(s))
	}
	// At alLen==256 every byte 0..255 maps to a distinct alphabet index with no
	// rejection; over 5000 draws we should see a broad spread, not a collapse.
	seen := map[byte]bool{}
	for i := range len(s) {
		seen[s[i]] = true
	}
	if len(seen) < 100 {
		t.Fatalf("256-byte alphabet collapsed: only %d distinct bytes in 5000 draws", len(seen))
	}
}

// Regression: an alphabet larger than 256 bytes used to silently truncate
// byte(len(alphabet)), collapsing the alphabet (e.g. a 300-byte all-"X"
// alphabet produced only "X"). It must now return an error rather than
// corrupt output. Breaking the fix (removing the guard) makes this fail.
func TestGenerateRandomString_AlphabetTooLarge(t *testing.T) {
	for _, size := range []int{257, 300, 1000} {
		s, err := randutil.GenerateRandomString(10, randutil.WithAlphabet(strings.Repeat("X", size)))
		if err == nil {
			t.Errorf("size=%d: expected error for oversized alphabet, got string %q", size, s)
		}
		if s != "" {
			t.Errorf("size=%d: expected empty string on error, got %q", size, s)
		}
	}
}

// n<=0 short-circuits before the alphabet is validated, so an oversized
// alphabet with n<=0 still returns an empty string and no error.
func TestGenerateRandomString_OversizedAlphabetWithZeroLength(t *testing.T) {
	s, err := randutil.GenerateRandomString(0, randutil.WithAlphabet(strings.Repeat("X", 1000)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != "" {
		t.Fatalf("expected empty string, got %q", s)
	}
}

func TestWithAlphabet_EmptyFallsBackToDefault(t *testing.T) {
	s, err := randutil.GenerateRandomString(100, randutil.WithAlphabet(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s) != 100 {
		t.Fatalf("expected length 100, got %d", len(s))
	}
	for i := range len(s) {
		if !strings.ContainsRune(defaultAlphabet, rune(s[i])) {
			t.Errorf("character %q not in default alphabet (empty WithAlphabet must fall back)", s[i])
		}
	}
}
