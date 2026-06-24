package randutil_test

import (
	"strings"
	"testing"

	"github.com/sxwebdev/xutils/randutil"
)

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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []randutil.Option
			if tt.alphabet != "" {
				opts = append(opts, randutil.WithAlphabet(tt.alphabet))
			}
			s, err := randutil.GenerateRandomString(tt.n, opts...)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if len(s) != tt.n {
				t.Errorf("expected length %d, got %d", tt.n, len(s))
			}
			if tt.alphabet != "" {
				for _, c := range s {
					if !strings.ContainsRune(tt.alphabet, c) {
						t.Errorf("character %q not in alphabet %q", c, tt.alphabet)
					}
				}
			}
		})
	}
}

func TestWithAlphabet_EmptyFallsBackToDefault(t *testing.T) {
	const def = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-@#$%"

	s, err := randutil.GenerateRandomString(100, randutil.WithAlphabet(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s) != 100 {
		t.Fatalf("expected length 100, got %d", len(s))
	}
	for _, c := range s {
		if !strings.ContainsRune(def, c) {
			t.Errorf("character %q not in default alphabet (empty WithAlphabet must fall back)", c)
		}
	}
}
