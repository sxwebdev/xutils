package randutil

import (
	"crypto/rand"
	"fmt"
)

const defaultAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-@#$%"

// Option configures GenerateRandomString.
type Option func(*stringOptions)

type stringOptions struct {
	alphabet string
}

// WithAlphabet sets the alphabet used to build the random string. An empty
// alphabet is ignored, keeping the default.
//
// The alphabet is byte-indexed, so use ASCII: a multi-byte (non-ASCII) alphabet
// can produce invalid UTF-8. It may hold at most 256 bytes; a larger one makes
// [GenerateRandomString] return an error.
func WithAlphabet(alphabet string) Option {
	return func(o *stringOptions) {
		if alphabet != "" {
			o.alphabet = alphabet
		}
	}
}

// GenerateRandomString generates a random string of the specified length.
// By default it uses a built-in alphabet; override it with WithAlphabet.
//
// Characters are drawn uniformly from the alphabet using crypto/rand with
// rejection sampling, so the result is free of modulo bias. The alphabet is
// byte-indexed (use ASCII) and may hold at most 256 bytes; a larger alphabet
// returns an error.
func GenerateRandomString(n int, opts ...Option) (string, error) {
	if n <= 0 {
		return "", nil
	}

	o := stringOptions{alphabet: defaultAlphabet}
	for _, opt := range opts {
		opt(&o)
	}
	alphabet := o.alphabet

	// Selection is driven by a single random byte (256 distinct values), so the
	// alphabet must fit in that space. Rejecting here avoids two bugs the byte
	// math otherwise hits: len==256 makes byte(len)==0 (divide by zero) and
	// len>256 truncates byte(len), silently collapsing the alphabet.
	alLen := len(alphabet)
	if alLen > 256 {
		return "", fmt.Errorf("randutil: alphabet too large: %d bytes (max 256)", alLen)
	}

	// Rejection sampling: accept only bytes in [0, accept), the largest multiple
	// of alLen that fits in a byte, so every alphabet index is equally likely
	// (no modulo bias). For alLen==256, accept==256: every byte is usable.
	accept := 256 - (256 % alLen)

	out := make([]byte, n)
	i := 0
	buf := make([]byte, 64) // read in batches for efficiency

	for i < n {
		// On Go 1.24+ crypto/rand never returns an error here (a reader failure
		// crashes the process instead), so this branch is defensive only.
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}
		for _, rb := range buf {
			if int(rb) >= accept {
				continue // discard to avoid modulo bias
			}
			out[i] = alphabet[int(rb)%alLen]
			i++
			if i == n {
				break
			}
		}
	}

	return string(out), nil
}
