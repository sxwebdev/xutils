package randutil

import "crypto/rand"

const defaultAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-@#$%"

// Option configures GenerateRandomString.
type Option func(*stringOptions)

type stringOptions struct {
	alphabet string
}

// WithAlphabet sets the alphabet used to build the random string. An empty
// alphabet is ignored, keeping the default.
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
// rejection sampling, so the result is free of modulo bias.
func GenerateRandomString(n int, opts ...Option) (string, error) {
	if n <= 0 {
		return "", nil
	}

	o := stringOptions{alphabet: defaultAlphabet}
	for _, opt := range opts {
		opt(&o)
	}
	alphabet := o.alphabet

	// rejection sampling: take bytes up to the nearest multiple of len(alphabet)
	alLen := byte(len(alphabet))
	maxrb := byte(255 - (256 % int(alLen)))

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
			if rb > maxrb {
				continue // discard to avoid modulo bias
			}
			out[i] = alphabet[int(rb)%int(alLen)]
			i++
			if i == n {
				break
			}
		}
	}

	return string(out), nil
}
