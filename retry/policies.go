package retry

import "fmt"

type Policy int

const (
	PolicyLinear Policy = iota
	PolicyBackoff
	PolicyInfinite
)

// String returns the human-readable name of the policy.
func (r Policy) String() string {
	switch r {
	case PolicyLinear:
		return "linear"
	case PolicyBackoff:
		return "backoff"
	case PolicyInfinite:
		return "infinite"
	default:
		return "unknown"
	}
}

// Validate validates the retry policy
func (r Policy) Validate() error {
	switch r {
	case PolicyLinear, PolicyBackoff, PolicyInfinite:
		return nil
	default:
		return fmt.Errorf("invalid retry policy")
	}
}
