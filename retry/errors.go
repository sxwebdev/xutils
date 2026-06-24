package retry

import (
	"errors"
	"fmt"
)

var (
	// ErrRetry is a convenience sentinel a retried function may return to
	// signal an ordinary, retryable failure. It has no special handling inside
	// the package — any non-nil error (other than one wrapping ErrExit) triggers
	// a retry — so returning your own error is equally valid.
	ErrRetry = errors.New("retry")

	// ErrExit stops the retry loop immediately when returned (or wrapped) by the
	// retried function, regardless of the remaining attempts. The error that
	// wraps ErrExit is returned from Do unchanged (it is not wrapped in *Error),
	// so errors.Is(err, retry.ErrExit) holds. The onFailed callback still fires
	// for the attempt that returned ErrExit.
	ErrExit = errors.New("exit")
)

// Error describes a failed retry run. It carries the retry policy, the number
// of attempts performed and the last error returned by the retried function.
//
// The original error is available via errors.Is / errors.As / errors.Unwrap.
//
// To extract the *Error itself (e.g. to read Policy / Attempts) prefer the
// generic errors.AsType helper (Go 1.20+ as errors.As, Go 1.26+ as
// errors.AsType):
//
//	if rerr, ok := errors.AsType[*retry.Error](err); ok {
//		log.Printf("policy=%s attempts=%d cause=%v", rerr.Policy, rerr.Attempts, rerr.Err)
//	}
type Error struct {
	Policy   Policy // policy used for the run
	Attempts int    // number of attempts performed
	Err      error  // last error returned by the retried function
}

// Error implements the error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("%s retry failed after %d attempts: %v", e.Policy, e.Attempts, e.Err)
}

// Unwrap returns the original error so it can be inspected with
// errors.Is / errors.As.
func (e *Error) Unwrap() error { return e.Err }
