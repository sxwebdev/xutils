package retry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sxwebdev/xutils/loggerutil"
)

// Retry represents a retry mechanism
type Retry struct {
	ctx         context.Context
	logger      loggerutil.Logger
	maxAttempts int
	policy      Policy
	delay       time.Duration
	maxDelay    time.Duration
	onFailedFn  func()
	onSuccessFn func()
}

// New creates a new Retry instance
//
// maxAttempts: the maximum number of attempts. Default is 5
//
// policy: the retry policy. Default is PolicyBackoff
func New(opts ...Option) *Retry {
	r := &Retry{
		maxAttempts: 5,
		policy:      PolicyBackoff,
		delay:       1 * time.Second,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Do - performs the retry mechanism.
//
// On exhaustion of all attempts (PolicyLinear, PolicyBackoff) Do returns an
// *Error wrapping the last error returned by fn; use errors.Is / errors.As /
// errors.AsType to inspect the cause. If fn returns an error wrapping ErrExit,
// that error is returned as-is. When a context is configured (WithContext) and
// it is cancelled while waiting between attempts, the context error is returned.
func (r *Retry) Do(fn func() error) error {
	if fn == nil {
		return fmt.Errorf("retry function cannot be nil")
	}

	if r.policy != PolicyInfinite && r.maxAttempts < 1 {
		return fmt.Errorf("retry: maxAttempts must be >= 1, got %d", r.maxAttempts)
	}

	var err error
	switch r.policy {
	case PolicyLinear:
		err = r.linearRetry(fn)
	case PolicyBackoff:
		err = r.backoffRetry(fn)
	case PolicyInfinite:
		err = r.infiniteRetry(fn)
	default:
		err = fmt.Errorf("unsupported retry policy")
	}

	if err == nil && r.onSuccessFn != nil {
		r.onSuccessFn()
	}

	return err
}

// linearRetry - performs a linear retry mechanism
func (r *Retry) linearRetry(fn func() error) error {
	var lastErr error
	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		if err := r.ctxErr(); err != nil {
			return err
		}

		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		if r.onFailedFn != nil {
			r.onFailedFn()
		}

		if errors.Is(err, ErrExit) {
			return err
		}

		if attempt < r.maxAttempts {
			if r.logger != nil {
				r.logger.Infof("linear retry attempt %d failed, retrying in %s...", attempt, r.delay)
			}
			if err := r.sleep(r.delay); err != nil {
				return err
			}
		}
	}
	return &Error{Policy: PolicyLinear, Attempts: r.maxAttempts, Err: lastErr}
}

// backoffRetry - performs a backoff retry mechanism
func (r *Retry) backoffRetry(fn func() error) error {
	var lastErr error
	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		if err := r.ctxErr(); err != nil {
			return err
		}

		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		if r.onFailedFn != nil {
			r.onFailedFn()
		}

		if errors.Is(err, ErrExit) {
			return err
		}

		if attempt < r.maxAttempts {
			delay := r.backoffDelay(attempt)
			if r.logger != nil {
				r.logger.Infof("backoff retry attempt %d failed, retrying in %s...", attempt, delay)
			}
			if err := r.sleep(delay); err != nil {
				return err
			}
		}
	}
	return &Error{Policy: PolicyBackoff, Attempts: r.maxAttempts, Err: lastErr}
}

func (r *Retry) infiniteRetry(fn func() error) error {
	if r.ctx == nil {
		return fmt.Errorf("infinite retry cannot be initialized without ctx")
	}

	for {
		if err := r.ctxErr(); err != nil {
			return err
		}

		err := fn()
		if err == nil {
			return nil
		}

		if r.onFailedFn != nil {
			r.onFailedFn()
		}

		if errors.Is(err, ErrExit) {
			return err
		}

		if r.logger != nil {
			r.logger.Infof("infinite retry attempt failed, retrying in %s...", r.delay)
		}

		if err := r.sleep(r.delay); err != nil {
			return err
		}
	}
}

// backoffDelay computes the delay before the next attempt for the backoff
// policy as delay * 2^(attempt-1). The shift is bounded and the result is
// guarded against integer overflow; when maxDelay is set the delay is capped.
func (r *Retry) backoffDelay(attempt int) time.Duration {
	shift := min(attempt-1, 62)

	delay := r.delay << shift
	if delay <= 0 { // overflow guard: fall back to a sane bound
		if r.maxDelay > 0 {
			return r.maxDelay
		}
		return r.delay
	}

	if r.maxDelay > 0 && delay > r.maxDelay {
		return r.maxDelay
	}
	return delay
}

// ctxErr reports the configured context's error, if any. It returns nil when
// no context is set or the context is still active, so callers can honor
// cancellation uniformly across all policies before each attempt.
func (r *Retry) ctxErr() error {
	if r.ctx == nil {
		return nil
	}
	return r.ctx.Err()
}

// sleep waits for d. When a context is configured (WithContext) the wait is
// interrupted on cancellation and the context error is returned; otherwise it
// is a plain time.Sleep that always returns nil.
func (r *Retry) sleep(d time.Duration) error {
	if r.ctx == nil {
		time.Sleep(d)
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case <-timer.C:
		return nil
	}
}
