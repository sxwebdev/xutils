// Package retry provides a flexible retry mechanism with linear, exponential
// backoff, and infinite retry policies.
//
// # Core Concepts
//
// A [Retry] wraps a function call and re-executes it on failure according to
// a configurable [Policy]. Three built-in policies are provided:
//
//   - [PolicyLinear] — constant delay between attempts
//   - [PolicyBackoff] — exponential backoff (delay doubles each attempt)
//   - [PolicyInfinite] — retries forever until the context is cancelled or [ErrExit] is returned
//
// # Quick Start
//
//	r := retry.New(
//		retry.WithMaxAttempts(3),
//		retry.WithPolicy(retry.PolicyBackoff),
//		retry.WithDelay(time.Second),
//	)
//
//	err := r.Do(func() error {
//		return callExternalAPI()
//	})
//
// # Policies
//
// Linear — fixed delay between attempts:
//
//	retry.New(
//		retry.WithPolicy(retry.PolicyLinear),
//		retry.WithDelay(2*time.Second),
//		retry.WithMaxAttempts(5),
//	)
//	// delays: 2s, 2s, 2s, 2s (4 waits between 5 attempts)
//
// Backoff — delay doubles on each attempt (delay * 2^(attempt-1)). Use
// [WithMaxDelay] to bound the exponential growth:
//
//	retry.New(
//		retry.WithPolicy(retry.PolicyBackoff),
//		retry.WithDelay(time.Second),
//		retry.WithMaxDelay(10*time.Second),
//		retry.WithMaxAttempts(7),
//	)
//	// delays: 1s, 2s, 4s, 8s, 10s, 10s (6 waits between 7 attempts; capped at 10s)
//
// Infinite — retries until context cancellation or [ErrExit]:
//
//	retry.New(
//		retry.WithPolicy(retry.PolicyInfinite),
//		retry.WithDelay(5*time.Second),
//		retry.WithContext(ctx),
//	)
//
// # Early Exit
//
// Return [ErrExit] from the retried function to stop retries immediately,
// regardless of the remaining attempts:
//
//	err := r.Do(func() error {
//		resp, err := client.Do(req)
//		if err != nil {
//			return err // will retry
//		}
//		if resp.StatusCode == http.StatusForbidden {
//			return retry.ErrExit // stop immediately, non-retryable
//		}
//		return nil
//	})
//
// # Error Handling
//
// When all attempts are exhausted (for [PolicyLinear] and [PolicyBackoff]),
// [Do] returns an [*Error] that wraps the last error returned by the function.
// The original error stays reachable through the standard helpers:
//
//	err := r.Do(doWork)
//
//	// match the original cause
//	if errors.Is(err, sql.ErrNoRows) { ... }
//
//	// read the retry metadata (policy, attempts) and the cause
//	if rerr, ok := errors.AsType[*retry.Error](err); ok {
//		log.Printf("policy=%s attempts=%d cause=%v", rerr.Policy, rerr.Attempts, rerr.Err)
//	}
//
// [PolicyInfinite] never returns an [*Error]: it returns nil on success, the
// context error on cancellation, or the [ErrExit]-wrapping error on early exit.
// An error wrapping [ErrExit] is likewise returned unchanged by all policies.
//
// # Context Cancellation
//
// When a context is configured with [WithContext], cancellation interrupts the
// wait between attempts for every policy (not just [PolicyInfinite]); [Do] then
// returns the context error. Without a context the wait is a plain sleep that
// cannot be cancelled.
//
// # Callbacks
//
// Optional callbacks are invoked on each failure or on final success. The
// onFailed callback also fires for an attempt that returns [ErrExit]:
//
//	retry.New(
//		retry.WithOnFailedFn(func() {
//			metrics.RetryFailed.Inc()
//		}),
//		retry.WithOnSuccessFn(func() {
//			metrics.RetrySucceeded.Inc()
//		}),
//	)
//
// # Chainable Setters
//
// All options are also available as chainable setter methods on [Retry]:
//
//	r := retry.New().
//		SetMaxAttempts(3).
//		SetPolicy(retry.PolicyBackoff).
//		SetDelay(time.Second)
//
// # Options
//
//   - [WithContext] — set context (required for [PolicyInfinite]; enables cancellation for all policies)
//   - [WithLogger] — set a logger for retry progress messages
//   - [WithMaxAttempts] — maximum number of attempts (default: 5; must be >= 1)
//   - [WithPolicy] — retry strategy (default: [PolicyBackoff])
//   - [WithDelay] — initial delay between attempts (default: 1s)
//   - [WithMaxDelay] — upper bound for the delay (default: 0, no cap)
//   - [WithOnFailedFn] — callback invoked on each failed attempt
//   - [WithOnSuccessFn] — callback invoked on success
package retry
