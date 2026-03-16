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
//	// delays: 2s, 2s, 2s, 2s, 2s
//
// Backoff — delay doubles on each attempt (delay * 2^(attempt-1)):
//
//	retry.New(
//		retry.WithPolicy(retry.PolicyBackoff),
//		retry.WithDelay(time.Second),
//		retry.WithMaxAttempts(5),
//	)
//	// delays: 1s, 2s, 4s, 8s, 16s
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
// # Callbacks
//
// Optional callbacks are invoked on each failure or on final success:
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
//   - [WithContext] — set context (required for [PolicyInfinite])
//   - [WithLogger] — set a structured logger
//   - [WithMaxAttempts] — maximum number of attempts (default: 5)
//   - [WithPolicy] — retry strategy (default: [PolicyBackoff])
//   - [WithDelay] — initial delay between attempts (default: 1s)
//   - [WithOnFailedFn] — callback invoked on each failed attempt
//   - [WithOnSuccessFn] — callback invoked on success
package retry
