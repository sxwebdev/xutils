package retry_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loggerutil"
	"github.com/sxwebdev/xutils/retry"
)

// retryablePolicies are the bounded policies that share the attempt-based loop.
var retryablePolicies = []retry.Policy{retry.PolicyLinear, retry.PolicyBackoff}

func newLogger() loggerutil.Logger { return loggerutil.NewTestLogger() }

func TestRetry_SucceedsImmediately(t *testing.T) {
	for _, policy := range retryablePolicies {
		t.Run(policy.String(), func(t *testing.T) {
			var calls int
			err := retry.New(
				retry.WithPolicy(policy),
				retry.WithMaxAttempts(3),
				retry.WithDelay(time.Millisecond),
			).SetLogger(newLogger()).Do(func() error {
				calls++
				return nil
			})

			require.NoError(t, err)
			require.Equal(t, 1, calls, "fn must be called exactly once on immediate success")
		})
	}
}

func TestRetry_SucceedsAfterFailures(t *testing.T) {
	for _, policy := range retryablePolicies {
		t.Run(policy.String(), func(t *testing.T) {
			var calls int
			err := retry.New(
				retry.WithPolicy(policy),
				retry.WithMaxAttempts(5),
				retry.WithDelay(time.Millisecond),
			).SetLogger(newLogger()).Do(func() error {
				calls++
				if calls < 3 {
					return retry.ErrRetry
				}
				return nil
			})

			require.NoError(t, err)
			// Must stop the instant fn succeeds — not earlier, not later.
			require.Equal(t, 3, calls)
		})
	}
}

func TestRetry_ExhaustsAllAttempts(t *testing.T) {
	cases := []struct {
		policy      retry.Policy
		maxAttempts int
		wantMsg     string
	}{
		{retry.PolicyLinear, 3, "linear retry failed after 3 attempts: boom"},
		{retry.PolicyLinear, 5, "linear retry failed after 5 attempts: boom"},
		{retry.PolicyBackoff, 3, "backoff retry failed after 3 attempts: boom"},
		{retry.PolicyBackoff, 5, "backoff retry failed after 5 attempts: boom"},
	}

	for _, tc := range cases {
		t.Run(tc.wantMsg, func(t *testing.T) {
			cause := errors.New("boom")

			var calls int
			err := retry.New(
				retry.WithPolicy(tc.policy),
				retry.WithMaxAttempts(tc.maxAttempts),
				retry.WithDelay(time.Millisecond),
			).SetLogger(newLogger()).Do(func() error {
				calls++
				return cause
			})

			// The real bug-catcher: fn must run exactly maxAttempts times
			// (independent of what the returned *Error reports).
			require.Equal(t, tc.maxAttempts, calls, "fn must run exactly maxAttempts times")

			rerr, ok := errors.AsType[*retry.Error](err)
			require.True(t, ok)
			require.Equal(t, tc.policy, rerr.Policy)
			require.Equal(t, tc.maxAttempts, rerr.Attempts)
			require.Equal(t, cause, rerr.Err)         // original cause preserved
			require.ErrorIs(t, err, cause)            // and reachable via Is
			require.Equal(t, tc.wantMsg, err.Error()) // human-readable format
		})
	}
}

func TestRetry_ErrExitStopsImmediately(t *testing.T) {
	for _, policy := range retryablePolicies {
		t.Run(policy.String(), func(t *testing.T) {
			exit := errors.New("non-retryable")

			var calls int
			err := retry.New(
				retry.WithPolicy(policy),
				retry.WithMaxAttempts(5),
				retry.WithDelay(time.Millisecond),
			).SetLogger(newLogger()).Do(func() error {
				calls++
				if calls == 2 {
					return errors.Join(retry.ErrExit, exit)
				}
				return retry.ErrRetry
			})

			// Stops on attempt 2 — does not exhaust the remaining 3 attempts.
			require.Equal(t, 2, calls)
			require.ErrorIs(t, err, retry.ErrExit)
			require.ErrorIs(t, err, exit) // the wrapping error is returned as-is

			// ErrExit short-circuits the loop, so it is NOT wrapped in *retry.Error.
			_, ok := errors.AsType[*retry.Error](err)
			require.False(t, ok)
		})
	}
}

func TestRetry_NilFunction(t *testing.T) {
	err := retry.New().SetLogger(newLogger()).Do(nil)
	require.Error(t, err)
}

func TestRetry_UnsupportedPolicy(t *testing.T) {
	var calls int
	err := retry.New(retry.WithPolicy(retry.Policy(99))).SetLogger(newLogger()).Do(func() error {
		calls++
		return nil
	})
	require.Error(t, err)
	require.Equal(t, 0, calls, "unsupported policy must not invoke fn")
}

func TestRetry_MaxAttemptsBelowOne(t *testing.T) {
	for _, policy := range retryablePolicies {
		for _, n := range []int{0, -1} {
			t.Run(fmt.Sprintf("%s/n=%d", policy, n), func(t *testing.T) {
				var calls int
				err := retry.New(
					retry.WithPolicy(policy),
					retry.WithMaxAttempts(n),
				).SetLogger(newLogger()).Do(func() error {
					calls++
					return nil
				})

				require.Error(t, err)
				require.Equal(t, 0, calls, "invalid maxAttempts must reject before calling fn")
			})
		}
	}
}

func TestRetry_Callbacks(t *testing.T) {
	t.Run("failures then success", func(t *testing.T) {
		var failed, succeeded, calls int
		err := retry.New(
			retry.WithPolicy(retry.PolicyLinear),
			retry.WithMaxAttempts(5),
			retry.WithDelay(time.Millisecond),
			retry.WithOnFailedFn(func() { failed++ }),
			retry.WithOnSuccessFn(func() { succeeded++ }),
		).SetLogger(newLogger()).Do(func() error {
			calls++
			if calls < 3 {
				return retry.ErrRetry
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 2, failed)    // one per failed attempt
		require.Equal(t, 1, succeeded) // exactly once on success
	})

	t.Run("all attempts fail", func(t *testing.T) {
		var failed, succeeded int
		err := retry.New(
			retry.WithPolicy(retry.PolicyBackoff),
			retry.WithMaxAttempts(3),
			retry.WithDelay(time.Millisecond),
			retry.WithOnFailedFn(func() { failed++ }),
			retry.WithOnSuccessFn(func() { succeeded++ }),
		).SetLogger(newLogger()).Do(func() error {
			return retry.ErrRetry
		})

		require.Error(t, err)
		require.Equal(t, 3, failed)    // one per attempt
		require.Equal(t, 0, succeeded) // never on failure
	})

	t.Run("onFailed fires for the ErrExit attempt", func(t *testing.T) {
		var failed int
		err := retry.New(
			retry.WithPolicy(retry.PolicyLinear),
			retry.WithMaxAttempts(5),
			retry.WithDelay(time.Millisecond),
			retry.WithOnFailedFn(func() { failed++ }),
		).SetLogger(newLogger()).Do(func() error {
			return retry.ErrExit
		})

		require.ErrorIs(t, err, retry.ErrExit)
		require.Equal(t, 1, failed)
	})
}

func TestRetry_ContextCancellation(t *testing.T) {
	t.Run("already cancelled before first attempt, every policy", func(t *testing.T) {
		policies := []retry.Policy{retry.PolicyLinear, retry.PolicyBackoff, retry.PolicyInfinite}
		for _, policy := range policies {
			t.Run(policy.String(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel() // cancelled up front

				var calls int
				err := retry.New(
					retry.WithPolicy(policy),
					retry.WithContext(ctx),
					retry.WithMaxAttempts(3),
					retry.WithDelay(time.Hour),
				).SetLogger(newLogger()).Do(func() error {
					calls++
					return retry.ErrRetry
				})

				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 0, calls, "fn must not run when ctx is already cancelled")
			})
		}
	})

	t.Run("cancellation interrupts the linear wait", func(t *testing.T) {
		// Deterministic with a fake clock: the wait is one hour, the cancel
		// fires at 50ms, so Do must abort at exactly 50ms — both bounds pinned.
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			go func() {
				time.Sleep(50 * time.Millisecond) // let the first attempt enter the wait
				cancel()
			}()

			var calls int
			base := time.Now()
			err := retry.New(
				retry.WithPolicy(retry.PolicyLinear),
				retry.WithMaxAttempts(10),
				retry.WithDelay(time.Hour),
				retry.WithContext(ctx),
			).SetLogger(newLogger()).Do(func() error {
				calls++
				return retry.ErrRetry
			})
			elapsed := time.Since(base)

			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, calls)                     // failed once, then cancelled mid-wait
			require.Equal(t, 50*time.Millisecond, elapsed) // aborted exactly at cancel, not the full hour
		})
	})

	t.Run("cancellation interrupts the backoff wait", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			t.Cleanup(cancel)

			var calls int
			base := time.Now()
			err := retry.New(
				retry.WithPolicy(retry.PolicyBackoff),
				retry.WithMaxAttempts(10),
				retry.WithDelay(time.Hour),
				retry.WithContext(ctx),
			).SetLogger(newLogger()).Do(func() error {
				calls++
				return retry.ErrRetry
			})
			elapsed := time.Since(base)

			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, 1, calls)
			require.Equal(t, 50*time.Millisecond, elapsed) // aborted exactly at the deadline
		})
	})
}

func TestRetry_InfinitePolicy(t *testing.T) {
	t.Run("requires a context", func(t *testing.T) {
		var calls int
		err := retry.New(retry.WithPolicy(retry.PolicyInfinite)).SetLogger(newLogger()).Do(func() error {
			calls++
			return nil
		})
		require.Error(t, err)
		require.Equal(t, 0, calls)
	})

	t.Run("retries past maxAttempts until success", func(t *testing.T) {
		var calls int
		err := retry.New(
			retry.WithPolicy(retry.PolicyInfinite),
			retry.WithMaxAttempts(2), // ignored by the infinite policy
			retry.WithContext(t.Context()),
			retry.WithDelay(time.Millisecond),
		).SetLogger(newLogger()).Do(func() error {
			calls++
			if calls < 5 {
				return retry.ErrRetry
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 5, calls, "must keep retrying beyond maxAttempts")
	})

	t.Run("stops on ErrExit", func(t *testing.T) {
		var calls, failed int
		err := retry.New(
			retry.WithPolicy(retry.PolicyInfinite),
			retry.WithContext(t.Context()),
			retry.WithDelay(time.Millisecond),
			retry.WithOnFailedFn(func() { failed++ }),
		).SetLogger(newLogger()).Do(func() error {
			calls++
			if calls == 3 {
				return retry.ErrExit
			}
			return retry.ErrRetry
		})

		require.ErrorIs(t, err, retry.ErrExit)
		require.Equal(t, 3, calls)
		require.Equal(t, 3, failed) // onFailed fires for every failed attempt incl. the ErrExit one
	})

	t.Run("stops on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		err := retry.New(
			retry.WithPolicy(retry.PolicyInfinite),
			retry.WithContext(ctx),
			retry.WithDelay(time.Millisecond),
		).SetLogger(newLogger()).Do(func() error {
			return retry.ErrRetry
		})

		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestRetry_ChainableSetters(t *testing.T) {
	var failed, succeeded int
	r := retry.New().
		SetLogger(newLogger()).
		SetPolicy(retry.PolicyLinear).
		SetMaxAttempts(2).
		SetDelay(time.Millisecond).
		SetMaxDelay(2 * time.Millisecond).
		SetContext(t.Context()).
		SetOnFailedFn(func() { failed++ }).
		SetOnSuccessFn(func() { succeeded++ })

	err := r.Do(func() error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 0, failed)
	require.Equal(t, 1, succeeded)
}

func TestPolicy_String(t *testing.T) {
	require.Equal(t, "linear", retry.PolicyLinear.String())
	require.Equal(t, "backoff", retry.PolicyBackoff.String())
	require.Equal(t, "infinite", retry.PolicyInfinite.String())
	require.Equal(t, "unknown", retry.Policy(99).String())
}

func TestPolicy_Validate(t *testing.T) {
	require.NoError(t, retry.PolicyLinear.Validate())
	require.NoError(t, retry.PolicyBackoff.Validate())
	require.NoError(t, retry.PolicyInfinite.Validate())
	require.Error(t, retry.Policy(99).Validate())
}
