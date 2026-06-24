package retry_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/retry"
)

// recordGaps drives a always-failing fn under a fake clock and returns the gap
// between each attempt (gaps[i] = time from attempt i to attempt i+1), i.e. the
// actual delay applied before each retry. With synctest the clock only advances
// when every goroutine is blocked, so the gaps are exact, not approximate.
func recordGaps(t *testing.T, r *retry.Retry, attempts int) []time.Duration {
	t.Helper()
	var stamps []time.Duration
	base := time.Now()
	err := r.Do(func() error {
		stamps = append(stamps, time.Since(base))
		return retry.ErrRetry
	})
	require.Error(t, err)
	require.Len(t, stamps, attempts, "fn must run exactly maxAttempts times")

	gaps := make([]time.Duration, len(stamps)-1)
	for i := 1; i < len(stamps); i++ {
		gaps[i-1] = stamps[i] - stamps[i-1]
	}
	return gaps
}

// TestTiming_LinearDelayIsConstant pins the linear progression exactly: every
// gap between attempts must equal the configured delay. An exact-equality check
// is a both-directions bound — it fails if a wait is dropped (too little) and if
// a wait grows like backoff (too much).
func TestTiming_LinearDelayIsConstant(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 10 * time.Millisecond
		gaps := recordGaps(t, retry.New(
			retry.WithPolicy(retry.PolicyLinear),
			retry.WithMaxAttempts(4),
			retry.WithDelay(delay),
		).SetLogger(newLogger()), 4)

		require.Equal(t, []time.Duration{delay, delay, delay}, gaps)
	})
}

// TestTiming_BackoffDelayDoubles pins the exponential progression exactly:
// delay * 2^(attempt-1). Exact equality is a both-directions bound — it catches
// a non-doubling delay (stayed linear) and an over-growing one (e.g. tripled).
func TestTiming_BackoffDelayDoubles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const base = 5 * time.Millisecond
		gaps := recordGaps(t, retry.New(
			retry.WithPolicy(retry.PolicyBackoff),
			retry.WithMaxAttempts(5),
			retry.WithDelay(base),
		).SetLogger(newLogger()), 5)

		// attempts 1..4 wait base*1, base*2, base*4, base*8.
		require.Equal(t, []time.Duration{base, 2 * base, 4 * base, 8 * base}, gaps)
	})
}

// TestTiming_MaxDelayCapsBackoff pins that the cap holds exactly: the delay
// doubles until it would exceed maxDelay, then stays clamped at maxDelay for
// every subsequent wait.
func TestTiming_MaxDelayCapsBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			base = 2 * time.Millisecond
			cap_ = 5 * time.Millisecond
		)
		gaps := recordGaps(t, retry.New(
			retry.WithPolicy(retry.PolicyBackoff),
			retry.WithMaxAttempts(6),
			retry.WithDelay(base),
			retry.WithMaxDelay(cap_),
		).SetLogger(newLogger()), 6)

		// raw doubling would be 2,4,8,16,32; capped at 5 → 2,4,5,5,5.
		require.Equal(t, []time.Duration{2 * time.Millisecond, 4 * time.Millisecond, cap_, cap_, cap_}, gaps)
	})
}

// TestTiming_BackoffOverflowFallsBackToMaxDelay covers the overflow guard when
// maxDelay is set: a base near the int64 limit makes the shift overflow to a
// non-positive duration, and the guard must return maxDelay rather than sleep
// for a negative/huge duration.
func TestTiming_BackoffOverflowFallsBackToMaxDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const cap_ = time.Millisecond
		// delay 1<<62: attempt 1 → 1<<62 (ok, but > cap → capped to cap_);
		// attempt 2 → 1<<63 overflow (<=0) → guard returns maxDelay = cap_;
		// attempt 3 (shift 2) also overflows → cap_.
		gaps := recordGaps(t, retry.New(
			retry.WithPolicy(retry.PolicyBackoff),
			retry.WithMaxAttempts(4),
			retry.WithDelay(1<<62),
			retry.WithMaxDelay(cap_),
		).SetLogger(newLogger()), 4)

		require.Equal(t, []time.Duration{cap_, cap_, cap_}, gaps)
	})
}

// TestTiming_NegativeDelayNoMaxDelay covers the no-maxDelay branch of the
// overflow/non-positive guard in backoffDelay (`return r.delay`). A negative
// configured delay makes `delay <= 0` true on the very first attempt; with no
// maxDelay set the guard returns r.delay as-is, and a non-positive sleep returns
// immediately. (The true integer-overflow path — a base near 1<<62 that
// overflows when shifted — cannot be exercised: with the real clock it would
// sleep for ~centuries, and under a synctest fake clock the cumulative bubble
// time overflows int64 and crashes the runtime. A negative delay reaches the
// same branch cheaply and deterministically.)
func TestTiming_NegativeDelayNoMaxDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gaps := recordGaps(t, retry.New(
			retry.WithPolicy(retry.PolicyBackoff),
			retry.WithMaxAttempts(3),
			retry.WithDelay(-time.Hour), // <= 0 → guard returns r.delay, sleep is a no-op
			retry.WithLogger(newLogger()),
		), 3)

		// A non-positive sleep returns immediately → no real time passes.
		require.Equal(t, []time.Duration{0, 0}, gaps)
	})
}

// TestTiming_ZeroDelay verifies a zero configured delay produces zero gaps for
// both bounded policies (no accidental minimum wait is injected).
func TestTiming_ZeroDelay(t *testing.T) {
	for _, policy := range retryablePolicies {
		t.Run(policy.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gaps := recordGaps(t, retry.New(
					retry.WithPolicy(policy),
					retry.WithMaxAttempts(3),
					retry.WithDelay(0),
				).SetLogger(newLogger()), 3)

				require.Equal(t, []time.Duration{0, 0}, gaps)
			})
		})
	}
}

// TestTiming_InfiniteDelayIsConstant pins the infinite policy's constant delay
// between attempts before a context cancellation stops it.
func TestTiming_InfiniteDelayIsConstant(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 7 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var stamps []time.Duration
		base := time.Now()
		err := retry.New(
			retry.WithPolicy(retry.PolicyInfinite),
			retry.WithContext(ctx),
			retry.WithDelay(delay),
		).SetLogger(newLogger()).Do(func() error {
			stamps = append(stamps, time.Since(base))
			if len(stamps) == 4 {
				cancel() // stop after the 4th attempt
			}
			return retry.ErrRetry
		})

		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, stamps, 4)
		for i := 1; i < len(stamps); i++ {
			require.Equal(t, delay, stamps[i]-stamps[i-1], "infinite gaps must be constant")
		}
	})
}
