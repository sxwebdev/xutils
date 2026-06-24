package loopper_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loopper"
)

// These tests use testing/synctest (Go 1.26) so the ticker fires on a fake clock
// that only advances when every goroutine is durably blocked. That makes the
// iteration count fully deterministic, letting us assert BOTH a lower and an
// upper bound on the number of runs over a window — catching "fired too
// early/often" and "didn't fire / too slow" alike. As a bonus, synctest.Test
// reports a deadlock if any goroutine is still alive when the bubble returns, so
// every test here also proves the ticker goroutine exits (no leak) after Wait.

// Non-leading: the task fires exactly once per elapsed period, never before, and
// the count matches the number of periods elapsed (no more, no less).
func TestTiming_NonLeading_OnePerPeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs atomic.Int64
		l := loopper.New(func(context.Context) { runs.Add(1) },
			loopper.WithPeriod(time.Second))
		l.Start(t.Context())

		// Before the first period elapses there must be zero runs (not leading).
		synctest.Wait()
		require.Equal(t, int64(0), runs.Load(), "non-leading must not run before the first period")

		// Just shy of one period: still nothing.
		time.Sleep(999 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int64(0), runs.Load(), "must not fire before a full period has elapsed")

		// Cross the first period boundary: exactly one run.
		time.Sleep(time.Millisecond)
		synctest.Wait()
		require.Equal(t, int64(1), runs.Load(), "exactly one run after the first full period")

		// Five more whole periods: exactly five more runs, no drift.
		time.Sleep(5 * time.Second)
		synctest.Wait()
		require.Equal(t, int64(6), runs.Load(), "exactly one run per elapsed period (no extra, no missed)")

		l.Stop()
		l.Wait()
	})
}

// Leading: one immediate run on Start, then one per period thereafter.
func TestTiming_Leading_ImmediatePlusPerPeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs atomic.Int64
		l := loopper.New(func(context.Context) { runs.Add(1) },
			loopper.WithLeading(), loopper.WithPeriod(time.Second))
		l.Start(t.Context())

		synctest.Wait()
		require.Equal(t, int64(1), runs.Load(), "leading must run exactly once immediately on Start")

		time.Sleep(3 * time.Second)
		synctest.Wait()
		require.Equal(t, int64(4), runs.Load(), "leading run plus one run per elapsed period")

		l.Stop()
		l.Wait()
	})
}

// Overlap/skip with deterministic timing: a task that outlasts several periods
// must cause those intervening ticks to be skipped (count stays at 1), then a
// fresh run starts once the long one finishes and a later tick fires.
func TestTiming_LongTaskSkipsIntermediateTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs atomic.Int64
		var maxConcurrent atomic.Int64
		var concurrent atomic.Int64

		l := loopper.New(func(context.Context) {
			runs.Add(1)
			c := concurrent.Add(1)
			for {
				m := maxConcurrent.Load()
				if c <= m || maxConcurrent.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(2500 * time.Millisecond) // spans the t=1s and t=2s ticks
			concurrent.Add(-1)
		}, loopper.WithLeading(), loopper.WithPeriod(time.Second), loopper.WithContextTimeout(0))
		l.Start(t.Context())

		// Leading run is in flight; the 1s and 2s ticks land while it runs and
		// must be skipped — so still exactly one run after 2.5 fake seconds.
		synctest.Wait()
		require.Equal(t, int64(1), runs.Load(), "leading run in progress")

		time.Sleep(2500 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int64(1), runs.Load(), "ticks during a long run must be skipped, not queued")

		// The first task has finished; the next whole tick starts run #2.
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, int64(2), runs.Load(), "a new run starts on the next tick after the long one ends")

		l.Stop()
		l.Wait()

		assert.Equal(t, int64(1), maxConcurrent.Load(), "no two runs may overlap")
	})
}

// Stop mid-period freezes the count: after Stop+Wait no further runs occur even
// as the fake clock advances many periods.
func TestTiming_StopHaltsScheduling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs atomic.Int64
		l := loopper.New(func(context.Context) { runs.Add(1) },
			loopper.WithPeriod(time.Second))
		l.Start(t.Context())

		time.Sleep(3 * time.Second)
		synctest.Wait()
		require.Equal(t, int64(3), runs.Load())

		l.Stop()
		l.Wait()
		frozen := runs.Load()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.Equal(t, frozen, runs.Load(), "no runs may be scheduled after Stop")
	})
}

// Parent-context cancellation (without Stop) must halt scheduling and let the
// ticker goroutine exit, exactly like Stop.
func TestTiming_ParentCancelHaltsScheduling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		var runs atomic.Int64
		l := loopper.New(func(context.Context) { runs.Add(1) },
			loopper.WithPeriod(time.Second))
		l.Start(ctx)

		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.Equal(t, int64(2), runs.Load())

		cancel()
		l.Wait() // ticker goroutine must observe ctx.Done and exit
		frozen := runs.Load()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.Equal(t, frozen, runs.Load(), "parent cancel must stop scheduling new runs")
	})
}
