package loopper_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loopper"
)

func TestNew_PanicsOnNilFn(t *testing.T) {
	require.Panics(t, func() { loopper.New(nil) })
}

func TestLeading_RunsImmediately(t *testing.T) {
	var runs atomic.Int64
	l := loopper.New(func(context.Context) { runs.Add(1) },
		loopper.WithLeading(),
		loopper.WithPeriod(time.Hour), // ticker won't fire during the test
	)
	l.Start(t.Context())
	defer l.Wait()
	defer l.Stop()

	require.Eventually(t, func() bool { return runs.Load() == 1 }, time.Second, 5*time.Millisecond,
		"leading mode must run once immediately, before the first tick")
}

func TestNonLeading_FirstRunAfterPeriod(t *testing.T) {
	var runs atomic.Int64
	l := loopper.New(func(context.Context) { runs.Add(1) },
		loopper.WithPeriod(60*time.Millisecond),
	)
	l.Start(t.Context())
	defer l.Wait()
	defer l.Stop()

	// Must not have run immediately.
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int64(0), runs.Load(), "non-leading must not run before the first period")

	require.Eventually(t, func() bool { return runs.Load() >= 1 }, time.Second, 5*time.Millisecond,
		"must run after the first period elapses")
}

func TestOverlapPrevention(t *testing.T) {
	var concurrent atomic.Int64
	var maxConcurrent atomic.Int64
	release := make(chan struct{})

	l := loopper.New(func(context.Context) {
		c := concurrent.Add(1)
		for {
			if m := maxConcurrent.Load(); c > m {
				if maxConcurrent.CompareAndSwap(m, c) {
					break
				}
				continue
			}
			break
		}
		<-release
		concurrent.Add(-1)
	}, loopper.WithLeading(), loopper.WithPeriod(10*time.Millisecond))

	l.Start(t.Context())

	// While the first (leading) run is blocked, ticks and triggers must be skipped.
	require.Eventually(t, func() bool { return concurrent.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond) // let several ticks fire and be skipped
	assert.False(t, l.Trigger(t.Context()), "Trigger must return false while a run is in progress")

	close(release)
	l.Stop()
	l.Wait()

	assert.Equal(t, int64(1), maxConcurrent.Load(), "no two runs may overlap")
}

func TestTrigger_TrueWhenIdle(t *testing.T) {
	var runs atomic.Int64
	done := make(chan struct{}, 1)
	l := loopper.New(func(context.Context) { runs.Add(1); done <- struct{}{} },
		loopper.WithPeriod(time.Hour),
	)
	l.Start(t.Context())
	defer l.Wait()
	defer l.Stop()

	require.True(t, l.Trigger(t.Context()), "Trigger must return true when idle")
	<-done
	assert.Equal(t, int64(1), runs.Load())
}

func TestPanicRecovery_LoopContinues(t *testing.T) {
	var runs atomic.Int64
	l := loopper.New(func(context.Context) {
		if runs.Add(1) == 1 {
			panic("boom")
		}
	}, loopper.WithLeading(), loopper.WithPeriod(20*time.Millisecond))

	l.Start(t.Context())
	defer l.Wait()
	defer l.Stop()

	require.Eventually(t, func() bool { return runs.Load() >= 2 }, time.Second, 5*time.Millisecond,
		"loop must keep running after a panic is recovered")
}

func TestStop_StopsScheduling(t *testing.T) {
	var runs atomic.Int64
	l := loopper.New(func(context.Context) { runs.Add(1) },
		loopper.WithPeriod(20*time.Millisecond),
	)
	l.Start(t.Context())

	require.Eventually(t, func() bool { return runs.Load() >= 1 }, time.Second, 5*time.Millisecond)

	l.Stop()
	l.Wait()

	after := runs.Load()
	time.Sleep(80 * time.Millisecond) // several periods
	assert.Equal(t, after, runs.Load(), "no runs may be scheduled after Stop")
}

func TestStop_Idempotent(t *testing.T) {
	l := loopper.New(func(context.Context) {}, loopper.WithPeriod(time.Hour))
	l.Start(t.Context())
	l.Stop()
	require.NotPanics(t, l.Stop)
	l.Wait()
}

func TestStop_WithoutStartIsSafe(t *testing.T) {
	l := loopper.New(func(context.Context) {}, loopper.WithPeriod(time.Hour))
	require.NotPanics(t, l.Stop)
	l.Wait()
}

func TestTrigger_AfterStopReturnsFalse(t *testing.T) {
	l := loopper.New(func(context.Context) {}, loopper.WithPeriod(time.Hour))
	l.Start(t.Context())
	l.Stop()
	assert.False(t, l.Trigger(t.Context()), "Trigger after Stop must not run")
	l.Wait()
}

func TestContextTimeout_AppliedToTask(t *testing.T) {
	gotDeadline := make(chan bool, 1)
	l := loopper.New(func(ctx context.Context) {
		_, ok := ctx.Deadline()
		gotDeadline <- ok
	}, loopper.WithLeading(), loopper.WithPeriod(time.Hour), loopper.WithContextTimeout(time.Minute))

	l.Start(t.Context())
	defer l.Wait()
	defer l.Stop()

	assert.True(t, <-gotDeadline, "task context must carry the configured timeout deadline")
}

func TestNoContextTimeout_WhenZero(t *testing.T) {
	gotDeadline := make(chan bool, 1)
	l := loopper.New(func(ctx context.Context) {
		_, ok := ctx.Deadline()
		gotDeadline <- ok
	}, loopper.WithLeading(), loopper.WithPeriod(time.Hour), loopper.WithContextTimeout(0))

	l.Start(t.Context())
	defer l.Wait()
	defer l.Stop()

	assert.False(t, <-gotDeadline, "zero timeout must not impose a deadline")
}

func TestTrigger_BeforeStart(t *testing.T) {
	var runs atomic.Int64
	done := make(chan struct{}, 1)
	l := loopper.New(func(context.Context) { runs.Add(1); done <- struct{}{} },
		loopper.WithPeriod(time.Hour),
	)
	require.True(t, l.Trigger(t.Context()), "Trigger should work before Start")
	<-done
	assert.Equal(t, int64(1), runs.Load())
	l.Wait()
}

// Stress: hammer Trigger while Start/Stop run concurrently. Run with -race to
// surface data races (e.g. WaitGroup Add racing Wait, or overlap violations).
func TestConcurrent_StartTriggerStop(t *testing.T) {
	var maxConcurrent atomic.Int64
	var concurrent atomic.Int64

	l := loopper.New(func(context.Context) {
		c := concurrent.Add(1)
		for {
			m := maxConcurrent.Load()
			if c <= m || maxConcurrent.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		concurrent.Add(-1)
	}, loopper.WithPeriod(time.Millisecond))

	l.Start(t.Context())

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				l.Trigger(t.Context())
			}
		}()
	}
	wg.Wait()

	l.Stop()
	l.Wait()

	assert.LessOrEqual(t, maxConcurrent.Load(), int64(1), "overlap prevention must hold under concurrency")
}

// Regression: Trigger racing Stop+Wait must not panic with
// "WaitGroup is reused before previous Wait has returned". The Add inside
// tryRun must be serialized with Stop so no Add can slip in after Wait starts.
func TestConcurrent_TriggerDuringShutdownNoPanic(t *testing.T) {
	for range 200 {
		l := loopper.New(func(context.Context) { time.Sleep(time.Microsecond) },
			loopper.WithPeriod(time.Millisecond))
		l.Start(t.Context())

		var wg sync.WaitGroup
		stop := make(chan struct{})
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						l.Trigger(context.Background())
					}
				}
			}()
		}

		time.Sleep(time.Millisecond)
		l.Stop()
		l.Wait() // must not panic even while Trigger is hammering concurrently
		close(stop)
		wg.Wait()
	}
}

func TestStart_SecondCallIsNoOp(t *testing.T) {
	var runs atomic.Int64
	l := loopper.New(func(context.Context) { runs.Add(1) },
		loopper.WithLeading(), loopper.WithPeriod(time.Hour))

	l.Start(t.Context())
	l.Start(t.Context()) // second Start must not start a second ticker/leading run
	defer l.Wait()
	defer l.Stop()

	// Exactly one leading run from the single effective Start.
	require.Eventually(t, func() bool { return runs.Load() == 1 }, time.Second, 5*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, int64(1), runs.Load(), "second Start must be a no-op")
}
