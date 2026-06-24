package pipeline

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// The attempt-count retry tests (TestRetry, TestRetryWithBackoff) prove the loop
// runs the right number of times but never observe the delay between attempts —
// so a regression that drops the wait entirely, or stops doubling it, passes
// them green. These tests pin the actual timing in both directions: each gap
// must be neither too short (delay skipped) nor too long (grew when it
// shouldn't). synctest's fake clock makes the gaps exact, so equality is safe
// and there are no real sleeps to make the test slow or flaky.

// gapsFor runs an action that always fails under the given retry config and
// returns the fake-clock gap observed before each attempt after the first.
func gapsFor(t *testing.T, retry StepOption, attempts int) []time.Duration {
	t.Helper()

	var gaps []time.Duration
	var last time.Time
	first := true

	p := &Pipeline{
		Name: "retry_timing",
		Steps: []Step{
			Action("flaky", func(_ context.Context, _ DataAccessor) error {
				now := time.Now() // fake clock inside the synctest bubble
				if !first {
					gaps = append(gaps, now.Sub(last))
				}
				first = false
				last = now
				return errors.New("always fails")
			}, retry),
		},
	}

	// Compensation runs (none here) → engine returns nil, failure lands in state.
	if _, err := newTestExecutor(t, nil).Run(t.Context(), p, RunState{}); err != nil {
		t.Fatalf("unexpected engine error: %v", err)
	}
	if len(gaps) != attempts-1 {
		t.Fatalf("got %d gaps, want %d (attempts=%d)", len(gaps), attempts-1, attempts)
	}
	return gaps
}

func TestRetryDelayConstantWhenNoBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 10 * time.Millisecond
		gaps := gapsFor(t, WithRetry(4, delay, false), 4)

		// Every gap is exactly the initial delay — neither skipped (would be 0)
		// nor growing (would catch backoff leaking into the no-backoff path).
		for i, g := range gaps {
			if g != delay {
				t.Errorf("gap %d = %s, want exactly %s", i, g, delay)
			}
		}
	})
}

func TestRetryDelayDoublesWithBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const initial = 10 * time.Millisecond
		gaps := gapsFor(t, WithRetry(4, initial, true), 4)

		// Gaps must be exactly initial, 2×, 4× — proving backoff both applies
		// (gap grows) and doubles precisely (not, say, +initial each time).
		want := []time.Duration{initial, 2 * initial, 4 * initial}
		for i, g := range gaps {
			if g != want[i] {
				t.Errorf("gap %d = %s, want exactly %s", i, g, want[i])
			}
		}
	})
}

// The default delay (no InitialDelay set) is one second; confirm it is applied
// exactly, so a regression to the default constant is caught too.
func TestRetryDefaultDelayApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gaps := gapsFor(t, WithRetry(2, 0, false), 2)
		if gaps[0] != time.Second {
			t.Errorf("default gap = %s, want exactly %s", gaps[0], time.Second)
		}
	})
}
