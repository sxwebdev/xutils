package workflow_test

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sxwebdev/xutils/workflow"
)

// The existing retry tests pin attempt counts, and TestRetry_BackoffHonorsTimeout
// pins only the FIRST backoff delay. None observe the delay between later
// attempts, so a regression that stops RunWithBackoff doubling (degrading it to a
// linear policy) — or one that lets RunWithLinear grow its delay — passes green.
// These tests pin every inter-attempt gap in both directions using synctest's
// fake clock, so equality is exact and there are no real sleeps.

// retryGaps runs policy on an always-failing step and returns the fake-clock gaps
// observed before each attempt after the first.
func retryGaps(t *testing.T, policy workflow.RetryPolicyFn, timeout time.Duration, maxRetries int) []time.Duration {
	t.Helper()

	var gaps []time.Duration
	var last time.Time
	first := true

	step, _ := workflow.NewStep("s", func(_ *workflow.StepContext) error {
		now := time.Now() // fake clock inside the synctest bubble
		if !first {
			gaps = append(gaps, now.Sub(last))
		}
		first = false
		last = now
		return errors.New("always fails")
	}, workflow.WithStepRetryPolicy(policy), workflow.WithStepMaxRetries(maxRetries), workflow.WithStepTimeout(timeout))

	sc := &workflow.StepContext{Context: t.Context(), Workflow: workflow.New(), Step: step}
	if err := policy(sc); err == nil {
		t.Fatal("always-failing step must return an error")
	}
	if len(gaps) != maxRetries-1 {
		t.Fatalf("got %d gaps, want %d (maxRetries=%d)", len(gaps), maxRetries-1, maxRetries)
	}
	return gaps
}

func TestRunWithBackoff_DelaysDouble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 10 * time.Millisecond
		gaps := retryGaps(t, workflow.RunWithBackoff, timeout, 4)

		// Exactly timeout, 2×, 4× — proves the delay both grows and doubles
		// precisely; catches a regression to constant (linear) backoff.
		want := []time.Duration{timeout, 2 * timeout, 4 * timeout}
		for i, g := range gaps {
			if g != want[i] {
				t.Errorf("backoff gap %d = %s, want exactly %s", i, g, want[i])
			}
		}
	})
}

func TestRunWithLinear_DelaysConstant(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 10 * time.Millisecond
		gaps := retryGaps(t, workflow.RunWithLinear, timeout, 4)

		// Every gap is exactly the step timeout — neither skipped (0) nor growing
		// (which would mean backoff leaked into the linear path).
		for i, g := range gaps {
			if g != timeout {
				t.Errorf("linear gap %d = %s, want exactly %s", i, g, timeout)
			}
		}
	})
}
