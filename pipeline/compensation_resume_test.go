package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Reproduces a saga crash-recovery scenario: compensation was interrupted right
// after the index-0 step became the only one left to compensate (the step at
// index 1 was already compensated and persisted with CompensationIndex=0).
// On resume, ONLY the index-0 step must be compensated.
func TestResumeCompensation_AtIndexZeroDoesNotDoubleCompensate(t *testing.T) {
	var comp1, comp2 int

	p := &Pipeline{
		Name: "repro",
		Steps: []Step{
			Action("step1",
				func(_ context.Context, _ DataAccessor) error { return nil },
				WithCompensate(func(_ context.Context, _ DataAccessor) error { comp1++; return nil }),
			),
			Action("step2",
				func(_ context.Context, _ DataAccessor) error { return nil },
				WithCompensate(func(_ context.Context, _ DataAccessor) error { comp2++; return nil }),
			),
		},
	}

	// Persisted state: step2 (index 1) already compensated; only step1 (index 0)
	// remains. This is exactly what runCompensation snapshots after doing index 1.
	state := RunState{
		Status: RunStatusCompensating,
		Error:  "boom",
		CompletedSteps: []CompletedStep{
			{Path: []string{"step1"}, HasCompensator: true},
			{Path: []string{"step2"}, HasCompensator: true},
		},
		CompensationIndex: 0,
	}

	executor := newTestExecutor(t, nil)
	_, err := executor.Run(t.Context(), p, state)
	require.NoError(t, err)

	require.Equal(t, 1, comp1, "step1 (index 0) must be compensated exactly once")
	require.Equal(t, 0, comp2, "step2 was already compensated before the crash; must NOT run again")
}

// CompensationIndex == -1 means everything was already compensated and only the
// finalize-to-failed step remains. Resuming must run NO compensators.
func TestResumeCompensation_AllDoneOnlyFinalizes(t *testing.T) {
	var comp1, comp2 int

	p := &Pipeline{
		Name: "repro2",
		Steps: []Step{
			Action("step1",
				func(_ context.Context, _ DataAccessor) error { return nil },
				WithCompensate(func(_ context.Context, _ DataAccessor) error { comp1++; return nil }),
			),
			Action("step2",
				func(_ context.Context, _ DataAccessor) error { return nil },
				WithCompensate(func(_ context.Context, _ DataAccessor) error { comp2++; return nil }),
			),
		},
	}

	state := RunState{
		Status: RunStatusCompensating,
		Error:  "boom",
		CompletedSteps: []CompletedStep{
			{Path: []string{"step1"}, HasCompensator: true},
			{Path: []string{"step2"}, HasCompensator: true},
		},
		CompensationIndex: -1, // all compensated; only finalization left
	}

	executor := newTestExecutor(t, nil)
	out, err := executor.Run(t.Context(), p, state)
	require.NoError(t, err)
	require.Equal(t, RunStatusFailed, out.Status)
	require.Equal(t, 0, comp1)
	require.Equal(t, 0, comp2)
}
