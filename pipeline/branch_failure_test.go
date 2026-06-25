package pipeline_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/sxwebdev/xutils/pipeline"
)

// TestBranchChildFailureStopsParentScope proves that when a step nested inside a
// branch fails and the pipeline fully compensates, the executor must NOT keep
// running subsequent steps in the PARENT scope.
func TestBranchChildFailureStopsParentScope(t *testing.T) {
	var cRan int

	p := &pipeline.Pipeline{
		Name: "nested-branch",
		Steps: []pipeline.Step{
			pipeline.Action("a", func(ctx context.Context, d pipeline.DataAccessor) error { return nil }),
			pipeline.Branch("b",
				func(ctx context.Context, d pipeline.DataAccessor) (string, error) { return "go", nil },
				map[string][]pipeline.Step{
					"go": {
						pipeline.Action("child-fail", func(ctx context.Context, d pipeline.DataAccessor) error {
							return errors.New("boom")
						}),
					},
				},
			),
			pipeline.Action("c", func(ctx context.Context, d pipeline.DataAccessor) error {
				cRan++
				return nil
			}),
		},
	}

	ex := pipeline.NewExecutor()
	st, err := ex.Run(t.Context(), p, pipeline.RunState{})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if st.Status != pipeline.RunStatusFailed {
		t.Fatalf("expected status %q, got %q", pipeline.RunStatusFailed, st.Status)
	}
	// Tie the result to the real code path: the failure must have come from the
	// step nested inside the branch, otherwise cRan==0 could pass via an
	// unrelated route (e.g. a refactor where the branch took the wrong path).
	if want := []string{"b", "go", "child-fail"}; !slices.Equal(st.FailedStepPath, want) {
		t.Fatalf("FailedStepPath = %v, want %v (failure must originate in the nested branch step)", st.FailedStepPath, want)
	}
	if cRan != 0 {
		t.Fatalf("step C ran %d time(s) after the pipeline already failed and compensated; it must not run", cRan)
	}
}

// TestBranchCompensationFailureNoDoubleCompensate proves that when a step nested
// in a branch fails AND a compensator also fails, the parent scope does not
// restart compensation from the top: each compensator must run exactly once and
// the original ErrCompensationFailed must propagate unchanged (resumable).
func TestBranchCompensationFailureNoDoubleCompensate(t *testing.T) {
	compErr := errors.New("comp boom")
	var compA int

	p := &pipeline.Pipeline{
		Name: "nested-branch-comp-fail",
		Steps: []pipeline.Step{
			pipeline.Action("a",
				func(ctx context.Context, d pipeline.DataAccessor) error { return nil },
				pipeline.WithCompensate(func(ctx context.Context, d pipeline.DataAccessor) error {
					compA++
					return compErr
				}),
			),
			pipeline.Branch("b",
				func(ctx context.Context, d pipeline.DataAccessor) (string, error) { return "go", nil },
				map[string][]pipeline.Step{
					"go": {
						pipeline.Action("child-fail", func(ctx context.Context, d pipeline.DataAccessor) error {
							return errors.New("boom")
						}),
					},
				},
			),
		},
	}

	ex := pipeline.NewExecutor()
	st, err := ex.Run(t.Context(), p, pipeline.RunState{})

	var compFailed *pipeline.ErrCompensationFailed
	if !errors.As(err, &compFailed) {
		t.Fatalf("expected *ErrCompensationFailed, got %T: %v", err, err)
	}
	if !errors.Is(err, compErr) {
		t.Fatalf("compensation error must wrap the failing compensator's error")
	}
	if compA != 1 {
		t.Fatalf("compensator for step A ran %d time(s); must run exactly once (no double compensation)", compA)
	}
	// The run is left mid-compensation so the caller can retry it.
	if st.Status != pipeline.RunStatusCompensating {
		t.Fatalf("expected status %q (resumable), got %q", pipeline.RunStatusCompensating, st.Status)
	}
}

// TestNestedNoCompensateKeepsRealFailedPath proves that an ErrNoCompensate from
// a step nested in a branch is propagated with the real (deep) FailedStepPath,
// and recorded by exactly one snapshot — parent scopes must not re-attribute the
// failure to the branch step or snapshot it again per level.
func TestNestedNoCompensateKeepsRealFailedPath(t *testing.T) {
	var snaps []pipeline.RunState

	p := &pipeline.Pipeline{
		Name: "nested-nocompensate",
		Steps: []pipeline.Step{
			pipeline.Branch("b",
				func(ctx context.Context, d pipeline.DataAccessor) (string, error) { return "go", nil },
				map[string][]pipeline.Step{
					"go": {
						pipeline.Action("child-nc", func(ctx context.Context, d pipeline.DataAccessor) error {
							return pipeline.NoCompensate(errors.New("retry me"))
						}),
					},
				},
			),
		},
	}

	ex := pipeline.NewExecutor(pipeline.WithSnapshotFn(func(ctx context.Context, st pipeline.RunState) error {
		snaps = append(snaps, st)
		return nil
	}))
	st, err := ex.Run(t.Context(), p, pipeline.RunState{})

	if !errors.Is(err, pipeline.ErrNoCompensate) {
		t.Fatalf("expected error wrapping ErrNoCompensate, got %T: %v", err, err)
	}

	want := []string{"b", "go", "child-nc"}
	if !slices.Equal(st.FailedStepPath, want) {
		t.Fatalf("FailedStepPath = %v, want %v (the real failing step, not the branch)", st.FailedStepPath, want)
	}

	// Exactly one snapshot records the failure (the deepest scope that owns it).
	var failSnaps int
	for _, s := range snaps {
		if len(s.FailedStepPath) == 0 {
			continue
		}
		failSnaps++
		if !slices.Equal(s.FailedStepPath, want) {
			t.Errorf("a snapshot recorded FailedStepPath %v, want %v", s.FailedStepPath, want)
		}
	}
	if failSnaps != 1 {
		t.Fatalf("got %d snapshots recording the failure, want exactly 1 (parent scopes must not re-snapshot it)", failSnaps)
	}
}
