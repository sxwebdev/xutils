package pipeline_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sxwebdev/xutils/pipeline"
)

func TestSnapshotFailureOnInitialTransitionPreventsCallbacks(t *testing.T) {
	cause := errors.New("storage down")
	var actions int
	p := &pipeline.Pipeline{Name: "initial_snapshot", Steps: []pipeline.Step{
		pipeline.Action("effect", func(context.Context, pipeline.DataAccessor) error { actions++; return nil }),
	}}

	state, err := pipeline.NewExecutor(pipeline.WithSnapshotFn(func(context.Context, pipeline.RunState) error {
		return cause
	})).Run(t.Context(), p, pipeline.RunState{})
	snapshotErr, ok := errors.AsType[*pipeline.ErrSnapshotFailed](err)
	if !ok || !errors.Is(err, cause) {
		t.Fatalf("error = %T %v, want snapshot failure wrapping cause", err, err)
	}
	if actions != 0 {
		t.Fatalf("actions = %d, want 0", actions)
	}
	if state.Status != pipeline.RunStatusRunning {
		t.Fatalf("returned status = %q, want updated running state", state.Status)
	}
	if snapshotErr.Operation == "" || snapshotErr.Error() == "" {
		t.Fatalf("snapshot error lacks diagnostics: %+v", snapshotErr)
	}
}

func TestSnapshotFailureAfterCompensatorStopsNextCompensator(t *testing.T) {
	cause := errors.New("storage down")
	var compensated []string
	p := &pipeline.Pipeline{
		Name: "comp_snapshot",
		Steps: []pipeline.Step{
			pipeline.Action("a", func(context.Context, pipeline.DataAccessor) error { return nil },
				pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error {
					compensated = append(compensated, "a")
					return nil
				})),
			pipeline.Action("b", func(context.Context, pipeline.DataAccessor) error { return nil },
				pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error {
					compensated = append(compensated, "b")
					return nil
				})),
			pipeline.Action("fail", func(context.Context, pipeline.DataAccessor) error { return errors.New("boom") }),
		},
	}

	executor := pipeline.NewExecutor(pipeline.WithSnapshotFn(func(_ context.Context, state pipeline.RunState) error {
		if state.Status == pipeline.RunStatusCompensating && state.CompensationIndex == 0 {
			return cause
		}
		return nil
	}))
	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if _, ok := errors.AsType[*pipeline.ErrSnapshotFailed](err); !ok || !errors.Is(err, cause) {
		t.Fatalf("error = %T %v, want snapshot failure", err, err)
	}
	if got, want := compensated, []string{"b"}; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("compensated = %v, want %v", got, want)
	}
	if state.CompensationIndex != 0 {
		t.Fatalf("compensation index = %d, want 0", state.CompensationIndex)
	}
}

func TestSnapshotFailureBeforeCompensationPreventsCompensator(t *testing.T) {
	cause := errors.New("storage down")
	var compensated int
	p := &pipeline.Pipeline{
		Name: "failure_snapshot",
		Steps: []pipeline.Step{
			pipeline.Action("a", func(context.Context, pipeline.DataAccessor) error { return nil },
				pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error { compensated++; return nil })),
			pipeline.Action("fail", func(context.Context, pipeline.DataAccessor) error { return errors.New("boom") }),
		},
	}
	executor := pipeline.NewExecutor(pipeline.WithSnapshotFn(func(_ context.Context, state pipeline.RunState) error {
		if state.Status == pipeline.RunStatusCompensating {
			return cause
		}
		return nil
	}))

	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if _, ok := errors.AsType[*pipeline.ErrSnapshotFailed](err); !ok || !errors.Is(err, cause) {
		t.Fatalf("error = %T %v, want snapshot failure", err, err)
	}
	if compensated != 0 {
		t.Fatalf("compensations = %d, want 0", compensated)
	}
	if state.Status != pipeline.RunStatusCompensating {
		t.Fatalf("status = %q, want compensating", state.Status)
	}
}

func TestCompensatorCanRequestRetryAfter(t *testing.T) {
	cause := errors.New("cleanup dependency busy")
	var attempts int
	p := &pipeline.Pipeline{
		Name: "deferred_compensation",
		Steps: []pipeline.Step{
			pipeline.Action("a", func(context.Context, pipeline.DataAccessor) error { return nil },
				pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error {
					attempts++
					if attempts == 1 {
						return pipeline.RetryAfter(0, cause)
					}
					return nil
				})),
			pipeline.Action("fail", func(context.Context, pipeline.DataAccessor) error { return errors.New("boom") }),
		},
	}
	executor := pipeline.NewExecutor()

	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok || !errors.Is(err, cause) {
		t.Fatalf("error = %T %v, want retry signal wrapping cause", err, err)
	}
	if state.Status != pipeline.RunStatusCompensating || state.CompensationIndex != 0 {
		t.Fatalf("state = status %q/index %d, want compensating/0", state.Status, state.CompensationIndex)
	}

	state, err = executor.Run(t.Context(), p, state)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if state.Status != pipeline.RunStatusFailed || attempts != 2 {
		t.Fatalf("final status/attempts = (%q, %d), want (failed, 2)", state.Status, attempts)
	}
}

func TestSnapshotFailureOnCompensationCompletionResumesWithoutRecompensating(t *testing.T) {
	cause := errors.New("storage down")
	var compensated int
	var lastDurable pipeline.RunState
	p := &pipeline.Pipeline{
		Name: "compensation_completion_snapshot",
		Steps: []pipeline.Step{
			pipeline.Action("a", func(context.Context, pipeline.DataAccessor) error { return nil },
				pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error { compensated++; return nil })),
			pipeline.Action("fail", func(context.Context, pipeline.DataAccessor) error { return errors.New("boom") }),
		},
	}
	executor := pipeline.NewExecutor(pipeline.WithSnapshotFn(func(_ context.Context, state pipeline.RunState) error {
		if state.Status == pipeline.RunStatusFailed {
			return cause
		}
		lastDurable = state
		return nil
	}))

	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if _, ok := errors.AsType[*pipeline.ErrSnapshotFailed](err); !ok || !errors.Is(err, cause) {
		t.Fatalf("error = %T %v, want final snapshot failure", err, err)
	}
	if state.Status != pipeline.RunStatusFailed || compensated != 1 {
		t.Fatalf("returned status/compensations = (%q, %d), want (failed, 1)", state.Status, compensated)
	}
	if lastDurable.Status != pipeline.RunStatusCompensating || lastDurable.CompensationIndex != -1 {
		t.Fatalf("durable state = status %q/index %d, want compensating/-1", lastDurable.Status, lastDurable.CompensationIndex)
	}

	state, err = pipeline.NewExecutor().Run(t.Context(), p, lastDurable)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if state.Status != pipeline.RunStatusFailed || compensated != 1 {
		t.Fatalf("resumed status/compensations = (%q, %d), want (failed, 1)", state.Status, compensated)
	}
}

func TestCASSnapshotUsesMonotonicRevision(t *testing.T) {
	var expected, revisions []uint64
	executor := pipeline.NewExecutor(pipeline.WithCASSnapshotFn(func(_ context.Context, want uint64, state pipeline.RunState) error {
		expected = append(expected, want)
		revisions = append(revisions, state.Revision)
		if state.Revision != want+1 {
			return errors.New("non-monotonic revision")
		}
		return nil
	}))
	p := &pipeline.Pipeline{Name: "cas", Steps: []pipeline.Step{
		pipeline.Action("a", func(context.Context, pipeline.DataAccessor) error { return nil }),
	}}

	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if state.Revision != uint64(len(revisions)) {
		t.Fatalf("final revision = %d, snapshots = %d", state.Revision, len(revisions))
	}
	for i := range expected {
		if expected[i] != uint64(i) || revisions[i] != uint64(i+1) {
			t.Fatalf("snapshot %d = expected %d/revision %d", i, expected[i], revisions[i])
		}
	}
}

func TestCASConflictPreventsAction(t *testing.T) {
	conflict := errors.New("revision conflict")
	var actions int
	executor := pipeline.NewExecutor(pipeline.WithCASSnapshotFn(func(context.Context, uint64, pipeline.RunState) error {
		return conflict
	}))
	p := &pipeline.Pipeline{Name: "cas_conflict", Steps: []pipeline.Step{
		pipeline.Action("effect", func(context.Context, pipeline.DataAccessor) error { actions++; return nil }),
	}}

	_, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if !errors.Is(err, conflict) {
		t.Fatalf("error = %v, want conflict", err)
	}
	if actions != 0 {
		t.Fatalf("actions = %d, want 0", actions)
	}
}

func TestCASConflictOnRunningStatePreventsAction(t *testing.T) {
	conflict := errors.New("revision conflict")
	var actions int
	p := &pipeline.Pipeline{Name: "cas_running", Steps: []pipeline.Step{
		pipeline.Action("effect", func(context.Context, pipeline.DataAccessor) error { actions++; return nil }),
	}}
	state := pipeline.RunState{Status: pipeline.RunStatusRunning, Revision: 7}
	executor := pipeline.NewExecutor(pipeline.WithCASSnapshotFn(func(_ context.Context, expected uint64, candidate pipeline.RunState) error {
		if expected != 7 || candidate.Revision != 8 {
			t.Fatalf("CAS arguments = %d/%d, want 7/8", expected, candidate.Revision)
		}
		return conflict
	}))

	_, err := executor.Run(t.Context(), p, state)
	if !errors.Is(err, conflict) {
		t.Fatalf("error = %v, want conflict", err)
	}
	if actions != 0 {
		t.Fatalf("actions = %d, want 0", actions)
	}
}

func TestCASConflictOnCompensatingStatePreventsCompensator(t *testing.T) {
	conflict := errors.New("revision conflict")
	var compensations int
	p := &pipeline.Pipeline{Name: "cas_compensating", Steps: []pipeline.Step{
		pipeline.Action("effect", func(context.Context, pipeline.DataAccessor) error { return nil },
			pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error { compensations++; return nil })),
	}}
	state := pipeline.RunState{
		Status:            pipeline.RunStatusCompensating,
		Revision:          3,
		CompletedSteps:    []pipeline.CompletedStep{{Path: []string{"effect"}, HasCompensator: true}},
		CompensationIndex: 0,
	}
	executor := pipeline.NewExecutor(pipeline.WithCASSnapshotFn(func(context.Context, uint64, pipeline.RunState) error {
		return conflict
	}))

	_, err := executor.Run(t.Context(), p, state)
	if !errors.Is(err, conflict) {
		t.Fatalf("error = %v, want conflict", err)
	}
	if compensations != 0 {
		t.Fatalf("compensations = %d, want 0", compensations)
	}
}

func TestAlreadyCanceledContextDoesNotSnapshotOrRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var snapshots, actions int
	p := &pipeline.Pipeline{Name: "already_canceled", Steps: []pipeline.Step{
		pipeline.Action("effect", func(context.Context, pipeline.DataAccessor) error { actions++; return nil }),
	}}
	executor := pipeline.NewExecutor(pipeline.WithSnapshotFn(func(context.Context, pipeline.RunState) error {
		snapshots++
		return nil
	}))

	state, err := executor.Run(ctx, p, pipeline.RunState{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if snapshots != 0 || actions != 0 || state.Status != pipeline.RunStatusNew {
		t.Fatalf("snapshots/actions/status = (%d, %d, %q), want (0, 0, new)", snapshots, actions, state.Status)
	}
}
