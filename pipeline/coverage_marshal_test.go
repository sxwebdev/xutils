package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// badJSON fails to marshal, forcing dataStore.marshalData to error so the
// engine's marshal-error branches can be exercised deterministically.
type badJSON struct{}

func (badJSON) MarshalJSON() ([]byte, error) { return nil, errors.New("cannot marshal") }

func setBad(data DataAccessor) { data.Set("bad", badJSON{}) }

// --- marshalData errors on the action/failure paths ---

func TestMarshalError_ActionCompletion(t *testing.T) {
	p := &Pipeline{Name: "m1", Steps: []Step{
		Action("a", func(_ context.Context, d DataAccessor) error { setBad(d); return nil }),
	}}
	_, err := newTestExecutor(t, nil).Run(context.Background(), p, RunState{})
	require.Error(t, err)
}

func TestMarshalError_NoCompensatePath(t *testing.T) {
	p := &Pipeline{Name: "m2", Steps: []Step{
		Action("a", func(_ context.Context, d DataAccessor) error {
			setBad(d)
			return NoCompensate(errors.New("transient"))
		}),
	}}
	_, err := newTestExecutor(t, nil).Run(context.Background(), p, RunState{})
	require.Error(t, err)
}

func TestMarshalError_CancellationPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipeline{Name: "m3", Steps: []Step{
		Action("a", func(c context.Context, d DataAccessor) error {
			setBad(d)
			cancel()
			return c.Err()
		}),
	}}
	_, err := newTestExecutor(t, nil).Run(ctx, p, RunState{})
	require.Error(t, err)
}

func TestMarshalError_FailurePath(t *testing.T) {
	p := &Pipeline{Name: "m4", Steps: []Step{
		Action("a", func(_ context.Context, d DataAccessor) error {
			setBad(d)
			return errors.New("boom")
		}),
	}}
	_, err := newTestExecutor(t, nil).Run(context.Background(), p, RunState{})
	require.Error(t, err)
}

// --- marshalData errors on the poll paths ---

func TestMarshalError_PollSnooze(t *testing.T) {
	p := &Pipeline{Name: "m5", Steps: []Step{
		Poll("p", func(_ context.Context, d DataAccessor) (bool, time.Duration, error) {
			setBad(d)
			return false, time.Millisecond, nil
		}),
	}}
	_, err := newTestExecutor(t, nil).Run(context.Background(), p, RunState{})
	require.Error(t, err)
	_, isSnooze := errors.AsType[ErrSnooze](err)
	require.False(t, isSnooze, "marshal error must surface, not ErrSnooze")
}

func TestMarshalError_PollComplete(t *testing.T) {
	p := &Pipeline{Name: "m6", Steps: []Step{
		Poll("p", func(_ context.Context, d DataAccessor) (bool, time.Duration, error) {
			setBad(d)
			return true, 0, nil
		}),
	}}
	_, err := newTestExecutor(t, nil).Run(context.Background(), p, RunState{})
	require.Error(t, err)
}

// --- marshalData errors on branch / top-of-loop / completion paths ---

func TestMarshalError_RunCompletionViaEmptyBranch(t *testing.T) {
	// A branch with an empty chosen path performs no per-step marshal, so the
	// only marshalData is Run's completion — where badJSON trips it.
	p := &Pipeline{Name: "m7", Steps: []Step{
		Branch("b", func(_ context.Context, d DataAccessor) (string, error) {
			setBad(d)
			return "empty", nil
		}, map[string][]Step{"empty": {}}),
	}}
	_, err := newTestExecutor(t, nil).Run(context.Background(), p, RunState{})
	require.Error(t, err)
}

func TestMarshalError_TopOfLoopCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Branch sets badJSON and cancels; the next top-level step's loop iteration
	// hits the cancellation marshal branch.
	p := &Pipeline{Name: "m8", Steps: []Step{
		Branch("b", func(_ context.Context, d DataAccessor) (string, error) {
			setBad(d)
			cancel()
			return "empty", nil
		}, map[string][]Step{"empty": {}}),
		Action("after", okAction),
	}}
	_, err := newTestExecutor(t, nil).Run(ctx, p, RunState{})
	// The cancellation branch marshals state first; with badJSON that marshal
	// fails, so the marshal error surfaces rather than ctx.Err().
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal")
}

// --- marshalData errors during compensation ---

func TestMarshalError_CompensationStepFailure(t *testing.T) {
	p := &Pipeline{Name: "m9", Steps: []Step{
		Action("a", okAction, WithCompensate(func(_ context.Context, d DataAccessor) error {
			setBad(d)
			return errors.New("rollback boom")
		})),
	}}
	state := RunState{
		Status:            RunStatusCompensating,
		Error:             "orig",
		CompletedSteps:    []CompletedStep{{Path: []string{"a"}, HasCompensator: true}},
		CompensationIndex: 0,
	}
	_, err := newTestExecutor(t, nil).Run(context.Background(), p, state)
	require.Error(t, err)
}

func TestMarshalError_CompensationStepSuccess(t *testing.T) {
	p := &Pipeline{Name: "m10", Steps: []Step{
		Action("a", okAction, WithCompensate(func(_ context.Context, d DataAccessor) error {
			setBad(d)
			return nil
		})),
	}}
	state := RunState{
		Status:            RunStatusCompensating,
		Error:             "orig",
		CompletedSteps:    []CompletedStep{{Path: []string{"a"}, HasCompensator: true}},
		CompensationIndex: 0,
	}
	_, err := newTestExecutor(t, nil).Run(context.Background(), p, state)
	require.Error(t, err)
}

// --- snapshot errors on poll and compensation-failure paths ---

func TestSnapshotError_PollSnooze(t *testing.T) {
	exec := NewExecutor(
		WithLogger(&testLogger{t: t}),
		WithSnapshotFn(func(_ context.Context, _ RunState) error { return errors.New("snap fail") }),
	)
	p := &Pipeline{Name: "s_snooze", Steps: []Step{
		Poll("p", func(_ context.Context, _ DataAccessor) (bool, time.Duration, error) {
			return false, time.Millisecond, nil
		}),
	}}
	_, err := exec.Run(context.Background(), p, RunState{})
	_, isSnooze := errors.AsType[ErrSnooze](err)
	require.True(t, isSnooze, "snapshot error is logged, snooze still returned")
}

func TestSnapshotError_PollComplete(t *testing.T) {
	exec := NewExecutor(
		WithLogger(&testLogger{t: t}),
		WithSnapshotFn(func(_ context.Context, _ RunState) error { return errors.New("snap fail") }),
	)
	p := &Pipeline{Name: "s_complete", Steps: []Step{
		Poll("p", func(_ context.Context, _ DataAccessor) (bool, time.Duration, error) {
			return true, 0, nil
		}),
	}}
	state, err := exec.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
}

func TestSnapshotError_CompensationStepFailure(t *testing.T) {
	exec := NewExecutor(
		WithLogger(&testLogger{t: t}),
		WithSnapshotFn(func(_ context.Context, _ RunState) error { return errors.New("snap fail") }),
	)
	p := &Pipeline{Name: "s_comp_fail", Steps: []Step{
		Action("a", okAction, WithCompensate(func(_ context.Context, _ DataAccessor) error {
			return errors.New("rollback boom")
		})),
		Action("b", func(_ context.Context, _ DataAccessor) error { return errors.New("boom") }),
	}}
	state, err := exec.Run(context.Background(), p, RunState{})
	var cf *ErrCompensationFailed
	require.True(t, errors.As(err, &cf))
	assert.Equal(t, RunStatusCompensating, state.Status)
}

// --- Defensive branches unreachable through Run, exercised directly ---

func TestExecuteStep_UnknownType(t *testing.T) {
	// validate() rejects typeless steps, so this default arm is unreachable via
	// Run; call executeStep directly to confirm the guard returns an error.
	e := NewExecutor()
	_, err := e.executeStep(context.Background(), &Pipeline{Name: "p"}, RunState{}, newDataStore(),
		&Step{Name: "x"}, []string{"x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no action")
}

func TestRunCompensation_FinalMarshalError(t *testing.T) {
	// The final marshalData cannot fail through Run (persisted data is always
	// valid RawMessage); call runCompensation directly with unmarshalable data
	// to confirm the guard surfaces the error instead of swallowing it.
	e := NewExecutor()
	ds := newDataStore()
	ds.Set("bad", badJSON{})
	state := RunState{
		Status:            RunStatusCompensating,
		CompletedSteps:    []CompletedStep{{Path: []string{"a"}, HasCompensator: false}},
		CompensationIndex: 0,
	}
	_, err := e.runCompensation(context.Background(), &Pipeline{Name: "p"}, state, ds)
	require.Error(t, err)
}

// --- snapshot error on the NoCompensate path ---

func TestSnapshotError_NoCompensate(t *testing.T) {
	exec := NewExecutor(
		WithLogger(&testLogger{t: t}),
		WithSnapshotFn(func(_ context.Context, _ RunState) error { return errors.New("snap fail") }),
	)
	p := &Pipeline{Name: "s_nocomp", Steps: []Step{
		Action("a", func(_ context.Context, _ DataAccessor) error {
			return NoCompensate(errors.New("transient"))
		}),
	}}
	_, err := exec.Run(context.Background(), p, RunState{})
	require.ErrorIs(t, err, ErrNoCompensate, "NoCompensate propagates despite snapshot error")
}

// --- findStepByPath: malformed completed paths during compensation ---

func TestCompensationMalformedPaths(t *testing.T) {
	var compReal int
	p := &Pipeline{Name: "comp_malformed", Steps: []Step{
		Action("real", okAction, WithCompensate(func(_ context.Context, _ DataAccessor) error {
			compReal++
			return nil
		})),
		Branch("b", func(_ context.Context, _ DataAccessor) (string, error) { return "x", nil },
			map[string][]Step{"x": {Action("inner", okAction)}}),
	}}

	// Completed steps whose paths resolve to nothing — findStepByPath must
	// return nil for each and the walk must skip them:
	//   - []:                       empty path
	//   - ["real","sub"]:           "real" is not a branch, 2-element path has no step
	//   - ["b","ghostkey","inner"]: the branch path key no longer exists
	state := RunState{
		Status: RunStatusCompensating,
		Error:  "orig",
		CompletedSteps: []CompletedStep{
			{Path: []string{"real"}, HasCompensator: true},
			{Path: []string{}, HasCompensator: true},
			{Path: []string{"real", "sub"}, HasCompensator: true},
			{Path: []string{"b", "ghostkey", "inner"}, HasCompensator: true},
		},
		CompensationIndex: 3,
	}
	out, err := newTestExecutor(t, nil).Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, out.Status)
	assert.Equal(t, 1, compReal, "real compensator runs; unresolvable paths are skipped")
}
