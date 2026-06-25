package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func okAction(_ context.Context, _ DataAccessor) error { return nil }

// --- Error types ---

func TestErrCompensationFailed_Message(t *testing.T) {
	cf := &ErrCompensationFailed{
		Original:     errors.New("forward boom"),
		Compensation: errors.New("rollback boom"),
	}
	assert.Contains(t, cf.Error(), "compensation failed")
	assert.Contains(t, cf.Error(), "forward boom")
	assert.Contains(t, cf.Error(), "rollback boom")
	require.Len(t, cf.Unwrap(), 2)
}

func TestErrVersionMismatch_Message(t *testing.T) {
	vm := &ErrVersionMismatch{PipelineName: "p", StateVersion: 1, PipelineVersion: 3, MinResumeVersion: 2}
	assert.Contains(t, vm.Error(), "version mismatch")
	assert.Contains(t, vm.Error(), "[2, 3]")
}

// --- Compensation failure path ---

func TestCompensationFailure(t *testing.T) {
	p := &Pipeline{
		Name: "comp_fail",
		Steps: []Step{
			Action("a", okAction, WithCompensate(func(_ context.Context, _ DataAccessor) error {
				return errors.New("rollback boom")
			})),
			Action("b", func(_ context.Context, _ DataAccessor) error {
				return errors.New("forward boom")
			}),
		},
	}

	exec := newTestExecutor(t, nil)
	state, err := exec.Run(t.Context(), p, RunState{})

	var cf *ErrCompensationFailed
	require.True(t, errors.As(err, &cf), "must return ErrCompensationFailed")
	assert.Contains(t, cf.Error(), "rollback boom")
	assert.Contains(t, cf.Error(), "forward boom")
	// Compensation did not finish, so the run stays in compensating (resumable).
	assert.Equal(t, RunStatusCompensating, state.Status)
}

func TestCompensationSkipsMissingOrUncompensatedSteps(t *testing.T) {
	var compA int
	p := &Pipeline{
		Name: "comp_skip",
		Steps: []Step{
			Action("a", okAction, WithCompensate(func(_ context.Context, _ DataAccessor) error {
				compA++
				return nil
			})),
		},
	}

	// Resume a compensating state: one completed step is not in the pipeline
	// anymore (must be skipped with a warning), one has no compensator.
	state := RunState{
		Status: RunStatusCompensating,
		Error:  "boom",
		CompletedSteps: []CompletedStep{
			{Path: []string{"a"}, HasCompensator: true},
			{Path: []string{"gone"}, HasCompensator: true},    // not in pipeline → skip
			{Path: []string{"nocomp"}, HasCompensator: false}, // no compensator → skip
		},
		CompensationIndex: 2,
	}

	exec := newTestExecutor(t, nil)
	out, err := exec.Run(t.Context(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, out.Status)
	assert.Equal(t, 1, compA, "only the real compensator runs")
}

// --- Branch error paths ---

func TestBranchUnknownPath(t *testing.T) {
	p := &Pipeline{
		Name: "branch_unknown",
		Steps: []Step{
			Branch("b", func(_ context.Context, _ DataAccessor) (string, error) {
				return "nope", nil
			}, map[string][]Step{
				"yes": {Action("x", okAction)},
			}),
		},
	}

	exec := newTestExecutor(t, nil)
	state, err := exec.Run(t.Context(), p, RunState{})
	require.NoError(t, err) // step failure compensated → failed state
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Contains(t, state.Error, "unknown path")
	assert.Contains(t, state.Error, "yes", "available paths must be listed")
}

func TestBranchDecideError(t *testing.T) {
	p := &Pipeline{
		Name: "branch_decide_err",
		Steps: []Step{
			Branch("b", func(_ context.Context, _ DataAccessor) (string, error) {
				return "", errors.New("decide boom")
			}, map[string][]Step{"yes": {Action("x", okAction)}}),
		},
	}

	exec := newTestExecutor(t, nil)
	state, err := exec.Run(t.Context(), p, RunState{})
	require.NoError(t, err) // step failure compensated → failed state
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Contains(t, state.Error, "decide")
}

func TestBranchResumeSavedPathNotFound(t *testing.T) {
	p := &Pipeline{
		Name: "branch_resume_missing",
		Steps: []Step{
			Branch("b", func(_ context.Context, _ DataAccessor) (string, error) {
				return "yes", nil
			}, map[string][]Step{"yes": {Action("x", okAction)}}),
		},
	}

	// Resume points inside a path that no longer exists.
	state := RunState{
		Status:      RunStatusRunning,
		CurrentPath: []string{"b", "ghost", "x"},
	}
	exec := newTestExecutor(t, nil)
	out, err := exec.Run(t.Context(), p, state)
	require.NoError(t, err) // surfaced through compensation → failed state
	assert.Equal(t, RunStatusFailed, out.Status)
	assert.Contains(t, out.Error, "saved path")
}

// --- OnEnter error ---

func TestOnEnterError(t *testing.T) {
	p := &Pipeline{
		Name: "on_enter_err",
		Steps: []Step{
			Action("a", okAction, WithOnEnter(func(_ context.Context, _ DataAccessor) error {
				return errors.New("enter boom")
			})),
		},
	}
	exec := newTestExecutor(t, nil)
	state, err := exec.Run(t.Context(), p, RunState{})
	require.NoError(t, err) // failure is compensated and surfaced via state
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Contains(t, state.Error, "on_enter")
}

func TestBranchOnEnterHookFires(t *testing.T) {
	var entered bool
	p := &Pipeline{
		Name: "branch_on_enter",
		Steps: []Step{
			Branch("b", func(_ context.Context, _ DataAccessor) (string, error) { return "yes", nil },
				map[string][]Step{"yes": {Action("x", okAction)}},
				WithOnEnter(func(_ context.Context, _ DataAccessor) error { entered = true; return nil }),
			),
		},
	}
	exec := newTestExecutor(t, nil)
	_, err := exec.Run(t.Context(), p, RunState{})
	require.NoError(t, err)
	assert.True(t, entered, "branch OnEnter must fire")
}

// --- Poll Check error ---

func TestPollCheckError(t *testing.T) {
	p := &Pipeline{
		Name: "poll_err",
		Steps: []Step{
			Poll("p", func(_ context.Context, _ DataAccessor) (bool, time.Duration, error) {
				return false, 0, errors.New("poll boom")
			}),
		},
	}
	exec := newTestExecutor(t, nil)
	// A step error triggers compensation; with no compensators it completes, so
	// Run reports success at the engine level and the failure lands in state.
	state, err := exec.Run(t.Context(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Contains(t, state.Error, "poll boom")
}

// --- Retry: backoff and context cancellation ---

func TestRetryWithBackoff(t *testing.T) {
	var attempts int
	p := &Pipeline{
		Name: "retry_backoff",
		Steps: []Step{
			Action("a", func(_ context.Context, _ DataAccessor) error {
				attempts++
				if attempts < 3 {
					return errors.New("transient")
				}
				return nil
			}, WithRetry(5, time.Millisecond, true)),
		},
	}
	exec := newTestExecutor(t, nil)
	state, err := exec.Run(t.Context(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, 3, attempts)
}

func TestRetryContextCancelledDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var attempts int
	p := &Pipeline{
		Name: "retry_cancel",
		Steps: []Step{
			Action("a", func(_ context.Context, _ DataAccessor) error {
				attempts++
				return errors.New("always fails")
			}, WithRetry(10, time.Hour, false)),
		},
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	exec := newTestExecutor(t, nil)
	state, err := exec.Run(ctx, p, RunState{})

	// Cancellation must abort the retry wait promptly...
	assert.Less(t, time.Since(start), 5*time.Second, "cancellation must abort the retry wait")
	assert.Equal(t, 1, attempts, "only the first attempt runs before cancellation interrupts the wait")
	// ...and return cleanly without rolling back — the run stays resumable, not
	// terminally failed (bug: a cancel mid-retry used to trigger compensation).
	require.ErrorIs(t, err, context.Canceled)
	assert.NotEqual(t, RunStatusFailed, state.Status, "cancellation must not compensate/terminate the run")
}

func TestCancellationDuringStepIsResumable(t *testing.T) {
	var calls [2]int
	firstRun := true
	ctx, cancel := context.WithCancel(t.Context())

	p := &Pipeline{
		Name: "cancel_resumable",
		Steps: []Step{
			Action("a", func(_ context.Context, _ DataAccessor) error { calls[0]++; return nil },
				WithCompensate(func(_ context.Context, _ DataAccessor) error {
					t.Error("compensation must NOT run on cancellation")
					return nil
				})),
			Action("b", func(c context.Context, _ DataAccessor) error {
				calls[1]++
				if firstRun {
					firstRun = false
					cancel()       // cancel while inside step b, before it returns
					return c.Err() // c is the cancelled context on the first run
				}
				return nil
			}),
		},
	}

	exec := newTestExecutor(t, nil)
	state, err := exec.Run(ctx, p, RunState{})
	require.ErrorIs(t, err, context.Canceled)
	require.NotEqual(t, RunStatusFailed, state.Status)
	require.Equal(t, []string{"b"}, state.CurrentPath, "resume position is the interrupted step")

	// Resume on a fresh context: completed step a must not re-run; b resumes.
	calls = [2]int{}
	exec2 := newTestExecutor(t, nil)
	out, err := exec2.Run(t.Context(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, out.Status)
	assert.Equal(t, 0, calls[0], "completed step a must not re-run")
	assert.Equal(t, 1, calls[1], "step b resumes")
}

// --- findTopLevelIndex: unknown resume step restarts from the beginning ---

func TestResumeUnknownStepRestartsFromStart(t *testing.T) {
	var calls int
	p := &Pipeline{
		Name: "unknown_resume",
		Steps: []Step{
			Action("a", func(_ context.Context, _ DataAccessor) error { calls++; return nil }),
		},
	}
	// A deeper unknown path also exercises the isPathPrefix mismatch branch.
	state := RunState{Status: RunStatusRunning, CurrentPath: []string{"ghost", "sub"}}
	exec := newTestExecutor(t, nil)
	out, err := exec.Run(t.Context(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, out.Status)
	assert.Equal(t, 1, calls)
}

// --- GetData and dataStore ---

func TestGetDataAllPaths(t *testing.T) {
	ds := newDataStore()

	ds.Set("n", 42)
	n, err := GetData[int](ds, "n")
	require.NoError(t, err)
	assert.Equal(t, 42, n)

	_, err = GetData[string](ds, "n")
	require.Error(t, err, "wrong type must fail")

	_, err = GetData[int](ds, "missing")
	require.Error(t, err, "missing key must fail")

	ds.Set("j", json.RawMessage(`123`))
	j, err := GetData[int](ds, "j")
	require.NoError(t, err)
	assert.Equal(t, 123, j)

	ds.Set("bad", json.RawMessage(`{not json`))
	_, err = GetData[int](ds, "bad")
	require.Error(t, err, "invalid JSON must fail")

	assert.Len(t, ds.All(), 3)
}

func TestMarshalDataError(t *testing.T) {
	p := &Pipeline{
		Name: "marshal_err",
		Steps: []Step{
			Action("a", func(_ context.Context, data DataAccessor) error {
				data.Set("ch", make(chan int)) // channels are not JSON-serializable
				return nil
			}),
		},
	}
	exec := newTestExecutor(t, nil)
	_, err := exec.Run(t.Context(), p, RunState{})
	require.Error(t, err, "unmarshalable data must surface a marshal error")
	assert.Contains(t, err.Error(), "marshal")
}

// --- Validation ---

func TestValidationErrors(t *testing.T) {
	mr := func(v int) *int { return &v }

	cases := []struct {
		name string
		p    *Pipeline
	}{
		{"empty name", &Pipeline{Steps: []Step{Action("a", okAction)}}},
		{"negative version", &Pipeline{Name: "p", Version: -1, Steps: []Step{Action("a", okAction)}}},
		{"negative min resume", &Pipeline{Name: "p", MinResumeVersion: mr(-1), Steps: []Step{Action("a", okAction)}}},
		{"min resume exceeds version", &Pipeline{Name: "p", Version: 1, MinResumeVersion: mr(2), Steps: []Step{Action("a", okAction)}}},
		{"no steps", &Pipeline{Name: "p"}},
		{"step without name", &Pipeline{Name: "p", Steps: []Step{{Action: &ActionStep{Do: okAction}}}}},
		{"step with no type", &Pipeline{Name: "p", Steps: []Step{{Name: "a"}}}},
		{"step with two types", &Pipeline{Name: "p", Steps: []Step{{
			Name:   "a",
			Action: &ActionStep{Do: okAction},
			Poll:   &PollStep{Check: func(context.Context, DataAccessor) (bool, time.Duration, error) { return true, 0, nil }},
		}}}},
		{"action nil Do", &Pipeline{Name: "p", Steps: []Step{{Name: "a", Action: &ActionStep{}}}}},
		{"poll nil Check", &Pipeline{Name: "p", Steps: []Step{{Name: "a", Poll: &PollStep{}}}}},
		{"branch nil Decide", &Pipeline{Name: "p", Steps: []Step{{Name: "a", Branch: &BranchStep{Paths: map[string][]Step{"x": {Action("y", okAction)}}}}}}},
		{"branch no paths", &Pipeline{Name: "p", Steps: []Step{Branch("a", func(context.Context, DataAccessor) (string, error) { return "x", nil }, map[string][]Step{})}}},
		{"duplicate names", &Pipeline{Name: "p", Steps: []Step{Action("a", okAction), Action("a", okAction)}}},
		{"invalid nested branch step", &Pipeline{Name: "p", Steps: []Step{
			Branch("a", func(context.Context, DataAccessor) (string, error) { return "x", nil }, map[string][]Step{
				"x": {{Name: "bad"}}, // no type
			}),
		}}},
	}

	exec := newTestExecutor(t, nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.Run(t.Context(), tc.p, RunState{})
			require.Error(t, err)
		})
	}
}

// --- Snapshot errors must be logged but never abort the run ---

func TestSnapshotErrorsAreNonFatal(t *testing.T) {
	exec := NewExecutor(
		WithLogger(&testLogger{t: t}),
		WithSnapshotFn(func(_ context.Context, _ RunState) error {
			return errors.New("snapshot unavailable")
		}),
	)

	// Success path: action + completion snapshots both fail, run still completes.
	okPipe := &Pipeline{
		Name:  "snap_ok",
		Steps: []Step{Action("a", okAction)},
	}
	state, err := exec.Run(t.Context(), okPipe, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)

	// Failure path: failure + compensation snapshots fail, compensation still runs.
	var comp int
	failPipe := &Pipeline{
		Name: "snap_fail",
		Steps: []Step{
			Action("a", okAction, WithCompensate(func(_ context.Context, _ DataAccessor) error {
				comp++
				return nil
			})),
			Action("b", func(_ context.Context, _ DataAccessor) error { return errors.New("boom") }),
		},
	}
	state, err = exec.Run(t.Context(), failPipe, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Equal(t, 1, comp, "compensation runs despite snapshot errors")
}

// --- findStepByPath edge cases via compensation in a branch ---

func TestCompensationInBranchFindsNestedStep(t *testing.T) {
	var compInner int
	p := &Pipeline{
		Name: "branch_comp",
		Steps: []Step{
			Branch("b", func(_ context.Context, _ DataAccessor) (string, error) { return "p", nil },
				map[string][]Step{
					"p": {
						Action("inner", okAction, WithCompensate(func(_ context.Context, _ DataAccessor) error {
							compInner++
							return nil
						})),
						Action("boom", func(_ context.Context, _ DataAccessor) error {
							return errors.New("fail")
						}),
					},
				}),
		},
	}
	exec := newTestExecutor(t, nil)
	state, err := exec.Run(t.Context(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Equal(t, 1, compInner, "nested compensator must be found and run")
}
