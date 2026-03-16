package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test helpers ---

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debugf(format string, args ...any) { l.t.Logf("[DEBUG] "+format, args...) }
func (l *testLogger) Debugw(format string, args ...any) { l.t.Logf("[DEBUG] "+format, args...) }
func (l *testLogger) Infof(format string, args ...any)  { l.t.Logf("[INFO]  "+format, args...) }
func (l *testLogger) Infow(format string, args ...any)  { l.t.Logf("[INFO]  "+format, args...) }
func (l *testLogger) Warnf(format string, args ...any)  { l.t.Logf("[WARN]  "+format, args...) }
func (l *testLogger) Warnw(format string, args ...any)  { l.t.Logf("[WARN]  "+format, args...) }
func (l *testLogger) Errorf(format string, args ...any) { l.t.Logf("[ERROR] "+format, args...) }
func (l *testLogger) Errorw(format string, args ...any) { l.t.Logf("[ERROR] "+format, args...) }

func newTestExecutor(t *testing.T, snapshots *[]RunState) *Executor {
	return NewExecutor(
		WithLogger(&testLogger{t: t}),
		WithDebug(true),
		WithSnapshotFn(func(_ context.Context, state RunState) error {
			if snapshots != nil {
				*snapshots = append(*snapshots, state)
			}
			return nil
		}),
	)
}

// --- Tests ---

func TestLinearPipeline(t *testing.T) {
	var callOrder []string

	p := &Pipeline{
		Name: "linear",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error {
				callOrder = append(callOrder, "step1")
				return nil
			}),
			Action("step2", func(_ context.Context, _ DataAccessor) error {
				callOrder = append(callOrder, "step2")
				return nil
			}),
			Action("step3", func(_ context.Context, _ DataAccessor) error {
				callOrder = append(callOrder, "step3")
				return nil
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})

	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, []string{"step1", "step2", "step3"}, callOrder)
	assert.Len(t, state.CompletedSteps, 3)
}

func TestPollWithSnooze(t *testing.T) {
	pollCount := 0

	p := &Pipeline{
		Name: "poll_test",
		Steps: []Step{
			Action("setup", func(_ context.Context, _ DataAccessor) error {
				return nil
			}),
			Poll("wait", func(_ context.Context, _ DataAccessor) (bool, time.Duration, error) {
				pollCount++
				if pollCount >= 3 {
					return true, 0, nil
				}
				return false, 100 * time.Millisecond, nil
			}),
			Action("finish", func(_ context.Context, _ DataAccessor) error {
				return nil
			}),
		},
	}

	executor := newTestExecutor(t, nil)

	// First run — poll returns not done.
	state, err := executor.Run(context.Background(), p, RunState{})
	require.Error(t, err)
	snooze, ok := errors.AsType[ErrSnooze](err)
	require.True(t, ok)
	assert.Equal(t, 100*time.Millisecond, snooze.Duration)
	assert.Equal(t, RunStatusPolling, state.Status)
	assert.Equal(t, 1, pollCount)

	// Second run — resume, poll returns not done again.
	state, err = executor.Run(context.Background(), p, state)
	require.Error(t, err)
	_, ok = errors.AsType[ErrSnooze](err)
	require.True(t, ok)
	assert.Equal(t, 2, pollCount)

	// Third run — resume, poll returns done, pipeline completes.
	state, err = executor.Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, 3, pollCount)
}

func TestBranch(t *testing.T) {
	var executedSteps []string

	p := &Pipeline{
		Name: "branch_test",
		Steps: []Step{
			Action("init", func(_ context.Context, data DataAccessor) error {
				executedSteps = append(executedSteps, "init")
				data.Set("mode", "fast")
				return nil
			}),
			Branch("decide", func(_ context.Context, data DataAccessor) (string, error) {
				mode, _ := data.Get("mode")
				return mode.(string), nil
			}, map[string][]Step{
				"fast": {
					Action("fast_action", func(_ context.Context, _ DataAccessor) error {
						executedSteps = append(executedSteps, "fast_action")
						return nil
					}),
				},
				"slow": {
					Action("slow_action1", func(_ context.Context, _ DataAccessor) error {
						executedSteps = append(executedSteps, "slow_action1")
						return nil
					}),
					Action("slow_action2", func(_ context.Context, _ DataAccessor) error {
						executedSteps = append(executedSteps, "slow_action2")
						return nil
					}),
				},
			}),
			Action("finish", func(_ context.Context, _ DataAccessor) error {
				executedSteps = append(executedSteps, "finish")
				return nil
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})

	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, []string{"init", "fast_action", "finish"}, executedSteps)
}

func TestCompensation(t *testing.T) {
	var executedSteps []string

	p := &Pipeline{
		Name: "compensation_test",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error {
				executedSteps = append(executedSteps, "step1")
				return nil
			}, WithCompensate(func(_ context.Context, _ DataAccessor) error {
				executedSteps = append(executedSteps, "compensate_step1")
				return nil
			})),
			Action("step2", func(_ context.Context, _ DataAccessor) error {
				executedSteps = append(executedSteps, "step2")
				return nil
			}, WithCompensate(func(_ context.Context, _ DataAccessor) error {
				executedSteps = append(executedSteps, "compensate_step2")
				return nil
			})),
			Action("step3_fails", func(_ context.Context, _ DataAccessor) error {
				executedSteps = append(executedSteps, "step3_fails")
				return fmt.Errorf("something went wrong")
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})

	require.NoError(t, err) // Compensation completed, pipeline is failed but no engine error.
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Contains(t, state.Error, "something went wrong")
	assert.Equal(t, []string{
		"step1", "step2", "step3_fails",
		"compensate_step2", "compensate_step1", // Reverse order.
	}, executedSteps)
}

func TestResumeAfterCrash(t *testing.T) {
	var callCounts [3]int

	p := &Pipeline{
		Name: "resume_test",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error {
				callCounts[0]++
				return nil
			}),
			Action("step2", func(_ context.Context, _ DataAccessor) error {
				callCounts[1]++
				return nil
			}),
			Action("step3", func(_ context.Context, _ DataAccessor) error {
				callCounts[2]++
				return nil
			}),
		},
	}

	// Simulate: steps 1 and 2 completed, then crash.
	savedState := RunState{
		Status:      RunStatusRunning,
		CurrentPath: []string{"step3"},
		CompletedSteps: []CompletedStep{
			{Path: []string{"step1"}, HasCompensator: false},
			{Path: []string{"step2"}, HasCompensator: false},
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, savedState)

	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	// Steps 1 and 2 should NOT have been called again.
	assert.Equal(t, 0, callCounts[0], "step1 should not be re-executed")
	assert.Equal(t, 0, callCounts[1], "step2 should not be re-executed")
	assert.Equal(t, 1, callCounts[2], "step3 should execute once")
}

func TestIdempotency_CompletedPipelineNoOp(t *testing.T) {
	callCount := 0

	p := &Pipeline{
		Name: "idempotency_test",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error {
				callCount++
				return nil
			}),
		},
	}

	executor := newTestExecutor(t, nil)

	// Run to completion.
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, 1, callCount)

	// Run again with completed state — should be no-op.
	state, err = executor.Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, 1, callCount) // Still 1, not called again.
}

func TestIdempotency_FailedPipelineNoOp(t *testing.T) {
	p := &Pipeline{
		Name: "failed_noop_test",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	executor := newTestExecutor(t, nil)

	// Already failed state.
	state := RunState{Status: RunStatusFailed, Error: "previous error"}
	state, err := executor.Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, state.Status)
}

func TestDataPassingBetweenSteps(t *testing.T) {
	p := &Pipeline{
		Name: "data_test",
		Steps: []Step{
			Action("producer", func(_ context.Context, data DataAccessor) error {
				data.Set("tx_hash", "0xabc123")
				data.Set("amount", 42.5)
				return nil
			}),
			Action("consumer", func(_ context.Context, data DataAccessor) error {
				hash, ok := data.Get("tx_hash")
				if !ok {
					return fmt.Errorf("tx_hash not found")
				}
				if hash != "0xabc123" {
					return fmt.Errorf("unexpected tx_hash: %v", hash)
				}
				return nil
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
}

func TestDataPersistenceThroughSnapshot(t *testing.T) {
	pollCount := 0

	p := &Pipeline{
		Name: "data_persistence_test",
		Steps: []Step{
			Action("setup", func(_ context.Context, data DataAccessor) error {
				data.Set("counter", 100)
				return nil
			}),
			Poll("wait", func(_ context.Context, data DataAccessor) (bool, time.Duration, error) {
				pollCount++
				if pollCount >= 2 {
					return true, 0, nil
				}
				return false, time.Millisecond, nil
			}),
			Action("verify", func(_ context.Context, data DataAccessor) error {
				// After restore from snapshot, data should be json.RawMessage.
				counter, err := GetData[float64](data, "counter")
				if err != nil {
					return err
				}
				if counter != 100 {
					return fmt.Errorf("expected counter=100, got %v", counter)
				}
				return nil
			}),
		},
	}

	executor := newTestExecutor(t, nil)

	// First run — setup completes, poll snoozes.
	state, err := executor.Run(context.Background(), p, RunState{})
	require.Error(t, err)
	_, ok := errors.AsType[ErrSnooze](err)
	require.True(t, ok)

	// Verify data is in the state.
	assert.NotNil(t, state.Data["counter"])

	// Second run — poll completes, verify reads persisted data.
	state, err = executor.Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
}

func TestSnapshotCalledAfterEachStep(t *testing.T) {
	var snapshots []RunState

	p := &Pipeline{
		Name: "snapshot_test",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
			Action("step2", func(_ context.Context, _ DataAccessor) error { return nil }),
			Action("step3", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	executor := newTestExecutor(t, &snapshots)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)

	// Should have snapshot after each of the 3 steps + final completed status.
	assert.GreaterOrEqual(t, len(snapshots), 3)
}

func TestOnEnterHook(t *testing.T) {
	var hookCalls []string

	p := &Pipeline{
		Name: "on_enter_test",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error {
				hookCalls = append(hookCalls, "action1")
				return nil
			}, WithOnEnter(func(_ context.Context, _ DataAccessor) error {
				hookCalls = append(hookCalls, "enter1")
				return nil
			})),
			Action("step2", func(_ context.Context, _ DataAccessor) error {
				hookCalls = append(hookCalls, "action2")
				return nil
			}, WithOnEnter(func(_ context.Context, _ DataAccessor) error {
				hookCalls = append(hookCalls, "enter2")
				return nil
			})),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, []string{"enter1", "action1", "enter2", "action2"}, hookCalls)
}

func TestRetry(t *testing.T) {
	var attempts int32

	p := &Pipeline{
		Name: "retry_test",
		Steps: []Step{
			Action("flaky", func(_ context.Context, _ DataAccessor) error {
				n := atomic.AddInt32(&attempts, 1)
				if n < 3 {
					return fmt.Errorf("attempt %d failed", n)
				}
				return nil
			}, WithRetry(5, time.Millisecond, false)),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
}

func TestRetryExhausted(t *testing.T) {
	p := &Pipeline{
		Name: "retry_exhausted_test",
		Steps: []Step{
			Action("always_fails", func(_ context.Context, _ DataAccessor) error {
				return fmt.Errorf("permanent failure")
			}, WithRetry(3, time.Millisecond, false)),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err) // Compensation ran (no compensators), pipeline is failed.
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Contains(t, state.Error, "permanent failure")
}

func TestNestedBranchWithCompensation(t *testing.T) {
	var executed []string

	p := &Pipeline{
		Name: "nested_branch_test",
		Steps: []Step{
			Action("outer_setup", func(_ context.Context, _ DataAccessor) error {
				executed = append(executed, "outer_setup")
				return nil
			}, WithCompensate(func(_ context.Context, _ DataAccessor) error {
				executed = append(executed, "comp_outer_setup")
				return nil
			})),
			Branch("outer_branch", func(_ context.Context, _ DataAccessor) (string, error) {
				return "path_a", nil
			}, map[string][]Step{
				"path_a": {
					Action("inner_action", func(_ context.Context, _ DataAccessor) error {
						executed = append(executed, "inner_action")
						return nil
					}, WithCompensate(func(_ context.Context, _ DataAccessor) error {
						executed = append(executed, "comp_inner_action")
						return nil
					})),
				},
				"path_b": {},
			}),
			Action("fails", func(_ context.Context, _ DataAccessor) error {
				executed = append(executed, "fails")
				return fmt.Errorf("boom")
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, state.Status)

	assert.Equal(t, []string{
		"outer_setup",
		"inner_action",
		"fails",
		"comp_inner_action",
		"comp_outer_setup",
	}, executed)
}

func TestPollMaxDuration(t *testing.T) {
	p := &Pipeline{
		Name: "poll_timeout_test",
		Steps: []Step{
			Poll("forever_poll", func(_ context.Context, _ DataAccessor) (bool, time.Duration, error) {
				return false, time.Millisecond, nil
			}, WithMaxPollDuration(50*time.Millisecond)),
		},
	}

	executor := newTestExecutor(t, nil)

	// Simulate multiple poll invocations with time passing.
	state := RunState{}
	start := time.Now()
	pastStart := start.Add(-100 * time.Millisecond)
	state.PollStartedAt = &pastStart
	state.CurrentPath = []string{"forever_poll"}
	state.Status = RunStatusPolling

	state, err := executor.Run(context.Background(), p, state)
	// Should fail due to timeout → compensation → failed.
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Contains(t, state.Error, "exceeded max duration")
}

func TestEmptyBranchPath(t *testing.T) {
	var executed []string

	p := &Pipeline{
		Name: "empty_branch_test",
		Steps: []Step{
			Branch("decide", func(_ context.Context, _ DataAccessor) (string, error) {
				return "skip", nil
			}, map[string][]Step{
				"skip": {},
				"do": {
					Action("work", func(_ context.Context, _ DataAccessor) error {
						executed = append(executed, "work")
						return nil
					}),
				},
			}),
			Action("after", func(_ context.Context, _ DataAccessor) error {
				executed = append(executed, "after")
				return nil
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, []string{"after"}, executed)
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	p := &Pipeline{
		Name: "cancel_test",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error {
				cancel() // Cancel after first step.
				return nil
			}),
			Action("step2", func(_ context.Context, _ DataAccessor) error {
				return nil
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(ctx, p, RunState{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	// Step1 should be completed, step2 should not have run.
	assert.Len(t, state.CompletedSteps, 1)
	assert.Equal(t, "step1", state.CompletedSteps[0].Path[0])
}

func TestValidation_DuplicateStepNames(t *testing.T) {
	p := &Pipeline{
		Name: "validation_test",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	executor := newTestExecutor(t, nil)
	_, err := executor.Run(context.Background(), p, RunState{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate step name")
}

func TestValidation_NoStepType(t *testing.T) {
	p := &Pipeline{
		Name: "validation_test",
		Steps: []Step{
			{Name: "empty"},
		},
	}

	executor := newTestExecutor(t, nil)
	_, err := executor.Run(context.Background(), p, RunState{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one of Action, Poll, or Branch")
}

func TestGetDataGeneric(t *testing.T) {
	ds := newDataStore()
	ds.Set("str", "hello")
	ds.Set("num", 42)

	str, err := GetData[string](ds, "str")
	require.NoError(t, err)
	assert.Equal(t, "hello", str)

	num, err := GetData[int](ds, "num")
	require.NoError(t, err)
	assert.Equal(t, 42, num)

	_, err = GetData[string](ds, "missing")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestCompensationInBranch(t *testing.T) {
	var executed []string

	p := &Pipeline{
		Name: "comp_branch_test",
		Steps: []Step{
			Branch("decide", func(_ context.Context, _ DataAccessor) (string, error) {
				return "path_a", nil
			}, map[string][]Step{
				"path_a": {
					Action("delegate", func(_ context.Context, _ DataAccessor) error {
						executed = append(executed, "delegate")
						return nil
					}, WithCompensate(func(_ context.Context, _ DataAccessor) error {
						executed = append(executed, "reclaim")
						return nil
					})),
					Action("fails_here", func(_ context.Context, _ DataAccessor) error {
						executed = append(executed, "fails_here")
						return fmt.Errorf("fail")
					}),
				},
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Equal(t, []string{"delegate", "fails_here", "reclaim"}, executed)
}

func TestResumePollInsideBranch(t *testing.T) {
	pollCount := 0

	p := &Pipeline{
		Name: "resume_branch_poll",
		Steps: []Step{
			Branch("decide", func(_ context.Context, _ DataAccessor) (string, error) {
				return "path_a", nil
			}, map[string][]Step{
				"path_a": {
					Action("setup", func(_ context.Context, _ DataAccessor) error { return nil }),
					Poll("wait", func(_ context.Context, _ DataAccessor) (bool, time.Duration, error) {
						pollCount++
						if pollCount >= 2 {
							return true, 0, nil
						}
						return false, time.Millisecond, nil
					}),
				},
			}),
			Action("after", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	executor := newTestExecutor(t, nil)

	// First run — enter branch, complete setup, poll snoozes.
	state, err := executor.Run(context.Background(), p, RunState{})
	require.Error(t, err)
	_, ok := errors.AsType[ErrSnooze](err)
	require.True(t, ok)
	assert.Equal(t, 1, pollCount)
	// CurrentPath should be inside the branch.
	assert.Equal(t, []string{"decide", "path_a", "wait"}, state.CurrentPath)

	// Second run — resume inside branch, poll completes, pipeline finishes.
	state, err = executor.Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, 2, pollCount)
}

func TestNoCompensateError(t *testing.T) {
	var compensated bool

	p := &Pipeline{
		Name: "no_compensate_test",
		Steps: []Step{
			Action("setup", func(_ context.Context, _ DataAccessor) error {
				return nil
			}, WithCompensate(func(_ context.Context, _ DataAccessor) error {
				compensated = true
				return nil
			})),
			Action("retryable_fail", func(_ context.Context, _ DataAccessor) error {
				return NoCompensate(fmt.Errorf("temporary network error"))
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})

	// Error should propagate to caller.
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoCompensate))
	// Compensation should NOT have run.
	assert.False(t, compensated)
	// State should still be running (not failed/compensating).
	assert.Equal(t, RunStatusRunning, state.Status)
	// FailedStepPath should be set.
	assert.Equal(t, []string{"retryable_fail"}, state.FailedStepPath)
}

func TestFailedStepPath(t *testing.T) {
	p := &Pipeline{
		Name: "failed_path_test",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
			Branch("decide", func(_ context.Context, _ DataAccessor) (string, error) {
				return "path_a", nil
			}, map[string][]Step{
				"path_a": {
					Action("inner_fail", func(_ context.Context, _ DataAccessor) error {
						return fmt.Errorf("inner error")
					}),
				},
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err) // Compensation completed.
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.Equal(t, []string{"decide", "path_a", "inner_fail"}, state.FailedStepPath)
	assert.Contains(t, state.Error, "inner error")
}

func TestErrorContextInCompensation(t *testing.T) {
	var compensationSawContext bool

	p := &Pipeline{
		Name: "error_context_test",
		Steps: []Step{
			Action("setup", func(_ context.Context, data DataAccessor) error {
				data.Set("resource_id", "res-42")
				return nil
			}, WithCompensate(func(_ context.Context, data DataAccessor) error {
				// Compensation can read data set by earlier steps.
				resID, _ := GetData[string](data, "resource_id")
				compensationSawContext = resID == "res-42"
				return nil
			})),
			Action("fails", func(_ context.Context, _ DataAccessor) error {
				return fmt.Errorf("boom")
			}),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, state.Status)
	assert.True(t, compensationSawContext, "compensation should see data from earlier steps")
}

// --- Version tests ---

func intPtr(v int) *int { return &v }

func TestVersionStampedOnNewExecution(t *testing.T) {
	p := &Pipeline{
		Name:    "versioned",
		Version: 3,
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, 3, state.Version)
}

func TestVersionMismatchRejectsOldState(t *testing.T) {
	p := &Pipeline{
		Name:    "versioned",
		Version: 2,
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	// Simulate a v1 state that is running (mid-execution).
	state := RunState{
		Version: 1,
		Status:  RunStatusRunning,
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, state)
	require.Error(t, err)

	var vErr *ErrVersionMismatch
	require.ErrorAs(t, err, &vErr)
	assert.Equal(t, 1, vErr.StateVersion)
	assert.Equal(t, 2, vErr.PipelineVersion)
	assert.Equal(t, 2, vErr.MinResumeVersion)
}

func TestVersionMismatchRejectsNewerState(t *testing.T) {
	p := &Pipeline{
		Name:    "versioned",
		Version: 1,
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	state := RunState{
		Version: 2,
		Status:  RunStatusRunning,
	}

	executor := newTestExecutor(t, nil)
	_, err := executor.Run(context.Background(), p, state)
	require.Error(t, err)

	var vErr *ErrVersionMismatch
	require.ErrorAs(t, err, &vErr)
	assert.Equal(t, 2, vErr.StateVersion)
	assert.Equal(t, 1, vErr.PipelineVersion)
}

func TestVersionMinResumeVersionAllowsOldState(t *testing.T) {
	var executed bool
	p := &Pipeline{
		Name:             "versioned",
		Version:          2,
		MinResumeVersion: intPtr(1),
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error {
				executed = true
				return nil
			}),
		},
	}

	state := RunState{
		Version: 1,
		Status:  RunStatusRunning,
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.True(t, executed)
}

func TestVersionAcceptLegacyStates(t *testing.T) {
	p := &Pipeline{
		Name:             "versioned",
		Version:          1,
		MinResumeVersion: new(int), // pointer to 0
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	// Legacy state with version 0.
	state := RunState{
		Version: 0,
		Status:  RunStatusRunning,
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
}

func TestVersionLegacyBackwardCompat(t *testing.T) {
	// v0 pipeline + v0 state — should work as before.
	p := &Pipeline{
		Name: "legacy",
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, RunState{})
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, 0, state.Version)
}

func TestVersionTerminalStateSkipsCheck(t *testing.T) {
	p := &Pipeline{
		Name:    "versioned",
		Version: 2,
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	// Completed state with old version — should return immediately, no error.
	state := RunState{
		Version: 1,
		Status:  RunStatusCompleted,
	}

	executor := newTestExecutor(t, nil)
	state, err := executor.Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
}

func TestVersionMismatchCompensatingState(t *testing.T) {
	p := &Pipeline{
		Name:    "versioned",
		Version: 2,
		Steps: []Step{
			Action("step1", func(_ context.Context, _ DataAccessor) error { return nil }),
		},
	}

	// State in compensating status with old version.
	state := RunState{
		Version: 1,
		Status:  RunStatusCompensating,
	}

	executor := newTestExecutor(t, nil)
	_, err := executor.Run(context.Background(), p, state)
	require.Error(t, err)

	var vErr *ErrVersionMismatch
	require.ErrorAs(t, err, &vErr)
}

func TestVersionValidation(t *testing.T) {
	executor := newTestExecutor(t, nil)

	// Negative version.
	_, err := executor.Run(context.Background(), &Pipeline{
		Name:    "bad",
		Version: -1,
		Steps:   []Step{Action("s", func(_ context.Context, _ DataAccessor) error { return nil })},
	}, RunState{})
	assert.ErrorContains(t, err, "version must be >= 0")

	// MinResumeVersion > Version.
	_, err = executor.Run(context.Background(), &Pipeline{
		Name:             "bad",
		Version:          1,
		MinResumeVersion: intPtr(2),
		Steps:            []Step{Action("s", func(_ context.Context, _ DataAccessor) error { return nil })},
	}, RunState{})
	assert.ErrorContains(t, err, "min resume version (2) cannot exceed version (1)")

	// Negative MinResumeVersion.
	_, err = executor.Run(context.Background(), &Pipeline{
		Name:             "bad",
		Version:          1,
		MinResumeVersion: intPtr(-1),
		Steps:            []Step{Action("s", func(_ context.Context, _ DataAccessor) error { return nil })},
	}, RunState{})
	assert.ErrorContains(t, err, "min resume version must be >= 0")
}

func TestVersionPollingResumeWithSameVersion(t *testing.T) {
	pollCount := 0
	p := &Pipeline{
		Name:    "poll-versioned",
		Version: 2,
		Steps: []Step{
			Poll("wait", func(_ context.Context, _ DataAccessor) (bool, time.Duration, error) {
				pollCount++
				if pollCount < 2 {
					return false, time.Millisecond, nil
				}
				return true, 0, nil
			}),
		},
	}

	executor := newTestExecutor(t, nil)

	// First run — gets snooze.
	state, err := executor.Run(context.Background(), p, RunState{})
	require.Error(t, err)
	var snooze ErrSnooze
	require.ErrorAs(t, err, &snooze)
	assert.Equal(t, 2, state.Version)

	// Resume with same version — should complete.
	state, err = executor.Run(context.Background(), p, state)
	require.NoError(t, err)
	assert.Equal(t, RunStatusCompleted, state.Status)
	assert.Equal(t, 2, state.Version)
}
