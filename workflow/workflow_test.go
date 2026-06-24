package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loggerutil"
	"github.com/sxwebdev/xutils/workflow"
)

func stageBeforeStart(_ context.Context, stage *workflow.Stage) error {
	fmt.Printf("Starting stage: %s\n", stage.Name)
	return nil
}

func stageAfterComplete(_ context.Context, stage *workflow.Stage) error {
	fmt.Printf("Stage %s completed\n", stage.Name)
	return nil
}

func stepBeforeStart(_ context.Context, step *workflow.Step) error {
	fmt.Printf("Starting step: %s\n", step.Name)
	return nil
}

func stepAfterComplete(_ context.Context, step *workflow.Step) error {
	fmt.Printf("Step %s completed\n\n", step.Name)
	return nil
}

// Example step functions
func failedStep(_ *workflow.StepContext) error {
	return fmt.Errorf("failed step")
}

func simpleStep(_ *workflow.StepContext) error {
	time.Sleep(300 * time.Millisecond)
	return nil
}

func mustStep(name string, fn workflow.StepFunc, opts ...workflow.StepOption) *workflow.Step {
	s, _ := workflow.NewStep(name, fn, opts...)
	return s
}

func mustStage(name string, opts ...workflow.StageOption) *workflow.Stage {
	s, _ := workflow.NewStage(name, opts...)
	return s
}

func TestWorflow(t *testing.T) {
	l := loggerutil.NewTestLogger()

	// Deterministic transient failures: each flaky step fails a fixed number of
	// times, then succeeds — so the test verifies the retry path without relying
	// on randomness (and without slow sleeps).
	var step12Attempts, step21Attempts int

	flaky := func(counter *int, failUntil int, msg string) workflow.StepFunc {
		return func(_ *workflow.StepContext) error {
			*counter++
			if *counter < failUntil {
				return errors.New(msg)
			}
			return nil
		}
	}

	wf := workflow.New(
		workflow.WithName("Test Workflow"),
		workflow.WithLogger(l),
		workflow.WithBeforeFn(func(_ context.Context, _ *workflow.Workflow) error { return nil }),
		workflow.WithAfterFn(func(_ context.Context, _ *workflow.Workflow) error { return nil }),
	)

	wf.Stages = []*workflow.Stage{
		{
			Name: "Stage 1",
			Steps: []*workflow.Step{
				mustStep("Step 1.1", func(_ *workflow.StepContext) error { return nil },
					workflow.WithStepBeforeFn(stepBeforeStart), workflow.WithStepAfterFn(stepAfterComplete)),
				{
					Name:       "Step 1.2",
					Func:       flaky(&step12Attempts, 3, "step 1.2 transient"),
					Timeout:    time.Millisecond,
					MaxRetries: 5,
					BeforeFn:   stepBeforeStart,
					AfterFn:    stepAfterComplete,
				},
			},
			BeforeFn: stageBeforeStart,
			AfterFn:  stageAfterComplete,
		},
		{
			Name: "Stage 2",
			Steps: []*workflow.Step{
				{
					Name:       "Step 2.1",
					Func:       flaky(&step21Attempts, 2, "step 2.1 transient"),
					Timeout:    time.Millisecond,
					MaxRetries: 5,
					BeforeFn:   stepBeforeStart,
					AfterFn:    stepAfterComplete,
				},
				mustStep("Step 2.2", func(_ *workflow.StepContext) error { return nil },
					workflow.WithStepBeforeFn(stepBeforeStart), workflow.WithStepAfterFn(stepAfterComplete)),
			},
			BeforeFn: stageBeforeStart,
			AfterFn:  stageAfterComplete,
		},
		mustStage("Stage 3", workflow.WithStageBeforeFn(stageBeforeStart), workflow.WithStageAfterFn(stageAfterComplete)),
	}

	require.NoError(t, wf.Run(t.Context()))
	require.Equal(t, 3, step12Attempts, "step 1.2 retried until its 3rd attempt")
	require.Equal(t, 2, step21Attempts, "step 2.1 retried until its 2nd attempt")
	require.True(t, wf.State.IsCompleted)
}

var simpleWorkflowSnapshot = `{
  "steps_states": [
    {
      "current_stage": "Stage 1",
      "current_step": "Step 1.1",
      "start_time": "2024-10-02T13:40:39.960708+03:00",
      "end_time": "2024-10-02T13:40:40.261771+03:00",
      "status": "completed"
    },
    {
      "previous_stage": "Stage 1",
      "previous_step": "Step 1.1",
      "current_stage": "Stage 1",
      "current_step": "Step 1.2",
      "start_time": "2024-10-02T13:40:40.26182+03:00",
      "end_time": "2024-10-02T13:40:40.562862+03:00",
      "status": "completed"
    },
    {
      "previous_stage": "Stage 1",
      "previous_step": "Step 1.2",
      "current_stage": "Stage 2",
      "current_step": "Step 2.1",
      "start_time": "2024-10-02T13:40:40.56294+03:00",
      "end_time": "2024-10-02T13:40:41.063041+03:00",
      "status": "completed"
    },
    {
      "current_stage": "Stage 2",
      "current_step": "Skipped step 2.2",
      "start_time": null,
      "end_time": null,
      "status": "skipped"
    },
    {
      "current_stage": "Stage 2",
      "current_step": "Skipped Step 2.3",
      "start_time": null,
      "end_time": null,
      "status": "skipped"
    },
    {
      "current_stage": "Stage 3",
      "current_step": "Step 3.1",
      "start_time": null,
      "end_time": null,
      "status": "skipped"
    },
    {
      "previous_stage": "Stage 2",
      "previous_step": "Step 2.1",
      "current_stage": "Stage 3",
      "current_step": "Step 3.2",
      "start_time": "2024-10-02T13:40:41.063296+03:00",
      "end_time": "2024-10-02T13:40:41.063297+03:00",
      "status": "completed"
    },
    {
      "previous_stage": "Stage 3",
      "previous_step": "Step 3.2",
      "current_stage": "Stage 3",
      "current_step": "Step 3.3",
      "start_time": "2024-10-02T13:40:41.063301+03:00",
      "end_time": "2024-10-02T13:40:41.063302+03:00",
      "status": "completed"
    }
  ],
  "workflow_state": {
    "is_suspended": false,
    "is_completed": false,
    "is_failed": false
  }
}`

func simpleWorkflow() *workflow.Workflow {
	l := loggerutil.NewTestLogger()

	_, stage3Ref := workflow.NewStage("Stage 3")
	_, step3_2Ref := workflow.NewStep("Step 3.2", nil)

	wf := workflow.New(
		workflow.WithLogger(l),
		workflow.WithDebug(true),
	).SetOnFailureFn(func(_ context.Context, w *workflow.Workflow, err error) error {
		w.Errorf("SetOnFailureFn: workflow failed with error: %s", err)
		return nil
	})

	wf.Stages = []*workflow.Stage{
		{
			Name: "Stage 1",
			Steps: []*workflow.Step{
				mustStep("Step 1.1", simpleStep),
				mustStep("Step 1.2", simpleStep),
			},
		},
		{
			Name: "Stage 2",
			Steps: []*workflow.Step{
				mustStep("Step 2.1", func(sc *workflow.StepContext) error {
					time.Sleep(500 * time.Millisecond)
					sc.Workflow.State.GoToStage(stage3Ref)
					sc.Workflow.State.GoToStep(step3_2Ref)
					return nil
				}),
				mustStep("Skipped step 2.2", failedStep),
				mustStep("Skipped Step 2.3", failedStep),
			},
		},
		{
			Name: "Stage 3",
			Steps: []*workflow.Step{
				mustStep("Step 3.1", simpleStep),
				mustStep("Step 3.2", func(_ *workflow.StepContext) error {
					return nil
				}),
				mustStep("Step 3.3", func(_ *workflow.StepContext) error {
					return nil
				}, workflow.WithStepMaxRetries(5), workflow.WithStepTimeout(500*time.Millisecond)),
				mustStep("Step 3.4", simpleStep),
			},
		},
	}

	return wf
}

func TestWorkflowSimple(t *testing.T) {
	wf := simpleWorkflow()

	err := wf.Run(context.Background())
	require.NoError(t, err)

	fmt.Println(wf.GetJSONSnapshot())
}

func TestWorkflowSimpleFromSnapshot(t *testing.T) {
	wf := simpleWorkflow()
	err := wf.SetJSONSnapshot(simpleWorkflowSnapshot)
	require.NoError(t, err)

	err = wf.Run(context.Background())
	require.NoError(t, err)

	fmt.Println(wf.GetJSONSnapshot())
}

func TestWorkflowVars(t *testing.T) {
	l := loggerutil.NewTestLogger()

	wf := workflow.New(
		workflow.WithLogger(l),
	)

	wf.Stages = []*workflow.Stage{
		{
			Name: "Stage 1",
			Steps: []*workflow.Step{
				mustStep("Producer", func(sc *workflow.StepContext) error {
					workflow.SetVar(sc.Workflow, "greeting", "hello from producer")
					return nil
				}),
				mustStep("Consumer", func(sc *workflow.StepContext) error {
					val, ok := workflow.GetVar[string](sc.Workflow, "greeting")
					if !ok {
						return fmt.Errorf("greeting var not found")
					}
					if val != "hello from producer" {
						return fmt.Errorf("unexpected value: %s", val)
					}
					return nil
				}),
			},
		},
	}

	err := wf.Run(context.Background())
	require.NoError(t, err)
}

func TestWorkflowSnapshotFn(t *testing.T) {
	l := loggerutil.NewTestLogger()

	var snapshotCount atomic.Int32

	wf := workflow.New(
		workflow.WithLogger(l),
		workflow.WithSnapshotFn(func(_ context.Context, _ *workflow.Workflow, _ workflow.Snapshot) error {
			snapshotCount.Add(1)
			return nil
		}),
	)

	wf.Stages = []*workflow.Stage{
		{
			Name: "Stage 1",
			Steps: []*workflow.Step{
				mustStep("Step 1", func(_ *workflow.StepContext) error { return nil }),
				mustStep("Step 2", func(_ *workflow.StepContext) error { return nil }),
			},
		},
	}

	err := wf.Run(context.Background())
	require.NoError(t, err)

	// 2 steps x 2 snapshots each (before + after) = 4
	require.Equal(t, int32(4), snapshotCount.Load())
}

func TestWorkflowCompensation(t *testing.T) {
	l := loggerutil.NewTestLogger()

	_, compensationRef := workflow.NewStage("Compensation")
	_, determineRef := workflow.NewStep("Determine", nil)

	wf := workflow.New(
		workflow.WithLogger(l),
		workflow.WithDebug(true),
		workflow.WithCompensationStage(compensationRef, determineRef),
	)

	wf.Stages = []*workflow.Stage{
		{
			Name: "Main",
			Steps: []*workflow.Step{
				mustStep("Failing Step", func(_ *workflow.StepContext) error {
					return fmt.Errorf("something went wrong")
				}),
			},
		},
		{
			Name: "Compensation",
			Steps: []*workflow.Step{
				mustStep("Determine", func(_ *workflow.StepContext) error {
					return nil
				}),
			},
		},
	}

	// First run: step fails, compensation is configured, error is suppressed
	err := wf.Run(context.Background())
	require.NoError(t, err)

	// State should have error and next stage/step set to compensation
	require.NotEmpty(t, wf.State.Error)
	require.Equal(t, "Compensation", wf.State.NextStage)
	require.Equal(t, "Determine", wf.State.NextStep)
}

func TestWorkflowStepRefNavigation(t *testing.T) {
	l := loggerutil.NewTestLogger()

	_, targetRef := workflow.NewStep("Target", nil)

	wf := workflow.New(
		workflow.WithLogger(l),
	)

	var targetExecuted bool

	wf.Stages = []*workflow.Stage{
		{
			Name: "Stage 1",
			Steps: []*workflow.Step{
				mustStep("Skipper", func(sc *workflow.StepContext) error {
					sc.Workflow.State.GoToStep(targetRef)
					return nil
				}),
				mustStep("Should Skip", func(_ *workflow.StepContext) error {
					return fmt.Errorf("should not be called")
				}),
				mustStep("Target", func(_ *workflow.StepContext) error {
					targetExecuted = true
					return nil
				}),
			},
		},
	}

	err := wf.Run(context.Background())
	require.NoError(t, err)
	require.True(t, targetExecuted)
}

func TestWorkflowSilentError(t *testing.T) {
	l := loggerutil.NewTestLogger()

	attempt := 0
	wf := workflow.New(
		workflow.WithLogger(l),
	)

	wf.Stages = []*workflow.Stage{
		{
			Name: "Stage 1",
			Steps: []*workflow.Step{
				{
					Name: "Silent Step",
					Func: func(_ *workflow.StepContext) error {
						attempt++
						if attempt < 3 {
							return workflow.SilentError(fmt.Errorf("not ready yet"))
						}
						return nil
					},
					MaxRetries: 5,
					Timeout:    time.Millisecond,
				},
			},
		},
	}

	err := wf.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, attempt)
}
