package pipeline_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sxwebdev/xutils/pipeline"
)

type recordingObserver struct {
	events []pipeline.Event
}

func (o *recordingObserver) Observe(_ context.Context, event pipeline.Event) {
	o.events = append(o.events, event)
}

func (o *recordingObserver) types() []pipeline.EventType {
	types := make([]pipeline.EventType, len(o.events))
	for i := range o.events {
		types[i] = o.events[i].Type
	}
	return types
}

func TestObserverSuccessOrder(t *testing.T) {
	observer := &recordingObserver{}
	p := &pipeline.Pipeline{Name: "success", Version: 4, Steps: []pipeline.Step{
		pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error { return nil }),
	}}
	executor := pipeline.NewExecutor(
		pipeline.WithObserver(observer),
		pipeline.WithSnapshotFn(func(context.Context, pipeline.RunState) error { return nil }),
	)
	state, err := executor.Run(t.Context(), p, pipeline.RunState{})
	if err != nil || state.Status != pipeline.RunStatusCompleted {
		t.Fatalf("Run() = status %q, err %v", state.Status, err)
	}
	want := []pipeline.EventType{
		pipeline.EventPipelineStarted,
		pipeline.EventSnapshotSucceeded,
		pipeline.EventStepStarted,
		pipeline.EventSnapshotSucceeded,
		pipeline.EventStepCompleted,
		pipeline.EventSnapshotSucceeded,
		pipeline.EventPipelineCompleted,
	}
	if got := observer.types(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for _, event := range observer.events {
		if event.PipelineName != "success" || event.PipelineVersion != 4 {
			t.Fatalf("event metadata = %#v", event)
		}
	}
}

func TestObserverDelayedContinuationOrderAndNormalizedDuration(t *testing.T) {
	observer := &recordingObserver{}
	cause := errors.New("busy")
	p := &pipeline.Pipeline{Name: "delay", Steps: []pipeline.Step{
		pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error {
			return pipeline.RetryAfter(-time.Second, cause)
		}),
	}}
	_, err := pipeline.NewExecutor(
		pipeline.WithObserver(observer),
		pipeline.WithSnapshotFn(func(context.Context, pipeline.RunState) error { return nil }),
	).Run(t.Context(), p, pipeline.RunState{})
	retryAfter, ok := errors.AsType[*pipeline.ErrRetryAfter](err)
	if !ok || retryAfter.Duration != 0 || !errors.Is(err, cause) {
		t.Fatalf("Run() error = %v", err)
	}
	wantTail := []pipeline.EventType{
		pipeline.EventStepStarted,
		pipeline.EventSnapshotSucceeded,
		pipeline.EventDelayedContinuation,
	}
	got := observer.types()
	if !reflect.DeepEqual(got[len(got)-len(wantTail):], wantTail) {
		t.Fatalf("events = %v", got)
	}
	delayed := observer.events[len(observer.events)-1]
	if delayed.Duration != 0 || !errors.Is(delayed.Cause, cause) {
		t.Fatalf("delayed event = %#v", delayed)
	}
}

func TestObserverSnapshotFailureOrder(t *testing.T) {
	observer := &recordingObserver{}
	cause := errors.New("storage")
	called := 0
	p := &pipeline.Pipeline{Name: "snapshot", Steps: []pipeline.Step{
		pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error { called++; return nil }),
	}}
	_, err := pipeline.NewExecutor(
		pipeline.WithObserver(observer),
		pipeline.WithSnapshotFn(func(context.Context, pipeline.RunState) error { return cause }),
	).Run(t.Context(), p, pipeline.RunState{})
	if _, ok := errors.AsType[*pipeline.ErrSnapshotFailed](err); !ok || called != 0 {
		t.Fatalf("Run() error=%v calls=%d", err, called)
	}
	want := []pipeline.EventType{pipeline.EventPipelineStarted, pipeline.EventSnapshotFailed}
	if got := observer.types(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestObserverCompensationOrder(t *testing.T) {
	observer := &recordingObserver{}
	p := &pipeline.Pipeline{Name: "compensation", Steps: []pipeline.Step{
		pipeline.Action("first", func(context.Context, pipeline.DataAccessor) error { return nil }, pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error { return nil })),
		pipeline.Action("fail", func(context.Context, pipeline.DataAccessor) error { return errors.New("boom") }),
	}}
	state, err := pipeline.NewExecutor(pipeline.WithObserver(observer)).Run(t.Context(), p, pipeline.RunState{})
	if err != nil || state.Status != pipeline.RunStatusFailed {
		t.Fatalf("Run() = status %q, err %v", state.Status, err)
	}
	var compensationEvents []pipeline.EventType
	for _, event := range observer.events {
		switch event.Type {
		case pipeline.EventCompensationStarted, pipeline.EventCompensationSucceeded, pipeline.EventCompensationFailed:
			compensationEvents = append(compensationEvents, event.Type)
		}
	}
	want := []pipeline.EventType{
		pipeline.EventCompensationStarted,
		pipeline.EventCompensationStarted,
		pipeline.EventCompensationSucceeded,
		pipeline.EventCompensationSucceeded,
	}
	if !reflect.DeepEqual(compensationEvents, want) {
		t.Fatalf("compensation events = %v, want %v", compensationEvents, want)
	}
}

func TestObserverMigrationOrder(t *testing.T) {
	observer := &recordingObserver{}
	p := &pipeline.Pipeline{Name: "migration", Version: 2, Steps: []pipeline.Step{
		pipeline.Action("new", func(context.Context, pipeline.DataAccessor) error { return nil }),
	}}
	state := pipeline.RunState{Version: 1, Status: pipeline.RunStatusRunning, CurrentPath: []string{"old"}}
	_, err := pipeline.NewExecutor(
		pipeline.WithObserver(observer),
		pipeline.WithStateMigrator(func(_ context.Context, _ string, _, _ int, state pipeline.RunState) (pipeline.RunState, error) {
			state.CurrentPath = []string{"new"}
			return state, nil
		}),
		pipeline.WithSnapshotFn(func(context.Context, pipeline.RunState) error { return nil }),
	).Run(t.Context(), p, state)
	if err != nil {
		t.Fatal(err)
	}
	types := observer.types()
	wantPrefix := []pipeline.EventType{
		pipeline.EventPipelineStarted,
		pipeline.EventSnapshotSucceeded,
		pipeline.EventStateMigrationSucceeded,
	}
	if !reflect.DeepEqual(types[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("events = %v", types)
	}
}

func TestObserverReportsMigrationAndCompensationFailures(t *testing.T) {
	t.Run("migration", func(t *testing.T) {
		observer := &recordingObserver{}
		cause := errors.New("bad state")
		p := &pipeline.Pipeline{Name: "migration_failure_event", Version: 2, Steps: []pipeline.Step{
			pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error { return nil }),
		}}
		_, err := pipeline.NewExecutor(
			pipeline.WithObserver(observer),
			pipeline.WithStateMigrator(func(context.Context, string, int, int, pipeline.RunState) (pipeline.RunState, error) {
				return pipeline.RunState{}, cause
			}),
		).Run(t.Context(), p, pipeline.RunState{Version: 1, Status: pipeline.RunStatusRunning})
		if _, ok := errors.AsType[*pipeline.ErrStateMigrationFailed](err); !ok {
			t.Fatalf("Run() error = %v", err)
		}
		if got := observer.types(); !reflect.DeepEqual(got, []pipeline.EventType{pipeline.EventPipelineStarted, pipeline.EventStateMigrationFailed}) {
			t.Fatalf("events = %v", got)
		}
	})

	t.Run("compensation", func(t *testing.T) {
		observer := &recordingObserver{}
		p := &pipeline.Pipeline{Name: "compensation_failure_event", Steps: []pipeline.Step{
			pipeline.Action("first", func(context.Context, pipeline.DataAccessor) error { return nil }, pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error { return errors.New("undo failed") })),
			pipeline.Action("fail", func(context.Context, pipeline.DataAccessor) error { return errors.New("boom") }),
		}}
		_, err := pipeline.NewExecutor(pipeline.WithObserver(observer)).Run(t.Context(), p, pipeline.RunState{})
		if _, ok := errors.AsType[*pipeline.ErrCompensationFailed](err); !ok {
			t.Fatalf("Run() error = %v", err)
		}
		found := false
		for _, event := range observer.events {
			if event.Type == pipeline.EventCompensationFailed && event.Operation == "compensation step" {
				found = true
			}
		}
		if !found {
			t.Fatalf("events = %v", observer.types())
		}
	})
}

func TestObserverPanicDoesNotAffectExecution(t *testing.T) {
	p := &pipeline.Pipeline{Name: "panic", Steps: []pipeline.Step{
		pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error { return nil }),
	}}
	observer := pipeline.ObserverFunc(func(context.Context, pipeline.Event) { panic("observer bug") })
	state, err := pipeline.NewExecutor(pipeline.WithObserver(observer)).Run(t.Context(), p, pipeline.RunState{})
	if err != nil || state.Status != pipeline.RunStatusCompleted {
		t.Fatalf("Run() = status %q, err %v", state.Status, err)
	}
}

func TestObserverCannotMutateExecutionFailure(t *testing.T) {
	compensations := 0
	observer := pipeline.ObserverFunc(func(_ context.Context, event pipeline.Event) {
		if event.Type != pipeline.EventStepFailed {
			return
		}
		stepErr, ok := errors.AsType[*pipeline.ErrStepFailed](event.Cause)
		if !ok {
			t.Fatalf("step failure cause = %T, want *pipeline.ErrStepFailed", event.Cause)
		}
		stepErr.Path[0] = "observer-mutated"
		stepErr.Err = pipeline.RetryAfter(time.Hour, nil)
	})
	p := &pipeline.Pipeline{Name: "observer_isolation", Steps: []pipeline.Step{
		pipeline.Action("prepare", func(context.Context, pipeline.DataAccessor) error { return nil },
			pipeline.WithCompensate(func(context.Context, pipeline.DataAccessor) error {
				compensations++
				return nil
			})),
		pipeline.Action("fail", func(context.Context, pipeline.DataAccessor) error { return errors.New("boom") }),
	}}

	state, err := pipeline.NewExecutor(pipeline.WithObserver(observer)).Run(t.Context(), p, pipeline.RunState{})
	if err != nil || state.Status != pipeline.RunStatusFailed {
		t.Fatalf("Run() = status %q, err %v", state.Status, err)
	}
	if compensations != 1 {
		t.Fatalf("compensations = %d, want 1", compensations)
	}
	if got, want := state.FailedStepPath, []string{"fail"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FailedStepPath = %v, want %v", got, want)
	}
}

func TestObserverRepeatIterationMetadata(t *testing.T) {
	observer := &recordingObserver{}
	p := &pipeline.Pipeline{Name: "repeat_metadata", Steps: []pipeline.Step{
		pipeline.Repeat("loop", []pipeline.Step{
			pipeline.Action("child", func(context.Context, pipeline.DataAccessor) error { return nil }),
		}, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) {
			return false, time.Second, nil
		}),
	}}

	_, err := pipeline.NewExecutor(pipeline.WithObserver(observer)).Run(t.Context(), p, pipeline.RunState{})
	if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok {
		t.Fatalf("Run() error = %v", err)
	}

	assertIteration := func(eventType pipeline.EventType, stepPath []string, want int) {
		t.Helper()
		for _, event := range observer.events {
			if event.Type != eventType || !reflect.DeepEqual(event.StepPath, stepPath) {
				continue
			}
			if event.RepeatIteration == nil || *event.RepeatIteration != want {
				t.Fatalf("%s event iteration = %v, want %d", eventType, event.RepeatIteration, want)
			}
			return
		}
		t.Fatalf("event %s for path %v not found", eventType, stepPath)
	}

	assertIteration(pipeline.EventStepStarted, []string{"loop", "0", "child"}, 0)
	assertIteration(pipeline.EventStepCompleted, []string{"loop", "0", "child"}, 0)
	assertIteration(pipeline.EventDelayedContinuation, []string{"loop"}, 0)
}

func TestObserverRepeatFailureIterationMetadata(t *testing.T) {
	observer := &recordingObserver{}
	p := &pipeline.Pipeline{Name: "repeat_failure_metadata", Steps: []pipeline.Step{
		pipeline.Repeat("loop", []pipeline.Step{
			pipeline.Action("child", func(context.Context, pipeline.DataAccessor) error { return errors.New("boom") }),
		}, func(context.Context, pipeline.DataAccessor, int) (bool, time.Duration, error) {
			return true, 0, nil
		}),
	}}

	state, err := pipeline.NewExecutor(pipeline.WithObserver(observer)).Run(t.Context(), p, pipeline.RunState{})
	if err != nil || state.Status != pipeline.RunStatusFailed {
		t.Fatalf("Run() = status %q, err %v", state.Status, err)
	}
	for _, event := range observer.events {
		if event.Type != pipeline.EventStepFailed || !reflect.DeepEqual(event.StepPath, []string{"loop", "0", "child"}) {
			continue
		}
		if event.RepeatIteration == nil || *event.RepeatIteration != 0 {
			t.Fatalf("step_failed event iteration = %v, want 0", event.RepeatIteration)
		}
		return
	}
	t.Fatal("step_failed event for repeat child not found")
}

func TestObserverReportsVersionMismatchAndCancellation(t *testing.T) {
	t.Run("version mismatch", func(t *testing.T) {
		observer := &recordingObserver{}
		p := &pipeline.Pipeline{Name: "version_event", Version: 2, Steps: []pipeline.Step{
			pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error { return nil }),
		}}
		_, err := pipeline.NewExecutor(pipeline.WithObserver(observer)).Run(t.Context(), p, pipeline.RunState{Version: 1, Status: pipeline.RunStatusRunning})
		if _, ok := errors.AsType[*pipeline.ErrVersionMismatch](err); !ok {
			t.Fatalf("Run() error = %v", err)
		}
		if got := observer.types(); !reflect.DeepEqual(got, []pipeline.EventType{pipeline.EventPipelineStarted, pipeline.EventVersionMismatch}) {
			t.Fatalf("events = %v", got)
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		observer := &recordingObserver{}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		p := &pipeline.Pipeline{Name: "cancel_event", Steps: []pipeline.Step{
			pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error { return nil }),
		}}
		_, err := pipeline.NewExecutor(pipeline.WithObserver(observer)).Run(ctx, p, pipeline.RunState{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v", err)
		}
		if got := observer.types(); !reflect.DeepEqual(got, []pipeline.EventType{pipeline.EventPipelineStarted, pipeline.EventContextCanceled}) {
			t.Fatalf("events = %v", got)
		}
	})
}
