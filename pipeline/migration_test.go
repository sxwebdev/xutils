package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/sxwebdev/xutils/pipeline"
)

func TestStateMigrationRenamesStepAndTransformsData(t *testing.T) {
	oldData, err := json.Marshal(map[string]any{"first": "Ada", "last": "Lovelace"})
	if err != nil {
		t.Fatal(err)
	}
	state := pipeline.RunState{
		Version:     1,
		Status:      pipeline.RunStatusRunning,
		CurrentPath: []string{"old_step"},
		Data:        map[string]json.RawMessage{"person": oldData},
	}
	called := 0
	p := &pipeline.Pipeline{Name: "migrate", Version: 3, Steps: []pipeline.Step{
		pipeline.Action("new_step", func(_ context.Context, data pipeline.DataAccessor) error {
			called++
			name, getErr := pipeline.GetData[string](data, "display_name")
			if getErr != nil {
				return getErr
			}
			if name != "Ada Lovelace" {
				return errors.New("unexpected migrated name")
			}
			return nil
		}),
	}}
	var snapshots []pipeline.RunState
	executor := pipeline.NewExecutor(
		pipeline.WithStateMigrator(func(_ context.Context, name string, from, to int, migrated pipeline.RunState) (pipeline.RunState, error) {
			if name != "migrate" || from != 1 || to != 3 {
				return migrated, errors.New("unexpected migration metadata")
			}
			migrated.CurrentPath = []string{"new_step"}
			migrated.Data = map[string]json.RawMessage{
				"display_name": json.RawMessage(`"Ada Lovelace"`),
			}
			return migrated, nil
		}),
		pipeline.WithSnapshotFn(func(_ context.Context, snapshot pipeline.RunState) error {
			snapshots = append(snapshots, snapshot)
			return nil
		}),
	)

	state, err = executor.Run(t.Context(), p, state)
	if err != nil || state.Status != pipeline.RunStatusCompleted || called != 1 {
		t.Fatalf("Run() = status %q, err %v, calls %d", state.Status, err, called)
	}
	if len(snapshots) == 0 || snapshots[0].Version != 3 || string(snapshots[0].Data["display_name"]) != `"Ada Lovelace"` {
		t.Fatalf("first snapshot was not migrated: %#v", snapshots)
	}
}

func TestStateMigrationFailureRunsNoCallbacks(t *testing.T) {
	cause := errors.New("cannot migrate")
	called := 0
	p := &pipeline.Pipeline{Name: "migration_failure", Version: 2, Steps: []pipeline.Step{
		pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error { called++; return nil }),
	}}
	executor := pipeline.NewExecutor(pipeline.WithStateMigrator(func(context.Context, string, int, int, pipeline.RunState) (pipeline.RunState, error) {
		return pipeline.RunState{}, cause
	}))
	state := pipeline.RunState{Version: 1, Status: pipeline.RunStatusRunning, CurrentPath: []string{"action"}}
	out, err := executor.Run(t.Context(), p, state)
	migrationErr, ok := errors.AsType[*pipeline.ErrStateMigrationFailed](err)
	if !ok || !errors.Is(err, cause) || migrationErr.FromVersion != 1 || migrationErr.ToVersion != 2 {
		t.Fatalf("migration error = %#v", err)
	}
	if called != 0 || out.Version != 1 {
		t.Fatalf("callback calls=%d state=%#v", called, out)
	}
}

func TestMigrationSnapshotFailureIsFailStopAndResumable(t *testing.T) {
	cause := errors.New("snapshot failed")
	called := 0
	migrations := 0
	p := &pipeline.Pipeline{Name: "migration_snapshot", Version: 2, Steps: []pipeline.Step{
		pipeline.Action("new", func(context.Context, pipeline.DataAccessor) error { called++; return nil }),
	}}
	migrator := pipeline.StateMigrator(func(_ context.Context, _ string, _, _ int, state pipeline.RunState) (pipeline.RunState, error) {
		migrations++
		state.CurrentPath = []string{"new"}
		return state, nil
	})
	state := pipeline.RunState{Version: 1, Status: pipeline.RunStatusRunning, CurrentPath: []string{"old"}}
	executor := pipeline.NewExecutor(
		pipeline.WithStateMigrator(migrator),
		pipeline.WithSnapshotFn(func(context.Context, pipeline.RunState) error { return cause }),
	)
	out, err := executor.Run(t.Context(), p, state)
	if _, ok := errors.AsType[*pipeline.ErrSnapshotFailed](err); !ok || !errors.Is(err, cause) {
		t.Fatalf("Run() error = %v", err)
	}
	if called != 0 || migrations != 1 || out.Version != 2 {
		t.Fatalf("called=%d migrations=%d state=%#v", called, migrations, out)
	}

	// Reloading the last durable v1 state safely re-runs the atomic migrator.
	state, err = pipeline.NewExecutor(pipeline.WithStateMigrator(migrator)).Run(t.Context(), p, state)
	if err != nil || state.Status != pipeline.RunStatusCompleted || called != 1 || migrations != 2 {
		t.Fatalf("resume: status=%q err=%v called=%d migrations=%d", state.Status, err, called, migrations)
	}
}

func TestStateMigrationRejectsDowngradeAndSkipsTerminalState(t *testing.T) {
	migrations := 0
	migrator := pipeline.StateMigrator(func(context.Context, string, int, int, pipeline.RunState) (pipeline.RunState, error) {
		migrations++
		return pipeline.RunState{}, nil
	})
	p := &pipeline.Pipeline{Name: "versions", Version: 2, Steps: []pipeline.Step{
		pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error { return nil }),
	}}
	executor := pipeline.NewExecutor(pipeline.WithStateMigrator(migrator))

	_, err := executor.Run(t.Context(), p, pipeline.RunState{Version: 3, Status: pipeline.RunStatusRunning})
	if _, ok := errors.AsType[*pipeline.ErrVersionMismatch](err); !ok || migrations != 0 {
		t.Fatalf("downgrade error=%v migrations=%d", err, migrations)
	}
	terminal := pipeline.RunState{Version: 1, Status: pipeline.RunStatusCompleted}
	out, err := executor.Run(t.Context(), p, terminal)
	if err != nil || out.Version != 1 || migrations != 0 {
		t.Fatalf("terminal Run() = %#v, %v; migrations=%d", out, err, migrations)
	}
}

func TestMigrationToTerminalStateDoesNotResumeCallbacks(t *testing.T) {
	called := 0
	p := &pipeline.Pipeline{Name: "terminal_migration", Version: 2, Steps: []pipeline.Step{
		pipeline.Action("action", func(context.Context, pipeline.DataAccessor) error { called++; return nil }),
	}}
	executor := pipeline.NewExecutor(pipeline.WithStateMigrator(func(_ context.Context, _ string, _, _ int, state pipeline.RunState) (pipeline.RunState, error) {
		state.Status = pipeline.RunStatusFailed
		state.Error = "retired by migration"
		return state, nil
	}))
	state, err := executor.Run(t.Context(), p, pipeline.RunState{Version: 1, Status: pipeline.RunStatusRunning})
	if err != nil || state.Status != pipeline.RunStatusFailed || called != 0 {
		t.Fatalf("Run() = status %q, err %v, calls %d", state.Status, err, called)
	}
}
