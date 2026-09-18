package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sxwebdev/xutils/pipeline"
)

func ExampleStateMigrator() {
	oldPerson, _ := json.Marshal(map[string]string{"first": "Ada", "last": "Lovelace"})
	state := pipeline.RunState{
		Version:     1,
		Status:      pipeline.RunStatusRunning,
		CurrentPath: []string{"format_person"},
		Data:        map[string]json.RawMessage{"person": oldPerson},
	}
	p := &pipeline.Pipeline{Name: "greeting", Version: 2, Steps: []pipeline.Step{
		pipeline.Action("build_greeting", func(_ context.Context, data pipeline.DataAccessor) error {
			name, _ := pipeline.GetData[string](data, "display_name")
			fmt.Println(name)
			return nil
		}),
	}}

	executor := pipeline.NewExecutor(
		pipeline.WithStateMigrator(func(_ context.Context, _ string, _, _ int, migrated pipeline.RunState) (pipeline.RunState, error) {
			var person map[string]string
			if err := json.Unmarshal(migrated.Data["person"], &person); err != nil {
				return migrated, err
			}
			displayName, _ := json.Marshal(person["first"] + " " + person["last"])
			migrated.Data = map[string]json.RawMessage{"display_name": displayName}
			migrated.CurrentPath = []string{"build_greeting"}
			return migrated, nil
		}),
		pipeline.WithSnapshotFn(func(context.Context, pipeline.RunState) error { return nil }),
	)

	state, err := executor.Run(context.Background(), p, state)
	fmt.Println(state.Status, err)
	// Output:
	// Ada Lovelace
	// completed <nil>
}

func ExampleObserver() {
	observer := pipeline.ObserverFunc(func(_ context.Context, event pipeline.Event) {
		fmt.Println(event.Type)
	})
	p := &pipeline.Pipeline{Name: "observed", Steps: []pipeline.Step{
		pipeline.Action("work", func(context.Context, pipeline.DataAccessor) error { return nil }),
	}}

	_, _ = pipeline.NewExecutor(pipeline.WithObserver(observer)).Run(context.Background(), p, pipeline.RunState{})
	// Output:
	// pipeline_started
	// step_started
	// step_completed
	// pipeline_completed
}
