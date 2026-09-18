package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sxwebdev/xutils/pipeline"
)

func ExampleRepeat() {
	var processed int
	p := &pipeline.Pipeline{
		Name: "batches",
		Steps: []pipeline.Step{
			pipeline.Repeat("batch", []pipeline.Step{
				pipeline.Action("process", func(context.Context, pipeline.DataAccessor) error {
					processed++
					return nil
				}),
			}, func(_ context.Context, _ pipeline.DataAccessor, iteration int) (bool, time.Duration, error) {
				return iteration == 2, 0, nil
			}, pipeline.WithRepeatHistory(pipeline.RepeatHistoryCompact)),
		},
	}

	state := pipeline.RunState{}
	executor := pipeline.NewExecutor()
	for {
		var err error
		state, err = executor.Run(context.Background(), p, state)
		if err == nil {
			break
		}
		if _, ok := errors.AsType[*pipeline.ErrRetryAfter](err); !ok {
			panic(err)
		}
		// A real application would enqueue the run after retryAfter.Duration.
	}

	fmt.Println(state.Status, processed)
	// Output: completed 3
}
