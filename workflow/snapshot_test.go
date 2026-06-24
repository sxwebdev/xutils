package workflow_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/workflow"
)

func TestVars_SetGet(t *testing.T) {
	wf := workflow.New()

	// Missing key on a nil store.
	_, ok := workflow.GetVar[string](wf, "missing")
	assert.False(t, ok)

	workflow.SetVar(wf, "s", "hello")
	workflow.SetVar(wf, "n", 42)

	s, ok := workflow.GetVar[string](wf, "s")
	assert.True(t, ok)
	assert.Equal(t, "hello", s)

	// Wrong type.
	_, ok = workflow.GetVar[string](wf, "n")
	assert.False(t, ok, "type mismatch returns false")

	// Missing key on a populated store.
	_, ok = workflow.GetVar[int](wf, "absent")
	assert.False(t, ok)
}

func argWorkflow(t *testing.T) workflow.Snapshot {
	t.Helper()
	step, _ := workflow.NewStep("producer", func(sc *workflow.StepContext) error {
		sc.Step.State.SetArg("tx", "0xabc")
		sc.Step.State.SetArg("count", 7)
		return nil
	})
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}
	require.NoError(t, wf.Run(t.Context()))
	return wf.GetSnapshot()
}

func TestGetArg(t *testing.T) {
	snap := argWorkflow(t)

	v, err := workflow.GetArg[string](snap, "producer", "tx")
	require.NoError(t, err)
	assert.Equal(t, "0xabc", v)

	n, err := workflow.GetArg[int](snap, "producer", "count")
	require.NoError(t, err)
	assert.Equal(t, 7, n)

	t.Run("empty step", func(t *testing.T) {
		_, err := workflow.GetArg[string](snap, "", "tx")
		require.Error(t, err)
	})
	t.Run("empty key", func(t *testing.T) {
		_, err := workflow.GetArg[string](snap, "producer", "")
		require.Error(t, err)
	})
	t.Run("key not found", func(t *testing.T) {
		_, err := workflow.GetArg[string](snap, "producer", "ghost")
		require.ErrorIs(t, err, workflow.ErrNotFound)
	})
	t.Run("wrong type", func(t *testing.T) {
		_, err := workflow.GetArg[int](snap, "producer", "tx")
		require.Error(t, err)
	})
	t.Run("step not found", func(t *testing.T) {
		_, err := workflow.GetArg[string](snap, "ghoststep", "tx")
		require.Error(t, err)
	})
}

func TestGetArgByRef(t *testing.T) {
	step, ref := workflow.NewStep("producer", func(sc *workflow.StepContext) error {
		sc.Step.State.SetArg("k", "v")
		return nil
	})
	wf := workflow.New()
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{step}}}
	require.NoError(t, wf.Run(t.Context()))

	v, err := workflow.GetArgByRef[string](wf.GetSnapshot(), ref, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", v)
}

func TestSnapshot_SaveRestoreRoundTrip(t *testing.T) {
	build := func() *workflow.Workflow {
		s1 := okStep("s1")
		s2 := okStep("s2")
		wf := workflow.New()
		wf.Stages = []*workflow.Stage{{Name: "stage", Steps: []*workflow.Step{s1, s2}}}
		return wf
	}

	src := build()
	require.NoError(t, src.Run(t.Context()))
	jsonSnap := src.GetJSONSnapshot()
	require.NotEmpty(t, jsonSnap)

	// Restore into a fresh workflow; completed steps must be skipped on re-run.
	restored := build()
	require.NoError(t, restored.SetJSONSnapshot(jsonSnap))
	require.NoError(t, restored.Run(t.Context()))

	for _, st := range restored.StepStates() {
		assert.Equal(t, workflow.StepStatusCompleted, st.Status)
	}
}

func TestSnapshot_JSONDebugFormatting(t *testing.T) {
	wf := workflow.New(workflow.WithDebug(true))
	wf.Stages = []*workflow.Stage{{Name: "s", Steps: []*workflow.Step{okStep("x")}}}
	require.NoError(t, wf.Run(t.Context()))
	assert.Contains(t, wf.GetJSONSnapshot(), "\n", "debug snapshot is indented")
}

func TestSetJSONSnapshot_InvalidJSON(t *testing.T) {
	wf := workflow.New()
	require.Error(t, wf.SetJSONSnapshot("{not json"))
}

func TestCurrentStageStep(t *testing.T) {
	t.Run("empty workflow", func(t *testing.T) {
		wf := workflow.New()
		assert.Nil(t, wf.CurrentStage())
		assert.Nil(t, wf.CurrentStep())
	})

	t.Run("after run", func(t *testing.T) {
		wf := workflow.New()
		wf.Stages = []*workflow.Stage{
			{Name: "s1", Steps: []*workflow.Step{okStep("a")}},
			{Name: "s2", Steps: []*workflow.Step{okStep("b")}},
		}
		require.NoError(t, wf.Run(t.Context()))
		require.NotNil(t, wf.CurrentStage())
		assert.Equal(t, "s2", wf.CurrentStage().Name)
		require.NotNil(t, wf.CurrentStep())
		assert.Equal(t, "b", wf.CurrentStep().Name)
	})
}

func TestSilentError(t *testing.T) {
	assert.Nil(t, workflow.SilentError(nil))

	err := workflow.SilentError(errors.New("boom"))
	require.Error(t, err)
	assert.ErrorIs(t, err, workflow.ErrSilent)
}
