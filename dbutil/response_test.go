package dbutil_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/dbutil"
)

func TestNewFindResponseWithCount(t *testing.T) {
	t.Run("with items", func(t *testing.T) {
		r := dbutil.NewFindResponseWithCount([]int{1, 2, 3}, 10)
		assert.Equal(t, []int{1, 2, 3}, r.Items)
		assert.Equal(t, uint32(10), r.Count)
	})

	t.Run("nil items normalized to empty slice", func(t *testing.T) {
		var nilItems []int
		r := dbutil.NewFindResponseWithCount(nilItems, 0)
		require.NotNil(t, r.Items)
		assert.Empty(t, r.Items)

		// Marshals as [] rather than null.
		b, err := json.Marshal(r)
		require.NoError(t, err)
		assert.JSONEq(t, `{"items":[],"count":0}`, string(b))
	})
}
