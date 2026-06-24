package dbutil_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/dbutil"
)

func TestDuration_JSONRoundTrip(t *testing.T) {
	d := dbutil.Duration(90 * time.Minute)

	b, err := json.Marshal(d)
	require.NoError(t, err)
	assert.JSONEq(t, `"1h30m0s"`, string(b))

	var got dbutil.Duration
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, 90*time.Minute, got.ToDuration())
}

func TestDuration_UnmarshalJSONErrors(t *testing.T) {
	var d dbutil.Duration
	// Not a JSON string.
	require.Error(t, d.UnmarshalJSON([]byte(`123`)))
	// A string that is not a valid duration.
	require.Error(t, d.UnmarshalJSON([]byte(`"not-a-duration"`)))
}

func TestDuration_ValueAndString(t *testing.T) {
	d := dbutil.Duration(2 * time.Second)

	v, err := d.Value()
	require.NoError(t, err)
	assert.Equal(t, "2s", v)
	assert.Equal(t, "2s", d.String())
	assert.Equal(t, 2*time.Second, d.ToDuration())
}

func TestDuration_Scan(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		var d dbutil.Duration
		require.NoError(t, d.Scan("1h30m"))
		assert.Equal(t, 90*time.Minute, d.ToDuration())
	})

	// Regression: Value stores a string, but many drivers read text columns back
	// as []byte; Scan used to reject []byte with "unsupported scan type".
	t.Run("bytes", func(t *testing.T) {
		var d dbutil.Duration
		require.NoError(t, d.Scan([]byte("1h30m")))
		assert.Equal(t, 90*time.Minute, d.ToDuration())
	})

	t.Run("invalid duration string", func(t *testing.T) {
		var d dbutil.Duration
		require.Error(t, d.Scan("nonsense"))
	})

	t.Run("unsupported type", func(t *testing.T) {
		var d dbutil.Duration
		require.Error(t, d.Scan(42))
	})
}
