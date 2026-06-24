package dbutil_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/dbutil"
)

func TestJSONField_MarshalJSON(t *testing.T) {
	t.Run("empty is null", func(t *testing.T) {
		var j dbutil.JSONField
		b, err := j.MarshalJSON()
		require.NoError(t, err)
		assert.Equal(t, "null", string(b))
	})

	t.Run("raw passthrough", func(t *testing.T) {
		j := dbutil.JSONField(`{"a":1}`)
		b, err := json.Marshal(j)
		require.NoError(t, err)
		assert.JSONEq(t, `{"a":1}`, string(b))
	})
}

func TestJSONField_UnmarshalJSON(t *testing.T) {
	var j dbutil.JSONField
	require.NoError(t, json.Unmarshal([]byte(`{"x":true}`), &j))
	assert.JSONEq(t, `{"x":true}`, string(j))

	// Method on a nil pointer must error rather than panic.
	var nilField *dbutil.JSONField
	require.Error(t, nilField.UnmarshalJSON([]byte(`{}`)))
}

func TestJSONField_UnmarshalFromAny(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		j := dbutil.JSONField(`{"old":1}`)
		require.NoError(t, j.UnmarshalFromAny(nil))
		assert.Nil(t, []byte(j))
	})
	t.Run("string", func(t *testing.T) {
		var j dbutil.JSONField
		require.NoError(t, j.UnmarshalFromAny(`{"a":1}`))
		assert.JSONEq(t, `{"a":1}`, string(j))
	})
	t.Run("bytes", func(t *testing.T) {
		var j dbutil.JSONField
		require.NoError(t, j.UnmarshalFromAny([]byte(`{"b":2}`)))
		assert.JSONEq(t, `{"b":2}`, string(j))
	})
	t.Run("struct is marshaled", func(t *testing.T) {
		var j dbutil.JSONField
		require.NoError(t, j.UnmarshalFromAny(map[string]int{"c": 3}))
		assert.JSONEq(t, `{"c":3}`, string(j))
	})
	t.Run("unmarshalable value errors", func(t *testing.T) {
		var j dbutil.JSONField
		require.Error(t, j.UnmarshalFromAny(make(chan int)))
	})
}

func TestJSONField_Scan(t *testing.T) {
	t.Run("nil clears", func(t *testing.T) {
		j := dbutil.JSONField(`{"a":1}`)
		require.NoError(t, j.Scan(nil))
		assert.Nil(t, []byte(j))
	})

	t.Run("string", func(t *testing.T) {
		var j dbutil.JSONField
		require.NoError(t, j.Scan(`{"a":1}`))
		assert.JSONEq(t, `{"a":1}`, string(j))
	})

	t.Run("bytes", func(t *testing.T) {
		var j dbutil.JSONField
		require.NoError(t, j.Scan([]byte(`{"a":1}`)))
		assert.JSONEq(t, `{"a":1}`, string(j))
	})

	t.Run("surrounding whitespace trimmed", func(t *testing.T) {
		var j dbutil.JSONField
		require.NoError(t, j.Scan([]byte("  \n {\"a\":1} \t ")))
		assert.JSONEq(t, `{"a":1}`, string(j))
	})

	t.Run("whitespace-only clears", func(t *testing.T) {
		j := dbutil.JSONField(`{"a":1}`)
		require.NoError(t, j.Scan([]byte("   \n\t  ")))
		assert.Nil(t, []byte(j))
	})

	// Regression: internal whitespace inside string values must be preserved.
	t.Run("preserves spaces inside string values", func(t *testing.T) {
		var j dbutil.JSONField
		require.NoError(t, j.Scan([]byte(`{"msg":"hello world"}`)))
		assert.Equal(t, "hello world", j.ConvertToMap()["msg"])
	})

	t.Run("invalid JSON errors", func(t *testing.T) {
		var j dbutil.JSONField
		require.Error(t, j.Scan([]byte(`{not json`)))
	})

	t.Run("unsupported type errors", func(t *testing.T) {
		var j dbutil.JSONField
		require.Error(t, j.Scan(42))
	})
}

func TestJSONField_Value(t *testing.T) {
	t.Run("empty is nil", func(t *testing.T) {
		var j dbutil.JSONField
		v, err := j.Value()
		require.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("valid returns string", func(t *testing.T) {
		j := dbutil.JSONField(`{"a":1}`)
		v, err := j.Value()
		require.NoError(t, err)
		assert.Equal(t, `{"a":1}`, v)
	})

	t.Run("invalid errors", func(t *testing.T) {
		j := dbutil.JSONField(`{bad`)
		_, err := j.Value()
		require.Error(t, err)
	})
}

func TestJSONField_Convert(t *testing.T) {
	t.Run("ConvertToMap valid", func(t *testing.T) {
		j := dbutil.JSONField(`{"a":1,"b":"x"}`)
		m := j.ConvertToMap()
		assert.Equal(t, float64(1), m["a"])
		assert.Equal(t, "x", m["b"])
	})
	t.Run("ConvertToMap empty", func(t *testing.T) {
		var j dbutil.JSONField
		assert.Equal(t, map[string]any{}, j.ConvertToMap())
	})
	t.Run("ConvertToMap non-object returns empty", func(t *testing.T) {
		j := dbutil.JSONField(`[1,2,3]`)
		assert.Equal(t, map[string]any{}, j.ConvertToMap())
	})
	t.Run("ConvertToAny valid", func(t *testing.T) {
		j := dbutil.JSONField(`{"a":1}`)
		var dst struct {
			A int `json:"a"`
		}
		require.NoError(t, j.ConvertToAny(&dst))
		assert.Equal(t, 1, dst.A)
	})
	t.Run("ConvertToAny empty is no-op", func(t *testing.T) {
		var j dbutil.JSONField
		var dst struct{ A int }
		require.NoError(t, j.ConvertToAny(&dst))
	})
	t.Run("ConvertToAny error", func(t *testing.T) {
		j := dbutil.JSONField(`{"a":1}`)
		var dst int // object cannot unmarshal into int
		require.Error(t, j.ConvertToAny(&dst))
	})
}
