package testutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/testutil"
)

func TestPrintJSON_Success(t *testing.T) {
	require.NoError(t, testutil.PrintJSON(map[string]int{"a": 1}))
}

func TestPrintJSON_MarshalError(t *testing.T) {
	// Channels are not JSON-serializable, so marshaling must fail.
	require.Error(t, testutil.PrintJSON(make(chan int)))
}
