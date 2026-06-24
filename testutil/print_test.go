package testutil_test

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/testutil"
)

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it. PrintJSON writes to the package-level os.Stdout,
// so this is the only way to observe its output.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = orig })

	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()

	fn()
	require.NoError(t, w.Close())
	return <-done
}

func TestPrintJSON_WritesIndentedJSON(t *testing.T) {
	var err error
	out := captureStdout(t, func() {
		err = testutil.PrintJSON(map[string]int{"a": 1})
	})
	require.NoError(t, err)
	// Two-space indentation and a trailing newline from fmt.Println are part of
	// the observable contract; assert the exact bytes, not just "non-empty".
	require.Equal(t, "{\n  \"a\": 1\n}\n", out)
}

func TestPrintJSON_MarshalError(t *testing.T) {
	// Channels are not JSON-serializable, so marshaling must fail. On error the
	// function must not write anything to stdout.
	var err error
	out := captureStdout(t, func() {
		err = testutil.PrintJSON(make(chan int))
	})
	require.Error(t, err)
	require.Empty(t, out, "nothing must be printed when marshaling fails")
}
