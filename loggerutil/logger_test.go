package loggerutil_test

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loggerutil"
)

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it. Both TestLogger and EmptyLogger write (or refrain
// from writing) to the package-level os.Stdout via fmt.Printf, so capturing it
// is the only way to assert their observable behavior.
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

func TestEmptyLogger_ImplementsLoggerAndIsSilent(t *testing.T) {
	var l loggerutil.Logger = &loggerutil.EmptyLogger{}
	out := captureStdout(t, func() {
		// Call every method; an EmptyLogger must produce no output at all.
		l.Debugf("d %d", 1)
		l.Debugw("d %d", 2)
		l.Infof("i %d", 3)
		l.Infow("i %d", 4)
		l.Warnf("w %d", 5)
		l.Warnw("w %d", 6)
		l.Errorf("e %d", 7)
		l.Errorw("e %d", 8)
	})
	require.Empty(t, out, "EmptyLogger must discard everything")
}

func TestTestLogger_WritesLevelPrefixedLines(t *testing.T) {
	var l loggerutil.Logger = loggerutil.NewTestLogger()

	// Each method must emit its level prefix, substitute the format args, and
	// terminate with a newline. Table-driven so every one of the 8 methods is
	// asserted independently — a mislabeled level or dropped newline fails here.
	tests := []struct {
		name string
		call func()
		want string
	}{
		{"Debugf", func() { l.Debugf("v=%d", 1) }, "[DEBUG] v=1\n"},
		{"Debugw", func() { l.Debugw("v=%d", 2) }, "[DEBUG] v=2\n"},
		{"Infof", func() { l.Infof("v=%d", 3) }, "[INFO] v=3\n"},
		{"Infow", func() { l.Infow("v=%d", 4) }, "[INFO] v=4\n"},
		{"Warnf", func() { l.Warnf("v=%d", 5) }, "[WARN] v=5\n"},
		{"Warnw", func() { l.Warnw("v=%d", 6) }, "[WARN] v=6\n"},
		{"Errorf", func() { l.Errorf("v=%d", 7) }, "[ERROR] v=7\n"},
		{"Errorw", func() { l.Errorw("v=%d", 8) }, "[ERROR] v=8\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := captureStdout(t, tt.call)
			require.Equal(t, tt.want, out)
		})
	}
}

func TestNewTestLogger_ReturnsNonNil(t *testing.T) {
	require.NotNil(t, loggerutil.NewTestLogger())
}
