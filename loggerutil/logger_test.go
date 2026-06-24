package loggerutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loggerutil"
)

// exercise calls every method of the Logger interface with a format + args.
func exercise(l loggerutil.Logger) {
	l.Debugf("d %d", 1)
	l.Debugw("d %d", 2)
	l.Infof("i %d", 3)
	l.Infow("i %d", 4)
	l.Warnf("w %d", 5)
	l.Warnw("w %d", 6)
	l.Errorf("e %d", 7)
	l.Errorw("e %d", 8)
}

func TestEmptyLogger_ImplementsLoggerAndIsSilent(t *testing.T) {
	var l loggerutil.Logger = &loggerutil.EmptyLogger{}
	require.NotPanics(t, func() { exercise(l) })
}

func TestTestLogger_ImplementsLoggerAndLogs(t *testing.T) {
	l := loggerutil.NewTestLogger()
	require.NotNil(t, l)

	var iface loggerutil.Logger = l
	require.NotPanics(t, func() { exercise(iface) })
}
