package loopper_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/loopper"
)

// captureLogger records Errorf calls to verify the logger is actually wired.
type captureLogger struct {
	mu     sync.Mutex
	errors []string
}

func (c *captureLogger) record(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors = append(c.errors, fmt.Sprintf(format, args...))
}

func (c *captureLogger) errorCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.errors)
}

func (c *captureLogger) Debugf(string, ...any)          {}
func (c *captureLogger) Debugw(string, ...any)          {}
func (c *captureLogger) Infof(string, ...any)           {}
func (c *captureLogger) Infow(string, ...any)           {}
func (c *captureLogger) Warnf(string, ...any)           {}
func (c *captureLogger) Warnw(string, ...any)           {}
func (c *captureLogger) Errorf(format string, a ...any) { c.record(format, a...) }
func (c *captureLogger) Errorw(format string, a ...any) { c.record(format, a...) }

func TestWithLogger_IsWired(t *testing.T) {
	logger := &captureLogger{}
	l := loopper.New(func(context.Context) { panic("boom") },
		loopper.WithLogger(logger),
		loopper.WithLeading(),
		loopper.WithPeriod(time.Hour),
	)
	l.Start(t.Context())
	defer l.Wait()
	defer l.Stop()

	// The recovered panic must be reported through the configured logger.
	require.Eventually(t, func() bool { return logger.errorCount() >= 1 }, time.Second, 5*time.Millisecond,
		"configured logger must receive the recovered-panic message")
}

func TestWithLogger_NilIsIgnored(t *testing.T) {
	var runs atomic.Int64
	// Passing nil must not override the default logger nor cause a nil deref.
	l := loopper.New(func(context.Context) { runs.Add(1) },
		loopper.WithLogger(nil),
		loopper.WithLeading(),
		loopper.WithPeriod(time.Hour),
	)
	require.NotPanics(t, func() { l.Start(t.Context()) })
	defer l.Wait()
	defer l.Stop()

	require.Eventually(t, func() bool { return runs.Load() == 1 }, time.Second, 5*time.Millisecond)
}

func TestWithPeriod_NonPositiveIsIgnored(t *testing.T) {
	// If WithPeriod(0) were honored, Start would panic in time.NewTicker(0).
	for _, d := range []time.Duration{0, -time.Second} {
		var runs atomic.Int64
		l := loopper.New(func(context.Context) { runs.Add(1) },
			loopper.WithLeading(),
			loopper.WithPeriod(d),
		)
		require.NotPanics(t, func() { l.Start(t.Context()) }, "WithPeriod(%v) must be ignored, default kept", d)
		require.Eventually(t, func() bool { return runs.Load() == 1 }, time.Second, 5*time.Millisecond)
		l.Stop()
		l.Wait()
		assert.Equal(t, int64(1), runs.Load())
	}
}
