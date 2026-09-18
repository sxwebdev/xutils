package pipeline

import (
	"context"
	"time"
)

// Clock supplies wall-clock time and cancellable waits to an Executor.
// Implementations must be safe for concurrent use. Sleep must return ctx.Err()
// when the context is cancelled.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, duration time.Duration) error
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) Sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(normalizeDuration(duration))
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func normalizeDuration(duration time.Duration) time.Duration {
	return max(duration, 0)
}
