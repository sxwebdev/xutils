package pipeline

import (
	"context"

	"github.com/sxwebdev/xutils/loggerutil"
)

// SnapshotFunc is called to persist pipeline state before resumed callbacks and
// after completed steps, status changes, delayed continuations, and failures.
// The implementation should be idempotent, atomic, and synchronous: returning
// nil declares the supplied state durable.
type SnapshotFunc func(ctx context.Context, state RunState) error

// CASSnapshotFunc persists state only if expectedRevision still matches the
// durable revision. state.Revision is expectedRevision+1. A conflict must be
// returned as an error; the executor wraps it in ErrSnapshotFailed.
type CASSnapshotFunc func(ctx context.Context, expectedRevision uint64, state RunState) error

// ExecutorOption configures an Executor.
type ExecutorOption func(*Executor)

// WithLogger sets the logger for the executor.
func WithLogger(l loggerutil.Logger) ExecutorOption {
	return func(e *Executor) {
		e.logger = l
	}
}

// WithDebug enables debug logging.
func WithDebug(debug bool) ExecutorOption {
	return func(e *Executor) {
		e.debug = debug
	}
}

// WithSnapshotFn sets the fail-stop persistence callback.
func WithSnapshotFn(fn SnapshotFunc) ExecutorOption {
	return func(e *Executor) {
		e.snapshotFn = fn
		e.casSnapshotFn = nil
	}
}

// WithCASSnapshotFn sets an optimistic-concurrency-aware persistence callback.
// It replaces any SnapshotFunc configured earlier.
func WithCASSnapshotFn(fn CASSnapshotFunc) ExecutorOption {
	return func(e *Executor) {
		e.casSnapshotFn = fn
		e.snapshotFn = nil
	}
}

// WithClock sets the clock used for timestamps, timeout checks, and internal
// retry waits. A nil Clock is ignored and the standard time clock is used.
func WithClock(clock Clock) ExecutorOption {
	return func(e *Executor) {
		if clock != nil {
			e.clock = clock
		}
	}
}

// WithStateMigrator configures an atomic from-version-to-current migration.
func WithStateMigrator(migrator StateMigrator) ExecutorOption {
	return func(e *Executor) {
		e.stateMigrator = migrator
	}
}

// WithObserver registers a synchronous lifecycle observer.
func WithObserver(observer Observer) ExecutorOption {
	return func(e *Executor) {
		e.observer = observer
	}
}
