package pipeline

import (
	"context"

	"github.com/sxwebdev/xutils/loggerutil"
)

// SnapshotFunc is called to persist the pipeline state.
// It is invoked after each completed step and on status changes.
// The implementation should be idempotent and atomic.
type SnapshotFunc func(ctx context.Context, state RunState) error

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

// WithSnapshotFn sets the persistence callback.
// This function is called after each completed step and on status changes.
func WithSnapshotFn(fn SnapshotFunc) ExecutorOption {
	return func(e *Executor) {
		e.snapshotFn = fn
	}
}
