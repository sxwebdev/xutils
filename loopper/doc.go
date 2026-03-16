// Package loopper provides a periodic task runner with context timeout,
// panic recovery, overlap prevention, and manual trigger support.
//
// # Core Concepts
//
// A [Loopper] executes a user-supplied function at a fixed interval.
// Each execution runs with its own context timeout. If the previous
// execution is still running when the next tick fires, the tick is skipped.
// Panics inside the task are recovered automatically.
//
// # Quick Start
//
//	l := loopper.New(
//		func(ctx context.Context) {
//			// periodic work: health check, cache refresh, polling, etc.
//			log.Println("tick")
//		},
//		loopper.WithPeriod(10*time.Second),
//		loopper.WithContextTimeout(5*time.Second),
//	)
//
//	l.Start(ctx)
//	defer l.Stop()
//	defer l.Wait()
//
// # Leading Mode
//
// By default, the first execution happens after the first tick (one period later).
// Use [WithLeading] to execute the task immediately on [Loopper.Start]:
//
//	l := loopper.New(taskFn,
//		loopper.WithLeading(),
//		loopper.WithPeriod(30*time.Second),
//	)
//
// # Manual Trigger
//
// [Loopper.Trigger] forces an immediate execution outside the normal schedule.
// It returns true if the task was started, or false if it was already running:
//
//	started := l.Trigger(ctx)
//
// # Overlap Prevention
//
// Only one instance of the task runs at a time. If the task is still executing
// when the next tick fires, the tick is skipped. This is enforced via an atomic
// flag, so there is no risk of concurrent execution.
//
// # Graceful Shutdown
//
// [Loopper.Stop] stops the ticker so no new executions are scheduled.
// [Loopper.Wait] blocks until all in-flight executions complete:
//
//	l.Stop()  // stop scheduling new ticks
//	l.Wait()  // wait for current execution to finish
//
// # Panic Recovery
//
// If the task function panics, the panic is recovered and the loop continues
// on the next tick. No special configuration is needed.
//
// # Options
//
//   - [WithPeriod] — set the tick interval (default: 60s)
//   - [WithContextTimeout] — set the per-execution context timeout (default: 30s)
//   - [WithLeading] — execute immediately on Start, before the first tick
//   - [WithLogger] — set a structured logger for debug output
package loopper
