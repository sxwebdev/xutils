// Package timeutil provides small time-related helpers.
//
// [TimeIt] runs a function and reports how long it took alongside the
// function's error, useful for ad-hoc timing and instrumentation:
//
//	d, err := timeutil.TimeIt(func() error {
//		return doWork(ctx)
//	})
//	log.Printf("doWork took %s (err=%v)", d, err)
package timeutil
