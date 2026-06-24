package timeutil

import "time"

// TimeIt runs fn and returns how long it took along with fn's error.
func TimeIt(fn func() error) (time.Duration, error) {
	start := time.Now()
	err := fn()
	return time.Since(start), err
}
