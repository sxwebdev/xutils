package util

import "time"

func TimeIt(fn func() error) (time.Duration, error) {
	start := time.Now()
	err := fn()
	return time.Since(start), err
}
