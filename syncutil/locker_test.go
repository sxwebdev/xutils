package syncutil_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/syncutil"
)

func TestLocker_GetSet(t *testing.T) {
	l := syncutil.NewLocker(10)
	assert.Equal(t, 10, l.Get())

	l.Set(42)
	assert.Equal(t, 42, l.Get())
}

func TestLocker_Update(t *testing.T) {
	type counter struct{ n int }
	l := syncutil.NewLocker(counter{n: 1})

	l.Update(func(c *counter) { c.n += 5 })
	assert.Equal(t, 6, l.Get().n)
}

func TestLocker_GetPointer(t *testing.T) {
	l := syncutil.NewLocker(7)
	p := l.GetPointer()
	require.NotNil(t, p)
	assert.Equal(t, 7, *p)
}

func TestLocker_ConcurrentUpdate(t *testing.T) {
	l := syncutil.NewLocker(0)

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				l.Update(func(v *int) { *v++ })
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, 100*100, l.Get(), "concurrent updates must not race or lose increments")
}
