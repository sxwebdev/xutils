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

// Get returns a copy: mutating the returned value must not change the container.
func TestLocker_GetReturnsCopy(t *testing.T) {
	type box struct{ n int }
	l := syncutil.NewLocker(box{n: 1})

	got := l.Get()
	got.n = 999
	assert.Equal(t, 1, l.Get().n, "Get must return an independent copy")
}

// Update must mutate the same value that Get/GetPointer observe (i.e. it edits
// in place, not a copy), and Set must replace what GetPointer subsequently sees.
func TestLocker_UpdateSetGetPointerConsistency(t *testing.T) {
	type box struct{ n int }
	l := syncutil.NewLocker(box{n: 1})

	l.Update(func(b *box) { b.n = 5 })
	assert.Equal(t, 5, l.GetPointer().n, "Update must edit the value GetPointer returns")

	l.Set(box{n: 42})
	assert.Equal(t, 42, l.GetPointer().n, "Set must replace the value GetPointer returns")
	assert.Equal(t, 42, l.Get().n)
}

// NewLocker copies its argument: later mutating the original value passed in
// must not affect the stored value.
func TestLocker_NewLockerCopiesArgument(t *testing.T) {
	type box struct{ n int }
	orig := box{n: 1}
	l := syncutil.NewLocker(orig)
	orig.n = 999
	assert.Equal(t, 1, l.Get().n, "NewLocker must store a copy of its argument")
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
