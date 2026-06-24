package syncutil_test

import (
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/syncutil"
)

func TestMap_SetGetHasDelete(t *testing.T) {
	m := syncutil.NewMap[string, int]()
	require.Equal(t, 0, m.Len())

	m.Set("a", 1)
	m.Set("b", 2)

	v, ok := m.Get("a")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	_, ok = m.Get("missing")
	assert.False(t, ok)

	assert.True(t, m.Has("b"))
	assert.False(t, m.Has("missing"))
	assert.Equal(t, 2, m.Len())

	m.Delete("a")
	assert.False(t, m.Has("a"))
	assert.Equal(t, 1, m.Len())
}

func TestMap_NewMapWithCapacity(t *testing.T) {
	m := syncutil.NewMapWithCapacity[string, int](10)
	m.Set("x", 1)
	assert.Equal(t, 1, m.Len())
}

func TestMap_GetAllReturnsCopy(t *testing.T) {
	m := syncutil.NewMap[string, int]()
	m.Set("a", 1)

	all := m.GetAll()
	all["a"] = 999
	all["b"] = 2 // mutating the returned map must not affect the container

	v, _ := m.Get("a")
	assert.Equal(t, 1, v)
	assert.False(t, m.Has("b"))
}

func TestMap_KeysValues(t *testing.T) {
	m := syncutil.NewMap[string, int]()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	keys := m.Keys()
	sort.Strings(keys)
	assert.Equal(t, []string{"a", "b", "c"}, keys)

	values := m.Values()
	sort.Ints(values)
	assert.Equal(t, []int{1, 2, 3}, values)
}

func TestMap_Range(t *testing.T) {
	m := syncutil.NewMap[string, int]()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	// Full iteration sums all values.
	sum := 0
	m.Range(func(_ string, v int) bool {
		sum += v
		return true
	})
	assert.Equal(t, 6, sum)

	// Early stop visits exactly one element.
	visited := 0
	m.Range(func(_ string, _ int) bool {
		visited++
		return false
	})
	assert.Equal(t, 1, visited)
}

func TestMap_Clear(t *testing.T) {
	m := syncutil.NewMap[string, int]()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Clear()
	assert.Equal(t, 0, m.Len())
	assert.False(t, m.Has("a"))
}

func TestMap_Overwrite(t *testing.T) {
	m := syncutil.NewMap[string, int]()
	m.Set("a", 1)
	m.Set("a", 2) // overwrite, not a second entry
	v, ok := m.Get("a")
	assert.True(t, ok)
	assert.Equal(t, 2, v)
	assert.Equal(t, 1, m.Len())
}

func TestMap_DeleteAbsentAndMissing(t *testing.T) {
	m := syncutil.NewMap[string, int]()
	m.Delete("nope") // deleting an absent key is a no-op, must not panic
	assert.Equal(t, 0, m.Len())

	v, ok := m.Get("nope")
	assert.False(t, ok)
	assert.Equal(t, 0, v, "missing Get must return the zero value")
}

func TestMap_EmptyAccessors(t *testing.T) {
	m := syncutil.NewMap[string, int]()
	assert.Empty(t, m.Keys())
	assert.Empty(t, m.Values())
	assert.Empty(t, m.GetAll())

	calls := 0
	m.Range(func(string, int) bool { calls++; return true })
	assert.Equal(t, 0, calls, "Range over an empty map must not invoke fn")
}

func TestMap_ConcurrentAccess(t *testing.T) {
	m := syncutil.NewMap[int, int]()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range 100 {
				k := base*100 + i
				m.Set(k, k)
				m.Get(k)
				m.Has(k)
				_ = m.GetAll()
				_ = m.Keys()
				_ = m.Values()
				_ = m.Len()
			}
		}(g)
	}
	wg.Wait()

	assert.Equal(t, 800, m.Len())
}

// TestMap_ConcurrentSetDelete exercises every mutating and reading method
// simultaneously, so the -race detector covers the write-side locks of Delete
// and Clear (which TestMap_ConcurrentAccess never calls). It also asserts an
// observable end state: each goroutine deletes exactly the keys it wrote, so the
// final map must be empty regardless of interleaving.
func TestMap_ConcurrentSetDelete(t *testing.T) {
	const goroutines, perG = 8, 200
	m := syncutil.NewMap[int, int]()

	var wg sync.WaitGroup
	// Writers: set then delete their own keys.
	for g := range goroutines {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range perG {
				k := base*perG + i
				m.Set(k, k)
				_, _ = m.Get(k)
				m.Delete(k)
			}
		}(g)
	}
	// Readers: iterate and snapshot concurrently with the writers.
	for range goroutines {
		wg.Go(func() {
			for range perG {
				m.Range(func(_, _ int) bool { return true })
				_ = m.GetAll()
				_ = m.Keys()
				_ = m.Values()
				_ = m.Has(0)
				_ = m.Len()
			}
		})
	}
	wg.Wait()

	// Every written key was also deleted, so the container must be empty.
	assert.Equal(t, 0, m.Len())
	assert.Empty(t, m.GetAll())
}

// TestMap_ConcurrentClear runs Clear against concurrent writers and readers so
// the -race detector covers Clear's write lock. The only assertable invariant
// is the absence of a race/panic; final length is non-deterministic.
func TestMap_ConcurrentClear(t *testing.T) {
	m := syncutil.NewMap[int, int]()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range 100 {
				m.Set(base*100+i, i)
				if i%10 == 0 {
					m.Clear()
				}
				_ = m.GetAll()
				_ = m.Len()
			}
		}(g)
	}
	wg.Wait()
}
