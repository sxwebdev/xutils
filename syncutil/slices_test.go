package syncutil_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/syncutil"
)

func TestSlice_AddAndGetAll(t *testing.T) {
	s := syncutil.NewSlice[int]()
	require.Equal(t, 0, s.Len())

	s.Add(1)
	s.Add(2)
	s.AddMany([]int{3, 4})

	assert.Equal(t, 4, s.Len())
	assert.Equal(t, []int{1, 2, 3, 4}, s.GetAll())
}

func TestSlice_NewSliceWithLengthAndAddToIndex(t *testing.T) {
	s := syncutil.NewSliceWithLength[string](3)
	require.Equal(t, 3, s.Len())

	s.AddToIndex(0, "a")
	s.AddToIndex(2, "c")
	assert.Equal(t, []string{"a", "", "c"}, s.GetAll())
}

// Regression: GetAll used to return the internal slice, so a caller could mutate
// the container's state (and race with concurrent writers). It must return a copy.
func TestSlice_GetAllReturnsCopy(t *testing.T) {
	s := syncutil.NewSlice[int]()
	s.AddMany([]int{1, 2, 3})

	got := s.GetAll()
	got[0] = 999 // mutating the returned slice must not affect the container

	assert.Equal(t, []int{1, 2, 3}, s.GetAll(), "GetAll must hand out an independent copy")
}

func TestSlice_ConcurrentAccess(t *testing.T) {
	s := syncutil.NewSlice[int]()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range 100 {
				s.Add(base*100 + i)
				// Read the snapshot concurrently; with the copy fix this is race-free.
				for _, v := range s.GetAll() {
					_ = v
				}
				_ = s.Len()
			}
		}(g)
	}
	wg.Wait()

	assert.Equal(t, 800, s.Len())
}
