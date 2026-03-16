// Package syncutil provides thread-safe generic containers for concurrent access
// to slices, maps, and single values.
//
// # Core Concepts
//
// Three container types are provided, each wrapping a standard Go data structure
// with mutex-based synchronization:
//
//   - [Slice] — thread-safe generic slice (RWMutex)
//   - [Map] — thread-safe generic map with iteration support (RWMutex)
//   - [Locker] — mutex-protected single value with atomic update callback
//
// All containers use generics (Go 1.18+), so values are fully typed without
// interface{} assertions.
//
// # Slice
//
// [Slice] wraps a Go slice with [sync.RWMutex] for safe concurrent access:
//
//	s := syncutil.NewSlice[string]()
//	s.Add("hello")
//	s.AddMany([]string{"world", "!"})
//
//	all := s.GetAll() // []string{"hello", "world", "!"}
//	n := s.Len()      // 3
//
// Use [NewSliceWithLength] to pre-allocate and set items by index:
//
//	s := syncutil.NewSliceWithLength[int](10)
//	s.AddToIndex(0, 42)
//
// # Map
//
// [Map] wraps a Go map with [sync.RWMutex]. It supports get, set, delete,
// existence checks, key/value extraction, and range iteration:
//
//	m := syncutil.NewMap[string, int]()
//	m.Set("connections", 42)
//
//	val, ok := m.Get("connections") // 42, true
//	m.Has("connections")            // true
//	m.Delete("connections")
//
// Iterate with [Map.Range]. Return false from the callback to stop early:
//
//	m.Range(func(key string, value int) bool {
//		fmt.Printf("%s = %d\n", key, value)
//		return true // continue
//	})
//
// Extract keys or values as slices:
//
//	keys := m.Keys()     // []string
//	values := m.Values() // []int
//
// # Locker
//
// [Locker] protects a single value of any type with a [sync.Mutex].
// Use [Locker.Update] for atomic read-modify-write operations:
//
//	l := syncutil.NewLocker(Config{Timeout: 30 * time.Second})
//
//	cfg := l.Get()       // read a copy
//	ptr := l.GetPointer() // read pointer to value
//
//	l.Set(Config{Timeout: 60 * time.Second}) // replace
//
//	l.Update(func(c *Config) {
//		c.Timeout = 90 * time.Second // modify in place under lock
//	})
package syncutil
