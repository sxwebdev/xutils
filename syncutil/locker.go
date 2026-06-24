package syncutil

import "sync"

// Locker is a mutex-protected wrapper for any type (stores a pointer to the value).
type Locker[T any] struct {
	mu    sync.Mutex
	value *T
}

// NewLocker creates a new mutex-protected value.
func NewLocker[T any](value T) *Locker[T] {
	return &Locker[T]{value: &value}
}

// Set replaces the value atomically.
func (l *Locker[T]) Set(value T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.value = &value
}

// Update safely modifies the value in-place.
// The function fn receives a pointer and can modify the value directly.
func (l *Locker[T]) Update(fn func(value *T)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn(l.value)
}

// Get safely returns a copy of the current value.
func (l *Locker[T]) Get() T {
	l.mu.Lock()
	defer l.mu.Unlock()
	return *l.value
}

// GetPointer returns a pointer to the current value.
//
// WARNING: the returned pointer escapes the lock. Reading or writing through it
// is NOT synchronized — doing so concurrently with Set or Update is a data race.
// Prefer [Locker.Get] (copy) or [Locker.Update] (mutate under the lock); use
// GetPointer only when the caller guarantees no concurrent access.
func (l *Locker[T]) GetPointer() *T {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.value
}
