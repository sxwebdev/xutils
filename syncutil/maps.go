package syncutil

import (
	"maps"
	"sync"
)

type Map[K comparable, V any] struct {
	mu    *sync.RWMutex
	items map[K]V
}

func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		mu:    &sync.RWMutex{},
		items: make(map[K]V),
	}
}

func NewMapWithCapacity[K comparable, V any](capacity int) *Map[K, V] {
	return &Map[K, V]{
		mu:    &sync.RWMutex{},
		items: make(map[K]V, capacity),
	}
}

func (m *Map[K, V]) Set(key K, value V) {
	m.mu.Lock()
	m.items[key] = value
	m.mu.Unlock()
}

func (m *Map[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.items[key]
	return value, ok
}

func (m *Map[K, V]) Delete(key K) {
	m.mu.Lock()
	delete(m.items, key)
	m.mu.Unlock()
}

func (m *Map[K, V]) Has(key K) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.items[key]
	return ok
}

func (m *Map[K, V]) GetAll() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to prevent external modifications
	result := make(map[K]V, len(m.items))
	maps.Copy(result, m.items)
	return result
}

func (m *Map[K, V]) Keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]K, 0, len(m.items))
	for k := range m.items {
		keys = append(keys, k)
	}
	return keys
}

func (m *Map[K, V]) Values() []V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	values := make([]V, 0, len(m.items))
	for _, v := range m.items {
		values = append(values, v)
	}
	return values
}

func (m *Map[K, V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.items)
}

func (m *Map[K, V]) Clear() {
	m.mu.Lock()
	m.items = make(map[K]V)
	m.mu.Unlock()
}
