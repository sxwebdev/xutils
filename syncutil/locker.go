package syncutil

import "sync"

// Locker — обёртка для любого типа с мьютексом (хранит указатель на значение).
type Locker[T any] struct {
	mu    sync.Mutex
	value *T // Всегда храним указатель
}

// NewLocker создаёт новую защищённую структуру.
func NewLocker[T any](value T) *Locker[T] {
	return &Locker[T]{value: &value} // Сохраняем указатель на переданное значение
}

// Set заменяет значение полностью (атомарно).
func (l *Locker[T]) Set(value T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.value = &value
}

// Update безопасно изменяет значение "на месте".
// Функция `fn` получает указатель и может менять значение без возврата.
func (l *Locker[T]) Update(fn func(value *T)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn(l.value) // Передаём указатель, изменения сохраняются напрямую
}

// Get безопасно возвращает копию текущего значения.
func (l *Locker[T]) Get() T {
	l.mu.Lock()
	defer l.mu.Unlock()
	return *l.value // Разыменовываем указатель
}

// GetPointer безопасно возвращает указатель на текущее значение.
func (l *Locker[T]) GetPointer() *T {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.value // Возвращаем указатель
}
