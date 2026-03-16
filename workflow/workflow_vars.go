package workflow

// SetVar stores a typed variable in the workflow's shared data store.
func SetVar[T any](w *Workflow, key string, value T) {
	w.varsMu.Lock()
	if w.vars == nil {
		w.vars = make(map[string]any)
	}
	w.vars[key] = value
	w.varsMu.Unlock()
}

// GetVar retrieves a typed variable from the workflow's shared data store.
func GetVar[T any](w *Workflow, key string) (T, bool) {
	w.varsMu.RLock()
	defer w.varsMu.RUnlock()

	var zero T
	if w.vars == nil {
		return zero, false
	}

	v, ok := w.vars[key]
	if !ok {
		return zero, false
	}

	typed, ok := v.(T)
	return typed, ok
}
