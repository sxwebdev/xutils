# syncutil

Thread-safe generic containers: `Slice`, `Map`, and `Locker`.

## Features

- **Generic** — fully typed with Go 1.18+ generics, no `interface{}` assertions
- **RWMutex-based** — concurrent reads with exclusive writes (`Slice`, `Map`)
- **Range iteration** — `Map.Range` with early exit support
- **Atomic updates** — `Locker.Update` for safe read-modify-write on a single value

## Installation

```bash
go get github.com/sxwebdev/xutils/syncutil
```

## Slice

Thread-safe generic slice:

```go
s := syncutil.NewSlice[string]()
s.Add("hello")
s.AddMany([]string{"world", "!"})

all := s.GetAll() // []string{"hello", "world", "!"}
n := s.Len()      // 3
```

Pre-allocate with a fixed length:

```go
s := syncutil.NewSliceWithLength[int](10)
s.AddToIndex(0, 42)
```

### Slice API

| Method                    | Description                 |
| ------------------------- | --------------------------- |
| `NewSlice[T]()`           | Create empty slice          |
| `NewSliceWithLength[T](n)` | Create with capacity `n`  |
| `Add(item)`               | Append a single item        |
| `AddToIndex(i, item)`     | Set item at index           |
| `AddMany(items)`          | Append multiple items       |
| `GetAll()`                | Return all items            |
| `Len()`                   | Return size                 |

## Map

Thread-safe generic map with iteration:

```go
m := syncutil.NewMap[string, int]()
m.Set("connections", 42)

val, ok := m.Get("connections") // 42, true
m.Has("connections")            // true
m.Delete("connections")
```

Range iteration with early exit:

```go
m.Range(func(key string, value int) bool {
    fmt.Printf("%s = %d\n", key, value)
    return true // return false to stop
})
```

Extract keys or values:

```go
keys := m.Keys()     // []string
values := m.Values() // []int
```

### Map API

| Method                          | Description                     |
| ------------------------------- | ------------------------------- |
| `NewMap[K, V]()`                | Create empty map                |
| `NewMapWithCapacity[K, V](cap)` | Create with initial capacity    |
| `Set(key, value)`               | Set key-value pair              |
| `Get(key)`                      | Get value (ok pattern)          |
| `Delete(key)`                   | Remove key                      |
| `Has(key)`                      | Check key existence             |
| `GetAll()`                      | Return copy of all items        |
| `Keys()`                        | Return all keys as slice        |
| `Values()`                      | Return all values as slice      |
| `Range(fn)`                     | Iterate with early exit support |
| `Len()`                         | Return size                     |
| `Clear()`                       | Remove all items                |

## Locker

Mutex-protected single value with atomic update callback:

```go
l := syncutil.NewLocker(Config{Timeout: 30 * time.Second})

cfg := l.Get()        // read a copy
ptr := l.GetPointer() // read pointer to value

l.Set(Config{Timeout: 60 * time.Second}) // replace

l.Update(func(c *Config) {
    c.Timeout = 90 * time.Second // modify in place under lock
})
```

### Locker API

| Method          | Description                              |
| --------------- | ---------------------------------------- |
| `NewLocker(v)`  | Create with initial value                |
| `Set(value)`    | Replace value                            |
| `Get()`         | Read a copy of the value                 |
| `GetPointer()`  | Get pointer to the value                 |
| `Update(fn)`    | Modify value in place under lock         |
