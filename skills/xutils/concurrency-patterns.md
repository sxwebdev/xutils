# Concurrency Patterns

## syncutil — Thread-safe containers

### Map[K, V]

Thread-safe generic map with RWMutex. Returns copies from `GetAll()` to prevent external mutation.

```go
import "github.com/sxwebdev/xutils/syncutil"

m := syncutil.NewMap[string, int]()
// or with pre-allocated capacity:
// m := syncutil.NewMapWithCapacity[string, int](100)

m.Set("users", 42)

val, ok := m.Get("users") // 42, true
m.Has("users")            // true
m.Delete("users")
m.Len()                   // 0

// Iterate (read-locked for the entire iteration)
m.Range(func(key string, value int) bool {
    fmt.Println(key, value)
    return true // return false to stop
})

all := m.GetAll()   // returns a copy
keys := m.Keys()    // []string
values := m.Values() // []int
m.Clear()
```

### Slice[T]

Thread-safe generic slice with RWMutex.

```go
s := syncutil.NewSlice[string]()
// or with pre-allocated length:
// s := syncutil.NewSliceWithLength[string](10)

s.Add("item1")
s.AddMany([]string{"item2", "item3"})
s.AddToIndex(0, "replaced") // set by index

items := s.GetAll() // []string
length := s.Len()   // 3
```

### Locker[T]

Mutex-protected single value wrapper. Useful for atomic read-modify-write on any type.

```go
type Config struct {
    MaxRetries int
    Timeout    time.Duration
}

cfg := syncutil.NewLocker(Config{MaxRetries: 3, Timeout: 5 * time.Second})

// Read
current := cfg.Get() // returns a copy

// Replace
cfg.Set(Config{MaxRetries: 5, Timeout: 10 * time.Second})

// In-place update (holds mutex during fn)
cfg.Update(func(c *Config) {
    c.MaxRetries = 10
})

// Get pointer (careful — holds no lock after return)
ptr := cfg.GetPointer()
```

## broker — Pub/Sub message broker

Generic thread-safe in-process pub/sub. Non-blocking broadcast (full subscriber buffers are skipped). Panic recovery on closed channels.

```go
import "github.com/sxwebdev/xutils/broker"

type Event struct {
    Type    string
    Payload any
}

b := broker.NewBroker[Event](
    broker.WithLogger[Event](logger), // optional
)

// Start broker goroutine
go b.Start()

// Subscribe (returns buffered channel, cap=8)
ch := b.Subscribe()
defer b.Unsubscribe(ch)

// Publish (non-blocking, drops if subscriber buffer is full)
b.Publish(Event{Type: "user.created", Payload: userID})

// Consume
go func() {
    for event := range ch {
        fmt.Println("received:", event.Type)
    }
}()

// Graceful shutdown
b.Stop() // closes all subscriber channels and waits
```

**Key details:**
- Publish channel buffer: 16
- Subscriber channel buffer: 8
- `Subscribe()` returns `nil` after `Stop()` is called
- `Publish()` is a no-op after `Stop()`
- Closed subscriber channels are automatically removed from the subs list

## loopper — Periodic task runner

Runs a function on a fixed interval. Prevents overlapping execution. Supports manual trigger, per-execution context timeout, and panic recovery.

```go
import "github.com/sxwebdev/xutils/loopper"

sync := loopper.New(
    func(ctx context.Context) {
        // ctx has the per-execution timeout
        if err := syncData(ctx); err != nil {
            log.Printf("sync failed: %v", err)
        }
    },
    loopper.WithPeriod(30*time.Second),         // default: 60s
    loopper.WithContextTimeout(10*time.Second),  // default: 30s
    loopper.WithLeading(true),                   // run immediately on Start
    loopper.WithLogger(logger),                  // optional
)

// Start periodic loop
sync.Start(ctx)

// Force immediate execution (returns false if already running)
triggered := sync.Trigger(ctx)

// Graceful shutdown
sync.Stop() // stop scheduling new ticks
sync.Wait() // wait for current execution to finish
```

**Key details:**
- Overlapping prevention: if fn is still running when the next tick fires, the tick is skipped
- Panic recovery: panics are logged and the loop continues
- Leading mode: executes fn immediately on `Start()` before the first tick
- `Trigger()` returns `false` if fn is already running
