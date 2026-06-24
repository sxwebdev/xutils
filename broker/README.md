# broker

Generic thread-safe pub/sub message broker with goroutine-based broadcasting.

## Features

- **Generic** — `Broker[T]` works with any message type
- **Thread-safe** — concurrent publish/subscribe/unsubscribe from any goroutine
- **Non-blocking broadcast** — full subscriber buffers are skipped, never stall other consumers
- **Panic recovery** — closed subscriber channels are handled gracefully
- **Graceful shutdown** — `Stop()` waits for the broker goroutine to finish and closes all channels

## Installation

```bash
go get github.com/sxwebdev/xutils/broker
```

## Quick Start

```go
b := broker.NewBroker[string]()
go b.Start()
defer b.Stop()

// Subscribe
ch := b.Subscribe()
defer b.Unsubscribe(ch)

// Publish from another goroutine
go func() {
    b.Publish("hello")
    b.Publish("world")
}()

// Receive messages
for msg := range ch {
    fmt.Println(msg)
}
```

## Multiple Subscribers

Every subscriber receives a copy of every published message:

```go
ch1 := b.Subscribe()
ch2 := b.Subscribe()

b.Publish("event")

msg1 := <-ch1 // "event"
msg2 := <-ch2 // "event"
```

## Delivery Semantics

Recipients are frozen at publish time. A subscriber only receives messages
published **after** its `Subscribe()` call returns; a message published while
there are no subscribers is dropped rather than queued for a future one:

```go
b.Publish("early") // no subscribers yet → dropped

ch := b.Subscribe()
b.Publish("late")

msg := <-ch // "late" (never "early")
```

## Buffering

| Channel            | Buffer Size |
| ------------------ | ----------- |
| Publish (internal) | 16          |
| Subscriber         | 8           |

If a subscriber's buffer is full, the message is dropped for that subscriber. Other subscribers are not affected.

## Graceful Shutdown

```go
b.Stop()

// After Stop:
ch := b.Subscribe() // returns nil
b.Publish("ignored") // no effect
```

`Stop()` closes all subscriber channels, so range loops over subscriber channels will terminate naturally. `Publish` never blocks, even when called concurrently with `Stop`.

## Options

| Option       | Description                          |
| ------------ | ------------------------------------ |
| `WithLogger` | Set a structured logger for warnings |
