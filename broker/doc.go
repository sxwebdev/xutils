// Package broker provides a generic, thread-safe pub/sub message broker
// with goroutine-based broadcasting and non-blocking delivery.
//
// # Core Concepts
//
// A [Broker] manages a set of subscriber channels and a single publish channel.
// When a message is published, the broker fans it out to every active subscriber.
// Delivery is non-blocking — if a subscriber's buffer is full, the message is
// dropped for that subscriber to avoid stalling other consumers.
//
// The broker is generic: [Broker][T] accepts any message type T.
//
// # Quick Start
//
//	b := broker.NewBroker[string]()
//	go b.Start()
//	defer b.Stop()
//
//	// Subscribe
//	ch := b.Subscribe()
//	defer b.Unsubscribe(ch)
//
//	// Publish
//	b.Publish("hello")
//
//	// Receive
//	msg := <-ch // "hello"
//
// # Publishing and Subscribing
//
// [Broker.Subscribe] returns a buffered channel (capacity 8) for receiving
// messages. Multiple goroutines can subscribe independently — each receives
// a copy of every published message:
//
//	ch1 := b.Subscribe()
//	ch2 := b.Subscribe()
//
//	b.Publish("event")
//	// both ch1 and ch2 receive "event"
//
// [Broker.Publish] sends a message to the internal publish queue (capacity 16).
// After [Broker.Stop] is called, Publish silently ignores new messages.
//
// # Unsubscribe
//
// [Broker.Unsubscribe] removes and closes a subscriber channel. It is safe
// to call from any goroutine:
//
//	ch := b.Subscribe()
//	// ... use ch ...
//	b.Unsubscribe(ch)
//
// # Graceful Shutdown
//
// [Broker.Stop] signals the broker goroutine to exit and waits for it to
// finish. All subscriber channels are closed during shutdown. After Stop,
// [Broker.Subscribe] returns nil and [Broker.Publish] is a no-op:
//
//	b.Stop()
//	ch := b.Subscribe() // returns nil
//	b.Publish("ignored") // no effect
//
// # Safety
//
// The broker handles panics from closed subscriber channels internally
// via [safeSend], so a misbehaving consumer cannot crash the broker.
// The [sync.Map] and [atomic.Bool] are used for thread-safe subscriber
// management and shutdown signaling.
//
// # Options
//
//   - [WithLogger] — set a structured logger for diagnostic warnings
package broker
