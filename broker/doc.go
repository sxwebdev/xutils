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
// The set of recipients is frozen when Publish is called: a subscriber that
// joins after Publish returns does not receive that message, and a message
// published while there are no subscribers is dropped rather than queued for a
// future one. After [Broker.Stop] is called, Publish silently ignores new
// messages and never blocks, even when racing a concurrent Stop.
//
// # Unsubscribe
//
// [Broker.Unsubscribe] removes and closes a subscriber channel. It is safe to
// call from any goroutine, including concurrently with [Broker.Publish] on the
// same channel:
//
//	ch := b.Subscribe()
//	// ... use ch ...
//	b.Unsubscribe(ch)
//
// The channel is closed by the broker's own goroutine — the same goroutine that
// broadcasts (and therefore sends on subscriber channels) — not by the caller.
// Because the only sender is also the only closer, a concurrent Publish can
// never race the close of the channel being unsubscribed. The close is therefore
// asynchronous: when Unsubscribe returns the channel may not be closed yet, and
// a message published just before the unsubscribe is processed may still be
// delivered. Callers that need to observe the close should read from the channel
// until it reports closed (ok == false).
//
// Unsubscribe never blocks indefinitely. The request queue is buffered, and if
// the broker has already been stopped Unsubscribe returns at once (the channel
// was already closed by Stop). Calling it on an unknown channel, or twice on the
// same channel, is a safe no-op — never a double close.
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
// All public operations — Subscribe, Unsubscribe, Publish, and Stop — are safe
// to call concurrently from any goroutine, including Unsubscribe concurrent with
// Publish on the same channel. This is achieved by serializing every close of a
// subscriber channel into the single broker goroutine, so a send and a close of
// the same channel never overlap (no send-on-closed-channel data race).
//
// One case remains the caller's responsibility: if a consumer closes its *own*
// subscriber channel directly (instead of calling Unsubscribe) while a Publish
// is in flight, that close races the broker's send. The broker contains the
// resulting panic via [safeSend] and drops the broken subscriber, so it cannot
// crash the broker — but the race itself is avoided only by using Unsubscribe.
//
// The [sync.Map] and [atomic.Bool] are used for thread-safe subscriber
// management and shutdown signaling.
//
// # Options
//
//   - [WithLogger] — set a structured logger for diagnostic warnings
package broker
