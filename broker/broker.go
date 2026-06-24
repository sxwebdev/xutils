package broker

import (
	"sync"
	"sync/atomic"

	"github.com/sxwebdev/xutils/loggerutil"
)

// Broker - a thread-safe broker for subscribers.
//   - T is the type of messages.
//   - All operations (Subscribe/Unsubscribe/Publish) can be called from different goroutines.
//   - Start() launches an internal goroutine to distribute messages.
//   - Stop() stops its operation and closes all subscriber channels.
type Broker[T any] struct {
	// subs stores all active subscriber channels using sync.Map,
	// to avoid blocking Subscribe/Unsubscribe during subscriber iteration.
	subs sync.Map // key: chan T, value: struct{}

	// publishCh - incoming messages for publishing.
	// Can be buffered if you want to avoid blocking Publish.
	publishCh chan envelope[T]

	// unsubCh - incoming unsubscribe requests. Unsubscribe routes the channel
	// here instead of closing it directly so that the *same* goroutine that
	// sends on subscriber channels (Start) is the only one that ever closes
	// them. This eliminates the concurrent "send on ch" vs "close(ch)" data
	// race between broadcast and Unsubscribe. It is buffered so a burst of
	// Unsubscribe calls during a Publish storm does not block callers.
	unsubCh chan chan T

	// stopCh - signal to stop the broker.
	stopCh chan struct{}

	// doneCh - signal that the broker's goroutine has completely stopped.
	doneCh chan struct{}

	// closed - a flag indicating that the broker is stopped (to prevent new subscriptions after Stop).
	closed atomic.Bool

	// logger is an optional logger for diagnostic messages.
	logger loggerutil.Logger
}

// envelope pairs a message with the snapshot of subscriber channels taken at
// publish time. Freezing the recipient set when Publish is called guarantees
// that a subscriber which joins afterwards does not receive this message.
type envelope[T any] struct {
	msg        T
	recipients []chan T
}

// BrokerOption configures a Broker.
type BrokerOption[T any] func(*Broker[T])

// WithLogger sets the logger for the broker.
func WithLogger[T any](l loggerutil.Logger) BrokerOption[T] {
	return func(b *Broker[T]) {
		b.logger = l
	}
}

// NewBroker initializes the broker but does NOT start its goroutine.
func NewBroker[T any](opts ...BrokerOption[T]) *Broker[T] {
	b := &Broker[T]{
		publishCh: make(chan envelope[T], 16), // can increase the buffer size if needed
		unsubCh:   make(chan chan T, 16),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Start starts the broker's goroutine: it reads from publishCh and distributes messages.
// Should be called once, usually via go broker.Start().
func (b *Broker[T]) Start() {
	defer close(b.doneCh)

	for {
		select {
		case <-b.stopCh:
			// Close all subscriber channels and exit
			b.closeAllSubscribers()
			return

		case ch := <-b.unsubCh:
			// Serialize the close into this goroutine: since this is the only
			// goroutine that sends on subscriber channels, performing the close
			// here means a send and a close can never race. LoadAndDelete makes
			// the close happen at most once even if the same channel is queued
			// twice or also reached by closeAllSubscribers.
			b.removeAndClose(ch)

		case env, ok := <-b.publishCh:
			if !ok {
				// Defensive (unreachable): publishCh is never closed anywhere in
				// this package, so this branch cannot be hit via the public API and
				// is intentionally left uncovered. Kept as a guard against future
				// changes that might close it.
				b.closeAllSubscribers()
				return
			}
			// Broadcast the message to the recipients snapshotted at publish time
			b.broadcast(env)
		}
	}
}

// Stop stops the broker: closes stopCh and waits for the goroutine to exit.
func (b *Broker[T]) Stop() {
	// Set the flag to prevent new subscriptions/publications
	if b.closed.CompareAndSwap(false, true) {
		close(b.stopCh) // signal the broker's goroutine
		<-b.doneCh      // wait for the goroutine to fully terminate
		// We don't close publishCh here to avoid a panic in other goroutines
		// if someone continues to call Publish (we show below how to ignore it).
	}
}

// Subscribe returns a new channel for receiving messages.
// If the broker is already closed, it returns nil.
func (b *Broker[T]) Subscribe() chan T {
	if b.closed.Load() {
		return nil
	}
	ch := make(chan T, 8) // buffered channel to avoid blocking the broker
	b.subs.Store(ch, struct{}{})
	return ch
}

// Unsubscribe removes a channel from the broker and closes it.
//
// The close is performed by the broker's Start goroutine, not by the caller:
// Unsubscribe only routes the channel through unsubCh. This guarantees the
// goroutine that broadcasts (and therefore sends on subscriber channels) is
// also the only one that closes them, so a concurrent Publish can never race
// the close of the channel being unsubscribed.
//
// Unsubscribe never blocks indefinitely:
//   - unsubCh is buffered, so a request normally returns immediately;
//   - if the broker goroutine has already exited (after Stop, doneCh closed),
//     the request is dropped because Stop already closed every subscriber
//     channel; the channel the caller passed is therefore already closed.
//
// Calling Unsubscribe on a channel the broker does not know about, or twice on
// the same channel, is a safe no-op (LoadAndDelete in the Start goroutine
// ensures the close happens at most once).
func (b *Broker[T]) Unsubscribe(ch chan T) {
	select {
	case b.unsubCh <- ch:
		// Queued; the Start goroutine will remove and close it.
	case <-b.doneCh:
		// The broker goroutine has stopped and will not drain unsubCh anymore.
		// Stop()'s closeAllSubscribers has already removed and closed every
		// remaining subscriber, so there is nothing left to do here.
	}
}

// Publish sends a message to the internal queue.
// If the broker is already closed, it ignores the message and returns.
func (b *Broker[T]) Publish(msg T) {
	if b.closed.Load() {
		// The broker is stopped - do not accept new messages.
		return
	}

	// Freeze the recipient set at publish time: a subscriber that joins after
	// this call returns must not receive this message, and a message published
	// with no subscribers is dropped rather than queued for a future one.
	var recipients []chan T
	b.subs.Range(func(key, _ any) bool {
		recipients = append(recipients, key.(chan T)) //nolint:forcetypeassert
		return true
	})
	if len(recipients) == 0 {
		return
	}

	select {
	case b.publishCh <- envelope[T]{msg: msg, recipients: recipients}:
	case <-b.stopCh:
		// Broker is stopping concurrently; drop instead of blocking forever on a
		// full buffer once the broker goroutine has stopped draining publishCh.
	}
}

// --- Helper methods ---

// broadcast sends the message to the recipients captured at publish time.
// On panic (e.g., if a subscriber closed their channel), it removes the subscription from subs.
func (b *Broker[T]) broadcast(env envelope[T]) {
	for _, ch := range env.recipients {
		safeSend(ch, env.msg, b.logger, func(ch chan T) {
			// If the send causes a panic (channel is closed),
			// remove it from the list of subscribers.
			b.subs.Delete(ch)
		})
	}
}

// removeAndClose atomically claims ownership of ch via LoadAndDelete and closes
// it exactly once. If ch is not (or no longer) in subs it is a no-op. It runs
// only on the Start goroutine, which is also the only sender on subscriber
// channels, so the close never races a send.
func (b *Broker[T]) removeAndClose(ch chan T) {
	if _, ok := b.subs.LoadAndDelete(ch); ok {
		close(ch)
	}
}

// closeAllSubscribers closes all channels in subs.
//
// It uses removeAndClose (LoadAndDelete + close) so a channel that was also
// queued for Unsubscribe is closed at most once — whichever path wins the
// delete performs the single close. It runs only on the Start goroutine.
func (b *Broker[T]) closeAllSubscribers() {
	b.subs.Range(func(key, _ any) bool {
		b.removeAndClose(key.(chan T)) //nolint:forcetypeassert
		return true
	})
}

// safeSend performs a non-blocking send to a channel with panic recovery.
// removeOnPanic is called if the channel is found to be closed (panic on send).
func safeSend[T any](ch chan T, msg T, logger loggerutil.Logger, removeOnPanic func(ch chan T)) {
	defer func() {
		if r := recover(); r != nil {
			// The channel is likely closed by the subscriber
			if logger != nil {
				logger.Warnf("subscriber channel is closed, removing from subs (panic: %v)", r)
			}
			removeOnPanic(ch)
		}
	}()
	select {
	case ch <- msg:
	default:
		// If there's no space in the channel's buffer - "skip" the message.
		// You can remove default if you want to wait for the subscriber to read.
	}
}
