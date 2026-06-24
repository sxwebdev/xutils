package broker_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/broker"
)

// TestBroker_UnsubscribeRacingStopDoesNotDoubleClose is a regression test for a
// double-close panic: Stop() (closeAllSubscribers) and Unsubscribe() both removed
// the channel from subs and closed it, but closeAllSubscribers used a plain
// Delete + unconditional close while Unsubscribe used LoadAndDelete. When the two
// raced, both could observe the channel and both call close(ch) -> "close of
// closed channel" panic that crashed the broker goroutine (and the process).
//
// Verified by reverting closeAllSubscribers to Delete + unconditional close:
// this test panics reliably within a few iterations under -race.
func TestBroker_UnsubscribeRacingStopDoesNotDoubleClose(t *testing.T) {
	for range 300 {
		b := broker.NewBroker[int]()
		go b.Start()

		const n = 32
		subs := make([]chan int, n)
		for i := range subs {
			subs[i] = b.Subscribe()
			require.NotNil(t, subs[i])
		}

		var wg sync.WaitGroup
		wg.Add(n + 1)
		for i := range subs {
			go func(ch chan int) {
				defer wg.Done()
				// Must never panic, regardless of whether Stop wins the close.
				require.NotPanics(t, func() { b.Unsubscribe(ch) })
			}(subs[i])
		}
		go func() {
			defer wg.Done()
			require.NotPanics(t, b.Stop)
		}()
		wg.Wait()
	}
}

// TestBroker_UnsubscribeUnknownOrTwiceIsNoOp verifies Unsubscribe is safe to call
// on a channel that is not (or no longer) subscribed: it must not panic and must
// not close the channel a second time.
func TestBroker_UnsubscribeUnknownOrTwiceIsNoOp(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()
	defer b.Stop()

	// A channel the broker never handed out.
	foreign := make(chan int, 1)
	require.NotPanics(t, func() { b.Unsubscribe(foreign) })
	// Foreign channel must remain open (not closed by the broker).
	select {
	case _, ok := <-foreign:
		require.Fail(t, "foreign channel was unexpectedly closed/written", "ok=%v", ok)
	default:
	}

	sub := b.Subscribe()
	require.NotNil(t, sub)
	b.Unsubscribe(sub)
	requireClosed(t, sub, time.Second)

	// Second Unsubscribe of the same channel must be a no-op (no double close).
	require.NotPanics(t, func() { b.Unsubscribe(sub) })
}

// TestBroker_ConcurrentPublishSubscribeAndDrain exercises the broker under
// simultaneous publish and subscribe traffic with subscribers draining and
// being torn down only via Stop. It is a race/safety test: it must complete
// without the -race detector firing and without any panic.
func TestBroker_ConcurrentPublishSubscribeAndDrain(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Publishers.
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					b.Publish(1)
				}
			}
		})
	}

	// Subscribers drain until their channel is closed by Stop.
	var delivered atomic.Int64
	const subscribers = 4
	for range subscribers {
		ch := b.Subscribe()
		require.NotNil(t, ch)
		wg.Add(1)
		go func(ch chan int) {
			defer wg.Done()
			for range ch { // ranges until Stop closes the channel
				delivered.Add(1)
			}
		}(ch)
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)

	// Stop closes subscriber channels, unblocking the range loops above.
	require.NotPanics(t, b.Stop)
	wg.Wait()

	require.Positive(t, delivered.Load(), "subscribers should have received some messages")
}

// TestBroker_ConcurrentPublishAndUnsubscribe is the regression test for the
// send-on-channel vs close-of-channel data race between broadcast (ch <- msg in
// safeSend, on the Start goroutine) and Unsubscribe (close(ch), formerly on the
// caller's goroutine).
//
// Several publishers hammer Publish while several subscribers each drain their
// channel and then Unsubscribe mid-stream, re-subscribing to keep traffic
// flowing. With the old direct-close Unsubscribe this trips the -race detector
// (DATA RACE on the channel buffer) and/or panics with "send on closed channel"
// that recover() turns into a flaky drop. With closes serialized into the Start
// goroutine it runs clean.
//
// PROOF it catches the bug: temporarily restore the old body of Unsubscribe to
//
//	func (b *Broker[T]) Unsubscribe(ch chan T) {
//	    if _, ok := b.subs.LoadAndDelete(ch); ok { close(ch) }
//	}
//
// and run `go test ./broker/ -run ConcurrentPublishAndUnsubscribe -race`: it
// reports "WARNING: DATA RACE" (concurrent send and close of the channel).
// Restoring the serialized fix makes it pass.
func TestBroker_ConcurrentPublishAndUnsubscribe(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var delivered atomic.Int64

	// Publishers keep the broadcast goroutine actively sending on subscriber
	// channels, maximizing the window for a send/close overlap.
	const publishers = 4
	for range publishers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					b.Publish(1)
				}
			}
		})
	}

	// Subscribers repeatedly subscribe, drain a bit, then Unsubscribe — exactly
	// the concurrent Publish + Unsubscribe pattern that used to race.
	const subscribers = 6
	for range subscribers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				ch := b.Subscribe()
				if ch == nil {
					return // broker stopped
				}
				// Drain a few messages, then tear the subscription down while
				// publishers are still sending.
				for range 5 {
					select {
					case _, ok := <-ch:
						if !ok {
							break // closed by Stop
						}
						delivered.Add(1)
					case <-stop:
					}
				}
				b.Unsubscribe(ch)
			}
		})
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)

	// Stop must cleanly tear everything down (close remaining subs, exit goroutine)
	// without panicking or double-closing while unsubscribes are still in flight.
	require.NotPanics(t, b.Stop)
	wg.Wait()

	require.Positive(t, delivered.Load(), "subscribers should have observed progress")

	// Teardown sanity: a fresh Subscribe after Stop returns nil, Publish is a
	// no-op, and Unsubscribe after Stop neither blocks nor panics.
	require.Nil(t, b.Subscribe())
	require.NotPanics(t, func() { b.Publish(1) })
}

// TestBroker_UnsubscribeAfterStopDoesNotBlockOrPanic verifies the deadlock-safe
// path: once the broker goroutine has exited (Stop done, doneCh closed), an
// Unsubscribe must return immediately via the doneCh branch instead of blocking
// forever trying to hand the channel to a goroutine that will never drain it.
func TestBroker_UnsubscribeAfterStopDoesNotBlockOrPanic(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()

	sub := b.Subscribe()
	require.NotNil(t, sub)

	b.Stop()                           // closes sub via closeAllSubscribers
	requireClosed(t, sub, time.Second) // already closed by Stop

	done := make(chan struct{})
	go func() {
		// Enough calls to overflow the buffered unsubCh, forcing the doneCh
		// branch of the select to be exercised (proves it cannot block).
		for range 100 {
			b.Unsubscribe(sub)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "Unsubscribe blocked after Stop")
	}
}
