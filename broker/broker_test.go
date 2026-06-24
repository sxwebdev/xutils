package broker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/broker"
	"github.com/sxwebdev/xutils/loggerutil"
)

// recvWithin reads one value from ch or fails the test on timeout.
func recvWithin[T any](t *testing.T, ch <-chan T, d time.Duration) T {
	t.Helper()
	select {
	case v, ok := <-ch:
		require.True(t, ok, "channel closed while a value was expected")
		return v
	case <-time.After(d):
		require.FailNow(t, "timed out waiting for a message")
		var zero T
		return zero
	}
}

// requireClosed asserts that ch is closed (drains any buffered values first).
func requireClosed[T any](t *testing.T, ch <-chan T, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed as expected
			}
			// buffered leftover — keep draining until the close is observed
		case <-deadline:
			require.FailNow(t, "channel was not closed in time")
		}
	}
}

func TestBroker_DeliversInOrderToSubscriber(t *testing.T) {
	b := broker.NewBroker[string]()
	go b.Start()
	defer b.Stop()

	sub := b.Subscribe()
	require.NotNil(t, sub)

	messages := []string{"hello", "world", "test"}
	for _, msg := range messages {
		b.Publish(msg)
	}

	// A single subscriber behind one broadcast goroutine must observe FIFO order.
	for _, want := range messages {
		require.Equal(t, want, recvWithin(t, sub, time.Second))
	}
}

func TestBroker_UnsubscribeClosesChannelAndStopsDelivery(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()
	defer b.Stop()

	sub := b.Subscribe()
	b.Publish(1)
	require.Equal(t, 1, recvWithin(t, sub, time.Second))

	b.Unsubscribe(sub)
	requireClosed(t, sub, time.Second)

	// Messages published after unsubscribe must never reach the closed channel.
	b.Publish(2)
	b.Publish(3)
	time.Sleep(50 * time.Millisecond) // let the broker process

	_, ok := <-sub
	require.False(t, ok, "no value must arrive after unsubscribe; channel stays closed")
}

func TestBroker_BroadcastsToAllSubscribers(t *testing.T) {
	b := broker.NewBroker[string]()
	go b.Start()
	defer b.Stop()

	const subCount = 5
	subs := make([]chan string, 0, subCount)
	for range subCount {
		sub := b.Subscribe()
		require.NotNil(t, sub)
		subs = append(subs, sub)
	}

	const msg = "broadcast"
	b.Publish(msg)

	var wg sync.WaitGroup
	wg.Add(subCount)
	for i := range subCount {
		go func(ch chan string) {
			defer wg.Done()
			require.Equal(t, msg, recvWithin(t, ch, time.Second))
		}(subs[i])
	}
	wg.Wait()
}

func TestBroker_StopClosesSubscribersAndRejectsNewWork(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()

	sub1 := b.Subscribe()
	sub2 := b.Subscribe()

	b.Publish(42)
	require.Equal(t, 42, recvWithin(t, sub1, time.Second))

	b.Stop()

	// Every subscriber channel must be closed after Stop.
	requireClosed(t, sub1, time.Second)
	requireClosed(t, sub2, time.Second)

	// Publish after Stop must be a no-op (must not panic or block).
	require.NotPanics(t, func() { b.Publish(100500) })

	// Subscribe after Stop must return nil.
	require.Nil(t, b.Subscribe())
}

func TestBroker_StopIsIdempotent(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()

	b.Stop()
	// A second Stop must not panic or block on the already-closed channels.
	require.NotPanics(t, b.Stop)
}

func TestBroker_SurvivesSubscriberClosingItsOwnChannel(t *testing.T) {
	// A logger exercises the warn path inside safeSend on a closed channel.
	b := broker.NewBroker(broker.WithLogger[string](loggerutil.NewTestLogger()))
	go b.Start()
	defer b.Stop()

	bad := b.Subscribe()
	healthy := b.Subscribe()
	require.NotNil(t, bad)
	require.NotNil(t, healthy)

	// Misbehaving subscriber closes its own channel.
	close(bad)

	// The broker must drop the broken subscriber and keep serving the rest.
	b.Publish("first")
	require.Equal(t, "first", recvWithin(t, healthy, time.Second))

	b.Publish("second")
	require.Equal(t, "second", recvWithin(t, healthy, time.Second))
}

func TestBroker_DropsMessagesWhenSubscriberBufferIsFull(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()
	defer b.Stop()

	sub := b.Subscribe()
	require.NotNil(t, sub)

	// Publish far more than the subscriber buffer without reading. The broker
	// must never block on a slow subscriber — excess messages are dropped.
	const published = 100
	done := make(chan struct{})
	go func() {
		for i := range published {
			b.Publish(i)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "Publish blocked on a slow subscriber")
	}

	time.Sleep(50 * time.Millisecond) // let the broker finish broadcasting

	// Drain whatever is buffered: it must be bounded (drops happened), proving
	// the broker did not block waiting for the slow subscriber.
	var got int
	for {
		select {
		case _, ok := <-sub:
			if !ok {
				require.FailNow(t, "subscriber closed unexpectedly")
			}
			got++
		default:
			require.Positive(t, got, "subscriber should have buffered at least one message")
			require.Less(t, got, published, "excess messages must be dropped, not all delivered")
			return
		}
	}
}

func TestBroker_PublishBeforeStartDoesNotBreakBroker(t *testing.T) {
	b := broker.NewBroker[int]()

	// Publishing before Start (and with no subscribers) must not block or break
	// anything; the message is dropped because there is nobody to deliver it to.
	require.NotPanics(t, func() { b.Publish(999) })

	go b.Start()
	defer b.Stop()

	// The broker must work normally afterwards: a fresh subscriber receives the
	// message published after it subscribed — and never the dropped 999.
	sub := b.Subscribe()
	require.NotNil(t, sub)

	b.Publish(1)
	require.Equal(t, 1, recvWithin(t, sub, time.Second))
}

func TestBroker_LateSubscriberDoesNotReceivePriorMessages(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()
	defer b.Stop()

	// Published while nobody is subscribed.
	b.Publish(1)
	b.Publish(2)
	time.Sleep(50 * time.Millisecond) // let the broker process the queue

	// A subscriber that joins afterwards must start with a clean slate: it only
	// receives messages published after Subscribe returned.
	sub := b.Subscribe()
	require.NotNil(t, sub)

	b.Publish(3)
	require.Equal(t, 3, recvWithin(t, sub, time.Second))

	// 1 and 2 (published before Subscribe) must never arrive.
	select {
	case msg := <-sub:
		require.Failf(t, "received a message published before subscribing", "got %d", msg)
	default:
	}
}

func TestBroker_PublishDoesNotBlockWhenRacingStop(t *testing.T) {
	// Regression: a Publish that passes the closed check just before Stop must
	// not block forever on a full buffer once the broker stops draining it.
	b := broker.NewBroker[int]()
	go b.Start()

	sub := b.Subscribe() // a real subscriber so publishes are actually enqueued
	require.NotNil(t, sub)

	const publishers = 50
	var wg sync.WaitGroup
	wg.Add(publishers)
	for range publishers {
		go func() {
			defer wg.Done()
			for i := range 1000 {
				b.Publish(i)
			}
		}()
	}

	time.Sleep(time.Millisecond) // let publishers build up buffer pressure
	b.Stop()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Publish blocked after Stop — goroutine leak from the Publish/Stop race")
	}
}
