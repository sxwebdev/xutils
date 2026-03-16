package broker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/sxwebdev/xutils/broker"
)

// TestBroker_BasicFlow verifies the basic scenario: subscribing, publishing, and reading messages.
func TestBroker_BasicFlow(t *testing.T) {
	b := broker.NewBroker[string]()

	// Start the broker in a separate goroutine
	go b.Start()

	// Subscribe
	subCh := b.Subscribe()
	if subCh == nil {
		t.Fatal("Subscribe() returned nil, although the broker is not closed")
	}

	// Publish several messages
	messages := []string{"hello", "world", "test"}
	for _, msg := range messages {
		b.Publish(msg)
	}

	received := make([]string, 0)
	// Read several messages from the subscription
	timeout := time.After(1 * time.Second)
loop:
	for {
		select {
		case msg, ok := <-subCh:
			if !ok {
				t.Fatal("The subscription channel was closed prematurely")
			}
			received = append(received, msg)
			if len(received) == len(messages) {
				break loop
			}
		case <-timeout:
			t.Fatal("Did not receive all messages")
		}
	}

	// Compare the received messages
	for i, r := range received {
		if r != messages[i] {
			t.Errorf("Expected message %q, but got %q", messages[i], r)
		}
	}

	// Verify that the channel is closed after Unsubscribe
	b.Unsubscribe(subCh)

	select {
	case _, ok := <-subCh:
		if ok {
			t.Error("Subscription not closed after Unsubscribe")
		}
	default:
		t.Error("Subscription channel is not closed and returns nothing; possible Unsubscribe error")
	}

	b.Stop()
}

// TestBroker_Stop verifies that after Stop(), all subscription channels are closed
// and new Publish/Subscribe calls are ignored (or do not cause errors).
func TestBroker_Stop(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()

	// Create two subscriptions
	subCh1 := b.Subscribe()
	subCh2 := b.Subscribe()

	// Publish something
	b.Publish(42)

	// Read the first message from the first subscription
	select {
	case msg := <-subCh1:
		if msg != 42 {
			t.Errorf("Expected 42, but got %d", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not receive message 42")
	}

	// Stop the broker
	b.Stop()

	// Verify that subscriptions are closed
	select {
	case _, ok := <-subCh1:
		if ok {
			t.Error("subCh1 is not closed after Stop()")
		}
	default:
		t.Error("subCh1 is not closed and did not return anything — possible error?")
	}

	// Wait until the channel either returns data or confirms closure
	closedOk := false
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case _, ok := <-subCh2:
			if !ok {
				// Channel is indeed closed
				closedOk = true
				break
			}
			// If ok == true, it means there is leftover data in the buffer
			// Continue reading until closure
		case <-timer.C:
			t.Error("subCh2 did not close within a reasonable time after Stop() (or there is unread data)")
			return
		}
		if closedOk {
			break
		}
	}

	if !closedOk {
		t.Error("subCh2 was not closed")
	}

	// Ensure new publishes do not panic and are simply ignored
	b.Publish(100500)

	// Ensure new subscriptions return nil
	subCh3 := b.Subscribe()
	if subCh3 != nil {
		t.Error("Expected nil subscription because the broker is already stopped, but got a valid channel")
	}
}

// TestBroker_MultipleSubscribers verifies that the same message is received by all subscribers.
func TestBroker_MultipleSubscribers(t *testing.T) {
	b := broker.NewBroker[string]()
	go b.Start()

	var wg sync.WaitGroup

	subCount := 5
	subs := make([]chan string, 0, subCount)
	for range subCount {
		subCh := b.Subscribe()
		if subCh == nil {
			t.Fatal("Subscribe() returned nil")
		}
		subs = append(subs, subCh)
	}

	testMsg := "broadcast"
	b.Publish(testMsg)

	// Each subscriber must receive the message
	wg.Add(subCount)
	for i := range subCount {
		go func(idx int, ch chan string) {
			defer wg.Done()

			select {
			case msg, ok := <-ch:
				if !ok {
					t.Errorf("[Sub %d] Subscription channel closed prematurely", idx)
					return
				}
				if msg != testMsg {
					t.Errorf("[Sub %d] Expected %q, but got %q", idx, testMsg, msg)
				}
			case <-time.After(time.Second):
				t.Errorf("[Sub %d] Did not receive message %q", idx, testMsg)
			}
		}(i, subs[i])
	}

	wg.Wait()
	b.Stop()
}

// TestBroker_Unsubscribe verifies that after unsubscribing, the subscriber does not receive new messages.
func TestBroker_Unsubscribe(t *testing.T) {
	b := broker.NewBroker[int]()
	go b.Start()

	subCh := b.Subscribe()
	b.Publish(1)

	select {
	case msg := <-subCh:
		if msg != 1 {
			t.Errorf("Expected 1, but got %d", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not receive the first message (1)")
	}

	// Now unsubscribe
	b.Unsubscribe(subCh)

	// After unsubscribing, the channel should be closed
	select {
	case _, ok := <-subCh:
		if ok {
			t.Fatal("Subscription channel remained open after Unsubscribe")
		}
	default:
		t.Fatal("Subscription channel is not returning data and is not closed — incorrect behavior")
	}

	// Publish new messages: the subscription should not receive them.
	b.Publish(2)
	b.Publish(3)
	time.Sleep(100 * time.Millisecond) // Small pause to allow the broker to distribute

	select {
	case msg, ok := <-subCh:
		if ok {
			t.Errorf("Subscription received message %d after Unsubscribe", msg)
		}
	default:
		// Correctly, the channel is already closed, and nothing arrives
	}

	b.Stop()
}

// TestBroker_PublishBeforeStart demonstrates that publishing before Start does not block:
// by default — this case may either block or collect messages in a buffer,
// depending on our decisions in NewBroker.
func TestBroker_PublishBeforeStart(t *testing.T) {
	b := broker.NewBroker[int]()

	// Attempt to Publish before Start
	// Thanks to publishCh being buffered, this will not block the test
	b.Publish(999)

	// Now start the broker
	go b.Start()

	// Subscribe
	subCh := b.Subscribe()
	if subCh == nil {
		t.Fatal("Subscribe() returned nil — the broker is not closed")
	}

	// Verify whether 999 arrives (depending on whether the broker started reading from publishCh)
	select {
	case msg := <-subCh:
		// It may be that 999 arrives, or it is lost —
		// this depends on when Start() began reading from publishCh.
		// To guarantee saving messages before Start, a different mechanism is needed.
		// For demonstration purposes, just log it.
		t.Logf("Received message %d (may be 999 if not lost)", msg)
	case <-time.After(500 * time.Millisecond):
		t.Log("Did not receive message 999 — it may have been read by the broker before subscribing")
	}

	b.Stop()
}

// TestBroker_ClosedChannelPanic verifies that if a subscriber closes their channel,
// the broker does not panic during broadcast.
func TestBroker_ClosedChannelPanic(t *testing.T) {
	b := broker.NewBroker[string]()
	go b.Start()

	subCh := b.Subscribe()
	if subCh == nil {
		t.Fatal("Subscribe() returned nil — the broker is not closed")
	}

	// The subscriber closes the channel themselves (which is generally NOT recommended)
	close(subCh)

	// Publish several messages to ensure the broker survives
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Expected the broker to survive, but got a panic: %v", r)
		}
	}()

	b.Publish("test1")
	b.Publish("test2")
	time.Sleep(100 * time.Millisecond) // Wait for processing

	// If we reach here without a panic — the test passes
	b.Stop()
}
