package rabbitmq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestChannelCloseContextCancellation tests that per-message contexts are cancelled
// when the channel closes.
func TestChannelCloseContextCancellation(t *testing.T) {
	ctx := context.Background()
	cm, err := NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
	if err != nil {
		t.Skipf("skipping: cannot connect to RabbitMQ: %v", err)
	}
	defer cm.Close()

	c, err := NewConsumer(cm)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	defer c.Close()

	// Verify that inflight map is initialized
	if c.inflight == nil {
		t.Fatal("inflight map not initialized")
	}

	// Verify that onChannelClose cancels all in-flight handlers
	var wg sync.WaitGroup
	var contextCancelled atomic.Bool

	// Simulate an in-flight handler by adding a cancel func to the map
	ctx, cancel := context.WithCancel(context.Background())
	c.inflightMu.Lock()
	c.inflight[1] = cancel
	c.inflightMu.Unlock()

	// Start a goroutine that waits for context cancellation
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		contextCancelled.Store(true)
	}()

	// Call onChannelClose to cancel all in-flight handlers
	c.onChannelClose()

	// Wait for the context to be cancelled
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if !contextCancelled.Load() {
			t.Error("context was not cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for context cancellation")
	}

	// Verify that the inflight map was cleared
	c.inflightMu.Lock()
	if len(c.inflight) != 0 {
		t.Errorf("inflight map not cleared: %d entries remaining", len(c.inflight))
	}
	c.inflightMu.Unlock()
}

// TestChannelCloseMultipleHandlers tests that onChannelClose cancels all in-flight handlers.
func TestChannelCloseMultipleHandlers(t *testing.T) {
	ctx := context.Background()
	cm, err := NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
	if err != nil {
		t.Skipf("skipping: cannot connect to RabbitMQ: %v", err)
	}
	defer cm.Close()

	c, err := NewConsumer(cm)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	defer c.Close()

	// Create multiple in-flight handlers
	var contexts []context.Context
	var cancels []context.CancelFunc
	numHandlers := 5

	for i := 0; i < numHandlers; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		contexts = append(contexts, ctx)
		cancels = append(cancels, cancel)

		c.inflightMu.Lock()
		c.inflight[uint64(i+1)] = cancel
		c.inflightMu.Unlock()
	}

	// Call onChannelClose to cancel all in-flight handlers
	c.onChannelClose()

	// Verify that all contexts were cancelled
	for i, ctx := range contexts {
		select {
		case <-ctx.Done():
			// Expected
		case <-time.After(1 * time.Second):
			t.Errorf("context %d was not cancelled", i)
		}
	}

	// Verify that the inflight map was cleared
	c.inflightMu.Lock()
	if len(c.inflight) != 0 {
		t.Errorf("inflight map not cleared: %d entries remaining", len(c.inflight))
	}
	c.inflightMu.Unlock()
}

// TestInflightMapInitialization tests that the inflight map is properly initialized.
func TestInflightMapInitialization(t *testing.T) {
	ctx := context.Background()
	cm, err := NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
	if err != nil {
		t.Skipf("skipping: cannot connect to RabbitMQ: %v", err)
	}
	defer cm.Close()

	c, err := NewConsumer(cm)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	defer c.Close()

	// Verify that inflight map is initialized and empty
	if c.inflight == nil {
		t.Fatal("inflight map is nil")
	}

	c.inflightMu.Lock()
	if len(c.inflight) != 0 {
		t.Errorf("inflight map should be empty, got %d entries", len(c.inflight))
	}
	c.inflightMu.Unlock()
}

// TestOnChannelCloseEmptyInflight tests that onChannelClose handles empty inflight map gracefully.
func TestOnChannelCloseEmptyInflight(t *testing.T) {
	ctx := context.Background()
	cm, err := NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
	if err != nil {
		t.Skipf("skipping: cannot connect to RabbitMQ: %v", err)
	}
	defer cm.Close()

	c, err := NewConsumer(cm)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	defer c.Close()

	// Call onChannelClose on empty inflight map (should not panic)
	c.onChannelClose()

	// Verify that the inflight map is still empty
	c.inflightMu.Lock()
	if len(c.inflight) != 0 {
		t.Errorf("inflight map should be empty, got %d entries", len(c.inflight))
	}
	c.inflightMu.Unlock()
}
