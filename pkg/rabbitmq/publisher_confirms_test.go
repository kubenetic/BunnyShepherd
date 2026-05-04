package rabbitmq

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MockMessage implements the model.Message interface for testing.
type MockMessage struct {
	payload       []byte
	correlationId string
	messageId     string
	contentType   string
	headers       amqp.Table
}

func (m *MockMessage) GetPayload() ([]byte, error) {
	return m.payload, nil
}

func (m *MockMessage) GetCorrelationId() string {
	return m.correlationId
}

func (m *MockMessage) GetMessageId() string {
	return m.messageId
}

func (m *MockMessage) GetContentType() string {
	return m.contentType
}

func (m *MockMessage) GetHeaders() amqp.Table {
	return m.headers
}

// TestPublisherConfirmTimeout tests that Publish returns an error when confirm timeout occurs.
func TestPublisherConfirmTimeout(t *testing.T) {
	ctx := context.Background()
	cm, err := NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
	if err != nil {
		t.Skipf("skipping: cannot connect to RabbitMQ: %v", err)
	}
	defer cm.Close()

	// Create publisher with very short confirm timeout
	p, err := NewPublisher(cm, WithConfirmTimeout(100*time.Millisecond), WithMaxRetries(0))
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer p.Close()

	// Try to publish to a non-existent exchange (will timeout waiting for confirm)
	msg := &MockMessage{
		payload:       []byte("test"),
		correlationId: "corr-1",
		messageId:     "msg-1",
		contentType:   "application/json",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// This should fail with a timeout or nack error
	err = p.Publish(ctx, "non.existent.exchange", "test.key", false, msg)
	if err == nil {
		t.Error("Publish should have failed with timeout or nack")
	}
}

// TestPublisherConfirmTimeoutConfigurable tests that confirm timeout is configurable.
func TestPublisherConfirmTimeoutConfigurable(t *testing.T) {
	ctx := context.Background()
	cm, err := NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
	if err != nil {
		t.Skipf("skipping: cannot connect to RabbitMQ: %v", err)
	}
	defer cm.Close()

	// Create publisher with custom confirm timeout
	customTimeout := 3 * time.Second
	p, err := NewPublisher(cm, WithConfirmTimeout(customTimeout))
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer p.Close()

	// Verify that the config was set correctly
	if p.config.ConfirmTimeout != customTimeout {
		t.Errorf("ConfirmTimeout = %v, want %v", p.config.ConfirmTimeout, customTimeout)
	}
}

// TestPublisherConfirmDefaultTimeout tests that the default confirm timeout is 5 seconds.
func TestPublisherConfirmDefaultTimeout(t *testing.T) {
	ctx := context.Background()
	cm, err := NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
	if err != nil {
		t.Skipf("skipping: cannot connect to RabbitMQ: %v", err)
	}
	defer cm.Close()

	p, err := NewPublisher(cm)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer p.Close()

	// Verify that the default confirm timeout is 5 seconds
	expectedTimeout := 5 * time.Second
	if p.config.ConfirmTimeout != expectedTimeout {
		t.Errorf("Default ConfirmTimeout = %v, want %v", p.config.ConfirmTimeout, expectedTimeout)
	}
}

// TestPublisherConfirmChannelInitialization tests that the confirms channel is initialized.
func TestPublisherConfirmChannelInitialization(t *testing.T) {
	ctx := context.Background()
	cm, err := NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
	if err != nil {
		t.Skipf("skipping: cannot connect to RabbitMQ: %v", err)
	}
	defer cm.Close()

	p, err := NewPublisher(cm)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer p.Close()

	// Verify that the confirms channel is initialized
	if p.confirms == nil {
		t.Error("confirms channel is nil")
	}
}

// TestPublisherConfirmChannelCapacity tests that the confirms channel has proper capacity.
func TestPublisherConfirmChannelCapacity(t *testing.T) {
	ctx := context.Background()
	cm, err := NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
	if err != nil {
		t.Skipf("skipping: cannot connect to RabbitMQ: %v", err)
	}
	defer cm.Close()

	p, err := NewPublisher(cm)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer p.Close()

	// Verify that the confirms channel has capacity
	if cap(p.confirms) == 0 {
		t.Error("confirms channel has no capacity")
	}
}
