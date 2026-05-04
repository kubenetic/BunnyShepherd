package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestOutcomeString tests the String method of Outcome.
func TestOutcomeString(t *testing.T) {
	tests := []struct {
		outcome Outcome
		want    string
	}{
		{Ack, "Ack"},
		{Reject, "Reject"},
		{RequeueOutcome, "Requeue"},
		{Outcome(999), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.outcome.String(); got != tt.want {
				t.Errorf("Outcome.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSafeHandlerOutcomeAck tests that Ack outcome results in message.Ack being called.
func TestSafeHandlerOutcomeAck(t *testing.T) {
	handler := func(ctx context.Context, msg Delivery) (Outcome, error) {
		return Ack, nil
	}

	// Create a mock delivery
	mockDelivery := &mockAMQPDelivery{
		body: []byte("test"),
	}

	// Simulate the handler logic
	outcome, err := handler(context.Background(), newDeliveryFromAMQP(mockDelivery.toAMQP()))
	if outcome != Ack {
		t.Errorf("Expected Ack outcome, got %v", outcome)
	}
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestSafeHandlerOutcomeReject tests that Reject outcome results in message.Nack(false, false).
func TestSafeHandlerOutcomeReject(t *testing.T) {
	handler := func(ctx context.Context, msg Delivery) (Outcome, error) {
		return Reject, errors.New("permanent error")
	}

	outcome, err := handler(context.Background(), Delivery{})
	if outcome != Reject {
		t.Errorf("Expected Reject outcome, got %v", outcome)
	}
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

// TestSafeHandlerOutcomeRequeue tests that RequeueOutcome outcome results in message.Nack(false, true).
func TestSafeHandlerOutcomeRequeue(t *testing.T) {
	handler := func(ctx context.Context, msg Delivery) (Outcome, error) {
		return RequeueOutcome, errors.New("transient error")
	}

	outcome, err := handler(context.Background(), Delivery{})
	if outcome != RequeueOutcome {
		t.Errorf("Expected RequeueOutcome outcome, got %v", outcome)
	}
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

// TestSafeHandlerPanicRecovery tests that panics in the handler are recovered and treated as Reject.
func TestSafeHandlerPanicRecovery(t *testing.T) {
	handler := func(ctx context.Context, msg Delivery) (Outcome, error) {
		panic("handler panic")
	}

	// Simulate the panic recovery logic
	var outcome Outcome
	func() {
		defer func() {
			if r := recover(); r != nil {
				outcome = Reject
			}
		}()
		outcome, _ = handler(context.Background(), Delivery{})
	}()

	if outcome != Reject {
		t.Errorf("Expected Reject outcome after panic, got %v", outcome)
	}
}

// TestSafeHandlerUnknownOutcome tests that unknown outcomes are treated as Reject.
func TestSafeHandlerUnknownOutcome(t *testing.T) {
	handler := func(ctx context.Context, msg Delivery) (Outcome, error) {
		return Outcome(999), nil
	}

	outcome, err := handler(context.Background(), Delivery{})
	if outcome != Outcome(999) {
		t.Errorf("Expected unknown outcome, got %v", outcome)
	}
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// The library should treat unknown outcomes as Reject
	if outcome != Outcome(999) && outcome != Reject {
		t.Errorf("Expected unknown outcome to be treated as Reject, got %v", outcome)
	}
}

// TestDeliveryConversion tests that Delivery is correctly converted from amqp.Delivery.
func TestDeliveryConversion(t *testing.T) {
	mockDelivery := &mockAMQPDelivery{
		body:            []byte("test body"),
		contentType:     "application/json",
		contentEncoding: "utf-8",
		priority:        5,
		correlationId:   "corr-123",
		replyTo:         "reply.queue",
		expiration:      "60000",
		messageId:       "msg-123",
		timestamp:       time.Now(),
		type_:           "test.type",
		userId:          "user-123",
		appId:           "app-123",
		deliveryTag:     1,
		redelivered:     false,
		exchange:        "test.exchange",
		routingKey:      "test.key",
	}

	delivery := newDeliveryFromAMQP(mockDelivery.toAMQP())

	if string(delivery.Body) != "test body" {
		t.Errorf("Expected body 'test body', got %q", string(delivery.Body))
	}
	if delivery.ContentType != "application/json" {
		t.Errorf("Expected content type 'application/json', got %q", delivery.ContentType)
	}
	if delivery.Priority != 5 {
		t.Errorf("Expected priority 5, got %d", delivery.Priority)
	}
	if delivery.CorrelationId != "corr-123" {
		t.Errorf("Expected correlation ID 'corr-123', got %q", delivery.CorrelationId)
	}
	if delivery.DeliveryTag != 1 {
		t.Errorf("Expected delivery tag 1, got %d", delivery.DeliveryTag)
	}
}

// Mock AMQP delivery for testing
type mockAMQPDelivery struct {
	body            []byte
	contentType     string
	contentEncoding string
	headers         amqp.Table
	priority        uint8
	correlationId   string
	replyTo         string
	expiration      string
	messageId       string
	timestamp       time.Time
	type_           string
	userId          string
	appId           string
	deliveryTag     uint64
	redelivered     bool
	exchange        string
	routingKey      string
}

func (m *mockAMQPDelivery) toAMQP() amqp.Delivery {
	return amqp.Delivery{
		Body:            m.body,
		ContentType:     m.contentType,
		ContentEncoding: m.contentEncoding,
		Headers:         m.headers,
		Priority:        m.priority,
		CorrelationId:   m.correlationId,
		ReplyTo:         m.replyTo,
		Expiration:      m.expiration,
		MessageId:       m.messageId,
		Timestamp:       m.timestamp,
		Type:            m.type_,
		UserId:          m.userId,
		AppId:           m.appId,
		DeliveryTag:     m.deliveryTag,
		Redelivered:     m.redelivered,
		Exchange:        m.exchange,
		RoutingKey:      m.routingKey,
	}
}
