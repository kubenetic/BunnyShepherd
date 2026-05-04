package rabbitmq

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestRequeueWithLimitBelowThreshold tests that RequeueWithLimit returns Requeue
// when the message has not yet reached the max redeliveries.
func TestRequeueWithLimitBelowThreshold(t *testing.T) {
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

	// Create a delivery with x-death header indicating 2 prior deaths
	delivery := Delivery{
		MessageId: "test-msg-1",
		Headers: amqp.Table{
			"x-death": []interface{}{
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
			},
		},
	}

	// With max=5, we should get Requeue (2 < 5)
	outcome := c.RequeueWithLimit(delivery, 5)
	if outcome != RequeueOutcome {
		t.Errorf("RequeueWithLimit(2 deaths, max=5) = %v, want %v", outcome, RequeueOutcome)
	}
}

// TestRequeueWithLimitAtThreshold tests that RequeueWithLimit returns Reject
// when the message has reached the max redeliveries.
func TestRequeueWithLimitAtThreshold(t *testing.T) {
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

	// Create a delivery with x-death header indicating 5 prior deaths
	delivery := Delivery{
		MessageId: "test-msg-2",
		Headers: amqp.Table{
			"x-death": []interface{}{
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
			},
		},
	}

	// With max=5, we should get Reject (5 >= 5)
	outcome := c.RequeueWithLimit(delivery, 5)
	if outcome != Reject {
		t.Errorf("RequeueWithLimit(5 deaths, max=5) = %v, want %v", outcome, Reject)
	}
}

// TestRequeueWithLimitExceedsThreshold tests that RequeueWithLimit returns Reject
// when the message has exceeded the max redeliveries.
func TestRequeueWithLimitExceedsThreshold(t *testing.T) {
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

	// Create a delivery with x-death header indicating 7 prior deaths
	delivery := Delivery{
		MessageId: "test-msg-3",
		Headers: amqp.Table{
			"x-death": []interface{}{
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
				amqp.Table{"count": int64(1)},
			},
		},
	}

	// With max=5, we should get Reject (7 >= 5)
	outcome := c.RequeueWithLimit(delivery, 5)
	if outcome != Reject {
		t.Errorf("RequeueWithLimit(7 deaths, max=5) = %v, want %v", outcome, Reject)
	}
}

// TestRequeueWithLimitNoXDeathHeader tests that RequeueWithLimit returns Requeue
// when there is no x-death header (first delivery).
func TestRequeueWithLimitNoXDeathHeader(t *testing.T) {
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

	// Create a delivery with no x-death header
	delivery := Delivery{
		MessageId: "test-msg-4",
		Headers:   amqp.Table{},
	}

	// With max=5, we should get Requeue (0 < 5)
	outcome := c.RequeueWithLimit(delivery, 5)
	if outcome != RequeueOutcome {
		t.Errorf("RequeueWithLimit(no x-death, max=5) = %v, want %v", outcome, RequeueOutcome)
	}
}

// TestRequeueWithLimitNilHeaders tests that RequeueWithLimit returns Requeue
// when headers are nil.
func TestRequeueWithLimitNilHeaders(t *testing.T) {
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

	// Create a delivery with nil headers
	delivery := Delivery{
		MessageId: "test-msg-5",
		Headers:   nil,
	}

	// With max=5, we should get Requeue (0 < 5)
	outcome := c.RequeueWithLimit(delivery, 5)
	if outcome != RequeueOutcome {
		t.Errorf("RequeueWithLimit(nil headers, max=5) = %v, want %v", outcome, RequeueOutcome)
	}
}

// TestCountXDeathHeaders tests the countXDeathHeaders helper function.
func TestCountXDeathHeaders(t *testing.T) {
	tests := []struct {
		name     string
		headers  amqp.Table
		expected int
	}{
		{
			name:     "nil headers",
			headers:  nil,
			expected: 0,
		},
		{
			name:     "empty headers",
			headers:  amqp.Table{},
			expected: 0,
		},
		{
			name: "no x-death header",
			headers: amqp.Table{
				"other": "value",
			},
			expected: 0,
		},
		{
			name: "x-death with 1 entry",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"count": int64(1)},
				},
			},
			expected: 1,
		},
		{
			name: "x-death with 3 entries",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"count": int64(1)},
					amqp.Table{"count": int64(1)},
					amqp.Table{"count": int64(1)},
				},
			},
			expected: 3,
		},
		{
			name: "x-death with malformed type",
			headers: amqp.Table{
				"x-death": "not-an-array",
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := countXDeathHeaders(tt.headers)
			if got != tt.expected {
				t.Errorf("countXDeathHeaders() = %d, want %d", got, tt.expected)
			}
		})
	}
}
