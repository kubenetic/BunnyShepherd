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

// newConsumerForTest constructs a minimal Consumer without a live AMQP connection.
// It is only valid for calling methods that do not use the channel (e.g. RequeueWithLimit).
func newConsumerForTest() *Consumer {
	return &Consumer{
		config:   DefaultConsumerConfig(),
		stopCh:   make(chan struct{}),
		inflight: make(map[uint64]context.CancelFunc),
	}
}

// TestRequeueWithLimitSingleEntryHighCount is the C-3 regression test.
// RabbitMQ keeps a single x-death entry per (queue, reason) pair and increments
// its "count" field on each re-death. The old code returned len(array)==1 and
// never tripped the limit; the fixed code sums count fields.
func TestRequeueWithLimitSingleEntryHighCount(t *testing.T) {
	c := newConsumerForTest()

	// Simulate a message dead-lettered 5 times from the same queue/reason:
	// RabbitMQ produces one entry with count=5, not five entries.
	delivery := Delivery{
		MessageId: "poison-single-entry",
		Headers: amqp.Table{
			"x-death": []interface{}{
				amqp.Table{"count": int64(5), "queue": "q.file.scan", "reason": "rejected"},
			},
		},
	}

	// With max=5, count==5 >= 5 → must Reject (poison message protection).
	// Before the fix this returned RequeueOutcome because len(array)==1 < 5.
	outcome := c.RequeueWithLimit(delivery, 5)
	if outcome != Reject {
		t.Errorf("RequeueWithLimit(single entry count=5, max=5) = %v, want Reject (C-3 regression)", outcome)
	}
}

// TestRequeueWithLimitMultiEntrySum is the C-3 multi-entry regression test.
// Two distinct (queue, reason) pairs each accumulated their own count.
// The total across entries must be summed to determine the true death count.
func TestRequeueWithLimitMultiEntrySum(t *testing.T) {
	c := newConsumerForTest()

	// Two entries: counts 3 and 4 → total 7.
	delivery := Delivery{
		MessageId: "poison-multi-entry",
		Headers: amqp.Table{
			"x-death": []interface{}{
				amqp.Table{"count": int64(3), "queue": "q.retry-1", "reason": "rejected"},
				amqp.Table{"count": int64(4), "queue": "q.retry-2", "reason": "expired"},
			},
		},
	}

	// Total=7 >= max=5 → Reject.
	// Before the fix this returned RequeueOutcome because len(array)==2 < 5.
	outcome := c.RequeueWithLimit(delivery, 5)
	if outcome != Reject {
		t.Errorf("RequeueWithLimit(entries counts 3+4=7, max=5) = %v, want Reject (C-3 regression)", outcome)
	}

	// Total=7 < max=10 → RequeueOutcome.
	outcome = c.RequeueWithLimit(delivery, 10)
	if outcome != RequeueOutcome {
		t.Errorf("RequeueWithLimit(entries counts 3+4=7, max=10) = %v, want RequeueOutcome", outcome)
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
			name: "x-death with 1 entry count=1",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"count": int64(1)},
				},
			},
			expected: 1,
		},
		{
			name: "x-death with 3 entries each count=1",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"count": int64(1)},
					amqp.Table{"count": int64(1)},
					amqp.Table{"count": int64(1)},
				},
			},
			expected: 3,
		},
		// C-3 regression: RabbitMQ increments the count field within a single
		// entry when the same (queue, reason) pair dead-letters the message
		// repeatedly. The array stays length 1 but count grows. We must sum
		// count fields, not count array entries.
		{
			name: "single entry with count=5 (RabbitMQ repeated DLQ from same queue)",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"count": int64(5)},
				},
			},
			expected: 5, // was incorrectly 1 before the fix
		},
		{
			name: "single entry with count=3 (below limit)",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"count": int64(3)},
				},
			},
			expected: 3,
		},
		// Multi-entry case: two distinct (queue, reason) pairs, each with
		// their own accumulated count. Total must be the sum of all counts.
		{
			name: "two entries with counts 3 and 4 (total=7)",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"count": int64(3), "queue": "q.retry-1"},
					amqp.Table{"count": int64(4), "queue": "q.retry-2"},
				},
			},
			expected: 7, // was incorrectly 2 (len) before the fix
		},
		{
			name: "entry missing count field is skipped (treated as 0)",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"queue": "q.no-count"},
					amqp.Table{"count": int64(2)},
				},
			},
			expected: 2,
		},
		{
			name: "count as int32 (robustness)",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"count": int32(4)},
				},
			},
			expected: 4,
		},
		{
			name: "count as int (robustness)",
			headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{"count": int(6)},
				},
			},
			expected: 6,
		},
		{
			name: "x-death with malformed type",
			headers: amqp.Table{
				"x-death": "not-an-array",
			},
			expected: 0,
		},
		{
			name: "entry is not a table (skipped gracefully)",
			headers: amqp.Table{
				"x-death": []interface{}{
					"not-a-table",
					amqp.Table{"count": int64(2)},
				},
			},
			expected: 2,
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
