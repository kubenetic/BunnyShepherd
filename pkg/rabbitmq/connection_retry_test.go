package rabbitmq

// SF-2: Tests for the initial-connect retry loop.
//
// Approach: inject a fake dial step via the ConnectionManager.dialFn field
// (a package-private seam added specifically for testing). This lets every
// test call the REAL production method initialConnectWithRetry — including
// its backoff.Jitter call — without touching a live AMQP broker.
//
// Why this approach over the previous testableInitialConnectWithRetry copy:
//   - The copy used `sleep = backoffTime` instead of `backoff.Jitter(backoffTime)`,
//     so it tested a different algorithm than what ships.
//   - A function-field seam is the idiomatic Go pattern for injecting
//     collaborators in unit tests without changing the public API.
//   - The seam is minimal: one unexported field, nil-checked at the call site,
//     zero impact on production code paths.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newManagerWithFakeDial builds a minimal ConnectionManager whose dial step
// is replaced by fakeDial. The manager is not started (no watch goroutine),
// so it is safe to call initialConnectWithRetry directly.
func newManagerWithFakeDial(fakeDial func(string, *amqp.Config) error) *ConnectionManager {
	return &ConnectionManager{
		url:      "amqp://test-host:5672/",
		shutdown: make(chan bool),
		dialFn:   fakeDial,
	}
}

// failNThenSucceed returns a dial function that returns an error for the first
// n calls and nil on the (n+1)-th call. The atomic counter records total calls.
func failNThenSucceed(n int) (fn func(string, *amqp.Config) error, calls *int32) {
	var c int32
	fn = func(_ string, _ *amqp.Config) error {
		if int(atomic.AddInt32(&c, 1)) <= n {
			return errors.New("transient dial error")
		}
		return nil
	}
	return fn, &c
}

// ---------------------------------------------------------------------------
// Test: success after N transient failures (exercises real jitter path)
// ---------------------------------------------------------------------------

func TestInitialConnectRetry_SuccessAfterFailures(t *testing.T) {
	const failCount = 3
	fakeDial, calls := failNThenSucceed(failCount)
	m := newManagerWithFakeDial(fakeDial)

	ctx := context.Background()
	// Use a short maxElapsed that is still long enough for 4 attempts with
	// jittered 1 ms → 8 ms backoff (well under 1 s total).
	err := m.initialConnectWithRetry(ctx, m.url, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("expected success after %d failures, got: %v", failCount, err)
	}

	got := int(atomic.LoadInt32(calls))
	want := failCount + 1
	if got != want {
		t.Errorf("dial called %d times, want %d", got, want)
	}
}

// ---------------------------------------------------------------------------
// Test: context cancellation returns promptly
// ---------------------------------------------------------------------------

func TestInitialConnectRetry_ContextCancellation(t *testing.T) {
	// Always-failing dial — loop would run until deadline without cancellation.
	fakeDial := func(_ string, _ *amqp.Config) error {
		return errors.New("always fails")
	}
	m := newManagerWithFakeDial(fakeDial)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := m.initialConnectWithRetry(ctx, m.url, nil, 10*time.Second)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error on context cancellation, got nil")
	}
	// The error wraps ctx.Err() — unwrap to check.
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected wrapped context.Canceled, got: %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("cancellation took too long: %v (want < 500ms)", elapsed)
	}
}

// ---------------------------------------------------------------------------
// Test: max-elapsed bound exhausted returns an error
// ---------------------------------------------------------------------------

func TestInitialConnectRetry_BoundExhausted(t *testing.T) {
	fakeDial := func(_ string, _ *amqp.Config) error {
		return errors.New("always fails")
	}
	m := newManagerWithFakeDial(fakeDial)

	ctx := context.Background()
	maxElapsed := 30 * time.Millisecond

	start := time.Now()
	err := m.initialConnectWithRetry(ctx, m.url, nil, maxElapsed)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error when bound is exhausted, got nil")
	}
	// Should not overshoot the bound by more than one backoff sleep (≤32 s in
	// production, but with a 30 ms bound the first sleep is capped to ≤30 ms,
	// so total overshoot is negligible).
	if elapsed > maxElapsed+200*time.Millisecond {
		t.Errorf("retry ran too long: %v (bound was %v)", elapsed, maxElapsed)
	}
}

// ---------------------------------------------------------------------------
// Test: negative maxElapsed disables retry (single attempt, fast path)
// ---------------------------------------------------------------------------

func TestInitialConnectRetry_NoRetryWhenNegativeMaxElapsed(t *testing.T) {
	fakeDial, calls := failNThenSucceed(1) // would succeed on 2nd call
	m := newManagerWithFakeDial(fakeDial)

	ctx := context.Background()
	err := m.initialConnectWithRetry(ctx, m.url, nil, -1)

	if err == nil {
		t.Fatal("expected error with retry disabled, got nil")
	}
	if got := int(atomic.LoadInt32(calls)); got != 1 {
		t.Errorf("dial called %d times with retry disabled, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Test: first-attempt success — no retry, dial called exactly once
// ---------------------------------------------------------------------------

func TestInitialConnectRetry_SuccessOnFirstAttempt(t *testing.T) {
	fakeDial, calls := failNThenSucceed(0) // succeeds immediately
	m := newManagerWithFakeDial(fakeDial)

	ctx := context.Background()
	err := m.initialConnectWithRetry(ctx, m.url, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("expected success on first attempt, got: %v", err)
	}
	if got := int(atomic.LoadInt32(calls)); got != 1 {
		t.Errorf("dial called %d times, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Test: WithInitialConnectMaxElapsed option wiring
// ---------------------------------------------------------------------------

func TestWithInitialConnectMaxElapsed_SetsValue(t *testing.T) {
	o := connectOptions{}
	WithInitialConnectMaxElapsed(42 * time.Second)(&o)
	if o.maxElapsed != 42*time.Second {
		t.Errorf("maxElapsed = %v, want %v", o.maxElapsed, 42*time.Second)
	}
}

func TestWithInitialConnectMaxElapsed_NegativeDisablesRetry(t *testing.T) {
	o := connectOptions{}
	WithInitialConnectMaxElapsed(-1)(&o)
	if o.maxElapsed >= 0 {
		t.Errorf("maxElapsed = %v, want negative", o.maxElapsed)
	}
}

// ---------------------------------------------------------------------------
// Test: default maxElapsed constant
// ---------------------------------------------------------------------------

func TestDefaultInitialConnectMaxElapsed(t *testing.T) {
	want := 2 * time.Minute
	if defaultInitialConnectMaxElapsed != want {
		t.Errorf("defaultInitialConnectMaxElapsed = %v, want %v",
			defaultInitialConnectMaxElapsed, want)
	}
}
