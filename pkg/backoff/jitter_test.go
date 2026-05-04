package backoff

import (
	"testing"
	"time"
)

// TestJitterZero tests that Jitter(0) returns 0 without panic.
func TestJitterZero(t *testing.T) {
	result := Jitter(0)
	if result != 0 {
		t.Errorf("Jitter(0) = %v, want 0", result)
	}
}

// TestJitterNegative tests that Jitter(-1s) returns -1s without panic.
func TestJitterNegative(t *testing.T) {
	d := -1 * time.Second
	result := Jitter(d)
	if result != d {
		t.Errorf("Jitter(%v) = %v, want %v", d, result, d)
	}
}

// TestJitterPositive tests that Jitter(100ms) returns a value in [100ms, 150ms).
func TestJitterPositive(t *testing.T) {
	d := 100 * time.Millisecond
	min := d
	max := d + d/2 // 100ms + 50ms = 150ms

	// Sample multiple iterations to verify the range
	samples := 100
	for i := 0; i < samples; i++ {
		result := Jitter(d)
		if result < min || result >= max {
			t.Errorf("Jitter(%v) = %v, want value in [%v, %v)", d, result, min, max)
		}
	}
}

// TestJitterVerySmallPositive tests that very small positive durations don't panic.
func TestJitterVerySmallPositive(t *testing.T) {
	d := 1 * time.Nanosecond
	result := Jitter(d)
	// For very small durations, half might be 0, so we should get d unchanged
	if result != d {
		t.Errorf("Jitter(%v) = %v, want %v", d, result, d)
	}
}
