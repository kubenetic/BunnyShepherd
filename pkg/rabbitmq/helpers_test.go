package rabbitmq

import (
	"errors"
	"testing"
)

// TestSanitizeAMQPURL tests URL sanitization for safe logging.
func TestSanitizeAMQPURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty URL",
			input:    "",
			expected: "amqp://***",
		},
		{
			name:     "valid URL without credentials",
			input:    "amqp://localhost:5672/",
			expected: "amqp://localhost:5672/",
		},
		{
			name:     "valid URL with credentials",
			input:    "amqp://guest:password@localhost:5672/",
			expected: "amqp://***@localhost:5672/",
		},
		{
			name:     "valid URL with username only",
			input:    "amqp://guest@localhost:5672/",
			expected: "amqp://***@localhost:5672/",
		},
		{
			name:     "malformed URL",
			input:    "not a valid url at all",
			expected: "amqp://***",
		},
		{
			name:     "amqps with credentials",
			input:    "amqps://admin:secret123@rabbitmq.example.com:5671/vhost",
			expected: "amqps://***@rabbitmq.example.com:5671/vhost",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeAMQPURL(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeAMQPURL(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestRequeueError tests the RequeueError type and helpers.
func TestRequeueError(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		shouldRequeue bool
		wantErr       string
	}{
		{
			name:          "plain error defaults to no requeue",
			err:           errors.New("some error"),
			shouldRequeue: false,
			wantErr:       "some error",
		},
		{
			name:          "Requeue wrapper enables requeue",
			err:           Requeue(errors.New("transient error")),
			shouldRequeue: true,
			wantErr:       "transient error",
		},
		{
			name:          "NoRequeue wrapper disables requeue",
			err:           NoRequeue(errors.New("permanent error")),
			shouldRequeue: false,
			wantErr:       "permanent error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test ShouldRequeue
			if got := ShouldRequeue(tt.err); got != tt.shouldRequeue {
				t.Errorf("ShouldRequeue(%v) = %v, want %v", tt.err, got, tt.shouldRequeue)
			}

			// Test error message
			if tt.err.Error() != tt.wantErr {
				t.Errorf("err.Error() = %q, want %q", tt.err.Error(), tt.wantErr)
			}

			// Test errors.As for RequeueError
			var re *RequeueError
			if errors.As(tt.err, &re) {
				if re.Requeue != tt.shouldRequeue {
					t.Errorf("RequeueError.Requeue = %v, want %v", re.Requeue, tt.shouldRequeue)
				}
			} else if _, ok := tt.err.(*RequeueError); ok {
				t.Errorf("errors.As failed for RequeueError")
			}
		})
	}
}

// TestRequeueErrorUnwrap tests error unwrapping.
func TestRequeueErrorUnwrap(t *testing.T) {
	originalErr := errors.New("original error")
	wrappedErr := Requeue(originalErr)

	var re *RequeueError
	if !errors.As(wrappedErr, &re) {
		t.Fatal("errors.As failed")
	}

	if !errors.Is(wrappedErr, originalErr) {
		t.Errorf("errors.Is(wrappedErr, originalErr) = false, want true")
	}

	if re.Unwrap() != originalErr {
		t.Errorf("Unwrap() = %v, want %v", re.Unwrap(), originalErr)
	}
}

// TestErrorConstantAlias tests that the corrected error constant is available.
func TestErrorConstantAlias(t *testing.T) {
	// Both constants should exist and be equal
	if ErrConnectionNotInitialized != ErrConnectionNotInitilized {
		t.Errorf("ErrConnectionNotInitialized != ErrConnectionNotInitilized")
	}

	// Both should have the same error message
	if ErrConnectionNotInitialized.Error() != "connection not initialized" {
		t.Errorf("ErrConnectionNotInitialized.Error() = %q, want %q",
			ErrConnectionNotInitialized.Error(), "connection not initialized")
	}

	if ErrConnectionNotInitilized.Error() != "connection not initialized" {
		t.Errorf("ErrConnectionNotInitilized.Error() = %q, want %q",
			ErrConnectionNotInitilized.Error(), "connection not initialized")
	}
}

// TestErrAlreadySubscribed tests the new error constant.
func TestErrAlreadySubscribed(t *testing.T) {
	if ErrAlreadySubscribed.Error() != "consumer already subscribed" {
		t.Errorf("ErrAlreadySubscribed.Error() = %q, want %q",
			ErrAlreadySubscribed.Error(), "consumer already subscribed")
	}
}
