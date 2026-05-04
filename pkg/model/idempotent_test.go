package model

import (
	"context"
	"testing"
)

func TestIdempotentHandler_DuplicateDetection(t *testing.T) {
	callCount := 0
	handler := func() (interface{}, error) {
		callCount++
		return "result", nil
	}

	idempotent := NewIdempotentHandler(nil, 10)

	// First call with message ID should execute
	result, err := idempotent.Handle(context.Background(), "msg-1", handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "result" {
		t.Fatalf("expected 'result', got %v", result)
	}
	if callCount != 1 {
		t.Fatalf("expected handler to be called once, got %d", callCount)
	}

	// Second call with same message ID should not execute
	result, err = idempotent.Handle(context.Background(), "msg-1", handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil for duplicate, got %v", result)
	}
	if callCount != 1 {
		t.Fatalf("expected handler to still be called once, got %d", callCount)
	}

	// Call with different message ID should execute
	result, err = idempotent.Handle(context.Background(), "msg-2", handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "result" {
		t.Fatalf("expected 'result', got %v", result)
	}
	if callCount != 2 {
		t.Fatalf("expected handler to be called twice, got %d", callCount)
	}
}

func TestIdempotentHandler_EmptyMessageID(t *testing.T) {
	callCount := 0
	handler := func() (interface{}, error) {
		callCount++
		return "result", nil
	}

	idempotent := NewIdempotentHandler(nil, 10)

	// Empty message ID should always execute (no caching)
	for i := 0; i < 3; i++ {
		result, err := idempotent.Handle(context.Background(), "", handler)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "result" {
			t.Fatalf("expected 'result', got %v", result)
		}
	}

	if callCount != 3 {
		t.Fatalf("expected handler to be called 3 times for empty IDs, got %d", callCount)
	}
}

func TestIdempotentHandler_GenerationRotation(t *testing.T) {
	callCount := 0
	handler := func() (interface{}, error) {
		callCount++
		return "result", nil
	}

	capacity := 3
	idempotent := NewIdempotentHandler(nil, capacity)

	// Fill the current generation
	for i := 1; i <= capacity; i++ {
		idempotent.Handle(context.Background(), "msg-"+string(rune('0'+i)), handler)
	}
	if callCount != capacity {
		t.Fatalf("expected %d calls, got %d", capacity, callCount)
	}

	// Next call should rotate generations
	idempotent.Handle(context.Background(), "msg-new", handler)
	if callCount != capacity+1 {
		t.Fatalf("expected %d calls after rotation, got %d", capacity+1, callCount)
	}

	// Old messages from previous generation should still be detected as duplicates
	idempotent.Handle(context.Background(), "msg-1", handler)
	if callCount != capacity+1 {
		t.Fatalf("expected %d calls (duplicate from previous gen), got %d", capacity+1, callCount)
	}
}
