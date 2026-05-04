// Package model defines message envelope abstractions shared by the RabbitMQ
// components. It includes the Message interface and a generic JSONMessage
// implementation to simplify producing JSON payloads.
package model

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

// Message represents a publishable envelope abstraction used by the RabbitMQ publisher.
// Implementations provide metadata (ids, headers, content-type) and a serialized payload.
// Methods may lazily populate defaults (e.g., generating MessageId or deriving CorrelationId).
type Message interface {
	GetMessageId() string
	GetCorrelationId() string
	GetHeaders() amqp.Table
	GetContentType() string
	GetPayload() ([]byte, error)
}

// JSONMessage is a generic implementation of Message that marshals Payload to JSON.
// Zero values are normalized on access: if MessageId is empty, a UUIDv4 is generated;
// if CorrelationId is empty, it falls back to MessageId; if ContentType is empty,
// it defaults to "application/json".
type JSONMessage[T any] struct {
	Payload       T
	MessageId     string
	CorrelationId string
	Headers       amqp.Table
	ContentType   string
}

// GetMessageId returns the message identifier, generating a UUIDv4 if it was not set.
func (m *JSONMessage[T]) GetMessageId() string {
	if m.MessageId == "" {
		m.MessageId = uuid.New().String()
	}
	return m.MessageId
}

// GetCorrelationId returns the correlation identifier. If empty, it falls back
// to the MessageId so producers/consumers can always correlate.
func (m *JSONMessage[T]) GetCorrelationId() string {
	if m.CorrelationId == "" {
		return m.GetMessageId()
	}
	return m.CorrelationId
}

// GetHeaders returns the AMQP headers map associated with the message. It may
// be nil if no headers were set.
func (m *JSONMessage[T]) GetHeaders() amqp.Table {
	return m.Headers
}

// GetContentType returns the MIME content type for the payload. If unset, it
// defaults to "application/json".
func (m *JSONMessage[T]) GetContentType() string {
	if m.ContentType == "" {
		return "application/json"
	}
	return m.ContentType
}

// GetPayload marshals the underlying payload to JSON and returns the bytes.
// It may return a json.Marshaler error if the payload cannot be encoded.
func (m *JSONMessage[T]) GetPayload() ([]byte, error) {
	return json.Marshal(m.Payload)
}

// SafeHandler is the handler type used by the consumer for safe message processing.
// It is defined here for use by IdempotentHandler.
type SafeHandler func(ctx context.Context, msg interface{}) (interface{}, error)

// IdempotentHandler wraps a SafeHandler and ensures that duplicate message IDs
// are not processed more than once within a bounded cache window. It uses a
// two-generation cache to prevent unbounded memory growth.
//
// If the message ID is empty, the message is passed through to the inner handler
// without caching (no idempotency guarantee for empty IDs).
//
// The handler is safe for concurrent use.
type IdempotentHandler struct {
	inner SafeHandler

	// Two-generation cache: current and previous.
	// When current reaches capacity, it becomes previous and a new current is created.
	mu       sync.Mutex
	current  map[string]struct{}
	previous map[string]struct{}
	capacity int
}

// NewIdempotentHandler creates a new IdempotentHandler wrapping the provided
// SafeHandler with a bounded cache of the given capacity.
func NewIdempotentHandler(inner SafeHandler, capacity int) *IdempotentHandler {
	if capacity <= 0 {
		capacity = 1000
	}
	return &IdempotentHandler{
		inner:    inner,
		current:  make(map[string]struct{}, capacity),
		previous: make(map[string]struct{}),
		capacity: capacity,
	}
}

// Handle processes a message, checking for duplicates in the cache.
// If the message ID is empty, it passes through to the inner handler.
// If the message ID has been seen before, it returns early without calling the inner handler.
// Otherwise, it calls the inner handler and caches the message ID.
func (h *IdempotentHandler) Handle(ctx context.Context, msgID string, handler func() (interface{}, error)) (interface{}, error) {
	// Empty message IDs bypass idempotency
	if msgID == "" {
		log.Debug().Msg("empty message ID; bypassing idempotency check")
		return handler()
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Check if already seen in current or previous generation
	if _, seen := h.current[msgID]; seen {
		log.Debug().Str("messageId", msgID).Msg("duplicate message ID in current generation; skipping")
		return nil, nil
	}
	if _, seen := h.previous[msgID]; seen {
		log.Debug().Str("messageId", msgID).Msg("duplicate message ID in previous generation; skipping")
		return nil, nil
	}

	// Mark as seen in current generation
	h.current[msgID] = struct{}{}

	// Rotate generations if current is at capacity
	if len(h.current) >= h.capacity {
		h.previous = h.current
		h.current = make(map[string]struct{}, h.capacity)
		log.Debug().Msg("idempotent cache rotated to new generation")
	}

	// Call the inner handler
	return handler()
}
