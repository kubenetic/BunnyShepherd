// Package model defines message envelope abstractions shared by the RabbitMQ
// components. It includes the Message interface and a generic JSONMessage
// implementation to simplify producing JSON payloads.
package model

import (
	"encoding/json"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
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
