// Package rabbitmq provides a thin, concurrency-aware wrapper around the
// github.com/rabbitmq/amqp091-go client. It manages a single long‑lived
// connection with automatic reconnecting and offers helpers for publishing
// with confirmations.
//
// The package exposes a ConnectionManager which:
//   - Establishes an initial connection to RabbitMQ using either a default
//     amqp.Config or a custom one supplied by the caller.
//   - Monitors the connection and automatically attempts to reconnect with
//     exponential backoff when the connection is closed or lost.
//   - Provides an internal helper to obtain AMQP channels derived from the
//     managed connection (used by higher‑level components like Publisher).
//   - Ensures orderly shutdown and cleanup when the provided context is
//     cancelled or when Close is called.
//
// It also provides a Publisher that publishes messages with confirmations and
// retries on transient errors.
package rabbitmq
