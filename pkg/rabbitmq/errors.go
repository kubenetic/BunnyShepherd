package rabbitmq

// MQError is a simple string-based error type used for well-known RabbitMQ
// error conditions surfaced by this package.
type MQError string

func (e MQError) Error() string {
	return string(e)
}

const (
	// ErrConnectionNotInitilized is returned when an operation requires an active
	// connection but the ConnectionManager has not yet established one.
	ErrConnectionNotInitilized MQError = "connection not initialized"

	// ErrConnectionClosed is returned when an operation cannot proceed because the
	// underlying AMQP connection has already been closed.
	ErrConnectionClosed MQError = "connection closed"

	// ErrNacked indicates that the broker negatively acknowledged a published
	// message (i.e., publish confirm received with nack). Callers may treat this
	// as a transient error and retry according to their policy.
	ErrNacked MQError = "publish not confirmed (nack)"

	// ErrConfirmTimeout indicates that no publisher confirm was received within the
	// expected time window. This often points to a network partition or a stalled
	// channel. Retrying after reinitializing the channel is typically appropriate.
	ErrConfirmTimeout MQError = "publish not confirmed (timeout)"

	// ErrChannelReinitBackoffExceed indicates the consumer/publisher exceeded
	// the maximum allowed backoff while attempting to reinitialize an AMQP channel.
	ErrChannelReinitBackoffExceed MQError = "channel reinit backoff exceeded"

	// ErrRepublishBackoffExceed indicates publish retries exceeded the maximum
	// backoff window without succeeding.
	ErrRepublishBackoffExceed MQError = "republish backoff exceeded"
)
