package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubenetic/BunnyShepherd/pkg/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

// MessageHandler processes a single delivered message. It should return
// nil on success. On error, the consumer will Nack the message without requeue.
// Implementations should be fast and resilient; panics are recovered and the
// message is negatively acknowledged.
//
// Deprecated: Use SafeHandler instead. SafeHandler provides explicit ack semantics
// and prevents message loss due to handler crashes or forgotten acks.
type MessageHandler func(ctx context.Context, message amqp.Delivery) error

// Outcome represents the result of processing a message by a SafeHandler.
// The library uses this to determine whether to ack, nack, or requeue the message.
type Outcome int

const (
	// Ack indicates successful processing — the message is acknowledged and removed from the queue.
	Ack Outcome = iota
	// Reject indicates permanent failure — the message is negatively acknowledged without requeue,
	// typically sending it to a dead-letter exchange if configured.
	Reject
	// RequeueOutcome indicates transient failure — the message is negatively acknowledged with requeue,
	// returning it to the queue for retry. Callers must implement retry limits to avoid infinite loops;
	// see bunny-H01-no-poison-message-handling.md for bounded retry patterns.
	RequeueOutcome
)

// String returns a human-readable representation of the Outcome.
func (o Outcome) String() string {
	switch o {
	case Ack:
		return "Ack"
	case Reject:
		return "Reject"
	case RequeueOutcome:
		return "Requeue"
	default:
		return "Unknown"
	}
}

// Delivery wraps an AMQP delivery for use with SafeHandler.
// It provides read-only access to message metadata and body.
type Delivery struct {
	Body            []byte
	ContentType     string
	ContentEncoding string
	Headers         amqp.Table
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
	DeliveryTag     uint64
	Redelivered     bool
	Exchange        string
	RoutingKey      string
}

// newDeliveryFromAMQP converts an amqp.Delivery to a Delivery.
func newDeliveryFromAMQP(d amqp.Delivery) Delivery {
	return Delivery{
		Body:            d.Body,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		Headers:         d.Headers,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserId:          d.UserId,
		AppId:           d.AppId,
		DeliveryTag:     d.DeliveryTag,
		Redelivered:     d.Redelivered,
		Exchange:        d.Exchange,
		RoutingKey:      d.RoutingKey,
	}
}

// SafeHandler processes a single delivered message and returns an explicit Outcome.
// The library guarantees that ack/nack is performed based on the returned Outcome,
// so handlers cannot accidentally lose messages by forgetting to ack or by crashing
// after acking but before completing work.
//
// If the handler panics, the library recovers and treats it as Reject.
// If the handler returns an error, it is logged but does not affect the Outcome
// (the Outcome is authoritative).
type SafeHandler func(ctx context.Context, msg Delivery) (Outcome, error)

// ConsumerConfig holds configuration for Consumer behavior.
type ConsumerConfig struct {
	MessageHandlerTimeout time.Duration
	InitialBackoff        time.Duration
	MaxBackoff            time.Duration
	PrefetchCount         int
}

// DefaultConsumerConfig returns sensible defaults.
func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		MessageHandlerTimeout: 30 * time.Second,
		InitialBackoff:        500 * time.Millisecond,
		MaxBackoff:            5 * time.Minute,
		PrefetchCount:         1,
	}
}

// Consumer is a placeholder for future consumer-related utilities
// built on top of ConnectionManager. It will manage consumer channels,
// subscriptions, and graceful shutdown semantics.
//
// Consumer is not safe for concurrent use. Subscribe should be called once
// per Consumer instance; subsequent calls will return ErrAlreadySubscribed.
type Consumer struct {
	config ConsumerConfig
	cm     *ConnectionManager

	conCh *amqp.Channel
	cra   int // consumer reinit attempts
	conMu sync.Mutex

	// Subscription state management
	running atomic.Bool
	stopCh  chan struct{}

	// Handler lifecycle management
	handlerWg sync.WaitGroup

	// Per-message context tracking for graceful channel close
	// Maps delivery tag to cancel function for in-flight handlers
	inflightMu sync.Mutex
	inflight   map[uint64]context.CancelFunc
}

// ConsumerOption defines a function that modifies the Consumer configuration.
// Use it with NewConsumer to customize behavior.
// Example:
//
//	c, _ := NewConsumer(cm, WithPrefetchCount(5))
//	c2, _ := NewConsumer(cm, WithConsumerConfig(ConsumerConfig{...}))
//
// If no options are provided, DefaultConsumerConfig() is used.
type ConsumerOption func(*ConsumerConfig)

// WithConsumerConfig sets the entire Consumer configuration at once.
func WithConsumerConfig(cfg ConsumerConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		*c = cfg
	}
}

// WithMessageHandlerTimeout overrides MessageHandlerTimeout.
func WithMessageHandlerTimeout(d time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) { c.MessageHandlerTimeout = d }
}

// WithBackoff sets initial and max backoff durations.
func WithBackoff(initial, max time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.InitialBackoff = initial
		c.MaxBackoff = max
	}
}

// WithPrefetchCount sets the QoS prefetch count.
func WithPrefetchCount(n int) ConsumerOption {
	return func(c *ConsumerConfig) { c.PrefetchCount = n }
}

// NewConsumer constructs a Consumer bound to the provided ConnectionManager.
// It applies the given options over DefaultConsumerConfig, initializes an
// AMQP channel configured with the desired QoS, and returns the ready Consumer.
//
// If the ConnectionManager was created with a plaintext amqp:// URI (not amqps://)
// and the host is not localhost or 127.0.0.1, a warning is logged.
func NewConsumer(cm *ConnectionManager, opts ...ConsumerOption) (*Consumer, error) {
	// Start from defaults, then apply any provided options.
	cfg := DefaultConsumerConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	c := &Consumer{
		cm:       cm,
		config:   cfg,
		stopCh:   make(chan struct{}),
		inflight: make(map[uint64]context.CancelFunc),
	}

	if err := c.initConsumerChannel(); err != nil {
		return nil, err
	}

	return c, nil
}

// initConsumerChannel opens a new AMQP channel via the ConnectionManager and
// applies QoS settings (PrefetchCount). It replaces any previously held
// consumer channel and prepares the Consumer for subscriptions.
// cra (channel reinit attempts) is incremented on each call and reset to 0
// by Subscribe after a successful ConsumeWithContext, so it reflects the
// number of consecutive reinit attempts in the current failure streak.
// MUST be called under the conMu lock.
func (c *Consumer) initConsumerChannel() error {
	ch, err := c.cm.getChannel()
	if err != nil {
		return err
	}

	if err := ch.Qos(c.config.PrefetchCount, 0, false); err != nil {
		_ = ch.Close()
		return err
	}

	c.conCh = ch
	c.cra++

	if c.cra > 1 {
		log.Debug().Int("attempt", c.cra).Msg("consumer channel reinitialized")
	} else {
		log.Debug().Msg("consumer channel initialized")
	}

	return nil
}

// Close closes the underlying consumer AMQP channel if it is open and waits for
// any in-flight message handlers to complete (with a 5-second timeout). It is safe
// to call multiple times and safe to call concurrently with Subscribe.
func (c *Consumer) Close() error {
	// Signal the active subscription (if any) to stop. Guard the close with
	// conMu so we don't race Subscribe's stopCh re-creation nor
	// double-close on concurrent Close calls. We probe the channel via a
	// non-blocking receive: an open channel yields default, a closed channel
	// yields zero-value with ok=false — the second case means someone has
	// already closed it and we must not close again (would panic).
	c.conMu.Lock()
	select {
	case <-c.stopCh:
		// already closed — no-op
	default:
		close(c.stopCh)
	}
	c.conMu.Unlock()

	// Cancel all in-flight handlers
	c.onChannelClose()

	// Wait for handlers with timeout
	done := make(chan struct{})
	go func() {
		c.handlerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All handlers completed
	case <-time.After(5 * time.Second):
		log.Warn().Msg("timeout waiting for message handlers during close")
	}

	c.conMu.Lock()
	defer c.conMu.Unlock()

	if c.conCh != nil && !c.conCh.IsClosed() {
		return c.conCh.Close()
	}
	return nil
}

// onChannelClose cancels all in-flight per-message contexts when the AMQP channel closes.
// This allows handlers to observe the cancellation and abort gracefully without attempting
// to ack/nack on a closed channel.
func (c *Consumer) onChannelClose() {
	c.inflightMu.Lock()
	defer c.inflightMu.Unlock()

	for _, cancel := range c.inflight {
		cancel()
	}
	// Clear the map after cancelling all
	c.inflight = make(map[uint64]context.CancelFunc)
}

// Subscribe initializes a consumer to a specified queue and processes messages using the provided MessageHandler
// callback.
//
// Concurrency: Subscribe is safe to call only once AT A TIME per Consumer
// instance — a concurrent second Subscribe returns ErrAlreadySubscribed.
// SERIAL re-subscription is supported: Subscribe → return-on-ctx-cancel or
// Subscribe → Close → Subscribe works cleanly. The implementation recreates
// the internal stop channel on each Subscribe entry so a previously-closed
// one does not short-circuit the new subscription's select loops.
//
// The provided context is used to signal the consumer to stop. Subscribe blocks until the context is canceled or
// the channel is closed.
//
// Example:
//
//	handler := func(hCtx context.Context, d amqp.Delivery) error {
//	    // process message
//	    return d.Ack(false)
//	}
//	tag := rabbitmq.GenConsumerTag("worker-1")
//	if err := consumer.Subscribe(ctx, "scan.jobs.q", tag, handler); err != nil {
//	    // handle error
//	}
func (c *Consumer) Subscribe(ctx context.Context, queue, consumer string, cb MessageHandler) error {
	// Ensure Subscribe is only active once per Consumer instance at a time.
	// Two concurrent Subscribe calls on the same instance are rejected with
	// ErrAlreadySubscribed. Serial re-subscription (Subscribe → Close →
	// Subscribe) is supported: the second Subscribe sees running=false (set
	// by the first Subscribe's defer on return), the CAS succeeds, and we
	// recreate stopCh below so the previously-closed channel doesn't
	// immediately short-circuit our select loops.
	if !c.running.CompareAndSwap(false, true) {
		return ErrAlreadySubscribed
	}
	defer c.running.Store(false)

	// Recreate stopCh for this subscription iff the previous one was closed.
	// Guarded by conMu so Close cannot race with us here.
	c.conMu.Lock()
	select {
	case <-c.stopCh:
		// Previous stopCh was closed by a prior Close — allocate a fresh one
		// for this subscription so case <-c.stopCh doesn't fire immediately.
		c.stopCh = make(chan struct{})
	default:
		// Still usable as-is.
	}
	c.conMu.Unlock()

	backoffTime := c.config.InitialBackoff
	maxBackoff := c.config.MaxBackoff
	backoffReset := false // Track if we've successfully received a message

	for ctx.Err() == nil {
		// Lazy reinit if the channel is not ready with retry logic
		c.conMu.Lock()
		if c.conCh == nil || c.conCh.IsClosed() {
			if err := c.initConsumerChannel(); err != nil {
				c.conMu.Unlock()

				sleep := backoff.Jitter(backoffTime)
				backoffTime *= 2
				if backoffTime > maxBackoff {
					log.Error().Msg("backoffTime exceeded")
					return ErrChannelReinitBackoffExceed
				}
				select {
				case <-time.After(sleep):
					continue

				case <-ctx.Done():
					return ctx.Err()

				case <-c.stopCh:
					return nil
				}
			}
		}

		// Channel is ready, capture reference under lock and proceed with consuming messages
		ch := c.conCh
		c.conMu.Unlock()

		if consumer == "" {
			consumer = GenConsumerTag("")
		}

		messages, err := ch.ConsumeWithContext(
			ctx, queue, consumer, false, false, false, false, nil)
		if err != nil {
			log.Error().Err(err).Msg("error consuming messages")

			c.conMu.Lock()
			c.conCh = nil
			c.conMu.Unlock()

			sleep := backoff.Jitter(backoffTime)
			backoffTime *= 2
			if backoffTime > maxBackoff {
				log.Error().Msg("backoffTime exceeded")
				return ErrChannelReinitBackoffExceed
			}
			select {
			case <-time.After(sleep):
				continue

			case <-ctx.Done():
				return ctx.Err()

			case <-c.stopCh:
				return nil
			}
		}

		// Reset backoff only after first successful message delivery (not after ConsumeWithContext)
		backoffReset = false

	messageLoop:
		for {
			select {
			case <-ctx.Done():
				_ = ch.Cancel(consumer, true)
				return ctx.Err()

			case <-c.stopCh:
				_ = ch.Cancel(consumer, true)
				return nil

			case message, ok := <-messages:
				if !ok {
					// Channel was closed by the broker or a channel-level AMQP error.
					// Nil out the channel so the outer loop reinitialises it.
					// Using a labeled break to exit the for loop, not just the select —
					// a bare break would only exit the select and spin on the closed channel at 100% CPU.
					c.conMu.Lock()
					if c.conCh != nil {
						_ = c.conCh.Cancel(consumer, true)
						c.conCh = nil
					}
					c.conMu.Unlock()
					break messageLoop
				}

				log.Debug().Msg("message received")

				// Reset backoff only after first successful message delivery
				if !backoffReset {
					backoffTime = c.config.InitialBackoff
					c.conMu.Lock()
					c.cra = 0
					c.conMu.Unlock()
					backoffReset = true
				}

				c.handlerWg.Add(1)
				func() {
					defer c.handlerWg.Done()

					cbCtx, cbCncl := context.WithTimeout(ctx, c.config.MessageHandlerTimeout)
					defer cbCncl()

					defer func() {
						if r := recover(); r != nil {
							log.Error().Interface("panic", r).Msg("panic in message handler")
							_ = message.Nack(false, false)
						}
					}()

					if err := cb(cbCtx, message); err != nil {
						log.Error().Err(err).Msg("error handling message")
						requeue := ShouldRequeue(err)
						_ = message.Nack(false, requeue)
					}
				}()
			}
		}
	}

	return ctx.Err()
}

// ConsumeWithSafeHandler initializes a consumer to a specified queue and processes messages
// using the provided SafeHandler callback. Unlike Subscribe, ConsumeWithSafeHandler guarantees
// that ack/nack is performed based on the handler's returned Outcome, preventing message loss
// due to handler crashes or forgotten acks.
//
// Concurrency: ConsumeWithSafeHandler is safe to call only once AT A TIME per Consumer
// instance — a concurrent second call returns ErrAlreadySubscribed.
// SERIAL re-subscription is supported: ConsumeWithSafeHandler → return-on-ctx-cancel or
// ConsumeWithSafeHandler → Close → ConsumeWithSafeHandler works cleanly.
//
// The provided context is used to signal the consumer to stop. ConsumeWithSafeHandler blocks
// until the context is canceled or the channel is closed.
//
// Panics in the handler are recovered and treated as Reject (permanent failure).
// Returned errors are logged but do not affect the Outcome (the Outcome is authoritative).
//
// Example:
//
//	handler := func(hCtx context.Context, d Delivery) (Outcome, error) {
//	    // process message
//	    if err := processMessage(d.Body); err != nil {
//	        return Reject, err
//	    }
//	    return Ack, nil
//	}
//	tag := rabbitmq.GenConsumerTag("worker-1")
//	if err := consumer.ConsumeWithSafeHandler(ctx, "scan.jobs.q", tag, handler); err != nil {
//	    // handle error
//	}
func (c *Consumer) ConsumeWithSafeHandler(ctx context.Context, queue, consumer string, cb SafeHandler) error {
	// Ensure ConsumeWithSafeHandler is only active once per Consumer instance at a time.
	if !c.running.CompareAndSwap(false, true) {
		return ErrAlreadySubscribed
	}
	defer c.running.Store(false)

	// Recreate stopCh for this subscription iff the previous one was closed.
	c.conMu.Lock()
	select {
	case <-c.stopCh:
		c.stopCh = make(chan struct{})
	default:
	}
	c.conMu.Unlock()

	backoffTime := c.config.InitialBackoff
	maxBackoff := c.config.MaxBackoff
	backoffReset := false

	for ctx.Err() == nil {
		// Lazy reinit if the channel is not ready with retry logic
		c.conMu.Lock()
		if c.conCh == nil || c.conCh.IsClosed() {
			if err := c.initConsumerChannel(); err != nil {
				c.conMu.Unlock()

				sleep := backoff.Jitter(backoffTime)
				backoffTime *= 2
				if backoffTime > maxBackoff {
					log.Error().Msg("backoffTime exceeded")
					return ErrChannelReinitBackoffExceed
				}
				select {
				case <-time.After(sleep):
					continue

				case <-ctx.Done():
					return ctx.Err()

				case <-c.stopCh:
					return nil
				}
			}
		}

		// Channel is ready, capture reference under lock and proceed with consuming messages
		ch := c.conCh
		c.conMu.Unlock()

		if consumer == "" {
			consumer = GenConsumerTag("")
		}

		messages, err := ch.ConsumeWithContext(
			ctx, queue, consumer, false, false, false, false, nil)
		if err != nil {
			log.Error().Err(err).Msg("error consuming messages")

			c.conMu.Lock()
			c.conCh = nil
			c.conMu.Unlock()

			sleep := backoff.Jitter(backoffTime)
			backoffTime *= 2
			if backoffTime > maxBackoff {
				log.Error().Msg("backoffTime exceeded")
				return ErrChannelReinitBackoffExceed
			}
			select {
			case <-time.After(sleep):
				continue

			case <-ctx.Done():
				return ctx.Err()

			case <-c.stopCh:
				return nil
			}
		}

		// Reset backoff only after first successful message delivery (not after ConsumeWithContext)
		backoffReset = false

	messageLoopSafe:
		for {
			select {
			case <-ctx.Done():
				_ = ch.Cancel(consumer, true)
				return ctx.Err()

			case <-c.stopCh:
				_ = ch.Cancel(consumer, true)
				return nil

			case message, ok := <-messages:
				if !ok {
					// Channel was closed by the broker or a channel-level AMQP error.
					c.conMu.Lock()
					if c.conCh != nil {
						_ = c.conCh.Cancel(consumer, true)
						c.conCh = nil
					}
					c.conMu.Unlock()
					break messageLoopSafe
				}

				log.Debug().Msg("message received")

				// Reset backoff only after first successful message delivery
				if !backoffReset {
					backoffTime = c.config.InitialBackoff
					c.conMu.Lock()
					c.cra = 0
					c.conMu.Unlock()
					backoffReset = true
				}

				c.handlerWg.Add(1)
				func() {
					defer c.handlerWg.Done()

					// Create per-message context derived from the consumer context
					perMsgCtx, cancel := context.WithCancel(ctx)
					deliveryTag := message.DeliveryTag

					// Track this in-flight handler
					c.inflightMu.Lock()
					c.inflight[deliveryTag] = cancel
					c.inflightMu.Unlock()

					// Ensure cleanup on handler completion
					defer func() {
						c.inflightMu.Lock()
						delete(c.inflight, deliveryTag)
						c.inflightMu.Unlock()
						cancel()
					}()

					cbCtx, cbCncl := context.WithTimeout(perMsgCtx, c.config.MessageHandlerTimeout)
					defer cbCncl()

					// Recover from panics and treat as Reject
					var outcome Outcome
					var handlerErr error
					func() {
						defer func() {
							if r := recover(); r != nil {
								log.Error().Interface("panic", r).Msg("panic in message handler; rejecting message")
								outcome = Reject
							}
						}()
						outcome, handlerErr = cb(cbCtx, newDeliveryFromAMQP(message))
					}()

					// Log error if present, but outcome is authoritative
					if handlerErr != nil {
						log.Warn().Err(handlerErr).Str("outcome", outcome.String()).Msg("handler returned error")
					}

					// If the channel was closed during processing, do not attempt ack/nack
					if perMsgCtx.Err() != nil {
						log.Warn().Msg("channel closed during processing; skipping ack/nack")
						return
					}

					// Perform ack/nack based on outcome
					switch outcome {
					case Ack:
						_ = message.Ack(false)
					case Reject:
						_ = message.Nack(false, false) // go to DLX
					case RequeueOutcome:
						_ = message.Nack(false, true) // back to queue
					default:
						// Defensive: unknown outcome → Reject, log error
						log.Error().Err(handlerErr).Str("outcome", outcome.String()).Msg("unknown outcome; rejecting")
						_ = message.Nack(false, false)
					}
				}()
			}
		}
	}

	return ctx.Err()
}

// RequeueWithLimit is a helper that converts a Requeue outcome to Reject if the
// message has already been redelivered N times. It inspects the x-death header
// (automatically populated by RabbitMQ when a message is nacked with requeue=false
// via a dead-letter exchange) to count redeliveries.
//
// This prevents poison messages from starving the queue. Use this in handlers that
// return Requeue to implement bounded retry:
//
//	if err := processMessage(msg); err != nil {
//	    return consumer.RequeueWithLimit(msg, 5), err
//	}
//
// Default max redeliveries is 5. After N redeliveries, the message is rejected
// and sent to the dead-letter exchange (if configured on the queue).
func (c *Consumer) RequeueWithLimit(d Delivery, maxRedeliveries int) Outcome {
	deaths := countXDeathHeaders(d.Headers)
	if deaths >= maxRedeliveries {
		log.Warn().
			Int("deaths", deaths).
			Int("maxRedeliveries", maxRedeliveries).
			Str("messageId", d.MessageId).
			Msg("message exceeded max redeliveries; rejecting to DLQ")
		return Reject
	}
	return RequeueOutcome
}

// countXDeathHeaders counts the number of times a message has been dead-lettered
// by inspecting the x-death header. RabbitMQ automatically populates this header
// when a message is nacked with requeue=false and a dead-letter exchange is configured.
// Returns 0 if the header is not present or malformed.
func countXDeathHeaders(headers amqp.Table) int {
	if headers == nil {
		return 0
	}

	// x-death is an array of tables, each representing a dead-letter event
	xDeathRaw, ok := headers["x-death"]
	if !ok {
		return 0
	}

	// Type assert to []interface{} (how amqp.Table stores arrays)
	xDeathArray, ok := xDeathRaw.([]interface{})
	if !ok {
		return 0
	}

	// Each entry in the array is a table representing one death event.
	// The length of the array is the number of times the message has been dead-lettered.
	return len(xDeathArray)
}

// id. If id is non-empty, it is included to help distinguish multiple
// consumers within the same process or host.
func GenConsumerTag(id string) string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	if id == "" {
		return fmt.Sprintf("c-%s-%d", hostname, os.Getpid())
	} else {
		return fmt.Sprintf("c-%s-%s-%d", hostname, id, os.Getpid())
	}
}
