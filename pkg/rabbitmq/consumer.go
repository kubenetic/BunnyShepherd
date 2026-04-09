package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/kubenetic/BunnyShepherd/pkg/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

// MessageHandler processes a single delivered message. It should return
// nil on success. On error, the consumer will Nack the message without requeue.
// Implementations should be fast and resilient; panics are recovered and the
// message is negatively acknowledged.
type MessageHandler func(ctx context.Context, message amqp.Delivery) error

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
		MaxBackoff:            10 * time.Second,
		PrefetchCount:         1,
	}
}

// Consumer is a placeholder for future consumer-related utilities
// built on top of ConnectionManager. It will manage consumer channels,
// subscriptions, and graceful shutdown semantics.
//
// Consumer is not safe for concurrent use.
type Consumer struct {
	config ConsumerConfig
	cm     *ConnectionManager

	conCh *amqp.Channel
	cra   int // consumer reinit attempts
	conMu sync.Mutex
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
func NewConsumer(cm *ConnectionManager, opts ...ConsumerOption) (*Consumer, error) {
	// Start from defaults, then apply any provided options.
	cfg := DefaultConsumerConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	c := &Consumer{
		cm:     cm,
		config: cfg,
	}

	if err := c.initConsumerChannel(); err != nil {
		return nil, err
	}

	return c, nil
}

// initConsumerChannel opens a new AMQP channel via the ConnectionManager and
// applies QoS settings (PrefetchCount). It replaces any previously held
// consumer channel and prepares the Consumer for subscriptions.
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
	c.cra = 1

	if c.cra > 1 {
		log.Debug().Msg("consumer channel reinitialized")
	} else {
		log.Debug().Msg("consumer channel initialized")
	}

	return nil
}

// Close closes the underlying consumer AMQP channel if it is open. It is safe
// to call multiple times.
func (c *Consumer) Close() error {
	c.conMu.Lock()
	defer c.conMu.Unlock()

	if c.conCh != nil && !c.conCh.IsClosed() {
		return c.conCh.Close()
	}

	return nil
}

// Subscribe initializes a consumer to a specified queue and processes messages using the provided MessageHandler
// callback.
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
	backoffTime := c.config.InitialBackoff
	maxBackoff := c.config.MaxBackoff

	for ctx.Err() == nil {
		// Lazy reinit if the channel is not ready with retry logic
		c.conMu.Lock()
		if c.conCh == nil || c.conCh.IsClosed() {
			if err := c.initConsumerChannel(); err != nil {
				c.conMu.Unlock()

				sleep := backoff.Jitter(backoffTime)
				select {
				case <-time.After(sleep):
					backoffTime *= 2
					if backoffTime > maxBackoff {
						log.Error().Msg("backoffTime exceeded")
						return ErrChannelReinitBackoffExceed
					}
					continue

				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		// Channel is ready, proceed with consuming messages
		ch := c.conCh
		c.conMu.Unlock()

		if consumer == "" {
			consumer = GenConsumerTag("")
		}

		messages, err := ch.ConsumeWithContext(
			ctx, queue, consumer, false, false, false, false, nil)
		if err != nil {
			log.Error().Err(err).Msg("error consuming messages")

			sleep := backoff.Jitter(backoffTime)
			select {
			case <-time.After(sleep):
				backoffTime *= 2
				if backoffTime > maxBackoff {
					log.Error().Msg("backoffTime exceeded")
					return ctx.Err()
				}
				continue

			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Reset backoff only after a successful ConsumeWithContext — not after
		// channel reinit alone. Resetting too early (on reinit) means a channel
		// that closes immediately after being opened (e.g. due to a poison
		// message or rapid RabbitMQ flapping) would never accumulate backoff,
		// causing a tight reinit loop at the initial 500 ms interval.
		backoffTime = c.config.InitialBackoff

	messageLoop:
		for {
			log.Debug().Msg("waiting for message...")

			select {
			case <-ctx.Done():
				_ = ch.Cancel(consumer, true)
				return ctx.Err()

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

				func() {
					cbCtx, cbCncl := context.WithTimeout(ctx, 30*time.Second)
					defer cbCncl()

					defer func() {
						if r := recover(); r != nil {
							log.Error().Msg("panic in message handler")
							_ = message.Nack(false, false)
						}
					}()

					if err := cb(cbCtx, message); err != nil {
						log.Error().Err(err).Msg("error handling message")
						_ = message.Nack(false, false)
					}
				}()
			}
		}
	}

	return ctx.Err()
}

// GenConsumerTag builds a consumer tag incorporating the hostname and process
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
