package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kubenetic/BunnyShepherd/pkg/backoff"
	"github.com/kubenetic/BunnyShepherd/pkg/model"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

// ReturnHandler is an optional callback invoked for unroutable messages
// that the broker returned to the publisher (i.e., when publishing with
// mandatory=true and no queue is bound to the routing key). The context is
// time-bounded by the publisher and should be respected by implementations.
// Implementations should be fast and non-blocking.
type ReturnHandler func(ctx context.Context, ret amqp.Return) error

// PublisherConfig holds configuration for Publisher behavior.
type PublisherConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	ConfirmTimeout time.Duration
}

// DefaultPublisherConfig returns sensible defaults.
func DefaultPublisherConfig() PublisherConfig {
	return PublisherConfig{
		MaxRetries:     5,
		InitialBackoff: 200 * time.Millisecond,
		ConfirmTimeout: 5 * time.Second,
	}
}

// Publisher is a simple publisher that uses a single channel with confirmations
// and background goroutines to drain returns and observe channel closes.
// It implements minimal reconnect-on-demand logic
//
// The zero value is not usable; always initialize with NewPublisher. It opens a new
// channel on first use and reuses it on later calls. The channel is closed
// when the publisher is closed. Publisher is not safe for concurrent use.
type Publisher struct {
	cm      *ConnectionManager
	ch      *amqp.Channel
	returns chan amqp.Return
	closes  chan *amqp.Error
	mu      sync.Mutex

	pra    int // publish reinit attempts
	config PublisherConfig
	// Optional callback for handling unroutable messages. If nil, unroutable messages
	// will be logged on error level and discarded.
	OnReturn ReturnHandler
}

// PublisherOption defines a function that modifies the Publisher configuration.
// Use it with NewPublisher to customize behavior.
// Example:
//
//	p, _ := NewPublisher(cm, WithMaxRetries(3))
//	p2, _ := NewPublisher(cm, WithPublisherConfig(PublisherConfig{...}))
//
// If no options are provided, DefaultPublisherConfig() is used.
type PublisherOption func(*PublisherConfig)

// WithPublisherConfig sets the entire Publisher configuration at once.
func WithPublisherConfig(cfg PublisherConfig) PublisherOption {
	return func(c *PublisherConfig) { *c = cfg }
}

// WithMaxRetries overrides the maximum number of retry attempts on publish.
func WithMaxRetries(n int) PublisherOption {
	return func(c *PublisherConfig) { c.MaxRetries = n }
}

// WithInitialBackoff sets the initial backoff delay for retries.
func WithInitialBackoff(d time.Duration) PublisherOption {
	return func(c *PublisherConfig) { c.InitialBackoff = d }
}

// WithConfirmTimeout sets the timeout waiting for publisher confirms.
func WithConfirmTimeout(d time.Duration) PublisherOption {
	return func(c *PublisherConfig) { c.ConfirmTimeout = d }
}

// NewPublisher constructs a Publisher bound to the given ConnectionManager and
// eagerly initializes an AMQP channel with confirmations enabled. Background
// goroutines are started to drain returned messages and observe channel close
// events. Use Close to release resources.
//
// Example:
//
//	pub, err := rabbitmq.NewPublisher(cm,
//	    rabbitmq.WithMaxRetries(3),
//	    rabbitmq.WithInitialBackoff(300*time.Millisecond),
//	)
//	if err != nil { panic(err) }
//	defer pub.Close()
func NewPublisher(cm *ConnectionManager, opts ...PublisherOption) (*Publisher, error) {
	// Start from defaults, then apply any provided options.
	cfg := DefaultPublisherConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	p := &Publisher{cm: cm, config: cfg}
	if err := p.reinit(); err != nil {
		return nil, err
	}
	return p, nil
}

// reinit (re)creates the underlying AMQP channel, enables confirmations,
// wires up NotifyReturn/NotifyClose handlers, and swaps the internal fields.
// It assumes the ConnectionManager will handle reconnecting the underlying
// connection if needed.
func (p *Publisher) reinit() error {
	ch, err := p.cm.getChannel()
	if err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return err
	}

	// Prepare fresh notify channels and goroutines.
	returns := make(chan amqp.Return, 128)
	ch.NotifyReturn(returns)
	closes := make(chan *amqp.Error, 1)
	ch.NotifyClose(closes)

	// Swap fields only after successful setup
	p.ch = ch
	p.returns = returns
	p.closes = closes

	go p.handleReturns(p.returns)
	go p.handleClose(p.ch, p.closes)

	p.pra = p.pra + 1
	if p.pra > 1 {
		log.Debug().Msg("publisher channel (re)initialized")
	} else {
		log.Debug().Msg("publisher channel initialized")
	}

	return nil
}

// handleReturns logs unroutable returned messages and, if OnReturn is set,
// invokes the user-provided callback with a bounded context. This function
// runs in a dedicated goroutine per AMQP channel instance and exits when the
// broker closes the NotifyReturn channel.
func (p *Publisher) handleReturns(returns <-chan amqp.Return) {
	for ret := range returns {
		log.Error().
			Str("exchange", ret.Exchange).
			Str("routingKey", ret.RoutingKey).
			Uint16("replyCode", ret.ReplyCode).
			Str("text", ret.ReplyText).
			Msg("message returned (unroutable)")

		if p.OnReturn != nil {
			retCpy := ret
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			go func() {
				defer cancel()
				if err := p.OnReturn(ctx, retCpy); err != nil {
					log.Error().Err(err).Msg("error handling return")
				}
			}()
		}
	}
}

// handleClose observes close notifications for a specific AMQP channel
// instance. If the observed channel is still the one referenced by the
// publisher, it marks the channel as unusable, so the next Publish lazily
// reinitialized it. Runs in its own goroutine and exits when the notification
// channel is closed.
func (p *Publisher) handleClose(ch *amqp.Channel, closes <-chan *amqp.Error) {
	if err, ok := <-closes; ok && err != nil {
		log.Error().Err(err).Msg("publisher channel closed; will attempt lazy reinit on next publish")
		// Mark channel unusable; next Publish will reinit lazily
		p.mu.Lock()
		if p.ch == ch {
			p.ch = nil
		}
		p.mu.Unlock()
	}
}

// Close releases the current AMQP channel if it is open. It is safe to call
// multiple times. Background goroutines will exit once the broker closes the
// notification channels for the closed AMQP channel.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch.Close()
	}
	return nil
}

// Publish sends the provided message envelope to the given exchange and
// routing key with publisher confirms enabled. It retries on transient
// failures (nack, confirm timeout, closed channel) with exponential backoff
// and jitter, respecting the caller's context. If ctx is canceled or times out,
// Publish returns ctx.Err(). When the AMQP channel is missing or closed, it is
// lazily reinitialized.
//
// Example:
//
//	msg := &model.JSONMessage[any]{Payload: map[string]any{"job_id": "job-123"}}
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	if err := pub.Publish(ctx, "scan.jobs.x", "scan.jobs", true, msg); err != nil {
//	    // handle error
//	}
func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, mandatory bool, envelope model.Message) error {
	startTime := time.Now()
	// Lazy reinit if the channel is not ready
	p.mu.Lock()
	if p.ch == nil || p.ch.IsClosed() {
		if err := p.reinit(); err != nil {
			p.mu.Unlock()
			return err
		}
	}
	ch := p.ch
	p.mu.Unlock()

	maxRetries := p.config.MaxRetries
	backoffTime := p.config.InitialBackoff
	confirmTimeout := p.config.ConfirmTimeout

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		msgBody, err := envelope.GetPayload()
		if err != nil {
			return fmt.Errorf("error getting message payload: %w", err)
		}

		dc, err := ch.PublishWithDeferredConfirmWithContext(
			ctx, exchange, routingKey, mandatory, false,
			amqp.Publishing{
				CorrelationId: envelope.GetCorrelationId(),
				MessageId:     envelope.GetMessageId(),
				ContentType:   envelope.GetContentType(),
				Headers:       envelope.GetHeaders(),
				Body:          msgBody,
				DeliveryMode:  amqp.Persistent,
				Timestamp:     time.Now(),
			},
		)

		if err != nil {
			// If the channel/connection is closed, try to reinit and retry
			if errors.Is(err, amqp.ErrClosed) || (ch != nil && ch.IsClosed()) {
				lastErr = err

				p.mu.Lock()
				if rerr := p.reinit(); rerr != nil {
					lastErr = rerr
				}
				ch = p.ch
				p.mu.Unlock()
			} else {
				return err
			}
		} else {
			select {
			case <-ctx.Done():
				log.Warn().Msg("publish cancelled by the caller")
				return ctx.Err()

			case <-dc.Done():
				if dc.Acked() {
					return nil
				}

				log.Warn().
					Int("attempt", attempt).
					Uint64("deliveryTag", dc.DeliveryTag).
					Msg("publish failed; will retry")

				// Nack: treat as transient and retry a few times
				lastErr = ErrNacked

			case <-time.After(confirmTimeout):
				elapsed := time.Since(startTime)
				log.Warn().
					Str("exchange", exchange).
					Str("routingKey", routingKey).
					Str("correlationId", envelope.GetCorrelationId()).
					Str("messageId", envelope.GetMessageId()).
					Int("attempt", attempt).
					Dur("elapsed", elapsed).
					Uint64("deliveryTag", dc.DeliveryTag).
					Msg("publish timed out; closing channel and retry")

				lastErr = ErrConfirmTimeout

				// Close the channel to force reinit on the next publication
				_ = ch.Close()

				p.mu.Lock()
				p.ch = nil
				p.mu.Unlock()
			}
		}

		// Backoff before retrying
		select {
		case <-ctx.Done():
			log.Debug().Msg("publish cancelled by the caller")
			return ctx.Err()

		case <-time.After(backoff.Jitter(backoffTime)):
			if backoffTime < 5*time.Second {
				backoffTime *= 2
			}
		}
	}

	if lastErr != nil {
		return lastErr
	}

	return fmt.Errorf("publish failed after retries")
}
