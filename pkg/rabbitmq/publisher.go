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
	cm       *ConnectionManager
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
	returns  chan amqp.Return
	closes   chan *amqp.Error
	mu       sync.Mutex

	pra    int // publish reinit attempts
	config PublisherConfig
	// Optional callback for handling unroutable messages. If nil, unroutable messages
	// will be logged on error level and discarded.
	OnReturn ReturnHandler

	// Handler lifecycle management to prevent goroutine leaks
	handlersCancel context.CancelFunc
	handlersWg     sync.WaitGroup

	// In-flight publish tracking for graceful close
	inflightWg sync.WaitGroup
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
// MUST be called under the publisher mutex.
func (p *Publisher) reinit() error {
	// Cancel and wait for any existing handlers before creating new ones
	if p.handlersCancel != nil {
		p.handlersCancel()
		p.handlersWg.Wait()
	}

	ch, err := p.cm.getChannel()
	if err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return err
	}

	// Prepare fresh notify channels and goroutines.
	confirms := make(chan amqp.Confirmation, 128)
	ch.NotifyPublish(confirms)
	returns := make(chan amqp.Return, 128)
	ch.NotifyReturn(returns)
	closes := make(chan *amqp.Error, 1)
	ch.NotifyClose(closes)

	// Swap fields only after successful setup
	p.ch = ch
	p.confirms = confirms
	p.returns = returns
	p.closes = closes

	// Create a new context for the handler goroutines
	handlerCtx, cancel := context.WithCancel(context.Background())
	p.handlersCancel = cancel

	// Start handler goroutines with proper lifecycle management
	p.handlersWg.Add(2)
	go func() {
		defer p.handlersWg.Done()
		p.handleReturns(handlerCtx, returns)
	}()
	go func() {
		defer p.handlersWg.Done()
		p.handleClose(handlerCtx, ch, closes)
	}()

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
// broker closes the NotifyReturn channel or the context is cancelled.
// The OnReturn callback is invoked synchronously to avoid unbounded goroutine
// growth under a flood of returned messages.
func (p *Publisher) handleReturns(ctx context.Context, returns <-chan amqp.Return) {
	for {
		select {
		case <-ctx.Done():
			return
		case ret, ok := <-returns:
			if !ok {
				return
			}
			log.Error().
				Str("exchange", ret.Exchange).
				Str("routingKey", ret.RoutingKey).
				Uint16("replyCode", ret.ReplyCode).
				Str("text", ret.ReplyText).
				Msg("message returned (unroutable)")

			if p.OnReturn != nil {
				cbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := p.OnReturn(cbCtx, ret); err != nil {
					log.Error().Err(err).Msg("error handling return")
				}
				cancel()
			}
		}
	}
}

// handleClose observes close notifications for a specific AMQP channel
// instance. If the observed channel is still the one referenced by the
// publisher, it marks the channel as unusable so the next Publish lazily
// reinitialises it. Runs in its own goroutine and exits when the notification
// channel is closed or the context is cancelled.
//
// Deadlock-avoidance note: this function intentionally uses a short-lived
// TryLock pattern under context cancellation. If the parent (Close) is already
// holding p.mu while draining handlersWg, a blocking p.mu.Lock() here would
// deadlock. Instead we race ctx.Done() against mutex acquisition; if the ctx
// was cancelled first (i.e. Close is shutting us down) we exit without
// touching p.ch — Close will close the channel itself.
func (p *Publisher) handleClose(ctx context.Context, ch *amqp.Channel, closes <-chan *amqp.Error) {
	select {
	case <-ctx.Done():
		return
	case err, ok := <-closes:
		if !ok || err == nil {
			return
		}
		log.Error().Err(err).Msg("publisher channel closed; will attempt lazy reinit on next publish")

		// Acquire the mutex without deadlocking against Close. We retry
		// TryLock in a tight loop (polling) with a short backoff, and bail out
		// if the context was cancelled in the meantime — which is exactly the
		// Close-is-running case.
		for {
			if p.mu.TryLock() {
				if p.ch == ch {
					p.ch = nil
				}
				p.mu.Unlock()
				return
			}
			select {
			case <-ctx.Done():
				// Close is running; it will take care of clearing / closing ch.
				return
			case <-time.After(5 * time.Millisecond):
				// Retry
			}
		}
	}
}

// Close releases the current AMQP channel if it is open and waits for in-flight
// publishes to complete (with a 5-second timeout). It is safe to call multiple times.
// Background handler goroutines are cancelled and waited for before returning.
func (p *Publisher) Close() error {
	return p.CloseWithContext(context.Background())
}

// CloseWithContext releases the current AMQP channel if it is open and waits for
// in-flight publishes to complete within the provided context timeout. Background
// handler goroutines are cancelled and waited for before returning.
//
// Ordering (avoids deadlock with handleClose):
//  1. Capture the cancel func under the lock, release the lock.
//  2. Cancel the handler context WITHOUT holding p.mu — handleClose may be
//     trying to acquire p.mu after observing a broker-side close; cancelling
//     its context lets it bail out via TryLock/ctx.Done (see handleClose).
//  3. Wait for in-flight publishes and handlers to drain, still without the lock.
//  4. Finally re-acquire the lock for the channel-close fastpath.
func (p *Publisher) CloseWithContext(ctx context.Context) error {
	// Step 1: capture cancel under the lock.
	p.mu.Lock()
	cancel := p.handlersCancel
	p.handlersCancel = nil
	p.mu.Unlock()

	// Step 2: cancel outside the lock.
	if cancel != nil {
		cancel()
	}

	// Step 3a: wait for in-flight publishes (bounded by caller ctx AND 5s).
	done := make(chan struct{})
	go func() {
		p.inflightWg.Wait()
		close(done)
	}()

	timeout := time.After(5 * time.Second)
	select {
	case <-done:
		// All in-flight publishes completed
	case <-ctx.Done():
		log.Warn().Msg("timeout waiting for in-flight publishes during close (caller ctx)")
	case <-timeout:
		log.Warn().Msg("timeout waiting for in-flight publishes during close (5s)")
	}

	// Step 3b: wait for handlers. They will observe the cancelled ctx and
	// exit cleanly without attempting to acquire p.mu.
	p.handlersWg.Wait()

	// Step 4: close the channel under the lock.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil && !p.ch.IsClosed() {
		err := p.ch.Close()
		p.ch = nil
		return err
	}
	p.ch = nil
	return nil
}

// Publish sends the provided message envelope to the given exchange and
// routing key with publisher confirms enabled. It retries on transient
// failures (nack, confirm timeout, closed channel) with exponential backoff
// and jitter, respecting the caller's context. If ctx is canceled or times out,
// Publish returns ctx.Err(). When the AMQP channel is missing or closed, it is
// lazily reinitialized.
//
// Concurrency: amqp091 channels are NOT safe for concurrent use
// (see https://pkg.go.dev/github.com/rabbitmq/amqp091-go#Channel). This method
// therefore holds p.mu for the entire publish — including the deferred-confirm
// wait — so that two concurrent Publish calls on the same Publisher are
// serialized. The serialization is conceptually free: amqp091 already
// serializes per-channel operations internally, and this Publisher uses a
// single channel. If you need higher throughput, create multiple Publisher
// instances on separate channels.
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
	// Track in-flight for graceful Close. Incremented before we take the
	// publisher lock so Close's drain loop observes every publish.
	p.inflightWg.Add(1)
	defer p.inflightWg.Done()

	// Hold p.mu for the entire publish. amqp091 Channel is not safe for
	// concurrent use; serializing here makes the publisher correct.
	p.mu.Lock()
	defer p.mu.Unlock()

	startTime := time.Now()

	// Lazy reinit if the channel is not ready. Both the nil/closed check and
	// reinit are protected by the mutex to prevent concurrent reinit attempts.
	// amqp091 channels are not safe for concurrent use, so we serialize all
	// channel operations under p.mu.
	if p.ch == nil || p.ch.IsClosed() {
		if err := p.reinit(); err != nil {
			return err
		}
	}

	maxRetries := p.config.MaxRetries
	backoffTime := p.config.InitialBackoff
	confirmTimeout := p.config.ConfirmTimeout

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		msgBody, err := envelope.GetPayload()
		if err != nil {
			return fmt.Errorf("error getting message payload: %w", err)
		}

		ch := p.ch // snapshot for this attempt; reassigned on reinit below

		// Get the next sequence number before publishing
		seq := ch.GetNextPublishSeqNo()

		err = ch.PublishWithContext(
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
			// If the channel/connection is closed, try to reinit and retry.
			if errors.Is(err, amqp.ErrClosed) || (ch != nil && ch.IsClosed()) {
				lastErr = err
				if rerr := p.reinit(); rerr != nil {
					lastErr = rerr
				}
			} else {
				return err
			}
		} else {
			// Wait for the publisher confirm for this sequence number
			select {
			case <-ctx.Done():
				log.Warn().Msg("publish cancelled by the caller")
				return ctx.Err()

			case confirm := <-p.confirms:
				if confirm.DeliveryTag != seq {
					log.Error().
						Uint64("expectedSeq", seq).
						Uint64("gotSeq", confirm.DeliveryTag).
						Msg("confirm sequence mismatch")
					lastErr = fmt.Errorf("confirm sequence mismatch: got %d, want %d", confirm.DeliveryTag, seq)
				} else if !confirm.Ack {
					log.Warn().
						Int("attempt", attempt).
						Uint64("deliveryTag", seq).
						Msg("publish nacked by broker; will retry")
					lastErr = ErrNacked
				} else {
					// Successfully acked
					return nil
				}

			case <-time.After(confirmTimeout):
				elapsed := time.Since(startTime)
				log.Warn().
					Str("exchange", exchange).
					Str("routingKey", routingKey).
					Str("correlationId", envelope.GetCorrelationId()).
					Str("messageId", envelope.GetMessageId()).
					Int("attempt", attempt).
					Dur("elapsed", elapsed).
					Uint64("deliveryTag", seq).
					Msg("publish confirm timeout; closing channel and retry")

				lastErr = ErrConfirmTimeout

				// Close the channel to force reinit on the next attempt.
				_ = ch.Close()
				p.ch = nil
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
