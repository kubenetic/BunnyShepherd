package rabbitmq

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/kubenetic/BunnyShepherd/pkg/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

// sanitizeAMQPURL removes credentials from an AMQP URL for safe logging.
func sanitizeAMQPURL(raw string) string {
	if raw == "" {
		return "amqp://***"
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "amqp://***"
	}

	// Check if URL has a scheme; if not, treat as malformed
	if u.Scheme == "" {
		return "amqp://***"
	}

	// If there are credentials, replace them with ***
	if u.User != nil {
		// Manually reconstruct to avoid URL encoding of the asterisks
		host := u.Host
		if u.Port() != "" {
			host = u.Hostname() + ":" + u.Port()
		}
		result := u.Scheme + "://***@" + host + u.Path
		if u.RawQuery != "" {
			result += "?" + u.RawQuery
		}
		if u.Fragment != "" {
			result += "#" + u.Fragment
		}
		return result
	}

	return u.String()
}

// isLocalhost checks if the given hostname is localhost or 127.0.0.1.
func isLocalhost(hostname string) bool {
	if hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1" {
		return true
	}
	return false
}

// warnIfPlaintextNonLocal logs a warning if the URI uses plaintext amqp:// with a non-local host.
func warnIfPlaintextNonLocal(uri string) {
	u, err := url.Parse(uri)
	if err != nil {
		return
	}

	// Only warn for plaintext amqp:// (not amqps://)
	if u.Scheme != "amqp" {
		return
	}

	hostname := u.Hostname()
	if hostname == "" {
		return
	}

	// Warn if not localhost
	if !isLocalhost(hostname) {
		log.Warn().
			Str("scheme", u.Scheme).
			Str("host", hostname).
			Msg("using plaintext amqp:// with non-local host; consider using amqps:// with TLS")
	}
}

// ConnectionManager manages a single RabbitMQ connection and provides utilities
// to get channels, monitor connection health, and perform automatic reconnection
// with exponential backoff.
//
// A ConnectionManager must be created via NewConnectionManager.
// Use Close to stop background monitoring and release resources.
//
// The zero value is not usable; always initialize with NewConnectionManager.
type ConnectionManager struct {
	url           string
	connection    *amqp.Connection
	config        amqp.Config
	notifyChannel chan *amqp.Error
	shutdown      chan bool
	wg            sync.WaitGroup
	sync.RWMutex
}

// NewConnectionManager creates and starts a ConnectionManager for the given
// RabbitMQ URL. If the config is nil, the default AMQP configuration is used.
// The manager establishes the initial connection and starts a background
// watcher that handles connection close events and automatic reconnection.
// The provided context controls the lifecycle of the background watcher.
//
// Example:
//
//	ctx := context.Background()
//	cm, err := rabbitmq.NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
//	if err != nil { panic(err) }
//	defer cm.Close()
func NewConnectionManager(ctx context.Context, url string, config *amqp.Config) (*ConnectionManager, error) {
	manager := &ConnectionManager{
		url:      url,
		shutdown: make(chan bool),
	}

	if err := manager.connect(url, config); err != nil {
		return nil, err
	}

	manager.wg.Add(1)
	go manager.watch(ctx)

	return manager, nil
}

// connect establishes a new AMQP connection using either the provided config
// or the library defaults. On success, it updates the manager state and
// registers a NotifyClose channel to observe connection close events.
func (m *ConnectionManager) connect(url string, config *amqp.Config) (err error) {
	// Warn if using plaintext with non-local host
	warnIfPlaintextNonLocal(url)

	m.Lock()
	if config == nil {
		log.Debug().Str("url", sanitizeAMQPURL(url)).Msg("connecting to rabbitmq with default config")
		m.connection, err = amqp.Dial(url)
		if err == nil {
			m.config = m.connection.Config
		}
	} else {
		log.Debug().Str("url", sanitizeAMQPURL(url)).Msg("connecting to rabbitmq with custom config")
		m.connection, err = amqp.DialConfig(url, *config)
		if err == nil {
			m.config = *config
		}
	}
	m.Unlock()

	if err != nil {
		return fmt.Errorf("error connecting to rabbitmq (%s): %w", sanitizeAMQPURL(url), err)
	}
	log.Debug().Str("url", sanitizeAMQPURL(url)).Msg("connected to rabbitmq")

	m.notifyChannel = make(chan *amqp.Error)
	m.connection.NotifyClose(m.notifyChannel)

	return
}

// watch monitors the connection lifecycle. It listens for context cancellation,
// explicit shutdown signals, and AMQP close notifications. When the connection
// is lost, it triggers cleanup followed by an automatic reconnection loop.
func (m *ConnectionManager) watch(ctx context.Context) {
	defer m.wg.Done()

	for {
		// Acquire read lock to get current notifyChannel
		m.RLock()
		notifyCh := m.notifyChannel
		m.RUnlock()

		select {
		case <-ctx.Done():
			if err := m.cleanup(); err != nil {
				log.Error().Err(err).Msg("error closing connection")
			}
			return

		case <-m.shutdown:
			if err := m.cleanup(); err != nil {
				log.Error().Err(err).Msg("error closing connection")
			}
			return

		case err, ok := <-notifyCh:
			if !ok {
				// Channel closed cleanly (e.g., broker graceful restart).
				// Attempt reconnection instead of exiting permanently.
				log.Warn().Msg("rabbitmq notify channel closed (clean disconnect); attempting reconnection")
				_ = m.cleanup()
				if err := m.reconnectionLoop(ctx); err != nil {
					log.Error().Err(err).Msg("error reconnecting to rabbitmq after clean disconnect")
				}
				continue
			}

			log.Error().Err(err).Msg("rabbitmq connection closed")
			_ = m.cleanup()
			if err := m.reconnectionLoop(ctx); err != nil {
				log.Error().Err(err).Msg("error reconnecting to rabbitmq")
			}
		}
	}

}

// reconnectionLoop attempts to re-establish the AMQP connection using
// exponential backoff. It returns when reconnection succeeds, the context is
// canceled, or the maximum backoff threshold is exceeded.
func (m *ConnectionManager) reconnectionLoop(ctx context.Context) error {
	backoffTime := 1 * time.Second
	maxBackoff := 32 * time.Second
	attempt := 0
	var lastErr error

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-time.After(backoffTime):
			attempt++
			if err := m.connect(m.url, &m.config); err == nil {
				log.Debug().Msg("successfully reconnected to rabbitmq")
				return nil
			} else {
				lastErr = err
			}

			backoffTime = backoff.Jitter(backoffTime * 2)
			if backoffTime > maxBackoff {
				log.Error().
					Int("attempts", attempt).
					Err(lastErr).
					Msg("max reconnection backoff reached; giving up")
				return fmt.Errorf("max reconnection backoff reached after %d attempts: %w", attempt, lastErr)
			}
		}
	}
}

// Close signals the background watcher to stop and waits for it to finish.
// It does not close the connection directly; the watcher will perform cleanup.
func (m *ConnectionManager) Close() {
	close(m.shutdown)
	m.wg.Wait()
}

// cleanup closes the underlying AMQP connection if it is open and clears the
// stored connection reference. It returns an error if the connection is already
// closed or was never initialized.
func (m *ConnectionManager) cleanup() error {
	var connection *amqp.Connection

	m.Lock()
	if m.connection != nil {
		connection = m.connection
		m.connection = nil
	}
	m.Unlock()

	if connection == nil {
		return fmt.Errorf("rabbitmq connection is not initialized")
	}

	if connection.IsClosed() {
		log.Debug().Msg("rabbitmq connection is already closed")
		return nil
	}

	if err := connection.Close(); err != nil {
		return fmt.Errorf("error closing connection: %w", err)
	}

	log.Debug().Msg("rabbitmq connection closed successfully")
	return nil
}

// getChannel returns a new AMQP channel created from the current managed
// connection. It is intended for internal use by components in this package.
// It returns ErrConnectionNotInitialized if the connection has not been
// established yet, or ErrConnectionClosed if the existing connection is
// closed.
func (m *ConnectionManager) getChannel() (*amqp.Channel, error) {
	m.RLock()
	conn := m.connection
	m.RUnlock()

	if conn == nil {
		return nil, ErrConnectionNotInitialized
	}

	if conn.IsClosed() {
		return nil, ErrConnectionClosed
	}

	return conn.Channel()
}
