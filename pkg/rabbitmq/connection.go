package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kubenetic/BunnyShepherd/pkg/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

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
	m.Lock()
	if config == nil {
		log.Debug().Msg("connecting to rabbitmq with default config")
		m.connection, err = amqp.Dial(url)
		m.config = m.connection.Config
	} else {
		log.Debug().Msg("connecting to rabbitmq with custom config")
		m.connection, err = amqp.DialConfig(url, *config)
		m.config = *config
	}
	m.Unlock()

	if err != nil {
		return fmt.Errorf("error connecting to rabbitmq: %w", err)
	}
	log.Debug().Msg("connected to rabbitmq")

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

		case err, ok := <-m.notifyChannel:
			if !ok {
				if err := m.cleanup(); err != nil {
					log.Error().Err(err).Msg("error closing connection")
				}
				return
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

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-time.After(backoffTime):
			if err := m.connect(m.url, &m.config); err == nil {
				log.Debug().Msg("successfully reconnected to rabbitmq")
				return nil
			}

			backoffTime = backoff.Jitter(backoffTime * 2)
			if backoffTime > maxBackoff {
				return fmt.Errorf("max reconnection backoff reached")
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
// It returns ErrConnectionNotInitilized if the connection has not been
// established yet, or ErrConnectionClosed if the existing connection is
// closed.
func (m *ConnectionManager) getChannel() (*amqp.Channel, error) {
	m.RLock()
	conn := m.connection
	m.RUnlock()

	if conn == nil {
		return nil, ErrConnectionNotInitilized
	}

	if conn.IsClosed() {
		return nil, ErrConnectionClosed
	}

	return conn.Channel()
}
