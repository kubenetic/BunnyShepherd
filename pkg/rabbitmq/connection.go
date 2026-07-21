package rabbitmq

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/kubenetic/BunnyShepherd/pkg/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

// defaultInitialConnectMaxElapsed is the default upper bound for the initial
// connection retry loop. Two minutes is chosen because:
//   - Istio/Envoy sidecar warm-up is typically 5–30 s; 2 min gives ample headroom.
//   - RabbitMQ cluster rolling restarts complete well within 2 min in practice.
//   - Kubernetes readiness probes (default failureThreshold=3, periodSeconds=10)
//     will mark the pod NotReady after ~30 s, but the pod stays alive and keeps
//     retrying — this is the desired behaviour.
//   - A genuinely misconfigured endpoint (wrong host/port) will still fail fast
//     enough (≤2 min) to surface the misconfiguration rather than hanging forever.
const defaultInitialConnectMaxElapsed = 2 * time.Minute

// connectOptions holds tunable parameters for the initial connection attempt.
type connectOptions struct {
	maxElapsed time.Duration // 0 means "use default"; negative means "no retry"
}

// ConnectionOption is a functional option for NewConnectionManager.
type ConnectionOption func(*connectOptions)

// WithInitialConnectMaxElapsed sets the maximum total elapsed time for the
// initial connection retry loop. Use 0 to keep the library default (2 min).
// Use a negative value (e.g. -1) to disable retry entirely and fail on the
// first error — useful in tests or when the caller manages its own retry.
func WithInitialConnectMaxElapsed(d time.Duration) ConnectionOption {
	return func(o *connectOptions) {
		o.maxElapsed = d
	}
}

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
		// u.Host already contains brackets for IPv6 literals and the port in
		// the correct form (e.g. "[::1]:5672"), so use it directly instead of
		// reconstructing from Hostname()+":"+Port() which strips the brackets.
		result := u.Scheme + "://***@" + u.Host + u.Path
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
	// dialFn is the low-level dial step called by initialConnectWithRetry.
	// It is nil in production (the real connect method is used) and can be
	// replaced in tests to inject a fake without touching a real broker.
	dialFn func(url string, config *amqp.Config) error
	sync.RWMutex
}

// NewConnectionManager creates and starts a ConnectionManager for the given
// RabbitMQ URL. If the config is nil, the default AMQP configuration is used.
// The manager establishes the initial connection and starts a background
// watcher that handles connection close events and automatic reconnection.
// The provided context controls the lifecycle of the background watcher.
//
// By default the initial connection attempt is retried with jittered
// exponential backoff (1 s → 32 s per attempt) for up to 2 minutes, mirroring
// the post-connection reconnection semantics. Pass WithInitialConnectMaxElapsed
// to override the bound or disable retry entirely.
//
// Example:
//
//	ctx := context.Background()
//	cm, err := rabbitmq.NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
//	if err != nil { panic(err) }
//	defer cm.Close()
func NewConnectionManager(ctx context.Context, url string, config *amqp.Config, opts ...ConnectionOption) (*ConnectionManager, error) {
	o := connectOptions{maxElapsed: 0} // 0 → use default
	for _, opt := range opts {
		opt(&o)
	}

	maxElapsed := o.maxElapsed
	if maxElapsed == 0 {
		maxElapsed = defaultInitialConnectMaxElapsed
	}

	manager := &ConnectionManager{
		url:      url,
		shutdown: make(chan bool),
	}

	if err := manager.initialConnectWithRetry(ctx, url, config, maxElapsed); err != nil {
		return nil, err
	}

	manager.wg.Add(1)
	go manager.watch(ctx)

	return manager, nil
}

// initialConnectWithRetry attempts to establish the initial AMQP connection,
// retrying with jittered exponential backoff until success, context
// cancellation, or maxElapsed is exceeded.
//
// When maxElapsed is negative, no retry is performed — the first error is
// returned immediately. This mirrors the behaviour of the old single-attempt
// code and is useful for callers that manage their own retry or in tests.
func (m *ConnectionManager) initialConnectWithRetry(ctx context.Context, url string, config *amqp.Config, maxElapsed time.Duration) error {
	// dial is the actual dial step. In production it is m.connect; in tests
	// m.dialFn can be set to a fake that fails N times then succeeds.
	dial := m.connect
	if m.dialFn != nil {
		dial = m.dialFn
	}

	// Fast path: retry disabled.
	if maxElapsed < 0 {
		return dial(url, config)
	}

	backoffTime := 1 * time.Second
	maxBackoff := 32 * time.Second
	deadline := time.Now().Add(maxElapsed)
	attempt := 0

	for {
		attempt++
		err := dial(url, config)
		if err == nil {
			if attempt > 1 {
				log.Info().
					Int("attempt", attempt).
					Str("url", sanitizeAMQPURL(url)).
					Msg("initial rabbitmq connection established after retries")
			}
			return nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			log.Warn().
				Int("attempts", attempt).
				Dur("maxElapsed", maxElapsed).
				Err(err).
				Msg("giving up on initial rabbitmq connection: max elapsed time exceeded")
			return fmt.Errorf("initial rabbitmq connection failed after %d attempt(s) over %s: %w", attempt, maxElapsed, err)
		}

		// Respect context cancellation.
		select {
		case <-ctx.Done():
			log.Debug().
				Int("attempts", attempt).
				Msg("initial rabbitmq connection cancelled by context")
			return fmt.Errorf("initial rabbitmq connection cancelled after %d attempt(s): %w", attempt, ctx.Err())
		default:
		}

		// Cap the sleep to the remaining deadline so we don't overshoot.
		sleep := backoff.Jitter(backoffTime)
		if sleep > remaining {
			sleep = remaining
		}

		log.Info().
			Int("attempt", attempt).
			Dur("retryIn", sleep).
			Err(err).
			Msg("initial rabbitmq connection failed; will retry")

		select {
		case <-ctx.Done():
			log.Debug().
				Int("attempts", attempt).
				Msg("initial rabbitmq connection cancelled by context during backoff")
			return fmt.Errorf("initial rabbitmq connection cancelled after %d attempt(s): %w", attempt, ctx.Err())
		case <-time.After(sleep):
		}

		backoffTime *= 2
		if backoffTime > maxBackoff {
			backoffTime = maxBackoff
		}
	}
}

// NewConnectionManagerFromCredentials creates a ConnectionManager from
// individual credential components instead of a pre-built URL string.
//
// This is the recommended approach when credentials may contain special
// characters (%, <, >, (, ), ?, ! etc.), because it passes the username and
// password directly through SASL PlainAuth — bypassing URL parsing entirely.
// No percent-encoding of credentials is required by the caller.
//
// scheme must be "amqp" or "amqps".
// port may be 0 to use the scheme default.
// vhost may be empty to use the default vhost ("/").
// tlsConfig may be nil; if non-nil it is used for TLS (typically with "amqps").
//
// Any ConnectionOption values are forwarded to NewConnectionManager unchanged.
//
// Example:
//
//	cm, err := rabbitmq.NewConnectionManagerFromCredentials(
//	    ctx,
//	    "amqp", "bunny", "TzE%F2u18Gkqt4GTISv?<ZLx166(98v!",
//	    "rabbitmq.example.com", 5672, "uploads",
//	    nil,
//	)
func NewConnectionManagerFromCredentials(
	ctx context.Context,
	scheme, user, password, host string,
	port int,
	vhost string,
	tlsConfig *tls.Config,
	opts ...ConnectionOption,
) (*ConnectionManager, error) {
	// Build a credential-free URL — credentials travel via SASL, not the URL.
	credentialFreeURL, err := BuildAMQPURL(scheme, "", "", host, port, vhost)
	if err != nil {
		return nil, fmt.Errorf("building AMQP URL: %w", err)
	}

	effectiveVhost := vhost
	if effectiveVhost == "" {
		effectiveVhost = "/"
	}

	cfg := amqp.Config{
		SASL: []amqp.Authentication{
			&amqp.PlainAuth{
				Username: user,
				Password: password,
			},
		},
		Vhost:           effectiveVhost,
		TLSClientConfig: tlsConfig,
	}

	return NewConnectionManager(ctx, credentialFreeURL, &cfg, opts...)
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
				log.Warn().
					Int("attempts", attempt).
					Err(lastErr).
					Msg("max reconnection backoff reached for this cycle; will retry from max backoff")
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
