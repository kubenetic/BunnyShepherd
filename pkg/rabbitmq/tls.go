package rabbitmq

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// NewSecureConfig loads a CA certificate from the provided file path and returns
// an amqp.Config with TLS configured. The returned config is ready to use with
// DialConfig or NewConnectionManager.
//
// If caFile is empty, an error is returned.
// If the file cannot be read or the PEM cannot be parsed, an error is returned.
//
// Example:
//
//	cfg, err := NewSecureConfig("/etc/ssl/certs/ca.pem")
//	if err != nil { panic(err) }
//	cm, err := NewConnectionManager(ctx, "amqps://user:pass@rabbitmq.example.com:5671/", &cfg)
func NewSecureConfig(caFile string) (amqp.Config, error) {
	if caFile == "" {
		return amqp.Config{}, fmt.Errorf("caFile cannot be empty")
	}

	// Read the CA certificate file
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return amqp.Config{}, fmt.Errorf("error reading CA file: %w", err)
	}

	// Create a certificate pool and add the CA
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caPEM) {
		return amqp.Config{}, fmt.Errorf("error parsing CA certificate from %s", caFile)
	}

	// Create TLS configuration
	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	// Return AMQP config with TLS
	return amqp.Config{
		TLSClientConfig: tlsConfig,
	}, nil
}
