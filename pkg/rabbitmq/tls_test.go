package rabbitmq

import (
	"os"
	"testing"
)

func TestNewSecureConfig_InvalidCAPath(t *testing.T) {
	_, err := NewSecureConfig("/nonexistent/path/ca.pem")
	if err == nil {
		t.Fatal("expected error for nonexistent CA file")
	}
}

func TestNewSecureConfig_EmptyCAPath(t *testing.T) {
	_, err := NewSecureConfig("")
	if err == nil {
		t.Fatal("expected error for empty CA path")
	}
}

func TestNewSecureConfig_InvalidPEM(t *testing.T) {
	// Create a temporary file with invalid PEM content
	tmpFile, err := os.CreateTemp("", "invalid-ca-*.pem")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString("not a valid PEM"); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	_, err = NewSecureConfig(tmpFile.Name())
	if err == nil {
		t.Fatal("expected error for invalid PEM content")
	}
}

func TestNewSecureConfig_ValidPEM(t *testing.T) {
	// Use a real certificate from the repo
	caFile := "/home/mikloshalasz/Workspace/AlbaOffice/SZTFH/common/banya-gitops/overlays/common/trust/GEO-Issuing-CA.pem"

	// Check if the file exists; skip if not
	if _, err := os.Stat(caFile); err != nil {
		t.Skipf("test certificate not found at %s: %v", caFile, err)
	}

	cfg, err := NewSecureConfig(caFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.TLSClientConfig == nil {
		t.Fatal("expected TLSClientConfig to be set")
	}

	if cfg.TLSClientConfig.RootCAs == nil {
		t.Fatal("expected RootCAs to be set")
	}
}
