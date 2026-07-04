package rabbitmq

import (
	"net/url"
	"strings"
	"testing"
)

// TestBuildAMQPURL verifies that BuildAMQPURL produces correctly percent-encoded
// URLs whose credentials survive a round-trip through url.Parse.
func TestBuildAMQPURL(t *testing.T) {
	tests := []struct {
		name     string
		scheme   string
		user     string
		password string
		host     string
		port     int
		vhost    string
		wantErr  bool
		wantUser string
		wantPass string
		wantHost string
		wantPath string
	}{
		{
			name:     "special characters in password — the real-world case",
			scheme:   "amqp",
			user:     "bunny",
			password: `TzE%F2u18Gkqt4GTISv?<ZLx166(98v!`,
			host:     "rabbitmq.example.com",
			port:     5672,
			vhost:    "uploads",
			wantUser: "bunny",
			wantPass: `TzE%F2u18Gkqt4GTISv?<ZLx166(98v!`,
			wantHost: "rabbitmq.example.com:5672",
			wantPath: "/uploads",
		},
		{
			name:     "simple credentials no special chars",
			scheme:   "amqp",
			user:     "guest",
			password: "guest",
			host:     "localhost",
			port:     5672,
			vhost:    "",
			wantUser: "guest",
			wantPass: "guest",
			wantHost: "localhost:5672",
			wantPath: "/",
		},
		{
			name:     "amqps scheme with TLS port",
			scheme:   "amqps",
			user:     "admin",
			password: "s3cr3t!@#$",
			host:     "secure.rabbitmq.example.com",
			port:     5671,
			vhost:    "prod",
			wantUser: "admin",
			wantPass: "s3cr3t!@#$",
			wantHost: "secure.rabbitmq.example.com:5671",
			wantPath: "/prod",
		},
		{
			name:     "password with percent sign",
			scheme:   "amqp",
			user:     "user",
			password: "pass%word",
			host:     "localhost",
			port:     5672,
			vhost:    "/",
			wantUser: "user",
			wantPass: "pass%word",
			wantHost: "localhost:5672",
			wantPath: "//",
		},
		{
			name:     "password with at-sign",
			scheme:   "amqp",
			user:     "user",
			password: "p@ssword",
			host:     "localhost",
			port:     5672,
			vhost:    "myvhost",
			wantUser: "user",
			wantPass: "p@ssword",
			wantHost: "localhost:5672",
			wantPath: "/myvhost",
		},
		{
			name:     "password with colon",
			scheme:   "amqp",
			user:     "user",
			password: "pass:word",
			host:     "localhost",
			port:     5672,
			vhost:    "myvhost",
			wantUser: "user",
			wantPass: "pass:word",
			wantHost: "localhost:5672",
			wantPath: "/myvhost",
		},
		{
			name:     "no port uses scheme default",
			scheme:   "amqp",
			user:     "guest",
			password: "guest",
			host:     "localhost",
			port:     0,
			vhost:    "myvhost",
			wantUser: "guest",
			wantPass: "guest",
			wantHost: "localhost",
			wantPath: "/myvhost",
		},
		{
			name:     "empty vhost defaults to slash",
			scheme:   "amqp",
			user:     "guest",
			password: "guest",
			host:     "localhost",
			port:     5672,
			vhost:    "",
			wantUser: "guest",
			wantPass: "guest",
			wantHost: "localhost:5672",
			wantPath: "/",
		},
		{
			name:     "no credentials",
			scheme:   "amqp",
			user:     "",
			password: "",
			host:     "localhost",
			port:     5672,
			vhost:    "myvhost",
			wantUser: "",
			wantPass: "",
			wantHost: "localhost:5672",
			wantPath: "/myvhost",
		},
		{
			name:    "invalid scheme returns error",
			scheme:  "http",
			user:    "user",
			host:    "localhost",
			port:    5672,
			wantErr: true,
		},
		{
			name:    "empty scheme returns error",
			scheme:  "",
			user:    "user",
			host:    "localhost",
			port:    5672,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildAMQPURL(tt.scheme, tt.user, tt.password, tt.host, tt.port, tt.vhost)

			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildAMQPURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			// The URL must be parseable — this is what amqp.Dial does internally.
			parsed, err := url.Parse(got)
			if err != nil {
				t.Fatalf("url.Parse(%q) failed: %v — the URL is not valid", got, err)
			}

			// Scheme must be preserved.
			if parsed.Scheme != tt.scheme {
				t.Errorf("scheme = %q, want %q", parsed.Scheme, tt.scheme)
			}

			// Host must be preserved.
			if parsed.Host != tt.wantHost {
				t.Errorf("host = %q, want %q", parsed.Host, tt.wantHost)
			}

			// Path must be preserved.
			if parsed.Path != tt.wantPath {
				t.Errorf("path = %q, want %q", parsed.Path, tt.wantPath)
			}

			// Credentials must round-trip exactly — this is the core of the fix.
			if tt.wantUser == "" && tt.wantPass == "" {
				if parsed.User != nil {
					t.Errorf("expected no credentials in URL, got user=%q", parsed.User.Username())
				}
				return
			}

			if parsed.User == nil {
				t.Fatal("parsed.User is nil — credentials were lost")
			}

			gotUser := parsed.User.Username()
			gotPass, hasPass := parsed.User.Password()

			if gotUser != tt.wantUser {
				t.Errorf("username round-trip failed: got %q, want %q", gotUser, tt.wantUser)
			}

			if tt.wantPass != "" && !hasPass {
				t.Errorf("password was lost during URL encoding")
			}

			if gotPass != tt.wantPass {
				t.Errorf("password round-trip failed: got %q, want %q", gotPass, tt.wantPass)
			}
		})
	}
}

// TestBuildAMQPURL_NaiveConcatenationWouldBreak demonstrates explicitly that
// naive string concatenation fails for the real-world password, while
// BuildAMQPURL succeeds.
func TestBuildAMQPURL_NaiveConcatenationWouldBreak(t *testing.T) {
	const (
		user     = "bunny"
		password = `TzE%F2u18Gkqt4GTISv?<ZLx166(98v!`
		host     = "rabbitmq.example.com"
		port     = "5672"
		vhost    = "uploads"
	)

	// Naive concatenation — what the old code does.
	naiveURL := "amqp://" + user + ":" + password + "@" + host + ":" + port + "/" + vhost

	parsed, err := url.Parse(naiveURL)
	if err != nil {
		// url.Parse is lenient; it rarely errors. The damage is silent.
		t.Logf("url.Parse on naive URL returned error (expected): %v", err)
	} else {
		// The password extracted from the naive URL will NOT match the original
		// because the % in the password is interpreted as a percent-encoding prefix.
		_, gotPass, _ := strings.Cut(parsed.User.String(), ":")
		if gotPass == password {
			t.Error("naive concatenation unexpectedly succeeded — test assumption is wrong")
		} else {
			t.Logf("naive URL password mismatch confirmed: got %q, want %q", gotPass, password)
		}
	}

	// BuildAMQPURL — the fix.
	safeURL, err := BuildAMQPURL("amqp", user, password, host, 5672, vhost)
	if err != nil {
		t.Fatalf("BuildAMQPURL() unexpected error: %v", err)
	}

	parsed2, err := url.Parse(safeURL)
	if err != nil {
		t.Fatalf("url.Parse on safe URL failed: %v", err)
	}

	if parsed2.User == nil {
		t.Fatal("credentials missing from safe URL")
	}

	gotPass, _ := parsed2.User.Password()
	if gotPass != password {
		t.Errorf("BuildAMQPURL password round-trip failed: got %q, want %q", gotPass, password)
	}

	t.Logf("safe URL: %s", safeURL)
	t.Logf("password round-trip: OK (%q)", gotPass)
}

// TestBuildAMQPURL_SchemePreserved checks that the scheme is always one of the
// two valid AMQP schemes and is never silently mangled.
func TestBuildAMQPURL_SchemePreserved(t *testing.T) {
	for _, scheme := range []string{"amqp", "amqps"} {
		got, err := BuildAMQPURL(scheme, "u", "p", "localhost", 5672, "/")
		if err != nil {
			t.Errorf("scheme %q: unexpected error: %v", scheme, err)
			continue
		}
		if !strings.HasPrefix(got, scheme+"://") {
			t.Errorf("scheme %q: URL %q does not start with expected prefix", scheme, got)
		}
	}
}
