package rabbitmq

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
)

// BuildAMQPURL constructs a properly percent-encoded AMQP URL from its
// individual components. Special characters in user and password (such as
// %, <, >, (, ), ?, !) are automatically percent-encoded so they survive
// URL parsing by amqp.Dial.
//
// scheme must be "amqp" or "amqps".
// port may be 0 to omit the explicit port (the scheme default will be used).
// vhost may be empty to use the default vhost ("/").
//
// This is the recommended way to construct an AMQP URL when credentials are
// sourced from environment variables or secrets managers, where the password
// may contain characters that break naive string concatenation.
//
// Example:
//
//	rawURL, err := BuildAMQPURL("amqp", "bunny", "TzE%F2u18Gkqt4GTISv?<ZLx166(98v!", "rabbitmq.example.com", 5672, "uploads")
//	if err != nil { panic(err) }
//	cm, err := rabbitmq.NewConnectionManager(ctx, rawURL, nil)
func BuildAMQPURL(scheme, user, password, host string, port int, vhost string) (string, error) {
	if scheme != "amqp" && scheme != "amqps" {
		return "", fmt.Errorf("invalid AMQP scheme %q: must be \"amqp\" or \"amqps\"", scheme)
	}

	u := &url.URL{
		Scheme: scheme,
	}

	// Encode credentials — url.UserPassword percent-encodes any character that
	// is not allowed unescaped in the userinfo component of a URI (RFC 3986).
	if user != "" || password != "" {
		u.User = url.UserPassword(user, password)
	}

	// Build host[:port]
	if port > 0 {
		u.Host = net.JoinHostPort(host, strconv.Itoa(port))
	} else {
		u.Host = host
	}

	// Build vhost path — always starts with "/"
	if vhost == "" {
		u.Path = "/"
	} else {
		u.Path = "/" + vhost
	}

	return u.String(), nil
}
