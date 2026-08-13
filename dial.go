package websockets

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"lowbit.dev/cooper"
)

var (
	ErrServerRejectedCompression = errors.New("server accepted connection but rejected required compression")
	ErrInvalidAcceptHash         = errors.New("server returned invalid Sec-WebSocket-Accept hash")
)

// dialConfig holds the internal configuration for a client connection.
type dialConfig struct {
	maxReadLimit      int64
	maxFrameSize      int64
	enableCompression bool
	compressionLevel  int
	deflatePool       *DeflateConnPool
	tlsConfig         *tls.Config
}

// DialOption configures how the WebSocket client connects and processes frames.
type DialOption func(*dialConfig)

// WithMaxReadLimit sets the maximum allowed size for an incoming message.
func WithMaxReadLimit(limit int64) DialOption {
	return func(c *dialConfig) { c.maxReadLimit = limit }
}

// WithMaxFrameSize sets the maximum size for a single WebSocket frame.
func WithMaxFrameSize(size int64) DialOption {
	return func(c *dialConfig) { c.maxFrameSize = size }
}

// WithDeflatePool enables permessage-deflate compression using a shared pool.
// Use this if your application is dialing short-lived connections at high frequency
// to eliminate heap allocation churn.
func WithDeflatePool(pool *DeflateConnPool) DialOption {
	return func(c *dialConfig) {
		c.enableCompression = true
		c.deflatePool = pool
	}
}

// WithUnPooledDeflate enables permessage-deflate compression at the specified level
// by allocating a standalone compression engine for this connection lifecycle.
// Use this for standard, single-connection clients.
func WithUnPooledDeflate(level int) DialOption {
	return func(c *dialConfig) {
		c.enableCompression = true
		c.compressionLevel = level
	}
}

// WithTLSConfig sets a custom TLS configuration for wss:// connections.
func WithTLSConfig(tlsCfg *tls.Config) DialOption {
	return func(c *dialConfig) { c.tlsConfig = tlsCfg }
}

// Dial initiates a WebSocket connection using the provided HTTP request
func Dial(req *http.Request, opts ...DialOption) (Connection, error) {
	cfg := &dialConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	nonce := make([]byte, 16)
	_, _ = rand.Read(nonce)
	clientKey := base64.StdEncoding.EncodeToString(nonce)
	expectedAccept := deriveAccept(clientKey)

	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", clientKey)

	if cfg.enableCompression {
		req.Header.Set("Sec-WebSocket-Extensions", "permessage-deflate; client_no_context_takeover; server_no_context_takeover")
	}

	serverAcceptedDeflate := false

	dialOpts := []cooper.DialOption{
		cooper.WithProtocol("websocket"),
	}

	if cfg.tlsConfig != nil {
		dialOpts = append(dialOpts, cooper.WithTLSConfig(cfg.tlsConfig))
	}

	dialOpts = append(dialOpts, cooper.WithUpgradeOptions(
		cooper.ResponseValidator(func(_ *http.Request, resp *http.Response) error {
			if resp.Header.Get("Sec-WebSocket-Accept") != expectedAccept {
				return ErrInvalidAcceptHash
			}

			// Did the server agree to compression?
			if cfg.enableCompression {
				ext := resp.Header.Get("Sec-WebSocket-Extensions")
				if strings.Contains(ext, "permessage-deflate") {
					serverAcceptedDeflate = true
				}
			}
			return nil
		}),
	))

	rawConn, err := cooper.Dial(req, dialOpts...)
	if err != nil {
		return nil, err
	}

	baseConn := NewConn(rawConn, cfg.maxReadLimit, cfg.maxFrameSize)
	baseConn.AssumeClientRole()

	if !serverAcceptedDeflate {
		return baseConn, nil
	}

	if cfg.deflatePool != nil {
		return cfg.deflatePool.Acquire(baseConn), nil
	}

	dc, err := WrapDeflate(baseConn, cfg.compressionLevel)
	if err != nil {
		baseConn.Close()
		return nil, err
	}

	return dc, nil
}
