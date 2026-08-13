package websockets

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"strings"

	"lowbit.dev/cooper"
)

var (
	ErrUpgradeFailed      = errors.New("websocket upgrade failed")
	ErrMissingKey         = errors.New("missing Sec-WebSocket-Key header")
	ErrUnsupportedVersion = errors.New("unsupported websocket version")
)

// Upgrader defines the server-side configuration for accepting WebSocket connections.
type Acceptor struct {
	MaxReadLimit int64
	MaxFrameSize int64

	Subprotocols []string

	connectionPool *ConnPool
	deflatePool    *DeflateConnPool
}

func NewAcceptor() *Acceptor {
	return &Acceptor{
		MaxReadLimit: ReadLimitStandard,
		MaxFrameSize: FrameSizeLowMemory,
	}
}

func NewAcceptorWithConnPool(p *ConnPool) *Acceptor {
	u := NewAcceptor()
	u.connectionPool = p
	return u
}

func NewAcceptorWithDeflatePool(p *DeflateConnPool) *Acceptor {
	u := NewAcceptor()
	u.deflatePool = p
	return u
}

func NewAcceptorWithConnAndDeflatePools(p *ConnPool, dp *DeflateConnPool) *Acceptor {
	u := NewAcceptor()
	u.connectionPool = p
	u.deflatePool = dp
	return u
}

// Accept inspects the HTTP request, performs the websocket handshake
// and returns a fully initialized WebSocket Connection interface.
func (a *Acceptor) Accept(w http.ResponseWriter, r *http.Request) (Connection, error) {
	return a.AcceptwWithConnFactory(w, r, func(c net.Conn) *Conn {
		if a.connectionPool != nil {
			return a.connectionPool.Acquire(c, 0, 0)
		}

		return NewConn(c, a.MaxReadLimit, a.MaxFrameSize)
	})
}

func (a *Acceptor) AcceptwWithConnFactory(w http.ResponseWriter, r *http.Request, connectionFactory func(net.Conn) *Conn) (Connection, error) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return nil, ErrUpgradeFailed
	}

	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		http.Error(w, "Upgrade Required", http.StatusUpgradeRequired)
		return nil, ErrUnsupportedVersion
	}

	clientKey := r.Header.Get("Sec-WebSocket-Key")
	if clientKey == "" {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return nil, ErrMissingKey
	}

	clientProtocols := r.Header.Get("Sec-WebSocket-Protocol")
	var selectedProtocol string
	if clientProtocols != "" && len(a.Subprotocols) > 0 {
		selectedProtocol = negotiateSubprotocol(clientProtocols, a.Subprotocols)
	}

	useDeflate := a.deflatePool != nil && strings.Contains(r.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate")

	rawConn, _, err := cooper.HijackAndReturn(w, r, cooper.BuildHijackConfig(
		cooper.Protocols("websocket"),
		cooper.ResponseHeaders(func(_ *http.Request, _ string) http.Header {
			h := http.Header{}
			h.Set("Sec-WebSocket-Accept", deriveAccept(clientKey))

			if selectedProtocol != "" {
				h.Set("Sec-WebSocket-Protocol", selectedProtocol)
			}

			if useDeflate {
				// Enforce strict no_context_takeover policies
				h.Set("Sec-WebSocket-Extensions", "permessage-deflate; server_no_context_takeover; client_no_context_takeover")
			}
			return h
		}),
	))

	if err != nil {
		var uerr *cooper.UpgradeError
		if errors.As(err, &uerr) {
			uerr.WriteTo(w)
		}

		return nil, err
	}

	c := connectionFactory(rawConn)
	c.isServer = true
	c.subprotocol = selectedProtocol

	if !useDeflate {
		return c, nil
	}

	return a.deflatePool.Acquire(c), nil
}

// negotiateSubprotocol matches the client's requested protocols against the server's
// supported list, returning the first server-preferred match.
func negotiateSubprotocol(clientHeader string, serverSupported []string) string {
	for _, supported := range serverSupported {
		for p := clientHeader; p != ""; {
			token, rest := nextToken(p)
			p = rest

			if strings.TrimSpace(token) == supported {
				return supported
			}
		}
	}

	return ""
}

func nextToken(s string) (token, rest string) {
	if idx := strings.IndexByte(s, ','); idx >= 0 {
		return s[:idx], s[idx+1:]
	}

	return s, ""
}

// Release safely returns a Connection and its underlying buffers to their
// respective pools. It should be explicitly deferred by the caller after Close.
func (a *Acceptor) Release(c Connection) {
	if c == nil {
		return
	}

	if dc, ok := c.(*PerMessageDeflateConn); ok {
		if a.deflatePool != nil {
			a.deflatePool.Release(dc)
		}

		c = dc.Conn
	}

	if base, ok := c.(*Conn); ok {
		if a.connectionPool != nil {
			a.connectionPool.Release(base)
		}
	}
}

// deriveAccept computes the response hash for a given client key.
func deriveAccept(clientKey string) string {
	h := sha1.New()
	h.Write([]byte(clientKey))
	h.Write([]byte("258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
