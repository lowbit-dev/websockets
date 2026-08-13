package websockets

import (
	"context"
	"io"
	"time"
)

// FrameReaderWriter exposes the raw RFC 6455 framing primitives.
// These always bypass the compression layer (control frames, raw fragments).
type FrameReaderWriter interface {
	ReadHeader() (Header, error)
	ReadFrame(p []byte) (Frame, error)
	WriteFrame(isFinal bool, rsv byte, op OpCode, payload []byte) error
}

// DeadlineManager exposes the low-level network timeout controls.
type DeadlineManager interface {
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// HandlerConfigurator exposes the configuration API for intercepting control frames.
type HandlerConfigurator interface {
	SetPingHandler(h PingHandler)
	SetPongHandler(h PongHandler)
	SetCloseHandler(h CloseHandler)
}

// MessageReaderWriter exposes the high-level, allocation-efficient message layer.
// This is what PerMessageDeflateConn explicitly overrides.
type MessageReaderWriter interface {
	ReadMessage(buf []byte) ([]byte, OpCode, error)
	WriteMessage(op OpCode, payload []byte) error
	StreamMessage(op OpCode, chunkSize int, r io.Reader) error
}

type LifecycleManager interface {
	KeepAlive(ctx context.Context, interval time.Duration) error
}

// Connection represents a fully featured, protocol-compliant WebSocket connection.
// It unifies raw framing, pooled message processing, heartbeats, and lifecycles.
type Connection interface {
	io.Closer
	HandlerConfigurator
	DeadlineManager
	FrameReaderWriter
	MessageReaderWriter
	LifecycleManager

	// Context returns the connection's lifecycle context, canceled
	// when the socket is closed or severed.
	Context() context.Context

	Subprotocol() string
}
