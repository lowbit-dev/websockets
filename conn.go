package websockets

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"time"
)

var (
	// ErrMessageTooBig is returned when an incoming message's total payload size
	// exceeds the configured maxReadLimit boundary to prevent out-of-memory vectors.
	ErrMessageTooBig = errors.New("message exceeds max read limit")

	// ErrProtocolReservedBits is returned when a frame arrives with its RSV1, RSV2,
	// or RSV3 bits set without an explicit extension (like permessage-deflate)
	// having negotiated their use during the handshake.
	ErrProtocolReservedBits = errors.New("protocol error, reserved bits must be 0")

	// ErrWebSocketClosed is returned when a read or write operation is attempted
	// on a connection that has already completed its closure handshake or has
	// been physically severed.
	ErrWebSocketClosed = errors.New("websocket closed")

	// ErrUnexpectedContinuation is returned when a continuation frame is received
	// on the wire but no fragmented message sequence was currently in progress.
	ErrUnexpectedContinuation = errors.New("protocol error, unexpected continuation frame")

	// ErrExpectedContinuation is returned when a new message initiator frame is
	// received before the prior fragmented frame sequence was properly concluded
	// with a final (FIN=true) continuation frame.
	ErrExpectedContinuation = errors.New("protocol error, expected continuation frame")

	// ErrChunkSizeExceeded is returned when a streaming operation requests a chunk
	// size that is larger than the pre-allocated maxChunkSize limit defined
	// during connection initialization.
	ErrChunkSizeExceeded = errors.New("requested chunk size exceeds connection limit")
)

// CloseCode represents a WebSocket close status code as defined by RFC 6455.
type CloseCode uint16

const (
	// CloseNormalClosure indicates a normal connection closure.
	CloseNormalClosure CloseCode = 1000

	// CloseGoingAway indicates that an endpoint is "going away",
	// such as a server shutdown or browser navigation.
	CloseGoingAway CloseCode = 1001

	// CloseProtocolError indicates a protocol error.
	CloseProtocolError CloseCode = 1002

	// CloseUnsupportedData indicates that the endpoint received
	// data of a type it cannot accept.
	CloseUnsupportedData CloseCode = 1003

	// CloseReserved is reserved and must not be used.
	CloseReserved CloseCode = 1004

	// CloseNoStatusReceived indicates that no status code was present.
	// This code is reserved and must not be sent in a Close frame.
	CloseNoStatusReceived CloseCode = 1005

	// CloseAbnormalClosure indicates that the connection closed
	// abnormally without sending or receiving a Close frame.
	// This code is reserved and must not be sent in a Close frame.
	CloseAbnormalClosure CloseCode = 1006

	// CloseInvalidFramePayloadData indicates that the endpoint received
	// inconsistent or invalid data within a message.
	CloseInvalidFramePayloadData CloseCode = 1007

	// ClosePolicyViolation indicates that the endpoint is terminating
	// the connection due to a policy violation.
	ClosePolicyViolation CloseCode = 1008

	// CloseMessageTooBig indicates that a message was too large to process.
	CloseMessageTooBig CloseCode = 1009

	// CloseMandatoryExtension indicates that the client expected one or
	// more extensions that were not negotiated by the server.
	CloseMandatoryExtension CloseCode = 1010

	// CloseInternalServerError indicates that the server encountered
	// an unexpected condition that prevented it from fulfilling the request.
	CloseInternalServerError CloseCode = 1011

	// CloseTLSHandshake indicates that the connection was closed due
	// to a failure during the TLS handshake.
	// This code is reserved and must not be sent in a Close frame.
	CloseTLSHandshake CloseCode = 1015

	// ReadLimitSmall (64 KB) restricts incoming messages to light updates,
	// individual events, or text command signals.
	ReadLimitSmall = 64 * 1024

	// ReadLimitStandard (1 MB) provides an industry-standard ceiling suitable
	// for typical JSON API payloads, application state syncs, and average documents.
	ReadLimitStandard = 1024 * 1024

	// ReadLimitLarge (16 MB) accommodates heavy incoming data pipelines, such as
	// raw image uploads, file attachments, or dense data arrays.
	ReadLimitLarge = 16 * 1024 * 1024

	// ChunkSizeLowMemory (4 KB) minimizes the per-connection RAM footprint.
	// Aligns with standard OS virtual memory pages and fits well within common
	// network MTU boundaries. Best for high-concurrency systems like chat,
	// notifications, or IoT gateways.
	ChunkSizeLowMemory = 4096

	// ChunkSizeBalanced (8 KB) provides a middle ground for typical web applications
	// transferring medium-sized text or JSON payloads.
	ChunkSizeBalanced = 8192

	// ChunkSizeStreaming (32 KB) maximizes processing throughput for heavy file
	// transfers or compressed data lines. This matches the internal sliding
	// history window of the DEFLATE algorithm and standard io.Copy buffers.
	ChunkSizeStreaming = 32768
)

type PingHandler func([]byte) error
type PongHandler func([]byte) error
type CloseHandler func(CloseCode, []byte)

// Conn represents an active RFC 6455 WebSocket connection.
type Conn struct {
	underlying net.Conn

	// fast path
	writeTCP *net.TCPConn
	writeTLS *tls.Conn

	writeBuf []byte

	// maxReadLimit protects the server from OOM attacks during continuation frame assembly.
	maxReadLimit int64
	maxChunkSize int64

	// pingHandler is invoked synchronously when a Ping frame is read.
	pingHandler  PingHandler
	pongHandler  PongHandler
	closeHandler CloseHandler
}

// New takes an already-upgraded HTTP connection and wraps it in a WebSocket Conn.
// It requires a maxReadLimit to strictly bound memory allocations during frame assembly,
// protecting the runtime from out-of-memory vulnerabilities.
// if maxReadLimit is 0, the default of ReadLimitStandard (1MB) will be applied
// if maxChunkSize is 0, the default of ChunkSizeLowMemory (4KB) will be applied
func NewConnection(conn net.Conn, maxReadLimit int64, maxChunkSize int64) *Conn {
	if maxReadLimit == 0 {
		maxReadLimit = ReadLimitStandard
	}

	if maxChunkSize == 0 {
		maxChunkSize = ChunkSizeLowMemory
	}

	c := &Conn{
		underlying:   conn,
		maxReadLimit: maxReadLimit,
		maxChunkSize: maxChunkSize,
		closeHandler: func(oc CloseCode, b []byte) {},
		writeBuf:     make([]byte, 14+maxChunkSize),
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		c.writeTCP = tcp
	} else if tc, ok := conn.(*tls.Conn); ok {
		c.writeTLS = tc
	}

	c.pingHandler = func(d []byte) error {
		return c.WriteFrame(true, 0, OpCodePong, d)
	}

	c.pongHandler = func(_ []byte) error {
		return nil
	}

	return c
}

// SetPingHandler allows callers to inject application logic (e.g., heartbeats).
func (c *Conn) SetPingHandler(h PingHandler) {
	c.pingHandler = h
}

func (c *Conn) SetPongHandler(h PongHandler) {
	c.pongHandler = h
}

func (c *Conn) SetCloseHandler(h CloseHandler) {
	c.closeHandler = h
}

func (c *Conn) ReadMessage(buf []byte) ([]byte, OpCode, error) {
	payload, op, rsv, err := c.ReadMessageExt(buf)
	if err != nil {
		return payload, op, err
	}

	if rsv != 0 {
		return payload, 0, fmt.Errorf("%w: 0x%02x", ErrProtocolReservedBits, rsv)
	}

	return payload, op, nil
}

func (c *Conn) ReadMessageExt(buf []byte) ([]byte, OpCode, byte, error) {
	var firstOpCode OpCode
	var isFragmented bool
	var ctrlBuf [125]byte
	var rsv byte

	for {
		header, err := c.ReadHeader()
		if err != nil {
			return buf, 0, 0, err
		}

		if header.Op < OpCodeClose {
			if int64(len(buf))+header.PayloadLen > c.maxReadLimit {
				return buf, 0, 0, fmt.Errorf("%w: %d bytes", ErrMessageTooBig, c.maxReadLimit)
			}
		}

		// route payload target explicitly
		var targetBuf []byte
		if header.Op >= OpCodeClose {
			targetBuf = ctrlBuf[:header.PayloadLen]
		} else {
			needed := int(header.PayloadLen)
			if cap(buf)-len(buf) < needed {
				buf = slices.Grow(buf, needed)
			}

			start := len(buf)
			buf = buf[:start+needed]
			targetBuf = buf[start : start+needed]
		}

		// Pull payload off the raw wire directly into the target segment
		if header.PayloadLen > 0 {
			if _, err := io.ReadFull(c.underlying, targetBuf); err != nil {
				return buf, 0, 0, err
			}

			if header.IsMasked {
				unmask(targetBuf, header.MaskKey)
			}
		}

		// Intercept Control Paths Synchronously
		if header.Op >= OpCodeClose {
			switch header.Op {
			case OpCodePing:
				if err := c.pingHandler(targetBuf); err != nil {
					return buf, 0, rsv, err
				}

				continue
			case OpCodePong:
				if err := c.pongHandler(targetBuf); err != nil {
					return buf, 0, rsv, err
				}

				continue
			case OpCodeClose:
				var code uint16 = 1000
				var text []byte
				if len(targetBuf) >= 2 {
					code = binary.BigEndian.Uint16(targetBuf[:2])
					text = targetBuf[2:]
				}

				c.closeHandler(CloseCode(code), text)

				return buf, 0, rsv, fmt.Errorf("%w: %d", ErrWebSocketClosed, code)
			}
		}

		// Enforce Sequencing Logic
		if header.Op == OpCodeContinuation && !isFragmented {
			return buf, 0, rsv, ErrUnexpectedContinuation
		}

		if header.Op != OpCodeContinuation && isFragmented {
			return buf, 0, rsv, fmt.Errorf("%w: got data opcode %d", ErrExpectedContinuation, header.Op)
		}

		if !isFragmented {
			firstOpCode = header.Op
			isFragmented = !header.IsFinal
			rsv = header.RSV
		}

		if header.IsFinal {
			break
		}
	}

	return buf, firstOpCode, rsv, nil
}

// WriteMessage sends a single, unfragmented WebSocket frame.
// It is a predictable convenience wrapper around WriteFrame for the vast majority of use cases.
func (c *Conn) WriteMessage(op OpCode, payload []byte) error {
	return c.WriteFrame(true, 0, op, payload)
}

// StreamMessageExt reads from r chunk-by-chunk and streams it over the network as fragmented frames.
// It utilizes a double-buffer lookahead to eliminate empty trailing closure fragments.
// Returns ErrInvalidOpCode if the initial opcode is not OpCodeText or OpCodeBinary.
func (c *Conn) StreamMessageExt(op OpCode, rsv byte, chunkSize int, r io.Reader) error {
	if op != OpCodeText && op != OpCodeBinary {
		return fmt.Errorf("%w: opcode(%d)", ErrInvalidOpCode, op)
	}

	if chunkSize > int(c.maxChunkSize) {
		return fmt.Errorf("%w: requested %d connection limit %d", ErrChunkSizeExceeded, chunkSize, c.maxChunkSize)
	}

	bufA := make([]byte, chunkSize)
	bufB := make([]byte, chunkSize)

	// Ingest the initial chunk up front
	nA, errA := r.Read(bufA)
	if nA == 0 && errors.Is(errA, io.EOF) {
		return c.WriteFrame(true, rsv, op, nil) // Pristine empty message
	}

	isFirst := true
	currentBuf, nextBuf := bufA, bufB
	nCurrent, errCurrent := nA, errA

	for {
		// Look ahead: Read the next block into the secondary buffer
		nNext, errNext := r.Read(nextBuf)
		isFinal := (nNext == 0 && errors.Is(errNext, io.EOF))

		effectiveOp := op
		effectiveRSV := rsv
		if !isFirst {
			effectiveOp = OpCodeContinuation
			effectiveRSV = 0
		}

		if err := c.WriteFrame(isFinal, effectiveRSV, effectiveOp, currentBuf[:nCurrent]); err != nil {
			return err
		}

		if isFinal {
			return nil
		}

		if errCurrent != nil && !errors.Is(errCurrent, io.EOF) {
			return errCurrent
		}

		// Rotate the buffers and state variables for the next fragment pass
		isFirst = false
		currentBuf, nextBuf = nextBuf, currentBuf
		nCurrent, errCurrent = nNext, errNext
	}
}

func (c *Conn) StreamMessage(op OpCode, chunkSize int, r io.Reader) error {
	return c.StreamMessageExt(op, 0, chunkSize, r)
}

// CloseWithCode sends a specific RFC 6455 closure status code before
// severing the underlying network connection.
func (c *Conn) CloseWithCode(code CloseCode) error {
	// Stack-allocate the standard 2-byte closure payload.
	var payload [2]byte
	binary.BigEndian.PutUint16(payload[:], uint16(code))

	// Attempt to gracefully notify the client.
	// We ignore the error because network state during teardown is highly
	// volatile, and we are closing the socket immediately regardless.
	_ = c.WriteMessage(OpCodeClose, payload[:])

	// Sever the underlying connection to free the file descriptor.
	return c.underlying.Close()
}

// Close satisfies the io.Closer interface.
// It initiates a standard normal closure (status 1000) and terminates the socket,
// allowing it to compose naturally with standard Go defer patterns.
func (c *Conn) Close() error {
	return c.CloseWithCode(CloseNormalClosure)
}

// KeepAlive blocks and sends a Ping frame at the specified interval.
// It relies on the caller to dispatch it in a goroutine. Hidden state fails silently;
// by forcing the caller to invoke this, concurrency management remains entirely
// visible and under the application's control.
func (c *Conn) KeepAlive(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// A standard RFC 6455 Ping frame has no payload requirements,
	// so we can use a zero-allocation empty byte slice.
	emptyPayload := []byte{}

	for {
		select {
		case <-ctx.Done():
			// The caller canceled the context; gracefully terminate the ping loop.
			return ctx.Err()
		case <-ticker.C:
			// WriteMessage handles the synchronization and buffer flushing.
			if err := c.WriteMessage(OpCodePing, emptyPayload); err != nil {
				return err
			}
		}
	}
}
