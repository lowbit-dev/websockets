package websockets

import (
	"errors"
	"fmt"
)

// Base Errors
var (
	// ErrProtocolError is the base error for all RFC 6455 specification violations.
	// Callers can use errors.Is(err, ErrProtocolError) to broadly catch any
	// structural or protocol-level failure on the connection.
	ErrProtocolError = errors.New("protocol error")

	// ErrLimitExceeded is the base error for all local size and policy limits.
	ErrLimitExceeded = errors.New("limit exceeded")
)

var (
	// ErrInvalidOpCode is returned when a frame arrives with an unrecognized
	// or unallocated opcode bit configuration, violating RFC 6455 framing definitions.
	ErrInvalidOpCode = fmt.Errorf("%w: invalid opcode", ErrProtocolError)

	// ErrFrameTooLarge is returned when an outbound payload, combined with its
	// necessary protocol header, exceeds the connection's configured maxFrameSize.
	// This enforces strict boundaries on outgoing data chunks and protects the
	// internal memory pools from overflowing.
	ErrFrameTooLarge = fmt.Errorf("%w: frame exceeds maximum configured frame size", ErrLimitExceeded)

	// ErrControlFramePayloadTooLarge is returned when a control frame (such as Ping,
	// Pong, or Close) arrives carrying a payload greater than 125 bytes, violating
	// the strict length boundary defined in RFC 6455 Section 5.5.
	ErrControlFramePayloadTooLarge = fmt.Errorf("%w: control frame payload exceeds 125 bytes", ErrProtocolError)

	// ErrFrameBufferTooSmall is returned when a slice provided to a low-level frame
	// reader function lacks the capacity or length necessary to ingest the incoming
	// frame's payload body without overflowing.
	ErrFrameBufferTooSmall = fmt.Errorf("%w: provided buffer capacity too small", ErrLimitExceeded)

	// ErrUnmaskedClientFrame is returned when a server receives a frame from a client
	// that does not have the mask bit set. RFC 6455 Section 5.1 mandates that all
	// client-to-server frames must be masked to prevent intermediate proxy cache
	// poisoning attacks.
	ErrUnmaskedClientFrame = fmt.Errorf("%w: client frame must be masked", ErrProtocolError)

	// ErrMaskedServerFrame is returned when a client receives a frame from a server
	// that has the mask bit set. RFC 6455 Section 5.1 specifies that a server MUST NOT
	// mask any frames it sends to the client.
	ErrMaskedServerFrame = fmt.Errorf("%w: server frame must not be masked", ErrProtocolError)

	// ErrInvalidUTF8 is returned when a completely assembled text message payload
	// (OpCodeText) contains invalid UTF-8 byte sequences. RFC 6455 Section 5.6 requires
	// that all text payloads be valid UTF-8. For fragmented messages, this is evaluated
	// only after all fragments are concatenated.
	ErrInvalidUTF8 = fmt.Errorf("%w: invalid utf-8 payload", ErrProtocolError)

	// ErrMessageTooBig is returned when an incoming message's total payload size
	// exceeds the configured maxReadLimit boundary to prevent out-of-memory vectors.
	ErrMessageTooBig = fmt.Errorf("%w: message exceeds max read limit", ErrLimitExceeded)

	// ErrProtocolReservedBits is returned when a frame arrives with its RSV1, RSV2,
	// or RSV3 bits set without an explicit extension (like permessage-deflate)
	// having negotiated their use during the handshake.
	ErrProtocolReservedBits = fmt.Errorf("%w: reserved bits must be 0", ErrProtocolError)

	// ErrWebSocketClosed is returned when a read or write operation is attempted
	// on a connection that has already completed its closure handshake or has
	// been physically severed.
	ErrWebSocketClosed = errors.New("websocket closed")

	// ErrUnexpectedContinuation is returned when a continuation frame is received
	// on the wire but no fragmented message sequence was currently in progress.
	ErrUnexpectedContinuation = fmt.Errorf("%w: unexpected continuation frame", ErrProtocolError)

	// ErrExpectedContinuation is returned when a new message initiator frame is
	// received before the prior fragmented frame sequence was properly concluded
	// with a final (FIN=true) continuation frame.
	ErrExpectedContinuation = fmt.Errorf("%w: expected continuation frame", ErrProtocolError)

	// ErrChunkSizeExceeded is returned when a streaming operation requests a chunk
	// size that is larger than the pre-allocated maxChunkSize limit defined
	// during connection initialization.
	ErrChunkSizeExceeded = fmt.Errorf("%w: requested chunk size exceeds connection limit", ErrLimitExceeded)

	// ErrConnectionReleasedToPool is returned when a read, write, or control operation
	// is attempted on a connection pointer that has already been returned to the sync.Pool.
	ErrConnectionReleasedToPool = errors.New("connection was released to the pool")

	// ErrFailedResetDecompressor is returned when the internal permessage-deflate engine
	// fails to reset its dictionary state for a new incoming message. Because this library
	// enforces strict "no_context_takeover", the decompressor must successfully clear its
	// memory between every fragmented message. If this fails, the compression state is
	// fatally corrupted and the connection must be aborted.
	ErrFailedResetDecompressor = errors.New("failed to reset the flate decompressor")
)
