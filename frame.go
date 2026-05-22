package websockets

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	// ErrInvalidOpCode is returned when a frame arrives with an unrecognized
	// or unallocated opcode bit configuration, violating RFC 6455 framing definitions.
	ErrInvalidOpCode = errors.New("invalid opcode")

	// ErrControlFramePayloadTooLarge is returned when a control frame (such as Ping,
	// Pong, or Close) arrives carrying a payload greater than 125 bytes, violating
	// the strict length boundary defined in RFC 6455 Section 5.5.
	ErrControlFramePayloadTooLarge = errors.New("control frame payload exceeds 125 bytes")

	// ErrFrameBufferTooSmall is returned when a slice provided to a low-level frame
	// reader function lacks the capacity or length necessary to ingest the incoming
	// frame's payload body without overflowing.
	ErrFrameBufferTooSmall = errors.New("provided buffer capacity too small for frame payload")
)

// OpCode represents the type of a WebSocket frame as defined by RFC 6455.
type OpCode byte

const (
	// =====================================================================
	// OpCodes
	// =====================================================================

	// OpCodeContinuation identifies a continuation frame.
	// It is used to continue a fragmented message started by a
	// preceding text or binary frame.
	OpCodeContinuation OpCode = 0

	// OpCodeText identifies a text frame containing UTF-8 encoded data.
	OpCodeText OpCode = 1

	// OpCodeBinary identifies a binary frame containing arbitrary
	// application-defined binary data.
	OpCodeBinary OpCode = 2

	// OpCodeClose identifies a close control frame used to initiate
	// or acknowledge the closing handshake.
	OpCodeClose OpCode = 8

	// OpCodePing identifies a ping control frame used to check whether
	// the peer is responsive. The recipient should reply with a Pong frame.
	OpCodePing OpCode = 9

	// OpCodePong identifies a pong control frame. It is sent in response
	// to a Ping frame or may be sent unsolicited as a heartbeat.
	OpCodePong OpCode = 10

	// =====================================================================
	// Frame header bit masks
	// =====================================================================

	// finalBit is the FIN bit in the WebSocket frame header.
	// When set, it indicates that this is the final frame of a message.
	finalBit byte = 1 << 7

	// maskBit is the MASK bit in the WebSocket frame header.
	// When set, it indicates that the payload data is masked and
	// is followed by a 4-byte masking key.
	maskBit byte = 1 << 7
)

// Frame represents the raw metadata of an individual RFC 6455 frame.
type Frame struct {
	IsFinal bool
	RSV     byte
	Op      OpCode
	Payload []byte
}

// Header represents the parsed metadata of an RFC 6455 frame header.
type Header struct {
	IsFinal    bool
	RSV        byte
	Op         OpCode
	IsMasked   bool
	PayloadLen int64
	MaskKey    [4]byte
}

// ReadHeader reads and parses a frame header directly from a raw connection.
// It performs no payload reading or heap allocations.
func (c *Conn) ReadHeader() (Header, error) {
	var hBuf [2]byte
	if _, err := io.ReadFull(c.underlying, hBuf[:]); err != nil {
		return Header{}, err
	}

	isFinal := (hBuf[0] & finalBit) != 0
	rsv := hBuf[0] & 0x70
	opCode := OpCode(hBuf[0] & 0x0F)
	isMasked := (hBuf[1] & maskBit) != 0
	payloadLen := int64(hBuf[1] & 0x7F)

	// Decode extended lengths sequentially from the wire
	switch payloadLen {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.underlying, ext[:]); err != nil {
			return Header{}, err
		}
		payloadLen = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.underlying, ext[:]); err != nil {
			return Header{}, err
		}
		payloadLen = int64(binary.BigEndian.Uint64(ext[:]))
	}

	if opCode >= OpCodeClose && payloadLen > 125 {
		return Header{}, ErrControlFramePayloadTooLarge
	}

	var maskKey [4]byte
	if isMasked {
		if _, err := io.ReadFull(c.underlying, maskKey[:]); err != nil {
			return Header{}, err
		}
	}

	return Header{
		IsFinal:    isFinal,
		RSV:        rsv,
		Op:         opCode,
		IsMasked:   isMasked,
		PayloadLen: payloadLen,
		MaskKey:    maskKey,
	}, nil
}

// ReadFrame reads a single raw frame from the connection buffer.
// It populates and returns the data inside the provided 'p' slice to minimize allocations.
func (c *Conn) ReadFrame(p []byte) (Frame, error) {
	// 1. Read the 2-byte frame header
	header := [2]byte{}
	if _, err := io.ReadFull(c.underlying, header[:]); err != nil {
		return Frame{}, err
	}

	isFinal := (header[0] & finalBit) != 0
	rsv := header[0] & 0x70
	opCode := OpCode(header[0] & 0x0F)
	isMasked := (header[1] & maskBit) != 0
	payloadLen := int64(header[1] & 0x7F)

	switch payloadLen {
	case 126:
		ext := [2]byte{}
		if _, err := io.ReadFull(c.underlying, ext[:]); err != nil {
			return Frame{}, err
		}

		payloadLen = int64(binary.BigEndian.Uint16(ext[:]))

	case 127:
		ext := [8]byte{}
		if _, err := io.ReadFull(c.underlying, ext[:]); err != nil {
			return Frame{}, err
		}

		payloadLen = int64(binary.BigEndian.Uint64(ext[:]))
	}

	if opCode >= OpCodeClose && payloadLen > 125 {
		return Frame{}, ErrControlFramePayloadTooLarge
	}

	var maskKey [4]byte
	if isMasked {
		if _, err := io.ReadFull(c.underlying, maskKey[:]); err != nil {
			return Frame{}, err
		}
	}

	var payload []byte
	if payloadLen > 0 {
		if int64(len(p)) < payloadLen {
			return Frame{}, fmt.Errorf("%w: %d bytes", ErrFrameBufferTooSmall, payloadLen)
		}
		payload = p[:payloadLen]
		if _, err := io.ReadFull(c.underlying, payload); err != nil {
			return Frame{}, err
		}
		if isMasked {
			unmask(payload, maskKey)
		}
	}

	return Frame{
		IsFinal: isFinal,
		RSV:     rsv,
		Op:      opCode,
		Payload: payload,
	}, nil
}

// WriteFrame is the lowest-level write primitive.
// It allows callers to manually construct fragmented messages by controlling the FIN bit.
// It uses the underlying buffered writer to minimize system calls and flushes immediately.
// It exposes the rsv byte (e.g., 0x40 for RSV1) so callers can implement extensions.
func (c *Conn) WriteFrame(isFinal bool, rsv byte, op OpCode, payload []byte) error {
	var firstByte byte
	if isFinal {
		firstByte |= 0x80
	}

	firstByte |= (rsv & 0x70) // Mask out everything but RSV1, RSV2, RSV3
	firstByte |= byte(op & 0x0F)

	c.writeBuf[0] = firstByte

	payloadLen := len(payload)
	headerLen := 2

	if payloadLen <= 125 {
		c.writeBuf[1] = byte(payloadLen) // Server frames do not mask data
	} else if payloadLen <= 65535 {
		c.writeBuf[1] = 126
		c.writeBuf[2] = byte(payloadLen >> 8)
		c.writeBuf[3] = byte(payloadLen)
		headerLen = 4
	} else {
		c.writeBuf[1] = 127
		c.writeBuf[2] = byte(payloadLen >> 56)
		c.writeBuf[3] = byte(payloadLen >> 48)
		c.writeBuf[4] = byte(payloadLen >> 40)
		c.writeBuf[5] = byte(payloadLen >> 32)
		c.writeBuf[6] = byte(payloadLen >> 24)
		c.writeBuf[7] = byte(payloadLen >> 16)
		c.writeBuf[8] = byte(payloadLen >> 8)
		c.writeBuf[9] = byte(payloadLen)
		headerLen = 10
	}

	copy(c.writeBuf[headerLen:], payload)

	if c.writeTCP != nil {
		_, err := c.writeTCP.Write(c.writeBuf[:headerLen+payloadLen])
		return err
	} else if c.writeTLS != nil {
		_, err := c.writeTLS.Write(c.writeBuf[:headerLen+payloadLen])
		return err
	} else {
		_, err := c.underlying.Write(c.writeBuf[:headerLen+payloadLen])
		return err
	}
}

// unmask applies the RFC 6455 XOR mask to the payload in-place.
// It requires zero allocations, satisfying the requirement to design for the runtime.
func unmask(payload []byte, maskKey [4]byte) {
	// For smaller payloads, a standard loop is perfectly traceable and fast.
	// The Go compiler is smart enough to optimize i%4 (or i&3) into fast bitwise ops.
	for i := 0; i < len(payload); i++ {
		payload[i] ^= maskKey[i&3] // i&3 is functionally identical to i%4 but faster
	}
}
