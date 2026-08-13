package websockets

/*
This file implements the RFC 7692 "Compression Extensions for WebSocket"
specification, focusing specifically on the "permessage-deflate" framework.

---

The Extension Negotiation Protocol

WebSocket extensions are requested by the client during the initial HTTP handshake upgrade
and must be explicitly acknowledged by the server. If the server does not return the matching
extension header, the client will assume compression is disabled.

1. Client Handshake Request
The client includes the `Sec-WebSocket-Extensions` header indicating support:
	GET /ws HTTP/1.1
	Host: server.example.com
	Upgrade: websocket
	Connection: Upgrade
	Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits

2. Server Handshake Response
If the server accepts compression, it must echo back the accepted extension name in its 101 response:
	HTTP/1.1 101 Switching Protocols
	Upgrade: websocket
	Connection: Upgrade
	Sec-WebSocket-Extensions: permessage-deflate

*/

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"slices"
)

const (
	// rsv1Bit indicates that the payload is compressed using per-message deflate.
	rsv1Bit byte = 0x40
)

// Compressor defines the structural lifecycle operations required by the deflation loop.
// It directly matches the method signature of the standard library's *flate.Writer,
// allowing seamless interoperability with optimized assembly-accelerated engines.
type Compressor interface {
	io.Writer
	Flush() error
	Reset(w io.Writer)
	Close() error
}

// PerMessageDeflateConn wraps a base Conn to handle transparent RFC 7692 compression.
// It recycles an internal scratch buffer to keep reading paths allocation-efficient.
type PerMessageDeflateConn struct {
	*Conn

	// shared between pooled and standalone
	compressed []byte

	// Standalone opperation
	boundBuffer       *bytes.Buffer
	boundCompressor   Compressor
	boundDecompressor io.ReadCloser

	// pooled opperation
	compBufPool      *TypedPool[bytes.Buffer]
	compressorPool   *TypedPool[Compressor]
	decompressorPool *TypedPool[io.ReadCloser]
}

// WrapDeflate decorates a base connection with a DEFLATE compression layer.
// It initializes the flate compression engine with the specified compression level.
func WrapDeflate(conn *Conn, compressionLevel int) (*PerMessageDeflateConn, error) {
	if compressionLevel == 0 {
		compressionLevel = flate.DefaultCompression
	}

	dw, err := flate.NewWriter(nil, compressionLevel)
	if err != nil {
		return nil, err
	}

	return &PerMessageDeflateConn{
		Conn:              conn,
		boundCompressor:   dw,
		boundDecompressor: flate.NewReader(bytes.NewReader(nil)),
		boundBuffer:       &bytes.Buffer{},
		compressed:        make([]byte, 0, 1024),
	}, nil
}

// WrapDeflateWithCompressor allows for injecting an alternative hardware-accelerated compressor
// conforming to the Compressor interface.
func WrapDeflateWithCompressor(conn *Conn, compressor Compressor) (*PerMessageDeflateConn, error) {
	return &PerMessageDeflateConn{
		Conn:            conn,
		boundCompressor: compressor,
		boundBuffer:     &bytes.Buffer{},
		compressed:      make([]byte, 0, 1024),
	}, nil
}

// ReadMessage reads an assembled message and deflates it.
func (dc *PerMessageDeflateConn) ReadMessage(buf []byte) ([]byte, OpCode, error) {
	dc.compressed = dc.compressed[:0]

	var rsv byte
	var op OpCode
	var err error

	dc.compressed, op, rsv, err = dc.Conn.ReadMessageExt(dc.compressed)
	if err != nil {
		return buf, 0, err
	}

	if (rsv & rsv1Bit) == 0 {
		return append(buf, dc.compressed...), op, nil
	}

	// Append the 4-byte sync tail AND the 5-byte RFC 1951 empty final block.
	// This creates a structurally perfect, closed DEFLATE stream.
	dc.compressed = append(dc.compressed,
		0x00, 0x00, 0xff, 0xff, // Sync tail
		0x01, 0x00, 0x00, 0xff, 0xff, // Empty Final Block Terminator
	)

	r := bytes.NewReader(dc.compressed)

	decompressor, decompressorPtr := dc.acquireDecompressor()
	defer dc.releaseDecompressor(decompressorPtr)

	if resetter, ok := decompressor.(flate.Resetter); ok {
		if err := resetter.Reset(r, nil); err != nil {
			return buf, op, fmt.Errorf("%w: %w", ErrFailedResetDecompressor, err)
		}
	}

	for {
		start := len(buf)
		if cap(buf) == start {
			buf = slices.Grow(buf, 1024)
		}

		n, readErr := decompressor.Read(buf[start:cap(buf)])
		buf = buf[:start+n]

		if readErr == io.EOF {
			break
		}

		if readErr != nil {
			return buf, 0, readErr
		}
	}

	return buf, op, nil
}

// WriteMessage compresses the payload, and writes it as
// an unfragmented frame with RSV1 set. Control frames bypass compression.
func (dc *PerMessageDeflateConn) WriteMessage(op OpCode, payload []byte) error {
	if op >= OpCodeClose {
		return dc.Conn.WriteMessage(op, payload)
	}

	comp, buf, compPtr := dc.acquireCompressor()
	defer dc.releaseCompressor(buf, compPtr)

	// no_context_takeover
	comp.Reset(buf)

	if _, err := comp.Write(payload); err != nil {
		return err
	}

	if err := comp.Flush(); err != nil {
		return err
	}

	compressed := buf.Bytes()

	if len(compressed) >= 4 &&
		compressed[len(compressed)-4] == 0x00 &&
		compressed[len(compressed)-3] == 0x00 &&
		compressed[len(compressed)-2] == 0xff &&
		compressed[len(compressed)-1] == 0xff {
		compressed = compressed[:len(compressed)-4]
	}

	return dc.Conn.WriteFrame(true, rsv1Bit, op, compressed)
}

// StreamMessage reads raw data from r chunk-by-chunk, compresses it on the fly,
// and streams it over the wire as spec-compliant WebSocket fragments using a single-chunk lookahead.
func (dc *PerMessageDeflateConn) StreamMessage(op OpCode, chunkSize int, r io.Reader) error {
	if op != OpCodeText && op != OpCodeBinary {
		return fmt.Errorf("%w: opcode(%d)", ErrInvalidOpCode, op)
	}

	if chunkSize > int(dc.Conn.maxFrameSize) {
		return fmt.Errorf("%w: requested %d connection limit %d", ErrChunkSizeExceeded, chunkSize, dc.Conn.maxFrameSize)
	}

	var bufA, bufB []byte
	if dc.Conn.streamBufPool != nil {
		scratchPtr := dc.Conn.streamBufPool.Get()
		defer dc.Conn.streamBufPool.Put(scratchPtr)

		scratch := (*scratchPtr)[:cap(*scratchPtr)]

		bufA = scratch[:chunkSize]
		bufB = scratch[chunkSize : chunkSize*2]
	} else {
		bufA = make([]byte, chunkSize)
		bufB = make([]byte, chunkSize)
	}

	nA, errA := r.Read(bufA)
	if nA == 0 && errors.Is(errA, io.EOF) {
		return dc.Conn.WriteFrame(true, rsv1Bit, op, nil)
	}

	isFirst := true
	currentBuf, nextBuf := bufA, bufB
	nCurrent, errCurrent := nA, errA

	comp, buf, compPtr := dc.acquireCompressor()
	defer dc.releaseCompressor(buf, compPtr)

	// no_context_takeover
	comp.Reset(buf)

	for {
		// Look ahead to check if the current payload block is the final one
		nNext, errNext := r.Read(nextBuf)
		isFinal := (nNext == 0 && errors.Is(errNext, io.EOF))

		if _, compErr := comp.Write(currentBuf[:nCurrent]); compErr != nil {
			return compErr
		}

		if compErr := comp.Flush(); compErr != nil {
			return compErr
		}

		compressed := buf.Bytes()

		if isFinal {
			// Per RFC 7692, strip the 4-byte sync tail from the absolute end of the message payload
			if len(compressed) >= 4 &&
				compressed[len(compressed)-4] == 0x00 &&
				compressed[len(compressed)-3] == 0x00 &&
				compressed[len(compressed)-2] == 0xff &&
				compressed[len(compressed)-1] == 0xff {
				compressed = compressed[:len(compressed)-4]
			}
		}

		currentOp := op
		currentRSV := rsv1Bit
		if !isFirst {
			currentOp = OpCodeContinuation
			currentRSV = 0
		}

		if err := dc.Conn.WriteFrame(isFinal, currentRSV, currentOp, compressed); err != nil {
			return err
		}

		if isFinal {
			return nil
		}

		if errCurrent != nil && !errors.Is(errCurrent, io.EOF) {
			return errCurrent
		}

		// Swap states for the next pass, recycling the memory footprint completely
		isFirst = false
		currentBuf, nextBuf = nextBuf, currentBuf
		nCurrent, errCurrent = nNext, errNext

		buf.Reset()
	}
}

// Close terminates the active decompressor stream and shuts down the underlying connection.
func (dc *PerMessageDeflateConn) Close() error {
	if dc.boundCompressor != nil {
		_ = dc.boundCompressor.Close()
	}

	return dc.Conn.Close()
}

// ==========================================
//
// ==========================================

func (dc *PerMessageDeflateConn) acquireCompressor() (Compressor, *bytes.Buffer, *Compressor) {
	if dc.compressorPool != nil {
		compPtr := dc.compressorPool.Get()
		buf := dc.compBufPool.Get()
		return *compPtr, buf, compPtr
	}
	return dc.boundCompressor, dc.boundBuffer, nil
}

func (dc *PerMessageDeflateConn) releaseCompressor(buf *bytes.Buffer, compPtr *Compressor) {
	buf.Reset()

	if dc.compressorPool != nil && compPtr != nil {
		dc.compBufPool.Put(buf)
		dc.compressorPool.Put(compPtr)
	}
}

func (dc *PerMessageDeflateConn) acquireDecompressor() (io.ReadCloser, *io.ReadCloser) {
	if dc.decompressorPool != nil {
		rcPtr := dc.decompressorPool.Get()
		return *rcPtr, rcPtr
	}

	return dc.boundDecompressor, nil
}

func (dc *PerMessageDeflateConn) releaseDecompressor(rcPtr *io.ReadCloser) {
	if dc.decompressorPool != nil && rcPtr != nil {
		dc.decompressorPool.Put(rcPtr)
	}
}
