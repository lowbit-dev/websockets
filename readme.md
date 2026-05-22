# websockets

[![Go Report Card](https://goreportcard.com/badge/lowbit.dev/websockets)](https://goreportcard.com/report/lowbit.dev/websockets)[![Go Reference](https://pkg.go.dev/badge/lowbit.dev/websockets.svg)](https://pkg.go.dev/lowbit.dev/websockets) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A minimalist WebSocket transport layer (RFC 6455 & RFC 7692) for Go.

This package provides synchronous, state-free primitives for WebSocket communication. It does not manage network concurrency via hidden background goroutines, automated heartbeats, or internal connection maps; control over scheduling, buffer reuse, and I/O execution is left entirely to the calling application.

---

## Features

* **Allocation-Free Framing:** Frame assembly writes directly into a reusable, contiguous scratchpad allocated during connection setup, bypassing the runtime heap entirely on writes.
* **Lookahead Streaming:** Both the raw and compressed streaming engines use a double-buffer lookahead strategy. 
* **Direct Dispatch Cache:** The connection extracts and caches concrete pointers for `*net.TCPConn` and `*tls.Conn` at initialization. This allows the compiler to inline writes and execute static method calls instead of generic interface lookups.
* **Pure Standard Library:** Zero external dependencies. Features like `permessage-deflate` are decoupled as decorators, exposing a loose `Compressor` interface to allow alternative implementations.

---

## Performance Baselines

The following metrics were captured on an Apple M1 Pro streaming 1 MB payloads over local loopbacks (`net.Pipe`):

| Operation                         | Throughput | Allocations | Heap Churn |
| --------------------------------- | ---------- | ----------- | ---------- |
| `StreamMessageExt` (Raw TCP Path) | ~4.01 GB/s | 0 allocs/op | 0 B/op     |
| `StreamMessage` (Deflate Path)    | ~770 MB/s  | 0 allocs/op | 0 B/op     |

*Note: The 3 setup allocations recorded during full benchmark runs represent the one-time initialization overhead of the test pipeline or the compression engine, not the hot-path loop execution.*

---

## Installation

```sh
go get lowbit.dev/websockets

```

---

## Quick Start

### 1. Connection Initialization

```go
package main

import (
	"net/http"
	"strings"
	"lowbit.dev/websockets"
)

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	extensions := r.Header.Get("Sec-WebSocket-Extensions")
	negotiateDeflate := strings.Contains(strings.ToLower(extensions), "permessage-deflate")

	hj, _ := w.(http.Hijacker)
	netConn, rw, _ := hj.Hijack()
	
	// NOTE: The returned 'rw' (*bufio.ReadWriter) may contain unread buffered bytes 
	// if the client sent data immediately following the upgrade request headers. 
	// Applications must inspect and drain rw.Reader, or use an upgrade coordinator 
	// (like Cooper) that automatically handles buffered byte replay.

	// [Perform standard HTTP 101 Handshake validation and write response headers here]
	
	var maxReadLimit int64 = 1024 * 1024 // 1 MB -- ReadLimitStandard
	var maxChunkSize int64 = 4096        // 4 KB -- ChunkSizeLowMemory
	
	conn := websockets.NewConnection(netConn, maxReadLimit, maxChunkSize)
	defer conn.Close()

	if negotiateDeflate {
		deflateConn, _ := websockets.WrapDeflate(conn, websockets.BestSpeed)
		defer deflateConn.Close()
		runEchoLoop(deflateConn)
	} else {
		runEchoLoop(conn)
	}
}

```

### 2. Reading & Writing Messages

To maintain a flat memory profile during read loops, provide a reusable byte buffer to `ReadMessage`. Incoming payloads will slice directly into this buffer when possible.

```go
type WebSocketPeer interface {
	ReadMessage(buf []byte) (payload []byte, op websockets.OpCode, err error)
	WriteMessage(op websockets.OpCode, payload []byte) error
}

func runEchoLoop(ws WebSocketPeer) {
	readBuf := make([]byte, 4096)

	for {
		payload, op, err := ws.ReadMessage(readBuf)
		if err != nil {
			return // Connection closed, dropped, or protocol error
		}

		switch op {
		case websockets.OpCodeText, websockets.OpCodeBinary:
			if err := ws.WriteMessage(op, payload); err != nil {
				return
			}
		case websockets.OpCodeClose:
			return
		}
	}
}

```

### 3. Chunked Streaming

The lookahead engine dynamically segments an `io.Reader` into fixed fragments. Memory consumption remains locked to the specified chunk size.

```go
func streamLargePayload(ws *websockets.Conn, data io.Reader) error {
	// Fragments and flushes data in 4KB chunks with zero heap allocations.
	// Returns ErrChunkSizeExceeded if the chunk size parameters exceed the connection maxChunkSize limit.
	return ws.StreamMessageExt(websockets.OpCodeBinary, 0x00, 4096, data)
}

```

---

## Advanced: Alternative Compressors

The package uses Go's standard `compress/flate` implementation by default. If your environment requires higher compression throughput, you can swap out the standard library engine for an assembly-optimized or SIMD-accelerated alternative (such as Klauspost's implementation) via the `WrapDeflateWithCompressor` hook:

```go
import (
	"io"
	"github.com/klauspost/compress/flate"
	"lowbit.dev/websockets"
)

func CustomCompressionUpgrade(netConn net.Conn) {
	baseConn := websockets.NewConnection(netConn, ReadLimitStandard, ChunkSizeLowMemory)

	// Initialize the alternative assembly-accelerated compressor
	klausCompressor, _ := flate.NewWriter(io.Discard, flate.BestSpeed)

	// Inject the alternative implementation into the deflation decorator layer
	deflateConn := websockets.WrapDeflateWithCompressor(baseConn, klausCompressor)
	defer deflateConn.Close()
}

```

---

## Ecosystem: Handshake Management with Cooper

This library integrates with `cooper`, a zero-dependency handshake tool that manages HTTP/1.1 protocol negotiation, connection hijacking, and response header verification.

`cooper.Hijack` transparently wraps the underlying `bufio` read buffers, prepending any leftover bytes back onto the returned connection before passing it to your WebSocket initialization loops:

```go
package main

import (
	"net"
	"net/http"
	"lowbit.dev/cooper"
	"lowbit.dev/websockets"
)

func main() {
	handler := cooper.Hijack(func(netConn net.Conn, proto string) {
		defer netConn.Close()

		// The connection returned by cooper is safe to use immediately; 
		// any early client data has already been prepended.
		conn := websockets.NewConnection(netConn, ReadLimitStandard, ChunkSizeLowMemory)
		
		// Run read/write loops...
	}, 
		cooper.Protocols("websocket"),
		cooper.ResponseHeaders(func(r *http.Request, proto string) http.Header {
			h := http.Header{}
			h.Set("Sec-WebSocket-Accept", calculateAcceptKey(r.Header.Get("Sec-WebSocket-Key")))
			return h
		}),
	)

	http.Handle("/ws", handler)
	http.ListenAndServe(":8080", nil)
}

```

---

## License

MIT License. See `LICENSE` for details.