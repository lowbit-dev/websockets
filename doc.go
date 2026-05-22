/*
Package websockets implements a minimalist, zero-allocation WebSocket transport
layer (RFC 6455 & RFC 7692) built entirely on the Go standard library.

Rather than managing network concurrency via hidden background goroutines, automated
heartbeats, or internal connection maps, this package provides synchronous, state-free
building blocks. Control over scheduling, buffer reuse, and I/O execution is left
completely to the calling application.

# Design Characteristics

  - Allocation-Free Framing: Frame assembly writes directly into a reusable,
    contiguous scratchpad allocated during connection setup, bypassing the runtime
    heap entirely on writes.

  - Lookahead Streaming: Both the raw and compressed streaming engines use a
    double-buffer lookahead strategy.

  - Direct Dispatch Cache: The connection extracts and caches concrete pointers
    for *net.TCPConn and *tls.Conn at initialization. This allows the compiler to
    inline writes and execute static method calls instead of generic interface lookups.

  - Abstract Extension Hooks: Features like permessage-deflate are decoupled as
    decorators. The package exposes a loose Compressor interface, letting you swap
    out the pure-Go stdlib flate engine for alternative hardware-accelerated
    or assembly-optimized implementations if needed.

# Usage

Connections are established by wrapping an existing network socket along with
explicit message and boundary size constraints:

	conn := websockets.NewConnection(netConn, maxReadLimit, maxChunkSize)
	defer conn.Close()

	// Reuse a local slice to keep read loops allocation-free
	readBuf := make([]byte, ReadLimitStandard, ChunkSizeLowMemory)
	for {
	    payload, op, err := conn.ReadMessage(readBuf)
	    if err != nil {
	        return // Physical drop, protocol error, or explicit close
	    }

	    if err := conn.WriteMessage(op, payload); err != nil {
	        return
	    }
	}
*/
package websockets
