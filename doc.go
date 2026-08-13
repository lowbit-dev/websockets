// Package websockets provides a high-performance, zero-allocation implementation 
// of the RFC 6455 WebSocket protocol and the RFC 7692 permessage-deflate extension.
//
// Designed for extreme throughput, this package natively supports memory pooling 
// for both byte buffers and compression engines, allowing it to saturate network 
// links at multi-gigabyte speeds with zero garbage collection overhead.
//
// This package delegates the HTTP/1.1 101 Switching Protocols handshake to the 
// lowbit.dev/cooper package, completely decoupling the complex HTTP lifecycle and 
// buffer management from the raw WebSocket framing engine.
//
// # Server-Side Usage
//
// Use an Acceptor to intercept incoming HTTP requests and upgrade them to a 
// WebSocket connection:
//
//	acceptor := &websockets.Acceptor{
//		MaxReadLimit:      8192,
//		MaxFrameSize:      4096,
//		EnableCompression: true,
//	}
//
//	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
//		conn, err := acceptor.Accept(w, r)
//		if err != nil {
//			return // Acceptor automatically handles 400/426 HTTP error responses
//		}
//		defer conn.Close()
//		
//		// Read loop
//		var buf []byte
//		for {
//			buf, op, err = conn.ReadMessage(buf[:0])
//			if err != nil {
//				break
//			}
//			conn.WriteMessage(op, buf)
//		}
//	})
//
// # Client-Side Usage
//
// Use a Dialer to establish outbound WebSocket connections. The dialer uses 
// cooper.Dial under the hood to perform the TCP connection and TLS handshake 
// in a single call.
//
//	dialer := &websockets.Dialer{
//		EnableCompression: true,
//	}
//	
//	conn, err := dialer.Dial(context.Background(), "wss://example.com/ws", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer conn.Close()
package websockets
