package websockets_test

import (
	"bytes"
	"compress/flate"
	"net"
	"testing"

	"lowbit.dev/websockets"
)

// Helper to generate a highly repetitive payload that mimics a large text stream/JSON payload.
func makeBenchPayload(size int) []byte {
	base := []byte("{\"status\":\"success\",\"message\":\"lowbit-performance-payload-validation-string\"}")
	return bytes.Repeat(base, (size/len(base))+1)[:size]
}

// Helper to spin up a background reader that drains the server pipe as fast as possible
// without allocating memory, isolating the writer's performance profile.
func startSink(conn net.Conn, done chan struct{}) {
	go func() {
		defer conn.Close()
		defer close(done)
		buf := make([]byte, 32*1024)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
}

func BenchmarkStreamMessageExt_Raw(b *testing.B) {
	payloadSize := 1024 * 1024 // 1 MB payload
	chunkSize := 4 * 1024      // 4 KB fragments
	payload := makeBenchPayload(payloadSize)

	b.ReportAllocs()
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clientConn, serverConn := net.Pipe()
		sinkDone := make(chan struct{})
		startSink(serverConn, sinkDone)

		client := websockets.NewConnection(clientConn, 4096, 0)
		r := bytes.NewReader(payload)

		b.StartTimer()
		if err := client.StreamMessageExt(websockets.OpCodeText, 0, chunkSize, r); err != nil {
			b.Fatalf("raw stream failed: %v", err)
		}
		b.StopTimer()

		clientConn.Close()
		<-sinkDone
	}
}

func BenchmarkStreamMessage_Deflate(b *testing.B) {
	payloadSize := 1024 * 1024 // 1 MB payload
	chunkSize := 4 * 1024      // 4 KB fragments
	payload := makeBenchPayload(payloadSize)

	b.ReportAllocs()
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clientConn, serverConn := net.Pipe()
		sinkDone := make(chan struct{})
		startSink(serverConn, sinkDone)

		baseClient := websockets.NewConnection(clientConn, 4096, 0)
		// We reuse flate.BestSpeed as our baseline compression profile
		deflateClient, err := websockets.WrapDeflate(baseClient, flate.BestSpeed)
		if err != nil {
			b.Fatalf("failed to init deflate: %v", err)
		}

		r := bytes.NewReader(payload)

		b.StartTimer()
		if err := deflateClient.StreamMessage(websockets.OpCodeText, chunkSize, r); err != nil {
			b.Fatalf("deflate stream failed: %v", err)
		}
		b.StopTimer()

		deflateClient.Close() // Flushes internal states and tears down wrappers
		<-sinkDone
	}
}
