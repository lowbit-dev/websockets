package websockets_test

import (
	"bytes"
	"compress/flate"
	"net"
	"testing"

	"lowbit.dev/websockets"
)

var benchmarkPayloadSize = (1024 * 1024) * 10

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

func BenchmarkWriteMessage_Raw(b *testing.B) {
	// A typical, small JSON payload
	payload := []byte(`{"type":"heartbeat","timestamp":1672531200,"status":"ok"}`)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	clientConn, serverConn := net.Pipe()
	sinkDone := make(chan struct{})
	startSink(serverConn, sinkDone)

	client := websockets.NewConn(clientConn, 4096, 4096)
	client.AssumeServerRole()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := client.WriteMessage(websockets.OpCodeText, payload); err != nil {
			b.Fatalf("write message failed: %v", err)
		}
	}

	b.StopTimer()
	clientConn.Close()
	<-sinkDone
}

func BenchmarkStreamMessageExt_Raw(b *testing.B) {
	chunkSize := (4 * 1024) - 8 // 4 KB fragments - 8 byte header
	payload := makeBenchPayload(benchmarkPayloadSize)

	b.ReportAllocs()
	b.SetBytes(int64(benchmarkPayloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clientConn, serverConn := net.Pipe()
		sinkDone := make(chan struct{})
		startSink(serverConn, sinkDone)

		client := websockets.NewConn(clientConn, 4096, 0)
		client.AssumeServerRole()

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

func BenchmarkStreamMessageExt_Raw_Pooled(b *testing.B) {
	chunkSize := (4 * 1024) - 8 // 4 KB fragments - 8 byte header
	payload := makeBenchPayload(benchmarkPayloadSize)

	b.ReportAllocs()
	b.SetBytes(int64(benchmarkPayloadSize))

	connPool := websockets.NewConnPool(4096, 4096)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clientConn, serverConn := net.Pipe()
		sinkDone := make(chan struct{})
		startSink(serverConn, sinkDone)

		client := connPool.Acquire(clientConn, 4096, 4096)
		client.AssumeServerRole()

		r := bytes.NewReader(payload)

		b.StartTimer()
		if err := client.StreamMessageExt(websockets.OpCodeText, 0, chunkSize, r); err != nil {
			b.Fatalf("pooled raw stream failed: %v", err)
		}
		b.StopTimer()

		client.Close()
		connPool.Release(client)
		<-sinkDone
	}
}

func BenchmarkWriteMessage_Deflate_Standalone(b *testing.B) {
	payload := []byte(`{"type":"heartbeat","timestamp":1672531200,"status":"ok"}`)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	clientConn, serverConn := net.Pipe()
	sinkDone := make(chan struct{})
	startSink(serverConn, sinkDone)

	baseClient := websockets.NewConn(clientConn, 4096, 4096)
	baseClient.AssumeServerRole()

	// Standalone: permanently binds the flate.Writer to this connection
	deflateClient, _ := websockets.WrapDeflate(baseClient, flate.BestSpeed)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := deflateClient.WriteMessage(websockets.OpCodeText, payload); err != nil {
			b.Fatalf("standalone deflate write failed: %v", err)
		}
	}

	b.StopTimer()
	deflateClient.Close()
	<-sinkDone
}

func BenchmarkWriteMessage_Deflate_Pooled(b *testing.B) {
	payload := []byte(`{"type":"heartbeat","timestamp":1672531200,"status":"ok"}`)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	deflatePool := websockets.NewDeflateConnectionPool(flate.BestSpeed)

	clientConn, serverConn := net.Pipe()
	sinkDone := make(chan struct{})
	startSink(serverConn, sinkDone)

	baseClient := websockets.NewConn(clientConn, 4096, 4096)
	baseClient.AssumeServerRole()

	deflateClient := deflatePool.Acquire(baseClient)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := deflateClient.WriteMessage(websockets.OpCodeText, payload); err != nil {
			b.Fatalf("pooled deflate write failed: %v", err)
		}
	}

	b.StopTimer()
	deflateClient.Close()
	deflatePool.Release(deflateClient)
	<-sinkDone
}

func BenchmarkStreamMessage_Deflate(b *testing.B) {
	chunkSize := (4 * 1024) - 8 // 4 KB fragments - 8 byte header
	payload := makeBenchPayload(benchmarkPayloadSize)

	b.ReportAllocs()
	b.SetBytes(int64(benchmarkPayloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clientConn, serverConn := net.Pipe()
		sinkDone := make(chan struct{})
		startSink(serverConn, sinkDone)

		baseClient := websockets.NewConn(clientConn, 4096, 0)
		baseClient.AssumeServerRole()

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

		deflateClient.Close()
		<-sinkDone
	}
}

func BenchmarkStreamMessage_Deflate_Pooled(b *testing.B) {
	chunkSize := (4 * 1024) - 8 // 4 KB fragments - 8 byte header
	payload := makeBenchPayload(benchmarkPayloadSize)

	b.ReportAllocs()
	b.SetBytes(int64(benchmarkPayloadSize))

	deflatePool := websockets.NewDeflateConnectionPool(flate.BestSpeed)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clientConn, serverConn := net.Pipe()
		sinkDone := make(chan struct{})
		startSink(serverConn, sinkDone)

		baseClient := websockets.NewConn(clientConn, 4096, 0)
		baseClient.AssumeServerRole()

		deflateClient := deflatePool.Acquire(baseClient)
		r := bytes.NewReader(payload)

		b.StartTimer()
		if err := deflateClient.StreamMessage(websockets.OpCodeText, chunkSize, r); err != nil {
			b.Fatalf("pooled deflate stream failed: %v", err)
		}
		b.StopTimer()

		deflateClient.Close()
		deflatePool.Release(deflateClient)
		<-sinkDone
	}
}
