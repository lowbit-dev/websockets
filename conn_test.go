package websockets_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"lowbit.dev/websockets"
)

// setupPipe initializes an in-memory network pipe and returns the client socket
// and a configured server-side WebSocket connection.
func setupPipe(t *testing.T, maxLimit int64) (net.Conn, *websockets.Conn) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	serverConn := websockets.NewConn(server, maxLimit, 0)
	serverConn.AssumeServerRole()

	return client, serverConn
}

func TestReadMessage_Unfragmented(t *testing.T) {
	client, server := setupPipe(t, 1024)

	// Use a goroutine to simulate the client writing a single, clean text frame
	go func() {
		// A helper or direct mock write. We can wrap the client in a Conn to write!
		clientWS := websockets.NewConn(client, 1024, 0)
		_ = clientWS.WriteFrame(true, 0, websockets.OpCodeText, []byte("hello lowbit"))
	}()

	buf := make([]byte, 0, 128)
	payload, op, err := server.ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if op != websockets.OpCodeText {
		t.Errorf("expected text opcode, got %d", op)
	}
	if !bytes.Equal(payload, []byte("hello lowbit")) {
		t.Errorf("unexpected payload: %s", string(payload))
	}
}

func TestReadMessage_FragmentedAssembly(t *testing.T) {
	client, server := setupPipe(t, 1024)

	go func() {
		clientWS := websockets.NewConn(client, 1024, 0)
		// Frame 1: Text opcode, FIN = false
		_ = clientWS.WriteFrame(false, 0, websockets.OpCodeText, []byte("part1 "))
		// Frame 2: Continuation opcode, FIN = true
		_ = clientWS.WriteFrame(true, 0, websockets.OpCodeContinuation, []byte("part2"))
	}()

	buf := make([]byte, 0, 128)
	payload, op, err := server.ReadMessage(buf)
	if err != nil {
		t.Fatalf("failed to read fragmented message: %v", err)
	}

	if op != websockets.OpCodeText {
		t.Errorf("expected message to inherit first opcode (Text), got %d", op)
	}
	if string(payload) != "part1 part2" {
		t.Errorf("fragment assembly failed, got: %q", string(payload))
	}
}

func TestReadMessage_InterleavedControlFrame(t *testing.T) {
	client, server := setupPipe(t, 1024)

	pingReceived := false
	server.SetPingHandler(func(payload []byte) error {
		pingReceived = true
		return server.WriteFrame(true, 0, websockets.OpCodePong, payload)
	})

	go func() {
		// FIX: Concurrently drain the client's inbound reader.
		// This absorbs the server's Pong frame, preventing the unbuffered net.Pipe from deadlocking.
		go func() {
			var discard [128]byte
			for {
				if _, err := client.Read(discard[:]); err != nil {
					return
				}
			}
		}()

		clientWS := websockets.NewConn(client, 1024, 0)
		// 1. Send first data fragment (FIN=false)
		_ = clientWS.WriteFrame(false, 0, websockets.OpCodeText, []byte("hello "))
		// 2. Interleave a Ping frame right in the middle of the message
		_ = clientWS.WriteFrame(true, 0, websockets.OpCodePing, []byte("heartbeat"))
		// 3. Send final data fragment (FIN=true)
		_ = clientWS.WriteFrame(true, 0, websockets.OpCodeContinuation, []byte("world"))
	}()

	buf := make([]byte, 0, 128)
	payload, _, err := server.ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected read failure: %v", err)
	}

	if !pingReceived {
		t.Error("expected ping handler to intercept and execute synchronously mid-stream")
	}

	if string(payload) != "hello world" {
		t.Errorf("interleaved frame corrupted message assembly, got: %q", string(payload))
	}
}

func TestReadMessage_OOMPreventionLimit(t *testing.T) {
	// Setup a strict safety limit of 20 bytes
	client, server := setupPipe(t, 20)

	go func() {
		clientWS := websockets.NewConn(client, 1024, 0)
		// Malicious client attempts to send a 25-byte frame
		_ = clientWS.WriteFrame(true, 0, websockets.OpCodeText, make([]byte, 25))
	}()

	buf := make([]byte, 0, 64)
	_, _, err := server.ReadMessage(buf)
	if err == nil {
		t.Fatal("expected error when frame exceeds maxReadLimit, but got nil")
	}

	if !errors.Is(err, websockets.ErrMessageTooBig) {
		t.Errorf("expected ErrMessageTooBig, got: %v", err)
	}
}

func TestStreamMessageExt_MultiChunk(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := websockets.NewConn(clientConn, 1024, 0)
	client.AssumeClientRole()

	server := websockets.NewConn(serverConn, 1024, 0)
	server.AssumeServerRole() // Server acts as a server. Good.

	rawData := []byte("ABC")
	chunkSize := 2

	var wg sync.WaitGroup
	var clientErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		r := bytes.NewReader(rawData)
		clientErr = client.StreamMessageExt(websockets.OpCodeText, 0x40, chunkSize, r)
	}()

	// Frame 1: Instead of io.ReadFull, let's use the engine's framing reader
	// Assuming your FrameReaderWriter interface exposes ReadFrame:
	p1 := make([]byte, 2)
	frame1, err := server.ReadFrame(p1)
	if err != nil {
		t.Fatalf("failed to read frame 1: %v", err)
	}

	if frame1.IsFinal || frame1.Op != websockets.OpCodeText || frame1.RSV != 0x40 {
		t.Fatalf("Frame 1 invalid: %+v", frame1)
	}
	if !bytes.Equal(p1, []byte("AB")) {
		t.Fatalf("Frame 1 payload invalid, expected 'AB', got: %q", p1)
	}

	// Frame 2:
	p2 := make([]byte, 1)
	frame2, err := server.ReadFrame(p2)
	if err != nil {
		t.Fatalf("failed to read frame 2: %v", err)
	}
	if !frame2.IsFinal || frame2.Op != websockets.OpCodeContinuation {
		t.Fatalf("Frame 2 invalid: %+v", frame2)
	}
	if !bytes.Equal(p2, []byte("C")) {
		t.Fatalf("Frame 2 payload invalid, expected 'C', got: %q", p2)
	}

	wg.Wait()

	if clientErr != nil {
		t.Fatalf("StreamMessageExt background writer failed: %v", clientErr)
	}
}

func TestStreamMessageExt_ExactBoundaryAlignment(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := websockets.NewConn(clientConn, 1024, 0)
	client.AssumeClientRole()

	server := websockets.NewConn(serverConn, 1024, 0)
	server.AssumeServerRole()

	rawData := []byte("1234")
	chunkSize := 2

	var wg sync.WaitGroup
	var clientErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		r := bytes.NewReader(rawData)
		clientErr = client.StreamMessageExt(websockets.OpCodeBinary, 0, chunkSize, r)
	}()

	// Frame 1: Contains "12" (IsFinal=false, RSV=0, Op=OpCodeBinary)
	h1, err := server.ReadHeader()
	if err != nil {
		t.Fatalf("failed to read header 1: %v", err)
	}
	if h1.IsFinal || h1.Op != websockets.OpCodeBinary || h1.PayloadLen != 2 {
		t.Fatalf("Frame 1 out of alignment: %+v", h1)
	}
	_, _ = io.ReadFull(serverConn, make([]byte, h1.PayloadLen))

	// Frame 2: Contains "34" (IsFinal=true, RSV=0, Op=OpCodeContinuation)
	// Even on perfect boundaries, lookahead cuts out the empty frame!
	h2, err := server.ReadHeader()
	if err != nil {
		t.Fatalf("failed to read header 2: %v", err)
	}
	if !h2.IsFinal || h2.Op != websockets.OpCodeContinuation || h2.PayloadLen != 2 {
		t.Fatalf("Frame 2 out of alignment: %+v", h2)
	}
	_, _ = io.ReadFull(serverConn, make([]byte, h2.PayloadLen))

	wg.Wait()

	if clientErr != nil {
		t.Fatalf("StreamMessageExt background writer failed: %v", clientErr)
	}
}
