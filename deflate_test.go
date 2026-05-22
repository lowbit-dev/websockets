package websockets_test

import (
	"bytes"
	"compress/flate"
	"io"
	"net"
	"sync"
	"testing"

	"lowbit.dev/websockets"
)

func TestDeflateWrapper_CompressedAndUncompressed(t *testing.T) {
	client, server := setupPipe(t, 4096)

	// Decorate server connection with our deflate wrapper
	serverDeflate, err := websockets.WrapDeflate(server, flate.BestSpeed)
	if err != nil {
		t.Fatalf("failed to create deflate wrapper: %v", err)
	}

	go func() {
		// Simulate a client that supports deflate
		clientDeflate, _ := websockets.WrapDeflate(websockets.NewConnection(client, 4096, 0), flate.BestSpeed)

		// 1. Send a compressed message
		_ = clientDeflate.WriteMessage(websockets.OpCodeText, []byte("compressed data payload"))

		// 2. Send an uncompressed control/standard frame down the same wire
		// (A standard client wrapper handles uncompressed bypass)
		clientBase := websockets.NewConnection(client, 4096, 0)
		_ = clientBase.WriteMessage(websockets.OpCodeText, []byte("raw uncompressed"))
	}()

	buf := make([]byte, 0, 256)

	// Test Path A: Read Compressed message
	payload, op, err := serverDeflate.ReadMessage(buf)
	if err != nil {
		t.Fatalf("failed to read compressed frame: %v", err)
	}
	if op != websockets.OpCodeText || string(payload) != "compressed data payload" {
		t.Errorf("failed to accurately inflate data, got: %s", string(payload))
	}

	// Test Path B: Read Uncompressed message on same connection
	buf = buf[:0]
	payload, op, err = serverDeflate.ReadMessage(buf)
	if err != nil {
		t.Fatalf("failed to read subsequent uncompressed frame: %v", err)
	}
	if string(payload) != "raw uncompressed" {
		t.Errorf("deflate wrapper broken when processing uncompressed bypass, got: %s", string(payload))
	}
}

func TestDeflateStream_RSVMaskAndContextTakeover(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// High repetition data to guarantee that the compressor leverages its
	// internal sliding window history across chunk iterations.
	repeatingData := bytes.Repeat([]byte("lowbit-streaming-test-string-"), 50) // ~1.5 KB

	clientBase := websockets.NewConnection(clientConn, 4096, 0)
	clientDeflate, err := websockets.WrapDeflate(clientBase, flate.BestSpeed)
	if err != nil {
		t.Fatalf("failed to wrap client deflate: %v", err)
	}

	serverBase := websockets.NewConnection(serverConn, 4096, 0)
	_, err = websockets.WrapDeflate(serverBase, flate.BestSpeed)
	if err != nil {
		t.Fatalf("failed to wrap server deflate: %v", err)
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		r := bytes.NewReader(repeatingData)
		// Force chunk size to 256 bytes to guarantee a multi-fragment sequence
		if err := clientDeflate.StreamMessage(websockets.OpCodeText, 256, r); err != nil {
			t.Errorf("deflate streaming failed: %v", err)
		}
	}()

	// Intercept the wire directly using the server's base connection to analyze frame headers
	frameCount := 0
	for {
		header, err := serverBase.ReadHeader()
		if err != nil {
			t.Fatalf("failed to read frame header: %v", err)
		}

		frameCount++

		// Validate RFC 7692 RSV1 Invariants
		if frameCount == 1 {
			// First frame must set RSV1
			if (header.RSV & 0x40) == 0 {
				t.Errorf("expected RSV1 bit to be set on message initiator frame")
			}
			if header.Op != websockets.OpCodeText {
				t.Errorf("expected frame 1 to carry OpCodeText, got %d", header.Op)
			}
		} else {
			// All subsequent fragments MUST clear RSV1
			if (header.RSV & 0x04) != 0 {
				t.Errorf("protocol violation: RSV1 bit detected on continuation frame %d", frameCount)
			}
			if header.Op != websockets.OpCodeContinuation {
				t.Errorf("expected frame %d to carry OpCodeContinuation, got %d", frameCount, header.Op)
			}
		}

		// Discard the payload bytes to move the stream forward
		_, _ = io.ReadFull(serverConn, make([]byte, header.PayloadLen))

		if header.IsFinal {
			break // Clean stream termination detected
		}
	}

	if frameCount <= 1 {
		t.Errorf("expected data to be fragmented across multiple frames, only processed %d", frameCount)
	}

	wg.Wait()
}

func TestDeflateStream_EndToEndReadVerification(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	clientBase := websockets.NewConnection(clientConn, 4096, 0)
	clientDeflate, _ := websockets.WrapDeflate(clientBase, flate.BestSpeed)

	serverBase := websockets.NewConnection(serverConn, 4096, 0)
	serverDeflate, _ := websockets.WrapDeflate(serverBase, flate.BestSpeed)

	originPayload := []byte("asserting that chunked compression inflates cleanly back to normal text")

	go func() {
		// Run a background reader loop on the client side to safely drain the server's
		// unbuffered pipe reads/acks without deadlocking.
		go func() {
			var discard [128]byte
			for {
				if _, err := clientConn.Read(discard[:]); err != nil {
					return
				}
			}
		}()

		r := bytes.NewReader(originPayload)
		_ = clientDeflate.StreamMessage(websockets.OpCodeText, 10, r) // Tiny 10-byte chunks
	}()

	// Verify that our reader-side patch inflates the streamed chunks flawlessly
	readBuf := make([]byte, 0, 256)
	resultPayload, op, err := serverDeflate.ReadMessage(readBuf)
	if err != nil {
		t.Fatalf("failed to read and inflate compressed stream: %v", err)
	}

	if op != websockets.OpCodeText {
		t.Errorf("expected OpCodeText, got %d", op)
	}
	if !bytes.Equal(resultPayload, originPayload) {
		t.Errorf("data corruption detected!\nexp: %q\ngot: %q", string(originPayload), string(resultPayload))
	}
}
