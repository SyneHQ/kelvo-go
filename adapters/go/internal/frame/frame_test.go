package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		ftype   Type
		stream  uint32
		payload []byte
	}{
		{"ping", Ping, 0, nil},
		{"pong", Pong, 0, nil},
		{"close", Close, 0, nil},
		{"cancel", Cancel, 7, nil},
		{"hello", Hello, 0, []byte{Version}},
		{"request", Request, 1, []byte(`{"version":1,"operation_id":"op-1"}`)},
		{"result", Result, 0xFFFFFFFF, []byte("fake-arrow-ipc-bytes")},
		{"receipt", Receipt, 3, []byte(`{"version":1,"operation_id":"op-1","outcome":"completed"}`)},
		{"error", Error, 9, []byte("connection closed by server")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Write(&buf, tc.ftype, tc.stream, tc.payload); err != nil {
				t.Fatalf("Write failed: %v", err)
			}

			f, err := Read(&buf)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}
			if f.Type != tc.ftype || f.Stream != tc.stream {
				t.Fatalf("got type %v stream %d, want %v stream %d", f.Type, f.Stream, tc.ftype, tc.stream)
			}
			if !bytes.Equal(f.Payload, tc.payload) {
				t.Fatalf("payload mismatch: got %q, want %q", f.Payload, tc.payload)
			}
		})
	}
}

func TestFrameOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	// Ping cannot have payload
	if err := Write(&buf, Ping, 0, []byte("forbidden")); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge for Ping with payload, got %v", err)
	}

	// Request > MaxRequestBytes
	oversizedReq := make([]byte, MaxRequestBytes+1)
	if err := Write(&buf, Request, 1, oversizedReq); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge for oversized Request, got %v", err)
	}

	// Receipt > MaxReceiptBytes
	oversizedReceipt := make([]byte, MaxReceiptBytes+1)
	if err := Write(&buf, Receipt, 1, oversizedReceipt); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge for oversized Receipt, got %v", err)
	}
}

func TestFrameReadOversizedHeader(t *testing.T) {
	var buf bytes.Buffer
	header := make([]byte, headerSize)
	header[0] = byte(Request)
	header[4] = 1
	binary.BigEndian.PutUint32(header[5:], uint32(MaxRequestBytes+10))
	buf.Write(header)

	_, err := Read(&buf)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge on oversized header, got %v", err)
	}
}

func TestFrameReadUnknownType(t *testing.T) {
	var buf bytes.Buffer
	header := make([]byte, headerSize)
	header[0] = 99 // unknown
	binary.BigEndian.PutUint32(header[5:], 0)
	buf.Write(header)

	_, err := Read(&buf)
	if !errors.Is(err, ErrUnknownType) {
		t.Fatalf("expected ErrUnknownType, got %v", err)
	}
}

func TestFrameReadTruncated(t *testing.T) {
	// Truncated header
	buf := bytes.NewReader([]byte{byte(Request), 0, 0})
	_, err := Read(buf)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("expected ErrTruncated for short header, got %v", err)
	}

	// Truncated payload
	var buf2 bytes.Buffer
	header := make([]byte, headerSize)
	header[0] = byte(Request)
	header[4] = 1
	binary.BigEndian.PutUint32(header[5:], 10)
	buf2.Write(header)
	buf2.WriteString("12345") // only 5 bytes instead of 10

	_, err = Read(&buf2)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("expected ErrTruncated for short payload, got %v", err)
	}
}

func TestFrameHelpers(t *testing.T) {
	var buf bytes.Buffer
	if err := WritePing(&buf); err != nil {
		t.Fatal(err)
	}
	if err := WritePong(&buf); err != nil {
		t.Fatal(err)
	}
	if err := WriteClose(&buf); err != nil {
		t.Fatal(err)
	}
	if err := WriteError(&buf, 1, "some error"); err != nil {
		t.Fatal(err)
	}
	if err := WriteRequest(&buf, 1, []byte("req")); err != nil {
		t.Fatal(err)
	}
	if err := WriteResult(&buf, 1, []byte("res")); err != nil {
		t.Fatal(err)
	}
	if err := WriteReceipt(&buf, 1, []byte("rec")); err != nil {
		t.Fatal(err)
	}

	expectedTypes := []Type{Ping, Pong, Close, Error, Request, Result, Receipt}
	for _, exp := range expectedTypes {
		f, err := Read(&buf)
		if err != nil {
			t.Fatalf("failed reading %v: %v", exp, err)
		}
		if f.Type != exp {
			t.Fatalf("got type %v, want %v", f.Type, exp)
		}
	}

	// Clean EOF
	_, err := Read(&buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF on empty reader, got %v", err)
	}
}

func TestFrameWriteErrorTruncation(t *testing.T) {
	var buf bytes.Buffer
	longErr := strings.Repeat("e", MaxErrorBytes+100)
	if err := WriteError(&buf, 1, longErr); err != nil {
		t.Fatal(err)
	}
	f, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if f.Type != Error {
		t.Fatalf("got %v, want Error", f.Type)
	}
	if len(f.Payload) != MaxErrorBytes {
		t.Fatalf("expected payload truncated to %d, got %d", MaxErrorBytes, len(f.Payload))
	}
}

func TestFrameBadStream(t *testing.T) {
	var buf bytes.Buffer
	bad := []struct {
		t      Type
		stream uint32
	}{
		{Ping, 1}, {Pong, 1}, {Close, 1}, {Hello, 1},
		{Request, 0}, {Result, 0}, {Receipt, 0}, {Error, 0}, {Cancel, 0},
	}
	for _, tc := range bad {
		payload := []byte(nil)
		if tc.t == Hello {
			payload = []byte{Version}
		}
		if err := Write(&buf, tc.t, tc.stream, payload); !errors.Is(err, ErrBadStream) {
			t.Fatalf("Write type %d stream %d: got %v, want ErrBadStream", tc.t, tc.stream, err)
		}
		if buf.Len() != 0 {
			t.Fatalf("Write type %d wrote bytes despite error", tc.t)
		}
		hdr := make([]byte, headerSize)
		hdr[0] = byte(tc.t)
		binary.BigEndian.PutUint32(hdr[1:], tc.stream)
		if _, err := Read(bytes.NewReader(hdr)); !errors.Is(err, ErrBadStream) {
			t.Fatalf("Read type %d stream %d: got %v, want ErrBadStream", tc.t, tc.stream, err)
		}
	}
}

func TestFrameHelloAndCancel(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteHello(&buf); err != nil {
		t.Fatal(err)
	}
	if err := WriteCancel(&buf, 42); err != nil {
		t.Fatal(err)
	}
	f, err := Read(&buf)
	if err != nil || CheckHello(f) != nil {
		t.Fatalf("hello: %v %v", f, err)
	}
	f, err = Read(&buf)
	if err != nil || f.Type != Cancel || f.Stream != 42 || len(f.Payload) != 0 {
		t.Fatalf("cancel: %+v %v", f, err)
	}
}

func TestCheckHello(t *testing.T) {
	bad := []Frame{
		{Type: Hello, Payload: []byte{Version + 1}},
		{Type: Hello, Payload: nil},
		{Type: Hello, Payload: []byte{Version, Version}},
		{Type: Ping, Payload: []byte{Version}},
	}
	for _, f := range bad {
		if err := CheckHello(f); !errors.Is(err, ErrVersion) {
			t.Fatalf("CheckHello(%+v) = %v, want ErrVersion", f, err)
		}
	}
	if err := CheckHello(Frame{Type: Hello, Payload: []byte{Version}}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncWriterConcurrent(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSyncWriter(&buf)
	var wg sync.WaitGroup
	for i := uint32(1); i <= 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte(i)}, 1000+int(i))
			if err := sw.Write(Result, i, payload); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	seen := map[uint32]bool{}
	for range 50 {
		f, err := Read(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if len(f.Payload) != 1000+int(f.Stream) || !bytes.Equal(f.Payload, bytes.Repeat([]byte{byte(f.Stream)}, len(f.Payload))) {
			t.Fatalf("stream %d payload corrupted", f.Stream)
		}
		seen[f.Stream] = true
	}
	if len(seen) != 50 {
		t.Fatalf("got %d distinct streams, want 50", len(seen))
	}
}
