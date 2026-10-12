package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		ftype   Type
		payload []byte
	}{
		{"ping", Ping, nil},
		{"pong", Pong, nil},
		{"close", Close, nil},
		{"request", Request, []byte(`{"version":1,"operation_id":"op-1"}`)},
		{"result", Result, []byte("fake-arrow-ipc-bytes")},
		{"receipt", Receipt, []byte(`{"version":1,"operation_id":"op-1","outcome":"completed"}`)},
		{"error", Error, []byte("connection closed by server")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Write(&buf, tc.ftype, tc.payload); err != nil {
				t.Fatalf("Write failed: %v", err)
			}

			gotType, gotPayload, err := Read(&buf)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}
			if gotType != tc.ftype {
				t.Fatalf("type mismatch: got %v, want %v", gotType, tc.ftype)
			}
			if !bytes.Equal(gotPayload, tc.payload) {
				t.Fatalf("payload mismatch: got %q, want %q", gotPayload, tc.payload)
			}
		})
	}
}

func TestFrameOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, Ping, []byte("forbidden")); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge for Ping with payload, got %v", err)
	}

	oversizedReq := make([]byte, MaxRequestBytes+1)
	if err := Write(&buf, Request, oversizedReq); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge for oversized Request, got %v", err)
	}

	oversizedReceipt := make([]byte, MaxReceiptBytes+1)
	if err := Write(&buf, Receipt, oversizedReceipt); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge for oversized Receipt, got %v", err)
	}
}

func TestFrameReadOversizedHeader(t *testing.T) {
	var buf bytes.Buffer
	header := make([]byte, 5)
	header[0] = byte(Request)
	binary.BigEndian.PutUint32(header[1:], uint32(MaxRequestBytes+10))
	buf.Write(header)

	_, _, err := Read(&buf)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge on oversized header, got %v", err)
	}
}

func TestFrameReadUnknownType(t *testing.T) {
	var buf bytes.Buffer
	header := make([]byte, 5)
	header[0] = 99 // unknown
	binary.BigEndian.PutUint32(header[1:], 0)
	buf.Write(header)

	_, _, err := Read(&buf)
	if !errors.Is(err, ErrUnknownType) {
		t.Fatalf("expected ErrUnknownType, got %v", err)
	}
}

func TestFrameReadTruncated(t *testing.T) {
	buf := bytes.NewReader([]byte{byte(Request), 0, 0})
	_, _, err := Read(buf)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("expected ErrTruncated for short header, got %v", err)
	}

	var buf2 bytes.Buffer
	header := make([]byte, 5)
	header[0] = byte(Request)
	binary.BigEndian.PutUint32(header[1:], 10)
	buf2.Write(header)
	buf2.WriteString("12345")

	_, _, err = Read(&buf2)
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
	if err := WriteError(&buf, "some error"); err != nil {
		t.Fatal(err)
	}
	if err := WriteRequest(&buf, []byte("req")); err != nil {
		t.Fatal(err)
	}
	if err := WriteResult(&buf, []byte("res")); err != nil {
		t.Fatal(err)
	}
	if err := WriteReceipt(&buf, []byte("rec")); err != nil {
		t.Fatal(err)
	}

	expectedTypes := []Type{Ping, Pong, Close, Error, Request, Result, Receipt}
	for _, exp := range expectedTypes {
		typ, _, err := Read(&buf)
		if err != nil {
			t.Fatalf("failed reading %v: %v", exp, err)
		}
		if typ != exp {
			t.Fatalf("got type %v, want %v", typ, exp)
		}
	}

	_, _, err := Read(&buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF on empty reader, got %v", err)
	}
}

func TestFrameWriteErrorTruncation(t *testing.T) {
	var buf bytes.Buffer
	longErr := strings.Repeat("e", MaxErrorBytes+100)
	if err := WriteError(&buf, longErr); err != nil {
		t.Fatal(err)
	}
	typ, payload, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if typ != Error {
		t.Fatalf("got %v, want Error", typ)
	}
	if len(payload) != MaxErrorBytes {
		t.Fatalf("expected payload truncated to %d, got %d", MaxErrorBytes, len(payload))
	}
}
