// Package frame implements length-prefixed binary framing for a long-lived
// adapter worker. Frame types carry the same ProcessRequest/receipt JSON used
// by the one-shot path, wrapped in a size-bounded envelope so a single
// reader/writer pair can multiplex request–result–receipt sequences.
//
// Wire format (v2): [type:1][stream:4 big-endian][length:4 big-endian][payload:length]
//
// Connection-level frames (Ping, Pong, Close, Hello) use stream 0; per-request
// frames (Request, Result, Receipt, Error, Cancel) use a non-zero stream.
//
// Payload sizes are bounded per type. A reader that encounters an unknown
// type, an oversized frame, a truncated read, or a malformed payload must
// return an error and the caller must terminate the worker.
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Version is the protocol version carried by Hello.
const Version byte = 2

// Type identifies the purpose of a frame on the wire.
type Type byte

const (
	// Request carries a JSON-encoded ProcessRequest (parent → worker).
	Request Type = 1
	// Result carries a chunk of Arrow IPC bytes (worker → parent).
	Result Type = 2
	// Receipt carries a JSON-encoded operations.Receipt (worker → parent).
	Receipt Type = 3
	// Error carries a UTF-8 error description (worker → parent).
	Error Type = 4
	// Ping is a keep-alive (parent → worker). Payload is empty.
	Ping Type = 5
	// Pong is a keep-alive response (worker → parent). Payload is empty.
	Pong Type = 6
	// Close signals graceful shutdown (parent → worker). Payload is empty.
	Close Type = 7
	// Cancel aborts one stream (parent → worker). Payload is empty.
	Cancel Type = 8
	// Hello carries the protocol version byte. Stream 0.
	Hello Type = 9
)

const (
	// headerSize is the fixed overhead: 1 byte type + 4 bytes stream + 4 bytes length.
	headerSize = 9

	// MaxRequestBytes bounds Request frame payloads. Matches
	// adapter.MaxProcessRequestBytes (3 MiB) with headroom for framing.
	MaxRequestBytes = 3 << 20

	// MaxResultBytes bounds a single Result frame. Arrow IPC chunks are
	// typically small; 16 MiB allows generous batches while preventing a
	// runaway writer from exhausting memory.
	MaxResultBytes = 16 << 20

	// MaxReceiptBytes bounds Receipt frame payloads.
	MaxReceiptBytes = 64 << 10

	// MaxErrorBytes bounds Error frame payloads.
	MaxErrorBytes = 4 << 10

	// MaxPingBytes, MaxPongBytes, MaxCloseBytes are zero: no payload.
	MaxPingBytes  = 0
	MaxPongBytes  = 0
	MaxCloseBytes = 0

	// MaxCancelBytes is zero; MaxHelloBytes is the single version byte.
	MaxCancelBytes = 0
	MaxHelloBytes  = 1
)

var (
	ErrFrameTooLarge = errors.New("frame: payload exceeds type limit")
	ErrUnknownType   = errors.New("frame: unknown frame type")
	ErrTruncated     = errors.New("frame: truncated read")
	ErrInvalidHeader = errors.New("frame: invalid header")
	ErrBadStream     = errors.New("frame: stream id invalid for frame type")
	ErrVersion       = errors.New("frame: protocol version mismatch")
)

// maxPayload returns the maximum allowed payload for a given type.
func maxPayload(t Type) (int, error) {
	switch t {
	case Request:
		return MaxRequestBytes, nil
	case Result:
		return MaxResultBytes, nil
	case Receipt:
		return MaxReceiptBytes, nil
	case Error:
		return MaxErrorBytes, nil
	case Ping:
		return MaxPingBytes, nil
	case Pong:
		return MaxPongBytes, nil
	case Close:
		return MaxCloseBytes, nil
	case Cancel:
		return MaxCancelBytes, nil
	case Hello:
		return MaxHelloBytes, nil
	default:
		return 0, ErrUnknownType
	}
}

// checkStream enforces the stream rule: connection-level types use stream 0,
// all others a non-zero stream.
func checkStream(t Type, stream uint32) error {
	conn := t == Ping || t == Pong || t == Close || t == Hello
	if conn != (stream == 0) {
		return ErrBadStream
	}
	return nil
}

// Frame is one decoded frame.
type Frame struct {
	Type    Type
	Stream  uint32
	Payload []byte
}

// Write writes a single frame to w. It returns an error if the type is
// unknown, the stream violates the type's rule, or the payload exceeds the
// type's limit.
func Write(w io.Writer, t Type, stream uint32, payload []byte) error {
	limit, err := maxPayload(t)
	if err != nil {
		return err
	}
	if err := checkStream(t, stream); err != nil {
		return err
	}
	if len(payload) > limit {
		return ErrFrameTooLarge
	}
	var header [headerSize]byte
	header[0] = byte(t)
	binary.BigEndian.PutUint32(header[1:], stream)
	binary.BigEndian.PutUint32(header[5:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a single frame from r. It returns an error on unknown types,
// bad stream ids, oversized frames, or truncated reads. An io.EOF from the
// header read is returned as-is to signal a clean stream close.
func Read(r io.Reader) (Frame, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, ErrTruncated
		}
		return Frame{}, err // io.EOF propagated cleanly
	}
	t := Type(header[0])
	stream := binary.BigEndian.Uint32(header[1:])
	length := binary.BigEndian.Uint32(header[5:])
	limit, err := maxPayload(t)
	if err != nil {
		return Frame{}, err
	}
	if err := checkStream(t, stream); err != nil {
		return Frame{}, err
	}
	if uint64(length) > uint64(limit) {
		return Frame{}, fmt.Errorf("%w: type %d, length %d, limit %d", ErrFrameTooLarge, t, length, limit)
	}
	if length == 0 {
		return Frame{Type: t, Stream: stream}, nil
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return Frame{}, ErrTruncated
		}
		return Frame{}, err
	}
	return Frame{Type: t, Stream: stream, Payload: payload}, nil
}

// WriteRequest is a convenience for writing a Request frame.
func WriteRequest(w io.Writer, stream uint32, payload []byte) error {
	return Write(w, Request, stream, payload)
}

// WriteResult is a convenience for writing a Result frame.
func WriteResult(w io.Writer, stream uint32, payload []byte) error {
	return Write(w, Result, stream, payload)
}

// WriteReceipt is a convenience for writing a Receipt frame.
func WriteReceipt(w io.Writer, stream uint32, payload []byte) error {
	return Write(w, Receipt, stream, payload)
}

// WriteError is a convenience for writing an Error frame with a message.
func WriteError(w io.Writer, stream uint32, msg string) error {
	b := []byte(msg)
	if len(b) > MaxErrorBytes {
		b = b[:MaxErrorBytes]
	}
	return Write(w, Error, stream, b)
}

// WriteCancel writes a Cancel frame for stream.
func WriteCancel(w io.Writer, stream uint32) error { return Write(w, Cancel, stream, nil) }

// WritePing writes a Ping frame.
func WritePing(w io.Writer) error { return Write(w, Ping, 0, nil) }

// WritePong writes a Pong frame.
func WritePong(w io.Writer) error { return Write(w, Pong, 0, nil) }

// WriteClose writes a Close frame.
func WriteClose(w io.Writer) error { return Write(w, Close, 0, nil) }

// WriteHello writes a Hello frame carrying Version.
func WriteHello(w io.Writer) error { return Write(w, Hello, 0, []byte{Version}) }

// CheckHello returns nil iff f is a Hello frame carrying exactly Version.
func CheckHello(f Frame) error {
	if f.Type != Hello || len(f.Payload) != 1 || f.Payload[0] != Version {
		return ErrVersion
	}
	return nil
}

// SyncWriter serializes whole frames onto one writer so concurrent
// goroutines never interleave bytes.
type SyncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewSyncWriter wraps w.
func NewSyncWriter(w io.Writer) *SyncWriter { return &SyncWriter{w: w} }

// Write writes one frame (header and payload) under the lock.
func (s *SyncWriter) Write(t Type, stream uint32, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Write(s.w, t, stream, payload)
}
