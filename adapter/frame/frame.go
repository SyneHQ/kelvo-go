// Package frame implements length-prefixed binary framing for a long-lived
// adapter worker. Frame types carry the same ProcessRequest/receipt JSON used
// by the one-shot path, wrapped in a size-bounded envelope so a single
// reader/writer pair can multiplex request–result–receipt sequences.
//
// Wire format: [type:1][length:4 big-endian][payload:length]
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
)

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
)

const (
	// headerSize is the fixed overhead: 1 byte type + 4 bytes length.
	headerSize = 5

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
)

var (
	ErrFrameTooLarge = errors.New("frame: payload exceeds type limit")
	ErrUnknownType   = errors.New("frame: unknown frame type")
	ErrTruncated     = errors.New("frame: truncated read")
	ErrInvalidHeader = errors.New("frame: invalid header")
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
	default:
		return 0, ErrUnknownType
	}
}

// Write writes a single frame to w. It returns an error if the type is
// unknown or the payload exceeds the type's limit.
func Write(w io.Writer, t Type, payload []byte) error {
	limit, err := maxPayload(t)
	if err != nil {
		return err
	}
	if len(payload) > limit {
		return ErrFrameTooLarge
	}
	var header [headerSize]byte
	header[0] = byte(t)
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
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

// Read reads a single frame from r. It returns the type and payload. The
// caller must not retain the payload beyond the next Read call (the buffer
// may be reused). It returns an error on unknown types, oversized frames,
// or truncated reads. An io.EOF from the header read is returned as-is to
// signal a clean stream close.
func Read(r io.Reader) (Type, []byte, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, ErrTruncated
		}
		return 0, nil, err // io.EOF propagated cleanly
	}
	t := Type(header[0])
	length := binary.BigEndian.Uint32(header[1:])
	limit, err := maxPayload(t)
	if err != nil {
		return 0, nil, err
	}
	if int(length) > limit {
		return 0, nil, fmt.Errorf("%w: type %d, length %d, limit %d", ErrFrameTooLarge, t, length, limit)
	}
	if length == 0 {
		return t, nil, nil
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return 0, nil, ErrTruncated
		}
		return 0, nil, err
	}
	return t, payload, nil
}

// WriteRequest is a convenience for writing a Request frame.
func WriteRequest(w io.Writer, payload []byte) error { return Write(w, Request, payload) }

// WriteResult is a convenience for writing a Result frame.
func WriteResult(w io.Writer, payload []byte) error { return Write(w, Result, payload) }

// WriteReceipt is a convenience for writing a Receipt frame.
func WriteReceipt(w io.Writer, payload []byte) error { return Write(w, Receipt, payload) }

// WriteError is a convenience for writing an Error frame with a message.
func WriteError(w io.Writer, msg string) error {
	b := []byte(msg)
	if len(b) > MaxErrorBytes {
		b = b[:MaxErrorBytes]
	}
	return Write(w, Error, b)
}

// WritePing writes a Ping frame.
func WritePing(w io.Writer) error { return Write(w, Ping, nil) }

// WritePong writes a Pong frame.
func WritePong(w io.Writer) error { return Write(w, Pong, nil) }

// WriteClose writes a Close frame.
func WriteClose(w io.Writer) error { return Write(w, Close, nil) }
