package relational

import (
	"bytes"
	"encoding/binary"
	"github.com/jackc/pgx/v5/pgproto3"
	"io"
	"testing"
)

func pgFrame(kind byte, body []byte) []byte {
	out := make([]byte, 5+len(body))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:5], uint32(4+len(body)))
	copy(out[5:], body)
	return out
}
func TestCleanupObserverRequiresCancellationAndReady(t *testing.T) {
	cancelled := pgFrame('E', []byte("SERROR\x00C57014\x00Mcancelled\x00\x00"))
	ready := pgFrame('Z', []byte{'I'})
	for _, test := range []struct {
		name   string
		frames []byte
		err    error
		want   bool
	}{
		{"complete", append(append([]byte(nil), cancelled...), ready...), nil, true},
		{"only-error", cancelled, nil, false}, {"only-ready", ready, nil, false},
		{"other-error", append(pgFrame('E', []byte("C42501\x00\x00")), ready...), nil, false},
		{"transport-eof", append(append([]byte(nil), cancelled...), ready...), io.EOF, false},
		{"bad-error", append(pgFrame('E', []byte("C57014\x00")), ready...), nil, false},
		{"invalid-ready", append(append([]byte(nil), cancelled...), pgFrame('Z', []byte{'X'})...), nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			for chunk := 1; chunk <= len(test.frames); chunk++ {
				s := &postgresCleanupState{active: true}
				for offset := 0; offset < len(test.frames); {
					end := offset + chunk
					if end > len(test.frames) {
						end = len(test.frames)
					}
					s.consume(test.frames[offset:end], nil)
					offset = end
				}
				s.consume(nil, test.err)
				if got := s.ready && !s.failed; got != test.want {
					t.Fatalf("chunk %d confirmation %v", chunk, got)
				}
			}
		})
	}
}
func TestCleanupObserverBoundsAndPreCancelMessages(t *testing.T) {
	s := &postgresCleanupState{}
	s.consume(pgFrame('E', []byte("C57014\x00\x00")), nil)
	s.active = true
	s.consume(pgFrame('Z', []byte{'I'}), nil)
	if s.ready {
		t.Fatal("used pre-cancel error")
	}
	s = &postgresCleanupState{active: true}
	s.consume(pgFrame('D', make([]byte, 256<<10)), nil)
	if !s.failed {
		t.Fatal("accepted oversized cleanup read")
	}
	s = &postgresCleanupState{active: true}
	s.consume(pgFrame('E', make([]byte, 8193))[:5], nil)
	if !s.failed {
		t.Fatal("accepted oversized error frame")
	}
}

func TestCleanupObserverDoesNotReadPastDecoderFailure(t *testing.T) {
	cancelled := pgFrame('E', []byte("C57014\x00\x00"))
	ready := pgFrame('Z', []byte{'I'})
	for _, tc := range []struct {
		name string
		data []byte
		want bool
	}{
		{"valid", append(append([]byte{}, cancelled...), ready...), true},
		{"unknown-before", append(append(pgFrame('?', nil), cancelled...), ready...), false},
		{"bad-row-before", append(append(pgFrame('D', []byte{'x'}), cancelled...), ready...), false},
		{"bad-row-between", append(append(append([]byte{}, cancelled...), pgFrame('D', []byte{'x'})...), ready...), false},
		{"unknown-between", append(append(append([]byte{}, cancelled...), pgFrame('?', nil)...), ready...), false},
		{"duplicate-code", append(pgFrame('E', []byte("C57014\x00C42501\x00\x00")), ready...), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &postgresCleanupState{active: true}
			frontend := pgproto3.NewFrontend(&postgresCleanupReader{reader: bytes.NewReader(tc.data), state: s}, io.Discard)
			for {
				m, err := frontend.Receive()
				if err != nil {
					break
				}
				if _, ok := m.(*pgproto3.ReadyForQuery); ok {
					break
				}
			}
			if got := s.ready && !s.failed; got != tc.want {
				t.Fatalf("confirmation %v; want %v", got, tc.want)
			}
		})
	}
}

func TestCleanupObserverActivationCannotReusePartialError(t *testing.T) {
	cancelled := pgFrame('E', []byte("SERROR\x00C57014\x00Mcancelled\x00\x00"))
	ready := pgFrame('Z', []byte{'I'})
	for offset := 1; offset <= len(cancelled); offset++ {
		s := &postgresCleanupState{}
		s.consume(cancelled[:offset], nil)
		s.active = true
		for _, b := range append(append([]byte{}, cancelled[offset:]...), ready...) {
			s.consume([]byte{b}, nil)
		}
		if s.ready && !s.failed {
			t.Fatalf("accepted pre-cancellation frame at offset %d", offset)
		}
	}
}
