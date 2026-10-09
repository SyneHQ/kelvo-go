package relational

import (
	"encoding/binary"
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
