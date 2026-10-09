package relational

import (
	"bytes"
	"github.com/jackc/pgx/v5/pgproto3"
	"io"
	"testing"
)

// Activates only after the observer classified and returned its first read,
// but before Frontend can inspect any returned bytes.
type activateAfterObservedRead struct {
	r          io.Reader
	state      *postgresCleanupState
	firstLimit int
	once       bool
}

func (r *activateAfterObservedRead) Read(p []byte) (int, error) {
	if !r.once && r.firstLimit > 0 && len(p) > r.firstLimit {
		p = p[:r.firstLimit]
	}
	n, err := r.r.Read(p)
	if !r.once {
		r.once = true
		r.state.mu.Lock()
		r.state.active = true
		r.state.mu.Unlock()
	}
	return n, err
}
func TestCleanupObserverPrebufferedEvidenceCannotBecomeActive(t *testing.T) {
	row := pgFrame('D', []byte{0, 0})
	cancelled := pgFrame('E', []byte("C57014\x00\x00"))
	ready := pgFrame('Z', []byte{'I'})
	for _, tc := range []struct {
		name  string
		data  []byte
		first int
		want  bool
	}{
		{"complete_inactive_evidence", append(append(append([]byte{}, row...), cancelled...), ready...), 0, false},
		{"unknown_inactive_prefix", append(append(pgFrame('?', nil), cancelled...), ready...), 0, false},
		{"active_after_row", append(append(append([]byte{}, row...), cancelled...), ready...), len(row), true},
		{"active_mid_error", append(append(append([]byte{}, row...), cancelled...), ready...), len(row) + 7, false},
		{"fresh_error_after_partial_old_error", append(append(append(append([]byte{}, row...), cancelled...), cancelled...), ready...), len(row) + 7, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &postgresCleanupState{}
			r := &postgresCleanupReader{reader: bytes.NewReader(tc.data), state: s}
			activated := &activateAfterObservedRead{r: r, state: s, firstLimit: tc.first}
			f := pgproto3.NewFrontend(activated, io.Discard)
			for {
				m, e := f.Receive()
				if e != nil {
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
