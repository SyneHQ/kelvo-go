package relational

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgproto3"
)

func configureTypedPostgresCleanup(config *pgx.ConnConfig, cleanup adapter.PostgresCleanup) {
	state := &postgresCleanupState{cleanup: cleanup}
	config.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		return pgproto3.NewFrontend(&postgresCleanupReader{reader: r, state: state}, w)
	}
	config.AfterConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
		key := append([]byte(nil), conn.SecretKey()...)
		defer clear(key)
		handle, err := cleanup.Register(ctx, conn.PID(), key)
		if err != nil || handle == "" {
			return adapter.ErrInvalid
		}
		state.mu.Lock()
		state.handle = handle
		state.mu.Unlock()
		return nil
	}
	config.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &typedPostgresWatcher{conn: conn, state: state}
	}
}

type postgresCleanupState struct {
	mu                                 sync.Mutex
	cleanup                            adapter.PostgresCleanup
	handle                             string
	active, failed, cancelError, ready bool
	bytes                              int
	header                             [5]byte
	headerN                            int
	frameActive                        bool
	remaining                          uint32
	kind                               byte
	errorBody                          []byte
}

// The frontend reads plaintext after the source TLS handshake. Inactive reads
// retain no row values. A read that overlaps cancellation can retain at most
// postgresCleanupReadLimit bytes until the frontend consumes them in order.
const postgresCleanupReadLimit = 32 << 10

type postgresCleanupReader struct {
	reader     io.Reader
	state      *postgresCleanupState
	pending    []byte
	pendingErr error
}

func (s *postgresCleanupState) readLimit(size int) int {
	if s.failed || !s.active {
		return size
	}
	if s.headerN < 5 {
		return min(size, 5-s.headerN)
	}
	return min(size, int(s.remaining))
}

func (r *postgresCleanupReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s := r.state
	s.mu.Lock()
	if len(r.pending) > 0 {
		n := s.readLimit(min(len(p), len(r.pending)))
		copy(p, r.pending[:n])
		clear(r.pending[:n])
		r.pending = r.pending[n:]
		var err error
		if len(r.pending) == 0 {
			r.pending = nil
			err, r.pendingErr = r.pendingErr, nil
		}
		s.consumeLocked(p[:n], err)
		s.mu.Unlock()
		return n, err
	}
	limit := s.readLimit(min(len(p), postgresCleanupReadLimit))
	s.mu.Unlock()
	n, err := r.reader.Read(p[:limit])
	s.mu.Lock()
	defer s.mu.Unlock()
	// Cancellation can start while Read blocks. Recheck under the same lock
	// used by the watcher. Deliver only this frame; pgproto3 must decode it
	// before the observer can accept evidence from a later frame.
	deliver := s.readLimit(n)
	if deliver < n {
		r.pending = append([]byte(nil), p[deliver:n]...)
		r.pendingErr = err
		if err != nil {
			s.failed = true
		}
		err = nil
	}
	// Inactive bytes are classified before releasing the lock. If the
	// watcher starts afterward, buffered frontend bytes cannot become evidence.
	s.consumeLocked(p[:deliver], err)
	return deliver, err
}
func (s *postgresCleanupState) consume(p []byte, readErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumeLocked(p, readErr)
}
func (s *postgresCleanupState) consumeLocked(p []byte, readErr error) {
	if s.failed {
		return
	}
	if s.active {
		s.bytes += len(p)
		if s.bytes > 256<<10 {
			s.failed = true
			return
		}
	}
	for len(p) > 0 {
		if s.headerN < 5 {
			if s.headerN == 0 {
				s.frameActive = s.active
			}
			n := copy(s.header[s.headerN:], p)
			s.headerN += n
			p = p[n:]
			if s.headerN < 5 {
				break
			}
			length := binary.BigEndian.Uint32(s.header[1:])
			if length < 4 {
				s.failed = true
				return
			}
			s.kind = s.header[0]
			s.remaining = length - 4
			if s.active && s.frameActive && s.kind == 'E' && s.remaining > 8192 {
				s.failed = true
				return
			}
		}
		if len(p) == 0 && s.remaining > 0 {
			break
		}
		n := len(p)
		if uint32(n) > s.remaining {
			n = int(s.remaining)
		}
		if s.active && s.frameActive && s.kind == 'E' {
			s.errorBody = append(s.errorBody, p[:n]...)
		}
		if s.active && s.frameActive && s.kind == 'Z' && (s.remaining != 1 || n != 1 || (p[0] != 'I' && p[0] != 'T' && p[0] != 'E')) {
			s.failed = true
			return
		}
		s.remaining -= uint32(n)
		p = p[n:]
		if s.remaining > 0 {
			break
		}
		if s.active && s.frameActive {
			if s.kind == 'E' {
				fields := s.errorBody
				valid := false
				seenCode := false
				codeCancelled := false
				for len(fields) > 0 {
					tag := fields[0]
					fields = fields[1:]
					if tag == 0 {
						valid = len(fields) == 0
						break
					}
					end := bytes.IndexByte(fields, 0)
					if end < 0 {
						break
					}
					if tag == 'C' {
						if seenCode {
							s.failed = true
							return
						}
						seenCode = true
						codeCancelled = bytes.Equal(fields[:end], []byte("57014"))
					}
					fields = fields[end+1:]
				}
				if !valid {
					s.failed = true
					return
				}
				s.cancelError = codeCancelled
			}
			if s.kind == 'Z' && s.cancelError {
				s.ready = true
			}
		}
		clear(s.errorBody)
		s.errorBody = nil
		s.headerN = 0
	}
	if readErr != nil && s.active {
		s.failed = true
	}
}

type typedPostgresWatcher struct {
	conn  *pgconn.PgConn
	state *postgresCleanupState
	done  chan struct{}
}

func (h *typedPostgresWatcher) HandleCancel(context.Context) {
	h.state.mu.Lock()
	h.state.active = true
	handle := h.state.handle
	h.state.mu.Unlock()
	h.done = make(chan struct{})
	// Keep the original connection readable while the parent delivers its
	// bounded cancellation. Closing it early loses termination evidence.
	h.conn.Conn().SetDeadline(time.Now().Add(5 * time.Second))
	go func() {
		defer close(h.done)
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if handle != "" {
			h.state.cleanup.Abort(ctx, handle)
		}
	}()
}
func (h *typedPostgresWatcher) HandleUnwatchAfterCancel() {
	<-h.done
	h.state.mu.Lock()
	observed := h.state.ready && !h.state.failed
	handle := h.state.handle
	h.state.mu.Unlock()
	if handle != "" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		h.state.cleanup.Observed(ctx, handle, observed)
		cancel()
	}
	h.conn.Conn().SetDeadline(time.Time{})
}
