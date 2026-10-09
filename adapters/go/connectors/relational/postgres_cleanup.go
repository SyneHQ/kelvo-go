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

// The frontend reads plaintext after the source TLS handshake. Keep only
// bounded ErrorResponse fields; row values are neither copied nor retained.
type postgresCleanupReader struct {
	reader io.Reader
	state  *postgresCleanupState
}

func (r *postgresCleanupReader) Read(p []byte) (int, error) {
	// Do not let pgproto3 read ahead across a frame boundary. It must
	// decode each preceding frame before this observer can see ReadyForQuery.
	// Apply this from connection startup, so cancellation cannot inherit
	// undecoded frames that were buffered before the watcher fired.
	r.state.mu.Lock()
	limit := len(p)
	if !r.state.failed {
		if r.state.headerN < 5 {
			limit = min(limit, 5-r.state.headerN)
		} else {
			limit = min(limit, int(r.state.remaining))
		}
	}
	r.state.mu.Unlock()
	n, err := r.reader.Read(p[:limit])
	r.state.consume(p[:n], err)
	return n, err
}
func (s *postgresCleanupState) consume(p []byte, readErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
