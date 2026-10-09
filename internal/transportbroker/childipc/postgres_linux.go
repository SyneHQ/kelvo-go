//go:build linux

package childipc

import (
	"context"
	"encoding/binary"
	"os"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"golang.org/x/sys/unix"
)

// NewPairWithPostgresCleanup permits one data open and one target registration.
// It never gives the child an auxiliary source descriptor.
func NewPairWithPostgresCleanup(ctx context.Context, authority string, data transportbroker.Dialer, cleanup adapter.PostgresCleanup) (*Server, *os.File, error) {
	if cleanup == nil {
		return nil, nil, ErrChannel
	}
	s, f, err := NewPair(ctx, authority, data, nil, 1)
	if err == nil {
		s.postgresCleanup = cleanup
	}
	return s, f, err
}

func validCleanupHandle(handle string) bool {
	if len(handle) < 1 || len(handle) > 128 {
		return false
	}
	for _, c := range []byte(handle) {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

func (s *Server) servePostgresCleanup(request []byte) error {
	if s.postgresCleanup == nil || !s.dataOpened {
		return ErrChannel
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	response := []byte{1, 0}
	var err error
	switch request[1] {
	case 3:
		if s.registered || len(request) < 10 || len(request) > 262 {
			return ErrChannel
		}
		pid := binary.BigEndian.Uint32(request[2:6])
		if pid == 0 {
			return ErrChannel
		}
		s.registered = true
		key := append([]byte(nil), request[6:]...)
		handle, registerErr := s.postgresCleanup.Register(ctx, pid, key)
		clear(key)
		err = registerErr
		if err == nil && !validCleanupHandle(handle) {
			return ErrChannel
		}
		if err == nil {
			s.cleanupHandle = handle
			response = append(response, handle...)
		}
	case 4, 5:
		offset := 2
		observed := false
		if request[1] == 5 {
			if len(request) < 4 || request[2] > 1 {
				return ErrChannel
			}
			observed = request[2] == 1
			offset = 3
		}
		handle := string(request[offset:])
		if !s.registered || !validCleanupHandle(handle) || handle != s.cleanupHandle {
			return ErrChannel
		}
		if request[1] == 4 {
			if s.aborted {
				return ErrChannel
			}
			s.aborted = true
			err = s.postgresCleanup.Abort(ctx, handle)
		} else {
			if !s.aborted || s.observed {
				return ErrChannel
			}
			s.observed = true
			err = s.postgresCleanup.Observed(ctx, handle, observed)
		}
	default:
		return ErrChannel
	}
	if err != nil {
		response = []byte{1, 1}
	}
	if s.control.SetWriteDeadline(time.Now().Add(setupLimit)) != nil {
		return ErrChannel
	}
	n, _, err := s.control.WriteMsgUnix(response, nil, nil)
	if err != nil || n != len(response) {
		return ErrChannel
	}
	return nil
}

func (c *Client) Register(ctx context.Context, pid uint32, key []byte) (string, error) {
	if pid == 0 || len(key) < 4 || len(key) > 256 {
		return "", ErrChannel
	}
	request := make([]byte, 6+len(key))
	request[0] = 1
	request[1] = 3
	binary.BigEndian.PutUint32(request[2:6], pid)
	copy(request[6:], key)
	defer clear(request)
	response, err := c.cleanupExchange(ctx, request)
	if err != nil {
		return "", err
	}
	handle := string(response)
	if !validCleanupHandle(handle) {
		c.Close()
		return "", ErrChannel
	}
	return handle, nil
}
func (c *Client) Abort(ctx context.Context, handle string) error {
	if !validCleanupHandle(handle) {
		return ErrChannel
	}
	response, err := c.cleanupExchange(ctx, append([]byte{1, 4}, handle...))
	if len(response) != 0 {
		c.Close()
		return ErrChannel
	}
	return err
}
func (c *Client) Observed(ctx context.Context, handle string, observed bool) error {
	if !validCleanupHandle(handle) {
		return ErrChannel
	}
	flag := byte(0)
	if observed {
		flag = 1
	}
	response, err := c.cleanupExchange(ctx, append([]byte{1, 5, flag}, handle...))
	if len(response) != 0 {
		c.Close()
		return ErrChannel
	}
	return err
}
func (c *Client) cleanupExchange(ctx context.Context, request []byte) ([]byte, error) {
	if c == nil || ctx == nil {
		return nil, ErrChannel
	}
	select {
	case c.slot <- struct{}{}:
		defer func() { <-c.slot }()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrChannel
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	deadline := time.Now().Add(5 * time.Second)
	if until, ok := ctx.Deadline(); ok && until.Before(deadline) {
		deadline = until
	}
	if c.control.SetDeadline(deadline) != nil {
		c.Close()
		return nil, ErrChannel
	}
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if n, _, err := c.control.WriteMsgUnix(request, nil, nil); err != nil || n != len(request) {
		c.Close()
		return nil, ErrChannel
	}
	var response [131]byte
	oob := make([]byte, ancillaryBytes)
	n, on, flags, _, err := c.control.ReadMsgUnix(response[:], oob)
	credentials, rights, parseErr := parseControl(oob[:on])
	for _, fd := range rights {
		unix.Close(fd)
	}
	if err != nil || parseErr != nil || credentials != nil || len(rights) != 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || n < 2 || response[0] != 1 || response[1] > 1 || ctx.Err() != nil {
		c.Close()
		return nil, ErrChannel
	}
	if c.control.SetDeadline(time.Time{}) != nil {
		c.Close()
		return nil, ErrChannel
	}
	if response[1] != 0 {
		if n != 2 {
			c.Close()
		}
		return nil, ErrChannel
	}
	return append([]byte(nil), response[2:n]...), nil
}
