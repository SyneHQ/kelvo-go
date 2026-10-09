//go:build linux

package childipc

import (
	"context"
	"os"
	"sync"
	"testing"
)

type cleanupFixture struct {
	mu                   sync.Mutex
	pid                  uint32
	key                  []byte
	aborts, observations int
	observed             bool
}

func (f *cleanupFixture) Register(_ context.Context, pid uint32, key []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pid = pid
	f.key = append([]byte(nil), key...)
	return "opaque-handle", nil
}
func (f *cleanupFixture) Abort(_ context.Context, handle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if handle != "opaque-handle" {
		return ErrChannel
	}
	f.aborts++
	return nil
}
func (f *cleanupFixture) Observed(_ context.Context, handle string, observed bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if handle != "opaque-handle" {
		return ErrChannel
	}
	f.observations++
	f.observed = observed
	return nil
}
func typedFixture(t *testing.T) (*Client, *cleanupFixture) {
	t.Helper()
	dial, _ := echoDialer(t)
	cleanup := &cleanupFixture{}
	server, file, err := NewPairWithPostgresCleanup(context.Background(), "source.private:5432", dial, cleanup)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(file, "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(os.Getpid())
	t.Cleanup(func() { client.Close(); closeServer(t, server) })
	return client, cleanup
}
func TestTypedPostgresCleanup(t *testing.T) {
	c, f := typedFixture(t)
	ctx := context.Background()
	conn, err := c.DialContext(ctx, "tcp", "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key := make([]byte, 256)
	key[255] = 1
	handle, err := c.Register(ctx, 23, key)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Abort(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if err = c.Observed(ctx, handle, true); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pid != 23 || len(f.key) != 256 || f.key[255] != 1 || f.aborts != 1 || f.observations != 1 || !f.observed {
		t.Fatal("cleanup binding lost")
	}
}
func TestTypedPostgresRejectsReplaysAndTargetReplacement(t *testing.T) {
	for _, action := range []string{"early-register", "second-data", "raw-cancel", "register-again", "wrong-handle", "second-abort", "early-observed", "second-observed"} {
		t.Run(action, func(t *testing.T) {
			c, _ := typedFixture(t)
			ctx := context.Background()
			if action == "early-register" {
				if _, err := c.Register(ctx, 1, []byte{1, 2, 3, 4}); err == nil {
					t.Fatal("accepted registration before data")
				}
				return
			}
			conn, err := c.DialContext(ctx, "tcp", "source.private:5432")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if action == "second-data" || action == "raw-cancel" {
				dial := c.DialContext
				if action == "raw-cancel" {
					dial = c.DialCancellation
				}
				if extra, err := dial(ctx, "tcp", "source.private:5432"); err == nil {
					extra.Close()
					t.Fatal("accepted extra descriptor")
				}
				return
			}
			handle, err := c.Register(ctx, 1, []byte{1, 2, 3, 4})
			if err != nil {
				t.Fatal(err)
			}
			switch action {
			case "register-again":
				_, err = c.Register(ctx, 2, []byte{4, 3, 2, 1})
			case "wrong-handle":
				err = c.Abort(ctx, "different-handle")
			case "early-observed":
				err = c.Observed(ctx, handle, true)
			case "second-abort":
				if err = c.Abort(ctx, handle); err != nil {
					t.Fatal(err)
				}
				err = c.Abort(ctx, handle)
			case "second-observed":
				if err = c.Abort(ctx, handle); err != nil {
					t.Fatal(err)
				}
				if err = c.Observed(ctx, handle, false); err != nil {
					t.Fatal(err)
				}
				err = c.Observed(ctx, handle, true)
			}
			if err == nil {
				t.Fatal("accepted invalid cleanup request")
			}
		})
	}
}
