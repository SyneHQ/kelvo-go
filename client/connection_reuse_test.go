// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/client/connreuse"
)

// Sequential calls share one connection, and a call that fails ambiguously on
// that reused connection is reported, not silently sent a second time.
func TestClientReusesOneConnectionWithoutReplay(t *testing.T) {
	var calls atomic.Int32
	var drop atomic.Bool
	var peers sync.Map // one client port per connection
	connections := func() (n int) {
		peers.Range(func(any, any) bool { n++; return true })
		return n
	}
	s := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		peers.Store(r.RemoteAddr, true)
		if drop.Swap(false) {
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				conn.Close()
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ready"}`)
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, s))
	if _, ok := c.http.Transport.(*connreuse.Transport); !ok || c.transport.DisableKeepAlives || c.transport.IdleConnTimeout != connectionIdle {
		t.Fatal("client does not use the bounded reuse transport")
	}
	for i := 0; i < 3; i++ {
		if err := c.Ready(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if connections() != 1 || calls.Load() != 3 {
		t.Fatalf("connections=%d calls=%d", connections(), calls.Load())
	}
	drop.Store(true)
	if err := c.Ready(context.Background()); err == nil {
		t.Fatal("a dropped call reported success")
	}
	if calls.Load() != 4 {
		t.Fatalf("the dropped GET was replayed: calls=%d", calls.Load())
	}
	if err := c.Ready(context.Background()); err != nil || connections() != 2 {
		t.Fatal("client did not recover on a new connection", err, connections())
	}
}
