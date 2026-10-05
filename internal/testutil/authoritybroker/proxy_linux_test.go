//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authoritybroker

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func echoServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	connections := map[net.Conn]bool{}
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections[connection] = true
			wg.Add(1)
			mu.Unlock()
			go func() {
				defer wg.Done()
				_, _ = io.Copy(connection, connection)
				_ = connection.Close()
				mu.Lock()
				delete(connections, connection)
				mu.Unlock()
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-accepted
		mu.Lock()
		for connection := range connections {
			_ = connection.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return listener.Addr().String()
}

func proxyConnection(t *testing.T, p *Proxy) net.Conn {
	t.Helper()
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(p.URL(), "tls://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func exchange(connection net.Conn) error {
	if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	want := []byte{0, 255, 3, 'I', 'N', 'F', 'O', '\r', '\n'}
	if _, err := connection.Write(want); err != nil {
		return err
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(connection, got); err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func TestProxyIsolatesGatewayConnections(t *testing.T) {
	server := echoServer(t)
	var proxies [2]*Proxy
	for i := range proxies {
		p, err := newProxy(context.Background(), server)
		if err != nil {
			t.Fatal(err)
		}
		proxies[i] = p
		t.Cleanup(func() {
			if err := p.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	a, b := proxyConnection(t, proxies[0]), proxyConnection(t, proxies[1])
	if exchange(a) != nil || exchange(b) != nil {
		t.Fatal("transparent bytes changed")
	}
	if err := proxies[0].Block(); err != nil {
		t.Fatal(err)
	}
	if exchange(a) == nil {
		t.Fatal("blocked gateway retained its prior connection")
	}
	if exchange(proxyConnection(t, proxies[0])) == nil {
		t.Fatal("blocked gateway opened a new connection")
	}
	if err := exchange(b); err != nil {
		t.Fatal("blocking one gateway affected the other", err)
	}
	if err := proxies[0].Unblock(); err != nil {
		t.Fatal(err)
	}
	recovered := proxyConnection(t, proxies[0])
	if err := exchange(recovered); err != nil {
		t.Fatal("unblocked gateway did not recover", err)
	}
	if err := proxies[0].Drop(); err != nil {
		t.Fatal(err)
	}
	if exchange(recovered) == nil {
		t.Fatal("drop retained old connection")
	}
	if err := exchange(proxyConnection(t, proxies[0])); err != nil {
		t.Fatal("drop blocked replacement connection", err)
	}
	for _, p := range proxies {
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal("close was not idempotent", err)
		}
		outcome := p.outcome()
		if outcome.Remaining != 0 || !outcome.ListenerJoined || !outcome.ConnectionsJoined || !socketAbsent(p.address) {
			t.Fatal("proxy cleanup incomplete", outcome)
		}
	}
}

func TestProxyCloseJoinsIdleConnections(t *testing.T) {
	server := echoServer(t)
	p, err := newProxy(context.Background(), server)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	connection := proxyConnection(t, p)
	if err := exchange(connection); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		_ = proxyConnection(t, p)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if p.outcome().Remaining != 0 || !socketAbsent(p.address) {
		t.Fatal("close retained proxy resources")
	}
	if p.Block() == nil || p.Unblock() == nil || p.Drop() == nil {
		t.Fatal("closed proxy accepted a control operation")
	}
}
