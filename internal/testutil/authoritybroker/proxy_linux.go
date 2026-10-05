//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authoritybroker

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type ProxyOutcome struct {
	Accepted          uint64 `json:"accepted"`
	Rejected          uint64 `json:"rejected"`
	Dropped           uint64 `json:"dropped"`
	PeakActive        int    `json:"peak_active"`
	Remaining         int    `json:"remaining"`
	ListenerJoined    bool   `json:"listener_joined"`
	ConnectionsJoined bool   `json:"connections_joined"`
}

type proxyPair struct {
	client, server net.Conn
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}
}

// Proxy passes TCP bytes unchanged, including TLS. Faults affect only its
// gateway's client connections; broker routes and other gateways stay intact.
type Proxy struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	listener             net.Listener
	address, destination string
	control              sync.Mutex
	mu                   sync.Mutex
	blocked, closed      bool
	pairs                map[*proxyPair]bool
	accepted             chan struct{}
	joined               chan struct{}
	wg                   sync.WaitGroup
	stats                ProxyOutcome
	closeErr             error
}

func newProxy(ctx context.Context, destination string) (*Proxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &Proxy{ctx: ctx, cancel: cancel, listener: listener, address: listener.Addr().String(), destination: destination, pairs: make(map[*proxyPair]bool), accepted: make(chan struct{}), joined: make(chan struct{})}
	go p.accept()
	// Waiting for accept to finish excludes a concurrent positive WaitGroup.Add.
	go func() { <-p.accepted; p.wg.Wait(); close(p.joined) }()
	return p, nil
}

func (p *Proxy) URL() string { return "tls://" + p.address }

func (p *Proxy) accept() {
	defer close(p.accepted)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.closed || p.blocked || p.ctx.Err() != nil || len(p.pairs) >= 32 {
			p.stats.Rejected++
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		ctx, cancel := context.WithCancel(p.ctx)
		pair := &proxyPair{client: client, ctx: ctx, cancel: cancel, done: make(chan struct{})}
		p.pairs[pair] = true
		p.stats.Accepted++
		p.stats.PeakActive = max(p.stats.PeakActive, len(p.pairs))
		p.wg.Add(1)
		p.mu.Unlock()
		go p.serve(pair)
	}
}

func (p *Proxy) serve(pair *proxyPair) {
	defer p.wg.Done()
	defer func() {
		pair.cancel()
		p.mu.Lock()
		_ = pair.client.Close()
		if pair.server != nil {
			_ = pair.server.Close()
		}
		delete(p.pairs, pair)
		p.mu.Unlock()
		close(pair.done)
	}()
	dialer := net.Dialer{Timeout: time.Second}
	server, err := dialer.DialContext(pair.ctx, "tcp", p.destination)
	if err != nil {
		return
	}
	p.mu.Lock()
	if p.closed || p.blocked || pair.ctx.Err() != nil {
		p.mu.Unlock()
		_ = server.Close()
		return
	}
	pair.server = server
	p.mu.Unlock()
	completed := make(chan struct{}, 2)
	copyBytes := func(dst, src net.Conn) {
		_, _ = io.CopyBuffer(dst, src, make([]byte, 16<<10))
		completed <- struct{}{}
	}
	go copyBytes(server, pair.client)
	go copyBytes(pair.client, server)
	remaining := 2
	select {
	case <-pair.ctx.Done():
	case <-completed:
		remaining--
	}
	_ = pair.client.Close()
	_ = server.Close()
	for range remaining {
		<-completed
	}
}

func (p *Proxy) change(block *bool) error {
	p.control.Lock()
	defer p.control.Unlock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("gateway proxy is closed")
	}
	if block != nil {
		p.blocked = *block
	}
	var dropped []*proxyPair
	if block == nil || *block {
		for pair := range p.pairs {
			pair.cancel()
			_ = pair.client.Close()
			if pair.server != nil {
				_ = pair.server.Close()
			}
			dropped = append(dropped, pair)
		}
		p.stats.Dropped += uint64(len(dropped))
	}
	p.mu.Unlock()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for _, pair := range dropped {
		select {
		case <-pair.done:
		case <-deadline.C:
			return errors.New("gateway proxy drop did not join owned connections")
		}
	}
	return nil
}

// Block joins existing client sockets and refuses subsequent connections.
func (p *Proxy) Block() error   { blocked := true; return p.change(&blocked) }
func (p *Proxy) Unblock() error { blocked := false; return p.change(&blocked) }

// Drop joins the connections present at this call; new clients may reconnect.
func (p *Proxy) Drop() error { return p.change(nil) }

func (p *Proxy) Close() error {
	p.control.Lock()
	defer p.control.Unlock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return p.closeErr
	}
	p.closed = true
	p.cancel()
	_ = p.listener.Close()
	for pair := range p.pairs {
		pair.cancel()
		_ = pair.client.Close()
		if pair.server != nil {
			_ = pair.server.Close()
		}
	}
	p.mu.Unlock()
	select {
	case <-p.joined:
		p.mu.Lock()
		p.stats.ListenerJoined, p.stats.ConnectionsJoined = true, true
		p.mu.Unlock()
	case <-time.After(2 * time.Second):
		p.closeErr = errors.New("gateway proxy cleanup did not join owned connections")
	}
	return p.closeErr
}

func (p *Proxy) outcome() ProxyOutcome {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := p.stats
	result.Remaining = len(p.pairs)
	return result
}
