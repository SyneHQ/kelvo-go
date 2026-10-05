//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type r3Socket struct{ local, remote, state, inode string }

func r3SocketAddress(raw string) (string, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return "", ErrInvalid
	}
	ip, err := hex.DecodeString(parts[0])
	if err != nil || (len(ip) != 4 && len(ip) != 16) {
		return "", ErrInvalid
	}
	for i := 0; i < len(ip); i += 4 {
		ip[i], ip[i+3] = ip[i+3], ip[i]
		ip[i+1], ip[i+2] = ip[i+2], ip[i+1]
	}
	port, err := strconv.ParseUint(parts[1], 16, 16)
	if err != nil {
		return "", ErrInvalid
	}
	return net.JoinHostPort(net.IP(ip).String(), strconv.FormatUint(port, 10)), nil
}

func r3ProcessSockets(pid int) ([]r3Socket, error) {
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil || len(entries) > 512 {
		return nil, ErrUnavailable
	}
	owned := map[string]bool{}
	for _, entry := range entries {
		link, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()))
		if err == nil && strings.HasPrefix(link, "socket:[") && strings.HasSuffix(link, "]") {
			owned[strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")] = true
		}
	}
	var result []r3Socket
	for _, table := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		file, err := os.Open(table)
		if err != nil {
			return nil, ErrUnavailable
		}
		raw, err := io.ReadAll(io.LimitReader(file, (2<<20)+1))
		_ = file.Close()
		if err != nil || len(raw) > 2<<20 {
			return nil, ErrUnavailable
		}
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 10 || !owned[fields[9]] {
				continue
			}
			local, e1 := r3SocketAddress(fields[1])
			remote, e2 := r3SocketAddress(fields[2])
			if e1 != nil || e2 != nil {
				return nil, ErrUnavailable
			}
			result = append(result, r3Socket{local, remote, fields[3], fields[9]})
		}
	}
	return result, nil
}

func (f *r3Fixture) routeSource(local, remote string) (int, error) {
	f.mu.RLock()
	var pids [3]int
	for i := range f.nodes {
		cmd := f.nodes[i].cmd
		if cmd != nil && cmd.Process != nil {
			pids[i] = cmd.Process.Pid
		}
	}
	f.mu.RUnlock()
	for i, pid := range pids {
		if pid == 0 {
			continue
		}
		sockets, err := r3ProcessSockets(pid)
		if err != nil {
			continue
		}
		for _, socket := range sockets {
			if socket.local == local && socket.remote == remote && socket.state == "01" {
				return i, nil
			}
		}
	}
	return -1, ErrUnavailable
}

type r3RoutePair struct {
	source         int
	client, server net.Conn
}

type r3RouteProxy struct {
	fixture     *r3Fixture
	destination int
	listener    net.Listener
	mu          sync.Mutex
	blocked     [3]bool
	closed      bool
	pairs       map[*r3RoutePair]bool
	wg          sync.WaitGroup
	accepted    chan struct{}
	joined      chan struct{}
	once        sync.Once
}

func newR3RouteProxy(f *r3Fixture, destination int) (*r3RouteProxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &r3RouteProxy{fixture: f, destination: destination, listener: listener, pairs: map[*r3RoutePair]bool{}, accepted: make(chan struct{}), joined: make(chan struct{})}
	go p.accept()
	return p, nil
}

func (p *r3RouteProxy) accept() {
	defer close(p.accepted)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.closed || len(p.pairs) >= 32 {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		pair := &r3RoutePair{source: -1, client: client}
		p.pairs[pair] = true
		p.wg.Add(1)
		p.mu.Unlock()
		go p.serve(pair)
	}
}

func (p *r3RouteProxy) serve(pair *r3RoutePair) {
	defer p.wg.Done()
	defer func() {
		p.mu.Lock()
		delete(p.pairs, pair)
		p.mu.Unlock()
		_ = pair.client.Close()
		if pair.server != nil {
			_ = pair.server.Close()
		}
	}()
	source := -1
	if !r3Wait(p.fixture.ctx, time.Second, func() bool {
		var err error
		source, err = p.fixture.routeSource(pair.client.RemoteAddr().String(), p.listener.Addr().String())
		return err == nil
	}) {
		return
	}
	p.mu.Lock()
	pair.source = source
	blocked := p.closed || p.blocked[source] || source == p.destination
	p.mu.Unlock()
	if blocked {
		return
	}
	server, err := (&net.Dialer{Timeout: time.Second}).DialContext(p.fixture.ctx, "tcp", p.fixture.nodes[p.destination].route)
	if err != nil {
		return
	}
	p.mu.Lock()
	if p.closed || p.blocked[source] {
		p.mu.Unlock()
		_ = server.Close()
		return
	}
	pair.server = server
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(server, pair.client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(pair.client, server); done <- struct{}{} }()
	<-done
	_ = server.Close()
	_ = pair.client.Close()
	<-done
}

func (p *r3RouteProxy) cut(source int, blocked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked[source] = blocked
	if blocked {
		for pair := range p.pairs {
			if pair.source == source {
				_ = pair.client.Close()
				if pair.server != nil {
					_ = pair.server.Close()
				}
			}
		}
	}
}

func (p *r3RouteProxy) stop() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		_ = p.listener.Close()
		for pair := range p.pairs {
			_ = pair.client.Close()
			if pair.server != nil {
				_ = pair.server.Close()
			}
		}
		p.mu.Unlock()
		go func() { <-p.accepted; p.wg.Wait(); close(p.joined) }()
	})
}

func (p *r3RouteProxy) join(budget time.Duration) bool {
	select {
	case <-p.joined:
		return true
	case <-time.After(budget):
		return false
	}
}

func (f *r3Fixture) partition(node int, blocked bool) {
	for destination, proxy := range f.routes {
		for source := range f.nodes {
			if source == node || destination == node {
				proxy.cut(source, blocked)
			}
		}
	}
}

func (f *r3Fixture) assertRoutes() error {
	proxies, localPorts := map[string]bool{}, map[string]bool{}
	for _, proxy := range f.routes {
		proxies[proxy.listener.Addr().String()] = true
	}
	for _, node := range f.nodes {
		localPorts[node.client], localPorts[node.route], localPorts[node.monitor] = true, true, true
	}
	for _, node := range f.nodes {
		sockets, err := r3ProcessSockets(node.cmd.Process.Pid)
		if err != nil {
			return err
		}
		for _, socket := range sockets {
			if socket.state != "01" || localPorts[socket.local] {
				continue
			}
			if !proxies[socket.remote] {
				return ErrDenied
			}
		}
	}
	edges := map[[2]int]bool{}
	for destination, proxy := range f.routes {
		proxy.mu.Lock()
		for pair := range proxy.pairs {
			if pair.source >= 0 && pair.server != nil {
				edges[[2]int{min(pair.source, destination), max(pair.source, destination)}] = true
			}
		}
		proxy.mu.Unlock()
	}
	if len(edges) != 3 {
		return ErrUnavailable
	}
	return nil
}

// The client proxy handles NATS' initial plaintext INFO then TLS upgrade. It
// parses bounded frames only after verifying fixture TLS on both connections.
type r3Frame struct {
	fields []string
	body   []byte
}

func r3ReadFrame(reader *bufio.Reader) (r3Frame, error) {
	line, err := reader.ReadSlice('\n')
	if err != nil || len(line) > 8192 || !bytes.HasSuffix(line, []byte("\r\n")) {
		return r3Frame{}, ErrUnavailable
	}
	fields := strings.Fields(string(line[:len(line)-2]))
	if len(fields) == 0 || len(fields) > 64 {
		return r3Frame{}, ErrUnavailable
	}
	frame := r3Frame{fields: fields}
	switch fields[0] {
	case "PUB", "HPUB", "MSG", "HMSG":
		n, err := strconv.ParseUint(fields[len(fields)-1], 10, 32)
		if err != nil || n > 128<<10 {
			return r3Frame{}, ErrUnavailable
		}
		frame.body = make([]byte, int(n)+2)
		if _, err = io.ReadFull(reader, frame.body); err != nil || !bytes.HasSuffix(frame.body, []byte("\r\n")) {
			return r3Frame{}, ErrUnavailable
		}
		frame.body = frame.body[:n]
	case "INFO", "CONNECT", "SUB", "UNSUB", "PING", "PONG", "+OK", "-ERR":
	default:
		return r3Frame{}, ErrUnavailable
	}
	return frame, nil
}

func (frame r3Frame) write(writer io.Writer) error {
	fields := append([]string(nil), frame.fields...)
	switch fields[0] {
	case "PUB", "HPUB", "MSG", "HMSG":
		fields[len(fields)-1] = strconv.Itoa(len(frame.body))
	}
	if _, err := io.WriteString(writer, strings.Join(fields, " ")+"\r\n"); err != nil {
		return err
	}
	switch fields[0] {
	case "PUB", "HPUB", "MSG", "HMSG":
		if _, err := writer.Write(frame.body); err != nil {
			return err
		}
		_, err := io.WriteString(writer, "\r\n")
		return err
	}
	return nil
}

type r3ResponseFault struct {
	kind, subject string
	replacement   []byte
	used          bool
}
type r3ClientPair struct{ client, server net.Conn }

type r3ClientProxy struct {
	f                *r3Fixture
	listener         net.Listener
	upstream         string
	mu               sync.Mutex
	pairs            map[*r3ClientPair]bool
	inboxes          map[string]bool
	fault            r3ResponseFault
	captured         []byte
	priorInbox       string
	requests         int
	invalid          bool
	closed           bool
	wg               sync.WaitGroup
	accepted, joined chan struct{}
	once             sync.Once
}

func newR3ClientProxy(f *r3Fixture, node int) (*r3ClientProxy, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &r3ClientProxy{f: f, listener: l, upstream: f.nodes[node].client, pairs: map[*r3ClientPair]bool{}, inboxes: map[string]bool{}, accepted: make(chan struct{}), joined: make(chan struct{})}
	go func() {
		defer close(p.accepted)
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.closed || len(p.pairs) >= 8 {
				p.mu.Unlock()
				_ = client.Close()
				continue
			}
			pair := &r3ClientPair{client: client}
			p.pairs[pair] = true
			p.wg.Add(1)
			p.mu.Unlock()
			go p.serve(pair)
		}
	}()
	return p, nil
}

func (p *r3ClientProxy) arm(kind, subject string, replacement []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fault = r3ResponseFault{kind: kind, subject: subject, replacement: append([]byte(nil), replacement...)}
}

func (p *r3ClientProxy) serve(pair *r3ClientPair) {
	defer p.wg.Done()
	pairCtx, cancel := context.WithCancel(p.f.ctx)
	defer cancel()
	defer func() {
		p.mu.Lock()
		delete(p.pairs, pair)
		p.mu.Unlock()
		_ = pair.client.Close()
		if pair.server != nil {
			_ = pair.server.Close()
		}
	}()
	raw, err := (&net.Dialer{Timeout: time.Second}).DialContext(p.f.ctx, "tcp", p.upstream)
	if err != nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = raw.Close()
		return
	}
	pair.server = raw
	p.mu.Unlock()
	deadline := time.Now().Add(4 * time.Second)
	_ = raw.SetDeadline(deadline)
	_ = pair.client.SetDeadline(deadline)
	reader := bufio.NewReaderSize(raw, 8192)
	info, err := r3ReadFrame(reader)
	if err != nil || info.fields[0] != "INFO" || reader.Buffered() != 0 || info.write(pair.client) != nil {
		return
	}
	serverTLS := tls.Client(raw, p.f.tlsConfig.Clone())
	clientTLS := tls.Server(pair.client, p.f.tlsConfig.Clone())
	if serverTLS.HandshakeContext(p.f.ctx) != nil || clientTLS.HandshakeContext(p.f.ctx) != nil {
		return
	}
	pending := map[string]string{}
	var pendingMu sync.Mutex
	finished := make(chan struct{}, 2)
	go func() {
		defer func() { finished <- struct{}{} }()
		reader := bufio.NewReaderSize(clientTLS, 8192)
		for frames := 0; frames < 64; frames++ {
			frame, err := r3ReadFrame(reader)
			if err != nil {
				return
			}
			if frame.fields[0] == "PUB" || frame.fields[0] == "HPUB" {
				minimum := 4
				if frame.fields[0] == "HPUB" {
					minimum = 5
				}
				if len(frame.fields) != minimum {
					p.mu.Lock()
					p.invalid = true
					p.mu.Unlock()
					return
				}
				subject, inbox := frame.fields[1], frame.fields[2]
				parts := strings.Split(inbox, ".")
				p.mu.Lock()
				valid := len(parts) == 4 && parts[0] == "_INBOX" && parts[1] == "kelvo-authfence" && len(parts[3]) == 48 && !p.inboxes[inbox] && len(p.inboxes) < 256
				if valid {
					_, err = hex.DecodeString(parts[3])
					valid = err == nil
				}
				if !valid {
					p.invalid = true
					p.mu.Unlock()
					return
				}
				p.inboxes[inbox] = true
				p.requests++
				p.mu.Unlock()
				pendingMu.Lock()
				pending[inbox] = subject
				pendingMu.Unlock()
			}
			if frame.write(serverTLS) != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { finished <- struct{}{} }()
		reader := bufio.NewReaderSize(serverTLS, 8192)
		for frames := 0; frames < 64; frames++ {
			frame, err := r3ReadFrame(reader)
			if err != nil {
				return
			}
			if frame.fields[0] == "MSG" || frame.fields[0] == "HMSG" {
				if len(frame.fields) < 4 {
					return
				}
				inbox := frame.fields[1]
				pendingMu.Lock()
				subject := pending[inbox]
				delete(pending, inbox)
				pendingMu.Unlock()
				p.mu.Lock()
				fault := p.fault
				apply := !fault.used && fault.kind != "" && fault.subject == subject
				if apply {
					p.fault.used = true
				}
				prior := p.priorInbox
				p.priorInbox = inbox
				if subject == messageGetSubject {
					p.captured = append([]byte(nil), frame.body...)
				}
				p.mu.Unlock()
				if apply {
					switch fault.kind {
					case "drop":
						<-pairCtx.Done()
						return
					case "delay":
						timer := time.NewTimer(2200 * time.Millisecond)
						select {
						case <-timer.C:
						case <-pairCtx.Done():
							timer.Stop()
							return
						}
					case "wrong_subject":
						frame.fields[1] = inbox + "-foreign"
					case "retired_inbox":
						if prior == "" {
							return
						}
						frame.fields[1] = prior
					case "duplicate":
						var ack map[string]any
						if json.Unmarshal(frame.body, &ack) != nil {
							return
						}
						ack["duplicate"] = true
						frame.body, _ = json.Marshal(ack)
					case "stale_get":
						frame.body = append([]byte(nil), fault.replacement...)
					}
				}
			}
			if frame.write(clientTLS) != nil {
				return
			}
		}
	}()
	<-finished
	cancel()
	_ = raw.Close()
	_ = pair.client.Close()
	<-finished
}

func (p *r3ClientProxy) stop() bool {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		_ = p.listener.Close()
		for pair := range p.pairs {
			_ = pair.client.Close()
			if pair.server != nil {
				_ = pair.server.Close()
			}
		}
		p.mu.Unlock()
		go func() { <-p.accepted; p.wg.Wait(); close(p.joined) }()
	})
	select {
	case <-p.joined:
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}
