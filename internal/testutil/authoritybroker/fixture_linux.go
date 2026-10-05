//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package authoritybroker owns opt-in TLS broker fixtures, not authority clients.
package authoritybroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const Env = "KELVO_TEST_GATEWAY_AUTHORITY"

type Endpoint struct {
	URL, CAFile, CertFile, KeyFile, Username, PasswordEnv string
}

// Outcome is a terminal fixture receipt. It contains no generated credentials.
type Outcome struct {
	Version            string          `json:"version"`
	ArchiveSHA256      string          `json:"archive_sha256"`
	BinarySHA256       string          `json:"binary_sha256"`
	Brokers            []BrokerOutcome `json:"brokers"`
	Proxies            []ProxyOutcome  `json:"proxies"`
	Ready              bool            `json:"ready"`
	CleanupPassed      bool            `json:"cleanup_passed"`
	CredentialsRemoved bool            `json:"credentials_removed"`
	SocketsAbsent      bool            `json:"sockets_absent"`
}

type Fixture struct {
	dir, version, binary, ca, cert, key string
	passwords, passwordEnvs             map[string]string
	nodes                               [3]node
	proxies                             [2]*Proxy
	ctx                                 context.Context
	cancel                              context.CancelFunc
	once                                sync.Once
	mu                                  sync.Mutex
	outcome                             Outcome
	closed                              bool
	closeErr                            error
}

func Versions() []string { return []string{"2.14.7", "2.15.0"} }

// Start requires cached pinned archives and a private output parent. Callers
// close their clients before Close; the fixture never owns caller goroutines.
// Each version uses independent nodes, credentials, storage and gateway proxies.
func Start(t testing.TB, version string) *Fixture {
	t.Helper()
	Require(t)
	parent := os.Getenv(Env + "_DIR")
	info, err := os.Lstat(parent)
	if !filepath.IsAbs(parent) || filepath.Clean(parent) != parent || err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal("gateway authority fixture needs an existing private 0700 output parent")
	}
	release, ok := releases[version]
	if !ok {
		t.Fatal("unsupported gateway authority fixture broker version")
	}
	dir, err := os.MkdirTemp(parent, "gateway-authority-"+version+"-")
	if err != nil {
		t.Fatal("cannot create gateway authority fixture directory")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	f := &Fixture{dir: dir, version: version, ctx: ctx, cancel: cancel, passwords: make(map[string]string), passwordEnvs: make(map[string]string)}
	f.outcome.Version, f.outcome.ArchiveSHA256 = version, release.archive
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error("gateway authority fixture cleanup failed", err)
		}
	})
	if err := f.start(release); err != nil {
		t.Fatal("gateway authority fixture startup failed", err)
	}
	t.Logf("GATEWAY_AUTHORITY_BROKERS version=%s archive_sha256=%s binary_sha256=%s", version, release.archive, f.outcome.BinarySHA256)
	return f
}

func (f *Fixture) start(release release) error {
	var err error
	f.binary, f.outcome.BinarySHA256, err = extractBinary(f.dir, f.version, release)
	if err != nil {
		return err
	}
	if err = f.credentials(); err != nil {
		return err
	}
	var reservations [3][]net.Listener
	defer func() {
		for _, group := range reservations {
			for _, listener := range group {
				_ = listener.Close()
			}
		}
	}()
	for i := range f.nodes {
		f.nodes[i].name = fmt.Sprintf("kelvo-gateway-authority-%d", i)
		for _, target := range []*string{&f.nodes[i].client, &f.nodes[i].route, &f.nodes[i].monitor} {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return errors.New("fixture port reservation failed")
			}
			reservations[i] = append(reservations[i], listener)
			*target = listener.Addr().String()
		}
	}
	for i := range f.nodes {
		raw, err := json.Marshal(f.serverConfig(i))
		if err != nil {
			return errors.New("fixture config encoding failed")
		}
		path := filepath.Join(f.dir, fmt.Sprintf("node-%d.conf", i))
		if err = writePrivate(path, raw, 0600); err != nil {
			return errors.New("fixture config write failed")
		}
		for _, listener := range reservations[i] {
			_ = listener.Close()
		}
		if err = f.nodes[i].start(f.binary, path, filepath.Join(f.dir, fmt.Sprintf("node-%d.log", i))); err != nil {
			return err
		}
	}
	if !waitFor(f.ctx, 25*time.Second, f.healthy) {
		return errors.New("three TLS broker nodes did not become ready")
	}
	for i := range f.proxies {
		f.proxies[i], err = newProxy(f.ctx, f.nodes[i].client)
		if err != nil {
			return errors.New("gateway proxy creation failed")
		}
	}
	f.outcome.Ready = true
	return nil
}

func (f *Fixture) Config(node int, role string) Endpoint {
	if node < 0 || node >= len(f.nodes) || f.passwordEnvs[role] == "" {
		return Endpoint{}
	}
	return Endpoint{URL: "tls://" + f.nodes[node].client, CAFile: f.ca, CertFile: f.cert, KeyFile: f.key, Username: role, PasswordEnv: f.passwordEnvs[role]}
}

func (f *Fixture) GatewayConfig(index int) Endpoint {
	if index < 0 || index >= len(f.proxies) || f.proxies[index] == nil {
		return Endpoint{}
	}
	endpoint := f.Config(index, []string{"gateway-a", "gateway-b"}[index])
	endpoint.URL = f.proxies[index].URL()
	return endpoint
}

func (f *Fixture) GatewayProxy(index int) *Proxy {
	if index < 0 || index >= len(f.proxies) {
		return nil
	}
	return f.proxies[index]
}

func (f *Fixture) ReceiptPath() string { return filepath.Join(f.dir, "receipt.json") }

func (f *Fixture) Receipt() (Outcome, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		return Outcome{}, false
	}
	result := f.outcome
	result.Brokers = append([]BrokerOutcome(nil), result.Brokers...)
	result.Proxies = append([]ProxyOutcome(nil), result.Proxies...)
	return result, true
}

func (f *Fixture) Close() error {
	f.once.Do(func() {
		f.cancel()
		quiescent := true
		for _, proxy := range f.proxies {
			if proxy != nil {
				if err := proxy.Close(); err != nil {
					f.closeErr = errors.Join(f.closeErr, err)
					quiescent = false
				}
				f.outcome.Proxies = append(f.outcome.Proxies, proxy.outcome())
			}
		}
		for i := range f.nodes {
			node := &f.nodes[i]
			if node.cmd == nil {
				continue
			}
			if err := node.stop(); err != nil {
				f.closeErr = errors.Join(f.closeErr, err)
			}
			f.outcome.Brokers = append(f.outcome.Brokers, node.receipt)
			quiescent = quiescent && node.receipt.Reaped && node.receipt.GroupAbsent
		}
		f.outcome.SocketsAbsent = true
		for _, node := range f.nodes {
			for _, address := range []string{node.client, node.route, node.monitor} {
				if address != "" && !socketAbsent(address) {
					f.outcome.SocketsAbsent = false
				}
			}
		}
		for _, proxy := range f.proxies {
			if proxy != nil && !socketAbsent(proxy.address) {
				f.outcome.SocketsAbsent = false
			}
		}
		quiescent = quiescent && f.outcome.SocketsAbsent
		if !quiescent {
			f.closeErr = errors.Join(f.closeErr, errors.New("fixture child or socket cleanup unproven"))
		} else {
			f.outcome.CredentialsRemoved = true
			for _, path := range []string{f.ca, f.cert, f.key, filepath.Join(f.dir, "node-0.conf"), filepath.Join(f.dir, "node-1.conf"), filepath.Join(f.dir, "node-2.conf")} {
				if path != "" {
					if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
						f.outcome.CredentialsRemoved = false
						f.closeErr = errors.Join(f.closeErr, errors.New("fixture credential removal failed"))
					}
				}
			}
			for _, name := range f.passwordEnvs {
				if err := os.Unsetenv(name); err != nil {
					f.outcome.CredentialsRemoved = false
					f.closeErr = errors.Join(f.closeErr, errors.New("fixture environment cleanup failed"))
				}
			}
			clear(f.passwords)
		}
		f.outcome.CleanupPassed = f.closeErr == nil && quiescent && f.outcome.CredentialsRemoved
		raw, err := json.MarshalIndent(f.outcome, "", "  ")
		if err != nil || writePrivate(f.ReceiptPath(), append(raw, '\n'), 0600) != nil {
			f.closeErr = errors.Join(f.closeErr, errors.New("fixture receipt write failed"))
		}
		f.mu.Lock()
		f.closed = true
		f.mu.Unlock()
	})
	return f.closeErr
}

func (f *Fixture) healthy() bool {
	var identities [3]string
	for i, node := range f.nodes {
		var info struct {
			Name    string `json:"server_name"`
			Version string `json:"version"`
			ID      string `json:"server_id"`
		}
		if f.monitor(node.monitor, "/varz", &info) != nil || info.Name != node.name || info.Version != f.version || info.ID == "" {
			return false
		}
		identities[i] = info.ID
	}
	if identities[0] == identities[1] || identities[0] == identities[2] || identities[1] == identities[2] {
		return false
	}
	for i, node := range f.nodes {
		var routes struct {
			NumRoutes int `json:"num_routes"`
			Routes    []struct {
				ID string `json:"remote_id"`
			} `json:"routes"`
		}
		if f.monitor(node.monitor, "/routez", &routes) != nil || routes.NumRoutes != len(routes.Routes) || len(routes.Routes) < 2 || len(routes.Routes) > 8 {
			return false
		}
		peers := map[string]bool{}
		for _, route := range routes.Routes {
			peers[route.ID] = true
		}
		if len(peers) != 2 {
			return false
		}
		for j, id := range identities {
			if i != j && !peers[id] {
				return false
			}
		}
	}
	return true
}

func (f *Fixture) monitor(address, path string, result any) error {
	ctx, cancel := context.WithTimeout(f.ctx, 500*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+path, nil)
	if err != nil {
		return err
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, (128<<10)+1))
	if err != nil || len(raw) > 128<<10 || response.StatusCode != http.StatusOK {
		return errors.New("fixture health response invalid")
	}
	return json.Unmarshal(raw, result)
}

func waitFor(ctx context.Context, timeout time.Duration, check func() bool) bool {
	deadline := time.Now().Add(timeout)
	for ctx.Err() == nil && time.Now().Before(deadline) {
		if check() {
			return true
		}
		timer := time.NewTimer(40 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
	return false
}

func writePrivate(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return errors.Join(err, f.Close())
}
