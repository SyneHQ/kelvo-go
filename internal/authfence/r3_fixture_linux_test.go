//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const r3Env = "KELVO_TEST_AUTHFENCE_R3"

var r3Archives = []struct{ version, digest, env string }{
	{"2.14.7", "e5c20b1cb2c0566b54c544312e91e011f9e130c5c80f16a14f4cf28ef30b8be2", r3Env + "_ARCHIVE_2_14_7"},
	{"2.15.0", "5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92", r3Env + "_ARCHIVE_2_15_0"},
}

type r3Node struct {
	name, client, route, monitor string
	cmd                          *exec.Cmd
	done                         chan error
	stopped                      bool
	identity                     r3BrokerOutcome
}

type r3BrokerOutcome struct {
	Name            string `json:"name"`
	PID             int    `json:"pid"`
	StartTicks      string `json:"start_ticks"`
	AliveBeforeStop bool   `json:"alive_before_stop"`
	ForcedKill      bool   `json:"forced_kill"`
	Reaped          bool   `json:"reaped"`
	WaitOutcome     string `json:"wait_outcome"`
	ExitCode        int    `json:"exit_code"`
	GroupAbsent     bool   `json:"process_group_absent"`
}

type r3Fixture struct {
	mu                   sync.RWMutex
	t                    *testing.T
	ctx                  context.Context
	cancel               context.CancelFunc
	dir, version, binary string
	binarySHA            string
	scope                Scope
	ca, cert, key        string
	tlsConfig            *tls.Config
	nodes                [3]r3Node
	routes               [3]*r3RouteProxy
	passwords            map[string]string
	closeOnce            sync.Once
	closeErr             error
	peakRSS              uint64
	peakFDs              int
	resourceReadErrors   int
	credentialsRemoved   bool
}

func r3RequireCaps(t *testing.T) {
	t.Helper()
	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil || len(raw) > 4096 {
		t.Fatal("cannot verify fixture cgroup")
	}
	path := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "0::/") {
			path = strings.TrimPrefix(line, "0::")
		}
	}
	if path == "" || strings.Contains(path, "..") {
		t.Fatal("fixture requires cgroup v2")
	}
	base := filepath.Join("/sys/fs/cgroup", path)
	read := func(name string) []string {
		data, err := os.ReadFile(filepath.Join(base, name))
		if err != nil || len(data) > 128 {
			t.Fatal("missing finite fixture cgroup limit", name)
		}
		return strings.Fields(string(data))
	}
	number := func(parts []string, maximum uint64) uint64 {
		if len(parts) != 1 {
			t.Fatal("invalid fixture cgroup limit")
		}
		value, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || value == 0 || value > maximum {
			t.Fatal("fixture cgroup is not finitely capped")
		}
		return value
	}
	memory := number(read("memory.max"), 4<<30)
	pids := number(read("pids.max"), 512)
	cpu := read("cpu.max")
	if len(cpu) != 2 {
		t.Fatal("invalid fixture CPU cap")
	}
	quota := number(cpu[:1], 4_000_000)
	period := number(cpu[1:], 1_000_000)
	if quota > 4*period {
		t.Fatal("fixture CPU cap exceeds four CPUs")
	}
	t.Logf("R3_CAPS memory_bytes=%d tasks=%d cpu_quota=%d cpu_period=%d", memory, pids, quota, period)
}

func r3Binary(t *testing.T, dir string, release struct{ version, digest, env string }) (string, string) {
	t.Helper()
	path := os.Getenv(release.env)
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		t.Fatal("opted-in fixture requires an absolute cached archive", release.env)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal("cannot open cached broker archive")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 64<<20 {
		t.Fatal("invalid cached broker archive")
	}
	compressed, err := io.ReadAll(io.LimitReader(f, (64<<20)+1))
	if err != nil || len(compressed) > 64<<20 || fmt.Sprintf("%x", sha256.Sum256(compressed)) != release.digest {
		t.Fatal("cached broker archive checksum mismatch")
	}
	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal("invalid pinned broker archive")
	}
	defer gz.Close()
	bundle := tar.NewReader(io.LimitReader(gz, 96<<20))
	member := "nats-server-v" + release.version + "-linux-amd64/nats-server"
	var binary []byte
	for i := 0; i < 32; i++ {
		header, err := bundle.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal("invalid pinned archive member")
		}
		if header.Name != member {
			continue
		}
		if binary != nil || header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > 64<<20 {
			t.Fatal("invalid pinned broker executable member")
		}
		binary, err = io.ReadAll(io.LimitReader(bundle, (64<<20)+1))
		if err != nil || int64(len(binary)) != header.Size {
			t.Fatal("truncated pinned broker executable")
		}
	}
	if len(binary) == 0 {
		t.Fatal("pinned broker executable missing")
	}
	destination := filepath.Join(dir, "nats-server")
	if err = os.WriteFile(destination, binary, 0700); err != nil {
		t.Fatal("cannot write owned broker executable")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(binary))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, destination, "-v")
	cmd.Env = []string{"GOMAXPROCS=1", "GOMEMLIMIT=96MiB"}
	output, err := cmd.Output()
	if err != nil || strings.TrimSpace(string(output)) != "nats-server: v"+release.version {
		t.Fatal("pinned executable reported an unexpected version")
	}
	return destination, digest
}

func r3Certificates(t *testing.T, dir string) (string, string, string, *tls.Config) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal("fixture CA key generation failed")
	}
	now := time.Now()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "kelvo-r3-fixture-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal("fixture CA certificate generation failed")
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal("fixture leaf key generation failed")
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "kelvo-r3-fixture"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal("fixture leaf certificate generation failed")
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal("fixture key encoding failed")
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	ca, cert, key := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "leaf.pem"), filepath.Join(dir, "leaf.key")
	for path, data := range map[string][]byte{ca: caPEM, cert: certPEM, key: keyPEM} {
		if os.WriteFile(path, data, 0600) != nil {
			t.Fatal("fixture certificate write failed")
		}
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal("fixture TLS identity invalid")
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	return ca, cert, key, &tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: pool, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS13}
}

func newR3Fixture(t *testing.T, release struct{ version, digest, env string }) *r3Fixture {
	t.Helper()
	parent := os.Getenv(r3Env + "_DIR")
	info, err := os.Lstat(parent)
	if !filepath.IsAbs(parent) || filepath.Clean(parent) != parent || err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal("fixture output parent must be an existing private 0700 directory")
	}
	dir, err := os.MkdirTemp(parent, "r3-"+release.version+"-")
	if err != nil {
		t.Fatal("cannot create private fixture directory")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	f := &r3Fixture{t: t, ctx: ctx, cancel: cancel, dir: dir, version: release.version, passwords: map[string]string{}}
	t.Cleanup(func() {
		if err := f.close(); err != nil {
			t.Error("R3 fixture cleanup failed", err)
		}
	})
	f.binary, f.binarySHA = r3Binary(t, dir, release)
	f.ca, f.cert, f.key, f.tlsConfig = r3Certificates(t, dir)
	f.scope, err = NewScope("r3-fixture", []string{"tenant-a", "tenant-b"}, []string{"gateway-a", "gateway-b"})
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"initializer", "control", "gateway-a", "gateway-b", "outsider"} {
		var secret [32]byte
		if _, err := rand.Read(secret[:]); err != nil {
			t.Fatal("fixture credential generation failed")
		}
		f.passwords[role] = hex.EncodeToString(secret[:])
		t.Setenv(f.passwordEnv(role), f.passwords[role])
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
		f.nodes[i].name = fmt.Sprintf("kelvo-authfence-r3-%d", i)
		for _, target := range []*string{&f.nodes[i].client, &f.nodes[i].route, &f.nodes[i].monitor} {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal("fixture port reservation failed")
			}
			reservations[i] = append(reservations[i], listener)
			*target = listener.Addr().String()
		}
	}
	for i := range f.routes {
		f.routes[i], err = newR3RouteProxy(f, i)
		if err != nil {
			t.Fatal("route proxy creation failed")
		}
	}
	for i := range f.nodes {
		cfg := f.serverConfig(i)
		raw, _ := json.Marshal(cfg)
		configFile := filepath.Join(dir, fmt.Sprintf("node-%d.conf", i))
		if os.WriteFile(configFile, raw, 0600) != nil {
			t.Fatal("broker configuration write failed")
		}
		for _, listener := range reservations[i] {
			_ = listener.Close()
		}
		log, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("node-%d.log", i)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal("broker private log creation failed")
		}
		cmd := exec.Command(f.binary, "-c", configFile)
		cmd.Env = []string{"GOMAXPROCS=1", "GOMEMLIMIT=96MiB"}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
		cmd.Stdout, cmd.Stderr = &r3BoundedLog{file: log}, nil
		cmd.Stderr = cmd.Stdout
		if err = cmd.Start(); err != nil {
			_ = log.Close()
			t.Fatal("owned broker start failed")
		}
		f.mu.Lock()
		f.nodes[i].cmd, f.nodes[i].done = cmd, make(chan error, 1)
		f.mu.Unlock()
		go func(node *r3Node) { err := node.cmd.Wait(); _ = log.Close(); node.done <- err }(&f.nodes[i])
		ticks, group, state, identityErr := r3ProcessIdentity(cmd.Process.Pid)
		f.nodes[i].identity = r3BrokerOutcome{Name: f.nodes[i].name, PID: cmd.Process.Pid, StartTicks: ticks, ExitCode: -1}
		if identityErr != nil || group != cmd.Process.Pid || state == "Z" || state == "X" {
			t.Fatal("owned broker start identity could not be verified")
		}
	}
	if !r3Wait(f.ctx, 20*time.Second, func() bool {
		for i := range f.nodes {
			var state struct {
				ServerName string `json:"server_name"`
				Version    string `json:"version"`
			}
			if f.monitor(i, "/varz", &state) != nil || state.ServerName != f.nodes[i].name || state.Version != release.version {
				return false
			}
		}
		return true
	}) {
		t.Fatal("three pinned brokers did not become available")
	}
	// Initialization waits for metadata quorum, but each individual attempt
	// retains its real two-second bound and new connection ownership.
	if !r3Wait(f.ctx, 20*time.Second, func() bool {
		client := f.client(t, 0, "initializer")
		attempt := r3Attempt(t)
		result, err := client.Initialize(f.ctx, attempt, r3Document(t, 1))
		r3CloseClient(t, client)
		if err == nil && result.Outcome() == OutcomeCreated {
			return true
		}
		if err == ErrConflict {
			return f.read(t, 0).Record().Document().Revision() == 1
		}
		return false
	}) {
		t.Fatal("R3 authority initialization failed")
	}
	if !r3Wait(f.ctx, 10*time.Second, func() bool { return f.assertRoutes() == nil }) {
		t.Fatal("route proxy coverage could not be proven")
	}
	t.Logf("R3_BROKER version=%s archive_sha256=%s binary_sha256=%s", release.version, release.digest, f.binarySHA)
	return f
}

func (f *r3Fixture) passwordEnv(role string) string {
	return r3Env + "_PASSWORD_" + strings.ToUpper(strings.ReplaceAll(role, "-", "_"))
}

func (f *r3Fixture) serverConfig(index int) map[string]any {
	users := []any{}
	for _, role := range []string{"initializer", "control", "gateway-a", "gateway-b"} {
		replica := role
		if role == "initializer" {
			replica = ControlReplicaID
		}
		publish := []string{streamInfoSubject, messageGetSubject, "auth.witness." + replica}
		if replica == ControlReplicaID {
			publish = append(publish, AuthoritySubject)
		}
		if role == "initializer" {
			publish = append(publish, streamCreateSubject)
		}
		users = append(users, map[string]any{"user": role, "password": f.passwords[role], "permissions": map[string]any{
			"publish": map[string]any{"allow": publish}, "subscribe": map[string]any{"allow": []string{"_INBOX.kelvo-authfence." + replica + ".*"}},
		}})
	}
	routes := []string{}
	for i, proxy := range f.routes {
		if i != index {
			routes = append(routes, "nats-route://"+proxy.listener.Addr().String())
		}
	}
	tlsOptions := map[string]any{"cert_file": f.cert, "key_file": f.key, "ca_file": f.ca, "verify": true, "min_version": "1.3", "timeout": 1}
	return map[string]any{
		"server_name": f.nodes[index].name, "listen": f.nodes[index].client, "http": f.nodes[index].monitor,
		"max_connections": 64, "max_subscriptions": 64, "max_payload": 32 << 10, "max_pending": 128 << 10, "write_deadline": "1s",
		"tls":       tlsOptions,
		"jetstream": map[string]any{"store_dir": filepath.Join(f.dir, fmt.Sprintf("state-%d", index)), "max_memory_store": 8 << 20, "max_file_store": 16 << 20},
		"accounts": map[string]any{
			"AUTH":  map[string]any{"jetstream": map[string]any{"max_memory": 8 << 20, "max_file": 8 << 20, "max_streams": 1, "max_consumers": 1}, "users": users},
			"OTHER": map[string]any{"users": []any{map[string]any{"user": "outsider", "password": f.passwords["outsider"], "permissions": map[string]any{"publish": map[string]any{"allow": []string{AuthoritySubject, "auth.witness.gateway-a", streamInfoSubject, messageGetSubject}}, "subscribe": map[string]any{"allow": []string{"_INBOX.kelvo-authfence.outsider.*"}}}}}},
		},
		"cluster": map[string]any{"name": "kelvo-authfence-r3", "listen": f.nodes[index].route, "advertise": f.routes[index].listener.Addr().String(), "routes": routes, "pool_size": 1, "tls": tlsOptions},
	}
}

type r3BoundedLog struct {
	mu      sync.Mutex
	file    *os.File
	written int
}

func (w *r3BoundedLog) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	if w.written < 1<<20 {
		part := p[:min(n, (1<<20)-w.written)]
		written, err := w.file.Write(part)
		w.written += written
		if err != nil {
			return written, err
		}
	}
	return n, nil
}

func r3Wait(ctx context.Context, budget time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(budget)
	for ctx.Err() == nil && time.Now().Before(deadline) {
		if fn() {
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

func (f *r3Fixture) monitor(node int, path string, target any) error {
	ctx, cancel := context.WithTimeout(f.ctx, 300*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "GET", "http://"+f.nodes[node].monitor+path, nil)
	if err != nil {
		return ErrUnavailable
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return ErrUnavailable
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, (128<<10)+1))
	if err != nil || response.StatusCode != http.StatusOK || len(body) > 128<<10 || json.Unmarshal(body, target) != nil {
		return ErrUnavailable
	}
	return nil
}

func (f *r3Fixture) config(node int, role string) Config {
	replica := role
	if role == "initializer" {
		replica = ControlReplicaID
	}
	return Config{URL: "tls://" + f.nodes[node].client, CAFile: f.ca, CertFile: f.cert, KeyFile: f.key, Username: role, PasswordEnv: f.passwordEnv(role), Scope: f.scope, ReplicaID: replica}
}

func (f *r3Fixture) client(t *testing.T, node int, role string) *Client {
	t.Helper()
	client, err := NewClient(f.config(node, role))
	if err != nil {
		t.Fatal("fixture client configuration rejected", err)
	}
	t.Cleanup(func() { r3CloseClient(t, client) })
	return client
}

func r3CloseClient(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Error("fixture client did not quiesce", err)
	}
	select {
	case <-client.Quiesced():
	default:
		t.Error("fixture client custody remains outstanding")
	}
}

func r3Attempt(t *testing.T) Attempt {
	t.Helper()
	started := time.Now()
	attempt, err := NewAttempt(started, started.Add(MaxAttemptDuration))
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}

func r3Document(t *testing.T, revision uint64) Document {
	t.Helper()
	digest := sha256.Sum256([]byte(fmt.Sprintf("kelvo-r3-public-fixture-%d", revision)))
	document, err := NewDocument(revision, hex.EncodeToString(digest[:]))
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func (f *r3Fixture) read(t *testing.T, node int) Snapshot {
	t.Helper()
	client := f.client(t, node, "control")
	defer r3CloseClient(t, client)
	snapshot, err := client.Read(f.ctx, r3Attempt(t))
	if err != nil || !snapshot.Valid() {
		t.Fatal("fixture authority read failed", err)
	}
	return snapshot
}

func (f *r3Fixture) connect(t *testing.T, node int, role string, handler nats.ErrHandler) *nats.Conn {
	t.Helper()
	options := []nats.Option{nats.Secure(f.tlsConfig.Clone()), nats.UserInfo(role, f.passwords[role]), nats.NoReconnect(), nats.ReconnectBufSize(-1), nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second), nats.ErrorHandler(handler)}
	connection, err := nats.Connect("tls://"+f.nodes[node].client, options...)
	if err != nil {
		t.Fatal("fixture direct TLS client connection failed")
	}
	t.Cleanup(connection.Close)
	return connection
}

func r3Request(connection *nats.Conn, replica, subject string, data []byte, headers nats.Header, budget time.Duration) (*nats.Msg, error) {
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	inbox := "_INBOX.kelvo-authfence." + replica + "." + hex.EncodeToString(nonce[:])
	sub, err := connection.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	if err = sub.SetPendingLimits(1, 128<<10); err != nil {
		return nil, err
	}
	if err = connection.PublishMsg(&nats.Msg{Subject: subject, Reply: inbox, Data: data, Header: headers}); err != nil {
		return nil, err
	}
	response, err := sub.NextMsg(budget)
	if err != nil || response == nil || response.Subject != inbox || response.Reply != "" || len(response.Data) > 128<<10 {
		return nil, ErrUnavailable
	}
	return response, nil
}

func (f *r3Fixture) sampleResources() error {
	missing := func() error { f.resourceReadErrors++; return ErrUnavailable }
	var total uint64
	fds := 0
	for _, node := range f.nodes {
		if node.cmd == nil || node.stopped {
			return missing()
		}
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", node.cmd.Process.Pid))
		if err != nil || len(data) > 64<<10 {
			return missing()
		}
		found := false
		for _, line := range strings.Split(string(data), "\n") {
			if fields := strings.Fields(line); len(fields) == 3 && fields[0] == "VmRSS:" {
				kb, err := strconv.ParseUint(fields[1], 10, 64)
				if found || err != nil || kb == 0 || fields[2] != "kB" {
					return missing()
				}
				total += kb * 1024
				found = true
			}
		}
		if !found {
			return missing()
		}
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", node.cmd.Process.Pid))
		if err != nil || len(entries) == 0 || len(entries) > 512 {
			return missing()
		}
		fds += len(entries)
	}
	f.peakRSS = max(f.peakRSS, total)
	f.peakFDs = max(f.peakFDs, fds)
	return nil
}

func r3ProcessIdentity(pid int) (string, int, string, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil || len(raw) > 8192 {
		return "", 0, "", ErrUnavailable
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return "", 0, "", ErrUnavailable
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) < 20 {
		return "", 0, "", ErrUnavailable
	}
	group, e1 := strconv.Atoi(fields[2])
	_, e2 := strconv.ParseUint(fields[19], 10, 64)
	if e1 != nil || e2 != nil || group < 0 {
		return "", 0, "", ErrUnavailable
	}
	return fields[19], group, fields[0], nil
}

func r3GroupAbsent(group int) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil || len(entries) > 65536 {
		return false
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		_, actual, _, err := r3ProcessIdentity(pid)
		if err == nil && actual == group {
			return false
		}
		if err != nil {
			// A vanished process is benign. A still-live unreadable process
			// leaves process-group absence unproven.
			if _, statErr := os.Stat("/proc/" + entry.Name()); !errors.Is(statErr, os.ErrNotExist) {
				return false
			}
		}
	}
	return true
}

func (f *r3Fixture) close() error {
	f.closeOnce.Do(func() {
		quiescent := true
		f.cancel()
		for _, proxy := range f.routes {
			if proxy != nil {
				proxy.stop()
			}
		}
		for i := range f.nodes {
			node := &f.nodes[i]
			if node.cmd == nil || node.stopped {
				continue
			}
			ticks, group, state, identityErr := r3ProcessIdentity(node.cmd.Process.Pid)
			node.identity.AliveBeforeStop = identityErr == nil && ticks == node.identity.StartTicks && group == node.cmd.Process.Pid && state != "Z" && state != "X"
			if !node.identity.AliveBeforeStop {
				f.closeErr = ErrUnknown
			} else if err := node.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				f.closeErr = ErrUnknown
			}
			var waitErr error
			select {
			case waitErr = <-node.done:
			case <-time.After(3 * time.Second):
				node.identity.ForcedKill = true
				f.closeErr = ErrUnknown
				_ = node.cmd.Process.Kill()
				select {
				case waitErr = <-node.done:
				case <-time.After(2 * time.Second):
					f.closeErr = ErrUnknown
					continue
				}
			}
			node.stopped = true
			node.identity.Reaped = true
			node.identity.WaitOutcome = "exit_zero"
			if node.cmd.ProcessState != nil {
				node.identity.ExitCode = node.cmd.ProcessState.ExitCode()
			} else {
				node.identity.WaitOutcome = "wait_error"
				f.closeErr = ErrUnknown
			}
			if waitErr != nil {
				node.identity.WaitOutcome = "wait_error"
				f.closeErr = ErrUnknown
				if node.cmd.ProcessState != nil {
					node.identity.WaitOutcome = "exit_nonzero"
					if node.identity.ExitCode < 0 {
						node.identity.WaitOutcome = "signal"
					}
				}
			}
			node.identity.GroupAbsent = r3GroupAbsent(node.cmd.Process.Pid)
			if !node.identity.GroupAbsent {
				f.closeErr = ErrUnknown
			}
		}
		for _, node := range f.nodes {
			if node.cmd != nil && (!node.identity.Reaped || !node.identity.GroupAbsent) {
				quiescent = false
			}
		}
		for _, proxy := range f.routes {
			if proxy != nil && !proxy.join(2*time.Second) {
				f.closeErr = ErrUnknown
				quiescent = false
			}
		}
		for _, node := range f.nodes {
			for _, address := range []string{node.client, node.route, node.monitor} {
				if address == "" {
					continue
				}
				connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
				if err == nil {
					_ = connection.Close()
					f.closeErr = ErrUnknown
					quiescent = false
				}
			}
		}
		if quiescent {
			removed := true
			for i := range f.nodes {
				if err := os.Remove(filepath.Join(f.dir, fmt.Sprintf("node-%d.conf", i))); err != nil && !errors.Is(err, os.ErrNotExist) {
					f.closeErr = ErrUnknown
					removed = false
				}
			}
			if f.key != "" {
				if err := os.Remove(f.key); err != nil && !errors.Is(err, os.ErrNotExist) {
					f.closeErr = ErrUnknown
					removed = false
				}
			}
			for role := range f.passwords {
				_ = os.Unsetenv(f.passwordEnv(role))
			}
			clear(f.passwords)
			f.credentialsRemoved = removed
		}
	})
	return f.closeErr
}

func r3OptIn(t *testing.T) {
	t.Helper()
	if os.Getenv(r3Env) != "1" {
		t.Skip("set KELVO_TEST_AUTHFENCE_R3=1 for the owned, pinned three-broker fixture")
	}
	if runtime.GOARCH != "amd64" {
		t.Fatal("pinned R3 fixture requires Linux amd64")
	}
	r3RequireCaps(t)
}
