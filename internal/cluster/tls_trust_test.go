// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func trustDoc(t *testing.T, epoch uint64, expires time.Time, roots ...*x509.Certificate) []byte {
	t.Helper()
	var rootsPEM bytes.Buffer
	for _, root := range roots {
		rootsPEM.Write(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: root.Raw}))
	}
	raw, err := yaml.Marshal(tlsTrustDocument{Version: 1, Epoch: epoch, IssuedAt: time.Now().Add(-time.Second), ExpiresAt: expires, RootsPEM: rootsPEM.String()})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func editTrustDoc(t *testing.T, raw []byte, edit func(*tlsTrustDocument)) []byte {
	t.Helper()
	var document tlsTrustDocument
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	edit(&document)
	result, err := yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func trustFixture(t *testing.T, raw []byte) *tlsTrust {
	t.Helper()
	config := TLSTrustConfig{File: filepath.Join(t.TempDir(), "trust.yml"), MinimumEpoch: 1, ReloadInterval: time.Second}
	publishTLSIdentity(t, config.File, raw)
	trust, err := newTLSTrust(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(trust.close)
	return trust
}
func waitTrustEpoch(t *testing.T, trust *tlsTrust, epoch uint64) {
	t.Helper()
	waitTLS(t, func() bool { s, err := trust.current(); return err == nil && s.epoch == epoch })
}
func trustClient(t *testing.T, c TLSConfig, identity *tlsIdentity, trust *tlsTrust, uri string) *http.Client {
	t.Helper()
	config, err := buildClientTLS(c, uri, identity)
	if err != nil {
		t.Fatal(err)
	}
	config.ServerName = "gateway.test"
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = config
	trusted, err := newTLSTrustTransport(transport, trust, identity, uri)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: trusted, Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	return client
}
func trustServer(t *testing.T, c TLSConfig, trust *tlsTrust, uri string, handler http.Handler) (*httptest.Server, *ServerTLS) {
	t.Helper()
	identity, err := OpenServerTLS(c, uri, GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(identity.Close)
	config, err := configureTLSTrustServer(identity.Config, trust, GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(&tlsTrustHandler{Handler: identity.Handler(handler), trust: trust, expectedURI: GatewayIdentity})
	server.TLS = config
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, identity
}
func trustGET(client *http.Client, url string) (int, error) {
	response, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, response.Body)
	return response.StatusCode, err
}
func requireTrustOK(t *testing.T, client *http.Client, url string) {
	t.Helper()
	status, err := trustGET(client, url)
	if err != nil || status != http.StatusNoContent {
		t.Fatalf("trusted request failed: status %d: %v", status, err)
	}
}

func TestTLSTrustTwoGatewayTwoWorkerCARollover(t *testing.T) {
	_, ca1, key1 := tlsFiles(t, GatewayIdentity, nil, nil)
	_, ca2, key2 := tlsFiles(t, GatewayIdentity, nil, nil)
	expires := time.Now().Add(20 * time.Minute)
	initial := trustDoc(t, 1, expires, ca1)
	type replica struct {
		config   TLSConfig
		trust    *tlsTrust
		identity *tlsIdentity
		uri      string
		url      string
		calls    *atomic.Int64
	}
	workers := make([]replica, 2)
	gateways := make([]replica, 2)
	clients := [2][2]*http.Client{}
	for n := range workers {
		uri := WorkerIdentity("tenant", []string{"one", "two"}[n])
		config, _ := rotatingTLSFixture(t, uri, ca1, key1)
		trust := trustFixture(t, initial)
		calls := &atomic.Int64{}
		server, identity := trustServer(t, config, trust, uri, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusNoContent) }))
		workers[n] = replica{config: config, trust: trust, identity: identity.identity, uri: uri, url: server.URL, calls: calls}
	}
	for n := range gateways {
		config, _ := rotatingTLSFixture(t, GatewayIdentity, ca1, key1)
		trust := trustFixture(t, initial)
		identity, err := newTLSIdentity(config, GatewayIdentity, x509.ExtKeyUsageClientAuth, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(identity.close)
		gateways[n] = replica{config: config, trust: trust, identity: identity, uri: GatewayIdentity}
		for w := range workers {
			clients[n][w] = trustClient(t, config, identity, trust, workers[w].uri)
		}
	}
	checkAll := func() {
		for g := range gateways {
			for w := range workers {
				requireTrustOK(t, clients[g][w], workers[w].url)
			}
		}
	}
	checkAll()
	all := append(append([]replica{}, workers...), gateways...)
	overlap := trustDoc(t, 2, expires, ca1, ca2)
	for _, replica := range all {
		publishTLSIdentity(t, replica.trust.config.File, overlap)
	}
	for _, replica := range all {
		waitTrustEpoch(t, replica.trust, 2)
	}
	for _, replica := range all {
		replacement := validIdentityPEM(t, replica.uri, ca2, key2)
		publishTLSIdentity(t, replica.config.IdentityFile, replacement)
		parsed, _, err := parseTLSIdentity(replacement, replica.uri, x509.ExtKeyUsageClientAuth, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		waitTLS(t, func() bool {
			current, err := replica.identity.current()
			return err == nil && bytes.Equal(current.Certificate[0], parsed.Certificate[0])
		})
	}
	checkAll()
	retired := trustDoc(t, 3, expires, ca2)
	for _, replica := range all {
		publishTLSIdentity(t, replica.trust.config.File, retired)
	}
	for _, replica := range all {
		waitTrustEpoch(t, replica.trust, 3)
	}
	checkAll()
	// A certificate from the removed CA cannot start a new authenticated request.
	oldGateway, _, _ := tlsFiles(t, GatewayIdentity, ca1, key1)
	oldClient := trustClient(t, oldGateway, nil, gateways[0].trust, workers[0].uri)
	before := workers[0].calls.Load()
	if status, err := trustGET(oldClient, workers[0].url); err == nil && status == http.StatusNoContent {
		t.Fatal("retired issuer accepted")
	}
	if workers[0].calls.Load() != before {
		t.Fatal("retired issuer reached handler")
	}
	// Revoke one worker certificate after its pool has an established connection.
	workerCert, err := workers[0].identity.current()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(workerCert.Certificate[0])
	revoked := editTrustDoc(t, retired, func(d *tlsTrustDocument) { d.Epoch = 4; d.RevokedCertificates = []string{hex.EncodeToString(sum[:])} })
	publishTLSIdentity(t, gateways[0].trust.config.File, revoked)
	waitTrustEpoch(t, gateways[0].trust, 4)
	before = workers[0].calls.Load()
	if status, err := trustGET(clients[0][0], workers[0].url); err == nil && status == http.StatusNoContent {
		t.Fatal("revoked worker accepted on reused pool")
	}
	if workers[0].calls.Load() != before {
		t.Fatal("revoked worker received request")
	}
	requireTrustOK(t, clients[0][1], workers[1].url)
	requireTrustOK(t, clients[1][0], workers[0].url)
	// Revoke the gateway role on one worker. The other worker remains available.
	revokedGateway := editTrustDoc(t, retired, func(d *tlsTrustDocument) { d.Epoch = 4; d.RevokedIdentities = []string{GatewayIdentity} })
	publishTLSIdentity(t, workers[0].trust.config.File, revokedGateway)
	waitTrustEpoch(t, workers[0].trust, 4)
	before = workers[0].calls.Load()
	if status, err := trustGET(clients[1][0], workers[0].url); err == nil && status == http.StatusNoContent {
		t.Fatal("revoked gateway accepted on reused session")
	}
	if workers[0].calls.Load() != before {
		t.Fatal("revoked gateway reached handler")
	}
	requireTrustOK(t, clients[1][1], workers[1].url)
}

func TestTLSTrustStaleReplicaExpiryAndRecovery(t *testing.T) {
	gateway, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	uri := WorkerIdentity("tenant", "one")
	worker, _, _ := tlsFiles(t, uri, ca, key)
	initial := trustDoc(t, 1, time.Now().Add(2500*time.Millisecond), ca)
	stale, fresh := trustFixture(t, initial), trustFixture(t, initial)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	staleServer, _ := trustServer(t, worker, stale, uri, handler)
	freshServer, _ := trustServer(t, worker, fresh, uri, handler)
	clientTrust := trustFixture(t, trustDoc(t, 2, time.Now().Add(20*time.Minute), ca))
	client := trustClient(t, gateway, nil, clientTrust, uri)
	requireTrustOK(t, client, staleServer.URL)
	requireTrustOK(t, client, freshServer.URL)
	updated := trustDoc(t, 2, time.Now().Add(20*time.Minute), ca)
	publishTLSIdentity(t, fresh.config.File, updated)
	waitTrustEpoch(t, fresh, 2)
	waitTLS(t, func() bool { return !stale.ready() })
	if status, err := trustGET(client, staleServer.URL); err == nil && status == http.StatusNoContent {
		t.Fatal("stale replica retained authority past trust expiry")
	}
	requireTrustOK(t, client, freshServer.URL)
	publishTLSIdentity(t, stale.config.File, updated)
	waitTrustEpoch(t, stale, 2)
	requireTrustOK(t, client, staleServer.URL)
}

func TestTLSTrustParserAndEpochFloors(t *testing.T) {
	_, ca, _ := tlsFiles(t, GatewayIdentity, nil, nil)
	now := time.Now()
	valid := trustDoc(t, 5, now.Add(20*time.Minute), ca)
	fixtures := map[string][]byte{
		"unknown":          append(bytes.Clone(valid), []byte("unknown: true\n")...),
		"duplicate":        append(bytes.Clone(valid), []byte("epoch: 5\n")...),
		"documents":        append(bytes.Clone(valid), []byte("---\nversion: 1\n")...),
		"version":          editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.Version = 2 }),
		"zero epoch":       editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.Epoch = 0 }),
		"future":           editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.IssuedAt = now.Add(time.Minute) }),
		"expired":          editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.ExpiresAt = now.Add(-time.Second) }),
		"long validity":    editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.ExpiresAt = now.Add(2 * time.Hour) }),
		"empty roots":      editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.RootsPEM = "" }),
		"non PEM":          editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.RootsPEM += "bad" }),
		"duplicate root":   editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.RootsPEM += d.RootsPEM }),
		"malformed prefix": editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.RootsPEM = "-----BEGIN CERTIFICATE-----\nBAD\n" + d.RootsPEM }),
		"bad URI":          editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.RevokedIdentities = []string{"spiffe://other/gateway"} }),
		"duplicate URI":    editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.RevokedIdentities = []string{GatewayIdentity, GatewayIdentity} }),
		"bad digest":       editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.RevokedCertificates = []string{strings.Repeat("A", 64)} }),
		"alias":            append(bytes.Clone(valid), []byte("revoked_identities: &ids [spiffe://kelvo/gateway]\nrevoked_certificates: *ids\n")...),
	}
	for name, raw := range fixtures {
		t.Run(name, func(t *testing.T) {
			if _, err := parseTLSTrust(raw, time.Now()); err == nil {
				t.Fatal("accepted invalid trust")
			}
		})
	}
	trust := &tlsTrust{config: TLSTrustConfig{MinimumEpoch: 5, ReloadInterval: time.Second}}
	apply := func(raw []byte) bool {
		snapshot, err := parseTLSTrust(raw, time.Now())
		return trust.apply(tlsTrustRead{snapshot: snapshot, err: err, started: time.Now()})
	}
	if !apply(valid) || !apply(valid) {
		t.Fatal("identical epoch could not renew bounded lease")
	}
	mutation := append(bytes.Clone(valid), '\n')
	if apply(mutation) || trust.ready() {
		t.Fatal("same-epoch mutation did not fail closed")
	}
	if !apply(valid) {
		t.Fatal("exact same epoch recovery failed")
	}
	lower := editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.Epoch = 4 })
	if apply(lower) || trust.ready() {
		t.Fatal("lower epoch accepted")
	}
	higher := editTrustDoc(t, valid, func(d *tlsTrustDocument) { d.Epoch = 6 })
	if !apply(higher) {
		t.Fatal("higher epoch rejected")
	}
	if apply(valid) {
		t.Fatal("process rollback floor lost")
	}
	restarted := &tlsTrust{config: TLSTrustConfig{MinimumEpoch: 6, ReloadInterval: time.Second}}
	snapshot, err := parseTLSTrust(valid, time.Now())
	if restarted.apply(tlsTrustRead{snapshot: snapshot, err: err, started: time.Now()}) {
		t.Fatal("configured restart floor ignored")
	}
}

func TestTLSTrustBlockedReaderBoundAndConcurrentSnapshots(t *testing.T) {
	_, ca, _ := tlsFiles(t, GatewayIdentity, nil, nil)
	raw := trustDoc(t, 1, time.Now().Add(20*time.Minute), ca)
	snapshot, err := parseTLSTrust(raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, second := make(chan struct{}), make(chan struct{})
	defer close(second)
	var reads atomic.Int64
	trust := &tlsTrust{config: TLSTrustConfig{MinimumEpoch: 1, ReloadInterval: 20 * time.Millisecond}, ctx: ctx, cancel: cancel, done: make(chan struct{}), read: func(context.Context, string, int) ([]byte, error) {
		if reads.Add(1) == 1 {
			<-first
		} else {
			<-second
		}
		return bytes.Clone(raw), nil
	}}
	if !trust.apply(tlsTrustRead{snapshot: snapshot, started: time.Now()}) {
		t.Fatal("initial state failed")
	}
	go trust.run()
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 200 {
				trust.current()
			}
		}()
	}
	readers.Wait()
	waitTLS(t, func() bool { return reads.Load() == 1 })
	waitTLS(t, func() bool { return !trust.ready() })
	if reads.Load() != 1 {
		t.Fatal("blocked read spawned more readers")
	}
	close(first)
	waitTLS(t, func() bool { return reads.Load() == 2 })
	if trust.ready() {
		t.Fatal("late result renewed expired trust")
	}
	closed := make(chan struct{})
	go func() { trust.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close waited for filesystem syscall")
	}
}

func TestTLSTrustActiveRequestFinishesAndNewRequestsFail(t *testing.T) {
	gateway, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	uri := WorkerIdentity("tenant", "one")
	worker, _, _ := tlsFiles(t, uri, ca, key)
	raw := trustDoc(t, 1, time.Now().Add(20*time.Minute), ca)
	serverTrust, clientTrust := trustFixture(t, raw), trustFixture(t, raw)
	entered, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	server, _ := trustServer(t, worker, serverTrust, uri, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		<-finish
		w.WriteHeader(http.StatusNoContent)
	}))
	client := trustClient(t, gateway, nil, clientTrust, uri)
	result := make(chan error, 1)
	go func() {
		status, err := trustGET(client, server.URL)
		if err == nil && status != http.StatusNoContent {
			err = errors.New("active request failed")
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request was not admitted")
	}
	serverRevocation := editTrustDoc(t, raw, func(d *tlsTrustDocument) { d.Epoch = 2; d.RevokedIdentities = []string{GatewayIdentity} })
	clientRevocation := editTrustDoc(t, raw, func(d *tlsTrustDocument) { d.Epoch = 2; d.RevokedIdentities = []string{uri} })
	publishTLSIdentity(t, serverTrust.config.File, serverRevocation)
	publishTLSIdentity(t, clientTrust.config.File, clientRevocation)
	waitTrustEpoch(t, serverTrust, 2)
	waitTrustEpoch(t, clientTrust, 2)
	if status, err := trustGET(client, server.URL); err == nil && status == http.StatusNoContent {
		t.Fatal("new request accepted after revocation")
	}
	close(finish)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestTLSTrustOverlapPoolLimitAndBodyOwnership(t *testing.T) {
	gateway, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	uri := WorkerIdentity("tenant", "one")
	worker, _, _ := tlsFiles(t, uri, ca, key)
	raw := trustDoc(t, 1, time.Now().Add(20*time.Minute), ca)
	serverTrust, clientTrust := trustFixture(t, raw), trustFixture(t, raw)
	server, _ := trustServer(t, worker, serverTrust, uri, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/free" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("held"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	client := trustClient(t, gateway, nil, clientTrust, uri)
	transport := client.Transport.(*tlsTrustTransport)
	var bodies []io.ReadCloser
	defer func() {
		for _, body := range bodies {
			body.Close()
		}
	}()
	for n := range maxTLSTrustPools {
		if n > 0 {
			raw = editTrustDoc(t, raw, func(d *tlsTrustDocument) { d.Epoch++ })
			publishTLSIdentity(t, clientTrust.config.File, raw)
			waitTrustEpoch(t, clientTrust, uint64(n+1))
		}
		response, err := client.Get(server.URL + "/held")
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, response.Body)
	}
	raw = editTrustDoc(t, raw, func(d *tlsTrustDocument) { d.Epoch++ })
	publishTLSIdentity(t, clientTrust.config.File, raw)
	waitTrustEpoch(t, clientTrust, 5)
	if _, err := client.Get(server.URL + "/free"); !errors.Is(err, errTLSTrustUnavailable) {
		t.Fatalf("overlap bound did not reject new pool: %v", err)
	}
	transport.mu.Lock()
	count := len(transport.pools)
	transport.mu.Unlock()
	if count != maxTLSTrustPools {
		t.Fatalf("pool count %d", count)
	}
	bodies[0].Close()
	requireTrustOK(t, client, server.URL+"/free")
	for _, body := range bodies {
		body.Close()
	}
	transport.mu.Lock()
	count = len(transport.pools)
	transport.mu.Unlock()
	if count != 1 {
		t.Fatalf("retired pools were retained: %d", count)
	}
	releases := 0
	body := &tlsTrustBody{ReadCloser: trustErrorBody{}, release: func() { releases++ }}
	if _, err := body.Read(make([]byte, 1)); err != io.ErrUnexpectedEOF || releases != 0 {
		t.Fatal("non-EOF failure released body ownership")
	}
	body.Close()
	body.Close()
	if releases != 1 {
		t.Fatal("body lease was not released exactly once")
	}
	if tlsTrustChainDigest([][]byte{{1}, {2, 3}}) == tlsTrustChainDigest([][]byte{{1, 2}, {3}}) {
		t.Fatal("chain digest lacks unambiguous framing")
	}
}

type trustErrorBody struct{}

func (trustErrorBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (trustErrorBody) Close() error             { return nil }

func TestTLSTrustPreservesDNSIPAndIdentityVerification(t *testing.T) {
	gateway, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	uri := WorkerIdentity("tenant", "one")
	worker, _, _ := tlsFiles(t, uri, ca, key)
	raw := trustDoc(t, 1, time.Now().Add(20*time.Minute), ca)
	serverTrust, clientTrust := trustFixture(t, raw), trustFixture(t, raw)
	server, _ := trustServer(t, worker, serverTrust, uri, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	client := trustClient(t, gateway, nil, clientTrust, uri)
	requireTrustOK(t, client, server.URL)
	requireTrustOK(t, client, server.URL)
	client.Transport.(*tlsTrustTransport).mu.Lock()
	for _, state := range client.Transport.(*tlsTrustTransport).active.peers {
		if state.ServerName != "gateway.test" {
			t.Errorf("unexpected DNS peer name %q", state.ServerName)
		}
	}
	client.Transport.(*tlsTrustTransport).mu.Unlock()
	for name, configure := range map[string]func(*tls.Config){
		"wrong DNS":      func(c *tls.Config) { c.ServerName = "wrong.test" },
		"IP without SAN": func(c *tls.Config) { c.ServerName = "" },
		"TLS 1.2":        func(c *tls.Config) { c.MaxVersion = tls.VersionTLS12 },
	} {
		t.Run(name, func(t *testing.T) {
			config, err := BuildClientTLS(gateway, uri)
			if err != nil {
				t.Fatal(err)
			}
			configure(config)
			base := http.DefaultTransport.(*http.Transport).Clone()
			base.Proxy = nil
			base.TLSClientConfig = config
			transport, err := newTLSTrustTransport(base, clientTrust, nil, uri)
			if err != nil {
				t.Fatal(err)
			}
			defer transport.CloseIdleConnections()
			if status, err := trustGET(&http.Client{Transport: transport, Timeout: 3 * time.Second}, server.URL); err == nil && status == http.StatusNoContent {
				t.Fatal("normal TLS verification was bypassed")
			}
		})
	}
	// An IP SAN is verified without relying on SNI, including connection reuse.
	identityKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	spiffe, _ := url.Parse(uri)
	certificate := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{spiffe}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, ca, &identityKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(identityKey)
	if err != nil {
		t.Fatal(err)
	}
	ipConfig, _ := rotatingTLSFixture(t, uri, ca, key)
	publishTLSIdentity(t, ipConfig.IdentityFile, append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})...))
	ipServer, _ := trustServer(t, ipConfig, serverTrust, uri, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	config, err := BuildClientTLS(gateway, uri)
	if err != nil {
		t.Fatal(err)
	}
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.Proxy = nil
	base.TLSClientConfig = config
	transport, err := newTLSTrustTransport(base, clientTrust, nil, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseIdleConnections()
	ipClient := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	requireTrustOK(t, ipClient, ipServer.URL)
	requireTrustOK(t, ipClient, ipServer.URL)
	transport.mu.Lock()
	if transport.active.hostname != "127.0.0.1" {
		t.Errorf("IP verification name lost: %q", transport.active.hostname)
	}
	for _, state := range transport.active.peers {
		if state.ServerName != "" {
			t.Errorf("IP connection unexpectedly used SNI: %q", state.ServerName)
		}
	}
	transport.mu.Unlock()
}

func TestTLSTrustRevokesAcceptedCertificateChain(t *testing.T) {
	gateway, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	uri := WorkerIdentity("tenant", "one")
	worker, _, _ := tlsFiles(t, uri, ca, key)
	_, otherCA, _ := tlsFiles(t, GatewayIdentity, nil, nil)
	raw := trustDoc(t, 1, time.Now().Add(20*time.Minute), ca, otherCA)
	serverTrust, clientTrust := trustFixture(t, raw), trustFixture(t, raw)
	server, _ := trustServer(t, worker, serverTrust, uri, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	client := trustClient(t, gateway, nil, clientTrust, uri)
	requireTrustOK(t, client, server.URL)
	transport := client.Transport.(*tlsTrustTransport)
	transport.mu.Lock()
	var peer tls.ConnectionState
	for _, state := range transport.active.peers {
		peer = state
	}
	transport.mu.Unlock()
	otherHash := sha256.Sum256(otherCA.Raw)
	rootHash := sha256.Sum256(ca.Raw)
	for name, hash := range map[string][32]byte{"unused root": otherHash, "accepted root": rootHash} {
		t.Run(name, func(t *testing.T) {
			changed := editTrustDoc(t, raw, func(d *tlsTrustDocument) { d.Epoch = 2; d.RevokedCertificates = []string{hex.EncodeToString(hash[:])} })
			snapshot, err := parseTLSTrust(changed, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			_, err = snapshot.verifyPeer(peer, x509.ExtKeyUsageServerAuth, "gateway.test", uri)
			if name == "unused root" && err != nil {
				t.Fatal("unrelated revoked root denied accepted chain")
			}
			if name == "accepted root" && err == nil {
				t.Fatal("accepted root revocation was ignored")
			}
		})
	}
	// Authority changes retire pools even when both servers use the same CA/URI.
	second, _ := trustServer(t, worker, serverTrust, uri, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	transport.mu.Lock()
	previous := transport.active
	transport.mu.Unlock()
	requireTrustOK(t, client, second.URL)
	transport.mu.Lock()
	if transport.active == previous || !previous.retired || len(transport.pools) != 1 {
		t.Error("authority change did not retire prior pool")
	}
	transport.mu.Unlock()
	request, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/", nil)
	if err != nil {
		t.Fatal(err)
	}
	closed := &trustCloseBody{}
	request.Body = closed
	if _, err := transport.RoundTrip(request); err == nil || !closed.closed {
		t.Fatal("non-TLS admission was not rejected and closed")
	}
}

type trustCloseBody struct{ closed bool }

func (*trustCloseBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *trustCloseBody) Close() error           { b.closed = true; return nil }

func TestTLSTrustExpiredPeerCannotReuseOutboundConnection(t *testing.T) {
	gateway, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	uri := WorkerIdentity("tenant", "one")
	short := identityPEM(t, uri, ca, key, time.Now().Add(-time.Minute), time.Now().Add(3*time.Second), x509.ExtKeyUsageServerAuth)
	certificate, err := tls.X509KeyPair(short, short)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	var calls atomic.Int64
	var remote atomic.Value
	// The fixture deliberately has no HTTP trust middleware: only the client's
	// admission check can prevent reuse after this server certificate expires.
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		remote.Store(r.RemoteAddr)
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	t.Cleanup(server.Close)
	trust := trustFixture(t, trustDoc(t, 1, time.Now().Add(20*time.Minute), ca))
	client := trustClient(t, gateway, nil, trust, uri)
	requireTrustOK(t, client, server.URL)
	connection := remote.Load()
	requireTrustOK(t, client, server.URL)
	if remote.Load() != connection {
		t.Fatal("fixture did not establish a reusable connection")
	}
	waitTLS(t, func() bool { return !time.Now().Before(leaf.NotAfter) })
	before := calls.Load()
	if status, err := trustGET(client, server.URL); err == nil && status == http.StatusNoContent {
		t.Fatal("expired peer was reused")
	}
	if calls.Load() != before {
		t.Fatal("expired peer received a new request")
	}
}
