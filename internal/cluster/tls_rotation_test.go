// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func identityPEM(t *testing.T, uri string, ca *x509.Certificate, caKey *rsa.PrivateKey, before, after time.Time, usages ...x509.ExtKeyUsage) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), DNSNames: []string{"gateway.test"}, URIs: []*url.URL{u}, NotBefore: before, NotAfter: after, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usages}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})...)
}

func validIdentityPEM(t *testing.T, uri string, ca *x509.Certificate, key *rsa.PrivateKey) []byte {
	return identityPEM(t, uri, ca, key, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth)
}

func publishTLSIdentity(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(path+".next", raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".next", path); err != nil {
		t.Fatal(err)
	}
}

func rotatingTLSFixture(t *testing.T, uri string, ca *x509.Certificate, key *rsa.PrivateKey) (TLSConfig, []byte) {
	t.Helper()
	dir := t.TempDir()
	config := TLSConfig{IdentityFile: filepath.Join(dir, "identity.pem"), CAFile: filepath.Join(dir, "ca.pem"), ReloadInterval: time.Second}
	if err := os.WriteFile(config.CAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	raw := validIdentityPEM(t, uri, ca, key)
	publishTLSIdentity(t, config.IdentityFile, raw)
	return config, raw
}

func waitTLS(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("TLS condition deadline exceeded")
}

func TestTLSIdentityRotationLiveMTLS(t *testing.T) {
	_, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	workerURI := WorkerIdentity("analytics", "a1")
	worker, workerRaw := rotatingTLSFixture(t, workerURI, ca, key)
	gateway, gatewayRaw := rotatingTLSFixture(t, GatewayIdentity, ca, key)
	serverTLS, err := OpenServerTLS(worker, workerURI, GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer serverTLS.Close()
	identity, err := newTLSIdentity(gateway, GatewayIdentity, x509.ExtKeyUsageClientAuth, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer identity.close()
	handlerCalls := atomic.Int64{}
	server := httptest.NewUnstartedServer(serverTLS.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalls.Add(1)
		w.Header().Set("Peer-Serial", r.TLS.PeerCertificates[0].SerialNumber.String())
		w.Header().Set("Peer-Connection", r.RemoteAddr)
		w.WriteHeader(http.StatusNoContent)
	})))
	server.TLS = serverTLS.Config
	server.StartTLS()
	defer server.Close()
	clientConfig, err := buildClientTLS(gateway, workerURI, identity)
	if err != nil {
		t.Fatal(err)
	}
	clientConfig.ServerName = "gateway.test"
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = clientConfig
	client := &http.Client{Transport: &tlsIdentityTransport{Transport: transport, identity: identity}, Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	request := func() (string, string, string) {
		t.Helper()
		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("status %d", response.StatusCode)
		}
		return response.TLS.PeerCertificates[0].SerialNumber.String(), response.Header.Get("Peer-Serial"), response.Header.Get("Peer-Connection")
	}
	serverSerial, clientSerial, _ := request()
	newWorker := validIdentityPEM(t, workerURI, ca, key)
	newGateway := validIdentityPEM(t, GatewayIdentity, ca, key)
	publishTLSIdentity(t, worker.IdentityFile, newWorker)
	publishTLSIdentity(t, gateway.IdentityFile, newGateway)
	waitTLS(t, func() bool {
		w, we := serverTLS.identity.current()
		g, ge := identity.current()
		return we == nil && ge == nil && w.Leaf.SerialNumber.String() != serverSerial && g.Leaf.SerialNumber.String() != clientSerial
	})
	client.CloseIdleConnections()
	nextServer, nextClient, connection := request()
	if nextServer == serverSerial || nextClient == clientSerial {
		t.Fatal("a new handshake retained the prior identity")
	}
	// The same connection is deliberately retained across invalidation.
	publishTLSIdentity(t, worker.IdentityFile, []byte("invalid identity\n"))
	waitTLS(t, func() bool { return !serverTLS.identity.ready() })
	calls := handlerCalls.Load()
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || handlerCalls.Load() != calls {
		t.Fatal("reused connection bypassed invalid-identity gate")
	}
	if connection == "" {
		t.Fatal("missing initial connection evidence")
	}
	client.CloseIdleConnections()
	if response, err = client.Get(server.URL); err == nil {
		response.Body.Close()
		t.Fatal("invalid identity permitted a new handshake")
	}
	publishTLSIdentity(t, worker.IdentityFile, workerRaw)
	waitTLS(t, serverTLS.identity.ready)
	request()
	// Invalid outbound identity must reject reuse before the remote handler runs.
	publishTLSIdentity(t, gateway.IdentityFile, []byte("invalid identity\n"))
	waitTLS(t, func() bool { return !identity.ready() })
	calls = handlerCalls.Load()
	if response, err = client.Get(server.URL); err == nil {
		response.Body.Close()
		t.Fatal("invalid client identity reused a connection")
	}
	if handlerCalls.Load() != calls {
		t.Fatal("invalid outbound request reached worker")
	}
	g := &Gateway{workerIdentity: identity, ctx: context.Background()}
	ready := httptest.NewRecorder()
	g.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatal("gateway remained ready with invalid worker credentials")
	}
	publishTLSIdentity(t, gateway.IdentityFile, gatewayRaw)
	waitTLS(t, identity.ready)
	client.CloseIdleConnections()
	request()
	// Certificate expiry must reject reuse even with successful file rereads.
	shortLived := identityPEM(t, workerURI, ca, key, time.Now().Add(-time.Hour), time.Now().Add(3*time.Second), x509.ExtKeyUsageServerAuth)
	shortCert, _, err := parseTLSIdentity(shortLived, workerURI, x509.ExtKeyUsageServerAuth, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	publishTLSIdentity(t, worker.IdentityFile, shortLived)
	waitTLS(t, func() bool {
		current, err := serverTLS.identity.current()
		return err == nil && current.Leaf.SerialNumber.Cmp(shortCert.Leaf.SerialNumber) == 0
	})
	client.CloseIdleConnections()
	request()
	waitTLS(t, func() bool { return !serverTLS.identity.ready() })
	response, err = client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("expired certificate permitted connection reuse")
	}
}

func TestTLSRotationPreservesPeerVerification(t *testing.T) {
	gatewayStatic, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	workerURI := WorkerIdentity("analytics", "a1")
	worker, _ := rotatingTLSFixture(t, workerURI, ca, key)
	runtime, err := OpenServerTLS(worker, workerURI, GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	server := httptest.NewUnstartedServer(runtime.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })))
	server.TLS = runtime.Config
	server.StartTLS()
	defer server.Close()
	for _, test := range []string{"wrong-worker-uri", "wrong-dns", "wrong-client-uri", "untrusted-client", "tls12", "missing-client"} {
		t.Run(test, func(t *testing.T) {
			peer := workerURI
			if test == "wrong-worker-uri" {
				peer = WorkerIdentity("other", "a1")
			}
			config, err := BuildClientTLS(gatewayStatic, peer)
			if err != nil {
				t.Fatal(err)
			}
			config.ServerName = "gateway.test"
			switch test {
			case "wrong-dns":
				config.ServerName = "wrong.test"
			case "tls12":
				config.MinVersion, config.MaxVersion = tls.VersionTLS12, tls.VersionTLS12
			case "missing-client":
				config.Certificates = nil
			case "wrong-client-uri", "untrusted-client":
				var foreign TLSConfig
				if test == "wrong-client-uri" {
					foreign, _, _ = tlsFiles(t, WorkerIdentity("other", "worker"), ca, key)
				} else {
					foreign, _, _ = tlsFiles(t, GatewayIdentity, nil, nil)
				}
				cert, err := loadIdentity(foreign)
				if err != nil {
					t.Fatal(err)
				}
				config.Certificates = []tls.Certificate{cert}
			}
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.Proxy, transport.TLSClientConfig = nil, config
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			defer client.CloseIdleConnections()
			if response, err := client.Get(server.URL); err == nil {
				response.Body.Close()
				t.Fatal("invalid peer accepted")
			}
		})
	}
}

func TestTLSIdentityRejectsMalformedExpiredAndWrongRole(t *testing.T) {
	_, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	good := validIdentityPEM(t, GatewayIdentity, ca, key)
	certificate, privateKey := pem.Decode(good)
	second := validIdentityPEM(t, GatewayIdentity, ca, key)
	_, otherKey := pem.Decode(second)
	cases := map[string][]byte{
		"mismatch":                append(pem.EncodeToMemory(certificate), otherKey...),
		"expired":                 identityPEM(t, GatewayIdentity, ca, key, time.Now().Add(-time.Hour), time.Now().Add(-time.Minute), x509.ExtKeyUsageClientAuth),
		"future":                  identityPEM(t, GatewayIdentity, ca, key, time.Now().Add(time.Hour), time.Now().Add(2*time.Hour), x509.ExtKeyUsageClientAuth),
		"wrong-role":              identityPEM(t, GatewayIdentity, ca, key, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), x509.ExtKeyUsageServerAuth),
		"wrong-uri":               validIdentityPEM(t, WorkerIdentity("other", "worker"), ca, key),
		"duplicate-key":           append(bytes.Clone(good), privateKey...),
		"missing-key":             pem.EncodeToMemory(certificate),
		"malformed-leading-block": append([]byte("-----BEGIN CERTIFICATE-----\nmalformed\n"), good...),
		"trailing-data":           append(bytes.Clone(good), []byte("secret garbage")...),
		"too-many-certificates":   append(bytes.Repeat(pem.EncodeToMemory(certificate), 17), privateKey...),
		"broken-chain":            append(bytes.Clone(good), pem.EncodeToMemory(certificate)...),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := parseTLSIdentity(raw, GatewayIdentity, x509.ExtKeyUsageClientAuth, time.Now()); err == nil {
				t.Fatal("invalid identity accepted")
			}
		})
	}
	for _, valid := range [][]byte{good, bytes.ReplaceAll(good, []byte("\n"), []byte("\r\n"))} {
		if _, _, err := parseTLSIdentity(valid, GatewayIdentity, x509.ExtKeyUsageClientAuth, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTLSIdentityPrivateFilesAndConfig(t *testing.T) {
	_, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	config, raw := rotatingTLSFixture(t, GatewayIdentity, ca, key)
	for _, mode := range []os.FileMode{0644, 0640} {
		if err := os.Chmod(config.IdentityFile, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := newTLSIdentity(config, GatewayIdentity, x509.ExtKeyUsageClientAuth, nil); err == nil {
			t.Fatalf("accepted mode %o", mode)
		}
	}
	publishTLSIdentity(t, config.IdentityFile, raw)
	link := filepath.Join(filepath.Dir(config.IdentityFile), "linked.pem")
	if err := os.Symlink(config.IdentityFile, link); err != nil {
		t.Fatal(err)
	}
	linked := config
	linked.IdentityFile = link
	if _, err := newTLSIdentity(linked, GatewayIdentity, x509.ExtKeyUsageClientAuth, nil); err == nil {
		t.Fatal("accepted symlink identity")
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(config.IdentityFile, link); err != nil {
		t.Fatal(err)
	}
	if _, err := newTLSIdentity(config, GatewayIdentity, x509.ExtKeyUsageClientAuth, nil); err == nil {
		t.Fatal("accepted hard-linked identity")
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	publishTLSIdentity(t, config.IdentityFile, bytes.Repeat([]byte("x"), (256<<10)+1))
	if _, err := newTLSIdentity(config, GatewayIdentity, x509.ExtKeyUsageClientAuth, nil); err == nil {
		t.Fatal("accepted oversized identity")
	}
	for _, config := range []TLSConfig{
		{ReloadInterval: time.Second},
		{IdentityFile: "/safe/identity.pem", CertFile: "cert.pem"},
		{IdentityFile: "/safe/identity.pem", KeyFile: "key.pem"},
		{IdentityFile: "/safe/identity.pem", ReloadInterval: time.Millisecond},
		{IdentityFile: "/safe/identity.pem", ReloadInterval: 2 * time.Hour},
	} {
		if validateTLSRotation(config) == nil {
			t.Fatal("invalid rotation config accepted")
		}
	}
	path := filepath.Join(t.TempDir(), "gateway.yml")
	yaml := strings.Replace(gatewayYAML, "tls: {cert_file: gateway.pem, key_file: gateway.key}", "tls: {identity_file: identity.pem, reload_interval: 2s}", 1)
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadGateway(path)
	if err != nil || loaded.TLS.IdentityFile != filepath.Join(filepath.Dir(path), "identity.pem") || loaded.TLS.ReloadInterval != 2*time.Second {
		t.Fatalf("rotation config resolution failed: %v", err)
	}
}

func TestTLSIdentityOneReaderRejectsLateResultAndCloses(t *testing.T) {
	_, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	raw := validIdentityPEM(t, GatewayIdentity, ca, key)
	cert, expiry, err := parseTLSIdentity(raw, GatewayIdentity, x509.ExtKeyUsageClientAuth, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, second := make(chan struct{}), make(chan struct{})
	defer close(second)
	var reads atomic.Int64
	identity := &tlsIdentity{config: TLSConfig{IdentityFile: "/unused", ReloadInterval: 20 * time.Millisecond}, ownURI: GatewayIdentity, usage: x509.ExtKeyUsageClientAuth, ctx: ctx, cancel: cancel, done: make(chan struct{}), read: func(context.Context, string, int) ([]byte, error) {
		if reads.Add(1) == 1 {
			<-first
		} else {
			<-second
		}
		return bytes.Clone(raw), nil
	}}
	if !identity.apply(tlsIdentityRead{certificate: cert, expires: expiry, started: time.Now()}) {
		t.Fatal("initial identity failed")
	}
	go identity.run()
	waitTLS(t, func() bool { return reads.Load() == 1 })
	waitTLS(t, func() bool { return !identity.ready() })
	if reads.Load() != 1 {
		t.Fatal("blocked read spawned another reader")
	}
	close(first)
	waitTLS(t, func() bool { return reads.Load() == 2 })
	if identity.ready() {
		t.Fatal("late reader renewed expired authority")
	}
	closed := make(chan struct{})
	go func() { identity.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close waited for a blocked filesystem reader")
	}
	if identity.ready() {
		t.Fatal("closed identity remained available")
	}
}

func TestTLSIdentityCertificateExpiryAndConcurrentSnapshots(t *testing.T) {
	_, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	config, raw := rotatingTLSFixture(t, GatewayIdentity, ca, key)
	identity, err := newTLSIdentity(config, GatewayIdentity, x509.ExtKeyUsageClientAuth, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer identity.close()
	cert, expires, err := parseTLSIdentity(raw, GatewayIdentity, x509.ExtKeyUsageClientAuth, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			identity.current()
		}
	}()
	for n := 0; n < 100; n++ {
		identity.invalidate()
		if !identity.apply(tlsIdentityRead{certificate: cert, expires: expires, started: time.Now()}) {
			t.Fatal("fresh identity rejected")
		}
	}
	stop()
	<-done
	identity.mu.Lock()
	identity.validUntil = time.Now().Add(-time.Second)
	identity.mu.Unlock()
	if identity.ready() {
		t.Fatal("request-time expiry ignored")
	}
	if identity.apply(tlsIdentityRead{certificate: cert, expires: time.Now().Add(-time.Second), started: time.Now()}) {
		t.Fatal("expired certificate accepted")
	}
}

type tlsDrainFixture struct{ drained bool }

func (f *tlsDrainFixture) ServeHTTP(http.ResponseWriter, *http.Request) {}
func (f *tlsDrainFixture) Drain(context.Context) error {
	f.drained = true
	return fmt.Errorf("drained")
}
func TestTLSIdentityHandlerPreservesDrain(t *testing.T) {
	fixture := &tlsDrainFixture{}
	wrapper := (&ServerTLS{identity: &tlsIdentity{}}).Handler(fixture)
	if err := wrapper.(interface{ Drain(context.Context) error }).Drain(context.Background()); err == nil || !fixture.drained {
		t.Fatal("TLS wrapper swallowed cluster drain")
	}
}
