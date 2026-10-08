// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
)

type issueFunc func(context.Context, IssueRequest) (string, error)

func (f issueFunc) Issue(ctx context.Context, request IssueRequest) (string, error) {
	return f(ctx, request)
}

type fixture struct {
	config Config
	key    ed25519.PrivateKey
	server *tls.Config
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	public, signing, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caPublic, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPublic, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	identity, _ := url.Parse("spiffe://kelvo.test/worker/test")
	certificate := func(serial int64, client bool) ([]byte, []byte, tls.Certificate) {
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(30 * time.Minute), KeyUsage: x509.KeyUsageDigitalSignature}
		if client {
			leaf.URIs = []*url.URL{identity}
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		} else {
			leaf.DNSNames = []string{"proxy.test"}
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return certPEM, keyPEM, pair
	}
	clientPEM, clientKey, _ := certificate(2, true)
	_, _, serverPair := certificate(3, false)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	return fixture{config: Config{ProxyAddress: "127.0.0.1:1", ProxyServerName: "proxy.test", RootCAPEM: caPEM,
		ClientCertificatePEM: clientPEM, ClientKeyPEM: clientKey, WorkerIdentity: identity.String(),
		Issuer: "application", Audience: "private-database", ClusterTenant: "cluster", ServicePrincipal: "gateway", IssuerPublicKey: public},
		key: signing, server: &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverPair},
			ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, NextProtos: []string{"http/1.1"}}}
}

func testRequest() transportbroker.OpenRequest {
	return transportbroker.OpenRequest{ID: strings.Repeat("a", 64), Purpose: transportbroker.Data,
		Binding: transportbroker.Binding{Issuer: "application", Audience: "private-database", ClusterTenant: "cluster", ServicePrincipal: "gateway",
			Tenant: "tenant", Source: "database", SourceRevision: strings.Repeat("b", 64), Authority: "database.customer.internal:5432",
			Execution: transportbroker.Execution{Kind: "query", ID: "query", GrantSHA256: strings.Repeat("c", 64), Worker: "worker", Owner: strings.Repeat("d", 32), Claim: strings.Repeat("e", 32)},
			ExpiresAt: time.Now().Add(10 * time.Minute)}}
}

func claimsFor(request IssueRequest) ticketClaims {
	b, e := request.Binding, request.Binding.Execution
	now := time.Now().Unix()
	return ticketClaims{Version: 1, Issuer: b.Issuer, Audience: b.Audience, ID: request.OpenID, IssuedAt: now, ExpiresAt: now + 30, SessionExpiresAt: now + 300,
		ClusterTenant: b.ClusterTenant, ServicePrincipal: b.ServicePrincipal, Tenant: b.Tenant, Source: b.Source, SourceRevision: b.SourceRevision,
		TokenID: "source-token", TokenGeneration: strings.Repeat("1", 64), TunnelID: strings.Repeat("2", 64), ControlOwner: strings.Repeat("3", 64), Authority: b.Authority,
		WorkerIdentity: request.WorkerIdentity, WorkerCertSHA256: request.WorkerCertSHA256,
		Execution: ticketExecution{e.Kind, e.ID, e.GrantSHA256, e.Worker, e.Owner, e.Claim}}
}

func signPayload(key ed25519.PrivateKey, header, payload []byte) string {
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(unsigned)))
}
func signTicket(key ed25519.PrivateKey, claims ticketClaims) string {
	header, _ := json.Marshal(ticketHeader{"EdDSA", "rabbit-connect+jwt", claims.Issuer})
	payload, _ := json.Marshal(claims)
	return signPayload(key, header, payload)
}
func (f fixture) issuer() Issuer {
	return issueFunc(func(_ context.Context, request IssueRequest) (string, error) {
		return signTicket(f.key, claimsFor(request)), nil
	})
}
func issueFor(o *Opener, r transportbroker.OpenRequest) IssueRequest {
	return IssueRequest{Binding: r.Binding, OpenID: r.ID, WorkerIdentity: o.identity, WorkerCertSHA256: o.certDigest}
}

func serveOnce(t *testing.T, config *tls.Config, handler func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if config != nil {
		listener = tls.NewListener(listener, config)
	}
	var mu sync.Mutex
	var active net.Conn
	closed := false
	done := make(chan struct{})
	closeRaw := func(conn net.Conn) {
		if secure, ok := conn.(*tls.Conn); ok {
			_ = secure.NetConn().Close()
		} else {
			_ = conn.Close()
		}
	}
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		mu.Lock()
		if closed {
			mu.Unlock()
			closeRaw(conn)
			return
		}
		active = conn
		mu.Unlock()
		defer closeRaw(conn)
		_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
		handler(conn)
	}()
	t.Cleanup(func() {
		mu.Lock()
		closed = true
		conn := active
		mu.Unlock()
		_ = listener.Close()
		if conn != nil {
			closeRaw(conn)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fixture server did not join")
		}
	})
	return listener.Addr().String()
}

func readCONNECT(conn net.Conn) (string, error) {
	reader := bufio.NewReaderSize(conn, 16<<10)
	var request strings.Builder
	for range 5 {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		request.WriteString(line)
		if line == "\r\n" {
			return request.String(), nil
		}
	}
	return "", fmt.Errorf("fixture received an unexpected request")
}
