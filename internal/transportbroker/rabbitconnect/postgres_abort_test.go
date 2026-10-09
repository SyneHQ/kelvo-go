// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

type abortFixtureIssuer struct {
	Issuer
	issue func(context.Context, transportissuer.CleanupRequest) (string, error)
}

func (i abortFixtureIssuer) IssueCleanup(ctx context.Context, r transportissuer.CleanupRequest) (string, error) {
	return i.issue(ctx, r)
}

func TestParentPostgresAbortPreservesTLSAndUsesOneTypedTarget(t *testing.T) {
	for _, size := range []int{4, 32, 256} {
		t.Run(string(rune('A'+size%26)), func(t *testing.T) {
			f := newFixture(t)
			var calls atomic.Int32
			packetSeen := make(chan []byte, 1)
			f.config.ProxyAddress = serveOnce(t, f.server, func(conn net.Conn) {
				request, err := readCONNECT(conn)
				if err != nil {
					t.Error(err)
					return
				}
				if !strings.Contains(request, "Rabbit-Postgres-Abort: required-v1\r\n") || strings.Contains(request, "Rabbit-Accepted-Open:") {
					t.Error("abort changed wire purpose")
					return
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
				ssl := make([]byte, 8)
				if _, err := io.ReadFull(conn, ssl); err != nil || !bytes.Equal(ssl, []byte{0, 0, 0, 8, 4, 210, 22, 47}) {
					t.Error("source TLS negotiation missing")
					return
				}
				_, _ = conn.Write([]byte{'S'})
				sourceTLS := f.server.Clone()
				sourceTLS.ClientAuth = tls.NoClientCert
				secure := tls.Server(conn, sourceTLS)
				defer secure.Close()
				if err := secure.Handshake(); err != nil {
					t.Error(err)
					return
				}
				packet := make([]byte, 12+size)
				if _, err := io.ReadFull(secure, packet); err != nil {
					t.Error(err)
					return
				}
				packetSeen <- packet
			})
			issuer := abortFixtureIssuer{Issuer: f.issuer()}
			o, err := New(f.config, issuer)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().Unix()
			accepted := transportissuer.AcceptedOpenClaims{Version: 1, DataTicketSHA256: strings.Repeat("a", 64), AcceptanceID: strings.Repeat("b", 32), AcceptedAt: now, ExpiresAt: now + 30}
			issuer.issue = func(ctx context.Context, r transportissuer.CleanupRequest) (string, error) {
				calls.Add(1)
				return transportissuer.SignPostgresAbort(transportissuer.PostgresAbortClaims{Version: 1, Issuer: o.issuer, Audience: o.audience, ID: r.OpenID, IssuedAt: now, ExpiresAt: now + 5, DataTicketSHA256: accepted.DataTicketSHA256, AcceptanceID: accepted.AcceptanceID, WorkerIdentity: o.identity, WorkerCertSHA256: o.certDigest, CancellationStartedAt: now, Protocol: transportissuer.PostgresCancel}, f.key)
			}
			o.tickets = issuer
			data, peer := net.Pipe()
			defer peer.Close()
			c := &AcceptedConn{tunnelConn: &tunnelConn{conn: data, raw: data}, opener: o, binding: testRequest().Binding, acceptance: accepted, receipt: "fixture-receipt"}
			defer c.Close()
			secret := bytes.Repeat([]byte{7}, size)
			target := PostgresTarget{PID: 123, Secret: secret, TLS: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "proxy.test", RootCAs: o.tls.RootCAs}}
			if err := c.AbortPostgres(context.Background(), target); err != nil {
				t.Fatal("typed TLS abort failed", err)
			}
			packet := <-packetSeen
			if binary.BigEndian.Uint32(packet[:4]) != uint32(12+size) || binary.BigEndian.Uint32(packet[4:8]) != 80877102 || binary.BigEndian.Uint32(packet[8:12]) != 123 || !bytes.Equal(packet[12:], secret) {
				t.Fatal("typed abort target changed")
			}
			if err := c.AbortPostgres(context.Background(), target); err == nil || calls.Load() != 1 {
				t.Fatal("abort authority replayed")
			}
		})
	}
}
func TestParentPostgresAbortRejectsUnverifiedTLSBeforeIssuance(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	issuer := abortFixtureIssuer{Issuer: f.issuer(), issue: func(context.Context, transportissuer.CleanupRequest) (string, error) { calls.Add(1); return "", nil }}
	o, _ := New(f.config, issuer)
	data, peer := net.Pipe()
	defer peer.Close()
	c := &AcceptedConn{tunnelConn: &tunnelConn{conn: data, raw: data}, opener: o}
	defer c.Close()
	for _, config := range []*tls.Config{nil, {MinVersion: tls.VersionTLS12, InsecureSkipVerify: true, ServerName: "source"}, {MinVersion: tls.VersionTLS12}} {
		if err := c.AbortPostgres(context.Background(), PostgresTarget{PID: 1, Secret: []byte{1, 2, 3, 4}, TLS: config}); err == nil {
			t.Fatal("unverified source TLS allowed")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid target consumed authority")
	}
}

func TestFailedAbortConnectRetainsUncertainAuxiliaryClose(t *testing.T) {
	f := newFixture(t)
	issuer := abortFixtureIssuer{Issuer: f.issuer()}
	o, _ := New(f.config, issuer)
	now := time.Now().Unix()
	a := transportissuer.AcceptedOpenClaims{Version: 1, DataTicketSHA256: strings.Repeat("a", 64), AcceptanceID: strings.Repeat("b", 32), AcceptedAt: now, ExpiresAt: now + 30}
	issuer.issue = func(ctx context.Context, r transportissuer.CleanupRequest) (string, error) {
		return transportissuer.SignPostgresAbort(transportissuer.PostgresAbortClaims{Version: 1, Issuer: o.issuer, Audience: o.audience, ID: r.OpenID, IssuedAt: now, ExpiresAt: now + 5, DataTicketSHA256: a.DataTicketSHA256, AcceptanceID: a.AcceptanceID, WorkerIdentity: o.identity, WorkerCertSHA256: o.certDigest, CancellationStartedAt: now, Protocol: transportissuer.PostgresCancel}, f.key)
	}
	o.tickets = issuer
	broken := &uncertainConn{}
	o.dial = func(context.Context, string, string) (net.Conn, error) { return broken, io.ErrUnexpectedEOF }
	data, peer := net.Pipe()
	defer peer.Close()
	c := &AcceptedConn{tunnelConn: &tunnelConn{conn: data, raw: data}, opener: o, binding: testRequest().Binding, acceptance: a, receipt: "fixture"}
	target := PostgresTarget{PID: 1, Secret: []byte{1, 2, 3, 4}, TLS: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "source", RootCAs: o.tls.RootCAs}}
	if err := c.AbortPostgres(context.Background(), target); err == nil {
		t.Fatal("uncertain abort socket accepted")
	}
	if c.auxiliary == nil || c.Close() == nil || c.Close() == nil {
		t.Fatal("uncertain auxiliary cleanup custody disappeared")
	}
}
