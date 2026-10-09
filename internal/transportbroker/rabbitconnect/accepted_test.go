// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

func TestAcceptedOpenNegotiationPreservesOriginalPayload(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "duplicate", "wrong-digest", "expired", "extended", "wrong-key"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			pub, key, _ := ed25519.GenerateKey(rand.Reader)
			f.config.AcceptedOpenTrust = &transportissuer.AcceptedOpenTrust{KeyID: "route-one", PublicKey: pub}
			f.config.ProxyAddress = serveOnce(t, f.server, func(conn net.Conn) {
				request, err := readCONNECT(conn)
				if err != nil {
					t.Error(err)
					return
				}
				if !strings.Contains(request, "Rabbit-Accepted-Open: required-v1\r\n") {
					t.Error("receipt negotiation missing")
					return
				}
				var token string
				for _, line := range strings.Split(request, "\r\n") {
					if strings.HasPrefix(line, "Proxy-Authorization: Bearer ") {
						token = strings.TrimPrefix(line, "Proxy-Authorization: Bearer ")
					}
				}
				sum := sha256.Sum256([]byte(token))
				now := time.Now().Unix()
				claims := transportissuer.AcceptedOpenClaims{Version: 1, DataTicketSHA256: hex.EncodeToString(sum[:]), AcceptanceID: strings.Repeat("b", 32), AcceptedAt: now, ExpiresAt: now + 120}
				if mode == "wrong-digest" {
					claims.DataTicketSHA256 = strings.Repeat("e", 64)
				}
				if mode == "expired" {
					claims.AcceptedAt = now - 10
					claims.ExpiresAt = now - 1
				}
				if mode == "extended" {
					claims.ExpiresAt = now + 600
				}
				signing := key
				if mode == "wrong-key" {
					_, signing, _ = ed25519.GenerateKey(rand.Reader)
				}
				receipt, _ := transportissuer.SignAcceptedOpen(claims, "route-one", signing)
				header := "Rabbit-Accepted-Open: " + receipt + "\r\n"
				if mode == "missing" {
					header = ""
				}
				if mode == "duplicate" {
					header += header
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n"+header+"\r\nsource-greeting")
			})
			opener, err := New(f.config, f.issuer())
			if err != nil {
				t.Fatal(err)
			}
			conn, err := opener.Open(context.Background(), testRequest())
			if conn != nil {
				defer conn.Close()
			}
			if mode != "valid" {
				if err == nil {
					t.Fatal("invalid accepted source receipt admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			accepted, ok := conn.(*AcceptedConn)
			if !ok || accepted.Accepted().AcceptanceID != strings.Repeat("b", 32) {
				t.Fatal("accepted receipt missing")
			}
			data := make([]byte, len("source-greeting"))
			if _, err := io.ReadFull(conn, data); err != nil || string(data) != "source-greeting" {
				t.Fatal("receipt changed source bytes", err)
			}
		})
	}
}
func TestCleanupIssuerUsesOriginalOriginAndSeparateAdmission(t *testing.T) {
	f := newFixture(t)
	request := transportissuer.CleanupRequest{Version: 1, DataTicketSHA256: strings.Repeat("a", 64), AcceptedOpen: "receipt", OpenID: strings.Repeat("b", 64), Protocol: transportissuer.PostgresCancel}
	client, _ := httpIssuerFixture(t, f, f.server, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var got transportissuer.CleanupRequest
		if r.URL.Path != transportissuer.CleanupPath || json.Unmarshal(raw, &got) != nil || got != request {
			t.Error("cleanup request changed scope or endpoint")
		}
		digest := sha256.Sum256(raw)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(transportissuer.CleanupResponse{Version: 1, RequestSHA256: hex.EncodeToString(digest[:]), Token: "fixture-token"})
	})
	client.slots <- struct{}{}
	defer func() { <-client.slots }()
	if got, err := client.IssueCleanup(context.Background(), request); err != nil || got != "fixture-token" {
		t.Fatal("cleanup blocked behind data admission", err)
	}
}

func TestCleanupHTTPDispatchCannotWaitBehindSaturatedDataLane(t *testing.T) {
	f := newFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var expected IssueRequest
	client, o := httpIssuerFixture(t, f, f.server, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(raw)
		token := "cleanup-fixture"
		if r.URL.Path == transportissuer.Path {
			close(entered)
			<-release
			token = signTicket(f.key, claimsFor(expected))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(transportissuer.Response{Version: 1, RequestSHA256: hex.EncodeToString(sum[:]), Token: token})
	})
	expected = issueFor(o, testRequest())
	done := make(chan error, 1)
	go func() { _, err := client.Issue(context.Background(), expected); done <- err }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err := client.IssueCleanup(ctx, transportissuer.CleanupRequest{Version: 1, DataTicketSHA256: strings.Repeat("a", 64), AcceptedOpen: "receipt", OpenID: strings.Repeat("b", 64), Protocol: transportissuer.PostgresCancel})
	cancel()
	close(release)
	if err != nil {
		t.Fatal("ordinary HTTP transport blocked reserved cleanup dispatch", err)
	}
	if err := <-done; err != nil {
		t.Fatal("ordinary issuance failed", err)
	}
}
