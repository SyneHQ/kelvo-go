// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestApplicationRetainedGrantCannotExecute(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := &Application{trust: operations.GrantTrust{Issuer: "synehq_oos", Audience: "kelvo_application", ClusterTenant: "oos-test", ServicePrincipal: "synehq_oos", PublicKey: public}}
	now := time.Now().Unix()
	claims := operations.GrantClaims{Version: 2, Issuer: a.trust.Issuer, Audience: a.trust.Audience, ClusterTenant: a.trust.ClusterTenant, ServicePrincipal: a.trust.ServicePrincipal, AppTeam: "installation-a", Subject: operations.Subject{Kind: "user", ID: "owner-a"}, ID: "grant-a", IssuedAt: now - 120, ExpiresAt: now - 60, ConnectionID: "connection-a", Operation: operations.QueryRead, RequestSHA256: strings.Repeat("a", 64), Authorization: operations.Authorization{Kind: "read"}}
	token, err := operations.SignGrant(claims, private)
	if err != nil {
		t.Fatal(err)
	}
	verified, retained, err := a.verifyHTTPGrant(token, true)
	if err != nil || !retained || verified.RequestSHA256 != claims.RequestSHA256 {
		t.Fatal("retained control rejected", err)
	}
	if _, _, err := a.verifyHTTPGrant(token, false); err == nil {
		t.Fatal("expired grant extended execution")
	}
	parts := strings.Split(token, ".")
	parts[2] = strings.Repeat("a", len(parts[2]))
	if _, _, err := a.verifyHTTPGrant(strings.Join(parts, "."), true); err == nil {
		t.Fatal("invalid signature accepted")
	}
	ctx := context.WithValue(context.Background(), retainedApplicationGrant{}, operations.GrantDigest(token))
	if !applicationControlMatches(ctx, operationstore.Record{AuthoritySHA256: operations.GrantDigest(token)}) {
		t.Fatal("original grant rejected")
	}
	if applicationControlMatches(ctx, operationstore.Record{AuthoritySHA256: strings.Repeat("b", 64)}) {
		t.Fatal("different grant obtained retained authority")
	}
	claims.IssuedAt = now
	claims.ExpiresAt = now + 60
	live, err := operations.SignGrant(claims, private)
	if err != nil {
		t.Fatal(err)
	}
	if _, retained, err := a.verifyHTTPGrant(live, false); err != nil || retained {
		t.Fatal("live execution grant rejected", err)
	}
}

func TestApplicationScopeAndWriteMode(t *testing.T) {
	c := ApplicationConfig{AppScope: "installation-a", WriteMode: "disabled"}
	claims := operations.GrantClaims{AppTeam: "installation-a", Subject: operations.Subject{Kind: "user"}, Operation: operations.QueryRead, Authorization: operations.Authorization{Kind: "read"}}
	if !c.allows(claims) {
		t.Fatal("read rejected")
	}
	claims.AppTeam = "installation-b"
	if c.allows(claims) {
		t.Fatal("cross installation access")
	}
	claims.AppTeam = c.AppScope
	claims.Operation = operations.StatementExecute
	claims.Authorization.Kind = "approved_change"
	if c.allows(claims) {
		t.Fatal("write enabled by grant")
	}
	c.WriteMode = "approved"
	if !c.allows(claims) {
		t.Fatal("approved write rejected")
	}
	claims.Authorization.Kind = "trusted_app"
	if c.allows(claims) {
		t.Fatal("trusted app bypass accepted")
	}
	claims.Authorization.Kind = "approved_change"
	claims.Subject.Kind = "api_key"
	if c.allows(claims) {
		t.Fatal("API key write accepted")
	}
	claims.Subject.Kind = "user"
	claims.Operation = operations.IngestionInstall
	if c.allows(claims) {
		t.Fatal("out of scope operation accepted")
	}
}

func TestApplicationHTTPRejectsAmbiguousAuthority(t *testing.T) {
	a := &Application{token: strings.Repeat("s", 32)}
	for _, headers := range [][]string{nil, {"Bearer incorrect"}, {"Bearer " + a.token, "Bearer " + a.token}} {
		request := httptest.NewRequest(http.MethodPost, "/v1/operations", nil)
		for _, value := range headers {
			request.Header.Add("Authorization", value)
		}
		response := httptest.NewRecorder()
		a.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatal("ambiguous authority accepted", response.Code)
		}
	}
}
