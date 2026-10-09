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

func TestApplicationSixEngineOperationBoundary(t *testing.T) {
	for _, engine := range []string{"postgres", "mysql", "clickhouse", "sqlite", "oracle"} {
		for _, kind := range []operations.Kind{operations.ConnectionTest, operations.MetadataInspect, operations.QueryRead, operations.StatementExecute} {
			if !applicationSourceSupported(engine, operations.Request{Kind: kind}) {
				t.Fatal("supported operation rejected", engine, kind)
			}
		}
		for _, kind := range []operations.Kind{operations.NativeRead, operations.NativeExecute, operations.MigrationApply, operations.WatchRead} {
			if applicationSourceSupported(engine, operations.Request{Kind: kind, Spec: operations.Spec{Native: &operations.NativeSpec{Provider: "mongodb"}}}) {
				t.Fatal("unrelated operation accepted", engine, kind)
			}
		}
	}
	for _, kind := range []operations.Kind{operations.NativeRead, operations.NativeExecute} {
		request := operations.Request{Kind: kind, Spec: operations.Spec{Native: &operations.NativeSpec{Provider: "mongodb", Command: "find"}}}
		if !applicationSourceSupported("mongodb", request) {
			t.Fatal("MongoDB native operation rejected", kind)
		}
		request.Spec.Native.Provider = "other"
		if applicationSourceSupported("mongodb", request) {
			t.Fatal("unrelated native provider accepted")
		}
	}
	for _, kind := range []operations.Kind{operations.QueryRead, operations.StatementExecute} {
		if applicationSourceSupported("mongodb", operations.Request{Kind: kind}) {
			t.Fatal("MongoDB SQL operation accepted", kind)
		}
	}
	for _, engine := range []string{"", "postgresql", "duckdb", "csv", "redis", "sqlserver", "mariadb"} {
		if applicationSourceSupported(engine, operations.Request{Kind: operations.ConnectionTest}) {
			t.Fatal("seventh engine accepted", engine)
		}
	}
}

func TestApplicationNativeApprovalAndResultReservation(t *testing.T) {
	c := ApplicationConfig{AppScope: "installation-a", WriteMode: "disabled"}
	claims := operations.GrantClaims{AppTeam: c.AppScope, Subject: operations.Subject{Kind: "user"}, Operation: operations.NativeRead, Authorization: operations.Authorization{Kind: "read"}}
	if !c.allows(claims) || !applicationProducesResult(operations.Request{Kind: operations.NativeRead}) {
		t.Fatal("native read rejected or result not reserved")
	}
	claims.Operation = operations.NativeExecute
	if c.allows(claims) {
		t.Fatal("native mutation admitted as a read")
	}
	claims.Authorization.Kind = "approved_change"
	if c.allows(claims) {
		t.Fatal("grant enabled disabled writes")
	}
	c.WriteMode = "approved"
	if !c.allows(claims) {
		t.Fatal("approved native mutation rejected")
	}
	request := operations.Request{Kind: operations.NativeExecute, Spec: operations.Spec{Native: &operations.NativeSpec{Provider: "mongodb"}}}
	if applicationProducesResult(request) {
		t.Fatal("unrequested mutation result reserved")
	}
	request.Spec.Native.ReturnResult = true
	if !applicationProducesResult(request) {
		t.Fatal("requested mutation result not reserved")
	}
}

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
