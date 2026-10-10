// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestMongoAuthenticationDatabaseCannotSelectExternalCredentials(t *testing.T) {
	for _, authSource := range []string{"", "%24external", "bad%2Fname", "bad%5Cname", "bad.name", "bad%20name", "bad%00name", "bad%0Aname", strings.Repeat("a", 64)} {
		dsn := "mongodb://reader:fixture-password@database.example:27017/?tls=true&authSource=" + authSource
		if validConnectionDSN("mongodb", dsn, "analytics") {
			t.Fatal("invalid authentication database accepted", authSource)
		}
	}
}

const connectionFixtureDSN = "postgres://reader:fixture-password@database.example:5432/analytics?connect_timeout=5&sslmode=verify-full"

func connectionFixture(t *testing.T) (query.Request, delegation.Execution, connectionResolution) {
	t.Helper()
	request := query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT dynamic_source_environment", Delegation: "node-verified-fixture-grant"}
	digest, err := delegation.QueryDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	execution := delegation.Execution{
		Token:   request.Delegation,
		Claims:  delegation.Claims{Issuer: "gateway", AppTeam: "team-a", QuerySHA256: digest, ExpiresAt: time.Now().Add(time.Minute).Unix(), Sources: []delegation.Source{{Alias: "selected", ConnectionID: "saved-connection", Database: "analytics"}}},
		Binding: delegation.ExecutionBinding{JobID: "job", WorkerID: "worker", Owner: "owner", Claim: "claim"},
	}
	response := connectionResolution{Version: 1, DelegationSHA256: delegation.Digest(request.Delegation), ValidUntil: execution.Claims.ExpiresAt,
		Sources: []catalog.Source{{ID: "selected", Type: "postgres", DSNEnv: "KELVO_SOURCE_REQUEST_0_DSN"}}, Secrets: map[string]string{"KELVO_SOURCE_REQUEST_0_DSN": connectionFixtureDSN}}
	return request, execution, response
}

func connectionTestContext(execution delegation.Execution) context.Context {
	return delegation.WithExecution(context.Background(), execution.Claims, execution.Token, execution.Binding)
}

func fixtureConnectionResolver(t *testing.T, handler http.HandlerFunc) *ConnectionResolver {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	// Unit response tests use HTTP locally but provide explicit verified TLS
	// metadata; TestConnectionResolverTLSRequiresWorkerIdentity tests real TLS.
	state := &tls.ConnectionState{HandshakeComplete: true, Version: tls.VersionTLS13, VerifiedChains: [][]*x509.Certificate{{{NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}}}}
	resolver := &ConnectionResolver{url: server.URL, client: &http.Client{Timeout: time.Second, Transport: &connectionTestTransport{http.DefaultTransport, state}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: make(chan struct{}, 2)}
	t.Cleanup(resolver.Close)
	return resolver
}

type connectionTestTransport struct {
	base  http.RoundTripper
	state *tls.ConnectionState
}

func (t *connectionTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if response != nil {
		response.TLS = t.state
	}
	return response, err
}

func TestConnectionResolverRejectsCertificateExpiryDuringResponse(t *testing.T) {
	request, execution, response := connectionFixture(t)
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(40 * time.Millisecond)
		writeConnectionResponse(t, w, response)
	})
	state := resolver.client.Transport.(*connectionTestTransport).state
	state.VerifiedChains[0][0].NotAfter = time.Now().Add(20 * time.Millisecond)
	if _, err := resolver.resolve(context.Background(), execution, request); err == nil {
		t.Fatal("expired response certificate delivered credentials")
	}
}

func TestConnectionResolverCapsReceiptToVerifiedChainExpiry(t *testing.T) {
	request, execution, response := connectionFixture(t)
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) { writeConnectionResponse(t, w, response) })
	state := resolver.client.Transport.(*connectionTestTransport).state
	expiry := time.Now().Add(10 * time.Second)
	state.VerifiedChains[0] = append(state.VerifiedChains[0], &x509.Certificate{NotBefore: time.Now().Add(-time.Hour), NotAfter: expiry})
	resolved, err := resolver.resolve(context.Background(), execution, request)
	if err != nil || resolved.ValidUntil != expiry.Unix() {
		t.Fatalf("receipt outlived certificate chain: %v", err)
	}
	state.VerifiedChains = nil
	if _, err := resolver.resolve(context.Background(), execution, request); err == nil {
		t.Fatal("unverified TLS response accepted")
	}
}

func writeConnectionResponse(t *testing.T, w http.ResponseWriter, response connectionResolution) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		t.Error(err)
	}
}

func TestConnectionResolverBindsProofAndResponse(t *testing.T) {
	request, execution, response := connectionFixture(t)
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		var body connectionResolutionRequest
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Error("invalid resolver request")
			w.WriteHeader(400)
			return
		}
		if body.Delegation != execution.Token || body.Query.Delegation != "" || body.Query.SQL != request.SQL || body.JobID != execution.Binding.JobID || body.WorkerID != execution.Binding.WorkerID || body.Owner != execution.Binding.Owner || body.Claim != execution.Binding.Claim {
			t.Error("resolver request lost execution proof")
			w.WriteHeader(400)
			return
		}
		writeConnectionResponse(t, w, response)
	})
	out, err := resolver.resolve(connectionTestContext(execution), execution, request)
	if err != nil || validateConnectionCatalog(out, execution.Claims, request) != nil {
		t.Fatalf("resolver failed: %v", err)
	}
}

func TestConnectionResolverRejectsInvalidResponsesWithoutDisclosure(t *testing.T) {
	tests := []struct {
		name     string
		change   func(*connectionResolution)
		status   int
		raw      string
		encoding string
	}{
		{name: "wrong grant", change: func(r *connectionResolution) { r.DelegationSHA256 = "wrong" }},
		{name: "expired receipt", change: func(r *connectionResolution) { r.ValidUntil = time.Now().Add(-time.Second).Unix() }},
		{name: "extended authority", change: func(r *connectionResolution) { r.ValidUntil = time.Now().Add(time.Hour).Unix() }},
		{name: "wrong version", change: func(r *connectionResolution) { r.Version = 2 }},
		{name: "oversized secret", change: func(r *connectionResolution) { r.Secrets["KELVO_SOURCE_REQUEST_0_DSN"] = strings.Repeat("x", 32<<10+1) }},
		{name: "unknown field", raw: `{"version":1,"password":"private-marker"}`},
		{name: "duplicate field", raw: `{"version":1,"version":1}`},
		{name: "oversized response", raw: strings.Repeat("x", maxConnectionResponseBytes+1)},
		{name: "provider failure", status: 500, raw: "private-marker"},
		{name: "revoked", status: 403, raw: "private-marker"},
		{name: "redirect", status: 302, raw: "private-marker"},
		{name: "compressed body", encoding: "gzip"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, execution, response := connectionFixture(t)
			if tt.change != nil {
				tt.change(&response)
			}
			resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if tt.encoding != "" {
					w.Header().Set("Content-Encoding", tt.encoding)
				}
				if tt.status != 0 {
					w.WriteHeader(tt.status)
				}
				if tt.raw != "" {
					_, _ = io.WriteString(w, tt.raw)
				} else {
					_ = json.NewEncoder(w).Encode(response)
				}
			})
			_, err := resolver.resolve(connectionTestContext(execution), execution, request)
			if err == nil || strings.Contains(err.Error(), "private-marker") || strings.Contains(err.Error(), "fixture-password") {
				t.Fatalf("invalid response accepted or exposed: %v", err)
			}
		})
	}
}

func TestConnectionCatalogRejectsExpandedAuthority(t *testing.T) {
	tests := []struct {
		name   string
		change func(*connectionResolution)
	}{
		{"other alias", func(r *connectionResolution) { r.Sources[0].ID = "other" }},
		{"local path", func(r *connectionResolution) { r.Sources[0].Path = "/etc/passwd" }},
		{"adapter", func(r *connectionResolution) { r.Sources[0].Adapter = "dbapi" }},
		{"object range", func(r *connectionResolution) { r.Sources[0].Ranges = []catalog.ObjectRange{} }},
		{"extra secret", func(r *connectionResolution) { r.Secrets["AWS_SECRET_ACCESS_KEY"] = "must-not-enter-child" }},
		{"missing secret", func(r *connectionResolution) { delete(r.Secrets, "KELVO_SOURCE_REQUEST_0_DSN") }},
		{"ambient reference", func(r *connectionResolution) { r.Sources[0].DSNEnv = "KELVO_SOURCE_AMBIENT_DSN" }},
		{"unknown native type", func(r *connectionResolution) { r.Sources[0].Type = "custom-driver" }},
		{"unknown options", func(r *connectionResolution) { r.Sources[0].Options = map[string]string{"sslcert": "/private/key"} }},
		{"different database", func(r *connectionResolution) {
			r.Secrets["KELVO_SOURCE_REQUEST_0_DSN"] = strings.ReplaceAll(connectionFixtureDSN, "/analytics?", "/other?")
		}},
		{"unverified TLS", func(r *connectionResolution) {
			r.Secrets["KELVO_SOURCE_REQUEST_0_DSN"] = strings.ReplaceAll(connectionFixtureDSN, "verify-full", "disable")
		}},
		{"local certificate", func(r *connectionResolution) { r.Secrets["KELVO_SOURCE_REQUEST_0_DSN"] += "&sslcert=/private/key" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, execution, response := connectionFixture(t)
			tt.change(&response)
			if validateConnectionCatalog(response, execution.Claims, request) == nil {
				t.Fatal("expanded source authority accepted")
			}
		})
	}
}

func TestConnectionCatalogRequiresExactFederationTables(t *testing.T) {
	request, execution, response := connectionFixture(t)
	request.Mode, request.ConnectionID, request.Sources = "federated", "", []string{"selected"}
	execution.Claims.Sources[0].Schema = "public"
	execution.Claims.Sources[0].Tables = []delegation.Table{{Name: "orders", Table: "orders", Schema: "public"}}
	response.Sources[0].Federation = &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Table: "orders", Schema: "public"}}}
	if err := validateConnectionCatalog(response, execution.Claims, request); err != nil {
		t.Fatal(err)
	}
	response.Sources[0].Federation.Tables[0].Table = "secret_orders"
	if validateConnectionCatalog(response, execution.Claims, request) == nil {
		t.Fatal("unsigned remote relation accepted")
	}
	response.Sources[0].Federation = nil
	if validateConnectionCatalog(response, execution.Claims, request) == nil {
		t.Fatal("whole database attachment accepted")
	}
}

func TestConnectionCatalogAcceptsExplicitNativeContracts(t *testing.T) {
	cases := []struct {
		kind     string
		database string
		dsn      string
		url      string
		options  map[string]string
	}{
		{kind: "postgres", database: "analytics", dsn: connectionFixtureDSN},
		{kind: "cockroachdb", database: "analytics", dsn: connectionFixtureDSN},
		{kind: "alloydb", database: "analytics", dsn: connectionFixtureDSN},
		{kind: "redshift", database: "analytics", dsn: connectionFixtureDSN},
		{kind: "mysql", database: "analytics", dsn: "reader:fixture-password@tcp(database.example:3306)/analytics?parseTime=true&tls=true&timeout=5s&time_zone=%27%2B00%3A00%27&loc=UTC"},
		{kind: "mariadb", database: "analytics", dsn: "reader:fixture-password@tcp(database.example:3306)/analytics?parseTime=true&tls=true&timeout=5s&time_zone=%27%2B00%3A00%27&loc=UTC"},
		{kind: "sqlserver", database: "analytics", dsn: "sqlserver://reader:fixture-password@database.example:1433?database=analytics&encrypt=true&TrustServerCertificate=false&connection+timeout=5"},
		{kind: "oracle", database: "analytics", dsn: "oracle://reader:fixture-password@database.example:2484/analytics?SSL=enable&SSL+VERIFY=true"},
		{kind: "mongodb", database: "analytics", dsn: "mongodb://reader:fixture-password@database.example:27017/?authSource=admin&tls=true", options: map[string]string{"database": "analytics"}},
		{kind: "mongodb", database: "analytics", dsn: "mongodb://reader:fixture-password@database.example:27017/?authSource=users&tls=true", options: map[string]string{"database": "analytics"}},
		{kind: "databricks", database: "analytics", url: "https://workspace.example:443", options: map[string]string{"warehouse_id": "ab123", "catalog": "analytics"}},
		{kind: "d1", database: "01234567-89ab-cdef-0123-456789abcdef", url: "https://api.cloudflare.com", options: map[string]string{"account_id": "0123456789abcdef0123456789abcdef", "database_id": "01234567-89ab-cdef-0123-456789abcdef"}},
		{kind: "clickhouse", database: "analytics", url: "https://database.example:8443?database=analytics"},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			request, execution, response := connectionFixture(t)
			execution.Claims.Sources[0].Database = tc.database
			source := catalog.Source{ID: "selected", Type: tc.kind, Options: tc.options}
			response.Secrets = map[string]string{}
			if tc.dsn != "" {
				source.DSNEnv = "KELVO_SOURCE_REQUEST_0_DSN"
				response.Secrets[source.DSNEnv] = tc.dsn
			} else {
				source.URLEnv = "KELVO_SOURCE_REQUEST_0_URL"
				response.Secrets[source.URLEnv] = tc.url
				if tc.kind == "clickhouse" {
					source.UsernameEnv, source.PasswordEnv = "KELVO_SOURCE_REQUEST_0_USERNAME", "KELVO_SOURCE_REQUEST_0_PASSWORD"
					response.Secrets[source.UsernameEnv], response.Secrets[source.PasswordEnv] = "reader", "fixture-password"
				} else {
					source.TokenEnv = "KELVO_SOURCE_REQUEST_0_TOKEN"
					response.Secrets[source.TokenEnv] = "fixture-token"
				}
			}
			response.Sources = []catalog.Source{source}
			if err := validateConnectionCatalog(response, execution.Claims, request); err != nil {
				t.Fatalf("explicit source contract rejected: %v", err)
			}
		})
	}
}

func TestConnectionResolverAdmissionAndCancellation(t *testing.T) {
	request, execution, response := connectionFixture(t)
	var active, peak atomic.Int32
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			old := peak.Load()
			if current <= old || peak.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		writeConnectionResponse(t, w, response)
	})
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := resolver.resolve(context.Background(), execution, request); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	if peak.Load() > 2 {
		t.Fatal("resolver exceeded configured concurrent requests")
	}
	resolver.slots <- struct{}{}
	resolver.slots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.resolve(ctx, execution, request); err != context.Canceled {
		t.Fatalf("admission ignored cancellation: %v", err)
	}
	<-resolver.slots
	<-resolver.slots
}

func TestDynamicExecutorDetachesCatalogAndStripsParentAuthority(t *testing.T) {
	request, execution, response := connectionFixture(t)
	var calls atomic.Int32
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); writeConnectionResponse(t, w, response) })
	config := catalog.Config{Sources: []catalog.Source{{ID: "operator", Type: "postgres", DSNEnv: "KELVO_SOURCE_OPERATOR_DSN"}}}
	_, digest, err := catalog.AuthoritySnapshot(config)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(config, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	registry := map[string]*ConnectionResolver{"gateway": resolver}
	e, err = e.WithConnectionResolvers(registry)
	if err != nil {
		t.Fatal(err)
	}
	e, err = e.WithCatalogBinding(digest)
	if err != nil {
		t.Fatal(err)
	}
	delete(registry, "gateway")
	e.Config.Sources[0].ID = "mutated-inspection-copy"
	t.Setenv("KELVO_SOURCE_OPERATOR_DSN", "must-stay-in-parent")
	ctx, err := WithCatalogAuthority(connectionTestContext(execution), digest)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		stats, err := e.Execute(ctx, request, &workerTestSink{})
		if err != nil || stats.Rows != 3 {
			t.Fatalf("dynamic worker failed: %v", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("credentials were reused across executions")
	}
	if e.catalogBinding.config.Sources[0].ID != "operator" || len(e.Config.Sources) != 1 || e.Config.Sources[0].ID != "mutated-inspection-copy" {
		t.Fatal("dynamic execution mutated static catalog")
	}
	badCtx, _ := WithCatalogAuthority(connectionTestContext(execution), strings.Repeat("0", 64))
	if _, err := e.Execute(badCtx, request, &workerTestSink{}); err == nil || calls.Load() != 2 {
		t.Fatal("dynamic request bypassed static authority check")
	}
	if _, err := e.Execute(context.Background(), request, &workerTestSink{}); err == nil || calls.Load() != 2 {
		t.Fatal("public request manufactured execution authority")
	}
	request.SQL = "SELECT different_query"
	if _, err := e.Execute(ctx, request, &workerTestSink{}); err == nil || calls.Load() != 2 {
		t.Fatal("delegation accepted changed SQL")
	}
}

func dynamicConnectionChildMatches(in Input) bool {
	return in.Request.Delegation == "" && in.Request.Mode == "native" && in.Request.ConnectionID == "selected" && len(in.Config.Sources) == 1 && in.Config.Sources[0].ID == "selected" && in.Config.Acceleration == nil && os.Getenv("KELVO_SOURCE_REQUEST_0_DSN") == connectionFixtureDSN && os.Getenv("KELVO_SOURCE_OPERATOR_DSN") == "" && os.Getenv("AWS_SECRET_ACCESS_KEY") == ""
}

func TestConnectionSecretsNeverFallbackAndChildRejectsGrant(t *testing.T) {
	t.Setenv("KELVO_SOURCE_REQUEST_0_DSN", "ambient-must-not-be-used")
	e := &Executor{Secrets: connectionSecrets{}}
	if value, _, err := e.resolveSecret(context.Background(), "KELVO_SOURCE_REQUEST_0_DSN"); err == nil || value != "" {
		t.Fatal("missing private secret inherited ambient environment")
	}
	request, _, _ := connectionFixture(t)
	if _, err := (Input{Request: request}).ExecutionContext(context.Background()); err == nil {
		t.Fatal("child accepted parent grant")
	}
}

type connectionQuotaFixture struct {
	ids   []string
	until time.Time
}

func (f *connectionQuotaFixture) Acquire(ctx context.Context, ids []string) (context.Context, func(), error) {
	f.ids = append([]string(nil), ids...)
	if wait := time.Until(f.until); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}
	return ctx, func() {}, nil
}

func TestDynamicQuotaIdentityAndExpiredDelivery(t *testing.T) {
	request, execution, response := connectionFixture(t)
	response.ValidUntil = time.Now().Add(2 * time.Second).Unix()
	var responseMu sync.Mutex
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		responseMu.Lock()
		defer responseMu.Unlock()
		writeConnectionResponse(t, w, response)
	})
	e, err := New(catalog.Config{}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e, err = e.WithConnectionResolvers(map[string]*ConnectionResolver{"gateway": resolver})
	if err != nil {
		t.Fatal(err)
	}
	quota := &connectionQuotaFixture{until: time.Unix(response.ValidUntil, 0).Add(30 * time.Millisecond)}
	e.SourceAdmission = quota
	if _, err := e.Execute(connectionTestContext(execution), request, &workerTestSink{}); err == nil {
		t.Fatal("expired credential receipt started child")
	}
	if len(quota.ids) != 1 || quota.ids[0] == "selected" || !catalog.ValidID(quota.ids[0]) {
		t.Fatal("dynamic source quota used caller alias")
	}
	first := quota.ids[0]
	quota.until = time.Time{}
	copy, _, err := e.resolveConnections(context.Background(), execution, request)
	if err == nil {
		t.Fatal("expired resolver response accepted")
	}
	responseMu.Lock()
	response.ValidUntil = execution.Claims.ExpiresAt
	responseMu.Unlock()
	copy, _, err = e.resolveConnections(context.Background(), execution, request)
	if err != nil {
		t.Fatal(err)
	}
	if copy.connectionSourceIDs["selected"] != first {
		t.Fatal("same connection changed quota identity")
	}
	other := execution
	other.Claims.AppTeam = "team-b"
	copy, _, err = e.resolveConnections(context.Background(), other, request)
	if err != nil {
		t.Fatal(err)
	}
	if copy.connectionSourceIDs["selected"] == first {
		t.Fatal("different teams shared quota identity")
	}
}

type connectionSlowSink struct {
	workerTestSink
	delay time.Duration
}

func (s *connectionSlowSink) Write(record arrow.RecordBatch) error {
	time.Sleep(s.delay)
	return s.workerTestSink.Write(record)
}

func TestConnectionReceiptDoesNotShortenExecutionGrant(t *testing.T) {
	request, execution, response := connectionFixture(t)
	response.ValidUntil = time.Now().Add(2 * time.Second).Unix()
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) { writeConnectionResponse(t, w, response) })
	e, err := New(catalog.Config{}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e, err = e.WithConnectionResolvers(map[string]*ConnectionResolver{"gateway": resolver})
	if err != nil {
		t.Fatal(err)
	}
	if stats, err := e.Execute(connectionTestContext(execution), request, &connectionSlowSink{delay: 2200 * time.Millisecond}); err != nil || stats.Rows != 3 {
		t.Fatalf("delivery receipt incorrectly bounded query execution: %v", err)
	}
}

func TestConnectionGrantExpiryStopsRunningQuery(t *testing.T) {
	request, execution, response := connectionFixture(t)
	request.Mode, request.ConnectionID, request.Sources, request.SQL = "federated", "", []string{"selected"}, "SELECT wait"
	execution.Claims.QuerySHA256, _ = delegation.QueryDigest(request)
	execution.Claims.ExpiresAt = time.Now().Add(2 * time.Second).Unix()
	execution.Claims.Sources[0].Schema = "public"
	execution.Claims.Sources[0].Tables = []delegation.Table{{Name: "orders", Table: "orders", Schema: "public"}}
	response.Sources[0].Federation = &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Table: "orders", Schema: "public"}}}
	response.ValidUntil = execution.Claims.ExpiresAt
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) { writeConnectionResponse(t, w, response) })
	e, err := New(catalog.Config{}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e, err = e.WithConnectionResolvers(map[string]*ConnectionResolver{"gateway": resolver})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(connectionTestContext(execution), request, &workerTestSink{}); err != context.DeadlineExceeded {
		t.Fatalf("grant expiry did not stop query: %v", err)
	}
}

func TestConnectionResolverTLSRequiresWorkerIdentity(t *testing.T) {
	identity := "spiffe://kelvo/tenant/test/worker/one"
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse(identity)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture-ca"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "fixture-worker"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{uri}}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	parsed, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	pool.AddCert(parsed)
	request, execution, response := connectionFixture(t)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.VerifiedChains) == 0 {
			t.Error("worker certificate was not verified")
		}
		writeConnectionResponse(t, w, response)
	}))
	var connections atomic.Int32
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	defer server.Close()
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	encodedKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	config := ConnectionResolverConfig{URL: server.URL + "/internal/kelvo/resolve", Timeout: time.Second, MaxConcurrent: 2,
		CAFile:   write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})),
		CertFile: write("client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		KeyFile:  write("client-key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey}))}
	resolver, err := NewConnectionResolver(config, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	if _, err := resolver.resolve(context.Background(), execution, request); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.resolve(context.Background(), execution, request); err != nil {
		t.Fatal(err)
	}
	if connections.Load() != 2 {
		t.Fatal("resolver reused a TLS connection instead of revalidating certificate lifetime")
	}
	transport := resolver.client.Transport.(*http.Transport)
	if !transport.DisableKeepAlives || transport.TLSClientConfig.ClientSessionCache != nil {
		t.Fatal("resolver cached certificate authority across requests")
	}
	if _, err := NewConnectionResolver(config, "spiffe://kelvo/tenant/other/worker/one"); err == nil {
		t.Fatal("wrong worker identity accepted")
	}
	config.URL = "http://resolver.invalid/internal/kelvo/resolve"
	if _, err := NewConnectionResolver(config, identity); err == nil {
		t.Fatal("plaintext resolver accepted")
	}
}
