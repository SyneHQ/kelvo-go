// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func operationResolverFixture(t *testing.T) (ledger.Record, operations.Request, operationResolution) {
	t.Helper()
	request := operations.Request{Version: 1, Kind: operations.StatementExecute, Connection: operations.ConnectionRef{ID: "saved-a", Database: "analytics", Schema: "reporting"}, IdempotencyKey: "operation-1",
		Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: "UPDATE analytics SET value=$1", Parameters: []operations.Parameter{{Type: "int64", Value: json.RawMessage(`1`)}}, Transaction: operations.TransactionRequired}}}
	digest, err := operations.Digest(request)
	if err != nil {
		t.Fatal(err)
	}
	record := ledger.Record{ID: "operation-1", State: ledger.Running, Kind: request.Kind, RequestSHA256: digest, AuthorityToken: "node-verified-operation-grant",
		Scope:   ledger.Scope{Issuer: "gateway", ClusterTenant: "shared", ServicePrincipal: "api", AppTeam: "customer-a", SubjectKind: "api_key", SubjectID: "key-a", ConnectionID: "saved-a"},
		Binding: ledger.Binding{WorkerID: "worker", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}, ExecuteBefore: time.Now().Add(time.Minute), AuthorityUntil: time.Now().Add(time.Minute)}
	record.AuthoritySHA256 = operations.GrantDigest(record.AuthorityToken)
	response := operationResolution{Version: 1, GrantSHA256: record.AuthoritySHA256, RequestSHA256: digest, SourceRevision: strings.Repeat("c", 64), ValidUntil: time.Now().Add(5 * time.Second).Unix(),
		Source: catalog.Source{ID: "source_1", Type: "postgres", DSNEnv: "KELVO_SOURCE_REQUEST_0_DSN"}, Secrets: map[string]string{"KELVO_SOURCE_REQUEST_0_DSN": connectionFixtureDSN}}
	return record, request, response
}

func TestOperationResolverBindsPrivateSourceToExactRequest(t *testing.T) {
	record, request, response := operationResolverFixture(t)
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		var input operationResolutionRequest
		if r.URL.Path != "/internal/kelvo/resolve-operation" || json.NewDecoder(r.Body).Decode(&input) != nil || input.Grant != record.AuthorityToken || input.OperationID != record.ID || input.Operation.Connection.Schema != "reporting" || input.WorkerID != record.Binding.WorkerID || input.Owner != record.Binding.Owner || input.Claim != record.Binding.Claim {
			t.Error("private resolution lost exact operation binding")
			w.WriteHeader(403)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})
	resolver.url += "/internal/kelvo/resolve"
	e := &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
	input, err := e.ResolveOperationSource(context.Background(), record, request)
	if err != nil {
		t.Fatal(err)
	}
	if input.Source.DSN != connectionFixtureDSN || input.Source.Schema != request.Connection.Schema || input.Source.Database != request.Connection.Database || input.Source.Revision != response.SourceRevision || input.RequestSHA256 != record.RequestSHA256 || input.CredentialsValidUntil > response.ValidUntil {
		t.Fatal("process input lost source or authority binding")
	}
	other := record
	other.Scope.AppTeam = "customer-b"
	second, err := e.ResolveOperationSource(context.Background(), other, request)
	if err != nil {
		t.Fatal(err)
	}
	if input.Source.TenantID == second.Source.TenantID {
		t.Fatal("shared cluster collapsed customer adapter identities")
	}
}

func TestOperationResolverRejectsAlteredAndAmbientSources(t *testing.T) {
	changes := map[string]func(*operationResolution){
		"grant":         func(r *operationResolution) { r.GrantSHA256 = strings.Repeat("d", 64) },
		"request":       func(r *operationResolution) { r.RequestSHA256 = strings.Repeat("d", 64) },
		"revision":      func(r *operationResolution) { r.SourceRevision = "unverified" },
		"expired lease": func(r *operationResolution) { r.ValidUntil = time.Now().Unix() },
		"long lease":    func(r *operationResolution) { r.ValidUntil = time.Now().Add(time.Minute).Unix() },
		"database": func(r *operationResolution) {
			r.Secrets[r.Source.DSNEnv] = strings.Replace(connectionFixtureDSN, "/analytics?", "/foreign?", 1)
		},
		"ambient secret": func(r *operationResolution) { r.Source.DSNEnv = "DATABASE_URL" },
		"extra secret":   func(r *operationResolution) { r.Secrets["UNSELECTED"] = "fixture" },
		"path":           func(r *operationResolution) { r.Source.Path = "/private/fixture" },
		"adapter url":    func(r *operationResolution) { r.Source.Adapter = "dbapi" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			record, request, response := operationResolverFixture(t)
			change(&response)
			resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			})
			resolver.url += "/internal/kelvo/resolve"
			e := &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
			if _, err := e.ResolveOperationSource(context.Background(), record, request); err == nil {
				t.Fatal("invalid source authority accepted")
			}
		})
	}
}

func TestOperationResolverRejectsPrestartAndChangedRequestBeforeNetwork(t *testing.T) {
	record, request, _ := operationResolverFixture(t)
	e := &Executor{Limits: query.DefaultLimits()}
	record.State = ledger.Assigned
	if _, err := e.ResolveOperationSource(context.Background(), record, request); err == nil {
		t.Fatal("assigned operation resolved credentials")
	}
	record.State = ledger.Running
	request.Spec.Statement.SQL = "DELETE FROM analytics"
	if _, err := e.ResolveOperationSource(context.Background(), record, request); err == nil {
		t.Fatal("changed request resolved credentials")
	}
}

func TestOperationResolverAllowsInlineVerifiedTrustWithoutWeakeningCatalog(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	root := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	for _, bad := range []map[string]string{
		{"tls_ca_pem": " ", "tls_server_name": "db.internal"},
		{"tls_ca_pem": root + "unexpected"},
		{"tls_ca_pem": "garbage" + root},
		{"tls_ca_pem": root + "\n-----BEGIN PRIVATE KEY-----\nYWJj\n-----END PRIVATE KEY-----"},
		{"tls_server_name": "https://db.internal"},
		{"tls_server_name": "db.internal\nunsafe"},
	} {
		if validateOperationTLSOptions(bad) == nil {
			t.Fatal("invalid inline trust accepted")
		}
	}
	record, request, response := operationResolverFixture(t)
	response.Source.Options = map[string]string{"tls_ca_pem": root, "tls_server_name": "db.internal"}
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})
	resolver.url += "/internal/kelvo/resolve"
	e := &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
	input, err := e.ResolveOperationSource(context.Background(), record, request)
	if err != nil || input.Source.Options["tls_ca_pem"] != root || input.Source.Options["tls_server_name"] != "db.internal" {
		t.Fatal("private CA not preserved", err)
	}
	if response.Source.Options["tls_ca_pem"] != root {
		t.Fatal("input options mutated")
	}
}

func TestOperationBusinessResolverBindsDemandCredentials(t *testing.T) {
	for _, engine := range []string{"salesforce_data360", "salesforce_tableau_next", "ramp", "daloopa", "motherduck", "posthog"} {
		t.Run(engine, func(t *testing.T) {
			record, request, response := operationResolverFixture(t)
			request.Kind, record.Kind = operations.NativeRead, operations.NativeRead
			request.Connection.Schema = ""
			request.Spec = operations.Spec{Native: &operations.NativeSpec{Provider: engine, Command: "query", Parameters: []operations.Parameter{{Type: "json", Value: json.RawMessage(`{"operation":"list_tools"}`)}}}}
			record.RequestSHA256, _ = operations.Digest(request)
			response.RequestSHA256 = record.RequestSHA256
			response.Source = catalog.Source{ID: "source_1", Type: engine, TokenEnv: "KELVO_SOURCE_REQUEST_0_TOKEN", Options: map[string]string{"environment": "sandbox"}}
			response.Secrets = map[string]string{response.Source.TokenEnv: "current-test-token"}
			if engine == "posthog" {
				request.Connection.Database = "17"
				record.RequestSHA256, _ = operations.Digest(request)
				response.RequestSHA256 = record.RequestSHA256
				response.Source.URLEnv, response.Source.Options = "KELVO_SOURCE_REQUEST_0_URL", nil
				response.Secrets[response.Source.URLEnv] = "https://analytics.example"
			}
			if engine == "ramp" {
				response.Source.UsernameEnv = "KELVO_SOURCE_REQUEST_0_USERNAME"
				response.Secrets[response.Source.UsernameEnv] = "test-client"
			}
			resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			})
			resolver.url += "/internal/kelvo/resolve"
			e := &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
			input, err := e.ResolveOperationSource(context.Background(), record, request)
			if err != nil || input.Source.Token != "current-test-token" || input.Source.Database != request.Connection.Database || input.Source.Options["environment"] != response.Source.Options["environment"] {
				t.Fatal("business source lost current credentials or database selection", err)
			}
			original := response.Source
			for name, change := range map[string]func(*operationResolution){
				"ambient token":       func(r *operationResolution) { r.Source.TokenEnv = "PROVIDER_TOKEN" },
				"endpoint":            func(r *operationResolution) { r.Source.URLEnv = "UNSCOPED_SOURCE_URL" },
				"path":                func(r *operationResolution) { r.Source.Path = "/tmp/private" },
				"adapter":             func(r *operationResolution) { r.Source.Adapter = "https://other.invalid" },
				"federation":          func(r *operationResolution) { r.Source.Federation = &catalog.FederationConfig{} },
				"tls override":        func(r *operationResolution) { r.Source.Options = map[string]string{"tls_server_name": "other.invalid"} },
				"unknown environment": func(r *operationResolution) { r.Source.Options = map[string]string{"environment": "custom"} },
			} {
				t.Run(name, func(t *testing.T) {
					response.Source = original
					change(&response)
					if _, err := e.ResolveOperationSource(context.Background(), record, request); err == nil {
						t.Fatal("unbounded business source accepted")
					}
				})
			}
			response.Source = original
			response.Secrets["UNREFERENCED"] = "secret"
			if _, err := e.ResolveOperationSource(context.Background(), record, request); err == nil {
				t.Fatal("unreferenced credential accepted")
			}
		})
	}
}

func TestOperationCQLCatalogUsesExactPrivateCredentials(t *testing.T) {
	_, request, response := operationResolverFixture(t)
	request.Connection.Schema = ""
	response.Source = catalog.Source{ID: "source_1", Type: "cassandra", URLEnv: "KELVO_SOURCE_REQUEST_0_URL", UsernameEnv: "KELVO_SOURCE_REQUEST_0_USERNAME", PasswordEnv: "KELVO_SOURCE_REQUEST_0_PASSWORD"}
	response.Secrets = map[string]string{response.Source.URLEnv: "tls://db.example:9042", response.Source.UsernameEnv: "reader", response.Source.PasswordEnv: "private-test-value"}
	for _, kind := range []string{"cassandra", "scylla"} {
		response.Source.Type = kind
		if err := validateOperationCQLCatalog(response.Source, response.Secrets, request); err != nil {
			t.Fatal(kind, err)
		}
	}
	for name, mutate := range map[string]func(*operationResolution, *operations.Request){
		"plaintext": func(r *operationResolution, _ *operations.Request) {
			r.Secrets[r.Source.URLEnv] = "tcp://db.example:9042"
		},
		"missing port": func(r *operationResolution, _ *operations.Request) { r.Secrets[r.Source.URLEnv] = "tls://db.example" },
		"URL option":   func(r *operationResolution, _ *operations.Request) { r.Secrets[r.Source.URLEnv] += "?keyspace=other" },
		"URL user": func(r *operationResolution, _ *operations.Request) {
			r.Secrets[r.Source.URLEnv] = "tls://other@db.example:9042"
		},
		"extra secret": func(r *operationResolution, _ *operations.Request) { r.Secrets["UNREFERENCED"] = "extra" },
		"ambient name": func(r *operationResolution, _ *operations.Request) { r.Source.UsernameEnv = "USER" },
		"schema":       func(_ *operationResolution, r *operations.Request) { r.Connection.Schema = "other" },
		"keyspace":     func(_ *operationResolution, r *operations.Request) { r.Connection.Database = "other.keyspace" },
		"DSN":          func(r *operationResolution, _ *operations.Request) { r.Source.DSNEnv = "KELVO_SOURCE_REQUEST_0_DSN" },
		"federation":   func(r *operationResolution, _ *operations.Request) { r.Source.Federation = &catalog.FederationConfig{} },
		"adapter":      func(r *operationResolution, _ *operations.Request) { r.Source.Adapter = "dbapi" },
	} {
		t.Run(name, func(t *testing.T) {
			r := response
			r.Secrets = make(map[string]string, len(response.Secrets))
			for k, v := range response.Secrets {
				r.Secrets[k] = v
			}
			q := request
			mutate(&r, &q)
			if validateOperationCQLCatalog(r.Source, r.Secrets, q) == nil {
				t.Fatal("unbounded CQL source accepted")
			}
		})
	}
}
