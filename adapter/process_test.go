package adapter

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func processFixture(t *testing.T, now time.Time) ProcessRequest {
	t.Helper()
	request := operations.Request{Version: operations.Version, Kind: operations.QueryRead,
		Connection: operations.ConnectionRef{ID: "saved-1", Database: "analytics", Schema: "public"},
		Spec:       operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT 1"}}}
	digest, err := operations.Digest(request)
	if err != nil {
		t.Fatal(err)
	}
	return ProcessRequest{Version: ProcessVersion, OperationID: "operation-1", RequestSHA256: digest, Request: request,
		Limits:                ProcessLimits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 16, TimeoutMS: 1000},
		Source:                ConnectionSpec{Engine: "postgresql", DSN: "postgresql://reader:test-only@db.example/analytics?sslmode=verify-full", TenantID: "tenant-1", ConnectionID: "saved-1", Revision: "revision-1", Database: "analytics", Schema: "public"},
		CredentialsValidUntil: now.Unix() + 5, ExpiresAt: now.Unix() + 60}
}

func TestProcessRequestBindsFreshSourceAndExactRequest(t *testing.T) {
	now := time.Unix(1800000000, 0)
	if err := processFixture(t, now).ValidateAt(now); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*ProcessRequest){
		"digest":                       func(r *ProcessRequest) { r.Request.Spec.Query.SQL = "SELECT 2" },
		"connection":                   func(r *ProcessRequest) { r.Source.ConnectionID = "other" },
		"database":                     func(r *ProcessRequest) { r.Source.Database = "other" },
		"schema":                       func(r *ProcessRequest) { r.Source.Schema = "other" },
		"missing revision":             func(r *ProcessRequest) { r.Source.Revision = "" },
		"expired operation":            func(r *ProcessRequest) { r.ExpiresAt = now.Unix() },
		"long operation":               func(r *ProcessRequest) { r.ExpiresAt = now.Unix() + 301 },
		"expired credentials":          func(r *ProcessRequest) { r.CredentialsValidUntil = now.Unix() },
		"long credentials":             func(r *ProcessRequest) { r.CredentialsValidUntil = now.Unix() + 6 },
		"credentials beyond operation": func(r *ProcessRequest) { r.ExpiresAt = now.Unix() + 1 },
		"option file":                  func(r *ProcessRequest) { r.Source.Options = map[string]string{"tls_ca_file": "/tmp/secret"} },
		"oversize secret":              func(r *ProcessRequest) { r.Source.Password = strings.Repeat("x", (64<<10)+1) },
		"nul secret":                   func(r *ProcessRequest) { r.Source.Password = "x\x00y" },
		"row limit":                    func(r *ProcessRequest) { r.Limits.MaxRows = 1000001 },
		"batch limit":                  func(r *ProcessRequest) { r.Limits.BatchRows = 4097 },
		"timeout":                      func(r *ProcessRequest) { r.Limits.TimeoutMS = 300001 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			r := processFixture(t, now)
			mutate(&r)
			if r.ValidateAt(now) == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}

func TestProcessPipeStrictDecoding(t *testing.T) {
	r := processFixture(t, time.Now())
	raw, err := EncodeProcessRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseProcessRequest(raw)
	if err != nil || decoded.Source.DSN != r.Source.DSN {
		t.Fatal("round trip failed", err)
	}
	for _, bad := range [][]byte{
		append(append([]byte{}, raw...), raw...),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"unknown":1`), 1),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		bytes.Repeat([]byte(" "), MaxProcessRequestBytes+1),
	} {
		if _, err := ParseProcessRequest(bad); err == nil {
			t.Fatal("invalid wire input accepted")
		}
	}
	r.CredentialsValidUntil = time.Now().Unix() - 1
	stale, _ := json.Marshal(r)
	if _, err := ParseProcessRequest(stale); err == nil {
		t.Fatal("stale pipe accepted")
	}
}

func TestBusinessProcessSourcesHaveNoEndpointOverrides(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, kind := range []string{"salesforce_data360", "salesforce_tableau_next", "ramp", "daloopa", "motherduck", "posthog"} {
		t.Run(kind, func(t *testing.T) {
			r := processFixture(t, now)
			r.Request.Connection.Schema = ""
			r.RequestSHA256, _ = operations.Digest(r.Request)
			r.Source.Engine, r.Source.DSN, r.Source.Schema, r.Source.Token = kind, "", "", "test-token"
			r.Source.Options = map[string]string{"environment": "sandbox"}
			if kind == "posthog" {
				r.Request.Connection.Database, r.Source.Database = "17", "17"
				r.RequestSHA256, _ = operations.Digest(r.Request)
				r.Source.URL, r.Source.Options = "https://analytics.example", nil
			}
			if kind == "ramp" {
				r.Source.Username = "test-client"
			}
			if err := r.ValidateAt(now); err != nil {
				t.Fatal(err)
			}
			for name, change := range map[string]func(*ProcessRequest){
				"dsn":                 func(r *ProcessRequest) { r.Source.DSN = "connection-override" },
				"url":                 func(r *ProcessRequest) { r.Source.URL = "http://other.invalid" },
				"password":            func(r *ProcessRequest) { r.Source.Password = "extra" },
				"missing token":       func(r *ProcessRequest) { r.Source.Token = "" },
				"header injection":    func(r *ProcessRequest) { r.Source.Token = "token\r\nHeader: value" },
				"custom roots":        func(r *ProcessRequest) { r.Source.Options = map[string]string{"tls_ca_pem": "override"} },
				"unknown environment": func(r *ProcessRequest) { r.Source.Options = map[string]string{"environment": "http://other.invalid"} },
				"uppercase engine":    func(r *ProcessRequest) { r.Source.Engine = strings.ToUpper(kind) },
			} {
				t.Run(name, func(t *testing.T) {
					bad := r
					change(&bad)
					if bad.ValidateAt(now) == nil {
						t.Fatal("business source override accepted")
					}
				})
			}
			if kind != "ramp" {
				r.Source.Username = "unexpected-client"
				if r.ValidateAt(now) == nil {
					t.Fatal("client ID accepted for another provider")
				}
			}
		})
	}
}
