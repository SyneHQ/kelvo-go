//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/client"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"go.yaml.in/yaml/v3"
)

type applicationBinaryPin struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}
type applicationArtifactReport struct {
	Passed           bool                            `json:"passed"`
	Binaries         map[string]applicationBinaryPin `json:"binaries"`
	ForbiddenImports []string                        `json:"forbidden_imports"`
	CGOPackages      []string                        `json:"cgo_packages"`
}
type applicationAudit struct {
	Version                      int   `json:"version"`
	QueryAuthorized              int64 `json:"query_authorized"`
	QueryPolicyDenied            int64 `json:"query_policy_denied"`
	QueryAuthorizationFailed     int64 `json:"query_authorization_failed"`
	OperationAuthorized          int64 `json:"operation_authorized"`
	OperationPolicyDenied        int64 `json:"operation_policy_denied"`
	OperationAuthorizationFailed int64 `json:"operation_authorization_failed"`
	CredentialLookups            int64 `json:"credential_lookups"`
	CredentialLookupFailed       int64 `json:"credential_lookup_failed"`
}
type applicationExerciseReport struct {
	Checks            []string            `json:"checks"`
	Team              string              `json:"team"`
	Connection        string              `json:"connection"`
	Mutation          operations.Response `json:"mutation"`
	Read              operations.Response `json:"read"`
	ReadRows          int64               `json:"read_rows"`
	QueryID           string              `json:"query_id"`
	QueryInitialState string              `json:"query_initial_state"`
	QueryFailureState string              `json:"query_state_on_failure"`
	QueryErrorCode    string              `json:"query_error_code"`
	QueryStats        client.Stats        `json:"query_stats"`
	CancelledQueryID  string              `json:"cancelled_query_id"`
	CancellationState string              `json:"cancellation_state"`
}
type applicationScenarioEvidence struct {
	Checks []string         `json:"checks"`
	Before applicationAudit `json:"before"`
	After  applicationAudit `json:"after"`
}

// The separately compiled application imports only public SDK packages. Every
// source request below travels through its real mTLS authority, live gateway
// custody, real worker and disposable TLS PostgreSQL; no callback is simulated.
func operationApplicationCrossService(t *testing.T, gateway *httptest.Server, resolverURL string, authorityTLS, workerTLS TLSConfig, signingKey ed25519.PrivateKey, token string) {
	t.Helper()
	artifacts := os.Getenv("KELVO_TEST_OPERATION_APPLICATION_ARTIFACTS")
	logs := os.Getenv("KELVO_TEST_OPERATION_APPLICATION_LOG_DIRECTORY")
	if !filepath.IsAbs(artifacts) || !filepath.IsAbs(logs) {
		t.Fatal("explicit standalone application artifacts and log directory required")
	}
	var pins applicationArtifactReport
	applicationReadJSON(t, filepath.Join(artifacts, "report.json"), 1<<20, &pins)
	if !pins.Passed || len(pins.ForbiddenImports) != 0 || len(pins.CGOPackages) != 0 {
		t.Fatal("standalone application dependency boundary did not pass")
	}
	verifyBinary := func(name string) string {
		path := filepath.Join(artifacts, name)
		pin, ok := pins.Binaries[name]
		info, err := os.Lstat(path)
		if !ok || !operations.ValidDigest(pin.SHA256) || err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || info.Size() != pin.Bytes {
			t.Fatal("standalone application binary identity unavailable")
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal("standalone application binary unavailable")
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, f)
		closeErr := f.Close()
		if copyErr != nil || closeErr != nil || hex.EncodeToString(hash.Sum(nil)) != pin.SHA256 {
			t.Fatal("standalone application binary identity changed")
		}
		return path
	}
	authorityBinary, exerciseBinary := verifyBinary("authority"), verifyBinary("exercise")
	info, err := os.Lstat(logs)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		t.Fatal("standalone application logs require a private directory")
	}
	private := t.TempDir()
	caFile := filepath.Join(private, "gateway-ca.pem")
	applicationWrite(t, caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: gateway.Certificate().Raw}))
	seedFile := filepath.Join(private, "signing-seed.base64")
	applicationWrite(t, seedFile, []byte(base64.StdEncoding.EncodeToString(signingKey.Seed())))
	dsnPaths := map[string]string{}
	for _, team := range []string{"a", "b"} {
		dsn := filepath.Join(private, "saved-"+team+".dsn")
		dsnPaths[team] = dsn
		applicationWrite(t, dsn, []byte(os.Getenv("KELVO_TEST_OPERATION_POSTGRES_DSN")))
		applicationWriteYAML(t, filepath.Join(private, "saved-"+team+".yaml"), map[string]any{"revision": "1", "dsn_file": dsn, "tls_ca_file": os.Getenv("KELVO_TEST_OPERATION_SOURCE_CA_FILE")})
	}
	livePolicy := filepath.Join(private, "live-policy.yaml")
	callerPolicy := filepath.Join(private, "caller-policy.yaml")
	originalPolicy := applicationFixturePolicy(private, os.Getenv("KELVO_TEST_OPERATION_DATABASE"))
	applicationWriteYAML(t, livePolicy, originalPolicy)
	applicationWriteYAML(t, callerPolicy, originalPolicy)
	endpoint, err := url.Parse(resolverURL)
	if err != nil || endpoint.Scheme != "https" {
		t.Fatal("invalid standalone authority endpoint")
	}
	config := func(policy string, sign bool) map[string]any {
		identity := map[string]any{"issuer": "fixture-gateway", "audience": "operations", "cluster_tenant": "a", "service_principal": "api", "public_key_env": "KELVO_APPLICATION_PUBLIC_KEY"}
		if sign {
			identity["signing_seed_file"] = seedFile
		}
		return map[string]any{"version": 1, "identity": identity, "gateway": map[string]any{"url": gateway.URL, "bearer_token_env": "KELVO_APPLICATION_TOKEN", "ca_file": caFile}, "authority": map[string]any{"listen": endpoint.Host, "ca_file": authorityTLS.CAFile, "cert_file": authorityTLS.CertFile, "key_file": authorityTLS.KeyFile}, "policy_file": policy}
	}
	authorityConfig, callerConfig := filepath.Join(private, "authority.yaml"), filepath.Join(private, "caller.yaml")
	applicationWriteYAML(t, authorityConfig, config(livePolicy, false))
	applicationWriteYAML(t, callerConfig, config(callerPolicy, true))
	env := []string{"GOMAXPROCS=1", "GOMEMLIMIT=192MiB", "KELVO_APPLICATION_PUBLIC_KEY=" + base64.StdEncoding.EncodeToString(signingKey.Public().(ed25519.PublicKey)), "KELVO_APPLICATION_TOKEN=" + token, "TMPDIR=" + private}
	for _, name := range []string{"PATH", "HOME", "LANG"} {
		if value := os.Getenv(name); value != "" {
			env = append(env, name+"="+value)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 330*time.Second)
	defer cancel()
	auditPath := filepath.Join(logs, "authority-audit.json")
	authorityLog, err := os.OpenFile(filepath.Join(logs, "authority.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal("cannot retain standalone authority log")
	}
	authority := exec.CommandContext(ctx, authorityBinary, "-config", authorityConfig, "-audit-file", auditPath)
	authority.Env, authority.Dir, authority.Stdout, authority.Stderr = env, private, authorityLog, authorityLog
	// A panic or test timeout can skip this helper's defers. The outer Python
	// supervisor also reaps the exact owned cgroup before declaring cleanup.
	authority.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	authority.WaitDelay = 3 * time.Second
	if err := authority.Start(); err != nil {
		_ = authorityLog.Close()
		t.Fatal("standalone authority did not start")
	}
	done := make(chan error, 1)
	go func() { done <- authority.Wait() }()
	var stopped bool
	var exitErr error
	evidence := struct {
		Passed          bool                                   `json:"passed"`
		AuthorityReaped bool                                   `json:"authority_reaped"`
		Binaries        map[string]applicationBinaryPin        `json:"binaries"`
		Scenarios       map[string]applicationScenarioEvidence `json:"scenarios"`
	}{Binaries: pins.Binaries, Scenarios: map[string]applicationScenarioEvidence{}}
	alive := func(t *testing.T) {
		t.Helper()
		select {
		case exitErr = <-done:
			stopped = true
			t.Fatal("standalone authority exited; retained log contains diagnostics")
		default:
		}
	}
	defer func() {
		if !stopped {
			// os.Process retains the Linux process handle across Wait; avoid
			// signalling a recycled numeric process-group ID after an early exit.
			_ = authority.Process.Signal(syscall.SIGTERM)
			select {
			case exitErr = <-done:
				stopped = true
			case <-time.After(7 * time.Second):
				_ = authority.Process.Kill()
				select {
				case exitErr = <-done:
					stopped = true
				case <-time.After(3 * time.Second):
				}
			}
		}
		_ = authorityLog.Close()
		evidence.AuthorityReaped = stopped && errors.Is(syscall.Kill(authority.Process.Pid, 0), syscall.ESRCH)
		if !evidence.AuthorityReaped || exitErr != nil {
			t.Error("standalone authority failed bounded clean shutdown")
		}
		evidence.Passed = !t.Failed() && evidence.AuthorityReaped && len(evidence.Scenarios) == 8
		raw, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil || os.WriteFile(filepath.Join(logs, "report.json"), append(raw, '\n'), 0600) != nil {
			t.Error("cannot retain standalone application evidence")
		}
	}()
	applicationWaitAuthority(t, ctx, endpoint.Host, workerTLS, func() { alive(t) })
	readAudit := func(t *testing.T) applicationAudit {
		t.Helper()
		var a applicationAudit
		applicationReadJSON(t, auditPath, 4096, &a)
		if a.Version != 1 {
			t.Fatal("standalone authority audit unavailable")
		}
		return a
	}
	run := func(t *testing.T, name, configPath, team, subject, connection, runID, mode string, denied bool) applicationExerciseReport {
		t.Helper()
		alive(t)
		args := []string{"-config", configPath, "-team", team, "-subject", subject, "-connection", connection, "-run-id", runID, "-journal", filepath.Join(private, name+"-journal.json"), "-mode", mode}
		if denied {
			args = append(args, "-expect-denied")
		}
		out, err := os.OpenFile(filepath.Join(logs, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal("cannot retain application exercise report")
		}
		defer out.Close()
		diagnostics, err := os.OpenFile(filepath.Join(logs, name+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal("cannot retain application exercise diagnostics")
		}
		defer diagnostics.Close()
		runCtx, runCancel := context.WithTimeout(ctx, 90*time.Second)
		defer runCancel()
		cmd := exec.CommandContext(runCtx, exerciseBinary, args...)
		cmd.Env, cmd.Dir, cmd.Stdout, cmd.Stderr = env, private, out, diagnostics
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
		cmd.WaitDelay = 3 * time.Second
		if err := cmd.Run(); err != nil {
			t.Fatal("standalone application exercise failed; private diagnostics retained")
		}
		alive(t)
		var result applicationExerciseReport
		applicationReadJSON(t, filepath.Join(logs, name+".json"), 1<<20, &result)
		if result.Team != team || result.Connection != connection || result.QueryID == "" || !slices.Contains(result.Checks, "query_status_observed") {
			t.Fatal("standalone report lost its request identity")
		}
		if denied {
			if len(result.Checks) != 2 || !slices.Contains(result.Checks, "query_denied") || result.QueryFailureState != "failed" || result.QueryErrorCode != "PERMISSION_DENIED" || result.QueryStats.Rows != 0 || result.QueryStats.Server.Rows != 0 || result.QueryStats.WireBytes != 0 {
				t.Fatal("standalone query did not confirm terminal resolver permission denial")
			}
		} else {
			if !slices.Contains(result.Checks, "analytical_result_verified") || result.QueryStats.Rows != 1 || result.QueryStats.Server.Rows != 1 || result.QueryStats.WireBytes < 1 {
				t.Fatal("standalone analytical result was not verified")
			}
			if mode != "query" && (result.Read.Validate() != nil || result.Read.Receipt == nil || result.Read.Receipt.Result == nil || result.Read.Receipt.Result.Rows != 1 || result.ReadRows != 1 || !slices.Contains(result.Checks, "operation_result_verified")) {
				t.Fatal("standalone immutable operation result was not verified")
			}
			if mode == "all" && (result.Mutation.Validate() != nil || result.Mutation.Receipt == nil || result.Mutation.Receipt.Effect != operations.EffectCommitted || !slices.Contains(result.Checks, "mutation_committed") || !slices.Contains(result.Checks, "cancellation_confirmed") || result.CancellationState != "cancelled" || result.CancelledQueryID == "") {
				t.Fatal("standalone mutation or cancellation was not confirmed")
			}
		}
		return result
	}
	positive := func(name, team, subject, connection, runID, mode string) bool {
		return t.Run(name, func(t *testing.T) {
			before := readAudit(t)
			result := run(t, name, callerConfig, team, subject, connection, runID, mode, false)
			after := readAudit(t)
			if after.CredentialLookups <= before.CredentialLookups || after.QueryAuthorized <= before.QueryAuthorized || after.QueryPolicyDenied != before.QueryPolicyDenied || after.CredentialLookupFailed != before.CredentialLookupFailed {
				t.Fatal("successful application request did not use current authority and fresh credentials")
			}
			evidence.Scenarios[name] = applicationScenarioEvidence{result.Checks, before, after}
		})
	}
	if !positive("team-a", "team-a", "alice", "saved-a", "smoke-a-001", "all") || !positive("team-b", "team-b", "bob", "saved-b", "smoke-b-001", "all") {
		return
	}
	if !t.Run("revoked-membership", func(t *testing.T) {
		before := readAudit(t)
		revoked := applicationFixturePolicy(private, os.Getenv("KELVO_TEST_OPERATION_DATABASE"))
		delete(revoked["subjects"].(map[string]any)["alice"].(map[string]any)["connections"].(map[string]string), "saved-a")
		applicationWriteYAML(t, livePolicy, revoked)
		defer applicationWriteYAML(t, livePolicy, originalPolicy)
		result := run(t, "revoked-membership", callerConfig, "team-a", "alice", "saved-a", "smoke-a-001", "query", true)
		after := readAudit(t)
		if after.QueryPolicyDenied <= before.QueryPolicyDenied || after.CredentialLookups != before.CredentialLookups || after.QueryAuthorizationFailed != before.QueryAuthorizationFailed {
			t.Fatal("membership revocation was not denied before credential lookup")
		}
		evidence.Scenarios["revoked-membership"] = applicationScenarioEvidence{result.Checks, before, after}
	}) {
		return
	}
	if !positive("membership-restored", "team-a", "alice", "saved-a", "smoke-a-001", "read") {
		return
	}
	if !t.Run("fresh-credentials-required", func(t *testing.T) {
		before := readAudit(t)
		missing := dsnPaths["a"] + ".withdrawn"
		if err := os.Rename(dsnPaths["a"], missing); err != nil {
			t.Fatal("cannot withdraw fixture credential file")
		}
		defer func() {
			if err := os.Rename(missing, dsnPaths["a"]); err != nil {
				t.Error("cannot restore fixture credential file")
			}
		}()
		result := run(t, "fresh-credentials-required", callerConfig, "team-a", "alice", "saved-a", "smoke-a-001", "query", true)
		after := readAudit(t)
		// Resolver callbacks fail closed with HTTP 403 for both policy and
		// credential failures. The terminal code alone cannot prove a fresh
		// lookup: require the independent authorization and credential counters.
		if after.QueryAuthorized <= before.QueryAuthorized || after.QueryAuthorizationFailed != before.QueryAuthorizationFailed || after.CredentialLookupFailed <= before.CredentialLookupFailed || after.CredentialLookups <= before.CredentialLookups || after.QueryPolicyDenied != before.QueryPolicyDenied {
			t.Fatal("credential withdrawal did not force a fresh failed lookup")
		}
		evidence.Scenarios["fresh-credentials-required"] = applicationScenarioEvidence{result.Checks, before, after}
	}) {
		return
	}
	if !positive("credentials-restored", "team-a", "alice", "saved-a", "smoke-a-001", "read") {
		return
	}
	if !t.Run("cross-team-forged-caller", func(t *testing.T) {
		before := readAudit(t)
		forged := applicationFixturePolicy(private, os.Getenv("KELVO_TEST_OPERATION_DATABASE"))
		forged["subjects"].(map[string]any)["alice"].(map[string]any)["connections"].(map[string]string)["saved-b"] = "write"
		forged["connections"].(map[string]any)["saved-b"].(map[string]any)["team"] = "team-a"
		forgedPolicy, forgedConfig := filepath.Join(private, "forged-policy.yaml"), filepath.Join(private, "forged-caller.yaml")
		applicationWriteYAML(t, forgedPolicy, forged)
		applicationWriteYAML(t, forgedConfig, config(forgedPolicy, true))
		result := run(t, "cross-team-forged-caller", forgedConfig, "team-a", "alice", "saved-b", "smoke-b-001", "query", true)
		after := readAudit(t)
		if after.QueryPolicyDenied <= before.QueryPolicyDenied || after.CredentialLookups != before.CredentialLookups || after.QueryAuthorizationFailed != before.QueryAuthorizationFailed {
			t.Fatal("forged caller scope was not isolated from the other team's source")
		}
		evidence.Scenarios["cross-team-forged-caller"] = applicationScenarioEvidence{result.Checks, before, after}
	}) {
		return
	}
	if !positive("cross-team-restored", "team-b", "bob", "saved-b", "smoke-b-001", "read") {
		return
	}
	verifyBinary("authority")
	verifyBinary("exercise")
	alive(t)
}

func applicationFixturePolicy(directory, database string) map[string]any {
	connections := map[string]any{}
	for _, team := range []string{"a", "b"} {
		connections["saved-"+team] = map[string]any{"team": "team-" + team, "type": "postgres", "database": database, "schema": "public", "revision": "1", "credentials_file": filepath.Join(directory, "saved-"+team+".yaml"), "read_sql": "SELECT id,value FROM public.team_" + team + "_rows WHERE id=$1", "write_sql": "INSERT INTO public.team_" + team + "_rows(id,value) VALUES($1,$2)", "cancel_sql": "SELECT pg_sleep(30)"}
	}
	return map[string]any{"version": 1, "subjects": map[string]any{"alice": map[string]any{"kind": "user", "team": "team-a", "connections": map[string]string{"saved-a": "write"}}, "bob": map[string]any{"kind": "user", "team": "team-b", "connections": map[string]string{"saved-b": "write"}}}, "connections": connections}
}

func applicationWrite(t *testing.T, path string, raw []byte) {
	t.Helper()
	f, err := os.CreateTemp(filepath.Dir(path), ".application-fixture-*")
	if err != nil {
		t.Fatal("cannot stage private application fixture")
	}
	defer os.Remove(f.Name())
	n, writeErr := f.Write(raw)
	syncErr := f.Sync()
	closeErr := f.Close()
	if n != len(raw) || writeErr != nil || syncErr != nil || closeErr != nil || os.Rename(f.Name(), path) != nil {
		t.Fatal("cannot atomically publish application fixture")
	}
}
func applicationWriteYAML(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := yaml.Marshal(value)
	if err != nil {
		t.Fatal("invalid application fixture schema")
	}
	applicationWrite(t, path, raw)
}
func applicationReadJSON(t *testing.T, path string, limit int64, value any) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal("application evidence unavailable")
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(raw)) > limit || json.Unmarshal(raw, value) != nil {
		t.Fatal("application evidence invalid")
	}
}
func applicationWaitAuthority(t *testing.T, ctx context.Context, address string, workerTLS TLSConfig, alive func()) {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(workerTLS.CertFile, workerTLS.KeyFile)
	if err != nil {
		t.Fatal("worker readiness identity unavailable")
	}
	raw, err := os.ReadFile(workerTLS.CAFile)
	if err != nil {
		t.Fatal("worker readiness trust unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(raw) {
		t.Fatal("worker readiness trust invalid")
	}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}}, DisableKeepAlives: true, TLSHandshakeTimeout: time.Second}
	defer transport.CloseIdleConnections()
	c := &http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		alive()
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+address+resolver.QueryPath, strings.NewReader(`{}`))
		if err != nil {
			t.Fatal("invalid authority readiness probe")
		}
		r.Header.Set("Content-Type", "application/json")
		response, err := c.Do(r)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusBadRequest {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("authority startup deadline")
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("standalone authority did not become ready")
}
