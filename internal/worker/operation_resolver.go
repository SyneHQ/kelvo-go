// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"maps"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

type operationResolutionRequest struct {
	Grant       string             `json:"grant"`
	Operation   operations.Request `json:"operation"`
	OperationID string             `json:"operation_id"`
	WorkerID    string             `json:"worker_id"`
	Owner       string             `json:"owner"`
	Claim       string             `json:"claim"`
}

type operationResolution struct {
	Version        int               `json:"version"`
	GrantSHA256    string            `json:"grant_sha256"`
	RequestSHA256  string            `json:"request_sha256"`
	SourceRevision string            `json:"source_revision"`
	ValidUntil     int64             `json:"valid_until"`
	Source         catalog.Source    `json:"source"`
	Secrets        map[string]string `json:"secrets"`
}

// ResolveOperationSource is called only by an admitted Running operation. The
// private resolver independently verifies the signed request and current worker
// lease. No credential is returned by a queue scan or by reading operation status.
func (e *Executor) ResolveOperationSource(ctx context.Context, record operationstore.Record, request operations.Request) (adapter.ProcessRequest, error) {
	return e.ResolveOperationSourceWithInput(ctx, record, request, nil)
}

// ResolveOperationSourceWithInput accepts only already verified sealed bytes.
// The caller loads them under resource admission before asking for credentials.
func (e *Executor) ResolveOperationSourceWithInput(ctx context.Context, record operationstore.Record, request operations.Request, payload []byte) (adapter.ProcessRequest, error) {
	if adapter.ValidateOperationInput(request, record.Scope.AppTeam, payload) != nil {
		return adapter.ProcessRequest{}, connectionUnavailable()
	}
	if ctx == nil || e == nil || request.Validate() != nil || record.State != operationstore.Running || record.Binding.Validate() != nil || record.Scope.Validate() != nil || record.AuthoritySHA256 != operations.GrantDigest(record.AuthorityToken) || request.Connection.ID != record.Scope.ConnectionID || request.Kind != record.Kind {
		return adapter.ProcessRequest{}, connectionUnavailable()
	}
	digest, err := operations.Digest(request)
	if err != nil || digest != record.RequestSHA256 {
		return adapter.ProcessRequest{}, connectionUnavailable()
	}
	resolver := e.connectionResolvers[record.Scope.Issuer]
	response, err := resolver.resolveOperation(ctx, record, request)
	if err != nil {
		return adapter.ProcessRequest{}, err
	}
	defer clear(response.Secrets)
	var snapshot *filesnapshot.Descriptor
	if filesnapshot.FormatSupported(response.Source.Type) {
		snapshot, err = operationFileDescriptor(response.Source, response.Secrets)
		if err != nil {
			return adapter.ProcessRequest{}, err
		}
	} else if response.Source.Type == "redis" {
		if err := validateOperationRedisCatalog(response, request); err != nil {
			return adapter.ProcessRequest{}, err
		}
	} else if adapter.NativeReader(response.Source.Type) {
		if err := validateOperationNativeReaderCatalog(response, request); err != nil {
			return adapter.ProcessRequest{}, err
		}
	} else if response.Source.Type == "google_sheets" {
		if err := validateOperationSheetsCatalog(response, request); err != nil {
			return adapter.ProcessRequest{}, err
		}
	} else if adapter.JDBCProfile(response.Source.Type) {
		if err := validateOperationJDBCCatalog(response, request); err != nil {
			return adapter.ProcessRequest{}, err
		}
	} else if provider.SaaS(response.Source.Type) {
		if err := validateOperationSaaSCatalog(response, request); err != nil {
			return adapter.ProcessRequest{}, err
		}
	} else if provider.Supported(response.Source.Type) {
		if err := validateOperationBusinessCatalog(response); err != nil {
			return adapter.ProcessRequest{}, err
		}
	} else {
		selection := delegation.Source{Alias: "source_1", ConnectionID: record.Scope.ConnectionID, Database: request.Connection.Database, Schema: request.Connection.Schema}
		validatedSource := response.Source
		validatedSource.Options = maps.Clone(response.Source.Options)
		if err := validateOperationTLSOptions(validatedSource.Options); err != nil {
			return adapter.ProcessRequest{}, err
		}
		delete(validatedSource.Options, "tls_ca_pem")
		delete(validatedSource.Options, "tls_server_name")
		if pin, ok := validatedSource.Options["migration_server_uuid"]; ok {
			if validatedSource.Type != "clickhouse" || !validOperationServerUUID(pin) {
				return adapter.ProcessRequest{}, connectionUnavailable()
			}
			delete(validatedSource.Options, "migration_server_uuid")
		}
		// Reuse the exact-secret-map and native DSN validation. A response cannot
		// smuggle paths, environment fallbacks, another database, or adapter URLs.
		if validatedSource.Type == "cassandra" || validatedSource.Type == "scylla" {
			if err := validateOperationCQLCatalog(validatedSource, response.Secrets, request); err != nil {
				return adapter.ProcessRequest{}, err
			}
		} else if err := validateConnectionCatalog(connectionResolution{Sources: []catalog.Source{validatedSource}, Secrets: response.Secrets},
			delegation.Claims{Sources: []delegation.Source{selection}}, query.Request{Mode: "native", ConnectionID: selection.Alias}); err != nil {
			return adapter.ProcessRequest{}, err
		}
	}
	source := response.Source
	identity, _ := json.Marshal([]string{record.Scope.ClusterTenant, record.Scope.AppTeam})
	tenantHash := sha256.Sum256(append([]byte("kelvo.operation.adapter-tenant.v1\x00"), identity...))
	connection := adapter.ConnectionSpec{Engine: source.Type, TenantID: hex.EncodeToString(tenantHash[:]), ConnectionID: request.Connection.ID,
		Revision: response.SourceRevision, Database: request.Connection.Database, Schema: request.Connection.Schema,
		DSN: response.Secrets[source.DSNEnv], URL: response.Secrets[source.URLEnv], Username: response.Secrets[source.UsernameEnv],
		Password: response.Secrets[source.PasswordEnv], Token: response.Secrets[source.TokenEnv], Options: maps.Clone(source.Options)}
	expires := record.ExecuteBefore.Unix()
	if deadline, ok := ctx.Deadline(); ok {
		expires = min(expires, deadline.Unix())
	}
	input := adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: record.ID, RequestSHA256: record.RequestSHA256,
		AppTeam: record.Scope.AppTeam, Input: payload, SourceFile: snapshot,
		Request: request, Source: connection, CredentialsValidUntil: min(response.ValidUntil, expires), ExpiresAt: expires,
		Limits: adapter.ProcessLimits{MemoryMB: e.Limits.MemoryMB, Threads: e.Limits.Threads, MaxTempMB: e.Limits.MaxTempMB, MaxRows: min(e.Limits.MaxRows, 1_000_000), MaxBytes: min(e.Limits.MaxBytes, 64<<20), BatchRows: 1024, TimeoutMS: min(time.Until(time.Unix(expires, 0)).Milliseconds(), e.Limits.Timeout.Milliseconds())}}
	if err := input.Validate(); err != nil {
		return adapter.ProcessRequest{}, connectionUnavailable()
	}
	return input, nil
}

// Business adapters select a compiled provider origin. The private resolver may
// supply only the current token and Ramp client ID, never another connection
// channel or an analytical source attachment.
func validateOperationBusinessCatalog(response operationResolution) error {
	s := response.Source
	if s.ID != "source_1" || s.Type != strings.ToLower(s.Type) || s.Adapter != "" || s.Path != "" || s.DSNEnv != "" || (s.URLEnv != "" && s.Type != "posthog") || s.PasswordEnv != "" || s.TokenEnv != "KELVO_SOURCE_REQUEST_0_TOKEN" || s.Federation != nil || s.LocalSnapshot != nil || s.ObjectSnapshot != nil || s.ParquetPaths != nil || s.Ranges != nil || s.Object != nil || s.Range != nil {
		return connectionUnavailable()
	}
	refs := []string{s.TokenEnv}
	if s.Type == "posthog" {
		if s.URLEnv != "KELVO_SOURCE_REQUEST_0_URL" {
			return connectionUnavailable()
		}
		refs = append(refs, s.URLEnv)
	}
	if s.UsernameEnv != "" {
		if s.Type != "ramp" || s.UsernameEnv != "KELVO_SOURCE_REQUEST_0_USERNAME" {
			return connectionUnavailable()
		}
		refs = append(refs, s.UsernameEnv)
	}
	if len(refs) != len(response.Secrets) {
		return connectionUnavailable()
	}
	for _, name := range refs {
		if response.Secrets[name] == "" {
			return connectionUnavailable()
		}
	}
	return nil
}

func validateOperationCQLCatalog(s catalog.Source, secrets map[string]string, request operations.Request) error {
	if s.ID != "source_1" || s.Adapter != "" || s.Path != "" || s.DSNEnv != "" || s.TokenEnv != "" || len(s.Options) != 0 || s.URLEnv != "KELVO_SOURCE_REQUEST_0_URL" || s.UsernameEnv != "KELVO_SOURCE_REQUEST_0_USERNAME" || s.PasswordEnv != "KELVO_SOURCE_REQUEST_0_PASSWORD" || s.Federation != nil || s.LocalSnapshot != nil || s.ObjectSnapshot != nil || s.ParquetPaths != nil || s.Ranges != nil || s.Object != nil || s.Range != nil || len(secrets) != 3 || request.Connection.Schema != "" || !catalog.ValidID(request.Connection.Database) || len(request.Connection.Database) > 48 {
		return connectionUnavailable()
	}
	for _, name := range []string{s.URLEnv, s.UsernameEnv, s.PasswordEnv} {
		if secrets[name] == "" || strings.ContainsAny(secrets[name], "\r\n\x00") {
			return connectionUnavailable()
		}
	}
	u, err := url.Parse(secrets[s.URLEnv])
	if err != nil || u.Scheme != "tls" || u.User != nil || u.Hostname() == "" || strings.ContainsAny(u.Hostname(), ",/\\ \t%") || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return connectionUnavailable()
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return connectionUnavailable()
	}
	return nil
}

// Operation adapters receive inline trust roots over the private resolver
// channel. These are not paths, ambient trust overrides or TLS bypass flags.
// Keep the ordinary analytical catalog validator unchanged.
func validateOperationTLSOptions(options map[string]string) error {
	if name, ok := options["tls_server_name"]; ok {
		if name == "" || len(name) > 253 || strings.ContainsAny(name, "\x00\r\n/,\\ \t") || (strings.Contains(name, ":") && net.ParseIP(name) == nil) {
			return connectionUnavailable()
		}
	}
	if raw, ok := options["tls_ca_pem"]; ok {
		if len(raw) == 0 || len(raw) > 64<<10 {
			return connectionUnavailable()
		}
		remaining := []byte(raw)
		count := 0
		for len(bytes.TrimSpace(remaining)) > 0 {
			remaining = bytes.TrimSpace(remaining)
			if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
				return connectionUnavailable()
			}
			block, rest := pem.Decode(remaining)
			if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
				return connectionUnavailable()
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil || !cert.IsCA || !cert.BasicConstraintsValid || time.Now().Before(cert.NotBefore) || !time.Now().Before(cert.NotAfter) {
				return connectionUnavailable()
			}
			count++
			if count > 8 {
				return connectionUnavailable()
			}
			remaining = rest
		}
		if count == 0 {
			return connectionUnavailable()
		}
	}
	return nil
}

func (r *ConnectionResolver) resolveOperation(ctx context.Context, record operationstore.Record, request operations.Request) (out operationResolution, resultErr error) {
	defer func() {
		if resultErr != nil {
			clear(out.Secrets)
			out = operationResolution{}
		}
	}()
	if r == nil || r.client == nil || r.slots == nil || !strings.HasSuffix(r.url, "/internal/kelvo/resolve") {
		return out, connectionUnavailable()
	}
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	case <-ctx.Done():
		return out, ctx.Err()
	}
	raw, err := json.Marshal(operationResolutionRequest{Grant: record.AuthorityToken, Operation: request, OperationID: record.ID, WorkerID: record.Binding.WorkerID, Owner: record.Binding.Owner, Claim: record.Binding.Claim})
	if err != nil || len(raw) > operations.MaxRequestBytes+operations.MaxGrantBytes+4096 {
		return out, connectionUnavailable()
	}
	defer clear(raw)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(r.url, "/resolve")+"/resolve-operation", bytes.NewReader(raw))
	if err != nil {
		return out, connectionUnavailable()
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := r.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		return out, connectionUnavailable()
	}
	defer resp.Body.Close()
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Encoding") != "" || resp.ContentLength > maxConnectionResponseBytes {
		return out, connectionUnavailable()
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxConnectionResponseBytes+1))
	defer clear(data)
	if err != nil || len(data) > maxConnectionResponseBytes || operations.DecodeStrict(data, &out, maxConnectionResponseBytes) != nil {
		return out, connectionUnavailable()
	}
	now := time.Now()
	peerUntil, ok := resolverCertificateExpiry(resp.TLS, now)
	if !ok || ctx.Err() != nil || out.Version != 1 || out.GrantSHA256 != record.AuthoritySHA256 || out.RequestSHA256 != record.RequestSHA256 || !operations.ValidDigest(out.SourceRevision) || out.ValidUntil <= now.Unix() || out.ValidUntil > now.Unix()+5 || out.ValidUntil > record.AuthorityUntil.Unix() {
		return out, connectionUnavailable()
	}
	out.ValidUntil = min(out.ValidUntil, peerUntil.Unix())
	if out.ValidUntil <= now.Unix() || len(out.Secrets) > 5 {
		return out, connectionUnavailable()
	}
	for _, value := range out.Secrets {
		if len(value) == 0 || len(value) > 32<<10 || strings.IndexByte(value, 0) >= 0 {
			return out, connectionUnavailable()
		}
	}
	return out, nil
}

// The topology pin is private operator configuration, never a SQL option.
func validOperationServerUUID(value string) bool {
	if len(value) != 36 || value == "00000000-0000-0000-0000-000000000000" {
		return false
	}
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
