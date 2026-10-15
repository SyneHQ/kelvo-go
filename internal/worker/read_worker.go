// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// Engines whose native reads run on read workers. The adapter opens all four
// from the same PostgreSQL URL shape, checked by readWorkerDatabase.
var readWorkerEngines = map[string]bool{"postgres": true, "cockroachdb": true, "alloydb": true, "redshift": true}

// The adapter refuses a request above these; a node allowing more keeps its
// reads on the one-shot path so both paths enforce the same limits.
const readWorkerMaxRows, readWorkerMaxBytes = 1_000_000, 64 << 20

// executeOnReadWorker runs a native read or a metadata inspection on a read
// worker. handled is false when the request should take the one-shot path:
// the pool is off, the source or limits do not qualify, or no worker could be
// obtained and nothing was sent. A worker holds its own reservation, so a read
// here takes none of its own.
func (e *Executor) executeOnReadWorker(ctx context.Context, execution delegation.Execution, dynamic bool, resolverRequest, r query.Request, sink query.Sink) (stats query.Stats, resultErr error, handled bool) {
	pool := e.WarmPool
	eligible := pool != nil && pool.Config().Enabled && pool.Adapter.Binary != "" && r.Mode == "native" && r.Mongo == nil && !r.ScanDiagnostics &&
		!access.Restricted(ctx) && e.Limits.MaxRows <= readWorkerMaxRows && e.Limits.MaxBytes <= readWorkerMaxBytes
	unavailable := query.NewError("UNAVAILABLE", "Metadata inspection requires a read worker for this connection")
	if !eligible {
		return stats, unavailable, r.Inspect != nil
	}

	source := e
	tenant, team, database, schema := "static", "", "", ""
	validUntil := time.Now().Unix() + 5
	if dynamic {
		// ponytail: resolves before any reservation; the node's job permits bound
		// it today. Phase 3 resolves once per worker instead of once per read.
		resolved, until, err := e.resolveConnections(ctx, execution, resolverRequest)
		if err != nil {
			return stats, err, true
		}
		source, validUntil = resolved, until
		selected := execution.Claims.Sources[0]
		database, schema, team = selected.Database, selected.Schema, execution.Claims.AppTeam
		identity, _ := json.Marshal([]string{execution.Claims.Issuer, execution.Claims.ClusterTenant, execution.Claims.AppTeam})
		sum := sha256.Sum256(append([]byte("kelvo.read-worker.tenant.v1\x00"), identity...))
		tenant = hex.EncodeToString(sum[:])
	}
	sources, err := source.Config.Select([]string{r.ConnectionID})
	if err != nil {
		return stats, query.NewError("INVALID_ARGUMENT", "Unknown or duplicate source"), true
	}
	s := sources[0]
	dsn := ""
	if readWorkerSourceShape(s) {
		v, ok, err := source.resolveSecret(ctx, s.DSNEnv)
		if err != nil {
			return stats, err, true
		}
		if ok {
			dsn = v
		}
	}
	db, ok := readWorkerDatabase(dsn, database)
	if !ok {
		return stats, unavailable, r.Inspect != nil
	}
	database = db

	request := operations.Request{Version: operations.Version, Connection: operations.ConnectionRef{ID: r.ConnectionID, Database: database, Schema: schema}}
	if r.Inspect != nil {
		request.Kind, request.Spec.Metadata = operations.MetadataInspect, r.Inspect
	} else {
		parameters := make([]operations.Parameter, len(r.Parameters))
		for i, p := range r.Parameters {
			parameters[i] = operations.Parameter{Type: p.Type, Value: p.Value}
		}
		request.Kind, request.Spec.Query = operations.QueryRead, &operations.QuerySpec{SQL: r.SQL, Parameters: parameters}
	}
	digest, err := operations.Digest(request)
	if err != nil {
		return stats, query.NewError("INVALID_ARGUMENT", "Invalid read request"), true
	}
	// The revision binds the source definition and its credentials, so a
	// rotated password or changed source gets a new worker and evicts the old.
	definition, _ := json.Marshal(s)
	revision := sha256.Sum256(append(append(definition, 0), dsn...))
	now := time.Now()
	expires := now.Unix() + 300
	if deadline, ok := ctx.Deadline(); ok {
		expires = min(expires, deadline.Unix())
	}
	id := make([]byte, 12)
	_, _ = rand.Read(id)
	input := adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: "read-" + hex.EncodeToString(id), RequestSHA256: digest, Request: request, AppTeam: team,
		Source:                adapter.ConnectionSpec{Engine: s.Type, DSN: dsn, Options: s.Options, TenantID: tenant, ConnectionID: r.ConnectionID, Revision: hex.EncodeToString(revision[:]), Database: database, Schema: schema},
		Limits:                adapter.ProcessLimits{MaxRows: e.Limits.MaxRows, MaxBytes: e.Limits.MaxBytes, BatchRows: 1024, TimeoutMS: min(time.Until(time.Unix(expires, 0)).Milliseconds(), e.Limits.Timeout.Milliseconds())},
		CredentialsValidUntil: min(validUntil, expires), ExpiresAt: expires}
	if input.Validate() != nil || !pool.IsEligible(input) {
		return stats, unavailable, r.Inspect != nil
	}

	quotaCtx, releaseQuota, err := source.acquireSourceQuota(ctx, r)
	if err != nil {
		return stats, err, true
	}
	defer releaseQuota()
	var receipt operations.Receipt
	if r.Inspect != nil {
		// Inspection has no one-shot query child; the operation path falls back
		// to a one-shot adapter itself.
		receipt, err = e.ExecuteOperation(withAdmissionClass(quotaCtx, admission.ClassInteractive), pool.Adapter, input, sink)
	} else {
		receipt, err = pool.Execute(quotaCtx, e, pool.Adapter, input, sink)
		if errors.Is(err, errWarmUnavailable) {
			return stats, nil, false
		}
	}
	if receipt.Result != nil {
		stats.Rows, stats.WireBytes = receipt.Result.Rows, receipt.Result.Bytes
	}
	if err == nil && receipt.Outcome != operations.Completed {
		code := receipt.ErrorCode
		if code == "" {
			code = "QUERY_FAILED"
		}
		err = query.NewError(code, "Source read failed")
	}
	return stats, err, true
}

// readWorkerSourceShape accepts a plain relational source whose only secret is
// a DSN and whose options the adapter understands.
func readWorkerSourceShape(s catalog.Source) bool {
	if !readWorkerEngines[s.Type] || s.Adapter != "" || s.Path != "" || s.Federation != nil || s.LocalSnapshot != nil || s.ObjectSnapshot != nil ||
		s.Ranges != nil || s.ParquetPaths != nil || s.Object != nil || s.Range != nil ||
		s.DSNEnv == "" || s.URLEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" || s.TokenEnv != "" {
		return false
	}
	for key := range s.Options {
		if key != "tls_ca_pem" && key != "tls_server_name" {
			return false
		}
	}
	return true
}

// readWorkerDatabase returns the DSN's database when the adapter will accept
// the DSN as is; want, when set, must name the same database.
func readWorkerDatabase(dsn, want string) (string, bool) {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.User == nil || u.Hostname() == "" || u.Fragment != "" || u.Opaque != "" || len(u.Path) < 2 {
		return "", false
	}
	if _, hasPassword := u.User.Password(); !hasPassword {
		return "", false
	}
	database := u.Path[1:]
	params, err := url.ParseQuery(u.RawQuery)
	if err != nil || (want != "" && want != database) {
		return "", false
	}
	if mode := params.Get("sslmode"); mode != "verify-full" && mode != "disable" {
		return "", false
	}
	for key, values := range params {
		if len(values) != 1 || (key != "sslmode" && key != "connect_timeout") {
			return "", false
		}
	}
	return database, true
}
