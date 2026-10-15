// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
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
//
// Caller authority (principal, policy, delegation) has been checked for this
// read before it gets here. What is reused is the source authorization: the
// first read for a connection resolves its credentials and they go to the
// worker; later reads within reauthorize_every carry no secrets. Past that, or
// when a new worker is needed, a read authorizes the source again; a failure
// or a changed revision kills the workers of the old authorization.
func (e *Executor) executeOnReadWorker(ctx context.Context, execution delegation.Execution, dynamic bool, resolverRequest, r query.Request, sink query.Sink) (stats query.Stats, resultErr error, handled bool) {
	pool := e.WarmPool
	eligible := pool != nil && pool.Config().Enabled && pool.Adapter.Binary != "" && r.Mode == "native" && r.Mongo == nil && !r.ScanDiagnostics &&
		!access.Restricted(ctx) && e.Limits.MaxRows <= readWorkerMaxRows && e.Limits.MaxBytes <= readWorkerMaxBytes
	unavailable := query.NewError("UNAVAILABLE", "Metadata inspection requires a read worker for this connection")
	if !eligible {
		return stats, unavailable, r.Inspect != nil
	}

	tenant, team, database, schema := "static", "", "", ""
	if dynamic {
		selected := execution.Claims.Sources[0]
		database, schema, team = selected.Database, selected.Schema, execution.Claims.AppTeam
		identity, _ := json.Marshal([]string{execution.Claims.Issuer, execution.Claims.ClusterTenant, execution.Claims.AppTeam})
		sum := sha256.Sum256(append([]byte("kelvo.read-worker.tenant.v1\x00"), identity...))
		tenant = hex.EncodeToString(sum[:])
	}
	authKey := strings.Join([]string{tenant, team, r.ConnectionID, database, schema}, "\x00")
	auth, dsn := pool.freshAuthorization(authKey), ""
	// authorize resolves the source again; ok is false when it no longer
	// qualifies for a read worker.
	authorize := func() (ok bool, err error) {
		a, secret, err := e.authorizeReadSource(ctx, execution, dynamic, resolverRequest, r.ConnectionID, tenant, team, database)
		if err != nil || a == nil {
			pool.revokeAuthorization(authKey)
			return false, err
		}
		pool.storeAuthorization(authKey, a)
		auth, dsn = a, secret
		return true, nil
	}
	if auth == nil {
		if ok, err := authorize(); err != nil || !ok {
			return stats, firstError(err, unavailable), err != nil || r.Inspect != nil
		}
	}

	request := operations.Request{Version: operations.Version, Connection: operations.ConnectionRef{ID: r.ConnectionID, Database: auth.database, Schema: schema}}
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
	expires := time.Now().Unix() + 300
	if deadline, ok := ctx.Deadline(); ok {
		expires = min(expires, deadline.Unix())
	}
	// build makes the worker request from the current authorization; it
	// carries the credentials only when this read just resolved them.
	build := func() (adapter.ProcessRequest, bool) {
		request.Connection.Database = auth.database
		digest, err = operations.Digest(request)
		id := make([]byte, 12)
		_, _ = rand.Read(id)
		source := adapter.ConnectionSpec{Engine: auth.def.Type, TenantID: tenant, ConnectionID: r.ConnectionID, Revision: auth.revision, Database: auth.database, Schema: schema}
		if dsn != "" {
			source.DSN, source.Options = dsn, auth.def.Options
		}
		input := adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: "read-" + hex.EncodeToString(id), RequestSHA256: digest, Request: request, AppTeam: team, Source: source,
			Limits:                adapter.ProcessLimits{MaxRows: e.Limits.MaxRows, MaxBytes: e.Limits.MaxBytes, BatchRows: 1024, TimeoutMS: min(time.Until(time.Unix(expires, 0)).Milliseconds(), e.Limits.Timeout.Milliseconds())},
			CredentialsValidUntil: min(time.Now().Unix()+5, expires), ExpiresAt: expires}
		return input, err == nil && input.Validate() == nil && pool.IsEligible(input)
	}
	input, ok := build()
	if !ok {
		return stats, unavailable, r.Inspect != nil
	}
	// withCredentials re-authorizes a secretless read that found no worker
	// holding credentials, and rebuilds it with them.
	withCredentials := func() (adapter.ProcessRequest, error) {
		if dsn == "" {
			if ok, err := authorize(); err != nil || !ok {
				return input, firstError(err, unavailable)
			}
		}
		input, ok = build()
		if !ok {
			return input, unavailable
		}
		return input, nil
	}

	if r.Inspect != nil {
		quotaCtx, releaseQuota, err := auth.source.acquireSourceQuota(ctx, r)
		if err != nil {
			return stats, err, true
		}
		defer releaseQuota()
		var receipt operations.Receipt
		if dsn == "" {
			receipt, err = pool.execute(quotaCtx, e, pool.Adapter, input, sink, nil)
		}
		if dsn != "" || errors.Is(err, errNeedsCredentials) || errors.Is(err, errWarmUnavailable) {
			if input, err = withCredentials(); err != nil {
				return stats, err, true
			}
			// Inspection has no one-shot query child; the operation path falls
			// back to a one-shot adapter itself.
			receipt, err = e.ExecuteOperation(withAdmissionClass(quotaCtx, admission.ClassInteractive), pool.Adapter, input, sink)
		}
		return receiptStats(receipt, err), receiptError(receipt, err), true
	}

	// run executes the read on a worker, copying its result to buffer if any.
	run := func(buffer *cappedBuffer) (query.Stats, error, bool) {
		quotaCtx, releaseQuota, err := auth.source.acquireSourceQuota(ctx, r)
		if err != nil {
			return query.Stats{}, err, false
		}
		defer releaseQuota()
		var tee io.Writer
		if buffer != nil {
			tee = buffer
		}
		receipt, err := pool.execute(quotaCtx, e, pool.Adapter, input, sink, tee)
		if errors.Is(err, errNeedsCredentials) {
			if input, err = withCredentials(); err != nil {
				return query.Stats{}, err, false
			}
			receipt, err = pool.execute(quotaCtx, e, pool.Adapter, input, sink, tee)
		}
		err = receiptError(receipt, err)
		return receiptStats(receipt, err), err, err == nil
	}
	if pool.cache == nil {
		stats, err, _ = run(nil)
	} else {
		key := resultCacheKey(tenant, team, r.ConnectionID, auth.revision, r, e.Limits)
		stats, err = pool.cache.do(ctx, key, r.Refresh, run, func(entry *cacheEntry) (query.Stats, error) {
			observed, err := e.readIPC(ctx, bytes.NewReader(entry.data), e.Limits, sink)
			stats := entry.stats
			stats.Rows, stats.WireBytes = observed.Rows, observed.WireBytes
			return stats, err
		})
	}
	if errors.Is(err, errWarmUnavailable) {
		return stats, nil, false
	}
	return stats, err, true
}

// authorizeReadSource resolves a connection's source and credentials. It
// returns nil without an error when the source does not qualify for a read
// worker. The authorization keeps no secret values; dsn is for this read only.
func (e *Executor) authorizeReadSource(ctx context.Context, execution delegation.Execution, dynamic bool, resolverRequest query.Request, connection, tenant, team, database string) (*readAuthorization, string, error) {
	source := e
	if dynamic {
		resolved, _, err := e.resolveConnections(ctx, execution, resolverRequest)
		if err != nil {
			return nil, "", err
		}
		source = resolved
	}
	sources, err := source.Config.Select([]string{connection})
	if err != nil {
		return nil, "", query.NewError("INVALID_ARGUMENT", "Unknown or duplicate source")
	}
	s := sources[0]
	if !readWorkerSourceShape(s) {
		return nil, "", nil
	}
	dsn, present, err := source.resolveSecret(ctx, s.DSNEnv)
	if err != nil {
		return nil, "", err
	}
	if !present {
		return nil, "", query.NewError("CONFIGURATION_ERROR", "A source environment variable is missing")
	}
	db, ok := readWorkerDatabase(dsn, database)
	if !ok {
		return nil, "", nil
	}
	// The revision binds the source definition and its credentials, so a
	// rotated password or changed source gets a new worker and kills the old.
	definition, _ := json.Marshal(s)
	sum := sha256.Sum256(append(append(definition, 0), dsn...))
	revision := hex.EncodeToString(sum[:])
	quota := *source
	quota.Secrets = nil
	return &readAuthorization{source: &quota, def: s, revision: revision, database: db, workerKey: warmKey(tenant, team, connection, revision), at: time.Now()}, dsn, nil
}

func firstError(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}

func receiptStats(receipt operations.Receipt, err error) query.Stats {
	var stats query.Stats
	if receipt.Result != nil {
		stats.Rows, stats.WireBytes = receipt.Result.Rows, receipt.Result.Bytes
	}
	return stats
}

// receiptError keeps err and turns a kept worker's non-Completed receipt into
// a public error.
func receiptError(receipt operations.Receipt, err error) error {
	if err != nil || receipt.Outcome == operations.Completed {
		return err
	}
	code := receipt.ErrorCode
	if code == "" {
		code = "QUERY_FAILED"
	}
	return query.NewError(code, "Source read failed")
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
