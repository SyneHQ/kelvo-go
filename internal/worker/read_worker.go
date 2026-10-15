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
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
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
// read before it gets here. What is reused is each principal's source
// authorization: its first read for a connection resolves credentials and
// they go to the worker; later reads within the authorization's window carry
// no secrets. Past the window, or when a new worker is needed, a read
// authorizes again. A definitive denial kills the workers of the authorization
// it ended; a new revision (credential rotation) drains them.
func (e *Executor) executeOnReadWorker(ctx context.Context, execution delegation.Execution, dynamic bool, resolverRequest, r query.Request, sink query.Sink) (stats query.Stats, resultErr error, handled bool) {
	// The job's rows and batches must be what this caller's sink received:
	// the client checks both against what it decoded.
	counted := &countingSink{Sink: sink}
	sink = counted
	defer func() { stats.Rows, stats.Batches = counted.rows, counted.batches }()
	pool := e.WarmPool
	eligible := pool != nil && pool.Config().Enabled && pool.Adapter.Binary != "" && r.Mode == "native" && r.Mongo == nil && !r.ScanDiagnostics &&
		!access.Restricted(ctx) && e.Limits.MaxRows <= readWorkerMaxRows && e.Limits.MaxBytes <= readWorkerMaxBytes
	id, saved, principal := readPrincipal(execution, dynamic, r.ConnectionID)
	// oneShotInspect runs an inspection the read worker cannot take on a
	// one-shot adapter, the path inspections took before read workers.
	oneShotInspect := func() (query.Stats, error, bool) {
		source, until := e, time.Now().Unix()+5
		if dynamic {
			resolved, validUntil, err := e.resolveConnections(ctx, execution, resolverRequest)
			if err != nil {
				return query.Stats{}, err, true
			}
			source, until = resolved, validUntil
		}
		stats, err := e.inspectOneShot(ctx, source, id, until, r, sink)
		return stats, err, true
	}
	if !eligible {
		if r.Inspect == nil {
			return stats, nil, false
		}
		return oneShotInspect()
	}

	authKey := readAuthKeyFor(id, r.ConnectionID, saved, principal)
	ctx = withReadAuthorization(ctx, authKey)
	auth, dsn := pool.freshAuthorization(authKey), ""
	// authorize resolves the source again; ok is false when it no longer
	// qualifies for a read worker.
	authorize := func(reuse bool) (ok bool, err error) {
		a, secret, err := pool.authorizeOnce(ctx, authKey, reuse, func() (*readAuthorization, string, error) {
			observed := pool.currentAuthorization(authKey)
			a, secret, err := e.authorizeReadSource(ctx, execution, dynamic, resolverRequest, r.ConnectionID, id, pool.cfg.reauthorizeEvery())
			switch {
			case err != nil:
				// Revoke only on a definitive denial, and only the authorization
				// this read observed: never on a cancel, deadline or outage.
				if definitiveDenial(err) {
					revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
					_ = pool.revokeAuthorization(revokeCtx, authKey, observed, err)
					cancel()
				}
			case a == nil:
				pool.retireAuthorization(authKey, observed)
			default:
				drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				pool.storeAuthorizationWithin(drainCtx, authKey, a)
				cancel()
			}
			return a, secret, err
		})
		if err != nil || a == nil {
			return false, err
		}
		auth, dsn = a, secret
		return true, nil
	}
	if auth == nil {
		if ok, err := authorize(true); err != nil {
			return stats, err, true
		} else if !ok {
			if r.Inspect != nil {
				return oneShotInspect()
			}
			return stats, nil, false
		}
	}

	request := operations.Request{Version: operations.Version, Connection: operations.ConnectionRef{ID: id.saved, Schema: id.schema}}
	if r.Inspect != nil {
		request.Kind, request.Spec.Metadata = operations.MetadataInspect, r.Inspect
	} else {
		parameters := make([]operations.Parameter, len(r.Parameters))
		for i, p := range r.Parameters {
			parameters[i] = operations.Parameter{Type: p.Type, Value: p.Value}
		}
		request.Kind, request.Spec.Query = operations.QueryRead, &operations.QuerySpec{SQL: r.SQL, Parameters: parameters}
	}
	expires, timeoutMS, ok := readDeadline(ctx, e.Limits.Timeout)
	if !ok {
		return stats, query.NewError("DEADLINE_EXCEEDED", "Read deadline passed before it could be sent"), true
	}
	// build makes the worker request from the current authorization; it
	// carries credentials only when this read just resolved them.
	build := func() (adapter.ProcessRequest, bool) {
		request.Connection.Database = auth.database
		digest, err := operations.Digest(request)
		now := time.Now()
		source := adapter.ConnectionSpec{Engine: auth.def.Type, TenantID: id.tenant, ConnectionID: id.saved, Revision: auth.revision, Database: auth.database, Schema: id.schema}
		if dsn != "" {
			source.DSN, source.Options = dsn, auth.def.Options
		}
		input := adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: readOperationID(), RequestSHA256: digest, Request: request, AppTeam: id.team, Source: source,
			Limits:                adapter.ProcessLimits{MaxRows: e.Limits.MaxRows, MaxBytes: e.Limits.MaxBytes, BatchRows: 1024, TimeoutMS: timeoutMS},
			CredentialsValidUntil: min(auth.credentialsValidUntil(now, pool.cfg.reauthorizeEvery()), expires), ExpiresAt: expires}
		return input, err == nil && input.Validate() == nil && pool.IsEligible(input)
	}
	// bounded carries the current authorization's credential bound, so the
	// window is stamped when the request is sent, not when the read queued.
	bounded := func(ctx context.Context) context.Context {
		return withCredentialBound(ctx, min(auth.credentialBound(pool.cfg.reauthorizeEvery()), expires))
	}
	input, ok := build()
	if !ok {
		if r.Inspect != nil {
			return oneShotInspect()
		}
		return stats, nil, false
	}
	// withCredentials authorizes again a read that found no worker holding
	// credentials of its current authorization, and rebuilds it with them.
	withCredentials := func() (adapter.ProcessRequest, error) {
		if ok, err := authorize(false); err != nil {
			return input, err
		} else if !ok {
			return input, errWarmUnavailable
		}
		if input, ok = build(); !ok {
			return input, errWarmUnavailable
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
			receipt, err = pool.execute(bounded(quotaCtx), e, pool.Adapter, input, sink, nil)
		}
		if dsn != "" || errors.Is(err, errNeedsCredentials) || errors.Is(err, errCredentialsExpired) || errors.Is(err, errWarmUnavailable) {
			if dsn == "" {
				if input, err = withCredentials(); err != nil {
					if errors.Is(err, errWarmUnavailable) {
						return oneShotInspect()
					}
					return stats, err, true
				}
			}
			// The operation path falls back to a one-shot adapter itself.
			receipt, err = e.ExecuteOperation(withAdmissionClass(quotaCtx, admission.ClassInteractive), pool.Adapter, input, sink)
		}
		return receiptStats(receipt, err), receiptError(receipt, err), true
	}

	// execute runs the read on a worker, copying its result to tee if any.
	execute := func(ctx context.Context, sink query.Sink, tee *cappedBuffer) (query.Stats, error) {
		quotaCtx, releaseQuota, err := auth.source.acquireSourceQuota(ctx, r)
		if err != nil {
			return query.Stats{}, err
		}
		defer releaseQuota()
		var copyTo io.Writer
		if tee != nil {
			copyTo = tee
		}
		receipt, err := pool.execute(bounded(quotaCtx), e, pool.Adapter, input, sink, copyTo)
		// A worker without this read's credentials, or a read that outwaited
		// its authorization while queued: authorize again (shared) and resend.
		if errors.Is(err, errNeedsCredentials) || errors.Is(err, errCredentialsExpired) {
			if input, err = withCredentials(); err != nil {
				return query.Stats{}, err
			}
			receipt, err = pool.execute(bounded(quotaCtx), e, pool.Adapter, input, sink, copyTo)
		}
		return receiptStats(receipt, err), receiptError(receipt, err)
	}
	if pool.cache == nil {
		stats, err := execute(ctx, sink, nil)
		if errors.Is(err, errWarmUnavailable) || errors.Is(err, errNeedsCredentials) || errors.Is(err, errCredentialsExpired) {
			return stats, nil, false
		}
		return stats, err, true
	}
	key := resultCacheKey(id.tenant, id.team, id.saved, auth.database, id.schema, auth.revision, r, e.Limits)
	stats, err := pool.cache.do(ctx, key, id.tenant, r.BypassCache, sink, execute, func(entry *cacheEntry, sink query.Sink) (query.Stats, error) {
		observed, err := e.readIPC(ctx, bytes.NewReader(entry.data), e.Limits, sink)
		stats := entry.stats
		stats.Rows, stats.WireBytes, stats.Bytes, stats.Batches = observed.Rows, observed.WireBytes, observed.Bytes, observed.Batches
		return stats, err
	})
	if errors.Is(err, errWarmUnavailable) || errors.Is(err, errNeedsCredentials) || errors.Is(err, errCredentialsExpired) {
		return stats, nil, false
	}
	return stats, err, true
}

// readPrincipal names who a read authorizes as: its tenant and app team, the
// saved connection behind its alias, and its subject and service principal.
// Static operator sources have one principal.
func readPrincipal(execution delegation.Execution, dynamic bool, alias string) (id readIdentity, saved, principal string) {
	id.tenant, id.team = readTenant(execution, dynamic)
	if !dynamic {
		id.saved = alias
		return id, alias, "static"
	}
	selected := execution.Claims.Sources[0]
	id.database, id.schema, id.saved = selected.Database, selected.Schema, selected.ConnectionID
	subject, _ := json.Marshal([]any{execution.Claims.ServicePrincipal, execution.Claims.Subject})
	return id, selected.ConnectionID, string(subject)
}

// readAuthKeyFor keys one principal's authorization of one source: tenant,
// team, alias, saved connection, subject and service principal, database and
// schema. Each principal authorizes once per window; workers are still shared
// by revision.
func readAuthKeyFor(id readIdentity, alias, saved, principal string) string {
	raw, _ := json.Marshal([]string{id.tenant, id.team, alias, saved, principal, id.database, id.schema})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// authorizeReadSource resolves a connection's source and credentials. It
// returns nil without an error when the source does not qualify for a read
// worker. The authorization keeps no secret values; dsn is for this read only.
func (e *Executor) authorizeReadSource(ctx context.Context, execution delegation.Execution, dynamic bool, resolverRequest query.Request, connection string, id readIdentity, every time.Duration) (*readAuthorization, string, error) {
	source := e
	var validUntil time.Time
	if dynamic {
		resolved, until, err := e.resolveConnections(ctx, execution, resolverRequest)
		if err != nil {
			return nil, "", err
		}
		source, validUntil = resolved, time.Unix(until, 0)
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
		return nil, "", err // the secret store failed: not a denial
	}
	if !present {
		return nil, "", query.NewError("NOT_FOUND", "Source credential not found")
	}
	db, ok := readWorkerDatabase(dsn, id.database)
	if !ok {
		return nil, "", nil
	}
	// The revision binds the source definition and its credentials, so a
	// rotated password or changed source gets new workers.
	definition, _ := json.Marshal(s)
	sum := sha256.Sum256(append(append(definition, 0), dsn...))
	revision := hex.EncodeToString(sum[:])
	quota := *source
	quota.Secrets = nil
	return &readAuthorization{source: &quota, def: s, revision: revision, database: db,
		workerKey: warmKey(id.tenant, id.team, id.saved, db, id.schema, revision), at: time.Now(), validUntil: validUntil}, dsn, nil
}

// readIdentity names whose read this is: tenant, app team, the saved
// connection behind the request's alias, database and schema. Workers are
// keyed by the saved connection: apps reuse one alias for every connection.
type readIdentity struct{ tenant, team, saved, database, schema string }

// countingSink counts the rows and batches its caller's sink received.
type countingSink struct {
	query.Sink
	rows, batches int64
}

func (s *countingSink) Write(batch arrow.RecordBatch) error {
	if err := s.Sink.Write(batch); err != nil {
		return err
	}
	s.rows += batch.NumRows()
	s.batches++
	return nil
}

// readTenant names a read's tenant: one app team of one cluster tenant at one
// delegation issuer, or "static" for operator-configured sources.
func readTenant(execution delegation.Execution, dynamic bool) (tenant, team string) {
	if !dynamic {
		return "static", ""
	}
	identity, _ := json.Marshal([]string{execution.Claims.Issuer, execution.Claims.ClusterTenant, execution.Claims.AppTeam})
	sum := sha256.Sum256(append([]byte("kelvo.read-worker.tenant.v1\x00"), identity...))
	return hex.EncodeToString(sum[:]), execution.Claims.AppTeam
}

func readOperationID() string {
	id := make([]byte, 12)
	_, _ = rand.Read(id)
	return "read-" + hex.EncodeToString(id)
}

// credentialWindow is the window one worker request carries: the resolver's
// validity (75s for read credentials) and the read's own expiry, within the
// five seconds a process request may claim.
func credentialWindow(validUntil, expires int64) int64 {
	return min(validUntil, expires, time.Now().Add(credentialWindowLimit()).Unix())
}

// credentialWindowMax is the most a process request may claim; a var so tests
// can make a queued read outlast it.
// Atomic: requests still in flight from one test may read it while the next
// test changes it.
var credentialWindowMax atomic.Int64 // nanoseconds; zero means 5s

func credentialWindowLimit() time.Duration {
	if d := time.Duration(credentialWindowMax.Load()); d > 0 {
		return d
	}
	return 5 * time.Second
}

type credentialBoundKey struct{}

// withCredentialBound records how long a read's credentials stay usable, so
// its window is stamped when the request is sent to a worker, not when the
// read started: a read that queued for a slot still carries a valid one.
func withCredentialBound(ctx context.Context, bound int64) context.Context {
	return context.WithValue(ctx, credentialBoundKey{}, bound)
}

func credentialBound(ctx context.Context) (int64, bool) {
	bound, ok := ctx.Value(credentialBoundKey{}).(int64)
	return bound, ok
}

// readDeadline returns a worker request's ExpiresAt (whole seconds, rounded
// up) and TimeoutMS at millisecond precision, so a deadline under a second is
// still sent. ok is false when the deadline has passed.
func readDeadline(ctx context.Context, limit time.Duration) (expires, timeoutMS int64, ok bool) {
	deadline := time.Now().Add(299 * time.Second) // ExpiresAt must stay within 300s
	if d, has := ctx.Deadline(); has && d.Before(deadline) {
		deadline = d
	}
	timeoutMS = min(time.Until(deadline).Milliseconds(), limit.Milliseconds())
	if timeoutMS < 1 {
		return 0, 0, false
	}
	return (deadline.UnixNano() + int64(time.Second) - 1) / int64(time.Second), timeoutMS, true
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

// inspectOneShot runs a metadata inspection on a one-shot adapter, without the
// operation ledger: the path for inspections a read worker cannot take.
func (e *Executor) inspectOneShot(ctx context.Context, source *Executor, id readIdentity, validUntil int64, r query.Request, sink query.Sink) (query.Stats, error) {
	if e.OperationAdapter.Binary == "" {
		return query.Stats{}, query.NewError("UNAVAILABLE", "Metadata inspection requires the operation adapter on this node")
	}
	sources, err := source.Config.Select([]string{r.ConnectionID})
	if err != nil {
		return query.Stats{}, query.NewError("INVALID_ARGUMENT", "Unknown or duplicate source")
	}
	s := sources[0]
	if s.Adapter != "" || s.Path != "" || s.Federation != nil || s.LocalSnapshot != nil || s.ObjectSnapshot != nil || s.Ranges != nil || s.ParquetPaths != nil || s.Object != nil || s.Range != nil {
		return query.Stats{}, query.NewError("INVALID_ARGUMENT", "Metadata inspection is not available for this source")
	}
	var secrets [5]string
	for i, name := range []string{s.DSNEnv, s.URLEnv, s.UsernameEnv, s.PasswordEnv, s.TokenEnv} {
		if name == "" {
			continue
		}
		v, present, err := source.resolveSecret(ctx, name)
		if err != nil {
			return query.Stats{}, err
		}
		if !present {
			return query.Stats{}, query.NewError("CONFIGURATION_ERROR", "A source environment variable is missing")
		}
		secrets[i] = v
	}
	database := id.database
	if u, err := url.Parse(secrets[0]); database == "" && readWorkerEngines[s.Type] && err == nil && len(u.Path) > 1 {
		database = u.Path[1:]
	}
	request := operations.Request{Version: operations.Version, Kind: operations.MetadataInspect,
		Connection: operations.ConnectionRef{ID: id.saved, Database: database, Schema: id.schema}, Spec: operations.Spec{Metadata: r.Inspect}}
	digest, err := operations.Digest(request)
	expires, timeoutMS, ok := readDeadline(ctx, e.Limits.Timeout)
	if err != nil || !ok {
		return query.Stats{}, query.NewError("INVALID_ARGUMENT", "Metadata inspection request is not valid")
	}
	definition, _ := json.Marshal(s)
	revision := sha256.Sum256(append(append(definition, 0), strings.Join(secrets[:], "\x00")...))
	input := adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: readOperationID(), RequestSHA256: digest, Request: request, AppTeam: id.team,
		Source: adapter.ConnectionSpec{Engine: s.Type, DSN: secrets[0], URL: secrets[1], Username: secrets[2], Password: secrets[3], Token: secrets[4], Options: s.Options,
			TenantID: id.tenant, ConnectionID: id.saved, Revision: hex.EncodeToString(revision[:]), Database: database, Schema: id.schema},
		Limits:                adapter.ProcessLimits{MaxRows: min(e.Limits.MaxRows, readWorkerMaxRows), MaxBytes: min(e.Limits.MaxBytes, readWorkerMaxBytes), BatchRows: 1024, TimeoutMS: timeoutMS},
		CredentialsValidUntil: credentialWindow(validUntil, expires), ExpiresAt: expires}
	if input.Validate() != nil {
		return query.Stats{}, query.NewError("INVALID_ARGUMENT", "Metadata inspection is not available for this source")
	}
	// An inspection is an interactive read, never background work.
	receipt, err := e.ExecuteOperation(withAdmissionClass(ctx, admission.ClassInteractive), e.OperationAdapter, input, sink)
	return receiptStats(receipt, err), receiptError(receipt, err)
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
