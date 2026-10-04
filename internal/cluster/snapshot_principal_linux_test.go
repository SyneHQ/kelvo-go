//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type snapshotPrincipalRows struct{}

func (snapshotPrincipalRows) Execute(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "account_id", Type: arrow.PrimitiveTypes.Int64}, {Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "secret", Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i := 0; i < 1024; i++ {
		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		account := int64(42)
		if i%2 != 0 {
			account = 7
		}
		builder.Field(1).(*array.Int64Builder).Append(account)
		if i%11 == 0 {
			builder.Field(2).AppendNull()
		} else {
			builder.Field(2).(*array.Int64Builder).Append(int64(i * 10))
		}
		builder.Field(3).(*array.StringBuilder).Append("private-source-value")
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	if err := ctx.Err(); err != nil {
		return query.Stats{}, err
	}
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	return query.Stats{Rows: record.NumRows()}, sink.Write(record)
}

func snapshotPrincipalFixture(t *testing.T) (catalog.Config, Policy) {
	t.Helper()
	config := catalog.Config{Sources: []catalog.Source{{ID: "origin", Type: "clickhouse", URLEnv: "KELVO_TEST_UNUSED_URL"}}, Acceleration: &catalog.AccelerationConfig{
		Directory: filepath.Join(t.TempDir(), "snapshots"), TenantID: "a", Datasets: []catalog.Dataset{{ID: "orders_fast", Query: query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT fixture"}, RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}},
	}}
	manager, err := acceleration.NewManager(config, func(catalog.Config, query.Limits) (query.Executor, error) { return snapshotPrincipalRows{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if _, err := manager.Refresh(context.Background(), "orders_fast", false); err != nil {
		t.Fatal(err)
	}
	p := principalTestPolicy()
	p.Limits.Timeout = 15 * time.Second
	p.Limits.Threads = 1
	p.Access.Principals = map[string]PrincipalGrant{
		"analyst": {Kind: "user", FederatedSources: []string{"orders_fast"}, RowColumnPolicy: &access.Policy{Sources: map[string]access.SourcePolicy{
			"orders_fast": {Tables: map[string]access.TablePolicy{"orders_fast": {Columns: []string{"id", "amount"}, Rows: &access.Predicate{Kind: "comparison", Column: "account_id", Type: "int64", Op: "eq", Value: "42"}}}},
		}}},
		"reports": {Kind: "service", FederatedSources: []string{"orders_fast"}},
	}
	if err := ValidatePolicy(p); err != nil {
		t.Fatal(err)
	}
	return config, p
}

func verifySnapshotPrincipalIPC(t *testing.T, data []byte) {
	t.Helper()
	if !bytes.HasSuffix(data, []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
		t.Fatal("worker output lacks successful EOS")
	}
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if reader.Schema().NumFields() != 2 || reader.Schema().Field(0).Name != "id" || reader.Schema().Field(1).Name != "amount" {
		t.Fatal("raw snapshot schema exposed")
	}
	var count int64
	for reader.Next() {
		record := reader.RecordBatch()
		ids := record.Column(0).(*array.Int64)
		amount := record.Column(1).(*array.Int64)
		for i := 0; i < int(record.NumRows()); i++ {
			id := count * 2
			if ids.Value(i) != id || amount.IsNull(i) != (id%11 == 0) || (!amount.IsNull(i) && amount.Value(i) != id*10) {
				t.Fatal("snapshot policy/type result changed")
			}
			count++
		}
	}
	if reader.Err() != nil || count != 512 {
		t.Fatal("incomplete guarded snapshot rows", count, reader.Err())
	}
}

// Runs real local snapshot resolution and the real child through Landlock, then
// the production gateway authentication and relay. Metadata CAS is a controlled
// in-memory fixture, so this does not claim broker or distributed revocation HA.
func TestSnapshotPrincipalRealWorkerRevocationWithholdsEOS(t *testing.T) {
	runSnapshotPrincipalRealWorkerRevocation(t, nil)
}

// configure replaces only the trusted, already resolved fixture sources. The
// authenticated gateway, real worker, principal checks, and four EOS modes are
// shared so local and object snapshots exercise the same authority boundary.
func runSnapshotPrincipalRealWorkerRevocation(t *testing.T, configure func(*testing.T, catalog.Config) catalog.Config) {
	t.Helper()
	binary, launcher := os.Getenv("KELVO_TEST_SNAPSHOT_BINARY"), os.Getenv("KELVO_TEST_SNAPSHOT_SANDBOX")
	if binary == "" || launcher == "" {
		t.Skip("set KELVO_TEST_SNAPSHOT_BINARY and KELVO_TEST_SNAPSHOT_SANDBOX for real guarded-snapshot principal acceptance")
	}
	for _, mode := range []string{"success", "active-key-revocation", "commit-key-revocation", "commit-document-expiry"} {
		t.Run(mode, func(t *testing.T) {
			config, p := snapshotPrincipalFixture(t)
			if configure != nil {
				config = configure(t, config)
			}
			engine, err := worker.New(config, p.Limits)
			if err != nil {
				t.Fatal(err)
			}
			engine.Binary, engine.SandboxPath = binary, launcher
			principal, ok := authorityForPrincipal(p, "analyst")
			if !ok {
				t.Fatal("principal missing")
			}
			request := query.Request{Mode: "federated", Sources: []string{"orders_fast"}, SQL: "SELECT id, amount FROM orders_fast ORDER BY id"}
			store := &compressionRelayStore{gatewayStore: gatewayStore{policy: p}, snapshot: Snapshot{Revision: 1, Job: Job{
				ID: "snapshot-result", TenantID: "a", Authority: &principal, State: Assigned, Owner: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", WorkerID: "worker-one", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute), Request: request,
			}}}
			auth := bareAuthenticator(t)
			auth.config.ReloadInterval = 30 * time.Second
			initial := principalKeySet(t, 1, map[string][]string{"analyst": {rotationOld}, "reports": {rotationNew}}, map[string][]string{"analyst": {rotationOther}})
			if !auth.apply(initial, time.Now()) {
				t.Fatal("principal keys rejected")
			}
			revoked := principalKeySet(t, 2, map[string][]string{"analyst": {}, "reports": {rotationNew}}, map[string][]string{"analyst": {rotationOther}})
			revoke := func() {
				if !auth.apply(revoked, time.Now()) {
					t.Error("principal revocation rejected")
				}
			}
			if mode == "commit-key-revocation" {
				store.beforeCommit = revoke
			}
			if mode == "commit-document-expiry" {
				store.beforeCommit = func() { auth.mu.Lock(); auth.validUntil = time.Now().Add(-time.Second); auth.mu.Unlock() }
			}
			type workerResult struct {
				data []byte
				err  error
			}
			produced := make(chan workerResult, 1)
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/internal/queries/snapshot-result/results" {
					http.Error(w, "unexpected route", 400)
					return
				}
				ctx, err := executionAuthorityContext(r.Context(), p, &principal, request)
				var out bytes.Buffer
				sink := worker.NewIPCSink(&out, p.Limits)
				defer sink.Abort()
				var stats query.Stats
				if err == nil {
					stats, err = engine.Execute(ctx, request, sink)
				}
				if err == nil {
					err = sink.Finish()
				}
				encoded := append([]byte(nil), out.Bytes()...)
				produced <- workerResult{encoded, err}
				if err != nil {
					http.Error(w, "worker failed", 503)
					return
				}
				stats.WireBytes = int64(len(encoded))
				w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
				if mode == "active-key-revocation" {
					_, _ = w.Write(encoded[:len(encoded)-8])
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					return
				}
				if !store.resultReady(r.Header.Get("X-Kelvo-Claim"), stats) {
					t.Error("result claim did not match")
					return
				}
				_, _ = w.Write(encoded)
			}))
			defer upstream.Close()
			endpoint, err := url.Parse(upstream.URL)
			if err != nil {
				t.Fatal(err)
			}
			foreign := p
			foreign.TenantID = "b"
			g := &Gateway{auth: auth, ctx: context.Background(), permits: make(chan struct{}, 2), resultWaiters: make(chan struct{}, 2), tenants: map[string]gatewayTenant{
				"a": {store: store, workers: map[string]workerEndpoint{"worker-one": {url: endpoint, client: upstream.Client()}}},
				"b": {store: &gatewayStore{policy: foreign, jobs: map[string]Snapshot{}}},
			}}
			// Both a different principal with the same source grant and a foreign
			// tenant see404, even when they supply forged identity headers.
			for _, key := range []string{rotationNew, rotationOther} {
				for _, route := range []struct{ method, path string }{{"GET", "/v1/queries/snapshot-result"}, {"POST", "/v1/queries/snapshot-result/cancel"}, {"GET", "/v1/queries/snapshot-result/results"}} {
					r := httptest.NewRequest(route.method, route.path, nil)
					r.Header.Set("Authorization", "Bearer "+key)
					r.Header.Set("X-Kelvo-Principal", "analyst")
					r.Header.Set("X-Kelvo-Tenant", "a")
					w := httptest.NewRecorder()
					g.ServeHTTP(w, r)
					if w.Code != 404 {
						t.Fatal("foreign handle exposed", route.path, w.Code)
					}
				}
			}
			output := &firstRelayWrite{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{})}
			done := make(chan any, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			go func() {
				defer func() { done <- recover() }()
				r := httptest.NewRequest("GET", "/v1/queries/snapshot-result/results", nil).WithContext(ctx)
				r.Header.Set("Authorization", "Bearer "+rotationOld)
				g.ServeHTTP(output, r)
			}()
			if mode == "active-key-revocation" {
				select {
				case <-output.started:
					revoke()
				case result := <-done:
					t.Fatal("gateway ended before delivery", result, output.Code)
				case <-ctx.Done():
					t.Fatal("gateway did not begin delivery")
				}
			}
			var result any
			select {
			case result = <-done:
			case <-ctx.Done():
				t.Fatal("gateway result did not finish")
			}
			var materialized workerResult
			select {
			case materialized = <-produced:
			case <-ctx.Done():
				t.Fatal("real child did not produce a result")
			}
			if materialized.err != nil {
				t.Fatal("real snapshot child failed", materialized.err)
			}
			verifySnapshotPrincipalIPC(t, materialized.data)
			if output.Code != 200 || output.Header().Get("Kelvo-Result-Completion") != "durable-eos-v1" {
				t.Fatal("gateway did not relay real child Arrow")
			}
			if len(g.permits) != 0 || len(g.resultWaiters) != 0 {
				t.Fatal("relay retained admission")
			}
			final, err := store.Get(context.Background(), "snapshot-result")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "success" {
				if result != nil || final.Job.State != Succeeded || !bytes.Equal(output.Body.Bytes(), materialized.data) {
					t.Fatal("authorized snapshot did not complete", result, final.Job.State)
				}
			} else {
				store.mu.Lock()
				commitAttempts := store.commitAttempts
				store.mu.Unlock()
				if mode == "active-key-revocation" {
					if final.Job.State == Succeeded || commitAttempts != 0 {
						t.Fatal("active revocation reached durable success", final.Job.State, commitAttempts)
					}
				} else if final.Job.State != Succeeded || commitAttempts != 1 {
					// These hooks revoke inside the controlled CAS after its
					// pre-check. Durable completion may win that race, while
					// the final authority check must still withhold client EOS.
					t.Fatal("commit-race fixture did not exercise durable completion", final.Job.State, commitAttempts)
				}
				if !errors.Is(errorFromSnapshotPanic(result), http.ErrAbortHandler) || bytes.HasSuffix(output.Body.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
					t.Fatal("revoked principal received successful EOS", result)
				}
				if output.Body.Len() == 0 {
					t.Fatal("revocation did not exercise active delivery")
				}
				for _, route := range []struct{ method, path string }{{"GET", "/v1/queries/snapshot-result"}, {"POST", "/v1/queries/snapshot-result/cancel"}, {"GET", "/v1/queries/snapshot-result/results"}} {
					r := httptest.NewRequest(route.method, route.path, nil)
					r.Header.Set("Authorization", "Bearer "+rotationOld)
					w := httptest.NewRecorder()
					g.ServeHTTP(w, r)
					if w.Code != 401 {
						t.Fatal("expired/revoked principal remained authenticated", route.path, w.Code)
					}
				}
			}
		})
	}
}

func errorFromSnapshotPanic(value any) error { err, _ := value.(error); return err }
