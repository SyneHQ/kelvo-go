// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

const exportGatewayID = "e0-0123456789abcdef0123456789abcdef"
const exportGatewayReportsKey = "reports-key-for-export-fixture-only"

// This controlled CAS fixture tests gateway authority and HTTP lifecycle. Real
// JetStream fencing and local export storage have their own acceptance gates.
type exportGatewayStore struct {
	mu            sync.Mutex
	policy        Policy
	job           ExportSnapshot
	submissions   int
	readyAttempts int
	assign        bool
	beforeReady   func()
}

func cloneExportGatewaySnapshot(s ExportSnapshot) ExportSnapshot {
	raw, _ := json.Marshal(s.Job)
	var job ExportJob
	_ = json.Unmarshal(raw, &job)
	return ExportSnapshot{Job: job, Revision: s.Revision}
}

func (s *exportGatewayStore) Policy() Policy { return s.policy }
func (s *exportGatewayStore) SubmitExport(ctx context.Context, in ExportSubmission) (ExportSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.submissions++
	if s.job.Revision != 0 {
		return ExportSnapshot{}, ErrExportCapacity
	}
	job, err := normalizeExportSubmission(ctx, s.policy, in, time.Now())
	if err != nil {
		return ExportSnapshot{}, err
	}
	job.ID = exportGatewayID
	if err := validateExportJob(s.policy, job); err != nil {
		return ExportSnapshot{}, err
	}
	s.job = ExportSnapshot{Job: job, Revision: 1}
	return cloneExportGatewaySnapshot(s.job), nil
}
func (s *exportGatewayStore) GetExport(ctx context.Context, id string) (ExportSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ExportSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job.Revision == 0 || id != s.job.Job.ID {
		return ExportSnapshot{}, ErrExportNotFound
	}
	return cloneExportGatewaySnapshot(s.job), nil
}
func (s *exportGatewayStore) CompareAndSwapExport(ctx context.Context, previous ExportSnapshot, next ExportJob) (ExportSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ExportSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job.Revision != previous.Revision || s.job.Job.ID != previous.Job.ID {
		return ExportSnapshot{}, ErrExportConflict
	}
	if err := validateExportJob(s.policy, next); err != nil {
		return ExportSnapshot{}, err
	}
	if next.State == ExportReady {
		s.readyAttempts++
		if s.beforeReady != nil {
			s.beforeReady()
		}
	}
	// beforeReady deliberately runs inside the accepted CAS to exercise
	// authority changing after the caller's pre-check but before its receipt.
	s.job = cloneExportGatewaySnapshot(ExportSnapshot{Job: next, Revision: previous.Revision + 1})
	return cloneExportGatewaySnapshot(s.job), nil
}
func (s *exportGatewayStore) EnqueueExport(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.assign && s.job.Job.State == ExportQueued {
		s.job.Job.State, s.job.Job.WorkerID, s.job.Job.WorkerOwner = ExportAssigned, "a1", strings.Repeat("b", 32)
		s.job.Job.HeartbeatAt = time.Now()
		s.job.Revision++
	}
	return nil
}
func (*exportGatewayStore) NextExport(context.Context) (Delivery, error) { return nil, ErrNoExport }
func (*exportGatewayStore) ReconcileExports(context.Context) error       { return nil }

func exportGatewayFixture(t *testing.T, codec string) (*Gateway, *exportGatewayStore, *gatewayAuthenticator) {
	t.Helper()
	p := runtimeExportConfigFixture(t).Policy
	p.Exports.QueueTimeout = time.Second
	p.Limits.Timeout = 2 * time.Second
	p.Exports.Limits.Compression = codec
	auth := bareAuthenticator(t)
	auth.config.ReloadInterval = time.Minute
	keys := principalKeySet(t, 1, map[string][]string{"analyst": {rotationOld, rotationNew}, "reports": {exportGatewayReportsKey}}, map[string][]string{"analyst": {rotationOther}})
	if !auth.apply(keys, time.Now()) {
		t.Fatal("export keys rejected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	store := &exportGatewayStore{policy: p}
	bPolicy := p
	bPolicy.TenantID = "b"
	bStore := &exportGatewayStore{policy: bPolicy}
	g := &Gateway{ctx: ctx, cancel: cancel, auth: auth, permits: make(chan struct{}, 16), resultWaiters: make(chan struct{}, 16),
		tenants: map[string]gatewayTenant{"a": {store: &gatewayStore{policy: p}, workers: map[string]workerEndpoint{}}, "b": {store: &gatewayStore{policy: bPolicy}, workers: map[string]workerEndpoint{}}},
		exports: map[string]ExportStore{"a": store, "b": bStore}, exportOwner: strings.Repeat("a", 32), exportSupervisors: make(chan struct{}, 1), exportDownloads: make(chan struct{}, 1)}
	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() { g.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("export supervision did not join")
		}
	})
	return g, store, auth
}

func exportGatewayRequest(method, path, token, body string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Kelvo-Tenant", "a")
	r.Header.Set("X-Kelvo-Principal", "analyst")
	return r
}

func exportGatewayCall(g *Gateway, w http.ResponseWriter, r *http.Request) (panicValue any) {
	defer func() { panicValue = recover() }()
	g.ServeHTTP(w, r)
	return nil
}

func exportGatewayWire(t *testing.T, codec string) ([]byte, query.Stats) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "label", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i := 0; i < 128; i++ {
		builder.Field(0).(*array.Int64Builder).Append(9007199254740993 + int64(i))
		if i%11 == 0 {
			builder.Field(1).(*array.StringBuilder).AppendNull()
		} else {
			builder.Field(1).(*array.StringBuilder).Append(strings.Repeat("durable-export-", 48))
		}
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	limits := query.DefaultLimits()
	limits.ResultCompression = codec
	var encoded bytes.Buffer
	sink := worker.NewIPCSink(&encoded, limits)
	defer sink.Abort()
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes(), query.Stats{Rows: 128, Batches: 1, Bytes: arrowutil.TotalRecordSize(record), WireBytes: sink.EncodedBytes(), DurationNS: 17}
}

func exportGatewayStored(t *testing.T, store *exportGatewayStore, current ExportSnapshot, wire []byte, stats query.Stats) ExportJob {
	t.Helper()
	job := current.Job
	job.State, job.WorkerID, job.WorkerOwner = ExportStored, "a1", strings.Repeat("b", 32)
	if job.Claim == "" {
		job.Claim = strings.Repeat("c", 32)
	}
	job.StartedAt, job.HeartbeatAt = time.Now(), time.Now()
	job.ExecutionDeadline = minTime(job.StartedAt.Add(job.Spec.QueryLimits.Timeout), job.ExpiresAt)
	locator := ExportLocator{StorageID: strings.Repeat("d", 32), ExportID: strings.Repeat("e", 32), Fence: strings.Repeat("f", 32)}
	identity, err := exportIdentity(store.policy, job.Authority)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(wire)
	manifest := exports.Manifest{Version: 1, ID: locator.ExportID, Fence: locator.Fence, Tenant: job.TenantID, Identity: identity,
		CreatedAt: job.StartedAt, ExpiresAt: job.ExpiresAt, SchemaSHA256: strings.Repeat("1", 64), Rows: stats.Rows, EncodedBytes: int64(len(wire)), DecodedBytes: stats.Bytes,
		Parts: []exports.PartInfo{{Index: 0, Rows: stats.Rows, Batches: 1, EncodedBytes: int64(len(wire)), DecodedBytes: stats.Bytes, SHA256: hex.EncodeToString(digest[:])}}}
	receipt, err := sealExportReceipt(locator, manifest)
	if err != nil {
		t.Fatal(err)
	}
	job.Local, job.Receipt, job.Stats = &locator, &receipt, stats
	if err := validateExportJob(store.policy, job); err != nil {
		t.Fatal("stored fixture invalid", err)
	}
	return job
}

func seedExportGatewayReady(t *testing.T, g *Gateway, store *exportGatewayStore, wire []byte, stats query.Stats) {
	t.Helper()
	_, key, ok := g.auth.lookup(rotationOld)
	if !ok {
		t.Fatal("missing key")
	}
	authority, _ := authorityForPrincipal(store.policy, "analyst")
	ctx := context.WithValue(context.Background(), jobAuthorityKey{}, authority)
	ctx = context.WithValue(ctx, keyAuthorizationContext{}, keyAuthorization{g.auth, key})
	job, err := normalizeExportSubmission(ctx, store.policy, ExportSubmission{Request: query.Request{Mode: "federated", Sources: []string{"sales"}, SQL: "SELECT id, label FROM sales"}, SupervisorOwner: g.exportOwner, AuthorityUntil: time.Now().Add(time.Second)}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	job.ID = exportGatewayID
	job = exportGatewayStored(t, store, ExportSnapshot{Job: job}, wire, stats)
	job.State = ExportReady
	// Finished exports outlive their initiating supervisor/key lease. New
	// requests still need a currently authorized principal and policy.
	job.AuthorityUntil = job.CreatedAt.Add(time.Nanosecond)
	store.mu.Lock()
	store.job = ExportSnapshot{Job: job, Revision: 1}
	store.mu.Unlock()
}

func installExportGatewayWorker(t *testing.T, g *Gateway, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	u, _ := url.Parse(server.URL)
	tenant := g.tenants["a"]
	tenant.workers["a1"] = workerEndpoint{url: u, client: server.Client()}
	g.tenants["a"] = tenant
}

func awaitExportGatewayState(t *testing.T, store *exportGatewayStore, state string) ExportSnapshot {
	t.Helper()
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		snapshot, err := store.GetExport(context.Background(), exportGatewayID)
		if err == nil && snapshot.Job.State == state {
			return snapshot
		}
		select {
		case <-deadline.C:
			t.Fatalf("export did not reach %s; last state %s", state, snapshot.Job.State)
		case <-tick.C:
		}
	}
}

func TestGatewayExportDetachedContextRetainsOnlyLiveAuthority(t *testing.T) {
	g, store, auth := exportGatewayFixture(t, "none")
	_, key, _ := auth.lookup(rotationOld)
	authority, _ := authorityForPrincipal(store.policy, "analyst")
	request, stop := context.WithCancel(context.Background())
	request = context.WithValue(request, jobAuthorityKey{}, authority)
	request = context.WithValue(request, keyAuthorizationContext{}, keyAuthorization{auth, key})
	ctx, cancel, err := g.detachedExportContext(request)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	stop()
	if ctx.Err() != nil || requestAuthorityErr(ctx) != nil {
		t.Fatal("HTTP disconnect cancelled detached authority")
	}
	until, err := exportAuthorityDeadline(ctx, store.policy, time.Now().Add(time.Hour))
	if err != nil || until.After(time.Now().Add(store.policy.LeaseDuration)) || until.After(auth.expiry()) {
		t.Fatal("authority renewal escaped bounds")
	}
	auth.mu.Lock()
	auth.validUntil = time.Now().Add(-time.Second)
	auth.mu.Unlock()
	if _, err := exportAuthorityDeadline(ctx, store.policy, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expired key document renewed authority")
	}
	auth.invalidate()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("revocation did not cancel supervisor")
	}
}

func TestGatewayExportForeignHandlesAndPublicMetadata(t *testing.T) {
	g, store, _ := exportGatewayFixture(t, "none")
	wire, stats := exportGatewayWire(t, "none")
	seedExportGatewayReady(t, g, store, wire, stats)
	for _, token := range []string{exportGatewayReportsKey, rotationOther} {
		for _, route := range []struct{ method, suffix string }{{"GET", ""}, {"POST", "/cancel"}, {"GET", "/manifest"}, {"GET", "/parts/0"}, {"GET", "/parts/invalid"}} {
			w := httptest.NewRecorder()
			r := exportGatewayRequest(route.method, "/v1/exports/"+exportGatewayID+route.suffix, token, "")
			r.Header.Set("Range", "bytes=0-10")
			if v := exportGatewayCall(g, w, r); v != nil || w.Code != 404 {
				t.Fatal("foreign export disclosed", route, w.Code, v)
			}
		}
	}
	for _, suffix := range []string{"", "/manifest"} {
		w := httptest.NewRecorder()
		if v := exportGatewayCall(g, w, exportGatewayRequest("GET", "/v1/exports/"+exportGatewayID+suffix, rotationNew, "")); v != nil || w.Code != 200 {
			t.Fatal("fresh key could not read ready export", w.Code, v)
		}
		for _, private := range []string{"SELECT", "sales", "storage_id", "worker_id", "supervisor_owner", "authorization", "receipt_sha256", "fence", "tenant"} {
			if strings.Contains(w.Body.String(), private) {
				t.Fatal("private metadata leaked", private)
			}
		}
	}
}

func TestGatewayExportRejectsUntrustedSubmissionFields(t *testing.T) {
	for name, body := range map[string]string{
		"authority":         `{"query":{"mode":"federated","sql":"SELECT 1","sources":["sales"]},"authority":{"principal":"reports"}}`,
		"native":            `{"query":{"mode":"native","sql":"SELECT 1","connection_id":"sales_native"}}`,
		"ttl overflow":      `{"query":{"mode":"federated","sql":"SELECT 1","sources":["sales"]},"ttl_seconds":9223372036854775807}`,
		"negative ttl":      `{"query":{"mode":"federated","sql":"SELECT 1","sources":["sales"]},"ttl_seconds":-1}`,
		"unsupported codec": `{"query":{"mode":"federated","sql":"SELECT 1","sources":["sales"]},"compression":"gzip"}`,
		"trailing document": `{"query":{"mode":"federated","sql":"SELECT 1","sources":["sales"]}} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			g, store, _ := exportGatewayFixture(t, "none")
			w := httptest.NewRecorder()
			if v := exportGatewayCall(g, w, exportGatewayRequest("POST", "/v1/exports", rotationOld, body)); v != nil || w.Code != 400 {
				t.Fatal("untrusted request accepted", w.Code, v)
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.submissions != 0 {
				t.Fatal("invalid request reached durable admission")
			}
		})
	}
}

func TestGatewayExportSupervisorOutlivesSubmitAndNeverReplays(t *testing.T) {
	for _, mode := range []string{"success", "lost-response", "receipt-mismatch", "gateway-close", "gateway-close-after-read", "key-revocation", "ready-cas-revocation"} {
		t.Run(mode, func(t *testing.T) {
			g, store, auth := exportGatewayFixture(t, "none")
			store.assign = true
			wire, stats := exportGatewayWire(t, "none")
			entered, release := make(chan struct{}), make(chan struct{})
			var released sync.Once
			unblock := func() { released.Do(func() { close(release) }) }
			defer unblock()
			var executions atomic.Int32
			installExportGatewayWorker(t, g, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/internal/exports/"+exportGatewayID+"/execute" || len(r.Header.Values("X-Kelvo-Claim")) != 1 || r.Header.Get("Authorization") != "" {
					t.Error("invalid execution envelope")
					w.WriteHeader(400)
					return
				}
				executions.Add(1)
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				var stored ExportJob
				for range 8 {
					current, err := store.GetExport(r.Context(), exportGatewayID)
					if err != nil || current.Job.State == ExportCancelled {
						return
					}
					if current.Job.State != ExportClaimed || current.Job.Claim != r.Header.Get("X-Kelvo-Claim") {
						t.Error("execute did not own one claim")
						w.WriteHeader(409)
						return
					}
					if mode == "gateway-close-after-read" {
						g.cancel()
						awaitExportGatewayState(t, store, ExportCancelled)
					}
					candidate := exportGatewayStored(t, store, current, wire, stats)
					// A worker completion must not overwrite a concurrent
					// withdrawal or authority renewal in this CAS fixture.
					_, err = store.CompareAndSwapExport(r.Context(), current, candidate)
					if errors.Is(err, ErrExportConflict) {
						continue
					}
					if err != nil {
						return
					}
					stored = candidate
					break
				}
				if stored.State != ExportStored {
					w.WriteHeader(409)
					return
				}
				if mode == "lost-response" {
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error("fixture could not cut the completion connection")
						return
					}
					_ = connection.Close()
					return
				}
				result := ExportRunResult{Receipt: *stored.Receipt, Stats: stats}
				if mode == "receipt-mismatch" {
					result.Receipt.ReceiptSHA256 = strings.Repeat("0", 64)
				}
				_ = json.NewEncoder(w).Encode(result)
			})
			if mode == "ready-cas-revocation" {
				store.beforeReady = auth.invalidate
			}
			w := httptest.NewRecorder()
			request, cancelRequest := context.WithCancel(context.Background())
			r := exportGatewayRequest("POST", "/v1/exports", rotationOld, `{"query":{"mode":"federated","sql":"SELECT id, label FROM sales","sources":["sales"]}}`).WithContext(request)
			if v := exportGatewayCall(g, w, r); v != nil || w.Code != 201 {
				t.Fatal("submission failed", w.Code, w.Body.String(), v)
			}
			cancelRequest()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("detached supervisor did not execute")
			}
			if mode == "gateway-close" {
				g.cancel()
			} else if mode == "key-revocation" {
				auth.invalidate()
			}
			unblock()
			state := ExportCancelled
			if mode == "success" {
				state = ExportReady
			}
			final := awaitExportGatewayState(t, store, state)
			if executions.Load() != 1 {
				t.Fatal("execution replayed", executions.Load())
			}
			if mode == "success" && (final.Job.Stats.Rows != 128 || final.Job.Receipt == nil) {
				t.Fatal("ready state lost exact receipt")
			}
			if mode == "ready-cas-revocation" {
				store.mu.Lock()
				attempts := store.readyAttempts
				store.mu.Unlock()
				if attempts != 1 {
					t.Fatal("fixture missed in-flight readiness CAS", attempts)
				}
			}
		})
	}
}

type exportGatewayWriteHook struct {
	*httptest.ResponseRecorder
	once sync.Once
	hook func()
}

func (w *exportGatewayWriteHook) Write(data []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(data)
	w.once.Do(w.hook)
	return n, err
}
func (w *exportGatewayWriteHook) Unwrap() http.ResponseWriter { return w.ResponseRecorder }

func TestGatewayExportPartRelayVerifiesBytesAndFinalAuthority(t *testing.T) {
	for _, codec := range []string{"none", "lz4_frame"} {
		for _, mode := range []string{"success", "revoked", "document-expired", "cancelled", "changed-receipt", "corrupt", "truncated"} {
			t.Run(codec+"/"+mode, func(t *testing.T) {
				g, store, auth := exportGatewayFixture(t, codec)
				wire, stats := exportGatewayWire(t, codec)
				seedExportGatewayReady(t, g, store, wire, stats)
				var requests atomic.Int32
				installExportGatewayWorker(t, g, func(w http.ResponseWriter, r *http.Request) {
					current, _ := store.GetExport(context.Background(), exportGatewayID)
					if r.URL.Path != "/internal/exports/"+exportGatewayID+"/parts/0" || len(r.Header.Values("X-Kelvo-Export-Receipt")) != 1 || r.Header.Get("X-Kelvo-Export-Receipt") != current.Job.Receipt.ReceiptSHA256 || r.Header.Get("Authorization") != "" {
						t.Error("invalid part envelope")
						w.WriteHeader(400)
						return
					}
					requests.Add(1)
					payload := append([]byte(nil), wire...)
					if mode == "corrupt" {
						payload[len(payload)/2] ^= 1
					}
					if mode == "truncated" {
						payload = payload[:len(payload)-8]
					}
					w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
					w.WriteHeader(200)
					if flush, ok := w.(http.Flusher); ok {
						flush.Flush()
					}
					_, _ = w.Write(payload)
				})
				hook := func() {
					switch mode {
					case "revoked":
						auth.invalidate()
					case "document-expired":
						auth.mu.Lock()
						auth.validUntil = time.Now().Add(-time.Second)
						auth.mu.Unlock()
					case "cancelled":
						store.mu.Lock()
						store.job.Job.State = ExportCancelled
						store.job.Job.Error = &query.Error{Code: "CANCELLED", Message: "Export cancelled"}
						store.job.Revision++
						store.mu.Unlock()
					case "changed-receipt":
						store.mu.Lock()
						manifest := store.job.Job.Receipt.Manifest
						manifest.SchemaSHA256 = strings.Repeat("2", 64)
						receipt, err := sealExportReceipt(*store.job.Job.Local, manifest)
						if err != nil {
							t.Error(err)
						}
						store.job.Job.Receipt = &receipt
						store.job.Revision++
						store.mu.Unlock()
					}
				}
				for attempt := 0; attempt < 2; attempt++ {
					w := &exportGatewayWriteHook{ResponseRecorder: httptest.NewRecorder(), hook: hook}
					panicValue := exportGatewayCall(g, w, exportGatewayRequest("GET", "/v1/exports/"+exportGatewayID+"/parts/0", rotationNew, ""))
					if mode == "success" {
						if panicValue != nil || w.Code != 200 || !bytes.Equal(w.Body.Bytes(), wire) {
							t.Fatal("repeated download changed complete bytes", w.Code, panicValue)
						}
						reader, err := ipc.NewReader(bytes.NewReader(w.Body.Bytes()))
						if err != nil {
							t.Fatal(err)
						}
						if !reader.Next() || reader.RecordBatch().NumRows() != 128 || reader.RecordBatch().Column(0).(*array.Int64).Value(0) != 9007199254740993 || !reader.RecordBatch().Column(1).IsNull(0) {
							t.Fatal("Arrow values changed")
						}
						reader.Release()
					} else {
						if panicValue != http.ErrAbortHandler || bytes.HasSuffix(w.Body.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
							t.Fatal("failed/revoked part got EOS", mode, w.Code, panicValue)
						}
						break
					}
				}
				if mode == "success" && requests.Load() != 2 {
					t.Fatal("ready export was not repeatable")
				}
			})
		}
	}
}

func TestGatewayExportAdmissionAndDrainBoundDetachedWork(t *testing.T) {
	g, store, _ := exportGatewayFixture(t, "none")
	body := `{"query":{"mode":"federated","sql":"SELECT id FROM sales","sources":["sales"]}}`
	w := httptest.NewRecorder()
	g.ServeHTTP(w, exportGatewayRequest("POST", "/v1/exports", rotationOld, body))
	if w.Code != 201 {
		t.Fatal("first export rejected", w.Code)
	}
	w = httptest.NewRecorder()
	g.ServeHTTP(w, exportGatewayRequest("POST", "/v1/exports", rotationOld, body))
	if w.Code != 429 {
		t.Fatal("supervisor admission exceeded", w.Code)
	}
	store.mu.Lock()
	submissions := store.submissions
	store.mu.Unlock()
	if submissions != 1 || len(g.exportSupervisors) != 1 {
		t.Fatal("HTTP return released detached reservation")
	}
	g.BeginDrain()
	w = httptest.NewRecorder()
	g.ServeHTTP(w, exportGatewayRequest("POST", "/v1/exports", rotationOld, body))
	if w.Code != 503 {
		t.Fatal("drain accepted export", w.Code)
	}
	w = httptest.NewRecorder()
	g.ServeHTTP(w, exportGatewayRequest("GET", "/v1/exports/"+exportGatewayID, rotationOld, ""))
	if w.Code != 200 {
		t.Fatal("drain hid accepted export", w.Code)
	}
	g.cancel()
	awaitExportGatewayState(t, store, ExportCancelled)
}

func TestGatewayExportNeverRenewsExpiredAuthorityOrAdoptsClaim(t *testing.T) {
	g, store, auth := exportGatewayFixture(t, "none")
	wire, stats := exportGatewayWire(t, "none")
	seedExportGatewayReady(t, g, store, wire, stats)
	store.mu.Lock()
	job := store.job.Job
	job.State = ExportClaimed
	job.StartedAt, job.ExecutionDeadline = time.Time{}, time.Time{}
	job.Local, job.Receipt, job.Stats = nil, nil, query.Stats{}
	store.job.Job = job
	initial := cloneExportGatewaySnapshot(store.job)
	store.mu.Unlock()
	_, key, _ := auth.lookup(rotationOld)
	ctx := context.WithValue(context.Background(), jobAuthorityKey{}, job.Authority.Principal)
	ctx = context.WithValue(ctx, keyAuthorizationContext{}, keyAuthorization{auth, key})
	if err := g.renewExportAuthority(ctx, store, initial); err == nil {
		t.Fatal("expired authority was renewed")
	}
	after, _ := store.GetExport(ctx, job.ID)
	if after.Revision != initial.Revision {
		t.Fatal("expired authority mutated durable state")
	}
	store.mu.Lock()
	store.job.Job.AuthorityUntil = time.Now().Add(4 * time.Second)
	store.mu.Unlock()
	if err := g.renewExportAuthority(ctx, store, initial); err != nil {
		t.Fatal("fresh authority check failed", err)
	}
	after, _ = store.GetExport(ctx, job.ID)
	if after.Revision != initial.Revision {
		t.Fatal("fresh authority performed a redundant CAS")
	}
	store.mu.Lock()
	store.job.Job.AuthorityUntil = time.Now().Add(time.Second)
	store.mu.Unlock()
	if err := g.renewExportAuthority(ctx, store, initial); err != nil {
		t.Fatal("bounded authority renewal failed", err)
	}
	after, _ = store.GetExport(ctx, job.ID)
	if !after.Job.HeartbeatAt.Equal(initial.Job.HeartbeatAt) || !after.Job.StartedAt.IsZero() {
		t.Fatal("supervisor renewal impersonated a worker heartbeat")
	}
	if _, err := g.claimSupervisedExport(ctx, store, initial); err == nil {
		t.Fatal("existing claim was adopted")
	}
	store.mu.Lock()
	store.job.Job.SupervisorOwner = strings.Repeat("9", 32)
	store.mu.Unlock()
	if err := g.renewExportAuthority(ctx, store, initial); err == nil {
		t.Fatal("foreign supervisor was adopted")
	}
}

// Keep these compile-time assertions close to the fixture's controlled scope.
var _ ExportStore = (*exportGatewayStore)(nil)
