//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.yaml.in/yaml/v3"
)

func serviceAuditConfig(t *testing.T) *ServiceAuditConfig {
	t.Helper()
	return &ServiceAuditConfig{ServiceID: "replica-one", Config: audit.Config{Directory: filepath.Join(t.TempDir(), "journal"), MaxEntries: 16, MaxPending: 4, Retention: time.Hour, WriteTimeout: 2 * time.Second}}
}
func openServiceAudit(t *testing.T, kind string) (*ServiceAudit, *ServiceAuditConfig) {
	t.Helper()
	cfg := serviceAuditConfig(t)
	s, err := OpenServiceAudit(cfg, kind, []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.CloseBounded() })
	return s, cfg
}
func serviceEvents(t *testing.T, s *ServiceAudit) []audit.Event {
	t.Helper()
	_ = s.CloseBounded()
	page, err := audit.ReadPage(context.Background(), s.config.Directory, 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	return page.Events
}
func corruptAudit(t *testing.T, s *ServiceAudit) {
	t.Helper()
	if err := os.Truncate(filepath.Join(s.config.Directory, "journal.bin"), 1); err != nil {
		t.Fatal(err)
	}
}

type auditedStore struct {
	*nodeTestStore
	submitted    atomic.Int32
	beforeSubmit func()
	beforeCAS    atomic.Pointer[func(Job)]
}

func (s *auditedStore) Submit(ctx context.Context, r query.Request) (Snapshot, error) {
	s.submitted.Add(1)
	if s.beforeSubmit != nil {
		s.beforeSubmit()
	}
	a, err := submissionAuthority(ctx, s.p, r)
	if err != nil {
		return Snapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job := Job{ID: "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", TenantID: s.p.TenantID, Request: r, Authority: a, State: Queued, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute)}
	value := Snapshot{Job: job, Revision: 1}
	s.jobs[job.ID] = value
	return value, nil
}
func (s *auditedStore) setBeforeCAS(hook func(Job)) { s.beforeCAS.Store(&hook) }
func (s *auditedStore) CompareAndSwap(ctx context.Context, old Snapshot, next Job) (Snapshot, error) {
	if hook := s.beforeCAS.Load(); hook != nil {
		(*hook)(next)
	}
	return s.nodeTestStore.CompareAndSwap(ctx, old, next)
}
func auditGatewayFixture(t *testing.T) (*Gateway, *auditedStore, *ServiceAudit) {
	t.Helper()
	a, _ := openServiceAudit(t, "gateway")
	s := &auditedStore{nodeTestStore: &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 16)}}
	g := &Gateway{audit: a, tenants: map[string]gatewayTenant{"a": {store: s}}, tokens: map[[32]byte]string{sha256.Sum256([]byte(rotationOld)): "a"}, permits: make(chan struct{}, 4), resultWaiters: make(chan struct{}, 4), ctx: context.Background(), reconcileOK: map[string]bool{"a": true}}
	return g, s, a
}
func auditRequest(method, path, body string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+rotationOld)
	return r
}

func TestAuditServiceConfigurationIsExplicitAndDoesNotOpenOnLoad(t *testing.T) {
	cfg := serviceAuditConfig(t)
	raw, err := yaml.Marshal(map[string]any{"audit": cfg})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "gateway.yml")
	if err = os.WriteFile(path, append([]byte(gatewayYAML), raw...), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadGateway(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Audit == nil || *loaded.Audit != *cfg {
		t.Fatal("audit YAML lost configuration")
	}
	if _, err = os.Stat(cfg.Directory); !os.IsNotExist(err) {
		t.Fatal("configuration loading created audit storage")
	}
	for _, id := range []string{"", "../replica", "Replica", "a" + strings.Repeat("b", 64)} {
		bad := *cfg
		bad.ServiceID = id
		if bad.Validate() == nil {
			t.Fatal("invalid audit service identity")
		}
	}
	for _, scratch := range []string{cfg.Directory, filepath.Dir(cfg.Directory), filepath.Join(cfg.Directory, "scratch")} {
		if validateNodeAudit(NodeConfig{Audit: cfg, ScratchDirectory: scratch}) == nil {
			t.Fatal("audit storage overlaps writable scratch")
		}
	}
	if validateNodeAudit(NodeConfig{Audit: cfg, ScratchDirectory: filepath.Join(filepath.Dir(cfg.Directory), "scratch")}) != nil {
		t.Fatal("disjoint audit and scratch rejected")
	}
}

func TestAuditGatewayAuthenticatesIdentityAndNeverPersistsPayload(t *testing.T) {
	g, s, a := auditGatewayFixture(t)
	s.p = principalTestPolicy()
	g.auth = bareAuthenticator(t)
	if !g.auth.apply(principalKeySet(t, 1, map[string][]string{"analyst": {rotationOld}}, map[string][]string{}), time.Now()) {
		t.Fatal("keys")
	}
	r := auditRequest("POST", "/v1/queries", `{"mode":"native","connection_id":"sales_native","sql":"SELECT private_value FROM private_table"}`)
	r.Header.Set("X-Kelvo-Principal", "administrator")
	r.Header.Set("X-Kelvo-Tenant", "foreign")
	s.beforeSubmit = func() {
		if a.journal.Snapshot().Active != 1 {
			t.Fatal("submit ran before durable start")
		}
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 201 {
		t.Fatalf("submit: %d %s", w.Code, w.Body.String())
	}
	denied := httptest.NewRecorder()
	g.ServeHTTP(denied, httptest.NewRequest("POST", "/v1/queries", nil))
	if denied.Code != 401 {
		t.Fatal("authentication denial changed")
	}
	events := serviceEvents(t, a)
	if len(events) != 2 {
		t.Fatalf("events: %+v", events)
	}
	var success, denial bool
	for _, event := range events {
		if event.Kind == audit.QuerySubmit {
			success = event.Binding.TenantID == "a" && event.Binding.PrincipalID == "analyst" && event.Binding.PrincipalKind == "user" && event.Binding.PolicyVersion == principalPolicyVersion(s.p) && event.Outcome == audit.Succeeded
		}
		if event.Kind == audit.Authentication {
			denial = event.Binding.TenantID == "" && event.Binding.PrincipalKind == "unknown" && event.Binding.PrincipalID == "" && event.Outcome == audit.Denied
		}
	}
	if !success || !denial {
		t.Fatalf("untrusted authority recorded: %+v", events)
	}
	raw, err := os.ReadFile(filepath.Join(a.config.Directory, "journal.bin"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{rotationOld, "private_value", "private_table", "sales_native", "administrator", "foreign"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatal("payload entered audit", secret)
		}
	}
}

func TestAuditGatewayFullRetentionRejectsEffectsAndKeepsDenial(t *testing.T) {
	g, s, a := auditGatewayFixture(t)
	for range a.config.MaxEntries {
		if err := a.journal.Record(context.Background(), a.binding("a", nil), audit.QuerySubmit, audit.Denied, audit.InvalidRequest); err != nil {
			t.Fatal(err)
		}
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, auditRequest("POST", "/v1/queries", `{"mode":"federated","sql":"SELECT 1"}`))
	if w.Code != 503 || s.submitted.Load() != 0 {
		t.Fatal("full audit admitted submission")
	}
	ready := httptest.NewRecorder()
	g.ServeHTTP(ready, httptest.NewRequest("GET", "/ready", nil))
	if ready.Code != 503 {
		t.Fatal("full audit remained ready")
	}
	denied := httptest.NewRecorder()
	g.ServeHTTP(denied, httptest.NewRequest("POST", "/v1/queries", nil))
	if denied.Code != 401 {
		t.Fatal("audit failure authorized a rejected caller")
	}
}

func TestAuditGatewaySubmissionAndCancellationWithholdSuccessOnStorageFailure(t *testing.T) {
	for _, operation := range []string{"submit", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			g, s, a := auditGatewayFixture(t)
			path := "/v1/queries"
			body := `{"mode":"federated","sql":"SELECT 1"}`
			if operation == "submit" {
				s.beforeSubmit = func() { corruptAudit(t, a) }
			} else {
				id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				s.add(id)
				path += "/" + id + "/cancel"
				body = ""
				s.setBeforeCAS(func(next Job) {
					if next.State == Cancelled {
						if a.journal.Snapshot().Active != 1 {
							t.Fatal("cancellation preceded audit start")
						}
						corruptAudit(t, a)
					}
				})
			}
			w := httptest.NewRecorder()
			g.ServeHTTP(w, auditRequest("POST", path, body))
			if w.Code != 503 || a.Ready() {
				t.Fatal("storage failure acknowledged success")
			}
			before := s.submitted.Load()
			w = httptest.NewRecorder()
			g.ServeHTTP(w, auditRequest("POST", "/v1/queries", `{"mode":"federated","sql":"SELECT 1"}`))
			if s.submitted.Load() != before {
				t.Fatal("new effects after failed audit")
			}
		})
	}
}

type auditRoundTrip func(*http.Request) (*http.Response, error)

func (f auditRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAuditGatewayRelayGatesSuccessCASAndEOS(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable", true: "terminal-failure"}[fail], func(t *testing.T) {
			g, s, a := auditGatewayFixture(t)
			id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			snapshot := Snapshot{Revision: 1, Job: Job{ID: id, TenantID: "a", State: Assigned, WorkerID: "a1", Owner: "owner", ExpiresAt: time.Now().Add(time.Minute), Request: query.Request{Mode: "federated", SQL: "SELECT 1"}}}
			s.jobs[id] = snapshot
			payload, _ := compressedRelayFixture(t, "none")
			committed := false
			s.setBeforeCAS(func(next Job) {
				if next.State == Succeeded {
					committed = true
					if a.journal.Snapshot().Active != 0 || a.journal.Snapshot().Retained != 1 {
						t.Fatal("success CAS preceded terminal audit")
					}
				}
			})
			u, _ := url.Parse("https://worker.invalid")
			client := &http.Client{Transport: auditRoundTrip(func(r *http.Request) (*http.Response, error) {
				old, err := s.Get(r.Context(), id)
				if err != nil {
					return nil, err
				}
				next := old.Job
				next.State = ResultReady
				if _, err = s.CompareAndSwap(r.Context(), old, next); err != nil {
					return nil, err
				}
				if fail {
					corruptAudit(t, a)
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
			})}
			g.tenants["a"] = gatewayTenant{store: s, workers: map[string]workerEndpoint{"a1": {url: u, client: client}}}
			w := httptest.NewRecorder()
			var panicked any
			func() {
				defer func() { panicked = recover() }()
				g.ServeHTTP(w, auditRequest("GET", "/v1/queries/"+id+"/results", ""))
			}()
			if fail {
				if panicked != http.ErrAbortHandler || committed || bytes.HasSuffix(w.Body.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
					t.Fatal("uncertain audit emitted success/EOS")
				}
			} else {
				if panicked != nil || !committed || !bytes.Equal(w.Body.Bytes(), payload) {
					t.Fatal("durable relay changed Arrow payload")
				}
				reader, err := ipc.NewReader(bytes.NewReader(w.Body.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Release()
				rows := int64(0)
				for reader.Next() {
					rows += reader.RecordBatch().NumRows()
				}
				if reader.Err() != nil || rows != 768 {
					t.Fatal("relay value count changed")
				}
				if events := serviceEvents(t, a); len(events) != 1 || events[0].Binding.PrincipalKind != "unknown" || events[0].Outcome != audit.Succeeded {
					t.Fatal("legacy identity or local outcome changed")
				}
			}
		})
	}
}

func TestAuditCanceledCompletionUsesFreshContextAndRetainsUnknownRecovery(t *testing.T) {
	a, _ := openServiceAudit(t, "worker")
	ctx, cancel := context.WithCancel(context.Background())
	op, err := a.begin(ctx, "a", nil, audit.QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err = op.complete(ctx.Err()); err != nil {
		t.Fatal("canceled context discarded terminal audit", err)
	}
	if _, err = a.begin(context.Background(), "a", nil, audit.QueryExecution); err != nil {
		t.Fatal(err)
	}
	events := serviceEvents(t, a)
	var cancelled, unknown int
	for _, event := range events {
		if event.Outcome == audit.Cancelled {
			cancelled++
		}
		if event.Outcome == audit.Unknown {
			unknown++
		}
	}
	if cancelled != 1 || unknown != 1 {
		t.Fatal("shutdown invented success or lost cancellation")
	}
}

type auditSource struct {
	calls  atomic.Int32
	probes atomic.Int32
	before func()
}

func (e *auditSource) Execute(_ context.Context, r query.Request, sink query.Sink) (query.Stats, error) {
	if r.SQL == "SELECT 1" {
		e.probes.Add(1)
		return query.Stats{}, nil
	}
	e.calls.Add(1)
	if e.before != nil {
		e.before()
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(42)
	a := b.NewArray()
	defer a.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{a}, 1)
	defer record.Release()
	return query.Stats{Rows: 1}, sink.Write(record)
}
func auditNodeRequest(path string) *http.Request {
	r := httptest.NewRequest("GET", path, nil)
	u, _ := url.Parse(GatewayIdentity)
	cert := &x509.Certificate{URIs: []*url.URL{u}}
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
	return r
}

func TestAuditNodeStopsBeforeExecutionAndBeforeResultReady(t *testing.T) {
	for _, stage := range []string{"success", "before-execution", "terminal"} {
		t.Run(stage, func(t *testing.T) {
			cfg := serviceAuditConfig(t)
			store := &auditedStore{nodeTestStore: &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}}
			executor := &auditSource{}
			n, err := newNode(NodeConfig{Policy: store.p, WorkerID: "a1", Audit: cfg}, store, executor)
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			store.add(id)
			waitFor(t, func() bool { v, _ := store.Get(context.Background(), id); return v.Job.State == Assigned })
			old, _ := store.Get(context.Background(), id)
			next := old.Job
			next.State = Claimed
			next.Claim = strings.Repeat("b", 32)
			if _, err = store.CompareAndSwap(context.Background(), old, next); err != nil {
				t.Fatal(err)
			}
			readyPublished := false
			store.setBeforeCAS(func(next Job) {
				if next.State == ResultReady {
					readyPublished = true
					if n.audit.journal.Snapshot().Active != 0 || n.audit.journal.Snapshot().Retained != 2 {
						t.Fatal("ResultReady preceded audit terminal")
					}
				}
			})
			if stage == "before-execution" {
				corruptAudit(t, n.audit)
			}
			executor.before = func() {
				if n.audit.journal.Snapshot().Active != 1 {
					t.Fatal("executor ran without durable start")
				}
				if stage == "terminal" {
					corruptAudit(t, n.audit)
				}
			}
			r := auditNodeRequest("/internal/queries/" + id + "/results")
			r.Header.Set("X-Kelvo-Claim", next.Claim)
			w := httptest.NewRecorder()
			var panicked any
			func() { defer func() { panicked = recover() }(); n.ServeHTTP(w, r) }()
			if stage == "success" {
				if panicked != nil || !readyPublished || executor.calls.Load() != 1 {
					t.Fatal("audited execution failed")
				}
				if err = n.Close(); err != nil {
					t.Fatal(err)
				}
				events := serviceEvents(t, n.audit)
				if len(events) != 2 {
					t.Fatal("startup or execution receipt absent")
				}
			} else {
				if readyPublished || n.audit.Ready() || bytes.HasSuffix(w.Body.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
					t.Fatal("uncertain audit published worker success")
				}
				if stage == "before-execution" && executor.calls.Load() != 0 {
					t.Fatal("source ran after failed start")
				}
			}
		})
	}
}

func TestAuditRefreshFailurePreservesPublishedGenerationAndStopsAutomaticRetry(t *testing.T) {
	a, _ := openServiceAudit(t, "worker")
	source := &auditSource{}
	c := catalog.Config{Sources: []catalog.Source{{ID: "source", Type: "clickhouse", URLEnv: "SOURCE_PRIVATE_URL"}}, Acceleration: &catalog.AccelerationConfig{Directory: filepath.Join(t.TempDir(), "snapshots"), TenantID: "a", Datasets: []catalog.Dataset{{ID: "orders_fast", Query: query.Request{Mode: "native", ConnectionID: "source", SQL: "SELECT id FROM orders"}, RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}}}}
	m, err := acceleration.NewManager(c, func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var generation string
	err = a.RunRefresh(context.Background(), testPolicy(), func(ctx context.Context) error {
		if a.journal.Snapshot().Active != 1 {
			t.Fatal("refresh began without receipt")
		}
		snapshot, err := m.Refresh(ctx, "orders_fast", true)
		if err != nil {
			return err
		}
		generation = snapshot.Generation
		corruptAudit(t, a)
		return nil
	})
	if err == nil || !classifyRefreshFailure(err).Permanent {
		t.Fatal("audit failure would automatically retry refresh")
	}
	current, err := m.Status("orders_fast")
	if err != nil || current.Generation != generation || source.calls.Load() != 1 {
		t.Fatal("published generation lost or replayed")
	}
	err = a.RunRefresh(context.Background(), testPolicy(), func(context.Context) error { t.Fatal("unhealthy audit repeated protected refresh"); return nil })
	if err == nil {
		t.Fatal("refresh resumed without operator recovery")
	}
}

func TestAuditNodeOpensBeforeProbeAndLeavesSharedJournalToLifecycleOwner(t *testing.T) {
	cfg := serviceAuditConfig(t)
	cfg.Directory = t.TempDir()
	store := &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}
	source := &auditSource{}
	if n, err := newNode(NodeConfig{Policy: store.p, WorkerID: "a1", Audit: cfg}, store, source); err == nil {
		n.Close()
		t.Fatal("unsafe audit directory admitted startup")
	}
	if source.probes.Load() != 0 {
		t.Fatal("startup probe executed before audit opened")
	}
	a, sharedConfig := openServiceAudit(t, "worker")
	n, err := newNode(NodeConfig{Policy: store.p, WorkerID: "a1", Audit: sharedConfig, RuntimeAudit: a}, store, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
	if !a.Ready() {
		t.Fatal("node closed journal still owned by refresh lifecycle")
	}
	if err = a.RunRefresh(context.Background(), store.p, func(context.Context) error { return nil }); err != nil {
		t.Fatal("shared refresh journal prematurely closed", err)
	}
	events := serviceEvents(t, a)
	if len(events) != 2 {
		t.Fatal("startup and shared refresh observations absent")
	}
	for _, event := range events {
		if event.Binding.PrincipalKind != "service" || event.Binding.PrincipalID != sharedConfig.ServiceID || event.Binding.PolicyVersion != principalPolicyVersion(store.p) {
			t.Fatal("service work attributed to a user")
		}
	}
}

func TestAuditOperationRejectsCompetingExplicitOutcomesAndIgnoresFallback(t *testing.T) {
	a, _ := openServiceAudit(t, "worker")
	op, err := a.begin(context.Background(), "a", nil, audit.QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	results := make(chan error, 2)
	for _, cause := range []error{nil, context.Canceled} {
		go func() { <-gate; results <- op.complete(cause) }()
	}
	close(gate)
	success, conflicts := 0, 0
	for range 2 {
		err := <-results
		if err == nil {
			success++
		} else if errors.Is(err, audit.ErrInvalid) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatal("conflicting completion was silently accepted")
	}
	op.abort(context.Background())
	if events := serviceEvents(t, a); len(events) != 1 || events[0].Outcome == audit.Unknown {
		t.Fatal("fallback overwrote explicit outcome")
	}
}

func TestAuditTypedCancellationCodesRemainCancellations(t *testing.T) {
	a, _ := openServiceAudit(t, "worker")
	for _, code := range []string{"CANCELLED", "DEADLINE_EXCEEDED"} {
		op, err := a.begin(context.Background(), "a", nil, audit.QueryExecution)
		if err != nil {
			t.Fatal(err)
		}
		if err = op.complete(query.NewError(code, "private error must not persist")); err != nil {
			t.Fatal(err)
		}
	}
	var canceled, timedOut bool
	for _, event := range serviceEvents(t, a) {
		if event.Outcome != audit.Cancelled {
			t.Fatal("typed cancellation became execution failure")
		}
		canceled = canceled || event.Category == audit.Cancellation
		timedOut = timedOut || event.Category == audit.Timeout
	}
	if !canceled || !timedOut {
		t.Fatal("cancellation categories lost")
	}
}

func TestAuditDirectWorkerCancellationUsesVerifiedGatewayActor(t *testing.T) {
	for _, stage := range []string{"success", "before-effect", "terminal"} {
		t.Run(stage, func(t *testing.T) {
			p := principalTestPolicy()
			grant := p.Access.Principals["analyst"]
			grant.AllowLiteralQueries = true
			p.Access.Principals["analyst"] = grant
			s := &auditedStore{nodeTestStore: &nodeTestStore{p: p, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}}
			n, err := newNode(NodeConfig{Policy: p, WorkerID: "a1", Audit: serviceAuditConfig(t)}, s, &auditSource{})
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			authority, _ := authorityForPrincipal(p, "analyst")
			s.mu.Lock()
			s.jobs[id] = Snapshot{Revision: 1, Job: Job{ID: id, TenantID: "a", State: Queued, Authority: &authority, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute), Request: query.Request{Mode: "federated", SQL: "SELECT 2"}}}
			s.mu.Unlock()
			if err = s.Enqueue(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { value, _ := s.Get(context.Background(), id); return value.Job.State == Assigned })
			if stage == "before-effect" {
				_ = n.audit.CloseBounded()
			}
			if stage == "terminal" {
				s.setBeforeCAS(func(next Job) {
					if next.State == Cancelled {
						corruptAudit(t, n.audit)
					}
				})
			}
			r := auditNodeRequest("/internal/queries/" + id + "/cancel")
			r.Method = "POST"
			w := httptest.NewRecorder()
			n.ServeHTTP(w, r)
			current, _ := s.Get(context.Background(), id)
			if stage == "before-effect" {
				if w.Code != 503 || current.Job.State != Assigned {
					t.Fatal("cancel changed state without audit start")
				}
				return
			}
			if current.Job.State != Cancelled {
				t.Fatal("accepted cancellation did not change state")
			}
			if stage == "terminal" {
				if w.Code != 503 || n.audit.Ready() {
					t.Fatal("cancel acknowledged audit failure")
				}
				return
			}
			if w.Code != 204 {
				t.Fatal("cancel failed")
			}
			if err = n.Close(); err != nil {
				t.Fatal(err)
			}
			var found bool
			for _, event := range serviceEvents(t, n.audit) {
				if event.Kind == audit.QueryCancel {
					found = true
					if event.Binding.PrincipalID != "gateway" || event.Binding.PrincipalKind != "service" || event.Outcome != audit.Succeeded {
						t.Fatal("cancel attributed to original user")
					}
				}
			}
			if !found {
				t.Fatal("direct cancellation missing audit")
			}
		})
	}
}
