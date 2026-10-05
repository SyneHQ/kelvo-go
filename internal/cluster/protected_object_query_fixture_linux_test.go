//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/testutil/protectedobject"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.yaml.in/yaml/v3"
)

// The barrier delegates every state change to the real broker. In particular,
// a successful terminal CAS remains durable when the local key is then revoked.
type protectedQueryStore struct {
	*NATSStore
	mu           sync.Mutex
	afterSuccess func()
	commits      int
}

func (s *protectedQueryStore) CompareAndSwap(ctx context.Context, old Snapshot, next Job) (Snapshot, error) {
	result, err := s.NATSStore.CompareAndSwap(ctx, old, next)
	if err == nil && next.State == Succeeded {
		s.mu.Lock()
		s.commits++
		after := s.afterSuccess
		s.afterSuccess = nil
		s.mu.Unlock()
		if after != nil {
			after()
		}
	}
	return result, err
}

type protectedQueryTenant struct {
	config            catalog.Config
	policy            Policy
	runtime           *acceleration.ObjectRuntime
	manager           *acceleration.Manager
	node              *Node
	store             *protectedQueryStore
	scratch           *worker.ScratchRoot
	scratchPath       string
	length            atomic.Bool
	framed            atomic.Int64
	allowCloseFailure bool
	wireMu            sync.Mutex
	lastWire          []byte
	export            *protectedExportProbe
}

type protectedQueryFixture struct {
	ctx                            context.Context
	service                        *protectedobject.Service
	containment                    *containment.Manager
	pool                           *admission.Pool
	tenants                        map[string]*protectedQueryTenant
	gateway                        *Gateway
	public                         *httptest.Server
	client                         *http.Client
	keys                           map[string]map[string][]string
	keyFile                        string
	revision                       uint64
	factories, executions, secrets atomic.Int64
}

type protectedQuerySecrets struct{ calls *atomic.Int64 }

func (r protectedQuerySecrets) Resolve(context.Context, string) (string, bool, error) {
	r.calls.Add(1)
	return "", false, fmt.Errorf("unexpected native source credential access")
}

func protectedQueryKey(tenant, principal, suffix string) string {
	return "protected-query-fixture-" + tenant + "-" + principal + "-" + suffix + strings.Repeat("k", 32)
}

// The hidden payload forces two physical parts through the real multipart
// encoder; a descriptor containing one small part is insufficient for this gate.
type protectedQueryRows struct {
	tenant string
	calls  *atomic.Int64
}

func (r protectedQueryRows) Execute(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	r.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return query.Stats{}, err
	}
	metadata := arrow.MetadataFrom(map[string]string{"private": "protected-query-private-metadata"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata},
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64, Metadata: metadata},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 30, Scale: 4}, Nullable: true},
		{Name: "observed", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "tiny", Type: arrow.PrimitiveTypes.Int8},
		{Name: "label", Type: arrow.BinaryTypes.String},
		{Name: "payload", Type: arrow.BinaryTypes.String},
	}, &metadata)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{7, 8, 7, 7}, nil)
	b.Field(1).(*array.Uint64Builder).AppendValues([]uint64{1, 2, ^uint64(0), 4}, nil)
	b.Field(2).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(-123456789), decimal128.FromI64(99), {}, decimal128.FromI64(1)}, []bool{true, true, false, true})
	b.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{-315521754876544, 0, 123456789, 0}, []bool{true, true, true, false})
	b.Field(4).(*array.Int8Builder).AppendValues([]int8{-128, 0, 127, 1}, nil)
	for i := 0; i < 4; i++ {
		b.Field(5).(*array.StringBuilder).Append(r.tenant)
		b.Field(6).(*array.StringBuilder).Append(strings.Repeat("private-payload-", 14000))
	}
	record := b.NewRecordBatch()
	defer record.Release()
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	return query.Stats{Rows: 4}, sink.Write(record)
}

func protectedQueryCatalog(t *testing.T, service *protectedobject.Service, tenant string) (catalog.Config, Policy) {
	t.Helper()
	input := filepath.Join(t.TempDir(), "direct.csv")
	if err := os.WriteFile(input, []byte("id\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	storage := service.Storage(tenant)
	config := catalog.Config{Sources: []catalog.Source{
		{ID: "origin", Type: "clickhouse", URLEnv: "KELVO_SOURCE_UNUSED_PROTECTED_QUERY_URL"},
		{ID: "direct", Type: "csv", Path: input},
	}, Acceleration: &catalog.AccelerationConfig{Directory: t.TempDir(), TenantID: tenant, ObjectStorage: &storage}}
	if err := os.Chmod(config.Acceleration.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	p := testPolicy()
	p.TenantID, p.Workers, p.MaxQueries = tenant, map[string]int{tenant + "1": 1}, 8
	p.JobTTL, p.LeaseDuration = 2*time.Minute, 10*time.Second
	p.Limits.Timeout, p.Limits.MemoryMB, p.Limits.MaxTempMB, p.Limits.Threads = 25*time.Second, 64, 16, 1
	p.Limits.MaxBytes, p.Limits.MaxRows = 16<<20, 1024
	p.Limits.ResultCompression = "none"
	if tenant == "b" {
		p.Limits.ResultCompression = "lz4_frame"
	}
	for _, id := range []string{"orders_single", "orders_multi", "hidden_orders"} {
		d := catalog.Dataset{ID: id, Query: query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT fixture"},
			MaxAge: time.Hour, AuthorizationVersion: "protected-query-v1", Limits: p.Limits,
			Verification: &catalog.VerificationLimits{MaxBytes: 16 << 20}}
		if id == "orders_multi" {
			d.Multipart = &catalog.MultipartConfig{MaxPartBytes: 1 << 20, MaxParts: 2}
		}
		config.Acceleration.Datasets = append(config.Acceleration.Datasets, d)
	}
	p.Access = &PrincipalPolicy{Revision: 1, Principals: map[string]PrincipalGrant{}}
	for _, principal := range []string{"analyst", "reports"} {
		columns, row, kind := []string{"id", "amount", "observed", "tiny", "label"}, "7", "user"
		if principal == "reports" {
			columns, row, kind = []string{"id", "tiny", "label"}, "8", "service"
		}
		rules := &access.Policy{Sources: map[string]access.SourcePolicy{}}
		for _, id := range []string{"orders_single", "orders_multi"} {
			rules.Sources[id] = access.SourcePolicy{Tables: map[string]access.TablePolicy{id: {Columns: columns,
				Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: row}}}}
		}
		p.Access.Principals[principal] = PrincipalGrant{Kind: kind, FederatedSources: []string{"orders_single", "orders_multi", "direct"}, RowColumnPolicy: rules}
	}
	approveCatalog(t, &p, config)
	if err := ValidatePolicy(p); err != nil {
		t.Fatal(err)
	}
	return config, p
}

func protectedQueryNATS(t *testing.T, tenant, role string) NATSConfig {
	t.Helper()
	prefix := "KELVO_TEST_QUERY_NATS_" + strings.ToUpper(tenant) + "_" + role
	c := NATSConfig{URL: os.Getenv("KELVO_TEST_QUERY_NATS_URL"), CAFile: os.Getenv("KELVO_TEST_QUERY_NATS_CA_FILE"), Username: os.Getenv(prefix + "_USER"), PasswordEnv: prefix + "_PASSWORD"}
	if c.URL == "" || c.CAFile == "" || c.Username == "" || len(os.Getenv(c.PasswordEnv)) < 32 {
		t.Fatal("missing dedicated protected-query broker identity")
	}
	return c
}

func newProtectedQueryFixture(t *testing.T) *protectedQueryFixture {
	t.Helper()
	return newProtectedQueryFixtureWithOptions(t, protectedQueryFixtureOptions{})
}

type protectedQueryFixtureOptions struct{ exports bool }

func newProtectedQueryFixtureWithOptions(t *testing.T, options protectedQueryFixtureOptions) *protectedQueryFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	f := &protectedQueryFixture{ctx: ctx, service: protectedobject.New(t, protectedobject.Options{Tenants: []string{"a", "b"}}), tenants: map[string]*protectedQueryTenant{}, keys: map[string]map[string][]string{}}
	var err error
	f.containment, err = containment.Open(containment.Config{Root: os.Getenv("KELVO_TEST_CGROUP_ROOT"), StateDirectory: os.Getenv("KELVO_TEST_CGROUP_STATE")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := f.containment.Close(cleanup); err != nil {
			t.Error("containment cleanup", err)
		}
	})
	f.pool, err = admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 256 << 20, ScratchBytes: 32 << 20})
	if options.exports {
		f.pool, err = protectedExportResources().NewPool()
	}
	if err != nil {
		t.Fatal(err)
	}
	gatewayTLS, workersTLS := protectedQueryTLS(t)
	var tenantConfigs []TenantConfig
	stores := map[string]Store{}
	exportStores := map[string]ExportStore{}
	broker := protectedQueryNATS
	if options.exports {
		broker = protectedExportNATS
	}
	for _, tenant := range []string{"a", "b"} {
		x := &protectedQueryTenant{}
		f.tenants[tenant] = x
		x.config, x.policy = protectedQueryCatalog(t, f.service, tenant)
		if options.exports {
			configureProtectedExportPolicy(t, &x.policy)
			x.export = &protectedExportProbe{}
		}
		x.runtime, err = acceleration.OpenObjectRuntime(x.config)
		if x.runtime != nil {
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				if err := x.runtime.Close(cleanup); err != nil && !x.allowCloseFailure {
					t.Error("runtime cleanup", err)
				}
				select {
				case <-x.runtime.Quiesced():
				case <-cleanup.Done():
					t.Error("runtime did not quiesce")
				}
			})
		}
		if err != nil {
			t.Fatal("open protected runtime", err)
		}
		x.manager, err = acceleration.NewManagerWithRuntime(x.config, func(catalog.Config, query.Limits) (query.Executor, error) {
			f.factories.Add(1)
			return protectedQueryRows{tenant: tenant, calls: &f.executions}, nil
		}, x.runtime)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := x.manager.Close(); err != nil {
				t.Error(err)
			}
		})
		x.scratchPath = t.TempDir()
		if err := os.Chmod(x.scratchPath, 0700); err != nil {
			t.Fatal(err)
		}
		x.scratch, err = worker.OpenScratchRoot(x.scratchPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := x.scratch.Close(); err != nil {
				t.Error("scratch cleanup", err)
			}
		})
		e, err := worker.New(x.config, x.policy.Limits)
		if err != nil {
			t.Fatal(err)
		}
		e.Binary, e.SandboxPath, e.ScratchRoot = os.Getenv("KELVO_TEST_BINARY"), os.Getenv("KELVO_TEST_SANDBOX"), x.scratch
		e.Containment, e.ContainmentBudget = f.containment, containment.Budget{NativeOverheadMB: 64, ParentOverheadMB: 32, MaxProcesses: 64}
		e.ResourcePool, e.ResourceOverheadBytes, e.ObjectRuntime = f.pool, 160<<20, x.runtime
		e.Secrets = protectedQuerySecrets{calls: &f.secrets}
		initialize, err := OpenStore(ctx, broker(t, tenant, "INITIALIZER"), x.policy, true)
		if err != nil {
			t.Fatal("initialize protected-query namespace", err)
		}
		if options.exports {
			if _, err := OpenExportStore(ctx, initialize, true); err != nil {
				_ = initialize.Close()
				t.Fatal("initialize protected export namespace", err)
			}
		}
		if err := initialize.Close(); err != nil {
			t.Fatal("close namespace initializer", err)
		}
		workerNATS := broker(t, tenant, "WORKER")
		workerStore, err := OpenStore(ctx, workerNATS, x.policy, false)
		if err != nil {
			t.Fatal("open node store", err)
		}
		nodeConfig := NodeConfig{Policy: x.policy, WorkerID: tenant + "1", NATS: workerNATS, RuntimeResources: f.pool, SandboxPath: e.SandboxPath}
		if options.exports {
			configureProtectedExportNode(t, x, e, &nodeConfig)
		}
		x.node, err = NewNode(nodeConfig, workerStore, e)
		if err != nil {
			_ = workerStore.Close()
			t.Fatal("start protected query node", err)
		}
		t.Cleanup(func() {
			if err := x.node.Close(); err != nil {
				t.Error("node cleanup", err)
			}
		})
		serverTLS, err := BuildServerTLS(workersTLS[tenant], WorkerIdentity(tenant, tenant+"1"), GatewayIdentity)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if x.export != nil && x.export.serve(x, w, r) {
				return
			}
			if !strings.HasSuffix(r.URL.Path, "/results") {
				x.node.ServeHTTP(w, r)
				return
			}
			if !x.length.Load() {
				x.framed.Add(1)
				chunks := &protectedQueryChunks{ResponseWriter: w}
				x.node.ServeHTTP(chunks, r)
				x.wireMu.Lock()
				x.lastWire = bytes.Clone(chunks.wire.Bytes())
				x.wireMu.Unlock()
				return
			}
			// Only HTTP framing changes. Node executes the real worker and commits
			// ResultReady to NATS before these tiny fixture bytes are forwarded.
			recorder := httptest.NewRecorder()
			x.node.ServeHTTP(recorder, r)
			x.wireMu.Lock()
			x.lastWire = bytes.Clone(recorder.Body.Bytes())
			x.wireMu.Unlock()
			for key, values := range recorder.Header() {
				w.Header()[key] = append([]string(nil), values...)
			}
			w.Header().Set("Content-Length", strconv.Itoa(recorder.Body.Len()))
			w.WriteHeader(recorder.Code)
			x.framed.Add(1)
			_, _ = w.Write(recorder.Body.Bytes())
		}))
		server.TLS = serverTLS
		server.StartTLS()
		t.Cleanup(server.Close)
		gatewayNATS := broker(t, tenant, "GATEWAY")
		gatewayStore, err := OpenStore(ctx, gatewayNATS, x.policy, false)
		if err != nil {
			t.Fatal("open gateway store", err)
		}
		x.store = &protectedQueryStore{NATSStore: gatewayStore}
		t.Cleanup(func() { _ = x.store.Close() })
		if options.exports {
			exportStores[tenant], err = OpenExportStore(ctx, gatewayStore, false)
			if err != nil {
				t.Fatal("open protected export gateway store", err)
			}
		}
		stores[tenant] = x.store
		tenantConfigs = append(tenantConfigs, TenantConfig{Policy: x.policy, NATS: gatewayNATS, Workers: []Endpoint{{ID: tenant + "1", URL: server.URL}}})
		f.keys[tenant] = map[string][]string{"analyst": {protectedQueryKey(tenant, "analyst", "old"), protectedQueryKey(tenant, "analyst", "new")}, "reports": {protectedQueryKey(tenant, "reports", "old")}}
	}
	f.keyFile = filepath.Join(t.TempDir(), "principals.yml")
	f.writeKeys(t)
	gatewayConfig := GatewayConfig{WorkerTLS: gatewayTLS, MaxHTTPRequests: 8, Tenants: tenantConfigs,
		Authentication: &GatewayAuthenticationConfig{KeysFile: f.keyFile, ReloadInterval: 30 * time.Second}}
	if options.exports {
		gatewayConfig.Exports = &GatewayExportConfig{MaxSupervisors: 4, MaxDownloads: 2}
		gatewayConfig.RuntimeExportStores = exportStores
	}
	f.gateway, err = NewGateway(gatewayConfig, stores)
	if err != nil {
		for _, s := range stores {
			_ = s.Close()
		}
		t.Fatal("start protected-query gateway", err)
	}
	t.Cleanup(func() {
		if err := f.gateway.Close(); err != nil {
			t.Error("gateway cleanup", err)
		}
	})
	f.public = httptest.NewTLSServer(f.gateway)
	t.Cleanup(f.public.Close)
	f.client = f.public.Client()
	f.client.Timeout = 30 * time.Second
	return f
}

// Split writes across IPC boundaries, and flush to require HTTP chunk framing.
type protectedQueryChunks struct {
	http.ResponseWriter
	wire bytes.Buffer
}

func (w *protectedQueryChunks) Write(data []byte) (int, error) {
	_, _ = w.wire.Write(data)
	written := 0
	for len(data) > 0 {
		n, err := w.ResponseWriter.Write(data[:min(113, len(data))])
		written += n
		data = data[n:]
		if err != nil {
			return written, err
		}
		w.ResponseWriter.(http.Flusher).Flush()
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}
func (w *protectedQueryChunks) Flush()                      { w.ResponseWriter.(http.Flusher).Flush() }
func (w *protectedQueryChunks) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (f *protectedQueryFixture) writeKeys(t *testing.T) gatewayKeySet {
	t.Helper()
	raw, set := f.keyDocument(t)
	publishTLSIdentity(t, f.keyFile, raw)
	return set
}

func (f *protectedQueryFixture) keyDocument(t *testing.T) ([]byte, gatewayKeySet) {
	t.Helper()
	f.revision++
	raw, err := yaml.Marshal(gatewayKeyDocument{Version: 2, Revision: f.revision, Principals: f.keys})
	if err != nil {
		t.Fatal(err)
	}
	set, err := parseGatewayKeys(raw, map[string]bool{"a": true, "b": true}, 1)
	if err != nil {
		t.Fatal(err)
	}
	return raw, set
}

// Prepare bytes on the test goroutine; the HTTP/CAS callback never calls
// testing.Fatal. Rename and apply publish the same complete key revision.
func (f *protectedQueryFixture) prepareKeyChange(t *testing.T) func() bool {
	t.Helper()
	raw, set := f.keyDocument(t)
	pending := filepath.Join(filepath.Dir(f.keyFile), fmt.Sprintf("pending-%d.yml", f.revision))
	if err := os.WriteFile(pending, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return func() bool {
		if err := os.Rename(pending, f.keyFile); err != nil {
			return false
		}
		return f.gateway.auth.apply(set, time.Now())
	}
}

type protectedQueryResponse struct {
	code   int
	data   []byte
	header http.Header
	err    error
}

func (f *protectedQueryFixture) call(ctx context.Context, method, path, key string, payload any, forged bool) protectedQueryResponse {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return protectedQueryResponse{err: err}
		}
		body = bytes.NewReader(raw)
	}
	r, err := http.NewRequestWithContext(ctx, method, f.public.URL+path, body)
	if err != nil {
		return protectedQueryResponse{err: err}
	}
	r.Header.Set("Authorization", "Bearer "+key)
	if forged {
		r.Header.Set("X-Kelvo-Tenant", "a")
		r.Header.Set("X-Kelvo-Principal", "analyst")
	}
	response, err := f.client.Do(r)
	if err != nil {
		return protectedQueryResponse{err: err}
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, (16<<20)+1))
	if len(data) > 16<<20 {
		return protectedQueryResponse{err: fmt.Errorf("fixture result exceeded bound")}
	}
	return protectedQueryResponse{response.StatusCode, data, response.Header.Clone(), err}
}

func (f *protectedQueryFixture) submit(t *testing.T, key string, request query.Request) string {
	t.Helper()
	r := f.call(f.ctx, http.MethodPost, "/v1/queries", key, request, false)
	if r.err != nil || r.code != http.StatusCreated {
		t.Fatalf("submit failed: status=%d error=%v", r.code, r.err)
	}
	var accepted struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(r.data, &accepted) != nil || accepted.ID == "" {
		t.Fatal("invalid query handle")
	}
	return accepted.ID
}

func (f *protectedQueryFixture) waitState(t *testing.T, tenant, id, state string) Snapshot {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		s, err := f.tenants[tenant].store.Get(f.ctx, id)
		if err == nil && s.Job.State == state {
			return s
		}
		if err != nil || time.Now().After(deadline) || s.Job.Terminal() {
			t.Fatalf("query state: wanted=%s observed=%s error=%v", state, s.Job.State, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *protectedQueryFixture) clean(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		active := f.pool.Snapshot().Active + f.containment.Status().Active + f.service.Readers("a") + f.service.Readers("b")
		for _, x := range f.tenants {
			x.node.mu.Lock()
			active += len(x.node.jobs)
			x.node.mu.Unlock()
		}
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("query retained native process, reservation, job, or durable pin")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, x := range f.tenants {
		if reclaimed, err := x.scratch.Reclaim(); err != nil || reclaimed != 0 {
			t.Fatal("query leaked scratch ownership", reclaimed, err)
		}
		entries, err := os.ReadDir(x.scratchPath)
		if err != nil || len(entries) != 1 || entries[0].Name() != ".kelvo-scratch.lock" {
			t.Fatal("query leaked scratch files", err)
		}
	}
	if f.service.Snapshot().Total.Violations != 0 {
		t.Fatal("provider role, tenant, or pinned-read violation")
	}
	if f.secrets.Load() != 0 {
		t.Fatal("protected query accessed native source credentials")
	}
}

func protectedQueryTLS(t *testing.T) (TLSConfig, map[string]TLSConfig) {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "protected-query-fixture"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	serial := int64(1)
	leaf := func(name, identity string) TLSConfig {
		serial++
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		uri, err := url.Parse(identity)
		if err != nil {
			t.Fatal(err)
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{uri}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &leafKey.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		pk, err := x509.MarshalPKCS8PrivateKey(leafKey)
		if err != nil {
			t.Fatal(err)
		}
		c := TLSConfig{CAFile: caPath, CertFile: filepath.Join(dir, name+".pem"), KeyFile: filepath.Join(dir, name+".key")}
		for path, data := range map[string][]byte{c.CertFile: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), c.KeyFile: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})} {
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		return c
	}
	return leaf("gateway", GatewayIdentity), map[string]TLSConfig{"a": leaf("worker-a", WorkerIdentity("a", "a1")), "b": leaf("worker-b", WorkerIdentity("b", "b1"))}
}
