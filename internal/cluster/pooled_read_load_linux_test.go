//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/client"
	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"go.yaml.in/yaml/v3"
)

type loadSample struct {
	T          float64 `json:"t_s"`
	CPUUsec    uint64  `json:"cpu_usage_usec"`
	MemCurrent uint64  `json:"memory_current_bytes"`
	JobGroups  int     `json:"job_cgroups"`
	RSSNode    uint64  `json:"rss_node_bytes"`    // the gateway and node process
	RSSWorkers uint64  `json:"rss_workers_bytes"` // every contained worker and one-shot child
}

// rssOf sums VmRSS of the processes listed by cgroup.procs files under dir.
func rssOf(dir string, recursive bool) (total uint64) {
	if raw, err := os.ReadFile(filepath.Join(dir, "cgroup.procs")); err == nil {
		for _, pid := range strings.Fields(string(raw)) {
			status, err := os.ReadFile("/proc/" + pid + "/status")
			if err != nil {
				continue
			}
			for _, line := range strings.Split(string(status), "\n") {
				if f := strings.Fields(line); len(f) == 3 && f[0] == "VmRSS:" {
					kb, _ := strconv.ParseUint(f[1], 10, 64)
					total += kb << 10
				}
			}
		}
	}
	if recursive {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if e.IsDir() {
				total += rssOf(filepath.Join(dir, e.Name()), true)
			}
		}
	}
	return total
}

func readUint(path, key string) uint64 {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	if key == "" {
		v, _ := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
		return v
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == key {
			v, _ := strconv.ParseUint(f[1], 10, 64)
			return v
		}
	}
	return 0
}

// statusRecorder keeps the status and the start of an error body, so a refusal
// is reported with the gateway's own message rather than the client's.
type statusRecorder struct {
	http.ResponseWriter
	status int
	body   string
}

func (s *statusRecorder) WriteHeader(code int) { s.status = code; s.ResponseWriter.WriteHeader(code) }
func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status >= 400 && len(s.body) < 160 {
		s.body += string(b[:min(len(b), 160)])
	}
	return s.ResponseWriter.Write(b)
}
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// TestPooledReadLoadNode runs the production gateway and node, in this process,
// under a cgroup the caller sized. It serves load from benchmarks/pooled-read-load
// (a separate process, outside the cgroup) until the stop file appears, then
// writes node-report.json. It skips unless scripts/pooled_read_load.py runs it.
func TestPooledReadLoadNode(t *testing.T) {
	scenarioPath, out := os.Getenv("KELVO_LOAD_SCENARIO"), os.Getenv("KELVO_LOAD_OUT")
	if scenarioPath == "" || out == "" {
		t.Skip("run through scripts/pooled_read_load.py")
	}
	get := func(name string) string {
		v := os.Getenv("KELVO_LOAD_" + name)
		if v == "" {
			t.Fatal("missing KELVO_LOAD_" + name)
		}
		return v
	}
	var sc loadScenario
	raw, err := os.ReadFile(scenarioPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &sc); err != nil {
		t.Fatal("scenario:", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	root := t.TempDir()
	cgroupRoot, cgroupState := get("CGROUP_ROOT"), get("CGROUP_STATE")
	unitCgroup := filepath.Dir(cgroupRoot) // the delegated service; jobs/ is a child

	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	gatewayTLS, workerTLS := exportIntegrationTLS(t)
	privateTLS, err := OperationInputTLS(gatewayTLS)
	if err != nil {
		t.Fatal(err)
	}
	publicTLS, err := BuildServerTLS(gatewayTLS, GatewayIdentity, "")
	if err != nil {
		t.Fatal(err)
	}

	// Every listener is started before the policy, which names the resolver URL.
	var (
		gatewayRef  atomic.Pointer[Gateway]
		nodeRef     atomic.Pointer[Node]
		resolverRef atomic.Pointer[resolver.Handler]
	)
	serve := func(h http.Handler, config *tls.Config) *httptest.Server {
		s := httptest.NewUnstartedServer(h)
		s.TLS = config.Clone()
		s.Config.ReadHeaderTimeout, s.Config.ReadTimeout, s.Config.IdleTimeout, s.Config.MaxHeaderBytes = 5*time.Second, 15*time.Second, 30*time.Second, 64<<10
		s.StartTLS()
		t.Cleanup(s.Close)
		return s
	}
	unavailable := func(w http.ResponseWriter) { http.Error(w, "starting", http.StatusServiceUnavailable) }
	var (
		gwMu    sync.Mutex
		gwCount = map[string]int{}
	)
	publicServer := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if g := gatewayRef.Load(); g != nil {
			rec := &statusRecorder{ResponseWriter: w, status: 200}
			g.ServeHTTP(rec, r)
			if rec.status >= 400 {
				route := r.Method + " " + strings.Join(strings.Split(r.URL.Path, "/")[:min(3, len(strings.Split(r.URL.Path, "/")))], "/")
				gwMu.Lock()
				gwCount[fmt.Sprintf("%s -> %d %s", route, rec.status, strings.TrimSpace(rec.body))]++
				gwMu.Unlock()
			}
			return
		}
		unavailable(w)
	}), publicTLS)
	inputServer := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if g := gatewayRef.Load(); g != nil {
			g.OperationInputs().ServeHTTP(w, r)
			return
		}
		unavailable(w)
	}), privateTLS)
	resolverTally := &refusalTally{m: map[string]int{}}
	resolverServer := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h := resolverRef.Load(); h != nil {
			rec := &statusRecorder{ResponseWriter: w, status: 200}
			h.ServeHTTP(rec, r)
			resolverTally.note(fmt.Sprintf("%s %s -> %d", r.Method, r.URL.Path, rec.status), nil)
			return
		}
		unavailable(w)
	}), privateTLS)
	workerServerTLS, err := BuildServerTLS(workerTLS, WorkerIdentity("a", "a1"), GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	workerServer := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n := nodeRef.Load(); n != nil {
			n.ServeHTTP(w, r)
			return
		}
		unavailable(w)
	}), workerServerTLS)

	const issuer, audience, principal = "load-db-api", "kelvo-load", "api"
	policy := sc.Policy
	policy.TenantID = "a"
	policy.Operations = &OperationPolicy{Shards: 128, SlotsPerShard: 8, Retention: time.Hour, ExecutionTimeout: 5 * time.Minute}
	policy.Workers = map[string]int{"a1": sc.Policy.Workers["a1"]}
	policy.Access = &PrincipalPolicy{Revision: 1, Principals: map[string]PrincipalGrant{principal: {Kind: "service", Operations: []operations.Kind{operations.QueryRead},
		DelegatedResolver: &ResolverTrust{Issuer: issuer, Audience: audience, URL: resolverServer.URL + "/internal/kelvo/resolve", PublicKey: base64.StdEncoding.EncodeToString(pub)}}}}
	if err := ValidatePolicy(policy); err != nil {
		t.Fatal("policy:", err)
	}

	natsConfig := func(role string) NATSConfig {
		name := "NATS"
		if role != "" {
			name += "_" + role
		}
		return NATSConfig{URL: get("NATS_URL"), CAFile: get("NATS_CA_FILE"), Username: get(name + "_USER"), PasswordEnv: "KELVO_LOAD_" + name + "_PASSWORD"}
	}
	initialize, err := OpenStore(ctx, natsConfig(""), policy, true)
	if err != nil {
		t.Fatal("broker initialize:", err)
	}
	if _, err = initialize.OpenOperations(ctx, true); err != nil {
		_ = initialize.Close()
		t.Fatal("operation namespace initialize:", err)
	}
	_ = initialize.Close()

	if err := os.Mkdir(filepath.Join(root, "scratch"), 0700); err != nil {
		t.Fatal(err)
	}
	scratch, err := worker.OpenScratchRoot(filepath.Join(root, "scratch"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scratch.Close() })
	sc.Containment.Root, sc.Containment.StateDirectory = cgroupRoot, cgroupState
	manager, err := containment.Open(sc.Containment)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	pool, err := sc.Resources.NewPool()
	if err != nil {
		t.Fatal("resources:", err)
	}
	engine, err := worker.New(catalog.Config{}, policy.Limits)
	if err != nil {
		t.Fatal(err)
	}
	registry := (&MetricsConfig{}).NewRegistry()
	engine.Binary, engine.SandboxPath, engine.ScratchRoot, engine.Metrics, engine.LogFailedReads = get("BINARY"), get("SANDBOX"), scratch, registry, true
	engine.ResourcePool, engine.ResourceOverheadBytes, engine.Containment = pool, sc.Resources.OverheadMB<<20, manager
	engine.ContainmentBudget = sc.Budget

	workerResolver, err := worker.NewConnectionResolver(worker.ConnectionResolverConfig{URL: resolverServer.URL + "/internal/kelvo/resolve", CAFile: workerTLS.CAFile, CertFile: workerTLS.CertFile, KeyFile: workerTLS.KeyFile, Timeout: 15 * time.Second, MaxConcurrent: max(sc.Resolver.MaxConcurrent, 1)}, WorkerIdentity("a", "a1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workerResolver.Close)
	engine, err = engine.WithConnectionResolvers(map[string]*worker.ConnectionResolver{issuer: workerResolver})
	if err != nil {
		t.Fatal(err)
	}

	adapterPath, err := filepath.EvalSymlinks(get("ADAPTER_BINARY"))
	if err != nil {
		t.Fatal(err)
	}
	adapterBytes, err := os.ReadFile(adapterPath)
	if err != nil {
		t.Fatal(err)
	}
	adapterSum := sha256.Sum256(adapterBytes)
	nodeAudit := serviceAuditConfig(t)
	nodeAudit.MaxEntries, nodeAudit.MaxPending = sc.Audit.MaxEntries, sc.Audit.MaxPending
	op := sc.Operations
	warm := sc.WarmPool
	config := NodeConfig{Policy: policy, WorkerID: "a1", NATS: natsConfig("WORKER"), SandboxPath: engine.SandboxPath, ScratchDirectory: filepath.Join(root, "scratch"),
		Resources: &sc.Resources, RuntimeResources: pool, RuntimeMetrics: registry, Audit: nodeAudit, ReadWorkers: sc.ReadWorkers,
		Containment: &ContainmentConfig{Config: sc.Containment, Budget: sc.Budget},
		Operations: &OperationNodeConfig{InputURL: inputServer.URL, TLS: workerTLS, Adapter: worker.OperationProcessConfig{Binary: adapterPath, SHA256: hex.EncodeToString(adapterSum[:])},
			Results: OperationInputConfig{Directory: filepath.Join(root, "results"), MaxEntries: 256, MaxStoredBytes: 1 << 30}, MaxResultBytes: op.MaxResultBytes,
			MaxConcurrent: op.MaxConcurrent, MaxDownloads: op.MaxDownloads, PollInterval: 20 * time.Millisecond, WarmPool: &warm,
			MaxPerTenant: op.MaxPerTenant, MaxPerConnection: op.MaxPerConnection}}
	if err := validateNodeOperations(config); err != nil {
		t.Fatal("node configuration is invalid:", err)
	}
	nodeStore, err := OpenStore(ctx, natsConfig("WORKER"), policy, false)
	if err != nil {
		t.Fatal(err)
	}
	node, err := NewNode(config, nodeStore, engine)
	if err != nil {
		_ = nodeStore.Close()
		t.Fatal("node startup:", err)
	}
	nodeRef.Store(node)
	t.Cleanup(func() { _ = node.Close() })

	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		t.Fatal(err)
	}
	token := hex.EncodeToString(tokenBytes)
	keys, err := yaml.Marshal(gatewayKeyDocument{Version: 2, Revision: 1, Principals: map[string]map[string][]string{"a": {principal: {token}}}})
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(root, "principals.yml")
	if err := os.WriteFile(keyFile, keys, 0600); err != nil {
		t.Fatal(err)
	}
	gatewayAudit := serviceAuditConfig(t)
	gatewayAudit.MaxEntries, gatewayAudit.MaxPending = sc.Audit.MaxEntries, sc.Audit.MaxPending
	gatewayStore, err := OpenStore(ctx, natsConfig("GATEWAY"), policy, false)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(GatewayConfig{WorkerTLS: gatewayTLS, MaxHTTPRequests: sc.Gateway.MaxHTTPRequests, Authentication: &GatewayAuthenticationConfig{KeysFile: keyFile, ReloadInterval: time.Second}, Audit: gatewayAudit,
		Operations: &GatewayOperationConfig{Listen: inputServer.Listener.Addr().String(), TLS: gatewayTLS, Inputs: map[string]OperationInputConfig{"a": {Directory: filepath.Join(root, "inputs"), MaxEntries: 256, MaxStoredBytes: 1 << 30}}},
		Tenants:    []TenantConfig{{Policy: policy, NATS: natsConfig("GATEWAY"), Workers: []Endpoint{{ID: "a1", URL: workerServer.URL}}}}}, map[string]Store{"a": gatewayStore})
	if err != nil {
		_ = gatewayStore.Close()
		t.Fatal("gateway startup:", err)
	}
	gatewayRef.Store(gateway)
	t.Cleanup(func() { _ = gateway.Close() })

	// The credential resolver stands in for db-api. It is the real
	// resolver.Handler, so delegation grants are verified, worker custody is
	// checked against the gateway through the public client, and the
	// authorization is checked before and after the credential load. The stub
	// adds the credential lookup cost and nothing else.
	caPool := x509.NewCertPool()
	caPEM, err := os.ReadFile(gatewayTLS.CAFile)
	if err != nil || !caPool.AppendCertsFromPEM(caPEM) {
		t.Fatal("gateway CA")
	}
	leases, err := client.New(client.Config{URL: publicServer.URL, BearerToken: token, TLSConfig: &tls.Config{RootCAs: caPool}, Timeout: 15 * time.Second, MaxConcurrent: 64, MaxControlConcurrent: 128})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = leases.Close() })
	refusals := &refusalTally{m: map[string]int{}}
	var (
		resolveMu    sync.Mutex
		resolveCalls = map[string]int{}
		resolveTotal atomic.Int64
		authCalls    atomic.Int64
	)
	dsn := func(connection string) (string, bool) {
		name, ok := strings.CutPrefix(connection, "cust-")
		if !ok {
			return "", false
		}
		n, err := strconv.Atoi(name)
		if err != nil || n < 0 || n >= sc.Source.Customers {
			return "", false
		}
		return fmt.Sprintf("postgres://cust%02d:%s@%s:%d/cust%02d?sslmode=disable&connect_timeout=10", n, sc.Source.Password, sc.Source.Host, sc.Source.Port, n), true
	}
	authorization := func(connection string) (resolver.Authorization, error) {
		authCalls.Add(1)
		if _, ok := dsn(connection); !ok {
			return resolver.Authorization{}, resolver.ErrInvalid
		}
		// One revision per connection: a change would revoke its workers.
		sum := sha256.Sum256([]byte("revision\x00" + connection))
		return resolver.Authorization{Revision: hex.EncodeToString(sum[:]), ValidUntil: time.Now().Add(10 * time.Minute)}, nil
	}
	material := func(ctx context.Context, id, connection string) (resolver.Source, map[string]string, error) {
		select {
		case <-time.After(sc.Resolver.Delay):
		case <-ctx.Done():
			return resolver.Source{}, nil, ctx.Err()
		}
		value, ok := dsn(connection)
		if !ok {
			return resolver.Source{}, nil, resolver.ErrInvalid
		}
		resolveTotal.Add(1)
		resolveMu.Lock()
		resolveCalls[connection]++
		resolveMu.Unlock()
		ref, _ := resolver.SecretReference(0, "DSN")
		return resolver.Source{ID: id, Type: "postgres", DSNEnv: ref}, map[string]string{ref: value}, nil
	}
	handler, err := resolver.NewHandler(resolver.Config{
		QueryTrust:     []delegation.Trust{{Issuer: issuer, Audience: audience, ClusterTenant: "a", ServicePrincipal: principal, PublicKey: pub}},
		OperationTrust: []operations.GrantTrust{{Issuer: issuer, Audience: audience, ClusterTenant: "a", ServicePrincipal: principal, PublicKey: pub}},
		Leases:         &countingLeases{inner: leases, errs: refusals}, Timeout: 15 * time.Second, MaxConcurrent: max(sc.Resolver.MaxConcurrent, 1),
		AuthorizeQuery: func(_ context.Context, _ resolver.QueryRequest, c delegation.Claims) (resolver.Authorization, error) {
			a, err := authorization(c.Sources[0].ConnectionID)
			refusals.note("authorize_query", err)
			return a, err
		},
		ResolveQuery: func(ctx context.Context, _ resolver.QueryRequest, c delegation.Claims, a resolver.Authorization) (resolver.QueryMaterial, error) {
			source, secrets, err := material(ctx, c.Sources[0].Alias, c.Sources[0].ConnectionID)
			refusals.note("resolve_query", err)
			return resolver.QueryMaterial{Sources: []resolver.Source{source}, Secrets: secrets, ValidUntil: a.ValidUntil}, err
		},
		AuthorizeOperation: func(_ context.Context, r resolver.OperationRequest, _ operations.GrantClaims) (resolver.Authorization, error) {
			return authorization(r.Operation.Connection.ID)
		},
		ResolveOperation: func(ctx context.Context, r resolver.OperationRequest, _ operations.GrantClaims, a resolver.Authorization) (resolver.OperationMaterial, error) {
			source, secrets, err := material(ctx, "source_1", r.Operation.Connection.ID)
			return resolver.OperationMaterial{Source: source, Secrets: secrets, ValidUntil: a.ValidUntil}, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolverRef.Store(handler)

	for name, a := range map[string]*ServiceAudit{"node": node.audit, "gateway": gateway.audit} {
		_, err := a.begin(ctx, "a", &JobAuthority{PrincipalID: principal, PrincipalKind: "service"}, audit.QueryExecution)
		t.Logf("AUDITPROBE %s: %v", name, err)
	}
	// One read through the whole path before the load starts. A failure is
	// reported with the job's own error, which the load generator cannot see.
	func() {
		probe := query.Request{Mode: "native", ConnectionID: "source_1", SQL: "SELECT 1 AS one"}
		digest, err := delegation.QueryDigest(probe)
		if err != nil {
			t.Log("probe digest:", err)
			return
		}
		now := time.Now().Unix()
		probe.Delegation, err = delegation.Sign(delegation.Claims{Version: 1, Issuer: issuer, Audience: audience, ClusterTenant: "a", ServicePrincipal: principal, AppTeam: "team-00",
			Subject: delegation.Subject{Kind: "user", ID: "probe"}, ID: strings.Repeat("a", 48), IssuedAt: now - 1, ExpiresAt: now + 120, QuerySHA256: digest,
			Sources: []delegation.Source{{Alias: "source_1", ConnectionID: "cust-00", Database: "cust00", Schema: "public"}}}, key)
		if err != nil {
			t.Log("probe sign:", err)
			return
		}
		auth := client.Authority{Delegation: probe.Delegation}
		handle, err := leases.Submit(ctx, probe, auth)
		if err != nil {
			t.Log("probe submit:", err)
			return
		}
		_, resultErr := leases.Results(ctx, handle.ID, auth, discardSink{})
		status, err := leases.Status(ctx, handle.ID, auth)
		e, _ := json.Marshal(status.Error)
		t.Logf("PROBE results=%v state=%s error=%s status_err=%v", resultErr, status.State, e, err)
		return
	}()

	seed := base64.StdEncoding.EncodeToString(key.Seed())
	info, _ := json.Marshal(map[string]any{"url": publicServer.URL, "token": token, "ca_file": gatewayTLS.CAFile, "signing_seed": seed, "issuer": issuer, "audience": audience,
		"cluster_tenant": "a", "service_principal": principal, "customers": sc.Source.Customers})
	infoPath := filepath.Join(out, "node.json")
	if err := os.WriteFile(infoPath+".tmp", info, 0600); err != nil || os.Rename(infoPath+".tmp", infoPath) != nil {
		t.Fatal("publish node.json", err)
	}

	// Sample the delegated cgroup until the load generator is done.
	began := time.Now()
	var samples []loadSample
	var peakGroups int
	interval := sc.Sample
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	stop := filepath.Join(out, "stop")
sampling:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("load run exceeded its deadline")
		case <-ticker.C:
			entries, _ := os.ReadDir(cgroupRoot)
			groups := 0
			for _, e := range entries {
				if e.IsDir() {
					groups++
				}
			}
			peakGroups = max(peakGroups, groups)
			samples = append(samples, loadSample{T: time.Since(began).Seconds(), CPUUsec: readUint(filepath.Join(unitCgroup, "cpu.stat"), "usage_usec"),
				MemCurrent: readUint(filepath.Join(unitCgroup, "memory.current"), ""), JobGroups: groups,
				RSSNode: rssOf(filepath.Join(unitCgroup, "supervisor"), false), RSSWorkers: rssOf(cgroupRoot, true)})
			if _, err := os.Stat(stop); err == nil {
				break sampling
			}
		}
	}

	pm := engine.WarmPool.Metrics()
	var text strings.Builder
	rec := httptest.NewRecorder()
	registry.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text.Write(rec.Body.Bytes())
	resolveMu.Lock()
	perConnection := map[string]int{}
	for k, v := range resolveCalls {
		perConnection[k] = v
	}
	resolveMu.Unlock()
	report := map[string]any{
		"duration_s":           time.Since(began).Seconds(),
		"cpu_usage_usec":       readUint(filepath.Join(unitCgroup, "cpu.stat"), "usage_usec"),
		"cpu_throttled_usec":   readUint(filepath.Join(unitCgroup, "cpu.stat"), "throttled_usec"),
		"nr_throttled":         readUint(filepath.Join(unitCgroup, "cpu.stat"), "nr_throttled"),
		"memory_peak_bytes":    readUint(filepath.Join(unitCgroup, "memory.peak"), ""),
		"memory_events_oom":    readUint(filepath.Join(unitCgroup, "memory.events"), "oom_kill"),
		"peak_job_cgroups":     peakGroups,
		"samples":              samples,
		"resolver_http":        resolverTally.copy(),
		"resolver_hook_errors": refusals.copy(),
		"gateway_refusals":     gwCountCopy(&gwMu, gwCount),
		"resolver_calls":       resolveTotal.Load(),
		"resolver_authorize":   authCalls.Load(),
		"resolver_per_conn":    perConnection,
		"warm_pool": map[string]int64{"hits": pm.Hits.Load(), "misses": pm.Misses.Load(), "spawns": pm.Spawns.Load(), "queue_wait_ms": pm.QueueWaitMS.Load(),
			"evictions_ttl": pm.EvictionsTTL.Load(), "evictions_revision": pm.EvictionsRev.Load(), "evictions_error": pm.EvictionsErr.Load(), "evictions_drain": pm.EvictionsDrain.Load(),
			"evictions_auth": pm.EvictionsAuth.Load(), "authorizations": pm.Authorizations.Load(), "fallbacks": pm.Fallbacks.Load()},
		"warm_pool_config": engine.WarmPool.Config(),
		"metrics_text":     text.String(),
	}
	raw, err = json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "node-report.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func gwCountCopy(mu *sync.Mutex, m map[string]int) map[string]int {
	mu.Lock()
	defer mu.Unlock()
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// refusalTally counts outcomes by label and error text, for the report.
type refusalTally struct {
	mu sync.Mutex
	m  map[string]int
}

func (r *refusalTally) note(label string, err error) {
	if err != nil {
		label += ": " + err.Error()
	}
	r.mu.Lock()
	r.m[label]++
	r.mu.Unlock()
}
func (r *refusalTally) copy() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.m))
	for k, v := range r.m {
		out[k] = v
	}
	return out
}

// countingLeases reports why a custody check failed; the handler hides it.
type countingLeases struct {
	inner *client.Client
	errs  *refusalTally
}

func (c *countingLeases) ValidateConnectionLease(ctx context.Context, id, grant string, b resolver.Binding) (time.Time, error) {
	until, err := c.inner.ValidateConnectionLease(ctx, id, grant, b)
	c.errs.note("lease_query", err)
	return until, err
}
func (c *countingLeases) ValidateOperationLease(ctx context.Context, id, grant string, b resolver.Binding) (time.Time, error) {
	until, err := c.inner.ValidateOperationLease(ctx, id, grant, b)
	c.errs.note("lease_operation", err)
	return until, err
}
