//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"go.yaml.in/yaml/v3"
)

// This gate owns a fresh broker namespace and scoped disposable source tables.
// It never substitutes an in-memory store, synthetic executor or source driver.
func TestOperationClusterActualNodeLifecycle(t *testing.T) {
	required := []string{"BINARY", "ADAPTER_BINARY", "ADAPTER_SHA256", "SANDBOX", "NATS_URL", "NATS_CA_FILE", "NATS_USER", "NATS_PASSWORD", "NATS_WORKER_USER", "NATS_WORKER_PASSWORD", "NATS_GATEWAY_USER", "NATS_GATEWAY_PASSWORD", "POSTGRES_DSN", "MYSQL_DSN", "DATABASE", "SOURCE_CA_FILE"}
	for _, name := range required {
		if os.Getenv("KELVO_TEST_OPERATION_"+name) == "" {
			t.Skip("explicit disposable operation cluster fixture required")
		}
	}
	cgroupRoot, cgroupState := os.Getenv("KELVO_TEST_CGROUP_ROOT"), os.Getenv("KELVO_TEST_CGROUP_STATE")
	if cgroupRoot == "" || cgroupState == "" {
		t.Skip("explicit delegated cgroup fixture required")
	}
	get := func(name string) string { return os.Getenv("KELVO_TEST_OPERATION_" + name) }
	applicationMode := os.Getenv("KELVO_TEST_OPERATION_APPLICATION_ARTIFACTS") != ""
	if applicationMode && os.Getenv("KELVO_TEST_OPERATION_API_BINARY") != "" {
		t.Fatal("application and API fixture modes are mutually exclusive")
	}
	timeout := 240 * time.Second
	if applicationMode {
		timeout = 360 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	root := t.TempDir()
	caPEM, err := os.ReadFile(get("SOURCE_CA_FILE"))
	if err != nil {
		t.Fatal("fixture CA unavailable")
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	gatewayTLS, workerTLS := exportIntegrationTLS(t)
	privateTLS, err := OperationInputTLS(gatewayTLS)
	if err != nil {
		t.Fatal(err)
	}
	var gatewayRef atomic.Pointer[Gateway]
	inputServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g := gatewayRef.Load()
		if g == nil {
			http.Error(w, "starting", 503)
			return
		}
		g.OperationInputs().ServeHTTP(w, r)
	}))
	inputServer.TLS = privateTLS.Clone()
	inputServer.StartTLS()
	t.Cleanup(inputServer.Close)
	var resolutions atomic.Int32
	resolverServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/kelvo/resolve-operation" {
			http.NotFound(w, r)
			return
		}
		if _, ok := operationWorkerPeer(r.TLS, WorkerIdentity("a", "a1"), time.Now()); !ok {
			http.Error(w, "worker required", 403)
			return
		}
		var input struct {
			Grant       string             `json:"grant"`
			Operation   operations.Request `json:"operation"`
			OperationID string             `json:"operation_id"`
			WorkerID    string             `json:"worker_id"`
			Owner       string             `json:"owner"`
			Claim       string             `json:"claim"`
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, operations.MaxRequestBytes+operations.MaxGrantBytes+4096))
		if err != nil || operations.DecodeStrict(raw, &input, operations.MaxRequestBytes+operations.MaxGrantBytes+4096) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		claims, err := operations.VerifyGrant(input.Grant, operations.GrantTrust{Issuer: "fixture-gateway", Audience: "operations", ClusterTenant: "a", ServicePrincipal: "api", PublicKey: pub}, input.Operation, time.Now())
		g := gatewayRef.Load()
		if err != nil || claims.AppTeam != "customer-a" || g == nil {
			http.Error(w, "denied", 403)
			return
		}
		state := g.operations["a"]
		record, err := state.store.Get(r.Context(), operationScope(claims), input.OperationID)
		binding := ledger.Binding{WorkerID: input.WorkerID, Owner: input.Owner, Claim: input.Claim}
		if err != nil || record.Record.State != ledger.Running || record.Record.AuthoritySHA256 != operations.GrantDigest(input.Grant) {
			http.Error(w, "unavailable", 409)
			return
		}
		until, err := currentOperationLease(r.Context(), g.tenants["a"].store, state.store, record.Record, binding)
		if err != nil {
			http.Error(w, "unavailable", 409)
			return
		}
		engine := strings.TrimSuffix(input.Operation.Connection.ID, "-fixture")
		if (engine != "postgres" && engine != "mysql") || input.Operation.Connection.Database != get("DATABASE") {
			http.Error(w, "denied", 403)
			return
		}
		dsn := get(strings.ToUpper(engine) + "_DSN")
		resolutions.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "grant_sha256": operations.GrantDigest(input.Grant), "request_sha256": record.Record.RequestSHA256, "source_revision": strings.Repeat("a", 64), "valid_until": min(until.Unix(), time.Now().Unix()+5), "source": catalog.Source{ID: "source_1", Type: engine, DSNEnv: "KELVO_SOURCE_REQUEST_0_DSN", Options: map[string]string{"tls_ca_pem": string(caPEM)}}, "secrets": map[string]string{"KELVO_SOURCE_REQUEST_0_DSN": dsn}})
	}))
	resolverURL := ""
	if applicationMode || os.Getenv("KELVO_TEST_OPERATION_API_BINARY") != "" {
		// NewUnstartedServer already owns a listener. External authority mode
		// must close it even though its synthetic handler is never started.
		if err := resolverServer.Listener.Close(); err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		resolverURL = "https://" + listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		resolverServer.TLS = privateTLS.Clone()
		resolverServer.StartTLS()
		t.Cleanup(resolverServer.Close)
		resolverURL = resolverServer.URL
	}
	policy := testPolicy()
	policy.Limits.MemoryMB = 64
	policy.Limits.MaxTempMB = 16
	policy.Limits.MaxRows = 1000
	policy.Limits.MaxBytes = 1 << 20
	policy.Limits.Timeout = 15 * time.Second
	policy.Operations = &OperationPolicy{Shards: 128, SlotsPerShard: 8, Retention: time.Hour, ExecutionTimeout: 30 * time.Second}
	policy.Access = &PrincipalPolicy{Revision: 1, Principals: map[string]PrincipalGrant{"api": {Kind: "service", Operations: []operations.Kind{operations.ConnectionTest, operations.QueryRead, operations.MetadataInspect, operations.StatementExecute, operations.IngestionInstall, operations.IngestionState, operations.IngestionCommit, operations.WatchInstall, operations.WatchRead, operations.WatchAck, operations.WatchRemove, operations.MigrationStatus, operations.MigrationApply}, DelegatedResolver: &ResolverTrust{Issuer: "fixture-gateway", Audience: "operations", URL: resolverURL + "/internal/kelvo/resolve", PublicKey: base64.StdEncoding.EncodeToString(pub)}}}}
	natsConfig := func(role string) NATSConfig {
		prefix := "NATS"
		if role != "" {
			prefix += "_" + role
		}
		return NATSConfig{URL: get("NATS_URL"), CAFile: get("NATS_CA_FILE"), Username: get(prefix + "_USER"), PasswordEnv: "KELVO_TEST_OPERATION_" + prefix + "_PASSWORD"}
	}
	initialize, err := OpenStore(ctx, natsConfig(""), policy, true)
	if err != nil {
		t.Fatal("broker initialize", err)
	}
	if _, err = initialize.OpenOperations(ctx, true); err != nil {
		_ = initialize.Close()
		t.Fatal("operation namespace initialize", err)
	}
	_ = initialize.Close()
	if err := os.Mkdir(filepath.Join(root, "scratch"), 0700); err != nil {
		t.Fatal(err)
	}
	scratch, err := worker.OpenScratchRoot(filepath.Join(root, "scratch"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scratch.Close(); err != nil {
			t.Error(err)
		}
	})
	manager, err := containment.Open(containment.Config{Root: cgroupRoot, StateDirectory: cgroupState})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	resources := &ResourceConfig{MaxConcurrent: 1, MemoryMB: 512, BaselineMB: 64, OverheadMB: 160, ScratchMB: 32}
	pool, err := resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := worker.New(catalog.Config{}, policy.Limits)
	if err != nil {
		t.Fatal(err)
	}
	engine.Binary, engine.SandboxPath, engine.ScratchRoot = get("BINARY"), get("SANDBOX"), scratch
	engine.ResourcePool, engine.ResourceOverheadBytes, engine.Containment = pool, 160<<20, manager
	engine.ContainmentBudget = containment.Budget{NativeOverheadMB: 64, ParentOverheadMB: 32, MaxProcesses: 64}
	resolver, err := worker.NewConnectionResolver(worker.ConnectionResolverConfig{URL: resolverURL + "/internal/kelvo/resolve", CAFile: workerTLS.CAFile, CertFile: workerTLS.CertFile, KeyFile: workerTLS.KeyFile, Timeout: 5 * time.Second, MaxConcurrent: 1}, WorkerIdentity("a", "a1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(resolver.Close)
	engine, err = engine.WithConnectionResolvers(map[string]*worker.ConnectionResolver{"fixture-gateway": resolver})
	if err != nil {
		t.Fatal(err)
	}
	nodeAudit := serviceAuditConfig(t)
	nodeAudit.MaxEntries = 512
	nodeAudit.MaxPending = 16
	config := NodeConfig{Policy: policy, WorkerID: "a1", NATS: natsConfig("WORKER"), SandboxPath: engine.SandboxPath, ScratchDirectory: filepath.Join(root, "scratch"), Resources: resources, RuntimeResources: pool, Audit: nodeAudit,
		Containment: &ContainmentConfig{Config: containment.Config{Root: cgroupRoot, StateDirectory: cgroupState}, Budget: engine.ContainmentBudget}, Operations: &OperationNodeConfig{InputURL: inputServer.URL, TLS: workerTLS, Adapter: worker.OperationProcessConfig{Binary: get("ADAPTER_BINARY"), SHA256: get("ADAPTER_SHA256")}, Results: OperationInputConfig{Directory: filepath.Join(root, "results"), MaxEntries: 256, MaxStoredBytes: 1 << 30}, MaxResultBytes: 1 << 20, MaxConcurrent: 1, MaxDownloads: 1, PollInterval: 20 * time.Millisecond}}
	var node atomic.Pointer[Node]
	startWorker := func() {
		store, err := OpenStore(ctx, natsConfig("WORKER"), policy, false)
		if err != nil {
			t.Fatal(err)
		}
		current, err := NewNode(config, store, engine)
		if err != nil {
			_ = store.Close()
			t.Fatal("real worker startup", err)
		}
		node.Store(current)
	}
	t.Cleanup(func() {
		if current := node.Swap(nil); current != nil {
			if err := current.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	workerServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := node.Load()
		if current == nil {
			http.Error(w, "restarting", 503)
			return
		}
		current.ServeHTTP(w, r)
	}))
	workerServer.TLS, err = BuildServerTLS(workerTLS, WorkerIdentity("a", "a1"), GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	workerServer.StartTLS()
	t.Cleanup(workerServer.Close)
	keyFile := filepath.Join(root, "principals.yml")
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		t.Fatal(err)
	}
	token := hex.EncodeToString(tokenBytes)
	keys, err := yaml.Marshal(gatewayKeyDocument{Version: 2, Revision: 1, Principals: map[string]map[string][]string{"a": {"api": {token}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keys, 0600); err != nil {
		t.Fatal(err)
	}
	gatewayAudit := serviceAuditConfig(t)
	gatewayAudit.MaxEntries = 512
	gatewayAudit.MaxPending = 16
	gatewayStore, err := OpenStore(ctx, natsConfig("GATEWAY"), policy, false)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(GatewayConfig{WorkerTLS: gatewayTLS, MaxHTTPRequests: 8, Authentication: &GatewayAuthenticationConfig{KeysFile: keyFile, ReloadInterval: time.Second}, Audit: gatewayAudit, Operations: &GatewayOperationConfig{Listen: inputServer.Listener.Addr().String(), TLS: gatewayTLS, Inputs: map[string]OperationInputConfig{"a": {Directory: filepath.Join(root, "inputs"), MaxEntries: 256, MaxStoredBytes: 1 << 30}}}, Tenants: []TenantConfig{{Policy: policy, NATS: natsConfig("GATEWAY"), Workers: []Endpoint{{ID: "a1", URL: workerServer.URL}}}}}, map[string]Store{"a": gatewayStore})
	if err != nil {
		_ = gatewayStore.Close()
		t.Fatal("real gateway startup", err)
	}
	gatewayRef.Store(gateway)
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Error(err)
		}
	})
	startWorker()
	var loseSubmit atomic.Bool
	public := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/operations" && loseSubmit.CompareAndSwap(true, false) {
			gateway.ServeHTTP(httptest.NewRecorder(), r)
			panic(http.ErrAbortHandler)
		}
		gateway.ServeHTTP(w, r)
	}))
	t.Cleanup(public.Close)
	if os.Getenv("KELVO_TEST_OPERATION_API_BINARY") != "" {
		t.Run("api-on-demand-cross-service", func(t *testing.T) {
			operationAPICrossService(t, public, resolverURL, gatewayTLS, key, token)
		})
		return
	}
	if applicationMode {
		t.Run("standalone-application", func(t *testing.T) {
			operationApplicationCrossService(t, public, resolverURL, gatewayTLS, workerTLS, key, token)
		})
		return
	}
	client := public.Client()
	client.Timeout = 20 * time.Second
	// The combined scenarios retain every record; reserve bounded shard headroom.
	parentTest, activeTest := t, t
	var sequence int
	sign := func(request operations.Request, team string) string {
		sequence++
		digest, err := operations.Digest(request)
		if err != nil {
			activeTest.Fatal(err)
		}
		now := time.Now()
		claims := operations.GrantClaims{Version: operations.GrantVersion, Issuer: "fixture-gateway", Audience: "operations", ClusterTenant: "a", ServicePrincipal: "api", AppTeam: team, Subject: operations.Subject{Kind: "api_key", ID: "fixture-key"}, ID: "grant-" + strconv.Itoa(sequence), IssuedAt: now.Unix(), ExpiresAt: now.Add(120 * time.Second).Unix(), ConnectionID: request.Connection.ID, Operation: request.Kind, RequestSHA256: digest, Authorization: operations.Authorization{Kind: "api_key"}}
		grant, err := operations.SignGrant(claims, key)
		if err != nil {
			activeTest.Fatal(err)
		}
		return grant
	}
	call := func(method, path, grant string, request *operations.Request) (int, []byte, error) {
		var body io.Reader
		if request != nil {
			raw, err := operations.Encode(*request)
			if err != nil {
				activeTest.Fatal(err)
			}
			body = bytes.NewReader(raw)
		}
		r, err := http.NewRequestWithContext(ctx, method, public.URL+path, body)
		if err != nil {
			activeTest.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set(operationGrantHeader, grant)
		r.Header.Set("Content-Type", "application/json")
		response, err := client.Do(r)
		if err != nil {
			return 0, nil, err
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		return response.StatusCode, raw, err
	}
	lookup := func(request operations.Request, grant string) operations.Response {
		digest, err := operations.Digest(request)
		if err != nil {
			activeTest.Fatal(err)
		}
		body, err := json.Marshal(operations.LookupRequest{Version: operations.Version, IdempotencyKey: request.IdempotencyKey, RequestSHA256: digest})
		if err != nil {
			activeTest.Fatal(err)
		}
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, public.URL+"/v1/operations/lookup", bytes.NewReader(body))
		if err != nil {
			activeTest.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set(operationGrantHeader, grant)
		r.Header.Set("Content-Type", "application/json")
		response, err := client.Do(r)
		if err != nil {
			activeTest.Fatal("lost submission lookup failed", err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, operations.MaxReceiptBytes+4096))
		var value operations.Response
		if err != nil || response.StatusCode != http.StatusOK || operations.DecodeStrict(raw, &value, operations.MaxReceiptBytes+4096) != nil || value.Validate() != nil || value.RequestSHA256 != digest {
			activeTest.Fatal("lost submission lookup returned invalid custody", response.StatusCode, err)
		}
		return value
	}
	submit := func(request operations.Request, grant string) operations.Response {
		status, raw, err := call(http.MethodPost, "/v1/operations", grant, &request)
		if err != nil || status != http.StatusAccepted {
			activeTest.Fatalf("submit kind=%s status=%d code=%s error=%v", request.Kind, status, operationFixtureErrorCode(raw), err)
		}
		var response operations.Response
		if operations.DecodeStrict(raw, &response, operations.MaxReceiptBytes+4096) != nil || response.Validate() != nil {
			activeTest.Fatal("invalid submit response")
		}
		return response
	}
	await := func(id, grant string, request operations.Request) operations.Response {
		deadline := time.Now().Add(25 * time.Second)
		for time.Now().Before(deadline) {
			status, raw, err := call(http.MethodGet, "/v1/operations/"+id, grant, nil)
			if err != nil || status != 200 {
				activeTest.Fatalf("status=%d error=%v", status, err)
			}
			var response operations.Response
			if operations.DecodeStrict(raw, &response, operations.MaxReceiptBytes+4096) != nil || response.Validate() != nil {
				activeTest.Fatal("invalid status response")
			}
			if response.Receipt != nil {
				if operations.ValidateStatementReceipt(request, *response.Receipt) != nil {
					activeTest.Fatal("unbound source receipt")
				}
				return response
			}
			time.Sleep(20 * time.Millisecond)
		}
		activeTest.Fatal("operation completion deadline")
		return operations.Response{}
	}
	readValue := func(raw []byte) int64 {
		reader, err := ipc.NewReader(bytes.NewReader(raw))
		if err != nil {
			activeTest.Fatal("invalid retained Arrow", err)
		}
		defer reader.Release()
		if !reader.Next() {
			activeTest.Fatal("missing source row")
		}
		column, ok := reader.RecordBatch().Column(0).(*array.Int64)
		if !ok || column.Len() != 1 {
			activeTest.Fatal("unexpected source value type")
		}
		value := column.Value(0)
		if reader.Next() || reader.Err() != nil {
			activeTest.Fatal("unexpected source rows")
		}
		return value
	}
	var retainedID, retainedGrant string
	var retainedRaw []byte
	var retainedRequest operations.Request
	for _, kind := range []string{"postgres", "mysql"} {
		t.Run(kind, func(t *testing.T) {
			activeTest = t
			defer func() { activeTest = parentTest }()
			connection := operations.ConnectionRef{ID: kind + "-fixture", Database: get("DATABASE")}
			if kind == "postgres" {
				connection.Schema = "public"
			}
			request := operations.Request{Version: operations.Version, Kind: operations.ConnectionTest, Connection: connection}
			grant := sign(request, "customer-a")
			result := await(submit(request, grant).ID, grant, request)
			if result.State != string(operations.Completed) {
				t.Fatal("connection failed", result.State)
			}
			request = operations.Request{Version: operations.Version, Kind: operations.StatementExecute, Connection: connection, IdempotencyKey: kind + "-increment", Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: "UPDATE kelvo_operation_fixture SET marker = marker + 1 WHERE id = 1", Transaction: operations.TransactionRequired}}}
			grant = sign(request, "customer-a")
			loseSubmit.Store(true)
			if _, _, err := call(http.MethodPost, "/v1/operations", grant, &request); err == nil {
				t.Fatal("submit response was not lost")
			}
			first, second := lookup(request, grant), lookup(request, grant)
			if first.ID != second.ID || submit(request, grant).ID != first.ID {
				t.Fatal("lost response recovery or duplicate operation identity changed")
			}
			result = await(first.ID, grant, request)
			if result.Receipt.Effect != operations.EffectCommitted {
				t.Fatal("write did not commit", result.State)
			}
			request = operations.Request{Version: operations.Version, Kind: operations.QueryRead, Connection: connection, Spec: operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT marker FROM kelvo_operation_fixture WHERE id = 1"}}}
			grant = sign(request, "customer-a")
			read := await(submit(request, grant).ID, grant, request)
			if read.Receipt.Result == nil {
				t.Fatal("read has no retained result", read.State)
			}
			before := resolutions.Load()
			status, raw, err := call(http.MethodGet, "/v1/operations/"+read.ID+"/results", grant, nil)
			if err != nil || status != 200 || readValue(raw) != 1 {
				t.Fatal("lost response replayed source write", status, err)
			}
			status, again, err := call(http.MethodGet, "/v1/operations/"+read.ID+"/results", grant, nil)
			if err != nil || status != 200 || !bytes.Equal(raw, again) || resolutions.Load() != before {
				t.Fatal("repeat result re-executed source")
			}
			hash := sha256.Sum256(raw)
			if hex.EncodeToString(hash[:]) != read.Receipt.Result.SHA256 {
				t.Fatal("retained result digest differs")
			}
			foreign := sign(request, "customer-b")
			for _, suffix := range []string{"", "/results"} {
				status, _, err := call(http.MethodGet, "/v1/operations/"+read.ID+suffix, foreign, nil)
				if err != nil || status != 404 {
					t.Fatal("foreign team accessed operation", status, err)
				}
			}
			retainedID, retainedGrant, retainedRaw, retainedRequest = read.ID, grant, raw, request
			request = operations.Request{Version: operations.Version, Kind: operations.MetadataInspect, Connection: connection, Spec: operations.Spec{Metadata: &operations.MetadataSpec{Object: "tables", Limit: 100}}}
			grant = sign(request, "customer-a")
			metadata := await(submit(request, grant).ID, grant, request)
			if metadata.State != string(operations.Completed) || metadata.Receipt.Result == nil || metadata.Receipt.Result.Rows < 1 {
				t.Fatal("live catalog inspection failed", metadata.State)
			}
			request = operations.Request{Version: operations.Version, Kind: operations.StatementExecute, Connection: connection, IdempotencyKey: kind + "-batch", Spec: operations.Spec{Statement: &operations.StatementSpec{Transaction: operations.TransactionRequired, Batch: &operations.BatchSpec{Statements: []operations.BoundStatement{{SQL: "UPDATE kelvo_operation_fixture SET marker = marker + 2 WHERE id = 1"}, {SQL: "UPDATE kelvo_operation_fixture SET marker = marker + 3 WHERE id = 1"}}}}}}
			grant = sign(request, "customer-a")
			batch := await(submit(request, grant).ID, grant, request)
			if batch.State != string(operations.Completed) || len(batch.Receipt.Steps) != 2 {
				t.Fatal("transactional batch failed", batch.State)
			}
			request.IdempotencyKey = kind + "-rollback"
			request.Spec.Statement.Batch = &operations.BatchSpec{Statements: []operations.BoundStatement{{SQL: "UPDATE kelvo_operation_fixture SET marker = marker + 100 WHERE id = 1"}, {SQL: "UPDATE kelvo_missing_fixture SET marker = 1"}}}
			grant = sign(request, "customer-a")
			rolledBack := await(submit(request, grant).ID, grant, request)
			if rolledBack.State != string(operations.Failed) || rolledBack.Receipt.Effect != operations.EffectNone || len(rolledBack.Receipt.Steps) != 2 {
				t.Fatal("rollback receipt did not preserve source effects", rolledBack.State)
			}
			request = operations.Request{Version: operations.Version, Kind: operations.QueryRead, Connection: connection, Spec: operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT marker FROM kelvo_operation_fixture WHERE id = 1"}}}
			grant = sign(request, "customer-a")
			verified := await(submit(request, grant).ID, grant, request)
			status, raw, err = call(http.MethodGet, "/v1/operations/"+verified.ID+"/results", grant, nil)
			if err != nil || status != 200 || readValue(raw) != 6 {
				t.Fatal("batch commit/rollback changed source unexpectedly", status, err)
			}
		})
	}
	if t.Failed() {
		return
	}
	t.Run("postgres-ingestion", func(t *testing.T) {
		activeTest = t
		defer func() { activeTest = parentTest }()
		runOperationIngestionLive(t, operationIngestionLive{
			ctx: ctx, client: client, url: public.URL, token: token, key: key, database: get("DATABASE"),
			sign: sign, submit: submit, await: await, call: call,
		})
	})
	if t.Failed() {
		return
	}
	for _, engine := range []string{"postgres", "mysql"} {
		t.Run(engine+"-watch", func(t *testing.T) {
			activeTest = t
			defer func() { activeTest = parentTest }()
			runOperationWatchLive(t, operationIngestionLive{
				ctx: ctx, client: client, url: public.URL, token: token, key: key, database: get("DATABASE"),
				sign: sign, submit: submit, await: await, call: call,
			}, engine)
		})
		if t.Failed() {
			return
		}
	}
	for _, migrationEngine := range []string{"postgres", "mysql"} {
		t.Run(migrationEngine+"-migrations", func(t *testing.T) {
			activeTest = t
			defer func() { activeTest = parentTest }()
			runOperationMigrationLive(t, operationIngestionLive{
				ctx: ctx, client: client, url: public.URL, token: token, key: key, database: get("DATABASE"),
				sign: sign, submit: submit, await: await, call: call,
			}, migrationEngine)
		})
		if t.Failed() {
			return
		}
	}
	t.Run("retained-result-survives-worker-restart", func(t *testing.T) {
		activeTest = t
		defer func() { activeTest = parentTest }()
		before := resolutions.Load()
		old := node.Swap(nil)
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(policy.LeaseDuration + 100*time.Millisecond)
		if _, err := gatewayStore.WorkerLease(ctx, config.WorkerID, old.owner); !errors.Is(err, ErrConflict) {
			t.Fatal("prior worker custody has not expired", err)
		}
		startWorker()
		result := await(retainedID, retainedGrant, retainedRequest)
		status, raw, err := call(http.MethodGet, "/v1/operations/"+retainedID+"/results", retainedGrant, nil)
		if err != nil || status != 200 || result.State != string(operations.Completed) || !bytes.Equal(raw, retainedRaw) || resolutions.Load() != before {
			t.Fatal("restart lost result or replayed source", status, err)
		}
	})
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("live operation leaked resource custody")
	}
	if reclaimed, err := scratch.Reclaim(); err != nil || reclaimed != 0 {
		t.Fatal("live operation leaked scratch", err)
	}
}

func operationFixtureErrorCode(raw []byte) string {
	var value struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
		Code string `json:"code"`
	}
	if len(raw) > 4096 || json.Unmarshal(raw, &value) != nil {
		return "invalid_error"
	}
	code := value.Error.Code
	if code == "" {
		code = value.Code
	}
	switch code {
	case "RESOURCE_EXHAUSTED", "UNAVAILABLE", "NOT_FOUND", "CONFLICT", "PERMISSION_DENIED", "INVALID_ARGUMENT":
		return code
	default:
		return "unknown_error"
	}
}
