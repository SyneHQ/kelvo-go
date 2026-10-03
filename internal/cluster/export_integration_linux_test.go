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
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"go.yaml.in/yaml/v3"
)

// This gate needs its own empty TLS broker. It provisions a tenant policy and
// must not share an account with the separate store-provisioning test.
func TestExportClusterActualWorkerLifecycle(t *testing.T) {
	binary, launcher := os.Getenv("KELVO_TEST_EXPORT_BINARY"), os.Getenv("KELVO_TEST_EXPORT_SANDBOX")
	brokerURL := os.Getenv("KELVO_TEST_EXPORT_E2E_NATS_URL")
	if binary == "" || launcher == "" || brokerURL == "" {
		t.Skip("set export binary/sandbox and dedicated KELVO_TEST_EXPORT_E2E_NATS_* fixture")
	}
	natsConfig := NATSConfig{URL: brokerURL, CAFile: os.Getenv("KELVO_TEST_EXPORT_E2E_NATS_CA_FILE"),
		Username: os.Getenv("KELVO_TEST_EXPORT_E2E_NATS_USER"), PasswordEnv: "KELVO_TEST_EXPORT_E2E_NATS_PASSWORD"}
	runtimeNATS := func(role string) NATSConfig {
		config := natsConfig
		prefix := "KELVO_TEST_EXPORT_E2E_NATS_" + role
		config.Username, config.PasswordEnv = os.Getenv(prefix+"_USER"), prefix+"_PASSWORD"
		if config.Username == "" || len(os.Getenv(config.PasswordEnv)) < 32 {
			t.Fatalf("missing separate %s broker identity", role)
		}
		return config
	}
	workerNATS, gatewayNATS := runtimeNATS("WORKER"), runtimeNATS("GATEWAY")
	catalogue, policy := snapshotPrincipalFixture(t)
	config := runtimeExportConfigFixture(t)
	policy.Exports = config.Policy.Exports
	policy.Exports.Limits.Compression = "lz4_frame"
	config.Policy, config.WorkerID, config.NATS, config.SandboxPath = policy, "a1", workerNATS, launcher
	if err := os.Mkdir(config.ScratchDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	scratch, err := worker.OpenScratchRoot(config.ScratchDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scratch.Close(); err != nil {
			t.Error("scratch cleanup", err)
		}
	})
	engine, err := worker.New(catalogue, policy.Limits)
	if err != nil {
		t.Fatal(err)
	}
	engine.Binary, engine.SandboxPath, engine.ScratchRoot = binary, launcher, scratch
	engine.ResourcePool, err = config.Resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	engine.ResourceOverheadBytes = config.Resources.OverheadMB << 20
	config.RuntimeResources = engine.ResourcePool
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	initialize, err := OpenStore(ctx, natsConfig, policy, true)
	if err != nil {
		t.Fatal("initialize query namespace", err)
	}
	if _, err = OpenExportStore(ctx, initialize, true); err != nil {
		_ = initialize.Close()
		t.Fatal("initialize export namespace", err)
	}
	_ = initialize.Close()
	var node atomic.Pointer[Node]
	startWorker := func(t *testing.T) {
		store, err := OpenStore(ctx, workerNATS, policy, false)
		if err != nil {
			t.Fatal(err)
		}
		current, err := NewNode(config, store, engine)
		if err != nil {
			_ = store.Close()
			t.Fatal("start real worker", err)
		}
		node.Store(current)
	}
	startWorker(t)
	t.Cleanup(func() {
		if current := node.Swap(nil); current != nil {
			if err := current.Close(); err != nil {
				t.Error("worker cleanup", err)
			}
		}
	})
	gatewayTLS, workerTLS := exportIntegrationTLS(t)
	serverTLS, err := BuildServerTLS(workerTLS, WorkerIdentity("a", "a1"), GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int32
	var loseCompletion atomic.Bool
	var holdExecution atomic.Bool
	executionEntered, executionRelease := make(chan struct{}, 1), make(chan struct{})
	var releaseOnce sync.Once
	releaseExecution := func() { releaseOnce.Do(func() { close(executionRelease) }) }
	workerServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := node.Load()
		if current == nil {
			http.Error(w, "worker restarting", http.StatusServiceUnavailable)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/execute") {
			executions.Add(1)
			if holdExecution.Load() {
				select {
				case executionEntered <- struct{}{}:
				default:
				}
				select {
				case <-executionRelease:
				case <-r.Context().Done():
					return
				}
			}
			if loseCompletion.Load() {
				// Execute the actual worker and durable Commit, then lose only
				// the response. No fake query/store success is injected.
				current.ServeHTTP(httptest.NewRecorder(), r)
				panic(http.ErrAbortHandler)
			}
		}
		current.ServeHTTP(w, r)
	}))
	workerServer.TLS = serverTLS
	workerServer.StartTLS()
	t.Cleanup(workerServer.Close)
	keyFile := filepath.Join(t.TempDir(), "principals.yml")
	writeKeys := func(t *testing.T, revision uint64, analyst []string) gatewayKeySet {
		raw, err := yaml.Marshal(gatewayKeyDocument{Version: 2, Revision: revision, Principals: map[string]map[string][]string{
			"a": {"analyst": analyst, "reports": {rotationOther}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		publishTLSIdentity(t, keyFile, raw)
		set, err := parseGatewayKeys(raw, map[string]bool{"a": true}, 1)
		if err != nil {
			t.Fatal(err)
		}
		return set
	}
	writeKeys(t, 1, []string{rotationOld, rotationNew})
	gatewayStore, err := OpenStore(ctx, gatewayNATS, policy, false)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(GatewayConfig{WorkerTLS: gatewayTLS, MaxHTTPRequests: 8,
		Authentication: &GatewayAuthenticationConfig{KeysFile: keyFile, ReloadInterval: time.Second},
		Exports:        &GatewayExportConfig{MaxSupervisors: 2, MaxDownloads: 2},
		Tenants:        []TenantConfig{{Policy: policy, NATS: gatewayNATS, Workers: []Endpoint{{ID: "a1", URL: workerServer.URL}}}},
	}, map[string]Store{"a": gatewayStore})
	if err != nil {
		_ = gatewayStore.Close()
		t.Fatal("start real gateway", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Error("gateway cleanup", err)
		}
	})
	public := httptest.NewTLSServer(gateway)
	t.Cleanup(public.Close)
	t.Cleanup(releaseExecution)
	client := public.Client()
	client.Timeout = 20 * time.Second
	call := func(t *testing.T, method, path, token string, payload any) (int, []byte, error) {
		var body io.Reader
		if payload != nil {
			raw, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			body = bytes.NewReader(raw)
		}
		req, err := http.NewRequestWithContext(ctx, method, public.URL+path, body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
		return response.StatusCode, raw, err
	}
	submit := func(t *testing.T, codec string) ExportAcceptedResponse {
		code, raw, err := call(t, "POST", "/v1/exports", rotationOld, ExportSubmitRequest{
			Query: query.Request{Mode: "federated", Sources: []string{"orders_fast"}, SQL: "SELECT id, amount FROM orders_fast ORDER BY id"}, Compression: codec,
		})
		if err != nil || code != http.StatusCreated {
			t.Fatalf("submit: code=%d err=%v body=%s", code, err, raw)
		}
		var result ExportAcceptedResponse
		if err := json.Unmarshal(raw, &result); err != nil || result.ID == "" {
			t.Fatal("invalid accepted handle", err)
		}
		return result
	}
	waitState := func(t *testing.T, id, wanted string) ExportSnapshot {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			snapshot, err := gateway.exports["a"].GetExport(ctx, id)
			if err == nil && snapshot.Job.State == wanted {
				logExportWaitObservation(t, wanted, snapshot.Job, err, time.Now().After(deadline), executions.Load())
				return snapshot
			}
			if err != nil || time.Now().After(deadline) || (err == nil && !exportActive(snapshot.Job) && snapshot.Job.State != ExportReady) {
				logExportWaitObservation(t, wanted, snapshot.Job, err, time.Now().After(deadline), executions.Load())
				t.Fatalf("await %s: state=%s error=%v job_error=%v", wanted, snapshot.Job.State, err, snapshot.Job.Error)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	download := func(t *testing.T, id, token string) []byte {
		code, raw, err := call(t, "GET", "/v1/exports/"+id+"/manifest", token, nil)
		if err != nil || code != http.StatusOK {
			t.Fatalf("manifest: %d %v %s", code, err, raw)
		}
		var manifest ExportManifestResponse
		if err := json.Unmarshal(raw, &manifest); err != nil || len(manifest.Parts) != 1 || manifest.Rows != 512 {
			t.Fatal("manifest mismatch", err, manifest.Rows, len(manifest.Parts))
		}
		code, raw, err = call(t, "GET", "/v1/exports/"+id+"/parts/0", token, nil)
		if err != nil || code != http.StatusOK {
			t.Fatalf("part: %d %v", code, err)
		}
		verifySnapshotPrincipalIPC(t, raw)
		return raw
	}
	var retained ExportAcceptedResponse
	t.Run("accepted-export-survives-submit-disconnect", func(t *testing.T) {
		holdExecution.Store(true)
		retained = submit(t, "none")
		client.CloseIdleConnections()
		select {
		case <-executionEntered:
		case <-ctx.Done():
			t.Fatal("worker execution not requested")
		}
		releaseExecution()
		holdExecution.Store(false)
		waitState(t, retained.ID, ExportReady)
		a, b := download(t, retained.ID, rotationOld), download(t, retained.ID, rotationNew)
		if !bytes.Equal(a, b) || executions.Load() != 1 {
			t.Fatal("repeat download reran SQL or changed bytes")
		}
		for _, route := range []string{"", "/manifest", "/parts/0"} {
			code, _, err := call(t, "GET", "/v1/exports/"+retained.ID+route, rotationOther, nil)
			if err != nil || code != http.StatusNotFound {
				t.Fatal("foreign principal handle disclosed", code, err)
			}
		}
		code, _, err := call(t, "POST", "/v1/exports/"+retained.ID+"/cancel", rotationOther, nil)
		if err != nil || code != http.StatusNotFound {
			t.Fatal("foreign principal could cancel export", code, err)
		}
	})
	if t.Failed() {
		return
	}
	t.Run("lz4-preserves-values-and-null-policy", func(t *testing.T) {
		accepted := submit(t, "lz4_frame")
		waitState(t, accepted.ID, ExportReady)
		download(t, accepted.ID, rotationOld)
		if executions.Load() != 2 {
			t.Fatal("unexpected execution count")
		}
	})
	if t.Failed() {
		return
	}
	t.Run("ready-export-survives-worker-restart", func(t *testing.T) {
		before, err := gateway.exports["a"].GetExport(ctx, retained.ID)
		if err != nil {
			t.Fatal(err)
		}
		old := node.Swap(nil)
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
		// Worker identities cannot be taken over until their prior lease expires.
		time.Sleep(policy.LeaseDuration + 100*time.Millisecond)
		startWorker(t)
		download(t, retained.ID, rotationNew)
		after, err := gateway.exports["a"].GetExport(ctx, retained.ID)
		if err != nil || !sameExportReceipt(before.Job.Receipt, after.Job.Receipt) || executions.Load() != 2 {
			t.Fatal("restart changed retained custody or reran SQL", err)
		}
	})
	if t.Failed() {
		return
	}
	t.Run("lost-completion-is-not-replayed", func(t *testing.T) {
		loseCompletion.Store(true)
		accepted := submit(t, "none")
		cancelled := waitState(t, accepted.ID, ExportCancelled)
		loseCompletion.Store(false)
		if cancelled.Job.Receipt == nil {
			t.Fatal("fixture did not reach durable Stored before losing response")
		}
		if err := gateway.exports["a"].ReconcileExports(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(300 * time.Millisecond)
		if executions.Load() != 3 {
			t.Fatal("lost completion replayed SQL")
		}
		code, _, err := call(t, "GET", "/v1/exports/"+accepted.ID+"/parts/0", rotationOld, nil)
		if err != nil || code == http.StatusOK {
			t.Fatal("unpublished part readable", code, err)
		}
	})
	if t.Failed() {
		return
	}
	t.Run("fresh-key-rechecks-ready-downloads", func(t *testing.T) {
		set := writeKeys(t, 2, []string{rotationNew})
		if !gateway.auth.apply(set, time.Now()) {
			t.Fatal("key revocation failed")
		}
		code, _, err := call(t, "GET", "/v1/exports/"+retained.ID+"/parts/0", rotationOld, nil)
		if err != nil || code != http.StatusUnauthorized {
			t.Fatal("revoked key downloaded export", code, err)
		}
		download(t, retained.ID, rotationNew)
		code, _, err = call(t, "POST", "/v1/exports/"+retained.ID+"/cancel", rotationNew, nil)
		if err != nil || code != http.StatusOK {
			t.Fatal("ready withdrawal failed", code, err)
		}
		code, _, err = call(t, "GET", "/v1/exports/"+retained.ID+"/parts/0", rotationNew, nil)
		if err != nil || code == http.StatusOK {
			t.Fatal("withdrawn export readable", code, err)
		}
	})
	// HTTP completion can precede the worker handler's deferred lease release.
	deadline := time.Now().Add(time.Second)
	for engine.ResourcePool.Snapshot().Active != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if state := engine.ResourcePool.Snapshot(); state.Active != 0 {
		t.Fatal("export reservations leaked", state.Active)
	}
	if reclaimed, err := scratch.Reclaim(); err != nil || reclaimed != 0 {
		t.Fatal("sandbox retained scratch ownership", reclaimed, err)
	}
	entries, err := os.ReadDir(config.ScratchDirectory)
	if err != nil || len(entries) != 1 || entries[0].Name() != ".kelvo-scratch.lock" || !entries[0].Type().IsRegular() {
		t.Fatal("sandbox scratch leaked", err, entries)
	}
}

// Public acceptance receipts retain only this closed schema. The full test log
// stays private; neither job identity nor arbitrary provider/error text enters
// the marker consumed by scripts/export_diagnostics.py.
func logExportWaitObservation(t *testing.T, wanted string, job ExportJob, err error, deadlineReached bool, executions int32) {
	t.Helper()
	state := func(value string) string {
		switch value {
		case ExportQueued, ExportAssigned, ExportClaimed, ExportRunning, ExportStored, ExportReady,
			ExportFailed, ExportCancelled, ExportPublicationUncertain:
			return value
		default:
			return "unknown"
		}
	}
	errorClass := "none"
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			errorClass = "context_cancelled"
		case errors.Is(err, context.DeadlineExceeded):
			errorClass = "deadline_exceeded"
		case errors.Is(err, ErrExportNotFound):
			errorClass = "not_found"
		case errors.Is(err, ErrExportConflict):
			errorClass = "conflict"
		default:
			errorClass = "store_error"
		}
	} else if job.Error != nil {
		switch job.Error.Code {
		case "CANCELLED":
			errorClass = "job_cancelled"
		case "DEADLINE_EXCEEDED":
			errorClass = "job_deadline_exceeded"
		case "UNAVAILABLE":
			errorClass = "job_unavailable"
		default:
			errorClass = "job_error"
		}
	}
	observation := struct {
		Wanted          string `json:"wanted"`
		Observed        string `json:"observed"`
		DeadlineReached bool   `json:"deadline_reached"`
		ReceiptPresent  bool   `json:"receipt_present"`
		ErrorClass      string `json:"error_class"`
		Executions      int32  `json:"executions"`
	}{state(wanted), state(job.State), deadlineReached, job.Receipt != nil, errorClass, executions}
	raw, marshalErr := json.Marshal(observation)
	if marshalErr != nil {
		t.Fatal("export wait observation encoding failed")
	}
	t.Logf("KELVO_EXPORT_WAIT_STATE %s", raw)
}

func exportIntegrationTLS(t *testing.T) (TLSConfig, TLSConfig) {
	t.Helper()
	root := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "export-test"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(root, "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	leaf := func(name, identity string) TLSConfig {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		uri, err := url.Parse(identity)
		if err != nil {
			t.Fatal(err)
		}
		certificate := &x509.Certificate{SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: name},
			NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{uri},
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, certificate, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		pk, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		config := TLSConfig{CAFile: caPath, CertFile: filepath.Join(root, name+".pem"), KeyFile: filepath.Join(root, name+".key")}
		for path, data := range map[string][]byte{config.CertFile: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), config.KeyFile: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})} {
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(fmt.Errorf("write fixture certificate: %w", err))
			}
		}
		return config
	}
	return leaf("gateway", GatewayIdentity), leaf("worker", WorkerIdentity("a", "a1"))
}
