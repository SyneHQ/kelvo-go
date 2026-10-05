//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func protectedExportNATS(t *testing.T, tenant, role string) NATSConfig {
	t.Helper()
	base := "KELVO_TEST_PROTECTED_EXPORT_NATS"
	prefix := base + "_" + strings.ToUpper(tenant) + "_" + role
	c := NATSConfig{URL: os.Getenv(base + "_URL"), CAFile: os.Getenv(base + "_CA_FILE"), Username: os.Getenv(prefix + "_USER"), PasswordEnv: prefix + "_PASSWORD"}
	if c.URL == "" || c.CAFile == "" || c.Username == "" || len(os.Getenv(c.PasswordEnv)) < 32 {
		t.Fatal("missing dedicated protected-export broker identity")
	}
	return c
}

func protectedExportResources() *ResourceConfig {
	return &ResourceConfig{MaxConcurrent: 2, MemoryMB: 640, BaselineMB: 64, OverheadMB: 160, ScratchMB: 32,
		QueryReserveSlots: 1, QueryReserveMemoryMB: 224, QueryReserveScratchMB: 16,
		Export: &ResourceClassConfig{MaxConcurrent: 1, MemoryMB: 256, ScratchMB: 16}}
}

func configureProtectedExportPolicy(t *testing.T, p *Policy) {
	t.Helper()
	p.Exports = &ExportPolicy{AuthorizationVersion: "protected-export-v1", MaxJobs: 32, QueueTimeout: 20 * time.Second,
		DefaultTTL: 5 * time.Minute, MaxTTL: 10 * time.Minute,
		Limits: exports.Limits{MaxRows: 1024, MaxEncodedBytes: 8 << 20, MaxDecodedBytes: 8 << 20,
			MaxPartBytes: 1 << 20, MaxPartDecodedBytes: 1 << 20, MaxParts: 8, Compression: "lz4_frame"}}
	if err := ValidatePolicy(*p); err != nil {
		t.Fatal("protected export policy", err)
	}
}

func configureProtectedExportNode(t *testing.T, x *protectedQueryTenant, e *worker.Executor, c *NodeConfig) {
	t.Helper()
	c.Resources = protectedExportResources()
	c.ScratchDirectory = x.scratchPath
	c.Containment = &ContainmentConfig{Config: containment.Config{Root: os.Getenv("KELVO_TEST_CGROUP_ROOT"), StateDirectory: os.Getenv("KELVO_TEST_CGROUP_STATE")}, Budget: e.ContainmentBudget}
	c.Exports = &ExportNodeConfig{Directory: filepath.Join(t.TempDir(), "exports"), MaxEntries: 32, MaxStoredBytes: 384 << 20,
		MaxConcurrent: 1, MaxDownloads: 2, DownloadMemoryMB: 16, CleanupInterval: time.Minute, CleanupMaxRemovals: 16}
	e.ResourceOverheadBytes = c.Resources.OverheadMB << 20
	if err := validateNodeExports(*c); err != nil {
		t.Fatal("protected export node budgets", err)
	}
}

// These hooks only delay or lose a response after the real node completes.
// They cannot manufacture an export receipt, Stored/Ready CAS or Arrow bytes.
type protectedExportProbe struct {
	executions, completions, parts atomic.Int64
	loseCompletion                 atomic.Bool
	mu                             sync.Mutex
	afterCompletion, beforeTail    func()
}

func (p *protectedExportProbe) take(completion bool) func() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if completion {
		callback := p.afterCompletion
		p.afterCompletion = nil
		return callback
	}
	callback := p.beforeTail
	p.beforeTail = nil
	return callback
}

func (p *protectedExportProbe) serve(x *protectedQueryTenant, w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, "/internal/exports/") {
		return false
	}
	execute := strings.HasSuffix(r.URL.Path, "/execute")
	part := strings.Contains(r.URL.Path, "/parts/")
	if !execute && !part {
		return false
	}
	if execute {
		p.executions.Add(1)
	} else {
		p.parts.Add(1)
	}
	response := httptest.NewRecorder()
	x.node.ServeHTTP(response, r)
	data := response.Body.Bytes()
	if execute && response.Code == http.StatusOK {
		p.completions.Add(1)
		if callback := p.take(true); callback != nil {
			callback()
		}
		if p.loseCompletion.CompareAndSwap(true, false) {
			panic(http.ErrAbortHandler)
		}
	}
	if part {
		x.wireMu.Lock()
		x.lastWire = bytes.Clone(data)
		x.wireMu.Unlock()
	}
	for key, values := range response.Header() {
		w.Header()[key] = append([]string(nil), values...)
	}
	if !part || response.Code != http.StatusOK || !bytes.HasSuffix(data, protectedQueryEOS) {
		w.WriteHeader(response.Code)
		_, _ = w.Write(data)
		return true
	}
	if x.length.Load() {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	} else {
		w.Header().Del("Content-Length")
	}
	w.WriteHeader(response.Code)
	x.framed.Add(1)
	// Expose the real part body before the final EOS barrier. The gateway must
	// independently reject completion after its initiating key is revoked.
	_, _ = w.Write(data[:len(data)-len(protectedQueryEOS)])
	w.(http.Flusher).Flush()
	if callback := p.take(false); callback != nil {
		callback()
	}
	_, _ = w.Write(data[len(data)-len(protectedQueryEOS):])
	return true
}

func newProtectedExportFixture(t *testing.T) *protectedQueryFixture {
	t.Helper()
	return newProtectedQueryFixtureWithOptions(t, protectedQueryFixtureOptions{exports: true})
}

func (f *protectedQueryFixture) submitExport(t *testing.T, key string, request query.Request, codec string) string {
	t.Helper()
	r := f.call(f.ctx, http.MethodPost, "/v1/exports", key, ExportSubmitRequest{Query: request, Compression: codec}, false)
	if r.err != nil || r.code != http.StatusCreated {
		t.Fatalf("protected export submission: status=%d error=%v", r.code, r.err)
	}
	var accepted ExportAcceptedResponse
	if json.Unmarshal(r.data, &accepted) != nil || accepted.ID == "" {
		t.Fatal("invalid protected export handle")
	}
	return accepted.ID
}

func (f *protectedQueryFixture) exportState(t *testing.T, tenant, id, wanted string) ExportSnapshot {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for {
		s, err := f.gateway.exports[tenant].GetExport(f.ctx, id)
		if err == nil && s.Job.State == wanted {
			return s
		}
		if err != nil || (!exportActive(s.Job) && s.Job.State != ExportReady) {
			t.Fatalf("export state: wanted=%s observed=%s error=%v job_error=%v", wanted, s.Job.State, err, s.Job.Error)
		}
		select {
		case <-deadline.C:
			t.Fatalf("export state deadline: wanted=%s observed=%s", wanted, s.Job.State)
		case <-f.ctx.Done():
			t.Fatal("export fixture deadline")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (f *protectedQueryFixture) exportPart(t *testing.T, tenant, id, key string) []byte {
	t.Helper()
	r := f.call(f.ctx, http.MethodGet, "/v1/exports/"+id+"/manifest", key, nil, false)
	if r.err != nil || r.code != http.StatusOK {
		t.Fatal("protected export manifest unavailable", r.code, r.err)
	}
	var manifest ExportManifestResponse
	if err := json.Unmarshal(r.data, &manifest); err != nil || len(manifest.Parts) != 1 || manifest.ID != id {
		t.Fatal("tiny export should have one complete Arrow batch/part", err)
	}
	ready := f.exportState(t, tenant, id, ExportReady)
	if ready.Job.Receipt == nil || manifest.Rows != ready.Job.Stats.Rows || manifest.SchemaSHA256 != ready.Job.Receipt.Manifest.SchemaSHA256 {
		t.Fatal("public export metadata differs from the durable receipt")
	}
	r = f.call(f.ctx, http.MethodGet, "/v1/exports/"+id+"/parts/0", key, nil, false)
	if r.err != nil || r.code != http.StatusOK || !bytes.HasSuffix(r.data, protectedQueryEOS) || r.header.Get("Kelvo-Result-Completion") != "durable-eos-v1" {
		t.Fatal("protected export part lacks complete HTTP/Arrow delivery", r.code, r.err)
	}
	digest := sha256.Sum256(r.data)
	if int64(len(r.data)) != manifest.Parts[0].EncodedBytes || hex.EncodeToString(digest[:]) != manifest.Parts[0].SHA256 {
		t.Fatal("protected export part did not match its advertised exact bytes")
	}
	return r.data
}

func (f *protectedQueryFixture) cleanExports(t *testing.T) {
	t.Helper()
	f.clean(t)
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		active := len(f.gateway.exportSupervisors) + len(f.gateway.exportDownloads)
		for _, x := range f.tenants {
			r := x.node.exports
			r.mu.Lock()
			active += len(r.jobs)
			r.mu.Unlock()
			// The idle dispatch loop may hold a permit while polling NATS.
			active += len(r.downloads)
		}
		if active == 0 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("export retained supervision, admission, download or execution custody")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (f *protectedQueryFixture) requireNoExportRead(t *testing.T, id, key string) {
	t.Helper()
	for _, suffix := range []string{"/manifest", "/parts/0"} {
		r := f.call(f.ctx, "GET", "/v1/exports/"+id+suffix, key, nil, false)
		if r.err != nil || r.code == http.StatusOK || bytes.HasSuffix(r.data, protectedQueryEOS) {
			t.Fatal("unready/withdrawn protected export became readable", r.code, r.err)
		}
	}
}

func (f *protectedQueryFixture) resetExportKeys(t *testing.T, tenant string) (old, replacement string) {
	t.Helper()
	old, replacement = protectedQueryKey(tenant, "analyst", "old"), protectedQueryKey(tenant, "analyst", "new")
	f.keys[tenant]["analyst"] = []string{old, replacement}
	if !f.gateway.auth.apply(f.writeKeys(t), time.Now()) {
		t.Fatal("restore protected export key revision")
	}
	return old, replacement
}

func (f *protectedQueryFixture) exportRevocation(t *testing.T, tenant, replacement string) func() bool {
	t.Helper()
	f.keys[tenant]["analyst"] = []string{replacement}
	return f.prepareKeyChange(t)
}

func (f *protectedQueryFixture) exportExecutionCount() int64 {
	var count int64
	for _, x := range f.tenants {
		count += x.export.executions.Load()
	}
	return count
}
