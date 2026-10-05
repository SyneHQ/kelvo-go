//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/testutil/protectedobject"
)

func TestContainedProtectedObjectExports(t *testing.T) {
	if os.Getenv("KELVO_TEST_PROTECTED_EXPORTS") != "1" {
		t.Skip("explicit protected-export containment acceptance required")
	}
	for _, name := range []string{"KELVO_TEST_BINARY", "KELVO_TEST_SANDBOX", "KELVO_TEST_CGROUP_ROOT", "KELVO_TEST_CGROUP_STATE", "KELVO_TEST_PROTECTED_EXPORT_NATS_URL"} {
		if os.Getenv(name) == "" {
			t.Fatal("missing protected-export fixture setting", name)
		}
	}
	if os.Getenv("KELVO_PROTECTED_EXPORT_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestContainedProtectedObjectExports$", "-test.v", "-test.timeout=4m30s")
		command.Env = append(os.Environ(), "KELVO_PROTECTED_EXPORT_CHILD=1")
		var output protectedQueryOutput
		command.Stdout, command.Stderr = &output, &output
		command.WaitDelay = 5 * time.Second
		if err := command.Run(); err != nil || output.truncated {
			t.Fatalf("protected export child failed: %v\n%s", err, output.String())
		}
		t.Log(output.String())
		t.Log("KELVO_PROTECTED_EXPORT_OUTER_PASS")
		return
	}
	t.Log("KELVO_PROTECTED_EXPORT_CHILD_START")
	f := newProtectedExportFixture(t)
	retained := map[string]string{}
	if !t.Run("publish-real-single-and-two-part", func(t *testing.T) {
		for _, tenant := range []string{"a", "b"} {
			x := f.tenants[tenant]
			for _, id := range []string{"orders_single", "orders_multi", "hidden_orders"} {
				snapshot, err := x.manager.Refresh(f.ctx, id, false)
				if err != nil || snapshot.Rows != 4 || snapshot.Generation == "" {
					t.Fatal("protected export source publication failed", id, err)
				}
				if id == "orders_multi" && (len(snapshot.Parts) != 2 || snapshot.Parts[0].Rows != 2 || snapshot.Parts[1].Rows != 2) {
					t.Fatal("source must contain two real Parquet parts")
				}
				if id != "orders_multi" && len(snapshot.Parts) != 0 {
					t.Fatal("single source unexpectedly became multipart")
				}
			}
		}
		if f.factories.Load() != 6 || f.executions.Load() != 6 {
			t.Fatal("unexpected source refresh count")
		}
		f.cleanExports(t)
	}) {
		return
	}
	if !t.Run("exact-principal-fills-and-repeat-downloads", func(t *testing.T) {
		for _, tenant := range []string{"a", "b"} {
			for _, principal := range []string{"analyst", "reports"} {
				key := protectedQueryKey(tenant, principal, "old")
				codec := f.tenants[tenant].policy.Limits.ResultCompression
				before := f.service.Snapshot().Tenants[tenant].RangeRequests
				id := f.submitExport(t, key, protectedQueryRequest(principal, false), codec)
				ready := f.exportState(t, tenant, id, ExportReady)
				retained[tenant+"/"+principal] = id
				if ready.Job.Spec.StorageLimits.Compression != codec || len(ready.Job.Stats.Accelerations) != 2 || f.service.Snapshot().Tenants[tenant].RangeRequests <= before {
					t.Fatal("export did not use both protected generations and requested codec")
				}
				f.cleanExports(t)
				requests, executions := f.service.Snapshot().Total.Requests, f.exportExecutionCount()
				first := f.exportPart(t, tenant, id, key)
				verifyProtectedQueryRows(t, first, tenant, principal)
				replacement := key
				if principal == "analyst" {
					replacement = protectedQueryKey(tenant, principal, "new")
				}
				second := f.exportPart(t, tenant, id, replacement)
				f.cleanExports(t)
				if !bytes.Equal(first, second) || f.exportExecutionCount() != executions || f.service.Snapshot().Total.Requests != requests || f.service.Readers(tenant) != 0 {
					t.Fatal("repeat download reran fill, reread source or retained source pins")
				}
				id = f.submitExport(t, key, protectedQueryRequest(principal, true), codec)
				f.exportState(t, tenant, id, ExportReady)
				want := int64(3)
				if principal == "reports" {
					want = 1
				}
				verifyProtectedQueryAggregate(t, f.exportPart(t, tenant, id, key), want)
				f.cleanExports(t)
			}
		}
		if f.exportExecutionCount() != 8 || f.executions.Load() != 6 {
			t.Fatal("export fill replayed or executed a native source")
		}
	}) {
		return
	}
	if !t.Run("foreign-handles-and-hidden-columns", func(t *testing.T) {
		id := retained["a/analyst"]
		for _, key := range []string{protectedQueryKey("a", "reports", "old"), protectedQueryKey("b", "analyst", "old"), protectedQueryKey("b", "reports", "old")} {
			for _, route := range []struct{ method, suffix string }{{"GET", ""}, {"GET", "/manifest"}, {"GET", "/parts/0"}, {"POST", "/cancel"}} {
				r := f.call(f.ctx, route.method, "/v1/exports/"+id+route.suffix, key, nil, true)
				if r.err != nil || r.code != http.StatusNotFound {
					t.Fatal("foreign export handle was disclosed", r.code, r.err)
				}
			}
		}
		key := protectedQueryKey("a", "analyst", "old")
		for _, sql := range []string{"SELECT tenant_id FROM orders_single", "SELECT payload FROM orders_single"} {
			request := protectedQueryRequest("analyst", false)
			request.SQL = sql
			id := f.submitExport(t, key, request, "none")
			f.exportState(t, "a", id, ExportFailed)
			f.requireNoExportRead(t, id, key)
			f.cleanExports(t)
		}
	}) {
		return
	}
	if !t.Run("forged-stale-and-unsupported-before-io", func(t *testing.T) {
		key := protectedQueryKey("a", "analyst", "old")
		requests, executions := f.service.Snapshot().Total.Requests, f.exportExecutionCount()
		for _, payload := range []any{
			map[string]any{"query": protectedQueryRequest("analyst", false), "authority": map[string]string{"principal_id": "reports"}},
			ExportSubmitRequest{Query: query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT 1"}},
			ExportSubmitRequest{Query: query.Request{Mode: "federated", Sources: []string{"hidden_orders"}, SQL: "SELECT * FROM hidden_orders"}},
		} {
			r := f.call(f.ctx, "POST", "/v1/exports", key, payload, true)
			if r.err != nil || (r.code != http.StatusBadRequest && r.code != http.StatusForbidden) {
				t.Fatal("forged or unsupported export admitted", r.code, r.err)
			}
		}
		// Change only a completed record in this disposable gateway's real KV.
		// Public DTOs cannot express this stale durable authority. Restore the
		// original bytes before later leaves use the retained export again.
		store := f.gateway.exports["a"].(*natsExportStore)
		id := retained["a/analyst"]
		slot, _ := exportSlot(id)
		entry, err := store.kv.Get(f.ctx, exportKey(slot))
		if err != nil {
			t.Fatal(err)
		}
		original := bytes.Clone(entry.Value())
		var stale ExportJob
		if err := json.Unmarshal(original, &stale); err != nil {
			t.Fatal(err)
		}
		stale.Authority.Principal.PolicyVersion = strings.Repeat("0", 64)
		raw, err := json.Marshal(stale)
		if err != nil {
			t.Fatal(err)
		}
		revision, err := store.kv.Update(f.ctx, exportKey(slot), raw, entry.Revision())
		if err != nil {
			t.Fatal("inject disposable stale export record", err)
		}
		defer func() {
			if _, err := store.kv.Update(f.ctx, exportKey(slot), original, revision); err != nil {
				t.Error("restore disposable export record", err)
			}
		}()
		f.requireNoExportRead(t, id, key)
		if f.service.Snapshot().Total.Requests != requests || f.exportExecutionCount() != executions {
			t.Fatal("rejected export accessed a protected source or executed SQL")
		}
		f.cleanExports(t)
	}) {
		return
	}
	if !t.Run("revocation-before-ready", func(t *testing.T) {
		old, replacement := f.resetExportKeys(t, "a")
		revoke := f.exportRevocation(t, "a", replacement)
		x := f.tenants["a"]
		var revoked atomic.Bool
		x.export.mu.Lock()
		x.export.afterCompletion = func() { revoked.Store(revoke()) }
		x.export.mu.Unlock()
		before := x.export.completions.Load()
		id := f.submitExport(t, old, protectedQueryRequest("analyst", false), "none")
		cancelled := f.exportState(t, "a", id, ExportCancelled)
		if !revoked.Load() || x.export.completions.Load() != before+1 || cancelled.Job.Receipt == nil {
			t.Fatal("revocation did not follow a real durable local commit")
		}
		f.requireNoExportRead(t, id, replacement)
		f.cleanExports(t)
	}) {
		return
	}
	for _, mode := range []struct {
		name, tenant, codec string
		length              bool
	}{{"none-chunked", "a", "none", false}, {"none-length", "a", "none", true}, {"lz4-chunked", "b", "lz4_frame", false}, {"lz4-length", "b", "lz4_frame", true}} {
		if !t.Run("revocation-"+mode.name, func(t *testing.T) {
			old, replacement := f.resetExportKeys(t, mode.tenant)
			x := f.tenants[mode.tenant]
			x.length.Store(mode.length)
			id := f.submitExport(t, old, protectedQueryRequest("analyst", false), mode.codec)
			f.exportState(t, mode.tenant, id, ExportReady)
			f.cleanExports(t)
			revoke := f.exportRevocation(t, mode.tenant, replacement)
			var revoked atomic.Bool
			x.export.mu.Lock()
			x.export.beforeTail = func() { revoked.Store(revoke()) }
			x.export.mu.Unlock()
			before := x.framed.Load()
			r := f.call(f.ctx, "GET", "/v1/exports/"+id+"/parts/0", old, nil, false)
			if !revoked.Load() || x.framed.Load() != before+1 || bytes.HasSuffix(r.data, protectedQueryEOS) || (r.err == nil && r.code == http.StatusOK) {
				t.Fatal("revoked download completed or missed the real framing barrier", r.code, r.err)
			}
			x.wireMu.Lock()
			produced := bytes.Clone(x.lastWire)
			x.wireMu.Unlock()
			if !bytes.HasSuffix(produced, protectedQueryEOS) {
				t.Fatal("revocation barrier did not receive a complete stored Arrow part")
			}
			verifyProtectedQueryRows(t, produced, mode.tenant, "analyst")
			// A download key revocation does not withdraw a previously ready
			// export from a still-authorized replacement of the same principal.
			verifyProtectedQueryRows(t, f.exportPart(t, mode.tenant, id, replacement), mode.tenant, "analyst")
			if r := f.call(f.ctx, "GET", "/v1/exports/"+id, old, nil, false); r.err != nil || r.code != http.StatusUnauthorized {
				t.Fatal("revoked export key stayed active")
			}
			x.length.Store(false)
			f.cleanExports(t)
		}) {
			return
		}
	}
	if !t.Run("lost-completion-is-not-replayed", func(t *testing.T) {
		key := protectedQueryKey("a", "analyst", "new")
		x := f.tenants["a"]
		x.export.loseCompletion.Store(true)
		before := x.export.executions.Load()
		id := f.submitExport(t, key, protectedQueryRequest("analyst", false), "none")
		cancelled := f.exportState(t, "a", id, ExportCancelled)
		if cancelled.Job.Local == nil || cancelled.Job.Receipt == nil || x.export.loseCompletion.Load() || x.export.executions.Load() != before+1 {
			t.Fatal("lost response was replayed or never followed actual local commit")
		}
		f.requireNoExportRead(t, id, key)
		f.cleanExports(t)
		root := filepath.Join(x.node.exports.custody.DataDirectory(), cancelled.Job.Local.ExportID)
		for _, name := range []string{"state.yml", "manifest.yml"} {
			if info, err := os.Stat(filepath.Join(root, name)); err != nil || !info.Mode().IsRegular() {
				t.Fatal("uncertain completion discarded retained local storage", err)
			}
		}
	}) {
		return
	}
	if !t.Run("cancelled-range-retains-custody", func(t *testing.T) {
		key := protectedQueryKey("a", "analyst", "new")
		gate := f.service.Hold(protectedobject.RangeFinalByte, "a", "orders_single")
		defer gate.Release()
		id := f.submitExport(t, key, protectedQueryRequest("analyst", false), "none")
		protectedQueryAwait(t, f.ctx, gate.Entered(), "export source range")
		if f.pool.Snapshot().Classes[admission.ClassExport].Active != 1 || f.containment.Status().Active != 1 || f.service.Readers("a") == 0 {
			t.Fatal("export source read lacks child, admission or reader custody")
		}
		r := f.call(f.ctx, "POST", "/v1/exports/"+id+"/cancel", key, nil, false)
		if r.err != nil || r.code != http.StatusOK {
			t.Fatal("active export cancellation failed", r.code, r.err)
		}
		gate.Release()
		f.exportState(t, "a", id, ExportCancelled)
		f.requireNoExportRead(t, id, key)
		f.cleanExports(t)
	}) {
		return
	}
	if !t.Run("stalled-registry-retains-custody", func(t *testing.T) {
		x := f.tenants["b"]
		key := protectedQueryKey("b", "analyst", "new")
		gate := f.service.Hold(protectedobject.PinReleaseBeforeCAS, "b", "orders_single")
		defer gate.Release()
		id := f.submitExport(t, key, protectedQueryRequest("analyst", false), "lz4_frame")
		protectedQueryAwait(t, f.ctx, gate.Entered(), "export source reader release")
		if f.pool.Snapshot().Classes[admission.ClassExport].Active != 1 || f.service.Readers("b") == 0 {
			t.Fatal("stalled protected export lost reader or export admission")
		}
		short, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err := x.node.Drain(short)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) || f.pool.Snapshot().Classes[admission.ClassExport].Active != 1 {
			t.Fatal("bounded drain released an unjoined protected export", err)
		}
		gate.Release()
		f.exportState(t, "b", id, ExportReady)
		f.cleanExports(t)
	}) {
		return
	}
	f.cleanExports(t)
	if f.executions.Load() != 6 || f.secrets.Load() != 0 {
		t.Fatal("protected exports fell back to native source execution")
	}
	t.Log("KELVO_PROTECTED_EXPORT_CHILD_PASS")
}
