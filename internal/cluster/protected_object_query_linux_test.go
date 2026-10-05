//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/testutil/protectedobject"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

var protectedQueryEOS = []byte{255, 255, 255, 255, 0, 0, 0, 0}

// This gate needs the runner's disposable TLS broker and containment delegation.
// Isolating the test process ensures the real provider CA is installed before
// Go first caches system roots. No production verification bypass is enabled.
func TestContainedProtectedObjectQueries(t *testing.T) {
	if os.Getenv("KELVO_TEST_PROTECTED_QUERIES") != "1" {
		t.Skip("explicit protected-query containment acceptance required")
	}
	for _, name := range []string{"KELVO_TEST_BINARY", "KELVO_TEST_SANDBOX", "KELVO_TEST_CGROUP_ROOT", "KELVO_TEST_CGROUP_STATE", "KELVO_TEST_QUERY_NATS_URL"} {
		if os.Getenv(name) == "" {
			t.Fatal("missing protected-query fixture setting", name)
		}
	}
	if os.Getenv("KELVO_PROTECTED_QUERY_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestContainedProtectedObjectQueries$", "-test.v", "-test.timeout=4m30s")
		command.Env = append(os.Environ(), "KELVO_PROTECTED_QUERY_CHILD=1")
		var output protectedQueryOutput
		command.Stdout, command.Stderr = &output, &output
		if err := command.Run(); err != nil || output.truncated {
			t.Fatalf("protected query child failed: %v\n%s", err, output.String())
		}
		t.Log(output.String())
		t.Log("KELVO_PROTECTED_QUERY_OUTER_PASS")
		return
	}
	t.Log("KELVO_PROTECTED_QUERY_CHILD_START")
	f := newProtectedQueryFixture(t)
	if !t.Run("publish-real-single-and-two-part", func(t *testing.T) {
		for _, tenant := range []string{"a", "b"} {
			x := f.tenants[tenant]
			for _, id := range []string{"orders_single", "orders_multi", "hidden_orders"} {
				snapshot, err := x.manager.Refresh(f.ctx, id, false)
				if err != nil || snapshot.Rows != 4 || snapshot.Generation == "" {
					t.Fatal("protected publication failed", id, err)
				}
				if id == "orders_multi" && (len(snapshot.Parts) != 2 || snapshot.Parts[0].Rows != 2 || snapshot.Parts[1].Rows != 2) {
					t.Fatal("multipart fixture must contain exactly two real two-row parts", len(snapshot.Parts))
				}
				if id != "orders_multi" && len(snapshot.Parts) != 0 {
					t.Fatal("single-file fixture became multipart")
				}
				root, ok := f.service.Root(tenant, id)
				if !ok || len(root.Data) == 0 || root.Version == "" || root.Digest == "" {
					t.Fatal("publication lacks real CAS manifest identity")
				}
			}
		}
		if f.factories.Load() != 6 || f.executions.Load() != 6 {
			t.Fatal("unexpected source publication count")
		}
		f.clean(t)
	}) {
		return
	}
	if !t.Run("tenant-principal-cte-join-aggregate", func(t *testing.T) {
		before := f.service.Snapshot()
		for _, tenant := range []string{"a", "b"} {
			for _, principal := range []string{"analyst", "reports"} {
				key := protectedQueryKey(tenant, principal, "old")
				id := f.submit(t, key, protectedQueryRequest(principal, false))
				f.waitState(t, tenant, id, Assigned)
				status := f.call(f.ctx, "GET", "/v1/queries/"+id, key, nil, false)
				if status.err != nil || status.code != http.StatusOK {
					t.Fatal("owned query status unavailable")
				}
				r := f.call(f.ctx, "GET", "/v1/queries/"+id+"/results", key, nil, false)
				protectedQuerySuccess(t, r)
				verifyProtectedQueryRows(t, r.data, tenant, principal)
				final := f.waitState(t, tenant, id, Succeeded)
				wantRows := int64(3)
				if principal == "reports" {
					wantRows = 1
				}
				if final.Job.Stats.Rows != wantRows || len(final.Job.Stats.Accelerations) != 2 {
					t.Fatal("durable filtered query statistics changed")
				}
				f.clean(t)
				id = f.submit(t, key, protectedQueryRequest(principal, true))
				r = f.call(f.ctx, "GET", "/v1/queries/"+id+"/results", key, nil, false)
				protectedQuerySuccess(t, r)
				verifyProtectedQueryAggregate(t, r.data, wantRows)
				f.waitState(t, tenant, id, Succeeded)
				f.clean(t)
			}
		}
		after := f.service.Snapshot()
		if after.Tenants["a"].RangeRequests <= before.Tenants["a"].RangeRequests || after.Tenants["b"].RangeRequests <= before.Tenants["b"].RangeRequests {
			t.Fatal("queries did not consume both real tenant providers")
		}
		if f.executions.Load() != 6 {
			t.Fatal("snapshot query reran a source refresh")
		}
	}) {
		return
	}
	if !t.Run("hidden-schema-and-foreign-handles", func(t *testing.T) {
		key := protectedQueryKey("a", "analyst", "old")
		id := f.submit(t, key, protectedQueryRequest("analyst", false))
		f.waitState(t, "a", id, Assigned)
		for _, foreign := range []string{protectedQueryKey("a", "reports", "old"), protectedQueryKey("b", "analyst", "old"), protectedQueryKey("b", "reports", "old")} {
			for _, route := range []struct{ method, suffix string }{{"GET", ""}, {"GET", "/results"}, {"POST", "/cancel"}} {
				r := f.call(f.ctx, route.method, "/v1/queries/"+id+route.suffix, foreign, nil, true)
				if r.err != nil || r.code != http.StatusNotFound {
					t.Fatal("foreign query handle disclosed", r.code, r.err)
				}
			}
		}
		r := f.call(f.ctx, "POST", "/v1/queries/"+id+"/cancel", key, nil, false)
		if r.err != nil || r.code != http.StatusOK {
			t.Fatal("owned cancellation failed", r.code, r.err)
		}
		f.waitState(t, "a", id, Cancelled)
		f.clean(t)
		for _, sql := range []string{"SELECT tenant_id FROM orders_single", "SELECT payload FROM orders_single", "SELECT * FROM hidden_orders"} {
			request := protectedQueryRequest("analyst", false)
			request.SQL = sql
			id := f.submit(t, key, request)
			r := f.call(f.ctx, "GET", "/v1/queries/"+id+"/results", key, nil, false)
			if bytes.HasSuffix(r.data, protectedQueryEOS) || (r.err == nil && r.code == http.StatusOK) {
				t.Fatal("hidden table or column produced successful results")
			}
			f.waitState(t, "a", id, Failed)
			f.clean(t)
		}
	}) {
		return
	}
	if !t.Run("forged-stale-and-unsupported-before-io", func(t *testing.T) {
		before := f.service.Snapshot().Total.Requests
		key := protectedQueryKey("a", "analyst", "old")
		for _, payload := range []any{
			map[string]any{"mode": "federated", "sources": []string{"orders_single"}, "sql": "SELECT * FROM orders_single", "authority": map[string]string{"principal_id": "reports"}},
			query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT 1"},
			query.Request{Mode: "federated", Sources: []string{"hidden_orders"}, SQL: "SELECT * FROM hidden_orders"},
		} {
			r := f.call(f.ctx, "POST", "/v1/queries", key, payload, true)
			if r.err != nil || (r.code != http.StatusBadRequest && r.code != http.StatusForbidden) {
				t.Fatal("unsupported or forged submission admitted", r.code, r.err)
			}
		}
		mixed := query.Request{Mode: "federated", Sources: []string{"orders_single", "direct"}, SQL: "SELECT * FROM orders_single JOIN direct ON true"}
		id := f.submit(t, key, mixed)
		r := f.call(f.ctx, "GET", "/v1/queries/"+id+"/results", key, nil, false)
		if r.err != nil || r.code != http.StatusServiceUnavailable || bytes.HasSuffix(r.data, protectedQueryEOS) {
			t.Fatal("restricted direct-reader mixture did not fail closed", r.code, r.err)
		}
		failed := f.waitState(t, "a", id, Failed)
		if failed.Job.Error == nil || failed.Job.Error.Code != "UNSUPPORTED" {
			t.Fatal("direct-reader refusal lost durable reason")
		}
		f.clean(t)
		// Corrupt only this disposable gateway account's KV record before enqueue.
		// Public API fields cannot express JobAuthority. This deliberately models
		// a forged/stale durable envelope, not public authority injection.
		for _, stale := range []bool{false, true} {
			x := f.tenants["a"]
			authority, _ := authorityForPrincipal(x.policy, "analyst")
			if stale {
				authority.PolicyVersion = strings.Repeat("0", 64)
			} else {
				authority.PrincipalKind = "service"
			}
			id, err := newID(7)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			job := Job{ID: id, TenantID: "a", State: Queued, Authority: &authority, Request: protectedQueryRequest("analyst", false), CreatedAt: now, ExpiresAt: now.Add(time.Minute), HeartbeatAt: now}
			raw, err := encodePersistedJob(&job)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := x.store.kv.Get(f.ctx, "slot.7")
			if err == nil {
				_, err = x.store.kv.Update(f.ctx, "slot.7", raw, entry.Revision())
			} else {
				_, err = x.store.kv.Create(f.ctx, "slot.7", raw)
			}
			if err != nil {
				t.Fatal("inject disposable forged envelope", err)
			}
			if err := x.store.Enqueue(f.ctx, id); err != nil {
				t.Fatal(err)
			}
			final := f.waitState(t, "a", id, Failed)
			if final.Job.Error == nil || final.Job.Error.Code != "PERMISSION_DENIED" || final.Job.WorkerID != "" {
				t.Fatal("forged durable envelope reached worker assignment")
			}
			f.clean(t)
		}
		if f.service.Snapshot().Total.Requests != before || f.executions.Load() != 6 {
			t.Fatal("refusal performed source/provider I/O")
		}
	}) {
		return
	}
	for _, mode := range []struct {
		name, tenant string
		length       bool
	}{{"none-chunked", "a", false}, {"none-length", "a", true}, {"lz4-chunked", "b", false}, {"lz4-length", "b", true}} {
		if !t.Run("revocation-"+mode.name, func(t *testing.T) {
			x := f.tenants[mode.tenant]
			x.length.Store(mode.length)
			old, newKey := protectedQueryKey(mode.tenant, "analyst", "old"), protectedQueryKey(mode.tenant, "analyst", "new")
			f.keys[mode.tenant]["analyst"] = []string{old, newKey}
			if !f.gateway.auth.apply(f.writeKeys(t), time.Now()) {
				t.Fatal("restore fixture key revision")
			}
			id := f.submit(t, old, protectedQueryRequest("analyst", false))
			f.keys[mode.tenant]["analyst"] = []string{newKey}
			revoke := f.prepareKeyChange(t)
			var revoked atomic.Bool
			x.store.mu.Lock()
			x.store.afterSuccess = func() { revoked.Store(revoke()) }
			x.store.mu.Unlock()
			beforeFrames := x.framed.Load()
			r := f.call(f.ctx, "GET", "/v1/queries/"+id+"/results", old, nil, false)
			if !revoked.Load() || x.framed.Load() != beforeFrames+1 || bytes.HasSuffix(r.data, protectedQueryEOS) || (r.err == nil && r.code == http.StatusOK) {
				t.Fatal("revocation released successful EOS or missed real framing/CAS barrier", r.code, r.err)
			}
			x.wireMu.Lock()
			produced := bytes.Clone(x.lastWire)
			x.wireMu.Unlock()
			if !bytes.HasSuffix(produced, protectedQueryEOS) {
				t.Fatal("revocation fixture did not receive a complete real worker stream")
			}
			verifyProtectedQueryRows(t, produced, mode.tenant, "analyst")
			// CAS already succeeded. Local final checks cannot make revocation
			// atomic with durable publication or recall bytes delivered earlier.
			f.waitState(t, mode.tenant, id, Succeeded)
			if r := f.call(f.ctx, "GET", "/v1/queries/"+id, old, nil, false); r.err != nil || r.code != http.StatusUnauthorized {
				t.Fatal("revoked key still active")
			}
			if r := f.call(f.ctx, "GET", "/v1/queries/"+id, newKey, nil, false); r.err != nil || r.code != http.StatusOK {
				t.Fatal("same-principal replacement key lost owned handle")
			}
			other := "a"
			if mode.tenant == "a" {
				other = "b"
			}
			for _, owner := range []string{mode.tenant, other} {
				ownerKey := protectedQueryKey(owner, "reports", "old")
				foreignID := f.submit(t, ownerKey, protectedQueryRequest("reports", false))
				for _, route := range []struct{ method, suffix string }{{"GET", ""}, {"GET", "/results"}, {"POST", "/cancel"}} {
					r := f.call(f.ctx, route.method, "/v1/queries/"+foreignID+route.suffix, newKey, nil, true)
					if r.err != nil || r.code != http.StatusNotFound {
						t.Fatal("replacement key acquired foreign principal or tenant handle")
					}
				}
				r = f.call(f.ctx, "POST", "/v1/queries/"+foreignID+"/cancel", ownerKey, nil, false)
				if r.err != nil || r.code != http.StatusOK {
					t.Fatal("cancel foreign fixture job")
				}
				f.clean(t)
			}
			x.length.Store(false)
		}) {
			return
		}
	}
	if !t.Run("cancelled-range-releases-custody", func(t *testing.T) {
		key := protectedQueryKey("a", "analyst", "new")
		gate := f.service.Hold(protectedobject.RangeFinalByte, "a", "orders_single")
		defer gate.Release()
		id := f.submit(t, key, protectedQueryRequest("analyst", false))
		done := make(chan protectedQueryResponse, 1)
		go func() { done <- f.call(f.ctx, "GET", "/v1/queries/"+id+"/results", key, nil, false) }()
		protectedQueryAwait(t, f.ctx, gate.Entered(), "real range body")
		if f.pool.Snapshot().Active != 1 || f.containment.Status().Active != 1 || f.service.Readers("a") == 0 {
			t.Fatal("range body lacked actual worker and pin custody")
		}
		r := f.call(f.ctx, "POST", "/v1/queries/"+id+"/cancel", key, nil, false)
		if r.err != nil || r.code != http.StatusOK {
			t.Fatal("active cancellation failed", r.code, r.err)
		}
		gate.Release()
		select {
		case r := <-done:
			if bytes.HasSuffix(r.data, protectedQueryEOS) || (r.err == nil && r.code == http.StatusOK) {
				t.Fatal("cancelled range succeeded")
			}
		case <-f.ctx.Done():
			t.Fatal("cancelled query did not return")
		}
		f.waitState(t, "a", id, Cancelled)
		f.clean(t)
	}) {
		return
	}
	if !t.Run("stalled-registry-retains-custody", func(t *testing.T) {
		x := f.tenants["b"]
		key := protectedQueryKey("b", "analyst", "new")
		gate := f.service.Hold(protectedobject.PinReleaseBeforeCAS, "b", "orders_single")
		defer gate.Release()
		id := f.submit(t, key, protectedQueryRequest("analyst", false))
		done := make(chan protectedQueryResponse, 1)
		go func() { done <- f.call(f.ctx, "GET", "/v1/queries/"+id+"/results", key, nil, false) }()
		protectedQueryAwait(t, f.ctx, gate.Entered(), "real reader-release CAS")
		if f.pool.Snapshot().Active != 1 || f.service.Readers("b") == 0 {
			t.Fatal("stalled registry released actual query custody")
		}
		x.node.mu.Lock()
		jobs := len(x.node.jobs)
		x.node.mu.Unlock()
		if jobs != 1 {
			t.Fatal("node handed back stalled query")
		}
		short, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err := x.node.Drain(short)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) || f.pool.Snapshot().Active != 1 {
			t.Fatal("bounded drain lost stalled cleanup custody", err)
		}
		// This holds registry CAS completion, not the local HTTP Body.Close.
		// Native child/scratch can finish before the parent's durable pin release.
		gate.Release()
		select {
		case r := <-done:
			protectedQuerySuccess(t, r)
			verifyProtectedQueryRows(t, r.data, "b", "analyst")
		case <-f.ctx.Done():
			t.Fatal("released registry did not return")
		}
		f.waitState(t, "b", id, Succeeded)
		f.clean(t)
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := x.runtime.Close(cleanup); err != nil {
			t.Fatal("runtime shutdown after handback", err)
		}
		select {
		case <-x.runtime.Quiesced():
		case <-cleanup.Done():
			t.Fatal("runtime retained local work")
		}
	}) {
		return
	}
	f.clean(t)
	t.Log("KELVO_PROTECTED_QUERY_CHILD_PASS")
}

type protectedQueryOutput struct {
	bytes.Buffer
	truncated bool
}

func (b *protectedQueryOutput) Write(data []byte) (int, error) {
	n := len(data)
	left := (256 << 10) - b.Len()
	if n > left {
		b.truncated = true
	}
	if left > 0 {
		_, _ = b.Buffer.Write(data[:min(n, left)])
	}
	return n, nil
}

func protectedQueryAwait(t *testing.T, ctx context.Context, ch <-chan struct{}, name string) {
	t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatal("barrier not reached", name)
	case <-ctx.Done():
		t.Fatal("fixture ended before barrier", name)
	}
}

func protectedQueryRequest(principal string, aggregate bool) query.Request {
	columns := "s.id,s.amount,s.observed,s.tiny,s.label"
	if principal == "reports" {
		columns = "s.id,s.tiny,s.label"
	}
	sql := "WITH m AS (SELECT * FROM orders_multi) SELECT " + columns + " FROM orders_single s JOIN m ON s.id=m.id ORDER BY s.id"
	if aggregate {
		sql = "WITH m AS (SELECT * FROM orders_multi), joined AS (SELECT s.id,s.tiny FROM orders_single s JOIN m ON s.id=m.id) SELECT CAST(count(*) AS BIGINT) AS n,CAST(sum(tiny) AS BIGINT) AS total FROM joined"
	}
	return query.Request{Mode: "federated", Sources: []string{"orders_single", "orders_multi"}, SQL: sql}
}

func protectedQuerySuccess(t *testing.T, r protectedQueryResponse) {
	t.Helper()
	if r.err != nil || r.code != http.StatusOK || r.header.Get("Kelvo-Result-Completion") != "durable-eos-v1" || r.header.Get("Content-Type") != "application/vnd.apache.arrow.stream" || !bytes.HasSuffix(r.data, protectedQueryEOS) {
		t.Fatalf("incomplete protected query: status=%d error=%v", r.code, r.err)
	}
}

func protectedQueryReader(t *testing.T, data []byte) *ipc.Reader {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Release)
	if reader.Schema().Metadata().Len() != 0 {
		t.Fatal("schema metadata escaped policy boundary")
	}
	for _, field := range reader.Schema().Fields() {
		if field.Metadata.Len() != 0 {
			t.Fatal("field metadata escaped policy boundary")
		}
	}
	return reader
}

func verifyProtectedQueryRows(t *testing.T, data []byte, tenant, principal string) {
	t.Helper()
	reader := protectedQueryReader(t, data)
	names := []string{"id", "amount", "observed", "tiny", "label"}
	if principal == "reports" {
		names = []string{"id", "tiny", "label"}
	}
	if reader.Schema().NumFields() != len(names) {
		t.Fatal("hidden columns escaped")
	}
	for i, name := range names {
		if reader.Schema().Field(i).Name != name {
			t.Fatal("unexpected public field")
		}
	}
	type value struct {
		id           uint64
		amount       decimal128.Num
		amountNull   bool
		observed     arrow.Timestamp
		observedNull bool
		tiny         int8
		label        string
	}
	var values []value
	for reader.Next() {
		batch := reader.RecordBatch()
		ids, ok := batch.Column(0).(*array.Uint64)
		if !ok {
			t.Fatal("uint64 result narrowed")
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			v := value{id: ids.Value(i)}
			if principal == "analyst" {
				amount, ok := batch.Column(1).(*array.Decimal128)
				if !ok || !arrow.TypeEqual(amount.DataType(), &arrow.Decimal128Type{Precision: 30, Scale: 4}) {
					t.Fatal("decimal type changed")
				}
				observed, ok := batch.Column(2).(*array.Timestamp)
				if !ok || observed.DataType().(*arrow.TimestampType).Unit != arrow.Microsecond {
					t.Fatal("timestamp unit changed")
				}
				zone := observed.DataType().(*arrow.TimestampType).TimeZone
				if zone != "UTC" && zone != "Etc/UTC" {
					t.Fatal("timestamp lost UTC semantics")
				}
				v.amountNull, v.observedNull = amount.IsNull(i), observed.IsNull(i)
				if !v.amountNull {
					v.amount = amount.Value(i)
				}
				if !v.observedNull {
					v.observed = observed.Value(i)
				}
				v.tiny = batch.Column(3).(*array.Int8).Value(i)
				v.label = batch.Column(4).(*array.String).Value(i)
			} else {
				v.tiny = batch.Column(1).(*array.Int8).Value(i)
				v.label = batch.Column(2).(*array.String).Value(i)
			}
			values = append(values, v)
		}
	}
	want := []value{{1, decimal128.FromI64(-123456789), false, -315521754876544, false, -128, tenant}, {4, decimal128.FromI64(1), false, 0, true, 1, tenant}, {^uint64(0), decimal128.Num{}, true, 123456789, false, 127, tenant}}
	if principal == "reports" {
		want = []value{{id: 2, tiny: 0, label: tenant}}
	}
	if reader.Err() != nil || !reflect.DeepEqual(values, want) {
		t.Fatal("filtered exact CTE join values changed", reader.Err(), fmt.Sprint(values))
	}
}

func verifyProtectedQueryAggregate(t *testing.T, data []byte, want int64) {
	t.Helper()
	reader := protectedQueryReader(t, data)
	if reader.Schema().NumFields() != 2 {
		t.Fatal("aggregate schema changed")
	}
	rows := 0
	for reader.Next() {
		b := reader.RecordBatch()
		for i := 0; i < int(b.NumRows()); i++ {
			rows++
			if b.Column(0).(*array.Int64).Value(i) != want || b.Column(1).(*array.Int64).Value(i) != 0 {
				t.Fatal("aggregate included unauthorized rows")
			}
		}
	}
	if reader.Err() != nil || rows != 1 {
		t.Fatal("aggregate incomplete")
	}
}
