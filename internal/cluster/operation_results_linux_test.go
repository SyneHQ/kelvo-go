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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func operationResultFixture(t *testing.T) (operationHTTPFixture, *Node, ledger.Record, []byte) {
	t.Helper()
	f := operationWorkerFixture(t)
	record := operationAssigned(t, f)
	if _, err := f.state.store.Start(f.context, record.Scope, record.ID, record.Binding); err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.NewGoAllocator())
	builder.Append(42)
	column := builder.NewArray()
	batch := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
	writer := ipc.NewWriter(&raw, ipc.WithSchema(schema))
	if err := writer.Write(batch); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	batch.Release()
	column.Release()
	builder.Release()
	store, err := operationinput.Open(operationinput.Config{MaxInputBytes: 1 << 20, Storage: exports.Config{Directory: filepath.Join(t.TempDir(), "results"), Tenant: "team-a", MaxEntries: 8, MaxStoredBytes: 16 << 20, MaxTTL: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	ref, err := store.Put(f.context, operationInputIdentity(record.Scope, record.AuthoritySHA256), record.RetainUntil, operationinput.ArrowIPC, bytes.NewReader(raw.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	receipt := operations.Receipt{Version: 1, OperationID: record.ID, RequestSHA256: record.RequestSHA256, Outcome: operations.Completed, Effect: operations.EffectCommitted,
		Result: &operations.ResultRef{ID: ref.ID, SHA256: ref.SHA256, Bytes: ref.Bytes, Rows: 1, Format: "arrow_ipc"}}
	completed, err := f.state.store.Complete(f.context, record.Scope, record.ID, record.Binding, receipt)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := admission.New(admission.Limits{MaxConcurrent: 2, MemoryBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	node := &Node{ctx: f.context, cfg: NodeConfig{Policy: f.policy, WorkerID: "worker-a", Operations: &OperationNodeConfig{MaxResultBytes: 1 << 20}}, audit: operationJournal(t, 4),
		operations: &nodeOperations{ledger: f.state.store, results: store, executor: &worker.Executor{ResourcePool: pool}, downloads: make(chan struct{}, 2)}}
	return f, node, completed.Record, raw.Bytes()
}

func TestOperationResultDownloadIsRepeatableAndTenantBound(t *testing.T) {
	f, node, record, payload := operationResultFixture(t)
	for range 2 {
		r := httptest.NewRequest(http.MethodGet, "/internal/operations/"+record.ID+"/results", nil)
		r.Header.Set(operationGrantHeader, f.grant)
		r.Header.Set(operationPrincipalHeader, "api")
		w := httptest.NewRecorder()
		if !node.serveOperationResult(w, r) || w.Code != 200 || !bytes.Equal(w.Body.Bytes(), payload) {
			t.Fatal("retained download failed", w.Code, w.Body.Len())
		}
	}
	claims := f.claims
	claims.AppTeam = "other-team"
	grant, err := operations.SignGrant(claims, f.key)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "/internal/operations/"+record.ID+"/results", nil)
	r.Header.Set(operationGrantHeader, grant)
	r.Header.Set(operationPrincipalHeader, "api")
	w := httptest.NewRecorder()
	node.serveOperationResult(w, r)
	if w.Code != 404 || bytes.Contains(w.Body.Bytes(), payload) {
		t.Fatal("foreign team read results", w.Code)
	}
	if got := node.operations.executor.ResourcePool.Snapshot(); got.Active != 0 {
		t.Fatal("download reservation leaked")
	}
}

func TestOperationResultProxyVerifiesDigestBeforeEOS(t *testing.T) {
	for _, mode := range []string{"valid", "changed", "trailing", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			f, _, record, payload := operationResultFixture(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(operationPrincipalHeader) != "api" || r.Header.Get(operationGrantHeader) != f.grant {
					t.Error("missing bound request authority")
				}
				w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
				data := bytes.Clone(payload)
				switch mode {
				case "changed":
					data[10] ^= 1
				case "trailing":
					data = append(data, 'x')
				case "truncated":
					data = data[:len(data)-1]
				}
				_, _ = w.Write(data)
			}))
			defer server.Close()
			u, _ := url.Parse(server.URL)
			f.g.tenants["team-a"] = gatewayTenant{store: f.cluster, workers: map[string]workerEndpoint{"worker-a": {url: u, client: server.Client()}}}
			r := httptest.NewRequest(http.MethodGet, "/v1/operations/"+record.ID+"/results", nil).WithContext(f.context)
			r.Header.Set(operationGrantHeader, f.grant)
			w := httptest.NewRecorder()
			aborted := false
			func() {
				defer func() {
					if got := recover(); got != nil {
						if got != http.ErrAbortHandler {
							panic(got)
						}
						aborted = true
					}
				}()
				f.g.operationResults(w, r, f.state, f.claims, record)
			}()
			if mode == "valid" && (aborted || !bytes.Equal(w.Body.Bytes(), payload)) {
				t.Fatal("valid proxy failed")
			}
			// Known Content-Length mismatches are rejected before response bytes;
			// bad same-length bodies abort before the final Arrow EOS is released.
			if mode != "valid" && !aborted && w.Code == http.StatusOK {
				t.Fatal("invalid result completed")
			}
			if mode == "changed" && bytes.HasSuffix(w.Body.Bytes(), payload[len(payload)-8:]) {
				t.Fatal("corrupt stream released EOS")
			}
		})
	}
}

func TestOperationResultFailurePreservesCommittedWrite(t *testing.T) {
	for _, mutating := range []bool{false, true} {
		sink := &operationResultSink{mutating: mutating}
		receipt := operations.Receipt{Version: 1, OperationID: "op-1", RequestSHA256: strings.Repeat("a", 64), Outcome: operations.Completed, Effect: operations.EffectNone}
		if mutating {
			receipt.Effect = operations.EffectCommitted
		}
		got, err := sink.FinalizeOperation(receipt, errors.New("delivery failed"))
		if err == nil || got.Validate() != nil {
			t.Fatal("invalid failure receipt", err)
		}
		if mutating && (got.Outcome != operations.Completed || got.Effect != operations.EffectCommitted) {
			t.Fatal("confirmed write was downgraded")
		}
		if !mutating && (got.Outcome != operations.Failed || got.Result != nil) {
			t.Fatal("failed read appeared complete")
		}
	}
}

func TestOperationResultSinkRetainsExactArrowAfterCommit(t *testing.T) {
	f, node, record, _ := operationResultFixture(t)
	stream, err := node.operations.results.BeginStream(context.Background(), operationInputIdentity(record.Scope, record.AuthoritySHA256), record.RetainUntil, operationinput.ArrowIPC)
	if err != nil {
		t.Fatal(err)
	}
	sink := &operationResultSink{stream: stream, inner: worker.NewIPCSink(stream, query.DefaultLimits())}
	if err := sink.Schema(arrow.NewSchema([]arrow.Field{{Name: "empty", Type: arrow.BinaryTypes.String}}, nil)); err != nil {
		t.Fatal(err)
	}
	receipt, err := sink.FinalizeOperation(operations.Receipt{Version: 1, OperationID: record.ID, RequestSHA256: record.RequestSHA256, Outcome: operations.Completed, Effect: operations.EffectNone}, nil)
	if err != nil || receipt.Validate() != nil || receipt.Result == nil || receipt.Result.Rows != 0 {
		t.Fatal("empty Arrow result was lost", receipt, err)
	}
	ref := receipt.Result
	payload, err := node.operations.results.Load(f.context, operationInputIdentity(record.Scope, record.AuthoritySHA256), operations.InputRef{ID: ref.ID, SHA256: ref.SHA256, Bytes: ref.Bytes, Format: ref.Format})
	if err != nil {
		t.Fatal(err)
	}
	r, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if r.Next() || r.Err() != nil || r.Schema().NumFields() != 1 {
		t.Fatal("empty retained Arrow invalid")
	}
}
