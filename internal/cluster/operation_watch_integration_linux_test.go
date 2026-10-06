//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func runOperationWatchLive(t *testing.T, h operationIngestionLive, engine string) {
	t.Helper()
	schema := "kelvo_ingestion_fixture"
	if engine == "mysql" {
		schema = h.database
	} else if engine != "postgres" {
		t.Fatal("unsupported watcher fixture engine")
	}
	scope := watch.Scope{TeamID: "customer-a", ConnectionID: engine + "-fixture", Database: h.database, Schema: schema, Table: "watch_source", ID: "watcher-fixture", Generation: "generation-1"}
	connection := operations.ConnectionRef{ID: scope.ConnectionID, Database: scope.Database, Schema: scope.Schema}
	sequence := 0
	run := func(r operations.Request) operations.Response {
		t.Helper()
		grant := h.sign(r, scope.TeamID)
		return h.await(h.submit(r, grant).ID, grant, r)
	}
	request := func(kind operations.Kind) operations.Request {
		sequence++
		r := operations.Request{Version: operations.Version, Kind: kind, Connection: connection, Spec: operations.Spec{Watch: &operations.WatchSpec{ID: scope.ID, Generation: scope.Generation, Target: operations.ObjectRef{Schema: scope.Schema, Name: scope.Table}, Mode: "native"}}}
		if kind.Mutating() {
			r.IdempotencyKey = fmt.Sprintf("watch-%d", sequence)
		}
		if kind == operations.WatchRead {
			r.Spec.Watch.MaxEvents = 100
			r.Spec.Watch.MaxWaitMS = 1
		}
		return r
	}
	complete := func(result operations.Response) {
		t.Helper()
		if result.Receipt == nil || result.Receipt.Outcome != operations.Completed {
			t.Fatalf("watch operation did not complete: %#v", result.Receipt)
		}
	}
	write := func(sql string) {
		t.Helper()
		sequence++
		r := operations.Request{Version: operations.Version, Kind: operations.StatementExecute, Connection: connection, IdempotencyKey: fmt.Sprintf("watch-source-%d", sequence), Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: sql, Transaction: operations.TransactionRequired}}}
		complete(run(r))
	}
	read := func() watch.Batch {
		t.Helper()
		r := request(operations.WatchRead)
		grant := h.sign(r, scope.TeamID)
		result := h.await(h.submit(r, grant).ID, grant, r)
		complete(result)
		if result.Receipt.Result == nil {
			t.Fatal("missing watcher Arrow result")
		}
		status, raw, err := h.call(http.MethodGet, "/v1/operations/"+result.ID+"/results", grant, nil)
		sum := sha256.Sum256(raw)
		if err != nil || status != 200 || hex.EncodeToString(sum[:]) != result.Receipt.Result.SHA256 {
			t.Fatal("watch result custody failed", status, err)
		}
		reader, err := ipc.NewReader(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Release()
		if !reader.Next() || reader.RecordBatch().NumCols() != 1 || reader.RecordBatch().NumRows() != 1 || reader.Schema().Field(0).Name != "watch" {
			t.Fatal("invalid watch Arrow schema")
		}
		column, ok := reader.RecordBatch().Column(0).(*array.Binary)
		if !ok || column.IsNull(0) {
			t.Fatal("invalid watch result type")
		}
		batch, err := watch.ParseBatch(column.Value(0), scope)
		if err != nil {
			t.Fatal(err)
		}
		if reader.Next() || reader.Err() != nil {
			t.Fatal("watch result has trailing batch")
		}
		return batch
	}
	ack := func(checkpoint watch.Checkpoint) operations.Response {
		t.Helper()
		raw, err := json.Marshal(checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		ref := h.upload(t, scope.ConnectionID, "watch_checkpoint_v1", raw)
		r := request(operations.WatchAck)
		r.Spec.Watch.Checkpoint = &ref
		r.Spec.Watch.SinkReceiptSHA256 = strings.Repeat("d", 64)
		return run(r)
	}
	complete(run(request(operations.WatchInstall)))
	complete(run(request(operations.WatchInstall))) // Same generation retains its existing objects.
	write(`INSERT INTO watch_source VALUES(1,12345678901234567890.123)`)
	write(`UPDATE watch_source SET amount=2.001 WHERE id=1`)
	write(`DELETE FROM watch_source WHERE id=1`)
	first := read()
	if len(first.Events) != 3 || first.Events[0].Operation != "INSERT" || first.Events[1].Operation != "UPDATE" || first.Events[2].Operation != "DELETE" || !bytes.Contains(first.Events[0].Data, []byte("12345678901234567890.123")) {
		t.Fatal("watch events lost precision or order")
	}
	again := read()
	firstRaw, _ := json.Marshal(first)
	againRaw, _ := json.Marshal(again)
	if !bytes.Equal(firstRaw, againRaw) {
		t.Fatal("read acknowledged or changed events")
	}
	changed := *first.Checkpoint
	changed.Entries = append([]watch.Entry(nil), changed.Entries...)
	changed.Entries[1].SHA256 = strings.Repeat("e", 64)
	failed := ack(changed)
	if failed.Receipt == nil || failed.Receipt.ErrorCode != "CONFLICT" || failed.Receipt.Effect != operations.EffectNone {
		t.Fatal("changed checkpoint acknowledged", failed.State)
	}
	if len(read().Events) != 3 {
		t.Fatal("failed acknowledgement partially removed events")
	}
	one := *first.Checkpoint
	one.Entries = one.Entries[:1]
	complete(ack(one))
	if len(read().Events) != 2 {
		t.Fatal("ack removed unlisted events")
	}
	complete(ack(one))
	complete(ack(*first.Checkpoint))
	if len(read().Events) != 0 {
		t.Fatal("ack did not drain exact events")
	}
	oldAck := *first.Checkpoint
	complete(run(request(operations.WatchRemove)))
	scope.Generation = "generation-2"
	complete(run(request(operations.WatchInstall)))
	write(`INSERT INTO watch_source VALUES(2,3.001)`)
	scope.Generation = "generation-1"
	stale := run(request(operations.WatchInstall))
	if stale.Receipt == nil || stale.Receipt.ErrorCode != "CONFLICT" {
		t.Fatal("retired generation reinstalled")
	}
	stale = ack(oldAck)
	if stale.Receipt == nil || stale.Receipt.ErrorCode != "CONFLICT" {
		t.Fatal("retired generation acknowledged replacement")
	}
	complete(run(request(operations.WatchRemove))) // Retired cleanup cannot remove a newer generation.
	scope.Generation = "generation-2"
	if len(read().Events) != 1 {
		t.Fatal("stale cleanup removed replacement watcher")
	}
	complete(run(request(operations.WatchRemove)))
}
