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
	"strconv"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func runOperationMigrationLive(t *testing.T, h operationIngestionLive, engine string) {
	t.Helper()
	connection := operations.ConnectionRef{ID: engine + "-fixture", Database: h.database, Schema: "kelvo_migration_fixture"}
	if engine == "mysql" {
		connection.Schema = h.database
	}
	sequence := 0
	run := func(r operations.Request) (operations.Response, string) {
		t.Helper()
		grant := h.sign(r, "customer-a")
		return h.await(h.submit(r, grant).ID, grant, r), grant
	}
	result := func(response operations.Response, grant string, destination any) {
		t.Helper()
		if response.Receipt == nil || response.Receipt.Outcome != operations.Completed || response.Receipt.Result == nil {
			t.Fatalf("migration failed: %#v", response.Receipt)
		}
		status, raw, err := h.call(http.MethodGet, "/v1/operations/"+response.ID+"/results", grant, nil)
		sum := sha256.Sum256(raw)
		if err != nil || status != 200 || hex.EncodeToString(sum[:]) != response.Receipt.Result.SHA256 {
			t.Fatal("migration result custody", status, err)
		}
		reader, err := ipc.NewReader(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Release()
		if !reader.Next() || reader.RecordBatch().NumRows() != 1 || reader.RecordBatch().NumCols() != 1 || reader.Schema().Field(0).Name != "migration" {
			t.Fatal("invalid migration result")
		}
		column, ok := reader.RecordBatch().Column(0).(*array.Binary)
		if !ok || column.IsNull(0) || json.Unmarshal(column.Value(0), destination) != nil || reader.Next() || reader.Err() != nil {
			t.Fatal("invalid migration result rows")
		}
	}
	state := func() migration.State {
		t.Helper()
		r := operations.Request{Version: operations.Version, Kind: operations.MigrationStatus, Connection: connection, Spec: operations.Spec{Migration: &operations.MigrationSpec{ID: migration.Table}}}
		response, grant := run(r)
		var s migration.State
		result(response, grant, &s)
		if response.Receipt.Effect != operations.EffectNone || s.Validate() != nil {
			t.Fatal("status changed source")
		}
		return s
	}
	apply := func(plan migration.Plan) (operations.Response, string, operations.Request) {
		t.Helper()
		sequence++
		raw, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		ref := h.upload(t, connection.ID, "migration_plan_v1", raw)
		r := operations.Request{Version: operations.Version, Kind: operations.MigrationApply, Connection: connection, IdempotencyKey: fmt.Sprintf("migration-%d", sequence), Spec: operations.Spec{Migration: &operations.MigrationSpec{ID: migration.Table, ExpectedVersion: strconv.FormatInt(plan.Expected.Version, 10), Transaction: operations.TransactionAutocommit, Plan: ref}}}
		response, grant := run(r)
		return response, grant, r
	}
	if got := state(); got != (migration.State{Version: -1}) {
		t.Fatal("fresh status", got)
	}
	// A status read must not create the history table.
	query := operations.Request{Version: operations.Version, Kind: operations.QueryRead, Connection: connection, Spec: operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT COUNT(*) AS tables FROM information_schema.tables WHERE table_schema='" + strings.ReplaceAll(connection.Schema, "'", "''") + "' AND table_name='schema_migrations'"}}}
	queryResponse, queryGrant := run(query)
	status, raw, err := h.call(http.MethodGet, "/v1/operations/"+queryResponse.ID+"/results", queryGrant, nil)
	if err != nil || status != 200 {
		t.Fatal("status initialization check failed")
	}
	reader, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !reader.Next() {
		reader.Release()
		t.Fatal("missing status initialization check")
	}
	column, ok := reader.RecordBatch().Column(0).(*array.Int64)
	if !ok || column.Value(0) != 0 {
		reader.Release()
		t.Fatal("status initialized migration table")
	}
	reader.Release()
	files := []migration.File{{Name: "1_orders.up.sql", Content: "CREATE TABLE orders(id bigint PRIMARY KEY, amount numeric(30,3)); INSERT INTO orders VALUES(1,12345678901234567890.123);"}, {Name: "1_orders.down.sql", Content: "DROP TABLE orders;"}, {Name: "2_region.up.sql", Content: "ALTER TABLE orders ADD COLUMN region varchar(64) DEFAULT 'north';"}, {Name: "2_region.down.sql", Content: "ALTER TABLE orders DROP COLUMN region;"}}
	plan := migration.Plan{Version: migration.Version, Expected: migration.State{Version: -1}, Direction: "up", Files: files}
	response, grant, request := apply(plan)
	var applied migration.Result
	result(response, grant, &applied)
	if applied.To != (migration.State{Version: 2}) || len(applied.FilesApplied) != 2 || applied.Effect != operations.EffectCommitted {
		t.Fatal("up migration", applied)
	}
	if duplicate := h.submit(request, grant); duplicate.ID != response.ID {
		t.Fatal("duplicate migration acquired a second operation")
	}
	stale, _, _ := apply(plan)
	if stale.Receipt == nil || stale.Receipt.ErrorCode != "CONFLICT" || stale.Receipt.Effect != operations.EffectNone {
		t.Fatal("stale plan executed", stale.Receipt)
	}
	plan.Expected = migration.State{Version: 2}
	plan.Direction = "down"
	plan.Steps = 1
	response, grant, _ = apply(plan)
	result(response, grant, &applied)
	if applied.To.Version != 1 || len(applied.FilesApplied) != 1 || applied.FilesApplied[0] != "2_region.down.sql" {
		t.Fatal("down migration", applied)
	}
	broken := migration.Plan{Version: migration.Version, Expected: migration.State{Version: 1}, Direction: "up", Files: []migration.File{files[0], {Name: "2_broken.up.sql", Content: "INSERT INTO nonexistent_migration_table VALUES(1);"}}}
	failed, _, _ := apply(broken)
	if failed.Receipt == nil || failed.Receipt.Outcome != operations.OutcomeUnknown {
		t.Fatal("failed script claimed rollback", failed.Receipt)
	}
	if got := state(); got != (migration.State{Version: 2, Dirty: true}) {
		t.Fatal("dirty version missing", got)
	}
	force := int64(2)
	repair := migration.Plan{Version: migration.Version, Expected: migration.State{Version: 2, Dirty: true}, Direction: "force", ForceVersion: &force}
	response, grant, _ = apply(repair)
	result(response, grant, &applied)
	openSQL := "BEGIN; CREATE TABLE kelvo_open_tx_probe(id bigint);"
	if engine == "mysql" {
		openSQL = "START TRANSACTION; INSERT INTO orders(id,amount) VALUES(2,1.000);"
	}
	openTransaction := migration.Plan{Version: migration.Version, Expected: migration.State{Version: 2}, Direction: "up", Files: []migration.File{{Name: "2_current.up.sql", Content: "SELECT 1;"}, {Name: "3_open.up.sql", Content: openSQL}}}
	failed, _, _ = apply(openTransaction)
	if failed.Receipt == nil || failed.Receipt.Outcome != operations.OutcomeUnknown {
		t.Fatal("open transaction was reported as committed", failed.Receipt)
	}
	if got := state(); got != (migration.State{Version: 3, Dirty: true}) {
		t.Fatal("open transaction cleared durable dirty state", got)
	}
	query.Spec.Query.SQL = "SELECT COUNT(*) AS tables FROM information_schema.tables WHERE table_schema='kelvo_migration_fixture' AND table_name='kelvo_open_tx_probe'"
	if engine == "mysql" {
		query.Spec.Query.SQL = "SELECT COUNT(*) AS total FROM orders WHERE id=2"
	}
	queryResponse, queryGrant = run(query)
	status, raw, err = h.call(http.MethodGet, "/v1/operations/"+queryResponse.ID+"/results", queryGrant, nil)
	if err != nil || status != 200 {
		t.Fatal("open transaction cleanup check failed")
	}
	reader, err = ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !reader.Next() {
		reader.Release()
		t.Fatal("missing open transaction cleanup check")
	}
	column, ok = reader.RecordBatch().Column(0).(*array.Int64)
	if !ok || column.Value(0) != 0 {
		reader.Release()
		t.Fatal("uncommitted migration table survived session cleanup")
	}
	reader.Release()
	force = 3
	repair.Expected = migration.State{Version: 3, Dirty: true}
	response, grant, _ = apply(repair)
	result(response, grant, &applied)
	unlockSQL := "SELECT pg_advisory_unlock_all();"
	if engine == "mysql" {
		unlockSQL = "SELECT RELEASE_ALL_LOCKS();"
	}
	lostLock := migration.Plan{Version: migration.Version, Expected: migration.State{Version: 3}, Direction: "up", Files: []migration.File{{Name: "3_current.up.sql", Content: "SELECT 1;"}, {Name: "4_unlock.up.sql", Content: unlockSQL}}}
	failed, _, _ = apply(lostLock)
	if failed.Receipt == nil || failed.Receipt.Outcome != operations.OutcomeUnknown {
		t.Fatal("migration released custody and still reported success", failed.Receipt)
	}
	if got := state(); got != (migration.State{Version: 4, Dirty: true}) {
		t.Fatal("lost migration lock cleared dirty history", got)
	}
	force = 1
	repair.Expected = migration.State{Version: 4, Dirty: true}
	response, grant, _ = apply(repair)
	result(response, grant, &applied)
	if applied.To != (migration.State{Version: 1}) || len(applied.FilesApplied) != 0 {
		t.Fatal("force executed scripts", applied)
	}
	plan.Expected = migration.State{Version: 1}
	plan.Steps = 0
	response, grant, _ = apply(plan)
	result(response, grant, &applied)
	if applied.To != (migration.State{Version: -1}) {
		t.Fatal("final down migration", applied)
	}
	if engine == "mysql" {
		return
	}
	// A status view can contain a volatile function. The adapter must enter a
	// read-only source transaction before asking that view for its version.
	statusConnection := connection
	statusConnection.Schema = "kelvo_migration_status_fixture"
	statusRequest := operations.Request{Version: operations.Version, Kind: operations.MigrationStatus, Connection: statusConnection, Spec: operations.Spec{Migration: &operations.MigrationSpec{ID: migration.Table}}}
	failed, _ = run(statusRequest)
	if failed.Receipt == nil || failed.Receipt.Outcome != operations.Failed || failed.Receipt.Effect != operations.EffectNone {
		t.Fatal("write-capable migration status view was accepted", failed.Receipt)
	}
	query.Connection = statusConnection
	query.Spec.Query.SQL = "SELECT COUNT(*) AS writes FROM status_write_probe"
	queryResponse, queryGrant = run(query)
	status, raw, err = h.call(http.MethodGet, "/v1/operations/"+queryResponse.ID+"/results", queryGrant, nil)
	if err != nil || status != 200 {
		t.Fatal("read-only migration status check failed")
	}
	reader, err = ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("missing read-only migration status check")
	}
	column, ok = reader.RecordBatch().Column(0).(*array.Int64)
	if !ok || column.Value(0) != 0 {
		t.Fatal("migration status wrote to the source")
	}
}
