//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	duck "github.com/duckdb/duckdb-go/v2"
)

func TestPublicErrorClassifiesOnlyTypedDuckDBOutOfMemory(t *testing.T) {
	const privateMessage = "private database path and SELECT secret_value"
	native := &duck.Error{Type: duck.ErrorTypeOutOfMemory, Msg: privateMessage}
	for _, err := range []error{native, fmt.Errorf("private wrapper: %w", native), errors.Join(errors.New("private close failure"), native)} {
		public := query.PublicError(publicError(err))
		if public.Code != "RESOURCE_EXHAUSTED" || public.Message != "Query exceeded DuckDB memory limit" {
			t.Fatal("native OOM classification lost")
		}
		if strings.Contains(public.Message, "private") || strings.Contains(public.Message, "SELECT") {
			t.Fatal("native details escaped")
		}
	}
	for _, err := range []error{
		errors.New("Out of Memory Error: private text"),
		&duck.Error{Type: duck.ErrorTypeIO, Msg: "Out of Memory Error: private disk path"},
		&duck.Error{Type: duck.ErrorTypeInvalid, Msg: privateMessage},
	} {
		if query.PublicError(publicError(err)).Code != "QUERY_FAILED" {
			t.Fatal("unrelated error misclassified as native OOM")
		}
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if query.PublicError(publicError(errors.Join(native, cause))).Code != query.PublicError(cause).Code {
			t.Fatal("native OOM overrode cancellation")
		}
	}
	trusted := query.NewError("PERMISSION_DENIED", "Selected source is unavailable")
	if publicError(fmt.Errorf("wrapped: %w", trusted)) != trusted {
		t.Fatal("trusted public error changed")
	}
}

// list aggregation cannot spill its variable-sized aggregate state. The outer
// length returns a single scalar: neither the row limit nor Arrow byte limit can
// explain failure. The first phase asserts the actual pinned native error type.
const nativeMemoryFailureSQL = "SELECT length(list(i)) AS n FROM range(2000000) AS input(i)"

func TestExecuteNativeMemoryLimitFailsBeforeArrowDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bounded := query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: 20 * time.Second, MemoryMB: 16, Threads: 1, MaxTempMB: 16}
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	nativeErr := conn.Raw(func(raw any) error {
		if err := configure(ctx, raw, "", t.TempDir(), bounded); err != nil {
			return err
		}
		connection, ok := raw.(driver.Conn)
		if !ok {
			return errors.New("unexpected driver connection")
		}
		engine, err := duck.NewArrowFromConn(connection)
		if err != nil {
			return err
		}
		reader, err := engine.QueryContext(ctx, nativeMemoryFailureSQL)
		if reader != nil {
			defer reader.Release()
			for reader.Next() {
			}
			if err == nil {
				err = reader.Err()
			}
		}
		return err
	})
	var native *duck.Error
	if !errors.As(nativeErr, &native) || native == nil || native.Type != duck.ErrorTypeOutOfMemory {
		t.Fatalf("workload did not produce genuine DuckDB OOM: error type=%T", nativeErr)
	}
	// Close the diagnostic connection before opening Kelvo's independently bounded
	// instance; this test does not claim a process RSS ceiling from memory_limit.
	_ = conn.Close()
	_ = db.Close()
	engine, err := New(catalog.Config{}, bounded)
	if err != nil {
		t.Fatal(err)
	}
	sink := &captureSink{}
	stats, err := engine.Execute(ctx, query.Request{Mode: "federated", SQL: nativeMemoryFailureSQL}, sink)
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("execution lost native OOM: %v", err)
	}
	if query.PublicError(err).Message != "Query exceeded DuckDB memory limit" {
		t.Fatal("memory error did not use static public message")
	}
	if sink.schema != nil || sink.rows != 0 || sink.batches != 0 || stats.Rows != 0 || stats.Bytes != 0 {
		t.Fatal("failed native execution delivered an Arrow result")
	}
	// Each execution owns a fresh native database. A failed query must not poison
	// the engine or prevent a subsequent small query from completing.
	healthy := &captureSink{}
	stats, err = engine.Execute(ctx, query.Request{Mode: "federated", SQL: "SELECT CAST(7 AS BIGINT) AS n"}, healthy)
	if err != nil || stats.Rows != 1 || healthy.rows != 1 {
		t.Fatalf("execution after OOM did not recover: %v", err)
	}
}
