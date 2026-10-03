// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The bridge lowers optional sparse IN to the existing OR/equality contract.
// These tests protect adapter SQL semantics independently of bridge extraction.
func sparseEqualities(column, kind string, values ...string) duckbridge.Filter {
	children := make([]duckbridge.Filter, len(values))
	for i, value := range values {
		children[i] = comparison(column, kind, value, "eq")
	}
	return duckbridge.Filter{Kind: "or", Children: children}
}
func TestSparseINLoweredORAcrossNativeDialects(t *testing.T) {
	for _, tc := range []struct {
		name         string
		dialect      scanDialect
		quoted, cast string
	}{
		{"clickhouse", dialectClickHouse, "`signed`", "Int64"},
		{"postgres", dialectPostgres, `"signed"`, "BIGINT"},
		{"mysql", dialectMySQL, "`signed`", "SIGNED"},
		{"sqlserver", dialectSQLServer, "[signed]", "BIGINT"},
		{"oracle", dialectOracle, `"signed"`, "NUMBER(38,0)"},
		{"snowflake", dialectSnowflake, `"signed"`, "NUMBER(38,0)"},
		{"databricks", dialectDatabricks, "`signed`", "BIGINT"},
		{"bigquery", dialectBigQuery, "`signed`", "INT64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := compilerTable()
			table.dialect = tc.dialect
			table.remoteName = "trusted_events"
			// Above2^53 and both int64 boundaries must remain exact scalar equality;
			// replacing sparse membership with its bounding range admits false matches.
			values := []string{"-9223372036854775808", "9007199254740993", "9223372036854775807"}
			plan := duckbridge.ScanPlan{Columns: []string{"signed"}, Filters: []duckbridge.Filter{
				sparseEqualities("signed", "int64", values...), {Kind: "is_not_null", Column: "signed"},
			}}
			sql, schema, err := table.compileScan(plan)
			if err != nil {
				t.Fatal(err)
			}
			terms := make([]string, len(values))
			for i, value := range values {
				terms[i] = "(" + tc.quoted + " = CAST('" + value + "' AS " + tc.cast + "))"
			}
			expected := "SELECT " + tc.quoted + " FROM trusted_events WHERE (" + strings.Join(terms, " OR ") + ") AND (" + tc.quoted + " IS NOT NULL)"
			if sql != expected {
				t.Fatalf("sparse membership changed: %s", sql)
			}
			if schema.NumFields() != 1 || schema.Field(0).Type.ID() != arrow.INT64 {
				t.Fatal("membership changed projection schema")
			}
			if _, err = sqlguard.ReadOnly(sql); err != nil {
				t.Fatalf("lowered SQL violates native read-only envelope: %v", err)
			}
		})
	}
}
func TestSparseINUnsignedAndBooleanDialectBoundaries(t *testing.T) {
	for _, d := range []scanDialect{dialectClickHouse, dialectPostgres, dialectMySQL, dialectSQLServer, dialectOracle, dialectSnowflake, dialectDatabricks, dialectBigQuery} {
		table := compilerTable()
		table.dialect = d
		table.remoteName = "trusted_events"
		sql, _, err := table.compileScan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{sparseEqualities("id", "uint64", "0", "9007199254740993", "18446744073709551615")}})
		if d == dialectClickHouse || d == dialectMySQL {
			if err != nil {
				t.Fatal(err)
			}
			cast := "UInt64"
			if d == dialectMySQL {
				cast = "UNSIGNED"
			}
			if !strings.Contains(sql, "CAST('18446744073709551615' AS "+cast+")") || strings.Count(sql, " OR ") != 2 {
				t.Fatal("uint64 membership lost exactness")
			}
		} else if err == nil {
			t.Fatal("unsupported unsigned membership accepted")
		}
		sql, _, err = table.compileScan(duckbridge.ScanPlan{Columns: []string{"active"}, Filters: []duckbridge.Filter{sparseEqualities("active", "bool", "false", "true")}})
		if d == dialectMySQL || d == dialectOracle {
			if err == nil {
				t.Fatal("version-dependent boolean membership accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(sql, "IS NULL") || strings.Contains(sql, "IS NOT NULL") {
			t.Fatal("boolean membership rewrote NULL semantics")
		}
		if d == dialectSQLServer {
			if !strings.Contains(sql, "= CAST(0 AS BIT)") || !strings.Contains(sql, "= CAST(1 AS BIT)") {
				t.Fatal("SQLServer bool membership lacks BIT literals")
			}
		} else if !strings.Contains(sql, "= false") || !strings.Contains(sql, "= true") {
			t.Fatal("boolean membership literals changed")
		}
	}
}
func TestSparseINLoweredORRejectsMalformedMemberWithoutPartialPredicate(t *testing.T) {
	for _, bad := range []duckbridge.Filter{
		comparison("id", "uint64", "18446744073709551616", "eq"),
		comparison("id", "uint64", "-1", "eq"), comparison("id", "uint64", "01", "eq"),
		comparison("id", "uint64", "NULL", "eq"), comparison("id", "int64", "3", "eq"),
		comparison("id", "uint64", "1' OR 1=1 --", "eq"),
	} {
		sql, schema, err := compilerTable().compileScan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{{Kind: "or", Children: []duckbridge.Filter{comparison("id", "uint64", "1", "eq"), bad}}}})
		if err == nil || sql != "" || schema != nil {
			t.Fatal("malformed membership emitted a partial source predicate")
		}
	}
}

// This is a real native HTTP/Arrow adapter protocol fixture, not a ClickHouse
// server or DuckDB optimizer benchmark. It verifies source SQL, full-value
// precision and actual fetched/wire statistics for residual versus selected scans.
func TestSparseINClickHouseAdapterSelectedAndResidualFetch(t *testing.T) {
	all := []uint64{0, 1, 2, 5, 17, 9007199254740992, 9007199254740993, 9007199254740994, 18446744073709551614, 18446744073709551615}
	selected := []uint64{1, 9007199254740993, 18446744073709551615}
	fullRecord := uintRecord(memory.DefaultAllocator, all...)
	defer fullRecord.Release()
	selectedRecord := uintRecord(memory.DefaultAllocator, selected...)
	defer selectedRecord.Release()
	schemaWire, fullWire, selectedWire := arrowWire(t), arrowWire(t, fullRecord), arrowWire(t, selectedRecord)
	const projection = "SELECT `id` FROM `reports`.`events`"
	const selectedSQL = projection + " WHERE ((`id` = CAST('1' AS UInt64)) OR (`id` = CAST('9007199254740993' AS UInt64)) OR (`id` = CAST('18446744073709551615' AS UInt64)))"
	var calls atomic.Int32
	table := nativeTable(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		request, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		if err != nil {
			t.Error(err)
			http.Error(w, "fixture read failed", 500)
			return
		}
		if r.URL.Query().Get("readonly") != "1" {
			t.Error("scan lost read-only source setting")
		}
		var payload []byte
		switch string(request) {
		case "SELECT * FROM `reports`.`events` LIMIT 0":
			payload = schemaWire
		case projection:
			payload = fullWire
		case selectedSQL:
			payload = selectedWire
		default:
			t.Errorf("unexpected native membership SQL: %s", request)
			http.Error(w, "unexpected source query", 400)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		_, _ = w.Write(payload)
	})
	scan := func(plan duckbridge.ScanPlan) []uint64 {
		t.Helper()
		reader, err := table.Scan(context.Background(), plan)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Release()
		var result []uint64
		for reader.Next() {
			column, ok := reader.RecordBatch().Column(0).(*array.Uint64)
			if !ok {
				t.Fatal("source uint64 changed type")
			}
			for i := 0; i < column.Len(); i++ {
				result = append(result, column.Value(i))
			}
		}
		if err = reader.Err(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	unfiltered := scan(duckbridge.ScanPlan{Columns: []string{"id"}})
	residualStats := table.Stats()
	var residual []uint64
	for _, value := range unfiltered {
		if value == 1 || value == 9007199254740993 || value == 18446744073709551615 {
			residual = append(residual, value)
		}
	}
	pushed := scan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{sparseEqualities("id", "uint64", "1", "9007199254740993", "18446744073709551615")}})
	total := table.Stats()
	if !reflect.DeepEqual(unfiltered, all) || !reflect.DeepEqual(residual, selected) || !reflect.DeepEqual(pushed, selected) {
		t.Fatal("selected/residual memberships differ or lose uint64 precision")
	}
	if residualStats.Rows != 10 || total.Rows-residualStats.Rows != 3 || total.Scans != 2 || calls.Load() != 3 {
		t.Fatal("source fetched rows or discovery accounting changed")
	}
	if residualStats.SourceWireBytes != int64(len(fullWire)) || total.SourceWireBytes-residualStats.SourceWireBytes != int64(len(selectedWire)) {
		t.Fatal("actual Arrow transport bytes differ from scan accounting")
	}
	if total.Bytes-residualStats.Bytes >= residualStats.Bytes {
		t.Fatal("selected scan did not reduce delivered Arrow buffers")
	}
}
