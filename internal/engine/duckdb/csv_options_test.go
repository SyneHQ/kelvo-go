//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func csvMicroOptions() map[string]string {
	return map[string]string{"buffer_size": "1048576", "maximum_line_size": "262144"}
}

func TestCSVOptionsTinyLateralQueryAtLowMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.csv")
	if err := os.WriteFile(path, []byte("n\n3\n"), 0600); err != nil {
		t.Fatal(err)
	}
	l := limits()
	l.MemoryMB, l.Threads, l.Timeout = 64, 1, 10*time.Second
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "events", Type: "csv", Path: path, Options: csvMicroOptions()}}}, l)
	if err != nil {
		t.Fatal(err)
	}
	sink := new(captureSink)
	_, err = e.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: "SELECT CAST(length(list(i))::BIGINT AS VARCHAR) FROM events, LATERAL range(events.n) input(i)"}, sink)
	if err != nil || sink.rows != 1 || !reflect.DeepEqual(sink.values, []string{"3"}) {
		t.Fatalf("low-memory CSV query failed: rows=%d error=%v", sink.rows, err)
	}
}

func TestCSVOptionsPreserveMultilineUTF8AndLongValues(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		name := "supported"
		value := strings.Repeat("界", 40000)
		if oversized {
			name, value = "oversized", strings.Repeat("界", 1<<20)
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.csv")
			file, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			writer := csv.NewWriter(file)
			want := []string{"quoted, value\nnext line ✓", value}
			if err := writer.WriteAll([][]string{{"value"}, {want[0]}, {want[1]}}); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			l := limits()
			l.MemoryMB, l.MaxBytes, l.Timeout = 64, 16<<20, 10*time.Second
			e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "events", Type: "csv", Path: path, Options: csvMicroOptions()}}}, l)
			if err != nil {
				t.Fatal(err)
			}
			sink := new(captureSink)
			_, err = e.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: "SELECT value FROM events"}, sink)
			if oversized && err != nil {
				// The pinned parser may accept slightly larger lines. An explicit
				// failed result is safe; success must preserve every complete value.
				return
			}
			if err != nil || sink.rows != int64(len(want)) || !reflect.DeepEqual(sink.values, want) {
				t.Fatalf("CSV values changed or rows were silently skipped: rows=%d error=%v", sink.rows, err)
			}
		})
	}
}

type csvSetupSpy struct{ statements []string }

func (s *csvSetupSpy) ExecContext(_ context.Context, statement string, _ []driver.NamedValue) (driver.Result, error) {
	s.statements = append(s.statements, statement)
	return driver.RowsAffected(0), nil
}

func TestCSVOptionsRejectProgrammaticConfigBeforeSourceAccess(t *testing.T) {
	invalid := catalog.Source{ID: "invalid", Type: "csv", Path: "/unavailable/private.csv", Options: map[string]string{"buffer_size": "1048576); SELECT private_value", "maximum_line_size": "262144"}}
	if _, err := New(catalog.Config{Sources: []catalog.Source{invalid}}, limits()); err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
		t.Fatalf("programmatic invalid option accepted: %v", err)
	}
	spy := new(csvSetupSpy)
	err := attachSources(context.Background(), spy, []catalog.Source{{ID: "first", Type: "csv", Path: "/unavailable/first.csv"}, invalid}, "", t.TempDir())
	if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" || len(spy.statements) != 0 {
		t.Fatalf("invalid option reached source access: calls=%d error=%v", len(spy.statements), err)
	}
}

func TestCSVOptionsOmittedDefaultsAndNormalizedSQL(t *testing.T) {
	for _, options := range []map[string]string{nil, csvMicroOptions()} {
		spy := new(csvSetupSpy)
		if err := attachSources(context.Background(), spy, []catalog.Source{{ID: "events", Type: "csv", Path: "/tmp/a'b.csv", Options: options}}, "", t.TempDir()); err != nil {
			t.Fatal(err)
		}
		want := "CREATE VIEW \"events\" AS SELECT * FROM read_csv_auto('/tmp/a''b.csv'"
		if options != nil {
			want += ", buffer_size=1048576, maximum_line_size=262144"
		}
		want += ")"
		if !reflect.DeepEqual(spy.statements, []string{want}) {
			t.Fatal("unexpected SQL option interpolation")
		}
	}
}
