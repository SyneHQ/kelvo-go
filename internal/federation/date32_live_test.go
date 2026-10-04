//go:build duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	native "github.com/SYNEHQ/kelvo-go/internal/federation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type date32LiveCase struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

type date32LivePlan struct {
	Table string              `json:"table"`
	Plan  duckbridge.ScanPlan `json:"plan"`
}

type date32LiveReport struct {
	Passed        bool                    `json:"passed"`
	Name          string                  `json:"name"`
	Mode          string                  `json:"mode"`
	Names         []string                `json:"names"`
	Types         []string                `json:"types"`
	Rows          [][]any                 `json:"rows"`
	Plans         []date32LivePlan        `json:"plans"`
	Stats         map[string]native.Stats `json:"stats"`
	Schema        map[string]string       `json:"schema"`
	ActiveReaders int64                   `json:"active_readers"`
	Seconds       float64                 `json:"seconds"`
}

type date32LiveReader struct {
	array.RecordReader
	refs   atomic.Int64
	active *atomic.Int64
}

func (r *date32LiveReader) Retain() { r.RecordReader.Retain(); r.refs.Add(1) }
func (r *date32LiveReader) Release() {
	r.RecordReader.Release()
	if r.refs.Add(-1) == 0 {
		r.active.Add(-1)
	}
}

// TestDate32LiveClickHouse is activated only by the fresh-fixture harness. The
// disabled lane changes only factory capabilities; SQL and discovered native
// ClickHouse tables remain identical to the pushed lane.
func TestDate32LiveClickHouse(t *testing.T) {
	configPath, casePath := os.Getenv("KELVO_TEST_DATE32_CONFIG"), os.Getenv("KELVO_TEST_DATE32_CASE")
	output, mode := os.Getenv("KELVO_TEST_DATE32_REPORT"), os.Getenv("KELVO_TEST_DATE32_MODE")
	if configPath == "" || casePath == "" || output == "" || mode == "" {
		t.Skip("fresh Date32 live fixture not configured")
	}
	if mode != "pushed" && mode != "disabled" {
		t.Fatal("invalid Date32 fixture mode")
	}
	data, err := os.ReadFile(casePath)
	if err != nil || len(data) > 32768 {
		t.Fatal("Date32 case unavailable or oversized")
	}
	var c date32LiveCase
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&c); err != nil || c.Name == "" || len(c.Name) > 80 || len(c.SQL) > 16000 {
		t.Fatal("invalid Date32 fixture case")
	}
	config, err := catalog.Load(configPath)
	if err != nil || len(config.Sources) != 1 {
		t.Fatal("Date32 fixture catalog unavailable")
	}
	source := config.Sources[0]
	if source.ID != "warehouse" || source.Type != "clickhouse" || source.Federation == nil || len(source.Federation.Tables) != 2 {
		t.Fatal("Date32 fixture requires two native ClickHouse table aliases")
	}
	started := time.Now()
	report := date32LiveReport{Name: c.Name, Mode: mode, Rows: [][]any{}, Plans: []date32LivePlan{}, Stats: map[string]native.Stats{}, Schema: map[string]string{}}
	t.Cleanup(func() {
		report.Passed = !t.Failed()
		report.Seconds = time.Since(started).Seconds()
		file, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Error("Date32 result file unavailable")
			return
		}
		if err := json.NewEncoder(file).Encode(report); err != nil {
			t.Error("Date32 result write failed")
		}
		if err := file.Close(); err != nil {
			t.Error("Date32 result close failed")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	var tables []*native.Table
	var factories []*duckbridge.Factory
	var active atomic.Int64
	var mu sync.Mutex
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error("Date32 DuckDB connection did not close")
		}
		if err := db.Close(); err != nil {
			t.Error("Date32 DuckDB database did not close")
		}
		for _, factory := range factories {
			factory.Close()
			if factory.Err() != nil {
				t.Error("Date32 native callback failed during teardown")
			}
		}
		for i, table := range tables {
			if err := table.Close(); err != nil {
				t.Error("Date32 source table did not close")
			}
			report.Stats[source.Federation.Tables[i].Name] = table.Stats()
		}
		report.ActiveReaders = active.Load()
		if report.ActiveReaders != 0 {
			t.Error("Date32 live reader remained active")
		}
	}()
	if _, err := conn.ExecContext(ctx, "SET threads=1; SET memory_limit='256MB'; SET enable_external_access=false; CREATE SCHEMA warehouse"); err != nil {
		t.Fatal(err)
	}
	limits := query.DefaultLimits()
	limits.Threads = 1
	limits.Timeout = 15 * time.Second
	limits.MaxRows = 10000
	limits.MaxBytes = 8 << 20
	for _, definition := range source.Federation.Tables {
		table, err := native.New(ctx, source, definition, limits)
		if err != nil {
			t.Fatal("native Date32 table creation failed")
		}
		tables = append(tables, table)
		for _, field := range table.Schema().Fields() {
			report.Schema[definition.Name+"."+field.Name] = field.Type.String()
			if (field.Name == "day" || field.Name == "classic") && field.Type.ID() != arrow.DATE32 {
				t.Fatal("installed source did not expose Arrow Date32")
			}
		}
		capabilities := table.PredicateCapabilities()
		if mode == "disabled" {
			capabilities = duckbridge.PredicateCapabilities{}
		}
		name := definition.Name
		producer := func(ctx context.Context, plan duckbridge.ScanPlan) (array.RecordReader, error) {
			encoded, err := json.Marshal(plan)
			if err != nil {
				return nil, err
			}
			var detached duckbridge.ScanPlan
			if err = json.Unmarshal(encoded, &detached); err != nil {
				return nil, err
			}
			mu.Lock()
			report.Plans = append(report.Plans, date32LivePlan{Table: name, Plan: detached})
			mu.Unlock()
			reader, err := table.Scan(ctx, plan)
			if err != nil {
				return reader, err
			}
			active.Add(1)
			wrapped := &date32LiveReader{RecordReader: reader, active: &active}
			wrapped.refs.Store(1)
			return wrapped, nil
		}
		factory, err := duckbridge.New(ctx, table.Schema(), producer, capabilities)
		if err != nil {
			t.Fatal("Date32 native bridge creation failed")
		}
		factories = append(factories, factory)
		if err = conn.Raw(func(raw any) error { return factory.Register(raw.(driver.Conn), "warehouse", name) }); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := conn.QueryContext(ctx, c.SQL)
	if err != nil {
		t.Fatal("Date32 live query failed")
	}
	columns, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	for _, column := range columns {
		report.Types = append(report.Types, strings.Clone(column.DatabaseTypeName()))
		report.Names = append(report.Names, strings.Clone(column.Name()))
	}
	for rows.Next() {
		values, targets := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for i, value := range values {
			if text, ok := value.(string); ok {
				values[i] = strings.Clone(text)
				continue
			}
			if date, ok := value.(time.Time); ok {
				if report.Types[i] != "DATE" {
					rows.Close()
					t.Fatal("unexpected temporal output type")
				}
				values[i] = date.Format("2006-01-02")
			} else if value != nil {
				switch reflect.TypeOf(value).Kind() {
				case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.String, reflect.Bool:
				default:
					rows.Close()
					t.Fatal("unsupported live output type")
				}
			}
		}
		report.Rows = append(report.Rows, values)
		if len(report.Rows) > 10000 {
			rows.Close()
			t.Fatal("Date32 fixture output bound exceeded")
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal("Date32 live reader failed")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if mode == "disabled" {
		mu.Lock()
		defer mu.Unlock()
		for _, entry := range report.Plans {
			if len(entry.Plan.Filters) != 0 {
				t.Fatal("disabled factory received pushed predicates")
			}
		}
	}
}
