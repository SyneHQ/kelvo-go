//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/engine/duckdb"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// This opt-in release gate deliberately writes over 4 GiB of incompressible
// Parquet data. The source is a bounded synthetic Arrow producer, not a native
// database driver benchmark. The read/aggregate phase uses the real DuckDB engine.
func TestMultipartDatasetLargerThanFourGiB(t *testing.T) {
	if os.Getenv("KELVO_TEST_MULTIPART_LARGE") != "1" {
		t.Skip("requires explicit large-dataset gate on a provisioned test VM")
	}
	const batchRows = 8192
	const payloadBytes = 1024
	const batches = 544 // 4.25 GiB raw binary values, before IDs and metadata.
	const rows = int64(batchRows * batches)
	dir := t.TempDir()
	expectedPath := filepath.Join(dir, "expected.csv")
	expected, err := os.Create(expectedPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintln(expected, "slot,digest"); err != nil {
		t.Fatal(err)
	}
	expectedRNG := rand.New(rand.NewSource(17))
	expectedPayload := make([]byte, payloadBytes)
	for i := 0; i < batchRows; i++ {
		_, _ = expectedRNG.Read(expectedPayload)
		if _, err = fmt.Fprintf(expected, "%d,%x\n", i, sha256.Sum256(expectedPayload)); err != nil {
			t.Fatal(err)
		}
	}
	if err = expected.Close(); err != nil {
		t.Fatal(err)
	}
	source := &largeArrowSource{batchRows: batchRows, batches: batches, payloadBytes: payloadBytes}
	limits := query.Limits{MaxRows: rows, MaxBytes: 6 << 30, Timeout: 25 * time.Minute, MemoryMB: 256, Threads: 2, MaxTempMB: 512}
	cfg := catalog.Config{Sources: []catalog.Source{{ID: "raw", Type: "csv", Path: filepath.Join(dir, "unused.csv")}, {ID: "expected", Type: "csv", Path: expectedPath}}, Acceleration: &catalog.AccelerationConfig{Directory: filepath.Join(dir, "snapshots"), TenantID: "large-test", Datasets: []catalog.Dataset{{ID: "large", Query: query.Request{Mode: "federated", Sources: []string{"raw"}, SQL: "SELECT id,payload FROM raw"}, MaxAge: time.Hour, AuthorizationVersion: "test-v1", Limits: limits, Multipart: &catalog.MultipartConfig{MaxPartBytes: 64 << 20, MaxParts: 256}}}}}
	manager, err := acceleration.NewManager(cfg, func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	started := time.Now()
	snapshot, err := manager.Refresh(ctx, "large", false)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Bytes <= 4<<30 || snapshot.Rows != rows || len(snapshot.Parts) < 2 {
		t.Fatalf("large generation size/rows invalid: bytes=%d rows=%d parts=%d", snapshot.Bytes, snapshot.Rows, len(snapshot.Parts))
	}
	for _, part := range snapshot.Parts {
		if part.Bytes > 64<<20 {
			t.Fatal("part exceeded encoded ceiling")
		}
	}
	if _, err = manager.Verify(ctx, "large"); err != nil {
		t.Fatal(err)
	}
	request := query.Request{Mode: "federated", Sources: []string{"large", "expected"}, SQL: "SELECT CAST(count(*) AS BIGINT) AS n, CAST(sum(l.id) AS BIGINT) AS ids, CAST(sum(octet_length(l.payload)) AS BIGINT) AS payload_bytes, CAST(sum(CASE WHEN sha256(l.payload) = e.digest THEN 0 ELSE 1 END) AS BIGINT) AS wrong_payloads FROM large l JOIN expected e ON l.id % 8192 = e.slot"}
	sources, versions, release, err := acceleration.Resolve(ctx, cfg, request)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(versions) != 1 || versions[0].Generation != snapshot.Generation {
		t.Fatal("generation pin lost")
	}
	readLimits := query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: 10 * time.Minute, MemoryMB: 256, Threads: 2, MaxTempMB: 512}
	engine, err := duckdb.New(catalog.Config{Sources: sources}, readLimits)
	if err != nil {
		t.Fatal(err)
	}
	sink := &largeAggregateSink{}
	if _, err = engine.Execute(ctx, request, sink); err != nil {
		t.Fatal(err)
	}
	if sink.rows != 1 || len(sink.values) != 4 || sink.values[0] != rows || sink.values[1] != rows*(rows-1)/2 || sink.values[2] != rows*payloadBytes || sink.values[3] != 0 {
		t.Fatalf("wrong aggregate: rows=%d values=%v", sink.rows, sink.values)
	}
	t.Logf("large multipart gate: rows=%d encoded_bytes=%d parts=%d elapsed=%s; synthetic Arrow ingestion plus real DuckDB aggregate, not source throughput", rows, snapshot.Bytes, len(snapshot.Parts), time.Since(started).Round(time.Millisecond))
}

type largeArrowSource struct{ batchRows, batches, payloadBytes int }

func (s *largeArrowSource) Execute(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "payload", Type: arrow.BinaryTypes.Binary}}, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	// Reuse a random 8 MiB array across batches: compression operates on much
	// smaller pages, so this stays incompressible without retaining the dataset.
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	rng := rand.New(rand.NewSource(17))
	payload := make([]byte, s.payloadBytes)
	for range s.batchRows {
		_, _ = rng.Read(payload)
		b.Append(payload)
	}
	values := b.NewArray()
	b.Release()
	defer values.Release()
	ids := array.NewInt64Builder(memory.DefaultAllocator)
	defer ids.Release()
	var total int64
	for range s.batches {
		if err := ctx.Err(); err != nil {
			return query.Stats{}, err
		}
		for i := 0; i < s.batchRows; i++ {
			ids.Append(total + int64(i))
		}
		idArray := ids.NewArray()
		record := array.NewRecordBatch(schema, []arrow.Array{idArray, values}, int64(s.batchRows))
		idArray.Release()
		err := sink.Write(record)
		record.Release()
		if err != nil {
			return query.Stats{}, err
		}
		total += int64(s.batchRows)
	}
	return query.Stats{Rows: total}, nil
}

type largeAggregateSink struct {
	rows   int64
	values []int64
}

func (s *largeAggregateSink) Schema(*arrow.Schema) error { return nil }
func (s *largeAggregateSink) Write(record arrow.RecordBatch) error {
	s.rows += record.NumRows()
	for i := 0; i < int(record.NumCols()); i++ {
		v := record.Column(i).(*array.Int64)
		for j := 0; j < v.Len(); j++ {
			s.values = append(s.values, v.Value(j))
		}
	}
	return nil
}
