// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type multipartSinkFixture struct {
	ctx    context.Context
	dir    string
	active *os.File
	data   [][]byte
	rows   []int64
	schema *arrow.Schema
}

func (f *multipartSinkFixture) Context() context.Context { return f.ctx }
func (f *multipartSinkFixture) NewPart() (*os.File, error) {
	if f.active != nil {
		return nil, errors.New("part still active")
	}
	file, err := os.CreateTemp(f.dir, "part-")
	f.active = file
	return file, err
}
func (f *multipartSinkFixture) SealPart(rows int64) error {
	if f.active == nil {
		return errors.New("no active part")
	}
	data, err := os.ReadFile(f.active.Name())
	if err != nil {
		return err
	}
	if err := f.active.Close(); err != nil {
		return err
	}
	f.active = nil
	f.data = append(f.data, data)
	f.rows = append(f.rows, rows)
	return nil
}
func (f *multipartSinkFixture) Commit(string) (Snapshot, error) {
	return Snapshot{}, errors.New("fixture does not publish")
}
func (f *multipartSinkFixture) Abort() error {
	if f.active != nil {
		_ = f.active.Close()
		f.active = nil
	}
	return nil
}
func (f *multipartSinkFixture) PreviousSchema() (*arrow.Schema, error) { return nil, ErrNotFound }
func (f *multipartSinkFixture) SetSchema(schema *arrow.Schema) error   { f.schema = schema; return nil }

func multipartRecord(t *testing.T, rows int) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "text", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	ids := builder.Field(0).(*array.Int64Builder)
	text := builder.Field(1).(*array.StringBuilder)
	for i := 0; i < rows; i++ {
		if i%7 == 0 {
			ids.AppendNull()
		} else {
			ids.Append(int64(i))
		}
		text.Append(fmt.Sprint(i) + strings.Repeat("x", 80))
	}
	record := builder.NewRecordBatch()
	t.Cleanup(record.Release)
	return record
}

func TestMultipartParquetRoundTripAndEmpty(t *testing.T) {
	for _, rows := range []int{0, 2000} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			tx := &multipartSinkFixture{ctx: context.Background(), dir: t.TempDir()}
			defer tx.Abort()
			limits := query.DefaultLimits()
			limits.MaxRows = 10000
			limits.MaxBytes = 2 << 20
			opts := MultipartOptions{MaxParts: 64, MaxPartBytes: 32 << 10, MaxTotalBytes: limits.MaxBytes}
			sink := NewMultipartParquetSink(tx, limits, opts)
			defer sink.Abort()
			record := multipartRecord(t, rows)
			if err := sink.Schema(record.Schema()); err != nil {
				t.Fatal(err)
			}
			if err := sink.Write(record); err != nil {
				t.Fatal(err)
			}
			if err := sink.Finish(); err != nil {
				t.Fatal(err)
			}
			if err := sink.Finish(); err != nil {
				t.Fatal("finish not idempotent", err)
			}
			if rows == 0 && len(tx.data) != 1 {
				t.Fatal("empty snapshot must contain one valid schema file")
			}
			if rows > 0 && len(tx.data) < 2 {
				t.Fatal("fixture did not rotate")
			}
			var totalRows, totalBytes int64
			for index, data := range tx.data {
				if int64(len(data)) > opts.MaxPartBytes {
					t.Fatal("encoded part exceeded hard limit")
				}
				totalBytes += int64(len(data))
				table := parquetReadTable(t, data)
				parquetAssertSchema(t, record.Schema(), table.Schema())
				if table.NumRows() != tx.rows[index] {
					t.Fatal("part row metadata differs")
				}
				reader := array.NewTableReader(table, 256)
				for reader.Next() {
					batch := reader.RecordBatch()
					ids := batch.Column(0).(*array.Int64)
					text := batch.Column(1).(*array.String)
					for row := 0; row < int(batch.NumRows()); row++ {
						want := totalRows + int64(row)
						if ids.IsNull(row) != (want%7 == 0) || (!ids.IsNull(row) && ids.Value(row) != want) || text.Value(row) != fmt.Sprint(want)+strings.Repeat("x", 80) {
							t.Fatal("multipart row order/value changed")
						}
					}
					totalRows += batch.NumRows()
				}
				reader.Release()
			}
			if totalRows != int64(rows) || sink.Rows() != int64(rows) || totalBytes > limits.MaxBytes {
				t.Fatal("total rows/bytes differ")
			}
		})
	}
}

func TestMultipartParquetFailsClosedAtEveryLimit(t *testing.T) {
	for _, tc := range []struct {
		name        string
		part, total int64
		parts       int
		rows        int64
	}{
		{"part_footer", 32, 4096, 4, 10000},
		{"total_footer", 4096, 64, 4, 10000},
		{"part_count", 4096, 1 << 20, 1, 10000},
		{"rows", 4096, 1 << 20, 4, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &multipartSinkFixture{ctx: context.Background(), dir: t.TempDir()}
			defer tx.Abort()
			limits := query.DefaultLimits()
			limits.MaxRows = tc.rows
			limits.MaxBytes = tc.total
			sink := NewMultipartParquetSink(tx, limits, MultipartOptions{MaxParts: tc.parts, MaxPartBytes: tc.part, MaxTotalBytes: tc.total})
			defer sink.Abort()
			record := multipartRecord(t, 100)
			err := sink.Schema(record.Schema())
			if err == nil {
				err = sink.Write(record)
			}
			if err == nil {
				err = sink.Finish()
			}
			if err == nil {
				t.Fatal("limit ignored")
			}
			if sink.Finish() == nil {
				t.Fatal("failed sink later finished")
			}
			var total int64
			for _, data := range tx.data {
				if int64(len(data)) > tc.part {
					t.Fatal("oversized sealed part")
				}
				total += int64(len(data))
			}
			if total > tc.total {
				t.Fatal("total bytes escaped bound")
			}
		})
	}
}

func TestMultipartParquetSchemaCancellationAndWideRow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := &multipartSinkFixture{ctx: ctx, dir: t.TempDir()}
	defer tx.Abort()
	limits := query.DefaultLimits()
	sink := NewMultipartParquetSink(tx, limits, MultipartOptions{MaxParts: 4, MaxPartBytes: 4096, MaxTotalBytes: limits.MaxBytes})
	defer sink.Abort()
	record := multipartRecord(t, 1)
	sink.expectedSchema = arrow.NewSchema([]arrow.Field{{Name: "different", Type: arrow.PrimitiveTypes.Int32}}, nil)
	if !errors.Is(sink.Schema(record.Schema()), ErrSchemaMismatch) {
		t.Fatal("schema replacement accepted")
	}
	tx2 := &multipartSinkFixture{ctx: ctx, dir: t.TempDir()}
	defer tx2.Abort()
	sink2 := NewMultipartParquetSink(tx2, limits, MultipartOptions{MaxParts: 4, MaxPartBytes: 4096, MaxTotalBytes: limits.MaxBytes})
	defer sink2.Abort()
	if err := sink2.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	cancel()
	if !errors.Is(sink2.Write(record), context.Canceled) || sink2.Finish() == nil {
		t.Fatal("canceled result sealed successfully")
	}
	tx3 := &multipartSinkFixture{ctx: context.Background(), dir: t.TempDir()}
	defer tx3.Abort()
	sink3 := NewMultipartParquetSink(tx3, limits, MultipartOptions{MaxParts: 4, MaxPartBytes: 64, MaxTotalBytes: limits.MaxBytes})
	defer sink3.Abort()
	err := sink3.Schema(record.Schema())
	if err == nil {
		err = sink3.Write(record)
	}
	if err == nil {
		t.Fatal("wide row escaped conservative limit")
	}
}

type multipartManagerExecutor struct {
	record arrow.RecordBatch
	fail   bool
}

func (e *multipartManagerExecutor) Execute(_ context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	if err := sink.Schema(e.record.Schema()); err != nil {
		return query.Stats{}, err
	}
	if err := sink.Write(e.record); err != nil {
		return query.Stats{}, err
	}
	if e.fail {
		return query.Stats{}, errors.New("interrupted extraction after sealed parts")
	}
	return query.Stats{Rows: e.record.NumRows()}, nil
}
func TestManagerMultipartAtomicPublicationAndResolve(t *testing.T) {
	config, manager, _ := managerFixture(t)
	defer manager.Close()
	config.Acceleration.Datasets[0].Multipart = &catalog.MultipartConfig{MaxPartBytes: 1 << 20, MaxParts: 16}
	executor := &multipartManagerExecutor{record: multipartRecord(t, 20000)}
	manager.factory = func(c catalog.Config, _ query.Limits) (query.Executor, error) {
		if c.Acceleration != nil {
			t.Fatal("recursive acceleration")
		}
		return executor, nil
	}
	snapshot, err := manager.Refresh(context.Background(), "orders_fast", false)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Path != "" || len(snapshot.Parts) < 2 || snapshot.Rows != 20000 {
		t.Fatalf("multipart snapshot: %+v", snapshot)
	}
	sources, versions, release, err := Resolve(context.Background(), config, query.Request{Mode: "federated", Sources: []string{"orders_fast"}})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(versions) != 1 || versions[0].Generation != snapshot.Generation || sources[0].Path != "" || len(sources[0].ParquetPaths) != len(snapshot.Parts) {
		t.Fatal("part source resolution differs")
	}
	for i, part := range snapshot.Parts {
		if sources[0].ParquetPaths[i] != part.Path {
			t.Fatal("part ordering changed")
		}
	}
	executor.fail = true
	if _, err := manager.Refresh(context.Background(), "orders_fast", false); err == nil {
		t.Fatal("partial extraction published")
	}
	current, err := manager.Status("orders_fast")
	if err != nil || current.Generation != snapshot.Generation {
		t.Fatal("failed multipart refresh replaced valid generation")
	}
	for _, part := range snapshot.Parts {
		if _, err := os.Stat(part.Path); err != nil {
			t.Fatal("pinned multipart part disappeared")
		}
	}
}
