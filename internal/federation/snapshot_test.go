//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"golang.org/x/sys/unix"
)

func snapshotFixture(t *testing.T, records ...arrow.RecordBatch) catalog.Source {
	t.Helper()
	root := t.TempDir()
	schema := records[0].Schema()
	hash, err := acceleration.SchemaFingerprint(schema)
	if err != nil {
		t.Fatal(err)
	}
	limits := query.DefaultLimits()
	limits.MaxRows, limits.MaxBytes = 1_000_000, 64<<20
	source := catalog.Source{ID: "orders_fast", Type: "parquet", LocalSnapshot: &catalog.LocalSnapshotRead{
		Dataset: "orders_fast", Generation: strings.Repeat("a", 32), SchemaSHA256: hash,
		Scan: catalog.SnapshotScanLimits{MaxRows: 1_000_000, MaxBytes: 64 << 20},
	}}
	for i, record := range records {
		path := filepath.Join(root, string(rune('a'+i))+".parquet")
		output, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		sink := acceleration.NewParquetSink(output, limits)
		if err := sink.Schema(schema); err != nil {
			output.Close()
			t.Fatal(err)
		}
		if err := sink.Write(record); err != nil {
			output.Close()
			t.Fatal(err)
		}
		if err := sink.Finish(); err != nil {
			output.Close()
			t.Fatal(err)
		}
		if err := output.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0400); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		source.ParquetPaths = append(source.ParquetPaths, path)
		source.LocalSnapshot.Parts = append(source.LocalSnapshot.Parts, catalog.LocalSnapshotPart{Rows: record.NumRows(), Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])})
	}
	if len(records) == 1 {
		source.Path, source.ParquetPaths = source.ParquetPaths[0], nil
	}
	return source
}

func snapshotPolicy(t *testing.T, columns []string, rows *access.Predicate) context.Context {
	t.Helper()
	ctx, err := access.WithPolicy(context.Background(), access.Policy{Sources: map[string]access.SourcePolicy{
		"orders_fast": {Tables: map[string]access.TablePolicy{"orders_fast": {Columns: columns, Rows: rows, AllRows: rows == nil}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return WithScanBudget(ctx, 4)
}

func snapshotTableFor(t *testing.T, ctx context.Context, source catalog.Source) *Table {
	t.Helper()
	limits := query.DefaultLimits()
	limits.MaxRows, limits.MaxBytes = 1, 1024 // Output caps do not truncate raw scans.
	table, err := NewSnapshot(ctx, source, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := table.Close(); err != nil {
			t.Error(err)
		}
		table.snapshot.memory.mu.Lock()
		used := table.snapshot.memory.used
		table.snapshot.memory.mu.Unlock()
		if used != 0 {
			t.Errorf("snapshot allocator retained %d bytes", used)
		}
	})
	return table
}

func snapshotScan(t *testing.T, table *Table, names ...string) (int64, error) {
	t.Helper()
	reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: names})
	if err != nil {
		return 0, err
	}
	defer reader.Release()
	var rows int64
	for reader.Next() {
		rows += reader.RecordBatch().NumRows()
	}
	return rows, reader.Err()
}

func TestSnapshotReaderExactTypesProjectionAndHiddenPredicate(t *testing.T) {
	md := arrow.NewMetadata([]string{"private"}, []string{"never expose"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tenant", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: md},
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 30, Scale: 4}, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "tiny", Type: arrow.PrimitiveTypes.Int8},
	}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "b", "a", ""}, []bool{true, true, true, false})
	b.Field(1).(*array.Uint64Builder).AppendValues([]uint64{9007199254740993, 2, 18446744073709551615, 4}, nil)
	b.Field(2).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(-123456789), decimal128.FromI64(99), decimal128.FromI64(0), decimal128.FromI64(1)}, []bool{true, true, false, true})
	b.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{-315521754876544, 0, 123456789, 0}, []bool{true, true, true, false})
	b.Field(4).(*array.Int8Builder).AppendValues([]int8{-128, 0, 127, 1}, nil)
	record := b.NewRecordBatch()
	b.Release()
	defer record.Release()
	source := snapshotFixture(t, record, record)
	ctx := snapshotPolicy(t, []string{"id", "amount", "at", "tiny"}, &access.Predicate{Kind: "comparison", Column: "tenant", Op: "eq", Type: "string", Value: "a"})
	table := snapshotTableFor(t, ctx, source)
	if table.Schema().Metadata().Len() != 0 || table.Schema().NumFields() != 4 {
		t.Fatal("raw metadata/schema exposed")
	}
	for _, field := range table.Schema().Fields() {
		if field.Metadata.Len() != 0 {
			t.Fatal("field metadata exposed")
		}
	}
	reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"tiny", "at", "amount", "id"}})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	var rows int64
	for reader.Next() {
		got := reader.RecordBatch()
		if got.NumRows() != 2 || got.NumCols() != 4 {
			t.Fatal("wrong guarded shape")
		}
		if got.Column(0).(*array.Int8).Value(0) != -128 || got.Column(0).(*array.Int8).Value(1) != 127 {
			t.Fatal("integer width/order lost")
		}
		if got.Column(1).(*array.Timestamp).Value(0) != -315521754876544 {
			t.Fatal("timestamp changed")
		}
		if got.Column(2).(*array.Decimal128).Value(0) != decimal128.FromI64(-123456789) || !got.Column(2).IsNull(1) {
			t.Fatal("decimal or NULL changed")
		}
		if got.Column(3).(*array.Uint64).Value(0) != 9007199254740993 || got.Column(3).(*array.Uint64).Value(1) != 18446744073709551615 {
			t.Fatal("uint64 changed")
		}
		rows += got.NumRows()
	}
	if reader.Err() != nil || rows != 4 {
		t.Fatal("multipart scan failed", rows, reader.Err())
	}
	if _, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"tenant"}}); err == nil {
		t.Fatal("hidden predicate column exposed")
	}
	if table.Stats().Rows != 8 {
		t.Fatal("raw rows must precede filtering", table.Stats())
	}
}

func TestSnapshotReaderRawBudgetAcrossRescansAndEnvelopeCopy(t *testing.T) {
	record := uintRecord(memory.DefaultAllocator, 1, 2, 3)
	defer record.Release()
	source := snapshotFixture(t, record)
	source.LocalSnapshot.Scan.MaxRows = 5
	table := snapshotTableFor(t, snapshotPolicy(t, []string{"id"}, nil), source)
	source.LocalSnapshot.Scan.MaxRows = 100
	source.LocalSnapshot.Parts[0].SHA256 = strings.Repeat("f", 64)
	if rows, err := snapshotScan(t, table, "id"); err != nil || rows != 3 {
		t.Fatal(rows, err)
	}
	if rows, err := snapshotScan(t, table, "id"); rows != 0 {
		t.Fatal("over-budget batch delivered")
	} else {
		checkCode(t, err, "RESOURCE_EXHAUSTED")
	}
}

func TestSnapshotReaderRejectsInvalidPartsAndProvenance(t *testing.T) {
	for _, mode := range []string{"digest", "size", "rows", "schema", "schema-less", "missing", "symlink", "fifo", "directory", "footer", "unrestricted", "alias", "writable", "public", "hardlink", "parent-symlink", "public-parent"} {
		t.Run(mode, func(t *testing.T) {
			record := uintRecord(memory.DefaultAllocator, 1, 2, 3)
			defer record.Release()
			source := snapshotFixture(t, record)
			ctx := snapshotPolicy(t, []string{"id"}, nil)
			switch mode {
			case "digest":
				source.LocalSnapshot.Parts[0].SHA256 = strings.Repeat("f", 64)
			case "size":
				source.LocalSnapshot.Parts[0].Bytes++
			case "rows":
				source.LocalSnapshot.Parts[0].Rows++
			case "schema":
				source.LocalSnapshot.SchemaSHA256 = strings.Repeat("f", 64)
			case "schema-less":
				source.LocalSnapshot.SchemaSHA256 = ""
			case "unrestricted":
				ctx = context.Background()
			case "alias":
				source.LocalSnapshot.Dataset = "other"
			case "writable":
				if err := os.Chmod(source.Path, 0600); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(source.Path, 0444); err != nil {
					t.Fatal(err)
				}
			case "public-parent":
				if err := os.Chmod(filepath.Dir(source.Path), 0755); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(source.Path, source.Path+".link"); err != nil {
					t.Fatal(err)
				}
			case "parent-symlink":
				link := filepath.Join(t.TempDir(), "linked")
				if err := os.Symlink(filepath.Dir(source.Path), link); err != nil {
					t.Fatal(err)
				}
				source.Path = filepath.Join(link, filepath.Base(source.Path))
			default:
				path := source.Path
				if mode == "footer" {
					data, _ := os.ReadFile(path)
					data[len(data)-8] = 255
					data[len(data)-7] = 255
					data[len(data)-6] = 255
					data[len(data)-5] = 255
					if err := os.Chmod(path, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.Chmod(path, 0400); err != nil {
						t.Fatal(err)
					}
					break
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "symlink":
					if err := os.Symlink("/dev/null", path); err != nil {
						t.Fatal(err)
					}
				case "fifo":
					if err := unix.Mkfifo(path, 0600); err != nil {
						t.Fatal(err)
					}
				case "directory":
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				}
			}
			started := time.Now()
			table, err := NewSnapshot(ctx, source, query.DefaultLimits())
			if table != nil {
				table.Close()
				t.Fatal("invalid snapshot accepted")
			}
			if err == nil {
				t.Fatal("missing rejection")
			}
			if time.Since(started) > time.Second {
				t.Fatal("invalid file open blocked")
			}
		})
	}
}

func TestSnapshotReaderRechecksPartBeforeDelivery(t *testing.T) {
	record := uintRecord(memory.DefaultAllocator, 1, 2, 3)
	defer record.Release()
	source := snapshotFixture(t, record)
	table := snapshotTableFor(t, snapshotPolicy(t, []string{"id"}, nil), source)
	data, err := os.ReadFile(source.Path)
	if err != nil {
		t.Fatal(err)
	}
	data[5] ^= 1
	if err := os.Chmod(source.Path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source.Path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(source.Path, 0400); err != nil {
		t.Fatal(err)
	}
	rows, err := snapshotScan(t, table, "id")
	if rows != 0 {
		t.Fatal("corrupt data delivered")
	}
	checkCode(t, err, "DATASET_UNAVAILABLE")
}

func TestSnapshotReaderConcurrentCancelReleasesLeasesAndBuffers(t *testing.T) {
	values := make([]uint64, 1024)
	record := uintRecord(memory.DefaultAllocator, values...)
	defer record.Release()
	source := snapshotFixture(t, record)
	table := snapshotTableFor(t, snapshotPolicy(t, []string{"id"}, nil), source)
	readers := make([]array.RecordReader, 4)
	for i := range readers {
		reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		readers[i] = reader
		if !reader.Next() {
			t.Fatal(reader.Err())
		}
	}
	input, err := os.Open(source.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if err := unix.Flock(int(input.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != unix.EWOULDBLOCK {
		t.Fatal("active scan has no payload lease", err)
	}
	var wg sync.WaitGroup
	for _, reader := range readers {
		wg.Add(1)
		go func(r array.RecordReader) { defer wg.Done(); r.Release() }(reader)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	await(t, done)
	if err := table.Close(); err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(input.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal("released scan retains payload lease", err)
	}
	if err := unix.Flock(int(input.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotReaderRawByteBudgetAndAllocator(t *testing.T) {
	values := make([]uint64, 256)
	record := uintRecord(memory.DefaultAllocator, values...)
	defer record.Release()
	source := snapshotFixture(t, record)
	source.LocalSnapshot.Scan.MaxBytes = 4096
	table := snapshotTableFor(t, snapshotPolicy(t, []string{"id"}, nil), source)
	if rows, err := snapshotScan(t, table, "id"); err != nil || rows != 256 {
		t.Fatal(rows, err)
	}
	var rows int64
	var err error
	for i := 0; i < 3; i++ {
		rows, err = snapshotScan(t, table, "id")
		if err != nil {
			break
		}
	}
	if rows != 0 {
		t.Fatal("raw byte excess delivered")
	}
	checkCode(t, err, "RESOURCE_EXHAUSTED")
	allocator := &snapshotAllocator{limit: 128}
	buffer := allocator.Allocate(64)
	func() {
		defer func() {
			if _, ok := recover().(snapshotMemoryExceeded); !ok {
				t.Error("allocator growth bypassed cap")
			}
		}()
		allocator.Reallocate(128, buffer)
	}()
	if allocator.used != 64 {
		t.Fatal("failed allocation changed accounting", allocator.used)
	}
	allocator.Free(buffer)
	if allocator.used != 0 {
		t.Fatal("allocator did not release buffer")
	}
}

type snapshotPanicSink struct{}

func (snapshotPanicSink) Schema(*arrow.Schema) error    { return nil }
func (snapshotPanicSink) Write(arrow.RecordBatch) error { panic("private decoder failure") }

func TestSnapshotReaderDecoderCapAndPanicRelease(t *testing.T) {
	for _, mode := range []string{"decoder-cap", "decoder-partial-cap", "sink-panic"} {
		t.Run(mode, func(t *testing.T) {
			record := uintRecord(memory.DefaultAllocator, 1, 2, 3)
			defer record.Release()
			table := snapshotTableFor(t, snapshotPolicy(t, []string{"id"}, nil), snapshotFixture(t, record))
			if mode != "sink-panic" {
				table.snapshot.memory.mu.Lock()
				table.snapshot.memory.limit = 1
				if mode == "decoder-partial-cap" {
					table.snapshot.memory.limit = 16 << 10
				}
				table.snapshot.memory.mu.Unlock()
				rows, err := snapshotScan(t, table, "id")
				if rows != 0 {
					t.Fatal("decoder cap delivered data")
				}
				checkCode(t, err, "RESOURCE_EXHAUSTED")
			} else {
				plan, schema, err := table.snapshot.prepare(duckbridge.ScanPlan{Columns: []string{"id"}})
				if err != nil {
					t.Fatal(err)
				}
				executor := snapshotExecution{table: table.snapshot, plan: plan, schema: schema}
				_, err = executor.Execute(context.Background(), query.Request{}, snapshotPanicSink{})
				checkCode(t, err, "DATASET_UNAVAILABLE")
			}
		})
	}
}

type snapshotCancelReader struct{ cancel context.CancelFunc }

func (r snapshotCancelReader) Read(buffer []byte) (int, error) {
	r.cancel()
	buffer[0] = 1
	return 1, nil
}

func TestSnapshotReaderCancellationDuringIntegrityRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	count, err := io.CopyBuffer(io.Discard, snapshotContextReader{ctx: ctx, reader: snapshotCancelReader{cancel: cancel}}, make([]byte, 32<<10))
	if count != 1 || err != context.Canceled {
		t.Fatal("integrity read ignored cancellation", count, err)
	}
}

func TestSnapshotReaderUnsupportedPhysicalSchema(t *testing.T) {
	for _, typ := range []arrow.DataType{arrow.BinaryTypes.LargeString, arrow.ListOf(arrow.PrimitiveTypes.Int64)} {
		t.Run(typ.String(), func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: typ}}, nil)
			builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			switch field := builder.Field(0).(type) {
			case *array.LargeStringBuilder:
				field.Append("unsupported")
			case *array.ListBuilder:
				field.Append(true)
				field.ValueBuilder().(*array.Int64Builder).Append(1)
			}
			record := builder.NewRecordBatch()
			builder.Release()
			defer record.Release()
			path := filepath.Join(t.TempDir(), "unsupported.parquet")
			output, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := pqarrow.NewFileWriter(schema, output, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
			if err != nil {
				output.Close()
				t.Fatal(err)
			}
			if err := writer.Write(record); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			// pqarrow closes its output as part of Close.
			if err := os.Chmod(path, 0400); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(data)
			hash, err := acceleration.SchemaFingerprint(schema)
			if err != nil {
				t.Fatal(err)
			}
			source := catalog.Source{ID: "orders_fast", Type: "parquet", Path: path, LocalSnapshot: &catalog.LocalSnapshotRead{
				Dataset: "orders_fast", Generation: strings.Repeat("a", 32), SchemaSHA256: hash,
				Parts: []catalog.LocalSnapshotPart{{Rows: 1, Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}}, Scan: catalog.SnapshotScanLimits{MaxRows: 100, MaxBytes: 1 << 20},
			}}
			table, err := NewSnapshot(snapshotPolicy(t, []string{"id"}, nil), source, query.DefaultLimits())
			if table != nil {
				table.Close()
				t.Fatal("unsupported schema admitted")
			}
			checkCode(t, err, "UNSUPPORTED")
		})
	}
}
