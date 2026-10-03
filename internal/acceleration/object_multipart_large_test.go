//go:build duckdb_arrow && (linux || darwin)

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/engine/duckdb"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// This opt-in release gate deliberately writes over 4 GiB of incompressible
// Parquet data through a disk-backed object fixture. The source is a bounded
// synthetic Arrow producer; this is not a cloud-provider or native driver
// benchmark. The read/aggregate phase uses the real DuckDB engine.
func TestObjectMultipartDatasetLargerThanFourGiB(t *testing.T) {
	if os.Getenv("KELVO_TEST_OBJECT_MULTIPART_LARGE") != "1" {
		t.Skip("requires explicit large-dataset gate on a provisioned test VM")
	}
	extensionDirectory := os.Getenv("KELVO_OBJECT_EXTENSION_DIRECTORY")
	if extensionDirectory == "" {
		t.Fatal("KELVO_OBJECT_EXTENSION_DIRECTORY must contain the signed httpfs extension")
	}
	if _, err := os.Stat(filepath.Join(extensionDirectory, "httpfs.duckdb_extension")); err != nil {
		t.Fatal(err)
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
	source := &objectLargeArrowSource{batchRows: batchRows, batches: batches, payloadBytes: payloadBytes}
	limits := query.Limits{MaxRows: rows, MaxBytes: 6 << 30, Timeout: 25 * time.Minute, MemoryMB: 256, Threads: 2, MaxTempMB: 512}
	cfg := catalog.Config{Sources: []catalog.Source{{ID: "raw", Type: "csv", Path: filepath.Join(dir, "unused.csv")}, {ID: "expected", Type: "csv", Path: expectedPath}}, Acceleration: &catalog.AccelerationConfig{Directory: filepath.Join(dir, "snapshots"), TenantID: "large-test", Datasets: []catalog.Dataset{{ID: "large", Query: query.Request{Mode: "federated", Sources: []string{"raw"}, SQL: "SELECT id,payload FROM raw"}, MaxAge: time.Hour, AuthorizationVersion: "test-v1", Limits: limits, Multipart: &catalog.MultipartConfig{MaxPartBytes: 64 << 20, MaxParts: 256}}}}}
	remoteConfig := testObjectConfig(t)
	cfg.Acceleration.Directory = remoteConfig.Directory
	cfg.Acceleration.ObjectStorage = remoteConfig.ObjectStorage
	objects := &objectLargeDiskClient{directory: filepath.Join(dir, "objects"), current: make(map[string]string), versions: make(map[string]map[string]objectLargeDiskEntry)}
	if err := os.Mkdir(objects.directory, 0700); err != nil {
		t.Fatal(err)
	}
	backend, err := newObjectBackend(*cfg.Acceleration, objects)
	if err != nil {
		t.Fatal(err)
	}
	checked := &objectLargeCheckedBackend{objectBackend: backend}
	manager := &Manager{config: cfg, store: checked, factory: func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil }}
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
	sourceMap, release, err := openObjectRanges(ctx, *cfg.Acceleration.ObjectStorage, []Snapshot{snapshot}, objects)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	sources := []catalog.Source{sourceMap["large"], {ID: "expected", Type: "csv", Path: expectedPath}}
	if checked.sealed != len(snapshot.Parts) {
		t.Fatalf("checked %d seals for %d parts", checked.sealed, len(snapshot.Parts))
	}
	readLimits := query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: 10 * time.Minute, MemoryMB: 256, Threads: 2, MaxTempMB: 512}
	engine, err := duckdb.New(catalog.Config{Sources: sources, ExtensionDirectory: extensionDirectory}, readLimits)
	if err != nil {
		t.Fatal(err)
	}
	sink := &objectLargeAggregateSink{}
	if _, err = engine.Execute(ctx, request, sink); err != nil {
		t.Fatal(err)
	}
	if sink.rows != 1 || len(sink.values) != 4 || sink.values[0] != rows || sink.values[1] != rows*(rows-1)/2 || sink.values[2] != rows*payloadBytes || sink.values[3] != 0 {
		t.Fatalf("wrong aggregate: rows=%d values=%v", sink.rows, sink.values)
	}
	t.Logf("large remote-layout multipart gate: rows=%d encoded_bytes=%d parts=%d elapsed=%s; synthetic Arrow ingestion plus real DuckDB aggregate, disk-backed object fixture, not provider/network/source throughput", rows, snapshot.Bytes, len(snapshot.Parts), time.Since(started).Round(time.Millisecond))
}

type objectLargeArrowSource struct{ batchRows, batches, payloadBytes int }

func (s *objectLargeArrowSource) Execute(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
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

type objectLargeAggregateSink struct {
	rows   int64
	values []int64
}

func (s *objectLargeAggregateSink) Schema(schema *arrow.Schema) error {
	if schema.NumFields() != 4 {
		return errors.New("expected four aggregate fields")
	}
	for _, f := range schema.Fields() {
		if !arrow.TypeEqual(f.Type, arrow.PrimitiveTypes.Int64) {
			return errors.New("aggregate field is not int64")
		}
	}
	return nil
}
func (s *objectLargeAggregateSink) Write(record arrow.RecordBatch) error {
	s.rows += record.NumRows()
	for i := 0; i < int(record.NumCols()); i++ {
		v, ok := record.Column(i).(*array.Int64)
		if !ok || v.NullN() != 0 {
			return errors.New("invalid aggregate value")
		}
		for j := 0; j < v.Len(); j++ {
			s.values = append(s.values, v.Value(j))
		}
	}
	return nil
}

// Preserve every immutable version until t.TempDir cleanup. Publication is an
// atomic metadata swap after a bounded streaming copy and exact digest check.
type objectLargeDiskEntry struct {
	path string
	info objectstore.Info
}
type objectLargeDiskClient struct {
	mu        sync.Mutex
	directory string
	current   map[string]string
	versions  map[string]map[string]objectLargeDiskEntry
	sequence  uint64
}

func (*objectLargeDiskClient) Close() {}
func (c *objectLargeDiskClient) lookup(ctx context.Context, key, version string) (objectLargeDiskEntry, error) {
	if err := ctx.Err(); err != nil {
		return objectLargeDiskEntry{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if version == "" {
		version = c.current[key]
	}
	entry, ok := c.versions[key][version]
	if !ok {
		return objectLargeDiskEntry{info: objectstore.Info{ServerTime: time.Now().UTC()}}, objectstore.ErrNotFound
	}
	entry.info.ServerTime = time.Now().UTC()
	return entry, nil
}
func (c *objectLargeDiskClient) Head(ctx context.Context, key, version string) (objectstore.Info, error) {
	entry, err := c.lookup(ctx, key, version)
	return entry.info, err
}
func (c *objectLargeDiskClient) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	entry, err := c.lookup(ctx, key, version)
	if err != nil {
		return nil, entry.info, err
	}
	f, err := os.Open(entry.path)
	if err != nil {
		return nil, entry.info, err
	}
	return &objectLargeReader{ctx: ctx, Reader: f, file: f}, entry.info, nil
}

type objectLargeReader struct {
	ctx context.Context
	io.Reader
	file *os.File
}

func (r *objectLargeReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}
func (r *objectLargeReader) Close() error { return r.file.Close() }
func (c *objectLargeDiskClient) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	entry, err := c.lookup(ctx, key, version)
	if err != nil {
		return nil, entry.info, err
	}
	if offset < 0 || length <= 0 || offset > entry.info.Size || length > entry.info.Size-offset {
		return nil, entry.info, errors.New("invalid disk object range")
	}
	f, err := os.Open(entry.path)
	if err != nil {
		return nil, entry.info, err
	}
	return &objectLargeReader{ctx: ctx, Reader: io.NewSectionReader(f, offset, length), file: f}, entry.info, nil
}
func (c *objectLargeDiskClient) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Info{}, err
	}
	if size < 0 || size > objectstore.MaxUploadBytes || condition.Absent == (condition.Version != "") {
		return objectstore.Info{}, errors.New("invalid disk object upload")
	}
	file, err := os.CreateTemp(c.directory, "version-")
	if err != nil {
		return objectstore.Info{}, err
	}
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(file.Name())
		}
	}()
	hash := sha256.New()
	reader := &objectLargeReader{ctx: ctx, Reader: body}
	n, err := io.CopyBuffer(io.MultiWriter(file, hash), io.LimitReader(reader, size+1), make([]byte, 64<<10))
	if err != nil {
		return objectstore.Info{}, err
	}
	if n != size || fmt.Sprintf("%x", hash.Sum(nil)) != digest {
		return objectstore.Info{}, errors.New("disk object size or digest mismatch")
	}
	if err := file.Sync(); err != nil {
		return objectstore.Info{}, err
	}
	if err := file.Chmod(0400); err != nil {
		return objectstore.Info{}, err
	}
	if err := file.Close(); err != nil {
		return objectstore.Info{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return objectstore.Info{}, err
	}
	current := c.current[key]
	if (condition.Absent && current != "") || (condition.Version != "" && condition.Version != current) {
		return objectstore.Info{ServerTime: time.Now().UTC()}, objectstore.ErrConflict
	}
	c.sequence++
	info := objectstore.Info{Size: size, SHA256: digest, Version: fmt.Sprintf("v%d", c.sequence), ServerTime: time.Now().UTC()}
	if c.versions[key] == nil {
		c.versions[key] = make(map[string]objectLargeDiskEntry)
	}
	c.versions[key][info.Version] = objectLargeDiskEntry{path: file.Name(), info: info}
	c.current[key] = info.Version
	keep = true
	return info, nil
}

// Check the actual stage path after every seal, before the next part can open.
type objectLargeCheckedBackend struct {
	*objectBackend
	sealed int
}

func (b *objectLargeCheckedBackend) BeginMultipart(ctx context.Context, dataset string, options MultipartOptions) (MultipartRefreshWriter, error) {
	writer, err := b.objectBackend.BeginMultipart(ctx, dataset, options)
	if err != nil {
		return nil, err
	}
	return &objectLargeCheckedWriter{objectMultipartTransaction: writer.(*objectMultipartTransaction), backend: b}, nil
}

type objectLargeCheckedWriter struct {
	*objectMultipartTransaction
	backend *objectLargeCheckedBackend
}

func (w *objectLargeCheckedWriter) stageGone() error {
	local := w.base.local
	if local.file != nil {
		return errors.New("multipart stage still open after sealing")
	}
	// The writer's directory handle may already be closed after commit.
	path := filepath.Join(w.base.backend.config.Directory, w.base.backend.config.TenantID, w.base.dataset, local.stage)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("multipart staging bytes remain: %v", err)
	}
	return nil
}
func (w *objectLargeCheckedWriter) SealPart(rows int64) error {
	if err := w.objectMultipartTransaction.SealPart(rows); err != nil {
		return err
	}
	if err := w.stageGone(); err != nil {
		return err
	}
	w.backend.sealed++
	return nil
}
func (w *objectLargeCheckedWriter) Commit(fingerprint string) (Snapshot, error) {
	snapshot, err := w.objectMultipartTransaction.Commit(fingerprint)
	if err != nil {
		return snapshot, err
	}
	return snapshot, w.stageGone()
}
