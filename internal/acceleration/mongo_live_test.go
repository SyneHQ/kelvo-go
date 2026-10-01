//go:build duckdb_arrow && (linux || darwin)

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	duckdbengine "github.com/SYNEHQ/kelvo-go/internal/engine/duckdb"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/mongodb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoPipelineLiveAcceleration(t *testing.T) {
	adminURI, readerURI := os.Getenv("KELVO_TEST_MONGO_ADMIN_URI"), os.Getenv("KELVO_TEST_MONGO_READER_URI")
	if adminURI == "" || readerURI == "" {
		t.Skip("set dedicated live MongoDB fixture URIs")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := mongo.Connect(options.Client().ApplyURI(adminURI))
	if err != nil {
		t.Fatal("acceleration fixture client initialization failed")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = admin.Disconnect(cleanup)
	}()
	collection := admin.Database("kelvo_native_test").Collection("acceleration_" + bson.NewObjectID().Hex())
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = collection.Drop(cleanup)
	}()
	decimal := func(value string) bson.Decimal128 {
		parsed, err := bson.ParseDecimal128(value)
		if err != nil {
			t.Fatal("invalid fixture decimal")
		}
		return parsed
	}
	documents := []any{
		bson.D{
			{Key: "_id", Value: int32(1)}, {Key: "status", Value: "paid"}, {Key: "region", Value: "east"},
			{Key: "exact_id", Value: int64(math.MaxInt64)}, {Key: "amount", Value: decimal("10.250000000000000000000000001")},
			{Key: "minimum", Value: int64(math.MinInt64)}, {Key: "negative_zero", Value: math.Copysign(0, -1)},
			{Key: "date", Value: bson.DateTime(-123456789)}, {Key: "timestamp", Value: bson.Timestamp{T: 123, I: 4}},
			{Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 255, 2}}},
			{Key: "object_id", Value: bson.NewObjectID()},
			{Key: "nested", Value: bson.D{{Key: "null", Value: nil}, {Key: "array", Value: bson.A{int32(1), "नमस्ते, 世界", nil}}}},
		},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "status", Value: "paid"}, {Key: "region", Value: "east"},
			{Key: "exact_id", Value: int64(math.MaxInt64)}, {Key: "amount", Value: decimal("20.750000000000000000000000002")},
			{Key: "different", Value: bson.A{false, nil, int64(9007199254740993)}}},
		bson.D{{Key: "_id", Value: int32(3)}, {Key: "status", Value: "paid"}, {Key: "region", Value: "west"},
			{Key: "exact_id", Value: int64(math.MaxInt64 - 1)}, {Key: "amount", Value: int64(9007199254740993)}},
		bson.D{{Key: "_id", Value: int32(4)}, {Key: "status", Value: "paid"}, {Key: "region", Value: "west"},
			{Key: "exact_id", Value: int64(math.MaxInt64 - 1)}, {Key: "amount", Value: int64(1)}},
		bson.D{{Key: "_id", Value: int32(5)}, {Key: "status", Value: "pending"}, {Key: "region", Value: "east"},
			{Key: "exact_id", Value: int64(math.MaxInt64)}, {Key: "amount", Value: int32(100)}},
	}
	if _, err := collection.InsertMany(ctx, documents); err != nil {
		t.Fatal("acceleration fixture insert failed")
	}
	var original []bson.Raw
	for id := int32(1); id <= int32(len(documents)); id++ {
		document, err := collection.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Raw()
		if err != nil {
			t.Fatal("acceleration fixture read failed")
		}
		original = append(original, bytes.Clone(document))
	}
	t.Setenv("KELVO_SOURCE_MONGO_ACCELERATION_DSN", readerURI)

	const filtered = `- $match:
    status: paid
    exact_id: 9223372036854775807
- $sort:
    _id: 1`
	const grouped = `- $match:
    status: paid
- $group:
    _id: "$region"
    count:
      $sum: 1
    total:
      $sum: "$amount"
- $sort:
    _id: 1`

	for _, test := range []struct {
		name, pipeline string
		want           []bson.Raw
		grouped        bool
	}{
		{name: "filtered_exact_bson", pipeline: filtered, want: original[:2]},
		{name: "grouped_exact_values", pipeline: grouped, grouped: true},
		{name: "zero_rows", pipeline: "- $match:\n    status: absent_from_fixture"},
		{name: "empty_pipeline", pipeline: "[]", want: original},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := loadMongoAccelerationYAML(t, t.TempDir(), collection.Name(), test.pipeline)
			want := test.want
			if test.grouped {
				// Read the same aggregation directly from MongoDB to compare every
				// BSON byte; separately assert its independently specified values.
				var pipeline mongo.Pipeline
				for _, raw := range config.Acceleration.Datasets[0].Query.Mongo.Pipeline {
					var stage bson.D
					if err := bson.UnmarshalExtJSON(raw, false, &stage); err != nil {
						t.Fatal("fixture pipeline conversion failed")
					}
					pipeline = append(pipeline, stage)
				}
				cursor, err := collection.Aggregate(ctx, pipeline)
				if err != nil {
					t.Fatal("direct aggregation fixture failed")
				}
				for cursor.Next(ctx) {
					want = append(want, bytes.Clone(cursor.Current))
				}
				cursorErr := cursor.Err()
				_ = cursor.Close(ctx)
				if cursorErr != nil || len(want) != 2 {
					t.Fatal("direct aggregation fixture returned unexpected results")
				}
				for i, region := range []string{"east", "west"} {
					if want[i].Lookup("_id").Type != bson.TypeString || want[i].Lookup("_id").StringValue() != region ||
						want[i].Lookup("count").Type != bson.TypeInt32 || want[i].Lookup("count").Int32() != 2 {
						t.Fatal("grouped result changed the expected region or count")
					}
				}
				if value := want[0].Lookup("total"); value.Type != bson.TypeDecimal128 || value.Decimal128() != decimal("31.000000000000000000000000003") {
					t.Fatal("grouped Decimal128 total lost precision")
				}
				if value := want[1].Lookup("total"); value.Type != bson.TypeInt64 || value.Int64() != 9007199254740994 {
					t.Fatal("grouped Int64 total lost precision")
				}
			}
			manager := newMongoAccelerationManager(t, config)
			snapshot, err := manager.Refresh(ctx, "mongo_cached", false)
			if err != nil {
				t.Fatalf("MongoDB pipeline refresh failed: %v", err)
			}
			if snapshot.Rows != int64(len(want)) || snapshot.Bytes <= 0 {
				t.Fatal("published snapshot metadata does not match the pipeline result")
			}
			verified, err := manager.Verify(ctx, "mongo_cached")
			if err != nil || verified.Generation != snapshot.Generation || verified.SHA256 != snapshot.SHA256 {
				t.Fatal("published pipeline snapshot failed verification")
			}
			got := readMongoAccelerationSnapshot(t, ctx, config, snapshot)
			assertMongoAccelerationDocuments(t, got, want)
		})
	}

	t.Run("write_stages_keep_last_good", func(t *testing.T) {
		directory := t.TempDir()
		goodConfig := loadMongoAccelerationYAML(t, directory, collection.Name(), filtered)
		manager := newMongoAccelerationManager(t, goodConfig)
		good, err := manager.Refresh(ctx, "mongo_cached", false)
		if err != nil {
			t.Fatalf("initial pipeline refresh failed: %v", err)
		}
		for _, stage := range []string{"$out", "$merge"} {
			t.Run(stage[1:], func(t *testing.T) {
				target := collection.Name() + "_forbidden"
				pipeline := "- " + stage + ": " + target
				if stage == "$merge" {
					pipeline = "- $merge:\n    into: " + target
				}
				badConfig := loadMongoAccelerationYAML(t, directory, collection.Name(), pipeline)
				badManager := newMongoAccelerationManager(t, badConfig)
				_, err := badManager.Refresh(ctx, "mongo_cached", false)
				var public *query.Error
				if !errors.As(err, &public) || public.Code != "PERMISSION_DENIED" {
					t.Fatalf("write stage bypassed the connector read-only gate: %v", err)
				}
				current, err := badManager.Verify(ctx, "mongo_cached")
				if err != nil || current.Generation != good.Generation || current.SHA256 != good.SHA256 || current.Rows != good.Rows {
					t.Fatal("rejected write stage changed the last committed snapshot")
				}
				assertMongoAccelerationDocuments(t, readMongoAccelerationSnapshot(t, ctx, goodConfig, good), original[:2])
				names, err := admin.Database("kelvo_native_test").ListCollectionNames(ctx, bson.D{{Key: "name", Value: target}})
				if err != nil || len(names) != 0 {
					t.Fatal("rejected write stage created a MongoDB output collection")
				}
			})
		}
	})
}

func loadMongoAccelerationYAML(t *testing.T, directory, collection, pipeline string) catalog.Config {
	t.Helper()
	directory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`sources:
  - id: mongo
    type: mongodb
    dsn_env: KELVO_SOURCE_MONGO_ACCELERATION_DSN
    options:
      database: kelvo_native_test
acceleration:
  directory: snapshots
  tenant_id: tenant-a
  datasets:
    - id: mongo_cached
      authorization_version: live-fixture-v1
      max_age: 1h
      limits:
        max_rows: 64
        max_bytes: 1048576
        timeout: 20s
        memory_mb: 64
        threads: 1
        max_temp_mb: 64
      query:
        mode: native
        connection_id: mongo
        mongo:
          collection: %s
          pipeline:
`, collection)
	for _, line := range strings.Split(pipeline, "\n") {
		content += "            " + line + "\n"
	}
	path := filepath.Join(directory, "sources.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := catalog.Load(path)
	if err != nil {
		t.Fatalf("MongoDB pipeline YAML was rejected: %v", err)
	}
	return config
}

func newMongoAccelerationManager(t *testing.T, config catalog.Config) *acceleration.Manager {
	t.Helper()
	manager, err := acceleration.NewManager(config, func(sourceConfig catalog.Config, limits query.Limits) (query.Executor, error) {
		return mongodb.New(sourceConfig, limits)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

func readMongoAccelerationSnapshot(t *testing.T, ctx context.Context, config catalog.Config, snapshot acceleration.Snapshot) []bson.Raw {
	t.Helper()
	request := query.Request{Mode: "federated", SQL: "SELECT document_bson FROM mongo_cached", Sources: []string{"mongo_cached"}}
	sources, versions, release, err := acceleration.Resolve(ctx, config, request)
	if err != nil {
		t.Fatalf("MongoDB snapshot resolution failed: %v", err)
	}
	defer release()
	if len(sources) != 1 || sources[0].Type != "parquet" || sources[0].Path != snapshot.Path || sources[0].DSNEnv != "" ||
		len(versions) != 1 || versions[0].Generation != snapshot.Generation {
		t.Fatal("snapshot resolution did not isolate the committed Parquet generation")
	}
	engine, err := duckdbengine.New(catalog.Config{Sources: sources}, config.Acceleration.Datasets[0].Limits)
	if err != nil {
		t.Fatal(err)
	}
	sink := new(mongoAccelerationBSONSink)
	stats, err := engine.Execute(ctx, request, sink)
	if err != nil {
		t.Fatalf("DuckDB could not read the MongoDB pipeline snapshot: %v", err)
	}
	if !sink.schema || stats.Rows != snapshot.Rows || int64(len(sink.documents)) != snapshot.Rows {
		t.Fatal("DuckDB result lost the BSON schema or row count")
	}
	return sink.documents
}

type mongoAccelerationBSONSink struct {
	schema    bool
	documents []bson.Raw
}

func (sink *mongoAccelerationBSONSink) Schema(schema *arrow.Schema) error {
	if schema.NumFields() != 1 || schema.Field(0).Name != "document_bson" || !arrow.TypeEqual(schema.Field(0).Type, arrow.BinaryTypes.Binary) {
		return errors.New("DuckDB did not preserve the BSON binary column")
	}
	sink.schema = true
	return nil
}

func (sink *mongoAccelerationBSONSink) Write(record arrow.RecordBatch) error {
	column, ok := record.Column(0).(*array.Binary)
	if !sink.schema || !ok || column.NullN() != 0 {
		return errors.New("DuckDB changed a BSON document to NULL or another type")
	}
	for i := 0; i < column.Len(); i++ {
		document := bson.Raw(bytes.Clone(column.Value(i)))
		if document.Validate() != nil {
			return errors.New("DuckDB returned malformed BSON bytes")
		}
		sink.documents = append(sink.documents, document)
	}
	return nil
}

func assertMongoAccelerationDocuments(t *testing.T, got, want []bson.Raw) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("BSON document count changed: got %d, want %d", len(got), len(want))
	}
	// SQL without ORDER BY promises no row order. Compare a sorted copy while
	// requiring every byte, including BSON types and field order, to survive.
	got = append([]bson.Raw(nil), got...)
	want = append([]bson.Raw(nil), want...)
	sort.Slice(got, func(i, j int) bool { return bytes.Compare(got[i], got[j]) < 0 })
	sort.Slice(want, func(i, j int) bool { return bytes.Compare(want[i], want[j]) < 0 })
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("BSON document %d changed bytes or exact value types", i)
		}
	}
}
