// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestReadPipelineRejectsNestedWritesAndJavaScript(t *testing.T) {
	for _, encoded := range []string{
		`{"$out":"copy"}`, `{"$merge":{"into":"copy"}}`,
		`{"$lookup":{"from":"x","pipeline":[{"$merge":"copy"}],"as":"joined"}}`,
		`{"$facet":{"x":[{"$match":{"$where":"return true"}}]}}`,
		`{"$project":{"x":{"$function":{"body":"return 1","args":[],"lang":"js"}}}}`,
		`{"$group":{"_id":null,"x":{"$accumulator":{"init":"return 0"}}}}`,
		`{"$match":{"x":{"$code":"return 1"}}}`,
		`{"$match":{"x":{"$code":"return 1","$scope":{"a":1}}}}`,
		`{"$changeStream":{}}`, `{"$match":{},"$out":"copy"}`,
		`{"$match":{"$where":"a","$where":"b"}}`,
		`null`, `[]`, `{}`, `{"plain":{}}`,
	} {
		t.Run(encoded, func(t *testing.T) {
			_, err := readPipeline(&query.MongoRequest{Collection: "events", Pipeline: []json.RawMessage{json.RawMessage(encoded)}}, 10)
			if err == nil {
				t.Fatal("unsafe or malformed pipeline accepted")
			}
		})
	}
}

func TestReadPipelinePreservesExtendedJSONAndBoundsOutput(t *testing.T) {
	r := &query.MongoRequest{Collection: "events", Pipeline: []json.RawMessage{json.RawMessage(`{"$match":{"large":{"$numberLong":"9223372036854775807"},"decimal":{"$numberDecimal":"123456789.01234567890123456789"}}}`)}}
	p, err := readPipeline(r, 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 2 || p[1][0].Key != "$limit" || p[1][0].Value != int64(43) {
		t.Fatalf("missing observable overflow limit: %#v", p)
	}
	match := p[0][0].Value.(bson.D)
	if match[0].Value != int64(9223372036854775807) {
		t.Fatal("int64 changed")
	}
	if _, ok := match[1].Value.(bson.Decimal128); !ok {
		t.Fatal("decimal lost native type")
	}
	for _, collection := range []string{"", "$cmd", "system.users", "x\x00y", strings.Repeat("x", 121)} {
		if _, err := readPipeline(&query.MongoRequest{Collection: collection}, 10); err == nil {
			t.Fatalf("accepted collection %q", collection)
		}
	}
	deep := `{"$project":{"x":` + strings.Repeat(`[`, 70) + `1` + strings.Repeat(`]`, 70) + `}}`
	if _, err := readPipeline(&query.MongoRequest{Collection: "events", Pipeline: []json.RawMessage{json.RawMessage(deep)}}, 10); err == nil {
		t.Fatal("deep pipeline accepted")
	}
}

type fakeCursor struct {
	documents []bson.Raw
	index     int
	err       error
	cancel    context.CancelFunc
}

func (c *fakeCursor) Next(context.Context) bool {
	if c.cancel != nil {
		c.cancel()
	}
	if c.index >= len(c.documents) {
		return false
	}
	c.index++
	return true
}
func (c *fakeCursor) Document() bson.Raw { return c.documents[c.index-1] }
func (c *fakeCursor) Err() error         { return c.err }

type bsonSink struct {
	schema    *arrow.Schema
	documents []bson.Raw
	writes    int
	err       error
}

type cancelSink struct {
	bsonSink
	cancel context.CancelFunc
}

func (s *cancelSink) Write(record arrow.RecordBatch) error {
	err := s.bsonSink.Write(record)
	s.cancel()
	return err
}

func (s *bsonSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *bsonSink) Write(record arrow.RecordBatch) error {
	if s.err != nil {
		return s.err
	}
	s.writes++
	for i := 0; i < int(record.NumRows()); i++ {
		s.documents = append(s.documents, append(bson.Raw(nil), record.Column(0).(*array.Binary).Value(i)...))
	}
	return nil
}

func rawDocument(t *testing.T, document bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestStreamPreservesHeterogeneousBSONExactly(t *testing.T) {
	decimal, err := bson.ParseDecimal128("123456789.01234567890123456789")
	if err != nil {
		t.Fatal(err)
	}
	documents := []bson.Raw{
		rawDocument(t, bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "int64", Value: int64(9223372036854775807)}, {Key: "decimal", Value: decimal}, {Key: "date", Value: bson.DateTime(-123456789)}, {Key: "timestamp", Value: bson.Timestamp{T: 123, I: 4}}, {Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 255, 2}}}, {Key: "nested", Value: bson.D{{Key: "null", Value: nil}, {Key: "list", Value: bson.A{int32(1), "two", nil}}}}}),
		rawDocument(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "different", Value: true}}),
	}
	sink := new(bsonSink)
	var stats query.Stats
	if err := streamCursor(context.Background(), &fakeCursor{documents: documents}, query.DefaultLimits(), sink, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 2 || stats.Batches != 1 || len(sink.documents) != 2 {
		t.Fatalf("stats=%+v", stats)
	}
	if sink.schema.Field(0).Name != "document_bson" || sink.schema.Field(0).Nullable || sink.schema.Field(0).Type.ID() != arrow.BINARY {
		t.Fatal("BSON schema differs")
	}
	for i := range documents {
		if !bytes.Equal(documents[i], sink.documents[i]) {
			t.Fatal("BSON changed")
		}
	}
}

func TestStreamLimitsCancellationAndFailure(t *testing.T) {
	doc := rawDocument(t, bson.D{{Key: "payload", Value: strings.Repeat("x", 700)}})
	t.Run("row limit", func(t *testing.T) {
		l := query.DefaultLimits()
		l.MaxRows = 1
		var stats query.Stats
		err := streamCursor(context.Background(), &fakeCursor{documents: []bson.Raw{doc, doc}}, l, new(bsonSink), &stats)
		assertCode(t, err, "RESOURCE_EXHAUSTED")
	})
	t.Run("byte limit", func(t *testing.T) {
		l := query.DefaultLimits()
		l.MaxBytes = 1024
		var stats query.Stats
		err := streamCursor(context.Background(), &fakeCursor{documents: []bson.Raw{doc, doc}}, l, new(bsonSink), &stats)
		assertCode(t, err, "RESOURCE_EXHAUSTED")
	})
	t.Run("invalid BSON", func(t *testing.T) {
		var stats query.Stats
		err := streamCursor(context.Background(), &fakeCursor{documents: []bson.Raw{[]byte{1, 2, 3}}}, query.DefaultLimits(), new(bsonSink), &stats)
		assertCode(t, err, "QUERY_FAILED")
	})
	t.Run("cancel while advancing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var stats query.Stats
		err := streamCursor(ctx, &fakeCursor{documents: []bson.Raw{doc}, cancel: cancel}, query.DefaultLimits(), new(bsonSink), &stats)
		assertCode(t, err, "CANCELLED")
	})
	t.Run("driver errors stay private", func(t *testing.T) {
		var stats query.Stats
		err := streamCursor(context.Background(), &fakeCursor{err: errors.New("mongodb://secret:password@host")}, query.DefaultLimits(), new(bsonSink), &stats)
		assertCode(t, err, "QUERY_FAILED")
		if strings.Contains(err.Error(), "password") {
			t.Fatal("source diagnostic leaked")
		}
	})
	t.Run("sink failure", func(t *testing.T) {
		var stats query.Stats
		err := streamCursor(context.Background(), &fakeCursor{documents: []bson.Raw{doc}}, query.DefaultLimits(), &bsonSink{err: query.NewError("RESOURCE_EXHAUSTED", "sink limit")}, &stats)
		assertCode(t, err, "RESOURCE_EXHAUSTED")
		if stats.Rows != 0 {
			t.Fatal("failed delivery counted")
		}
	})
	t.Run("empty result schema", func(t *testing.T) {
		var stats query.Stats
		sink := new(bsonSink)
		if err := streamCursor(context.Background(), &fakeCursor{}, query.DefaultLimits(), sink, &stats); err != nil || sink.schema == nil || sink.writes != 0 {
			t.Fatal("empty result lost schema")
		}
	})
	t.Run("paged batches", func(t *testing.T) {
		var stats query.Stats
		docs := make([]bson.Raw, 300)
		for i := range docs {
			docs[i] = doc
		}
		sink := new(bsonSink)
		if err := streamCursor(context.Background(), &fakeCursor{documents: docs}, query.DefaultLimits(), sink, &stats); err != nil {
			t.Fatal(err)
		}
		if stats.Rows != 300 || stats.Batches != 3 {
			t.Fatalf("stats=%+v", stats)
		}
	})
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	var public *query.Error
	if !errors.As(err, &public) || public.Code != code {
		t.Fatalf("got %v, want %s", err, code)
	}
}

func TestConfigurationAndCredentialErrors(t *testing.T) {
	config := catalog.Config{Sources: []catalog.Source{{ID: "mongo", Type: "mongodb", DSNEnv: "KELVO_SOURCE_MONGO_DSN", Options: map[string]string{"database": "analytics"}}}}
	e, err := New(config, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("KELVO_SOURCE_MONGO_DSN", "mongodb://password:secret@host/?maxPoolSize=invalid")
	req := query.Request{Mode: "native", ConnectionID: "mongo", Mongo: &query.MongoRequest{Collection: "events"}}
	_, err = e.Execute(context.Background(), req, new(bsonSink))
	assertCode(t, err, "CONFIGURATION_ERROR")
	if strings.Contains(err.Error(), "secret") {
		t.Fatal("credential exposed")
	}
	config.Sources[0].DSNEnv = "HOME"
	if _, err := New(config, query.DefaultLimits()); err == nil {
		t.Fatal("untrusted environment reference accepted")
	}
}

func TestMongoLiveReadOnlyAggregation(t *testing.T) {
	adminURI, readerURI := os.Getenv("KELVO_TEST_MONGO_ADMIN_URI"), os.Getenv("KELVO_TEST_MONGO_READER_URI")
	if adminURI == "" || readerURI == "" {
		t.Skip("set dedicated live MongoDB fixture URIs")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := mongo.Connect(options.Client().ApplyURI(adminURI))
	if err != nil {
		t.Fatal("fixture client initialization failed")
	}
	defer admin.Disconnect(context.Background())
	collectionName := "acceptance_" + bson.NewObjectID().Hex()
	collection := admin.Database("kelvo_native_test").Collection(collectionName)
	defer collection.Drop(context.Background())
	decimal, _ := bson.ParseDecimal128("123456789.01234567890123456789")
	docs := []any{bson.D{{Key: "_id", Value: int32(1)}, {Key: "large", Value: int64(9223372036854775807)}, {Key: "decimal", Value: decimal}, {Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 255, 2}}}, {Key: "nested", Value: bson.D{{Key: "missing_neighbor", Value: nil}}}}, bson.D{{Key: "_id", Value: int32(2)}, {Key: "different", Value: bson.A{"hello", nil, int32(3)}}}}
	if _, err := collection.InsertMany(ctx, docs); err != nil {
		t.Fatal("fixture insert failed")
	}
	t.Setenv("KELVO_SOURCE_MONGO_DSN", readerURI)
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "mongo", Type: "mongodb", DSNEnv: "KELVO_SOURCE_MONGO_DSN", Options: map[string]string{"database": "kelvo_native_test"}}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	req := query.Request{Mode: "native", ConnectionID: "mongo", Mongo: &query.MongoRequest{Collection: collectionName, Pipeline: []json.RawMessage{json.RawMessage(`{"$sort":{"_id":1}}`)}}}
	sink := new(bsonSink)
	stats, err := e.Execute(ctx, req, sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 2 || len(sink.documents) != 2 {
		t.Fatalf("stats=%+v", stats)
	}
	for i := range docs {
		expected, err := collection.FindOne(ctx, bson.D{{Key: "_id", Value: int32(i + 1)}}).Raw()
		if err != nil || !bytes.Equal(expected, sink.documents[i]) {
			t.Fatal("live BSON bytes changed")
		}
	}
	l := query.DefaultLimits()
	l.MaxRows = 1
	e.limits = l
	_, err = e.Execute(ctx, req, new(bsonSink))
	assertCode(t, err, "RESOURCE_EXHAUSTED")
	reader, err := mongo.Connect(options.Client().ApplyURI(readerURI))
	if err != nil {
		t.Fatal("reader setup failed")
	}
	defer reader.Disconnect(context.Background())
	if _, err := reader.Database("kelvo_native_test").Collection(collectionName).InsertOne(ctx, bson.D{{Key: "blocked", Value: true}}); err == nil {
		t.Fatal("fixture reader unexpectedly has write grants")
	}
	_, err = e.Execute(ctx, query.Request{Mode: "native", ConnectionID: "mongo", Mongo: &query.MongoRequest{Collection: collectionName, Pipeline: []json.RawMessage{json.RawMessage(`{"$out":"forbidden_write"}`)}}}, new(bsonSink))
	assertCode(t, err, "PERMISSION_DENIED")

	paged := admin.Database("kelvo_native_test").Collection(collectionName + "_paged")
	defer paged.Drop(context.Background())
	pageDocs := make([]any, 300)
	for i := range pageDocs {
		pageDocs[i] = bson.D{{Key: "_id", Value: int32(i)}, {Key: "payload", Value: strings.Repeat("x", 1000)}}
	}
	if _, err := paged.InsertMany(ctx, pageDocs); err != nil {
		t.Fatal("paged fixture insert failed")
	}
	e.limits = query.DefaultLimits()
	pagedRequest := query.Request{Mode: "native", ConnectionID: "mongo", Mongo: &query.MongoRequest{Collection: paged.Name()}}
	pagedSink := new(bsonSink)
	pageStats, err := e.Execute(ctx, pagedRequest, pagedSink)
	if err != nil || pageStats.Rows != 300 || pageStats.Batches != 3 {
		t.Fatalf("live paging failed: %+v %v", pageStats, err)
	}

	// First establish that this fixture can leave a real server cursor open.
	control, err := paged.Aggregate(ctx, mongo.Pipeline{}, options.Aggregate().SetBatchSize(1))
	if err != nil || control.ID() == 0 {
		t.Fatal("open-cursor control unavailable")
	}
	if err := control.Close(ctx); err != nil {
		t.Fatal("control cursor cleanup failed")
	}
	cancelCtx, cancelQuery := context.WithCancel(ctx)
	cancelledSink := &cancelSink{cancel: cancelQuery}
	_, err = e.Execute(cancelCtx, pagedRequest, cancelledSink)
	cancelQuery()
	assertCode(t, err, "CANCELLED")
	if cancelledSink.writes != 1 {
		t.Fatal("cancellation did not occur after the first live batch")
	}
	status, err := admin.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Raw()
	if err != nil {
		t.Fatal("cursor cleanup metrics unavailable")
	}
	metric := status.Lookup("metrics", "cursor", "open", "total")
	var open int64
	switch metric.Type {
	case bson.TypeInt32:
		open = int64(metric.Int32())
	case bson.TypeInt64:
		open = metric.Int64()
	default:
		t.Fatal("unexpected server cursor metric type")
	}
	if open != 0 {
		t.Fatalf("server cursor leaked after cancellation: %d", open)
	}
}
