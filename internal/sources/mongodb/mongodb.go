// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package mongodb streams native, read-only aggregation results as exact BSON
// documents carried in Arrow Binary. It deliberately avoids schema inference.
package mongodb

import (
	"context"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Engine struct {
	sources map[string]catalog.Source
	limits  query.Limits
}

func New(config catalog.Config, limits query.Limits) (*Engine, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	e := &Engine{sources: make(map[string]catalog.Source), limits: limits}
	for _, source := range config.Sources {
		if source.Type != "mongodb" {
			continue
		}
		if !catalog.ValidID(source.ID) || source.DSNEnv == "" || catalog.ValidateEnvironment(source.DSNEnv) != nil || len(source.Options) != 1 || !validDatabase(source.Options["database"]) {
			return nil, query.NewError("CONFIGURATION_ERROR", "MongoDB requires a registered source, credential reference and database option")
		}
		if _, duplicate := e.sources[source.ID]; duplicate {
			return nil, query.NewError("CONFIGURATION_ERROR", "Duplicate MongoDB source")
		}
		// Retain only supported public options and avoid aliasing caller maps.
		source.Options = map[string]string{"database": source.Options["database"]}
		e.sources[source.ID] = source
	}
	return e, nil
}

// Clients are scoped to a query and disconnected even on cancellation. The
// disposable worker architecture does not retain pools between queries.
func (e *Engine) Close() error { return nil }

func (e *Engine) Execute(parent context.Context, request query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	stats.Backend, stats.EngineStreaming = "mongodb", true
	defer func() { stats.DurationNS = time.Since(started).Nanoseconds() }()
	if err := query.ValidateRequest(request); err != nil {
		return stats, err
	}
	if sink == nil || request.Mode != "native" {
		return stats, query.NewError("INVALID_ARGUMENT", "MongoDB requires a native query and result sink")
	}
	command := request.Mongo
	if command == nil {
		command, err = sqlRequest(request)
		if err != nil {
			return stats, err
		}
	}
	pipeline, err := readPipeline(command, e.limits.MaxRows)
	if err != nil {
		return stats, err
	}
	source, found := e.sources[request.ConnectionID]
	if !found {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return stats, query.PublicError(err)
	}
	uri := os.Getenv(source.DSNEnv)
	if (!strings.HasPrefix(uri, "mongodb://") && !strings.HasPrefix(uri, "mongodb+srv://")) || len(uri) > 32<<10 {
		return stats, query.NewError("CONFIGURATION_ERROR", "MongoDB connection details are unavailable or invalid")
	}
	clientOptions := options.Client().ApplyURI(uri).
		SetAppName("kelvo-go").SetMaxPoolSize(1).SetMinPoolSize(0).SetMaxConnecting(1).
		SetRetryWrites(false).SetTimeout(e.limits.Timeout).
		SetConnectTimeout(min(5*time.Second, e.limits.Timeout)).
		SetServerSelectionTimeout(min(10*time.Second, e.limits.Timeout))
	if err := clientOptions.Validate(); err != nil {
		return stats, query.NewError("CONFIGURATION_ERROR", "MongoDB connection details are unavailable or invalid")
	}
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return stats, query.NewError("CONFIGURATION_ERROR", "Could not initialize MongoDB client")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_ = client.Disconnect(cleanup)
	}()
	// The driver's context deadline also sets server maxTimeMS for aggregation.
	// Disable server spill explicitly, rather than inheriting allowDiskUseByDefault.
	cursor, err := client.Database(source.Options["database"]).Collection(command.Collection).
		Aggregate(ctx, pipeline, options.Aggregate().SetBatchSize(128).SetAllowDiskUse(false))
	if err != nil {
		return stats, sourceError(ctx)
	}
	defer func() {
		// A fresh bounded context allows killCursors after the query was cancelled.
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_ = cursor.Close(cleanup)
	}()
	stats.PrepareNS = time.Since(started).Nanoseconds()
	err = streamCursor(ctx, &nativeCursor{cursor}, e.limits, sink, &stats)
	return stats, err
}

func validDatabase(name string) bool {
	return name != "" && len(name) <= 63 && utf8.ValidString(name) && !strings.ContainsAny(name, "/\\.\"$ \x00")
}

func sourceError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return query.PublicError(err)
	}
	return query.NewError("QUERY_FAILED", "MongoDB aggregation failed")
}

type bsonCursor interface {
	Next(context.Context) bool
	Document() bson.Raw
	Err() error
}
type nativeCursor struct{ *mongo.Cursor }

func (c *nativeCursor) Document() bson.Raw { return c.Current }

func resultSchema() *arrow.Schema {
	metadata := arrow.NewMetadata([]string{"kelvo.logical_type", "content_type"}, []string{"bson", "application/bson"})
	return arrow.NewSchema([]arrow.Field{{Name: "document_bson", Type: arrow.BinaryTypes.Binary, Nullable: false, Metadata: metadata}}, nil)
}

func streamCursor(ctx context.Context, cursor bsonCursor, limits query.Limits, sink query.Sink, stats *query.Stats) error {
	schema := resultSchema()
	if err := sink.Schema(schema); err != nil {
		return query.PublicError(err)
	}
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	// Bound Arrow staging separately from the driver's wire batch. Large BSON
	// documents are permitted as singleton batches up to one quarter of the
	// configured client budget. Container memory limits remain the RSS boundary.
	maxDocumentBytes := min(int64(limits.MemoryMB)*(1<<20)/4, limits.MaxBytes-8, 16<<20)
	var bufferedBytes int64
	flush := func() error {
		if builder.Len() == 0 {
			return nil
		}
		values := builder.NewArray()
		record := array.NewRecordBatch(schema, []arrow.Array{values}, int64(values.Len()))
		values.Release()
		defer record.Release()
		size := arrowutil.TotalRecordSize(record)
		if size > limits.MaxBytes-stats.Bytes {
			return query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds byte limit")
		}
		if err := sink.Write(record); err != nil {
			return query.PublicError(err)
		}
		stats.Rows += record.NumRows()
		stats.Bytes += size
		stats.Batches++
		bufferedBytes = 0
		return nil
	}
	for cursor.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return query.PublicError(err)
		}
		document := cursor.Document()
		if int64(len(document)) > maxDocumentBytes {
			return query.NewError("RESOURCE_EXHAUSTED", "MongoDB document exceeds result buffer budget")
		}
		if err := document.Validate(); err != nil {
			return query.NewError("QUERY_FAILED", "MongoDB returned invalid BSON")
		}
		if stats.Rows+int64(builder.Len()) >= limits.MaxRows {
			return query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds row limit")
		}
		if builder.Len() > 0 && bufferedBytes+int64(len(document))+4 > 1<<20 {
			if err := flush(); err != nil {
				return err
			}
		}
		// One uint32 offset per row, plus the terminal offset. All documents are
		// non-null; the encoded BSON itself preserves null versus absent fields.
		if bufferedBytes+int64(len(document))+8 > limits.MaxBytes-stats.Bytes {
			return query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds byte limit")
		}
		builder.Append(document)
		bufferedBytes += int64(len(document)) + 4
		if builder.Len() >= 128 || bufferedBytes >= 1<<20 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return query.PublicError(err)
	}
	if cursor.Err() != nil {
		return sourceError(ctx)
	}
	return flush()
}

var _ query.Executor = (*Engine)(nil)
