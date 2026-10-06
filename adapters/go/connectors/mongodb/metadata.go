// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func (s *Session) metadataScope(spec operations.MetadataSpec, limits adapter.Limits) (int, error) {
	if s == nil || spec.Limit < 1 || spec.Limit > 10000 || int64(spec.Limit) > limits.MaxRows ||
		spec.Target.Catalog != "" && spec.Target.Catalog != s.database || spec.Target.Schema != "" && spec.Target.Schema != s.database ||
		spec.Target.Name != "" && !validCollection(spec.Target.Name) ||
		(adapter.Query{Statement: "metadata", MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}).Validate() != nil {
		return 0, adapter.ErrInvalid
	}
	offset := 0
	if spec.Cursor != "" {
		var err error
		offset, err = strconv.Atoi(spec.Cursor)
		if err != nil || offset < 0 || offset > 10000 || strconv.Itoa(offset) != spec.Cursor {
			return 0, adapter.ErrInvalid
		}
	}
	switch spec.Object {
	case "catalogs", "databases", "schemas":
		if spec.Target.Name != "" {
			return 0, adapter.ErrInvalid
		}
	case "tables":
	default:
		// A document collection has no declared SQL column schema. Do not infer
		// columns from a sample and report it as complete catalog metadata.
		return 0, adapter.ErrUnsupported
	}
	return offset, nil
}

func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	started := time.Now()
	var stats adapter.QueryStats
	if ctx == nil || sink == nil || s == nil || s.client == nil {
		return stats, adapter.ErrInvalid
	}
	offset, err := s.metadataScope(spec, limits)
	if err != nil {
		return stats, err
	}
	fields := []string{"catalog"}
	rows := [][]string{{s.database}}
	if spec.Object == "schemas" {
		fields, rows = []string{"catalog", "schema_name"}, [][]string{{s.database, s.database}}
	}
	if spec.Object != "tables" {
		found, err := s.client.ListDatabases(ctx, bson.D{{Key: "name", Value: s.database}}, options.ListDatabases().SetNameOnly(true).SetAuthorizedDatabases(true))
		if err != nil {
			return stats, err
		}
		if len(found.Databases) == 0 {
			rows = nil
		}
		if len(found.Databases) > 1 || len(found.Databases) == 1 && found.Databases[0].Name != s.database {
			return stats, adapter.ErrInvalid
		}
	}
	if spec.Object == "tables" {
		fields, rows = []string{"catalog", "schema_name", "name", "type"}, nil
		filter := bson.D{}
		if spec.Target.Name != "" {
			filter = bson.D{{Key: "name", Value: spec.Target.Name}}
		}
		cursor, err := s.client.Database(s.database).ListCollections(ctx, filter, options.ListCollections().SetBatchSize(128).SetNameOnly(true).SetAuthorizedCollections(true))
		if err != nil {
			return stats, err
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_ = cursor.Close(cleanup)
		}()
		var bytes int64
		scanned := 0
		for cursor.Next(ctx) {
			// Sorted pages require bounded catalog staging. Never silently return
			// a truncated list when this source has more than 10,000 collections.
			if scanned >= 10000 {
				return stats, adapter.ErrLimit
			}
			scanned++
			var item struct {
				Name string `bson:"name"`
				Type string `bson:"type"`
			}
			if cursor.Decode(&item) != nil {
				return stats, adapter.ErrInvalid
			}
			if !validCollection(item.Name) {
				continue
			}
			if spec.Target.Name != "" && item.Name != spec.Target.Name {
				return stats, adapter.ErrInvalid
			}
			kind := "BASE TABLE"
			switch item.Type {
			case "view":
				kind = "VIEW"
			case "collection", "timeseries":
			default:
				return stats, adapter.ErrUnsupported
			}
			bytes += int64(len(item.Name) + len(s.database)*2 + len(kind) + 32)
			if bytes > limits.MaxBytes {
				return stats, adapter.ErrLimit
			}
			rows = append(rows, []string{s.database, s.database, item.Name, kind})
		}
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		if cursor.Err() != nil {
			return stats, cursor.Err()
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i][2] < rows[j][2] })
	}
	start, end := min(offset, len(rows)), min(offset+spec.Limit, len(rows))
	stats, err = writeMetadata(ctx, fields, rows[start:end], limits, sink)
	stats.Elapsed = time.Since(started)
	return stats, err
}

func writeMetadata(ctx context.Context, fields []string, rows [][]string, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	var stats adapter.QueryStats
	columns := make([]arrow.Field, len(fields))
	for i, name := range fields {
		columns[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	}
	schema := arrow.NewSchema(columns, nil)
	if err := sink.Schema(schema); err != nil {
		return stats, err
	}
	for start := 0; start < len(rows); start += limits.BatchRows {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		end := min(start+limits.BatchRows, len(rows))
		arrays := make([]arrow.Array, len(fields))
		for col := range fields {
			builder := array.NewStringBuilder(memory.DefaultAllocator)
			for _, row := range rows[start:end] {
				builder.Append(row[col])
			}
			arrays[col] = builder.NewArray()
			builder.Release()
		}
		record := array.NewRecordBatch(schema, arrays, int64(end-start))
		for _, values := range arrays {
			values.Release()
		}
		bytes := arrowutil.TotalRecordSize(record)
		if bytes > limits.MaxBytes-stats.Bytes || record.NumRows() > limits.MaxRows-stats.Rows {
			record.Release()
			return stats, adapter.ErrLimit
		}
		err := sink.Write(record)
		record.Release()
		if err != nil {
			return stats, err
		}
		stats.Rows += int64(end - start)
		stats.Bytes += bytes
	}
	return stats, ctx.Err()
}
