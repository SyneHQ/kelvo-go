// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"context"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	native "github.com/SYNEHQ/kelvo-go/internal/sources/mongodb"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func (s *Session) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s == nil || s.client == nil || ctx == nil || sink == nil || q.Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	if len(q.Parameters) != 0 {
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	command, err := native.SQLRead(q.Statement)
	if err != nil {
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	pipeline, err := native.ReadPipeline(command.Collection, command.Pipeline, q.MaxRows)
	if err != nil {
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	if err := readStages(pipeline, 0); err != nil {
		return adapter.QueryStats{}, err
	}
	return s.aggregate(ctx, command.Collection, pipeline, adapter.Limits{MaxRows: q.MaxRows, MaxBytes: q.MaxBytes, BatchRows: q.BatchRows}, sink)
}

func (s *Session) RunNative(ctx context.Context, request adapter.Native, sink adapter.Sink) (adapter.NativeResult, error) {
	result := adapter.NativeResult{Outcome: operations.Rejected, Effect: operations.EffectNone}
	if s == nil || s.client == nil || ctx == nil || (request.Kind == operations.NativeRead || request.Spec.ReturnResult) && sink == nil || request.Kind == operations.NativeExecute && !request.Spec.ReturnResult && sink != nil {
		return result, adapter.ErrInvalid
	}
	plan, err := parseCommand(request)
	if err != nil {
		return result, err
	}
	if ctx.Err() != nil {
		result.Outcome = operations.CancelledBeforeStart
		return result, ctx.Err()
	}
	if request.Kind == operations.NativeExecute {
		result, details, err := s.executeDetailed(ctx, plan)
		if err == nil && request.Spec.ReturnResult {
			var raw []byte
			raw, err = bson.Marshal(details)
			if err == nil {
				result.Stats, err = streamDocuments(ctx, &singleDocument{raw: raw}, request.Limits, sink)
			}
		}
		return result, err
	}
	result.Outcome = operations.Failed
	if plan.command == "count" {
		var count int64
		count, err = s.client.Database(s.database).Collection(plan.collection).CountDocuments(ctx, plan.filter)
		if err == nil {
			var raw []byte
			raw, err = bson.Marshal(bson.D{{Key: "count", Value: count}})
			if err == nil {
				result.Stats, err = streamDocuments(ctx, &singleDocument{raw: raw}, request.Limits, sink)
			}
		}
	} else if plan.command == "list_indexes" {
		var cursor *mongo.Cursor
		cursor, err = s.client.Database(s.database).Collection(plan.collection).Indexes().List(ctx, options.ListIndexes().SetBatchSize(32))
		if err == nil {
			result.Stats, err = stream(ctx, cursor, request.Limits, sink)
		}
	} else {
		result.Stats, err = s.aggregate(ctx, plan.collection, plan.pipeline, request.Limits, sink)
	}
	if err == nil {
		result.Outcome = operations.Completed
	}
	return result, err
}

func (s *Session) aggregate(ctx context.Context, collection string, pipeline mongo.Pipeline, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	cursor, err := s.client.Database(s.database).Collection(collection).Aggregate(ctx, pipeline, options.Aggregate().SetBatchSize(int32(min(limits.BatchRows, 128))).SetAllowDiskUse(false))
	if err != nil {
		return adapter.QueryStats{}, err
	}
	return stream(ctx, cursor, limits, sink)
}

type documentCursor struct{ *mongo.Cursor }

func (c documentCursor) Document() bson.Raw { return c.Current }

func stream(ctx context.Context, cursor *mongo.Cursor, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = cursor.Close(cleanup)
	}()
	return streamDocuments(ctx, documentCursor{cursor}, limits, sink)
}

type singleDocument struct {
	raw      bson.Raw
	consumed bool
}

func (c *singleDocument) Next(ctx context.Context) bool {
	if c.consumed || ctx.Err() != nil {
		return false
	}
	c.consumed = true
	return true
}
func (c *singleDocument) Document() bson.Raw { return c.raw }
func (c *singleDocument) Err() error         { return nil }

func streamDocuments(ctx context.Context, cursor native.DocumentCursor, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	started := time.Now()
	var stats query.Stats
	timeout := 5 * time.Minute
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	if timeout <= 0 {
		return adapter.QueryStats{}, context.DeadlineExceeded
	}
	err := native.StreamDocuments(ctx, cursor, query.Limits{MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, Timeout: timeout, MemoryMB: 64, Threads: 1, MaxTempMB: 1}, splitSink{next: sink, rows: int64(limits.BatchRows)}, &stats)
	return adapter.QueryStats{Rows: stats.Rows, Bytes: stats.Bytes, Elapsed: time.Since(started)}, err
}

type splitSink struct {
	next adapter.Sink
	rows int64
}

func (s splitSink) Schema(schema *arrow.Schema) error { return s.next.Schema(schema) }
func (s splitSink) Write(record arrow.RecordBatch) error {
	for start := int64(0); start < record.NumRows(); start += s.rows {
		part := record.NewSlice(start, min(start+s.rows, record.NumRows()))
		err := s.next.Write(part)
		part.Release()
		if err != nil {
			return err
		}
	}
	return nil
}
