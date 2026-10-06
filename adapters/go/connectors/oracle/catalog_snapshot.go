package oracle

import (
	"context"
	"encoding/json"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	"strings"
	"time"
)

func (s *Session) inspectObjects(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	started := time.Now()
	if ctx == nil || s == nil || s.Session == nil || s.Pool == nil || sink == nil || spec.Object != "objects" || spec.Cursor != "" || spec.Limit < 1 || spec.Limit > Limit || int64(spec.Limit) > limits.MaxRows || limits.MaxBytes < 1 || spec.Target.Catalog != "" && spec.Target.Catalog != s.database || s.schema != "" && spec.Target.Schema != "" && spec.Target.Schema != s.schema || len(spec.Target.Schema) > 128 || len(spec.Target.Name) > 256 || strings.ContainsAny(spec.Target.Schema+spec.Target.Name, "\x00\r\n") {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	schema := spec.Target.Schema
	if schema == "" {
		schema = s.schema
	}
	result, err := inspectOracle(ctx, s.Pool, s.database, schema, spec.ObjectKind, spec.Target.Name, spec.Limit)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	raw, err := json.Marshal(result)
	if err != nil || int64(len(raw))+8 > limits.MaxBytes || len(raw) > 4<<20 {
		return adapter.QueryStats{}, adapter.ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return adapter.QueryStats{}, err
	}
	layout := arrow.NewSchema([]arrow.Field{{Name: "catalog_document", Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	builder.Append(string(raw))
	column := builder.NewArray()
	builder.Release()
	defer column.Release()
	record := array.NewRecordBatch(layout, []arrow.Array{column}, 1)
	defer record.Release()
	size := arrowutil.TotalRecordSize(record)
	if size > limits.MaxBytes {
		return adapter.QueryStats{}, adapter.ErrLimit
	}
	if err = sink.Schema(layout); err != nil {
		return adapter.QueryStats{}, err
	}
	if err = sink.Write(record); err != nil {
		return adapter.QueryStats{}, err
	}
	return adapter.QueryStats{Rows: 1, Bytes: size, Elapsed: time.Since(started)}, nil
}
