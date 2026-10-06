// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"context"
	"encoding/json"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/provider"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"time"
)

func writeObjects(ctx context.Context, rows []map[string]any, limits adapter.Limits, sink adapter.Sink) (stats adapter.QueryStats, err error) {
	started := time.Now()
	defer func() { stats.Elapsed = time.Since(started) }()
	if int64(len(rows)) > limits.MaxRows {
		return stats, adapter.ErrLimit
	}
	meta := arrow.NewMetadata([]string{"kelvo_document_format"}, []string{provider.ResultFormat})
	schema := arrow.NewSchema([]arrow.Field{{Name: "document", Type: arrow.BinaryTypes.Binary}}, &meta)
	if err = sink.Schema(schema); err != nil {
		return stats, err
	}
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer b.Release()
	flush := func() error {
		values := b.NewArray()
		defer values.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{values}, int64(values.Len()))
		defer record.Release()
		if err := sink.Write(record); err != nil {
			return err
		}
		stats.Rows += record.NumRows()
		return nil
	}
	for _, row := range rows {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		raw, e := json.Marshal(row)
		if e != nil {
			return stats, adapter.ErrInvalid
		}
		size := int64(len(raw) + 8)
		if size > limits.MaxBytes-stats.Bytes {
			return stats, adapter.ErrLimit
		}
		stats.Bytes += size
		b.Append(raw)
		if b.Len() == limits.BatchRows {
			if err = flush(); err != nil {
				return stats, err
			}
		}
	}
	if b.Len() > 0 {
		err = flush()
	}
	return stats, err
}
