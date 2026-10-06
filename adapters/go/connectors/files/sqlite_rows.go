//go:build cgo

package files

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func jsonParameter(kind string, value any) operations.Parameter {
	raw, _ := json.Marshal(value)
	return operations.Parameter{Type: kind, Value: raw}
}

// SQLite expressions have dynamic storage types. Inspect the bounded result
// before publishing a schema; never coerce a later integer to an earlier float.
// Mixed storage types require an explicit SQL CAST from the caller.
func streamSQLiteRows(ctx context.Context, rows *sql.Rows, q adapter.Query, sink adapter.Sink) (stats adapter.QueryStats, err error) {
	columns, err := rows.ColumnTypes()
	if err != nil {
		return stats, err
	}
	fields := make([]arrow.Field, len(columns))
	seen := map[string]bool{}
	for i, col := range columns {
		if col.Name() == "" || !utf8.ValidString(col.Name()) || seen[col.Name()] {
			return stats, adapter.ErrUnsupported
		}
		seen[col.Name()] = true
		switch strings.ToUpper(col.DatabaseTypeName()) {
		// This driver coerces these declarations while reading. CAST to a chosen
		// SQL storage type so malformed dates or non-boolean integers cannot vanish.
		case "DATE", "DATETIME", "TIMESTAMP", "BOOLEAN":
			return stats, adapter.ErrUnsupported
		}
		fields[i] = arrow.Field{Name: col.Name(), Type: arrow.Null, Nullable: true}
	}
	var records [][]any
	for rows.Next() {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		if int64(len(records)) >= q.MaxRows {
			return stats, adapter.ErrLimit
		}
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range pointers {
			pointers[i] = &values[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			return stats, err
		}
		for i, value := range values {
			typ := arrow.DataType(arrow.Null)
			size := int64(17)
			switch v := value.(type) {
			case nil:
			case int64:
				typ = arrow.PrimitiveTypes.Int64
			case float64:
				if math.IsNaN(v) || math.IsInf(v, 0) {
					return stats, adapter.ErrUnsupported
				}
				typ = arrow.PrimitiveTypes.Float64
			case string:
				if !utf8.ValidString(v) {
					return stats, adapter.ErrUnsupported
				}
				typ = arrow.BinaryTypes.String
				size += int64(len(v))
			case []byte:
				typ = arrow.BinaryTypes.Binary
				values[i] = append([]byte{}, v...)
				size += int64(len(v))
			default:
				return stats, adapter.ErrUnsupported
			}
			if size > q.MaxBytes-stats.Bytes {
				return stats, adapter.ErrLimit
			}
			stats.Bytes += size
			if typ.ID() != arrow.NULL {
				if fields[i].Type.ID() == arrow.NULL {
					fields[i].Type = typ
				} else if !arrow.TypeEqual(fields[i].Type, typ) {
					return stats, adapter.ErrUnsupported
				}
			}
		}
		records = append(records, values)
	}
	if err = rows.Err(); err != nil {
		return stats, err
	}
	schema := arrow.NewSchema(fields, nil)
	if err = sink.Schema(schema); err != nil {
		return stats, err
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	flush := func() error {
		record := b.NewRecordBatch()
		defer record.Release()
		if err := sink.Write(record); err != nil {
			return err
		}
		stats.Rows += record.NumRows()
		return nil
	}
	for _, record := range records {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		for i, value := range record {
			builder := b.Field(i)
			if value == nil {
				builder.AppendNull()
				continue
			}
			switch field := builder.(type) {
			case *array.Int64Builder:
				field.Append(value.(int64))
			case *array.Float64Builder:
				field.Append(value.(float64))
			case *array.StringBuilder:
				field.Append(value.(string))
			case *array.BinaryBuilder:
				field.Append(value.([]byte))
			default:
				return stats, adapter.ErrUnsupported
			}
		}
		if b.Field(0).Len() >= q.BatchRows {
			if err = flush(); err != nil {
				return stats, err
			}
		}
	}
	if len(records) > 0 && b.Field(0).Len() > 0 {
		err = flush()
	}
	return stats, err
}
