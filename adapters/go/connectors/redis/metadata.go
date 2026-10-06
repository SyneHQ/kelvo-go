package redis

import (
	"context"
	"strconv"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	if ctx == nil || sink == nil || spec.ObjectKind != "" || spec.Limit < 1 || int64(spec.Limit) > limits.MaxRows || spec.Target.Catalog != "" && spec.Target.Catalog != s.namespace || spec.Target.Schema != "" && spec.Target.Schema != s.namespace || (adapter.Query{Statement: "metadata", MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}).Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	offset := int64(0)
	if spec.Cursor != "" {
		var err error
		offset, err = strconv.ParseInt(spec.Cursor, 10, 32)
		if err != nil || offset < 0 || offset > 1_000_000 || strconv.FormatInt(offset, 10) != spec.Cursor {
			return adapter.QueryStats{}, adapter.ErrInvalid
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	var fields []arrow.Field
	var rows [][]any
	textFields := func(names ...string) []arrow.Field {
		out := make([]arrow.Field, len(names))
		for i, name := range names {
			out[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
		}
		return out
	}
	switch spec.Object {
	case "catalogs", "databases":
		fields = textFields("catalog")
		if spec.Target.Name != "" {
			return adapter.QueryStats{}, adapter.ErrInvalid
		}
		if offset == 0 {
			rows = [][]any{{s.namespace}}
		}
	case "schemas":
		fields = textFields("catalog", "schema_name")
		if spec.Target.Name != "" {
			return adapter.QueryStats{}, adapter.ErrInvalid
		}
		if offset == 0 {
			rows = [][]any{{s.namespace, s.namespace}}
		}
	case "tables":
		fields = textFields("catalog", "schema_name", "name", "type")
		keys := []string{spec.Target.Name}
		if spec.Target.Name == "" {
			var err error
			keys, err = s.metadataKeys(ctx, offset+int64(spec.Limit), limits.MaxBytes)
			if err != nil {
				return adapter.QueryStats{}, err
			}
		}
		for index, key := range keys {
			if int64(index) < offset || len(rows) >= spec.Limit {
				continue
			}
			reply, err := s.command(ctx, []string{"TYPE", key}, 4096, 8)
			if err != nil {
				return adapter.QueryStats{}, err
			}
			kind, ok := redisString(reply)
			if !ok {
				return adapter.QueryStats{}, adapter.ErrInvalid
			}
			if kind == "none" {
				continue // A key can expire after SCAN.
			}
			rows = append(rows, []any{s.namespace, s.namespace, key, kind})
		}
	case "columns":
		if spec.Target.Name == "" {
			return adapter.QueryStats{}, adapter.ErrUnsupported
		}
		fields = textFields("schema_name", "table_name", "name", "type")
		fields = append(fields, arrow.Field{Name: "position", Type: arrow.PrimitiveTypes.Int64}, arrow.Field{Name: "nullable", Type: arrow.BinaryTypes.String})
		reply, err := s.command(ctx, []string{"TYPE", spec.Target.Name}, 4096, 8)
		if err != nil {
			return adapter.QueryStats{}, err
		}
		kind, ok := redisString(reply)
		if !ok {
			return adapter.QueryStats{}, adapter.ErrInvalid
		}
		if kind == "hash" {
			names, err := s.hashFields(ctx, spec.Target.Name, offset+int64(spec.Limit), limits.MaxBytes)
			if err != nil {
				return adapter.QueryStats{}, err
			}
			for i, name := range names {
				if int64(i) >= offset && len(rows) < spec.Limit {
					rows = append(rows, []any{s.namespace, spec.Target.Name, name, "string", int64(i + 1), "YES"})
				}
			}
		} else if kind != "none" && offset == 0 {
			rows = [][]any{{s.namespace, spec.Target.Name, "value", kind, int64(1), "YES"}}
		}
	default:
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	return writeMetadata(ctx, fields, rows, limits, sink)
}

// Each page replays a bounded SCAN from cursor zero to the requested offset.
// This matches the metadata API's numeric cursor without storing worker state.
// Redis SCAN is not a snapshot; concurrent key expiry/addition can change pages.
func (s *Session) metadataKeys(ctx context.Context, needed, maxBytes int64) ([]string, error) {
	return s.scanNames(ctx, "SCAN", "", needed, maxBytes)
}
func (s *Session) hashFields(ctx context.Context, key string, needed, maxBytes int64) ([]string, error) {
	return s.scanNames(ctx, "HSCAN", key, needed, maxBytes)
}
func (s *Session) scanNames(ctx context.Context, command, key string, needed, maxBytes int64) ([]string, error) {
	if needed < 1 || needed > 1_000_000 {
		return nil, adapter.ErrLimit
	}
	cursor := "0"
	seen := make(map[string]struct{})
	names := make([]string, 0)
	var consumed int64
	for page := 0; page < 10000; page++ {
		args := []string{command, cursor, "COUNT", "128"}
		if key != "" {
			args = []string{command, key, cursor, "COUNT", "128"}
		}
		reply, err := s.command(ctx, args, maxBytes-consumed, 1_000_000)
		if err != nil {
			return nil, err
		}
		parts, ok := reply.([]any)
		if !ok || len(parts) != 2 {
			return nil, adapter.ErrInvalid
		}
		cursor, ok = redisString(parts[0])
		if _, err := strconv.ParseUint(cursor, 10, 64); !ok || err != nil {
			return nil, adapter.ErrInvalid
		}
		values, ok := parts[1].([]any)
		if !ok || command == "HSCAN" && len(values)%2 != 0 {
			return nil, adapter.ErrInvalid
		}
		step := 1
		if command == "HSCAN" {
			step = 2
		}
		for i := 0; i < len(values); i += step {
			name, ok := redisString(values[i])
			if !ok || !utf8.ValidString(name) {
				return nil, adapter.ErrUnsupported
			}
			if _, exists := seen[name]; exists {
				continue
			}
			consumed += int64(len(name) + 16)
			if consumed > maxBytes {
				return nil, adapter.ErrLimit
			}
			seen[name] = struct{}{}
			names = append(names, name)
			if int64(len(names)) == needed {
				return names, nil
			}
		}
		if cursor == "0" {
			return names, nil
		}
	}
	return nil, adapter.ErrLimit
}

func writeMetadata(ctx context.Context, fields []arrow.Field, rows [][]any, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	var stats adapter.QueryStats
	schema := arrow.NewSchema(fields, nil)
	if err := sink.Schema(schema); err != nil {
		return stats, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i, row := range rows {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		for column, value := range row {
			switch value := value.(type) {
			case string:
				stats.Bytes += int64(len(value) + 8)
				if stats.Bytes > limits.MaxBytes {
					return stats, adapter.ErrLimit
				}
				builder.Field(column).(*array.StringBuilder).Append(value)
			case int64:
				stats.Bytes += 8
				if stats.Bytes > limits.MaxBytes {
					return stats, adapter.ErrLimit
				}
				builder.Field(column).(*array.Int64Builder).Append(value)
			}
		}
		if (i+1)%limits.BatchRows == 0 || i == len(rows)-1 {
			record := builder.NewRecordBatch()
			err := sink.Write(record)
			stats.Rows += record.NumRows()
			record.Release()
			if err != nil {
				return stats, err
			}
		}
	}
	return stats, nil
}
