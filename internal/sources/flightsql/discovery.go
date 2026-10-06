package flightsql

import (
	"context"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func (e *Engine) Inspect(ctx context.Context, spec operations.MetadataSpec, sink query.Sink) (query.Stats, error) {
	if e == nil || ctx == nil || sink == nil || spec.ObjectKind != "" || spec.Limit < 1 || spec.Limit > 10000 || int64(spec.Limit) > e.limits.MaxRows {
		return query.Stats{}, query.NewError("INVALID_ARGUMENT", "Invalid Flight SQL metadata request")
	}
	switch spec.Object {
	case "catalogs", "databases", "schemas", "tables", "columns":
	default:
		return query.Stats{}, query.NewError("UNSUPPORTED", "Flight SQL metadata object is unsupported")
	}
	offset := 0
	if spec.Cursor != "" {
		var err error
		offset, err = strconv.Atoi(spec.Cursor)
		if err != nil || offset < 0 || offset > 10000 || strconv.Itoa(offset) != spec.Cursor {
			return query.Stats{}, query.NewError("INVALID_ARGUMENT", "Invalid Flight SQL metadata cursor")
		}
	}
	for _, v := range []string{spec.Target.Catalog, spec.Target.Schema, spec.Target.Name} {
		if len(v) > 8192 || !utf8.ValidString(v) || strings.ContainsAny(v, "\x00\r\n") {
			return query.Stats{}, query.NewError("INVALID_ARGUMENT", "Invalid Flight SQL metadata target")
		}
	}
	if len(e.sources) != 1 {
		return query.Stats{}, query.NewError("INVALID_ARGUMENT", "Flight SQL discovery requires one selected source")
	}
	id := ""
	for name := range e.sources {
		id = name
	}
	translated := &discoverySink{spec: spec, next: sink, limits: e.limits, offset: offset, allocator: &boundedAllocator{base: memory.NewGoAllocator(), limit: int64(e.limits.MemoryMB) << 18}}
	defer func() {
		if translated.writer != nil {
			translated.writer.Close()
		}
	}()
	stats, err := e.execute(ctx, query.Request{Mode: "native", ConnectionID: id}, translated, &spec)
	if err != nil {
		return stats, err
	}
	if translated.writer == nil {
		return stats, query.NewError("QUERY_FAILED", "Flight SQL discovery omitted schema")
	}
	rows, err := translated.writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = rows.Rows, rows.Bytes, rows.Batches
	return stats, err
}

type discoverySink struct {
	spec                  operations.MetadataSpec
	next                  query.Sink
	limits                query.Limits
	offset, seen, emitted int
	columns               map[string]int
	writer                *rowarrow.Writer
	allocator             *boundedAllocator
}

func (s *discoverySink) Schema(schema *arrow.Schema) error {
	required := []string{"catalog_name"}
	switch s.spec.Object {
	case "schemas":
		required = append(required, "db_schema_name")
	case "tables", "columns":
		required = append(required, "db_schema_name", "table_name", "table_type")
		if s.spec.Object == "columns" {
			required = append(required, "table_schema")
		}
	}
	if schema == nil || len(schema.Fields()) != len(required) {
		return query.NewError("QUERY_FAILED", "Unexpected Flight SQL metadata schema")
	}
	s.columns = map[string]int{}
	for _, name := range required {
		indices := schema.FieldIndices(name)
		if len(indices) != 1 {
			return query.NewError("QUERY_FAILED", "Flight SQL metadata field missing")
		}
		i := indices[0]
		want := arrow.STRING
		if name == "table_schema" {
			want = arrow.BINARY
		}
		if schema.Field(i).Type.ID() != want {
			return query.NewError("QUERY_FAILED", "Flight SQL metadata field has wrong type")
		}
		s.columns[name] = i
	}
	text := func(name string) arrow.Field { return arrow.Field{Name: name, Type: arrow.BinaryTypes.String} }
	fields := []arrow.Field{text("catalog")}
	switch s.spec.Object {
	case "schemas":
		fields = append(fields, text("schema_name"))
	case "tables":
		fields = append(fields, text("schema_name"), text("name"), text("type"))
	case "columns":
		fields = append(fields, text("schema_name"), text("table_name"), text("name"), text("type"), arrow.Field{Name: "position", Type: arrow.PrimitiveTypes.Int64}, arrow.Field{Name: "nullable", Type: arrow.FixedWidthTypes.Boolean})
	}
	var err error
	s.writer, err = rowarrow.NewWriter(arrow.NewSchema(fields, nil), s.limits, s.next)
	return err
}
func (s *discoverySink) string(batch arrow.RecordBatch, name string, row int) (string, error) {
	i, ok := s.columns[name]
	if !ok {
		return "", nil
	}
	column, ok := batch.Column(i).(*array.String)
	if !ok {
		return "", query.NewError("QUERY_FAILED", "Invalid Flight metadata array")
	}
	if column.IsNull(row) {
		return "", nil
	}
	v := column.Value(row)
	if len(v) > 8192 || !utf8.ValidString(v) || strings.ContainsRune(v, 0) {
		return "", query.NewError("QUERY_FAILED", "Invalid Flight metadata value")
	}
	return v, nil
}
func (s *discoverySink) Write(batch arrow.RecordBatch) error {
	if s.writer == nil {
		return query.NewError("QUERY_FAILED", "Flight metadata schema missing")
	}
	for i := 0; i < int(batch.NumRows()); i++ {
		catalog, err := s.string(batch, "catalog_name", i)
		if err != nil {
			return err
		}
		schema, err := s.string(batch, "db_schema_name", i)
		if err != nil {
			return err
		}
		table, err := s.string(batch, "table_name", i)
		if err != nil {
			return err
		}
		typ, err := s.string(batch, "table_type", i)
		if err != nil {
			return err
		}
		if s.spec.Target.Catalog != "" && catalog != s.spec.Target.Catalog || s.spec.Target.Schema != "" && schema != s.spec.Target.Schema || s.spec.Target.Name != "" && table != s.spec.Target.Name {
			continue
		}
		if s.spec.Object == "columns" {
			column, ok := batch.Column(s.columns["table_schema"]).(*array.Binary)
			if !ok || column.IsNull(i) {
				return query.NewError("QUERY_FAILED", "Flight table schema missing")
			}
			raw := column.Value(i)
			if validateSchema(raw) != nil {
				return query.NewError("QUERY_FAILED", "Invalid Flight table schema")
			}
			decoded, err := safeDeserializeSchema(raw, s.allocator)
			if err != nil {
				return err
			}
			for position, field := range decoded.Fields() {
				if err = s.emit([]any{catalog, schema, table, field.Name, field.Type.String(), int64(position + 1), field.Nullable}); err != nil {
					return err
				}
			}
		} else {
			row := []any{catalog}
			switch s.spec.Object {
			case "schemas":
				row = append(row, schema)
			case "tables":
				row = append(row, schema, table, typ)
			}
			if err = s.emit(row); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *discoverySink) emit(row []any) error {
	s.seen++
	if s.seen <= s.offset || s.emitted >= s.spec.Limit {
		return nil
	}
	if err := s.writer.Write(row); err != nil {
		return err
	}
	s.emitted++
	return nil
}
