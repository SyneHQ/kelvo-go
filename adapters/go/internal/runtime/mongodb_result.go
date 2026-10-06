package runtime

import (
	"encoding/json"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/provider"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Conversion stays beside the MongoDB driver. The gateway receives canonical
// Extended JSON and has no BSON library or customer connector dependency.
type mongoJSONSink struct {
	next            adapter.Sink
	input, output   *arrow.Schema
	bytes, maxBytes int64
}

func (s *mongoJSONSink) Schema(schema *arrow.Schema) error {
	if s.input != nil || schema == nil || schema.NumFields() != 1 {
		return adapter.ErrInvalid
	}
	field := schema.Field(0)
	if field.Name != "document_bson" || field.Nullable || field.Type.ID() != arrow.BINARY {
		return adapter.ErrInvalid
	}
	if kind, ok := field.Metadata.GetValue("kelvo.logical_type"); !ok || kind != "bson" {
		return adapter.ErrInvalid
	}
	meta := arrow.NewMetadata([]string{"kelvo_document_format", "source_format"}, []string{provider.ResultFormat, "mongodb_canonical_ejson"})
	s.input, s.output = schema, arrow.NewSchema([]arrow.Field{{Name: "document", Type: arrow.BinaryTypes.Binary}}, &meta)
	return s.next.Schema(s.output)
}
func (s *mongoJSONSink) Write(record arrow.RecordBatch) error {
	if s.input == nil || record == nil || !record.Schema().Equal(s.input) || !record.Schema().Metadata().Equal(s.input.Metadata()) {
		return adapter.ErrInvalid
	}
	column, ok := record.Column(0).(*array.Binary)
	if !ok {
		return adapter.ErrInvalid
	}
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	for i := 0; i < column.Len(); i++ {
		if column.IsNull(i) {
			return adapter.ErrInvalid
		}
		raw, err := bson.MarshalExtJSON(bson.Raw(column.Value(i)), true, false)
		if err != nil {
			return adapter.ErrInvalid
		}
		if int64(len(raw)+8) > s.maxBytes-s.bytes {
			return adapter.ErrLimit
		}
		var document map[string]json.RawMessage
		if provider.DecodeDocument(raw, &document, int(min(s.maxBytes, 8<<20))) != nil || document == nil {
			return adapter.ErrInvalid
		}
		s.bytes += int64(len(raw) + 8)
		builder.Append(raw)
	}
	values := builder.NewArray()
	defer values.Release()
	out := array.NewRecordBatch(s.output, []arrow.Array{values}, record.NumRows())
	defer out.Release()
	return s.next.Write(out)
}
