package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoDocumentSink struct {
	schema    *arrow.Schema
	documents [][]byte
}

func (s *mongoDocumentSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *mongoDocumentSink) Write(record arrow.RecordBatch) error {
	col := record.Column(0).(*array.Binary)
	for i := 0; i < col.Len(); i++ {
		s.documents = append(s.documents, bytes.Clone(col.Value(i)))
	}
	return nil
}

func writeMongoResult(sink adapter.Sink, raw []byte) error {
	meta := arrow.NewMetadata([]string{"kelvo.logical_type"}, []string{"bson"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "document_bson", Type: arrow.BinaryTypes.Binary, Metadata: meta}}, nil)
	if err := sink.Schema(schema); err != nil {
		return err
	}
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer b.Release()
	b.Append(raw)
	values := b.NewArray()
	defer values.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	defer record.Release()
	return sink.Write(record)
}

func TestMongoResultKeepsCanonicalBSONTypes(t *testing.T) {
	decimal, _ := bson.ParseDecimal128("12345678901234567890.123456789")
	id, _ := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: id}, {Key: "int", Value: int64(9007199254740993)},
		{Key: "decimal", Value: decimal}, {Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 2}}},
		{Key: "time", Value: bson.DateTime(123456789)}, {Key: "nil", Value: nil},
		{Key: "Name", Value: "UPPER"}, {Key: "name", Value: "lower"},
	})
	if err != nil {
		t.Fatal(err)
	}
	next := &mongoDocumentSink{}
	sink := &mongoJSONSink{next: next, maxBytes: 1 << 20}
	if err := writeMongoResult(sink, raw); err != nil || len(next.documents) != 1 {
		t.Fatal(err)
	}
	want, _ := bson.MarshalExtJSON(bson.Raw(raw), true, false)
	if !bytes.Equal(want, next.documents[0]) {
		t.Fatal("canonical type representation changed")
	}
	for _, text := range []string{`"$oid"`, `"9007199254740993"`, `"12345678901234567890.123456789"`, `"subType":"80"`, `"$date"`, `"nil":null`, `"Name":"UPPER"`, `"name":"lower"`} {
		if !bytes.Contains(next.documents[0], []byte(text)) {
			t.Fatal("value lost", text)
		}
	}
	var restored bson.D
	if bson.UnmarshalExtJSON(next.documents[0], true, &restored) != nil {
		t.Fatal("invalid canonical Extended JSON")
	}
	restoredRaw, _ := bson.Marshal(restored)
	if !bytes.Equal(raw, restoredRaw) {
		t.Fatal("BSON did not round trip")
	}
	clear(raw)
	if !bytes.Equal(want, next.documents[0]) {
		t.Fatal("retained borrowed result storage")
	}
}

func TestMongoResultEnforcesDecodedBudgetAndDocuments(t *testing.T) {
	raw, _ := bson.Marshal(bson.D{{Key: "payload", Value: strings.Repeat("x", 200)}})
	sink := &mongoJSONSink{next: &mongoDocumentSink{}, maxBytes: 64}
	if err := writeMongoResult(sink, raw); !errors.Is(err, adapter.ErrLimit) {
		t.Fatal("decoded limit ignored", err)
	}
	for _, raw := range [][]byte{{1, 2, 3}, func() []byte { b, _ := bson.Marshal(bson.D{{Key: "id", Value: 1}, {Key: "id", Value: 2}}); return b }()} {
		if err := writeMongoResult(&mongoJSONSink{next: &mongoDocumentSink{}, maxBytes: 1 << 20}, raw); err == nil {
			t.Fatal("malformed or duplicate BSON accepted")
		}
	}
}

func TestMongoMutationResultDeliveryCannotUndoConfirmedWrite(t *testing.T) {
	for _, fail := range []bool{false, true} {
		input := runtimeRequest(t, operations.QueryRead)
		input.Source.Engine = "mongodb"
		input.Request.Kind = operations.NativeExecute
		input.Request.IdempotencyKey = "mongo-write"
		input.Request.Spec = operations.Spec{Native: &operations.NativeSpec{Provider: "mongodb", Command: "insert_one", ReturnResult: true}}
		input.RequestSHA256, _ = operations.Digest(input.Request)
		raw, _ := json.Marshal(input)
		session := &nativeProcessSession{processSession: processSession{rows: 1, bsonOutput: true}, nativeResult: adapter.NativeResult{Outcome: operations.Completed, Effect: operations.EffectCommitted}}
		var out, receiptBytes bytes.Buffer
		var output io.Writer = &out
		if fail {
			output = nativeFailWriter{}
		}
		err := (Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
			return session, nil
		}}).Run(context.Background(), bytes.NewReader(raw), output, &receiptBytes)
		var receipt operations.Receipt
		if operations.DecodeStrict(receiptBytes.Bytes(), &receipt, operations.MaxReceiptBytes) != nil || receipt.Validate() != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted || session.nativeCalls != 1 {
			t.Fatal("confirmed write lost or replayed", receipt, err)
		}
		if fail && (err == nil || receipt.Result != nil) || !fail && (err != nil || receipt.Result == nil || receipt.Result.Rows != 1) {
			t.Fatal("invalid result receipt", receipt, err)
		}
	}
}
