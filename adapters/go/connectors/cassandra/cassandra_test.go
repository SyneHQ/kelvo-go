package cassandra

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gopkg.in/inf.v0"
)

type fakeIterator struct {
	rows   []map[string]any
	index  int
	closed int
	err    error
}

func (f *fakeIterator) Columns() []column {
	return []column{{Name: "id", SourceType: "bigint"}}
}
func (f *fakeIterator) Scan(row map[string]any) bool {
	if f.index == len(f.rows) {
		return false
	}
	for k, v := range f.rows[f.index] {
		row[k] = v
	}
	f.index++
	return true
}
func (f *fakeIterator) Close() error { f.closed++; return f.err }

type fakeBackend struct {
	iter    *fakeIterator
	queries int
	closed  int
}

type testSink struct{ write func(arrow.RecordBatch) error }

func (s testSink) Schema(*arrow.Schema) error           { return nil }
func (s testSink) Write(record arrow.RecordBatch) error { return s.write(record) }

func (f *fakeBackend) Query(context.Context, string, []any, int) iterator { f.queries++; return f.iter }
func (f *fakeBackend) Close()                                             { f.closed++ }

func TestBoundedQueryPreservesValuesAndCloses(t *testing.T) {
	iter := &fakeIterator{rows: []map[string]any{{"id": int64(9007199254740993)}, {"id": nil}}}
	backend := &fakeBackend{iter: iter}
	session := &Session{backend: backend}
	var received []any
	stats, err := session.Query(context.Background(), adapter.Query{Statement: "SELECT id FROM items", MaxRows: 10, MaxBytes: 4096, BatchRows: 1}, testSink{write: func(record arrow.RecordBatch) error {
		column := record.Column(0).(*array.Int64)
		for i := 0; i < column.Len(); i++ {
			if column.IsNull(i) {
				received = append(received, nil)
			} else {
				received = append(received, column.Value(i))
			}
		}
		return nil
	}})
	if err != nil || stats.Rows != 2 || stats.Bytes != 34 || iter.closed != 1 {
		t.Fatalf("result %+v %v; close=%d", stats, err, iter.closed)
	}
	want := []any{int64(9007199254740993), nil}
	if !reflect.DeepEqual(received, want) {
		t.Fatalf("values: %+v", received)
	}
	_ = session.Close()
	_ = session.Close()
	if backend.closed != 1 {
		t.Fatal("session close not idempotent")
	}
}

func TestQueryLimitsAndSinkFailureCloseIterator(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rows    int64
		bytes   int64
		sinkErr error
		want    error
	}{
		{"rows", 1, 4096, nil, adapter.ErrLimit},
		{"bytes", 10, 1, nil, adapter.ErrLimit},
		{"sink", 10, 4096, errors.New("sink closed"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iter := &fakeIterator{rows: []map[string]any{{"id": int64(1)}, {"id": int64(2)}}}
			session := &Session{backend: &fakeBackend{iter: iter}}
			_, err := session.Query(context.Background(), adapter.Query{Statement: "SELECT id FROM t", MaxRows: tc.rows, MaxBytes: tc.bytes, BatchRows: 1}, testSink{write: func(arrow.RecordBatch) error { return tc.sinkErr }})
			if tc.want == nil {
				tc.want = tc.sinkErr
			}
			if !errors.Is(err, tc.want) || iter.closed != 1 {
				t.Fatalf("err=%v closed=%d", err, iter.closed)
			}
		})
	}
}

func TestCQLReadGuard(t *testing.T) {
	for _, query := range []string{"INSERT INTO t(a) VALUES(1)", "SELECT * FROM t; DELETE FROM t", "SELECT '--' FROM t; DROP TABLE t", "SELECT * FROM t /* comment */", "SELECT * FROM t WHERE a='unterminated"} {
		if readCQL(query) {
			t.Errorf("accepted %q", query)
		}
	}
	for _, query := range []string{"SELECT * FROM t", "select a FROM t WHERE b='it''s;valid';", `SELECT "item" FROM "table" WHERE a=?`} {
		if !readCQL(query) {
			t.Errorf("rejected %q", query)
		}
	}
}

func TestExactComplexValuesAndBounds(t *testing.T) {
	integer, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
	decimal := inf.NewDec(123456789, 5)
	value, _, err := normalizeValue("varint", integer)
	if err != nil || value != "123456789012345678901234567890" {
		t.Fatalf("varint: %v %v", value, err)
	}
	value, _, err = normalizeValue("decimal", decimal)
	if err != nil || value != "1234.56789" {
		t.Fatalf("decimal: %+v %v", value, err)
	}
	if _, _, err := normalizeValue("decimal", inf.NewDec(1, 1000000000)); !errors.Is(err, adapter.ErrLimit) {
		t.Fatal("unbounded decimal scale accepted")
	}
	if _, _, err := normalizeValue("text", strings.Repeat("a", 1<<20)); !errors.Is(err, adapter.ErrLimit) {
		t.Fatal("oversized value accepted")
	}
	if _, err := resultSchema([]column{{Name: "items", SourceType: "list<text>"}}); !errors.Is(err, adapter.ErrUnsupported) {
		t.Fatal("unsupported collection advertised")
	}
}

func TestParametersAndCapabilities(t *testing.T) {
	for _, engine := range []string{"cassandra", "scylla"} {
		if err := (Driver{Engine: engine}).Capabilities().Validate(); err != nil {
			t.Fatal(err)
		}
	}
	values, err := parameterValues([]operations.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)}, {Type: "binary", Value: json.RawMessage(`"AP8="`)}})
	if err != nil || values[0] != int64(9007199254740993) || !reflect.DeepEqual(values[1], []byte{0, 255}) {
		t.Fatalf("values: %+v %v", values, err)
	}
}

func TestArrowBatchesMayBeRetainedBySink(t *testing.T) {
	iter := &fakeIterator{rows: []map[string]any{{"id": int64(1)}, {"id": int64(2)}}}
	session := &Session{backend: &fakeBackend{iter: iter}}
	var batches []arrow.RecordBatch
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	_, err := session.Query(context.Background(), adapter.Query{Statement: "SELECT id FROM t", MaxRows: 10, MaxBytes: 4096, BatchRows: 1}, testSink{write: func(batch arrow.RecordBatch) error { batch.Retain(); batches = append(batches, batch); return nil }})
	if err != nil || len(batches) != 2 {
		t.Fatalf("batches=%d err=%v", len(batches), err)
	}
	for i, batch := range batches {
		if got := batch.Column(0).(*array.Int64).Value(0); got != int64(i+1) {
			t.Fatalf("retained batch changed: %d", got)
		}
	}
}
