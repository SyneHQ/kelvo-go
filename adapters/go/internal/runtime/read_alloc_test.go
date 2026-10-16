package runtime

import (
	"context"
	"io"
	"runtime"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// oneRowSession answers every query with a single int64 row.
type oneRowSession struct{}

func (oneRowSession) Close() error { return nil }
func (oneRowSession) Query(_ context.Context, _ adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := sink.Schema(schema); err != nil {
		return adapter.QueryStats{}, err
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	record := b.NewRecordBatch()
	defer record.Release()
	return adapter.QueryStats{Rows: 1}, sink.Write(record)
}

func smallRead(t testing.TB) {
	input := adapter.ProcessRequest{OperationID: "op", Limits: adapter.ProcessLimits{MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 10}}
	invocation := adapter.Invocation{Query: &adapter.Query{Statement: "SELECT 1", MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 10}}
	receipt, err := executeRead(context.Background(), input, invocation, oneRowSession{}, io.Discard, operations.Receipt{})
	if err != nil || receipt.Outcome != operations.Completed || receipt.Result == nil || receipt.Result.Rows != 1 {
		t.Fatal("read failed", err, receipt.Outcome)
	}
}

// A small read must not allocate its 64 KiB result buffer: the adapter serves
// many reads per second per worker, and this once grew a read from ~89 KB to
// ~152 KB of allocation.
func TestSmallReadDoesNotAllocateResultBuffer(t *testing.T) {
	smallRead(t) // fill the pool
	const reads = 200
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range reads {
		smallRead(t)
	}
	runtime.ReadMemStats(&after)
	if perRead := (after.TotalAlloc - before.TotalAlloc) / reads; perRead > resultBufferBytes/2 {
		t.Fatalf("a small read allocated %d bytes, want well under the %d byte result buffer", perRead, resultBufferBytes)
	}
}

func BenchmarkSmallRead(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		smallRead(b)
	}
}
