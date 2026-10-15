package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/frame"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// batchSession emits batches of rows int64 values each; after the first batch
// it waits for proceed, when set.
type batchSession struct {
	batches, rows int
	proceed       chan struct{}
	step          chan struct{} // when set, each batch after the first waits for it
}

func (s *batchSession) Close() error { return nil }
func (s *batchSession) KeepOpen()    {}
func (s *batchSession) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := sink.Schema(schema); err != nil {
		return adapter.QueryStats{}, err
	}
	for b := 0; b < s.batches; b++ {
		if b > 0 && s.step != nil {
			select {
			case <-s.step:
			case <-ctx.Done():
				return adapter.QueryStats{}, ctx.Err()
			case <-time.After(15 * time.Second): // a failed test stopped stepping
				return adapter.QueryStats{}, context.DeadlineExceeded
			}
		}
		if b == 1 && s.proceed != nil {
			select {
			case <-s.proceed:
			case <-ctx.Done():
				return adapter.QueryStats{}, ctx.Err()
			}
		}
		builder := array.NewInt64Builder(memory.NewGoAllocator())
		for i := 0; i < s.rows; i++ {
			builder.Append(int64(i))
		}
		col := builder.NewArray()
		batch := array.NewRecordBatch(schema, []arrow.Array{col}, int64(s.rows))
		err := sink.Write(batch)
		batch.Release()
		col.Release()
		builder.Release()
		if err != nil {
			return adapter.QueryStats{}, err
		}
	}
	return adapter.QueryStats{Rows: int64(s.batches * s.rows)}, nil
}

func batchRunner(s *batchSession) Runner {
	return Runner{
		SessionCache: &SessionCache{TTL: time.Minute, Max: 4},
		Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
			return s, nil
		},
	}
}

// collect reads the only stream to its terminal frame, returning its Result
// payloads joined, how many Result frames carried them, and the terminal.
func (h *harness) collect() ([]byte, int, frame.Frame) {
	h.t.Helper()
	var body bytes.Buffer
	frames := 0
	for {
		f := h.next()
		switch f.Type {
		case frame.Result:
			frames++
			body.Write(f.Payload)
		case frame.Receipt, frame.Error:
			return body.Bytes(), frames, f
		}
	}
}

func checkResult(t *testing.T, body []byte, r operations.Receipt, rows int64) {
	t.Helper()
	sum := sha256.Sum256(body)
	if r.Outcome != operations.Completed || r.Result == nil || r.Result.Rows != rows || r.Result.Bytes != int64(len(body)) || r.Result.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("receipt does not describe the wire bytes: %+v (got %d bytes)", r.Result, len(body))
	}
	reader, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	var got int64
	for reader.Next() {
		got += reader.RecordBatch().NumRows()
	}
	if reader.Err() != nil || got != rows {
		t.Fatalf("result does not decode: rows=%d err=%v", got, reader.Err())
	}
}

// A small result leaves in a couple of Result frames, not one per Arrow
// message, buffer and pad.
func TestServe_SmallResultInFewFrames(t *testing.T) {
	h := startServe(t, batchRunner(&batchSession{batches: 1, rows: 50}), true)
	h.request(1, sqlRequest("op-small", "SELECT 1"))
	body, frames, term := h.collect()
	checkResult(t, body, receiptOf(t, term), 50)
	t.Logf("%d bytes in %d Result frames", len(body), frames)
	// One write for the schema and the batch, one for the end of stream.
	if frames > 2 {
		t.Fatalf("a %d-byte result took %d Result frames", len(body), frames)
	}
}

// A large result still streams: the first batch reaches the node while the
// source is still producing the next.
func TestServe_LargeResultStillStreams(t *testing.T) {
	proceed := make(chan struct{})
	h := startServe(t, batchRunner(&batchSession{batches: 3, rows: 4096, proceed: proceed}), true)
	req := sqlRequest("op-large", "SELECT 1")
	req.Limits.BatchRows, req.Limits.MaxRows = 4096, 100_000
	h.request(1, req)
	var body bytes.Buffer
	first := h.next()
	if first.Type != frame.Result {
		t.Fatalf("expected the first batch before the source finished, got %v", first.Type)
	}
	body.Write(first.Payload)
	close(proceed)
	rest, _, term := h.collect()
	body.Write(rest)
	checkResult(t, body.Bytes(), receiptOf(t, term), 3*4096)
}

// The byte limit still counts every byte as it is produced, buffered or not.
func TestServe_ByteLimitStillEnforced(t *testing.T) {
	h := startServe(t, batchRunner(&batchSession{batches: 1, rows: 50}), true)
	req := sqlRequest("op-limit", "SELECT 1")
	req.Limits.MaxBytes = 200
	h.request(1, req)
	_, _, term := h.collect()
	wantFailedReceipt(t, term)
}

// Small batches each reach the node when produced: every batch is flushed,
// not held until 64 KiB have built up.
func TestServe_SmallBatchesFlushEach(t *testing.T) {
	step := make(chan struct{})
	h := startServe(t, batchRunner(&batchSession{batches: 5, rows: 100, step: step}), true)
	req := sqlRequest("op-batches", "SELECT 1")
	req.Limits.BatchRows = 100
	h.request(1, req)
	var body bytes.Buffer
	for b := 0; b < 5; b++ {
		f := h.next() // batch b, before the source makes the next
		if f.Type != frame.Result {
			t.Fatalf("batch %d: expected a Result frame, got %v %q", b, f.Type, f.Payload)
		}
		body.Write(f.Payload)
		if b < 4 {
			select {
			case step <- struct{}{}:
			case <-time.After(10 * time.Second):
				t.Fatalf("batch %d: the source stopped before its next batch", b)
			}
		}
	}
	rest, _, term := h.collect()
	body.Write(rest)
	checkResult(t, body.Bytes(), receiptOf(t, term), 500)
}
