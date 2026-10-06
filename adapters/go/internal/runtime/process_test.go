package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func runtimeRequest(t *testing.T, kind operations.Kind) adapter.ProcessRequest {
	t.Helper()
	r := operations.Request{Version: operations.Version, Kind: kind, Connection: operations.ConnectionRef{ID: "saved-1", Database: "app"}}
	switch kind {
	case operations.QueryRead:
		r.Spec.Query = &operations.QuerySpec{SQL: "SELECT 1"}
	case operations.StatementExecute:
		r.IdempotencyKey = "write-1"
		r.Spec.Statement = &operations.StatementSpec{SQL: "UPDATE items SET n=1", Transaction: operations.TransactionRequired}
	case operations.MetadataInspect:
		r.Spec.Metadata = &operations.MetadataSpec{Object: "tables", Limit: 10}
	}
	digest, err := operations.Digest(r)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	return adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: "op-1", RequestSHA256: digest, Request: r,
		Source: adapter.ConnectionSpec{Engine: "postgresql", TenantID: "tenant-1", ConnectionID: "saved-1", Database: "app", Revision: "revision-1"},
		Limits: adapter.ProcessLimits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 100, TimeoutMS: 5000}, ExpiresAt: now + 60, CredentialsValidUntil: now + 5}
}

type processSession struct {
	closeCount, executeCount, queryCount int
	closeErr, errorResult                error
	result                               adapter.ChangeResult
	rows                                 int
	noSchema, badStats                   bool
	bsonOutput                           bool
}

func (s *processSession) Close() error               { s.closeCount++; return s.closeErr }
func (s *processSession) Test(context.Context) error { return s.errorResult }
func (s *processSession) Execute(context.Context, adapter.Change) (adapter.ChangeResult, error) {
	s.executeCount++
	return s.result, s.errorResult
}
func (s *processSession) Inspect(ctx context.Context, _ operations.MetadataSpec, _ adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	return s.Query(ctx, adapter.Query{}, sink)
}
func (s *processSession) Query(_ context.Context, _ adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	s.queryCount++
	if s.noSchema {
		return adapter.QueryStats{}, s.errorResult
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if s.bsonOutput {
		meta := arrow.NewMetadata([]string{"kelvo.logical_type"}, []string{"bson"})
		schema = arrow.NewSchema([]arrow.Field{{Name: "document_bson", Type: arrow.BinaryTypes.Binary, Metadata: meta}}, nil)
	}
	if err := sink.Schema(schema); err != nil {
		return adapter.QueryStats{}, err
	}
	if s.rows > 0 {
		if s.bsonOutput {
			builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
			defer builder.Release()
			for i := 0; i < s.rows; i++ {
				raw, err := bson.Marshal(bson.D{{Key: "id", Value: int64(i)}})
				if err != nil {
					return adapter.QueryStats{}, err
				}
				builder.Append(raw)
			}
			values := builder.NewArray()
			defer values.Release()
			record := array.NewRecordBatch(schema, []arrow.Array{values}, int64(s.rows))
			defer record.Release()
			if err := sink.Write(record); err != nil {
				return adapter.QueryStats{}, err
			}
		} else {
			builder := array.NewInt64Builder(memory.DefaultAllocator)
			defer builder.Release()
			for i := 0; i < s.rows; i++ {
				builder.Append(int64(i))
			}
			values := builder.NewArray()
			defer values.Release()
			record := array.NewRecordBatch(schema, []arrow.Array{values}, int64(s.rows))
			defer record.Release()
			if err := sink.Write(record); err != nil {
				return adapter.QueryStats{}, err
			}
		}
	}
	n := int64(s.rows)
	if s.badStats {
		n++
	}
	return adapter.QueryStats{Rows: n}, s.errorResult
}

type afterCloseWriter struct {
	bytes.Buffer
	session *processSession
	t       *testing.T
}

func (w *afterCloseWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	if w.session != nil && w.session.closeCount != 1 {
		w.t.Fatal("receipt delivered before final Close")
	}
	return w.Buffer.Write(p)
}

func runProcess(t *testing.T, request adapter.ProcessRequest, session *processSession) (operations.Receipt, []byte, error) {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	receipts := &afterCloseWriter{session: session, t: t}
	openCount := 0
	err = (Runner{Open: func(ctx context.Context, _ adapter.ConnectionSpec, _ operations.Request) (adapter.Session, error) {
		openCount++
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("open has no freshness deadline")
		}
		return session, nil
	}}).Run(context.Background(), bytes.NewReader(raw), &out, receipts)
	if openCount > 1 {
		t.Fatal("source open retried")
	}
	var receipt operations.Receipt
	if receipts.Len() != 0 {
		if e := operations.DecodeStrict(receipts.Bytes(), &receipt, operations.MaxReceiptBytes); e != nil {
			t.Fatal(e)
		}
		if e := receipt.Validate(); e != nil {
			t.Fatal(e)
		}
	}
	if bytes.Contains(receipts.Bytes(), []byte("secret-database-error")) {
		t.Fatal("source error leaked")
	}
	return receipt, out.Bytes(), err
}

func TestProcessArrowReceiptMatchesCompleteOutput(t *testing.T) {
	for _, kind := range []operations.Kind{operations.QueryRead, operations.MetadataInspect} {
		for _, n := range []int{0, 5} {
			t.Run(string(kind)+string(rune('0'+n)), func(t *testing.T) {
				s := &processSession{rows: n}
				r := runtimeRequest(t, kind)
				receipt, out, err := runProcess(t, r, s)
				if err != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectNone || receipt.Result == nil {
					t.Fatal(receipt, err)
				}
				digest := sha256.Sum256(out)
				if receipt.Result.ID != r.OperationID || receipt.Result.SHA256 != hex.EncodeToString(digest[:]) || receipt.Result.Bytes != int64(len(out)) || receipt.Result.Rows != int64(n) || receipt.Result.Format != "arrow_ipc" {
					t.Fatal("result integrity mismatch")
				}
				reader, err := ipc.NewReader(bytes.NewReader(out))
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Release()
				if reader.Schema().Field(0).Name != "id" {
					t.Fatal("empty/schema output lost")
				}
				rows := int64(0)
				for reader.Next() {
					rows += reader.RecordBatch().NumRows()
				}
				if reader.Err() != nil || rows != int64(n) {
					t.Fatal("incomplete Arrow output", reader.Err())
				}
			})
		}
	}
}

func TestProcessFailureCannotProduceSuccessfulReadReceipt(t *testing.T) {
	for _, name := range []string{"rows", "batch", "bytes", "stats", "schema", "source"} {
		t.Run(name, func(t *testing.T) {
			r := runtimeRequest(t, operations.QueryRead)
			s := &processSession{rows: 5}
			switch name {
			case "rows":
				r.Limits.MaxRows = 4
			case "batch":
				r.Limits.BatchRows = 4
			case "bytes":
				r.Limits.MaxBytes = 1
			case "stats":
				s.badStats = true
			case "schema":
				s.noSchema = true
			case "source":
				s.errorResult = errors.New("secret-database-error")
			}
			receipt, _, err := runProcess(t, r, s)
			if err == nil || receipt.Outcome != operations.Failed || receipt.Effect != operations.EffectNone || receipt.Result != nil {
				t.Fatal(receipt, err)
			}
		})
	}
}

func TestProcessPreservesSourceEffectsAcrossCleanupFailure(t *testing.T) {
	for _, outcome := range []string{"succeeded", "failed", "unknown"} {
		t.Run(outcome, func(t *testing.T) {
			s := &processSession{result: adapter.ChangeResult{Outcome: outcome, Completed: 1}, closeErr: errors.New("secret-database-error")}
			if outcome != "succeeded" {
				s.errorResult = errors.New("secret-database-error")
			}
			receipt, out, err := runProcess(t, runtimeRequest(t, operations.StatementExecute), s)
			if err == nil || len(out) != 0 || s.executeCount != 1 {
				t.Fatal("missing cleanup error or duplicate execution")
			}
			switch outcome {
			case "succeeded":
				if receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted {
					t.Fatal(receipt)
				}
			case "failed":
				if receipt.Outcome != operations.Failed || receipt.Effect != operations.EffectPartial {
					t.Fatal(receipt)
				}
			default:
				if receipt.Outcome != operations.OutcomeUnknown || receipt.Effect != operations.EffectUnknown {
					t.Fatal(receipt)
				}
			}
		})
	}
}

func TestProcessRejectsExpiredInputBeforeOpen(t *testing.T) {
	r := runtimeRequest(t, operations.StatementExecute)
	r.CredentialsValidUntil = time.Now().Unix()
	raw, _ := json.Marshal(r)
	var out, receipts bytes.Buffer
	err := (Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		t.Fatal("opened expired source")
		return nil, nil
	}}).Run(context.Background(), bytes.NewReader(raw), &out, &receipts)
	if err == nil || out.Len() != 0 || receipts.Len() != 0 {
		t.Fatal("expired process accepted")
	}
}

func TestProcessClosesSessionReturnedWithOpenError(t *testing.T) {
	r := runtimeRequest(t, operations.ConnectionTest)
	s := &processSession{}
	receipt, err := (Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		return s, errors.New("secret-database-error")
	}}).execute(context.Background(), r, io.Discard)
	if err == nil || s.closeCount != 1 || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone {
		t.Fatal(receipt, err, s.closeCount)
	}
}

func TestProcessCancellationAfterOpenHasNoSourceEffect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &processSession{}
	r, err := (Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		cancel()
		return s, nil
	}}).execute(ctx, runtimeRequest(t, operations.StatementExecute), io.Discard)
	if !errors.Is(err, context.Canceled) || r.Outcome != operations.CancelledBeforeStart || r.Effect != operations.EffectNone || s.executeCount != 0 || s.closeCount != 1 {
		t.Fatal(r, err)
	}
}

func TestProcessWriterRejectsShortWritesAndOversizeInput(t *testing.T) {
	w := &boundedHashWriter{writer: shortWriter{}, hash: sha256.New(), maximum: 100}
	if n, err := w.Write([]byte("hello")); n != 4 || !errors.Is(err, io.ErrShortWrite) || w.bytes != 4 {
		t.Fatal(n, err)
	}
	if (Runner{}).Run(context.Background(), strings.NewReader(strings.Repeat(" ", adapter.MaxProcessRequestBytes+1)), io.Discard, io.Discard) == nil {
		t.Fatal("oversize input accepted")
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
