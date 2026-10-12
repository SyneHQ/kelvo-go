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
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/frame"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type fakeQuerySession struct {
	kept   bool
	closed bool
	mu     sync.Mutex
	err    error
}

func (s *fakeQuerySession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeQuerySession) KeepOpen() { s.kept = true }

func (s *fakeQuerySession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *fakeQuerySession) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s.err != nil {
		return adapter.QueryStats{}, s.err
	}

	// Emit schema and 1 batch
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	if err := sink.Schema(schema); err != nil {
		return adapter.QueryStats{}, err
	}

	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3}, nil)
	col := builder.NewArray()
	defer col.Release()

	batch := array.NewRecord(schema, []arrow.Array{col}, 3)
	defer batch.Release()

	if err := sink.Write(batch); err != nil {
		return adapter.QueryStats{}, err
	}

	return adapter.QueryStats{Rows: 3, Bytes: 24, Elapsed: 10 * time.Millisecond}, nil
}

func validTestProcessRequest(opID, tenantID, appTeam, connID, rev string, kind operations.Kind) adapter.ProcessRequest {
	now := time.Now()
	req := operations.Request{
		Version: operations.Version,
		Kind:    kind,
		Connection: operations.ConnectionRef{
			ID:       connID,
			Database: "testdb",
			Schema:   "public",
		},
	}
	if kind == operations.QueryRead {
		req.Spec.Query = &operations.QuerySpec{SQL: "SELECT 1"}
	} else if kind == operations.MetadataInspect {
		req.Spec.Metadata = &operations.MetadataSpec{Object: "tables"}
	} else if kind == operations.StatementExecute {
		req.IdempotencyKey = "idem-1"
		req.Spec.Statement = &operations.StatementSpec{SQL: "INSERT INTO t VALUES (1)"}
	}

	digest, _ := operations.Digest(req)

	return adapter.ProcessRequest{
		Version:       adapter.ProcessVersion,
		OperationID:   opID,
		RequestSHA256: digest,
		Request:       req,
		Limits: adapter.ProcessLimits{
			MaxRows:   1000,
			MaxBytes:  10 << 20,
			BatchRows: 100,
			TimeoutMS: 5000,
		},
		Source: adapter.ConnectionSpec{
			Engine:       "postgres",
			TenantID:     tenantID,
			ConnectionID: connID,
			Revision:     rev,
			Database:     "testdb",
			Schema:       "public",
		},
		AppTeam:               appTeam,
		CredentialsValidUntil: now.Unix() + 3,
		ExpiresAt:             now.Unix() + 10,
	}
}

func TestServeLoop_SuccessAndReuse(t *testing.T) {
	pipeInR, pipeInW := io.Pipe()
	pipeOutR, pipeOutW := io.Pipe()

	session := &fakeQuerySession{}
	opens := 0
	open := func(ctx context.Context, spec adapter.ConnectionSpec, req operations.Request) (adapter.Session, error) {
		opens++
		return session, nil
	}

	runner := Runner{
		Open:         open,
		SessionCache: &SessionCache{TTL: 30 * time.Minute, Max: 4},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- runner.Serve(ctx, pipeInR, pipeOutW)
	}()

	// Request 1
	req1 := validTestProcessRequest("op-1", "tenant-1", "team-1", "conn-1", strings.Repeat("a", 64), operations.QueryRead)
	req1Bytes, err := adapter.EncodeProcessRequest(req1)
	if err != nil {
		t.Fatalf("encode request 1: %v", err)
	}

	if err := frame.WriteRequest(pipeInW, req1Bytes); err != nil {
		t.Fatalf("write req1: %v", err)
	}

	// Read Result frame(s) and Receipt frame
	for {
		typ, payload, err := frame.Read(pipeOutR)
		if err != nil {
			t.Fatalf("read frame 1: %v", err)
		}
		if typ == frame.Receipt {
			var receipt operations.Receipt
			if err := json.Unmarshal(payload, &receipt); err != nil {
				t.Fatalf("unmarshal receipt: %v", err)
			}
			if receipt.Outcome != operations.Completed {
				t.Fatalf("expected completed receipt, got %+v", receipt)
			}
			if receipt.Result == nil || receipt.Result.Rows != 3 {
				t.Fatalf("expected 3 rows, got %+v", receipt.Result)
			}
			break
		}
		if typ != frame.Result {
			t.Fatalf("unexpected frame type: %v", typ)
		}
	}

	if opens != 1 {
		t.Fatalf("expected 1 open, got %d", opens)
	}
	if session.isClosed() {
		t.Fatal("session should remain open in cache")
	}

	// Request 2 with same key -> should reuse session, opens should still be 1!
	req2 := validTestProcessRequest("op-2", "tenant-1", "team-1", "conn-1", strings.Repeat("a", 64), operations.QueryRead)
	req2Bytes, err := adapter.EncodeProcessRequest(req2)
	if err != nil {
		t.Fatalf("encode request 2: %v", err)
	}

	if err := frame.WriteRequest(pipeInW, req2Bytes); err != nil {
		t.Fatalf("write req2: %v", err)
	}

	for {
		typ, payload, err := frame.Read(pipeOutR)
		if err != nil {
			t.Fatalf("read frame 2: %v", err)
		}
		if typ == frame.Receipt {
			var receipt operations.Receipt
			if err := json.Unmarshal(payload, &receipt); err != nil {
				t.Fatalf("unmarshal receipt 2: %v", err)
			}
			if receipt.Outcome != operations.Completed {
				t.Fatalf("expected completed receipt, got %+v", receipt)
			}
			break
		}
	}

	if opens != 1 {
		t.Fatalf("expected session reuse (opens=1), but opens was %d", opens)
	}

	// Send Close frame
	if err := frame.WriteClose(pipeInW); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("expected clean exit on Close, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not exit on Close frame")
	}

	if !session.isClosed() {
		t.Fatal("session should be closed on server shutdown")
	}
}

func TestServeLoop_PingPong(t *testing.T) {
	pipeInR, pipeInW := io.Pipe()
	pipeOutR, pipeOutW := io.Pipe()

	runner := Runner{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- runner.Serve(ctx, pipeInR, pipeOutW)
	}()

	if err := frame.WritePing(pipeInW); err != nil {
		t.Fatal(err)
	}

	typ, _, err := frame.Read(pipeOutR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != frame.Pong {
		t.Fatalf("expected Pong, got %v", typ)
	}

	_ = frame.WriteClose(pipeInW)
	<-serveDone
}

func TestServeLoop_CrossTenantRejection(t *testing.T) {
	pipeInR, pipeInW := io.Pipe()
	pipeOutR, pipeOutW := io.Pipe()

	session := &fakeQuerySession{}
	runner := Runner{
		Open: func(ctx context.Context, spec adapter.ConnectionSpec, req operations.Request) (adapter.Session, error) {
			return session, nil
		},
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- runner.Serve(context.Background(), pipeInR, pipeOutW)
	}()

	// Req 1: Tenant 1
	req1 := validTestProcessRequest("op-1", "tenant-1", "team-1", "conn-1", strings.Repeat("a", 64), operations.QueryRead)
	req1Bytes, _ := adapter.EncodeProcessRequest(req1)
	_ = frame.WriteRequest(pipeInW, req1Bytes)

	// Consume until receipt
	for {
		typ, _, err := frame.Read(pipeOutR)
		if err != nil {
			t.Fatal(err)
		}
		if typ == frame.Receipt {
			break
		}
	}

	// Req 2: Different tenant!
	req2 := validTestProcessRequest("op-2", "tenant-2", "team-1", "conn-1", strings.Repeat("a", 64), operations.QueryRead)
	req2Bytes, _ := adapter.EncodeProcessRequest(req2)
	_ = frame.WriteRequest(pipeInW, req2Bytes)

	// Should receive Error frame and server terminates
	typ, payload, err := frame.Read(pipeOutR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != frame.Error {
		t.Fatalf("expected Error frame on cross-tenant request, got %v", typ)
	}
	if !strings.Contains(string(payload), "cross-tenant") {
		t.Fatalf("unexpected error payload: %s", string(payload))
	}

	err = <-serveDone
	if err == nil {
		t.Fatal("expected serve error on cross-tenant request, got nil")
	}

	if !session.isClosed() {
		t.Fatal("session must be closed on security violation")
	}
}

func TestServeLoop_IneligibleMutationRejected(t *testing.T) {
	pipeInR, pipeInW := io.Pipe()
	pipeOutR, pipeOutW := io.Pipe()

	runner := Runner{}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- runner.Serve(context.Background(), pipeInR, pipeOutW)
	}()

	req := validTestProcessRequest("op-mut", "tenant-1", "team-1", "conn-1", strings.Repeat("a", 64), operations.StatementExecute)
	reqBytes, _ := adapter.EncodeProcessRequest(req)
	_ = frame.WriteRequest(pipeInW, reqBytes)

	typ, _, err := frame.Read(pipeOutR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != frame.Error {
		t.Fatalf("expected Error frame for ineligible mutation, got %v", typ)
	}

	err = <-serveDone
	if err == nil {
		t.Fatal("expected serve error for ineligible request")
	}
}

func TestServeLoop_QueryErrorClosesSessionAndTerminates(t *testing.T) {
	pipeInR, pipeInW := io.Pipe()
	pipeOutR, pipeOutW := io.Pipe()

	session := &fakeQuerySession{err: errors.New("query failed: table not found")}
	runner := Runner{
		Open: func(ctx context.Context, spec adapter.ConnectionSpec, req operations.Request) (adapter.Session, error) {
			return session, nil
		},
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- runner.Serve(context.Background(), pipeInR, pipeOutW)
	}()

	req := validTestProcessRequest("op-err", "tenant-1", "team-1", "conn-1", strings.Repeat("a", 64), operations.QueryRead)
	reqBytes, _ := adapter.EncodeProcessRequest(req)
	_ = frame.WriteRequest(pipeInW, reqBytes)

	typ, payload, err := frame.Read(pipeOutR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != frame.Receipt {
		t.Fatalf("expected Receipt frame with failure outcome, got %v", typ)
	}
	var receipt operations.Receipt
	_ = json.Unmarshal(payload, &receipt)
	if receipt.Outcome != operations.Failed {
		t.Fatalf("expected Failed outcome, got %v", receipt.Outcome)
	}

	err = <-serveDone
	if err == nil {
		t.Fatal("expected serve error on query failure")
	}

	if !session.isClosed() {
		t.Fatal("failed session MUST be closed and never reused")
	}
}

func TestServeLoop_ExpiredCredentialsRejected(t *testing.T) {
	pipeInR, pipeInW := io.Pipe()
	pipeOutR, pipeOutW := io.Pipe()

	runner := Runner{}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- runner.Serve(context.Background(), pipeInR, pipeOutW)
	}()

	req := validTestProcessRequest("op-exp", "tenant-1", "team-1", "conn-1", strings.Repeat("a", 64), operations.QueryRead)
	req.CredentialsValidUntil = time.Now().Unix() - 10 // expired!
	// Re-compute digest
	req.RequestSHA256, _ = operations.Digest(req.Request)
	// We encode raw directly
	raw, _ := json.Marshal(req)
	_ = frame.WriteRequest(pipeInW, raw)

	typ, _, err := frame.Read(pipeOutR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != frame.Error {
		t.Fatalf("expected Error frame for expired credentials, got %v", typ)
	}

	err = <-serveDone
	if err == nil {
		t.Fatal("expected serve error for expired credentials")
	}
}

// Suppress unused imports
var _ = sha256.New
var _ = hex.EncodeToString
var _ = bytes.NewReader

// singleUseSession does not declare reuse support, like every session whose
// first read consumes its connection.
type singleUseSession struct{ inner *fakeQuerySession }

func (s singleUseSession) Close() error { return s.inner.Close() }
func (s singleUseSession) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	return s.inner.Query(ctx, q, sink)
}

func serveOneRequest(t *testing.T, in io.Writer, out io.Reader, opID string) {
	t.Helper()
	request := validTestProcessRequest(opID, "tenant-1", "team-1", "conn-1", strings.Repeat("a", 64), operations.QueryRead)
	raw, err := adapter.EncodeProcessRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := frame.WriteRequest(in, raw); err != nil {
		t.Fatal(err)
	}
	for {
		kind, payload, err := frame.Read(out)
		if err != nil {
			t.Fatal(err)
		}
		if kind == frame.Receipt {
			var receipt operations.Receipt
			if json.Unmarshal(payload, &receipt) != nil || receipt.Outcome != operations.Completed {
				t.Fatalf("request %s did not complete: %s", opID, payload)
			}
			return
		}
		if kind != frame.Result {
			t.Fatalf("unexpected frame type: %v", kind)
		}
	}
}

// A session is kept warm only after it accepted KeepOpen. A single-use session
// is closed after each request and opened again for the next one.
func TestServeLoop_OnlyReusableSessionsStayWarm(t *testing.T) {
	for _, reusable := range []bool{true, false} {
		inR, inW := io.Pipe()
		outR, outW := io.Pipe()
		var opened []*fakeQuerySession
		runner := Runner{SessionCache: &SessionCache{TTL: 30 * time.Minute, Max: 4},
			Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
				session := &fakeQuerySession{}
				opened = append(opened, session)
				if reusable {
					return session, nil
				}
				return singleUseSession{session}, nil
			}}
		done := make(chan error, 1)
		go func() { done <- runner.Serve(context.Background(), inR, outW) }()
		serveOneRequest(t, inW, outR, "op-1")
		serveOneRequest(t, inW, outR, "op-2")
		if reusable && (len(opened) != 1 || !opened[0].kept || opened[0].isClosed()) {
			t.Fatalf("reusable session was not kept warm: opens=%d", len(opened))
		}
		if !reusable && (len(opened) != 2 || opened[0].kept || !opened[0].isClosed() || !opened[1].isClosed()) {
			t.Fatalf("single-use session was reused or left open: opens=%d", len(opened))
		}
		if err := frame.WriteClose(inW); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
