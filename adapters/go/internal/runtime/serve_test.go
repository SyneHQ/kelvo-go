package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
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
	kept   bool // guarded by mu
	closed bool
	mu     sync.Mutex
	err    error
	fn     func(ctx context.Context, sql string) error // optional per-query hook
}

func (s *fakeQuerySession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeQuerySession) KeepOpen() { s.mu.Lock(); s.kept = true; s.mu.Unlock() }

func (s *fakeQuerySession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *fakeQuerySession) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s.err != nil {
		return adapter.QueryStats{}, s.err
	}
	if s.fn != nil {
		if err := s.fn(ctx, q.Statement); err != nil {
			return adapter.QueryStats{}, err
		}
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
			DSN:          "postgres://reader:secret@db.internal/testdb?sslmode=verify-full",
		},
		AppTeam:               appTeam,
		CredentialsValidUntil: now.Unix() + 3,
		ExpiresAt:             now.Unix() + 10,
	}
}

var testKey = strings.Repeat("a", 64)

func sqlRequest(op, sql string) adapter.ProcessRequest {
	req := validTestProcessRequest(op, "tenant-1", "team-1", "conn-1", testKey, operations.QueryRead)
	req.Request.Spec.Query.SQL = sql
	req.RequestSHA256, _ = operations.Digest(req.Request)
	return req
}

// harness drives Serve over pipes and collects every frame it emits.
type harness struct {
	t      *testing.T
	in     *io.PipeWriter
	frames chan frame.Frame
	done   chan error
}

func startServe(t *testing.T, runner Runner, hello bool) *harness {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	h := &harness{t: t, in: inW, frames: make(chan frame.Frame, 4096), done: make(chan error, 1)}
	go func() {
		err := runner.Serve(context.Background(), inR, outW)
		_ = outW.Close()
		h.done <- err
	}()
	go func() {
		for {
			f, err := frame.Read(outR)
			if err != nil {
				close(h.frames)
				return
			}
			h.frames <- f
		}
	}()
	t.Cleanup(func() { _ = inW.Close() })
	if hello {
		if err := frame.WriteHello(inW); err != nil {
			t.Fatal(err)
		}
		if f := h.next(); f.Type != frame.Hello {
			t.Fatalf("expected Hello reply, got %v", f.Type)
		}
	}
	return h
}

func (h *harness) next() frame.Frame {
	h.t.Helper()
	select {
	case f, ok := <-h.frames:
		if !ok {
			h.t.Fatal("output closed")
		}
		return f
	case <-time.After(10 * time.Second):
		h.t.Fatal("timed out waiting for frame")
	}
	return frame.Frame{}
}

func (h *harness) request(stream uint32, req adapter.ProcessRequest) {
	h.t.Helper()
	raw, err := adapter.EncodeProcessRequest(req)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := frame.WriteRequest(h.in, stream, raw); err != nil {
		h.t.Fatal(err)
	}
}

// terminals reads until n streams have ended; it returns the terminal frame
// per stream and the number of Result frames seen per stream.
func (h *harness) terminals(n int) (map[uint32]frame.Frame, map[uint32]int) {
	h.t.Helper()
	term, results := map[uint32]frame.Frame{}, map[uint32]int{}
	for len(term) < n {
		f := h.next()
		switch f.Type {
		case frame.Result:
			results[f.Stream]++
		case frame.Receipt, frame.Error:
			if _, dup := term[f.Stream]; dup {
				h.t.Fatalf("stream %d got two terminal frames", f.Stream)
			}
			term[f.Stream] = f
		default:
			h.t.Fatalf("unexpected frame %v", f.Type)
		}
	}
	return term, results
}

func (h *harness) wantDone(wantErr bool) {
	h.t.Helper()
	select {
	case err := <-h.done:
		if (err != nil) != wantErr {
			h.t.Fatalf("serve returned %v, wantErr=%v", err, wantErr)
		}
	case <-time.After(5 * time.Second):
		h.t.Fatal("serve did not exit")
	}
}

func receiptOf(t *testing.T, f frame.Frame) operations.Receipt {
	t.Helper()
	if f.Type != frame.Receipt {
		t.Fatalf("expected Receipt, got %v %q", f.Type, f.Payload)
	}
	var r operations.Receipt
	if err := json.Unmarshal(f.Payload, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func wantFailedReceipt(t *testing.T, f frame.Frame) {
	t.Helper()
	if r := receiptOf(t, f); r.Outcome == operations.Completed || r.ErrorCode == "" {
		t.Fatalf("expected failure receipt with error code, got %+v", r)
	}
}

func sessionRunner(session *fakeQuerySession, opens *atomic.Int32) Runner {
	return Runner{
		SessionCache: &SessionCache{TTL: 30 * time.Minute, Max: 4},
		Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
			if opens != nil {
				opens.Add(1)
				time.Sleep(30 * time.Millisecond)
			}
			return session, nil
		},
	}
}

func TestServeLoop_SuccessAndReuse(t *testing.T) {
	session := &fakeQuerySession{}
	var opens atomic.Int32
	h := startServe(t, sessionRunner(session, &opens), true)
	for i, op := range []string{"op-1", "op-2"} {
		stream := uint32(i + 1)
		h.request(stream, sqlRequest(op, "SELECT 1"))
		term, results := h.terminals(1)
		r := receiptOf(t, term[stream])
		if r.Outcome != operations.Completed || r.Result == nil || r.Result.Rows != 3 || results[stream] == 0 {
			t.Fatalf("bad completion: %+v results=%d", r, results[stream])
		}
	}
	if opens.Load() != 1 || session.isClosed() {
		t.Fatalf("expected one warm open, opens=%d closed=%v", opens.Load(), session.isClosed())
	}
	if err := frame.WriteClose(h.in); err != nil {
		t.Fatal(err)
	}
	h.wantDone(false)
	if !session.isClosed() {
		t.Fatal("session should be closed on server shutdown")
	}
}

func TestServeLoop_PingPong(t *testing.T) {
	h := startServe(t, Runner{}, true)
	if err := frame.WritePing(h.in); err != nil {
		t.Fatal(err)
	}
	if f := h.next(); f.Type != frame.Pong {
		t.Fatalf("expected Pong, got %v", f.Type)
	}
	_ = frame.WriteClose(h.in)
	h.wantDone(false)
}

func TestServeLoop_HelloRequiredFirst(t *testing.T) {
	for name, send := range map[string]func(w io.Writer) error{
		"ping":        func(w io.Writer) error { return frame.WritePing(w) },
		"request":     func(w io.Writer) error { return frame.WriteRequest(w, 1, []byte("{}")) },
		"bad version": func(w io.Writer) error { return frame.Write(w, frame.Hello, 0, []byte{frame.Version + 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			h := startServe(t, Runner{}, false)
			go func() { _ = send(h.in) }()
			h.wantDone(true)
		})
	}
}

func TestServeLoop_SecondHelloIsFatal(t *testing.T) {
	h := startServe(t, Runner{}, true)
	if err := frame.WriteHello(h.in); err != nil {
		t.Fatal(err)
	}
	h.wantDone(true)
}

func TestServeLoop_CrossTenantRejection(t *testing.T) {
	session := &fakeQuerySession{}
	h := startServe(t, sessionRunner(session, nil), true)
	h.request(1, sqlRequest("op-1", "SELECT 1"))
	h.terminals(1)

	h.request(2, validTestProcessRequest("op-2", "tenant-2", "team-1", "conn-1", testKey, operations.QueryRead))
	f := h.next()
	if f.Type != frame.Error || f.Stream != 2 || !strings.Contains(string(f.Payload), "cross-tenant") {
		t.Fatalf("expected cross-tenant Error on stream 2, got %v %q", f.Type, f.Payload)
	}
	h.wantDone(true)
	if !session.isClosed() {
		t.Fatal("session must be closed on security violation")
	}
}

func TestServeLoop_IneligibleMutationRejected(t *testing.T) {
	h := startServe(t, Runner{}, true)
	raw, _ := json.Marshal(validTestProcessRequest("op-mut", "tenant-1", "team-1", "conn-1", testKey, operations.StatementExecute))
	if err := frame.WriteRequest(h.in, 1, raw); err != nil {
		t.Fatal(err)
	}
	if f := h.next(); f.Type != frame.Error {
		t.Fatalf("expected Error frame for ineligible mutation, got %v", f.Type)
	}
	h.wantDone(true)
}

func TestServeLoop_ExpiredCredentialsRejected(t *testing.T) {
	h := startServe(t, Runner{}, true)
	req := sqlRequest("op-exp", "SELECT 1")
	req.CredentialsValidUntil = time.Now().Unix() - 10
	raw, _ := json.Marshal(req)
	if err := frame.WriteRequest(h.in, 1, raw); err != nil {
		t.Fatal(err)
	}
	if f := h.next(); f.Type != frame.Error {
		t.Fatalf("expected Error frame for expired credentials, got %v", f.Type)
	}
	h.wantDone(true)
}

func TestServeLoop_QueryErrorFailsOnlyThatStream(t *testing.T) {
	session := &fakeQuerySession{err: errors.New("query failed: table not found")}
	h := startServe(t, sessionRunner(session, nil), true)
	h.request(1, sqlRequest("op-err", "SELECT 1"))
	term, _ := h.terminals(1)
	wantFailedReceipt(t, term[1])
	if session.isClosed() {
		t.Fatal("per-stream failure must not close the shared session")
	}
	session.mu.Lock()
	session.err = nil
	session.mu.Unlock()
	h.request(2, sqlRequest("op-ok", "SELECT 1"))
	term, _ = h.terminals(1)
	if r := receiptOf(t, term[2]); r.Outcome != operations.Completed {
		t.Fatalf("next request on same worker failed: %+v", r)
	}
	_ = frame.WriteClose(h.in)
	h.wantDone(false)
}

// singleUseSession does not declare reuse support, like every session whose
// first read consumes its connection.
type singleUseSession struct{ inner *fakeQuerySession }

func (s singleUseSession) Close() error { return s.inner.Close() }
func (s singleUseSession) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	return s.inner.Query(ctx, q, sink)
}

// A session is kept warm only after it accepted KeepOpen. A single-use session
// is closed after each request and opened again for the next one.
func TestServeLoop_OnlyReusableSessionsStayWarm(t *testing.T) {
	for _, reusable := range []bool{true, false} {
		var mu sync.Mutex
		var opened []*fakeQuerySession
		runner := Runner{SessionCache: &SessionCache{TTL: 30 * time.Minute, Max: 4},
			Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
				session := &fakeQuerySession{}
				mu.Lock()
				opened = append(opened, session)
				mu.Unlock()
				if reusable {
					return session, nil
				}
				return singleUseSession{session}, nil
			}}
		h := startServe(t, runner, true)
		for i := uint32(1); i <= 2; i++ {
			h.request(i, sqlRequest(fmt.Sprintf("op-%d", i), "SELECT 1"))
			term, _ := h.terminals(1)
			receiptOf(t, term[i])
		}
		if reusable && (len(opened) != 1 || !opened[0].kept || opened[0].isClosed()) {
			t.Fatalf("reusable session was not kept warm: opens=%d", len(opened))
		}
		if !reusable && (len(opened) != 2 || opened[0].kept || !opened[0].isClosed() || !opened[1].isClosed()) {
			t.Fatalf("single-use session was reused or left open: opens=%d", len(opened))
		}
		_ = frame.WriteClose(h.in)
		h.wantDone(false)
	}
}

func TestServeLoop_ConcurrentStreams(t *testing.T) {
	session := &fakeQuerySession{}
	var opens atomic.Int32
	h := startServe(t, sessionRunner(session, &opens), true)
	const n = 50
	for i := 1; i <= n; i++ {
		h.request(uint32(i), sqlRequest(fmt.Sprintf("op-%d", i), "SELECT 1"))
	}
	term, results := h.terminals(n)
	for i := 1; i <= n; i++ {
		s := uint32(i)
		r := receiptOf(t, term[s])
		if r.OperationID != fmt.Sprintf("op-%d", i) || r.Outcome != operations.Completed || results[s] == 0 {
			t.Fatalf("stream %d: %+v results=%d", i, r, results[s])
		}
	}
	if opens.Load() != 1 {
		t.Fatalf("session opened %d times for concurrent requests, want 1", opens.Load())
	}
	_ = frame.WriteClose(h.in)
	h.wantDone(false)
}

func TestServeLoop_ConcurrentOneFailingStream(t *testing.T) {
	session := &fakeQuerySession{fn: func(_ context.Context, sql string) error {
		if sql == "FAIL" {
			return errors.New("boom")
		}
		return nil
	}}
	h := startServe(t, sessionRunner(session, nil), true)
	const n, bad = 50, 17
	for i := 1; i <= n; i++ {
		sql := "SELECT 1"
		if i == bad {
			sql = "FAIL"
		}
		h.request(uint32(i), sqlRequest(fmt.Sprintf("op-%d", i), sql))
	}
	term, _ := h.terminals(n)
	for i := 1; i <= n; i++ {
		if i == bad {
			wantFailedReceipt(t, term[uint32(i)])
			continue
		}
		receiptOf(t, term[uint32(i)])
	}
	if session.isClosed() {
		t.Fatal("session closed by one stream's failure")
	}
	_ = frame.WriteClose(h.in)
	h.wantDone(false)
}

func TestServeLoop_CancelMidQuery(t *testing.T) {
	started := make(chan struct{}, 1)
	session := &fakeQuerySession{fn: func(ctx context.Context, sql string) error {
		if sql == "BLOCK" {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}}
	h := startServe(t, sessionRunner(session, nil), true)
	h.request(1, sqlRequest("op-1", "BLOCK"))
	h.request(2, sqlRequest("op-2", "SELECT 1"))
	<-started
	if err := frame.WriteCancel(h.in, 1); err != nil {
		t.Fatal(err)
	}
	if err := frame.WriteCancel(h.in, 99); err != nil { // unknown stream is ignored
		t.Fatal(err)
	}
	term, _ := h.terminals(2)
	wantFailedReceipt(t, term[1])
	receiptOf(t, term[2])
	h.request(3, sqlRequest("op-3", "SELECT 1"))
	term, _ = h.terminals(1)
	receiptOf(t, term[3])
	_ = frame.WriteClose(h.in)
	h.wantDone(false)
}

func TestServeLoop_DuplicateInFlightStreamIsFatal(t *testing.T) {
	started := make(chan struct{}, 1)
	session := &fakeQuerySession{fn: func(ctx context.Context, sql string) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}}
	h := startServe(t, sessionRunner(session, nil), true)
	h.request(7, sqlRequest("op-1", "BLOCK"))
	<-started
	h.request(7, sqlRequest("op-2", "BLOCK"))
	h.wantDone(true)
	// the in-flight stream still ends with its terminal frame
	for f := range h.frames {
		if f.Type == frame.Receipt || f.Type == frame.Error {
			return
		}
	}
	t.Fatal("in-flight stream got no terminal frame")
}

func TestServeLoop_BusyBeyondMaxStreams(t *testing.T) {
	release := make(chan struct{})
	var running atomic.Int32
	session := &fakeQuerySession{fn: func(ctx context.Context, sql string) error {
		running.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}}
	h := startServe(t, sessionRunner(session, nil), true)
	for i := 1; i <= maxServeStreams; i++ {
		h.request(uint32(i), sqlRequest(fmt.Sprintf("op-%d", i), "SELECT 1"))
	}
	for running.Load() < maxServeStreams {
		time.Sleep(5 * time.Millisecond)
	}
	h.request(maxServeStreams+1, sqlRequest("op-extra", "SELECT 1"))
	f := h.next()
	if f.Type != frame.Error || f.Stream != maxServeStreams+1 || string(f.Payload) != "busy" {
		t.Fatalf("expected busy on stream %d, got %v %q", maxServeStreams+1, f.Type, f.Payload)
	}
	close(release)
	term, _ := h.terminals(maxServeStreams)
	for _, f := range term {
		receiptOf(t, f)
	}
	_ = frame.WriteClose(h.in)
	h.wantDone(false)
}

// Reads after the first carry no secrets: the worker serves them with the
// credentials it was given, and refuses a secretless request it cannot match.
func TestServeLoop_SecretlessRequestsUseBoundCredentials(t *testing.T) {
	session := &fakeQuerySession{}
	var opened []adapter.ConnectionSpec
	var mu sync.Mutex
	runner := Runner{SessionCache: &SessionCache{TTL: time.Minute, Max: 4}, Open: func(_ context.Context, spec adapter.ConnectionSpec, _ operations.Request) (adapter.Session, error) {
		mu.Lock()
		opened = append(opened, spec)
		mu.Unlock()
		return session, nil
	}}
	secretless := func(op string) adapter.ProcessRequest {
		r := sqlRequest(op, "SELECT 1")
		r.Source.DSN = ""
		return r
	}

	h := startServe(t, runner, true)
	h.request(1, secretless("op-0"))
	if f := h.next(); f.Type != frame.Error || f.Stream != 1 {
		t.Fatalf("secretless request before credentials: %v", f.Type)
	}
	h.wantDone(true)

	h = startServe(t, runner, true)
	h.request(1, sqlRequest("op-1", "SELECT 1"))
	h.terminals(1)
	h.request(2, secretless("op-2"))
	term, _ := h.terminals(1)
	if r := receiptOf(t, term[2]); r.Outcome != operations.Completed {
		t.Fatalf("secretless read failed: %+v", r)
	}
	other := secretless("op-3")
	other.Source.Database, other.Request.Connection.Database = "otherdb", "otherdb"
	other.RequestSHA256, _ = operations.Digest(other.Request)
	h.request(3, other)
	if f := h.next(); f.Type != frame.Error || f.Stream != 3 {
		t.Fatalf("secretless request for another database: %v", f.Type)
	}
	h.wantDone(true)
	mu.Lock()
	defer mu.Unlock()
	if len(opened) != 1 || opened[0].DSN == "" {
		t.Fatalf("session opened %d times, with DSN %q", len(opened), opened[0].DSN)
	}
}

// A worker is bound to one database and schema of a connection: a request for
// another one is a cross-connection request, not a secretless reuse.
func TestServeLoop_BoundKeyIncludesDatabaseAndSchema(t *testing.T) {
	h := startServe(t, sessionRunner(&fakeQuerySession{}, nil), true)
	h.request(1, sqlRequest("op-1", "SELECT 1"))
	h.terminals(1)
	other := sqlRequest("op-2", "SELECT 1")
	other.Source.Schema, other.Request.Connection.Schema = "sales", "sales"
	other.RequestSHA256, _ = operations.Digest(other.Request)
	h.request(2, other)
	if f := h.next(); f.Type != frame.Error || f.Stream != 2 || !strings.Contains(string(f.Payload), "cross-tenant") {
		t.Fatalf("request for another schema accepted: %v %q", f.Type, f.Payload)
	}
	h.wantDone(true)
}
