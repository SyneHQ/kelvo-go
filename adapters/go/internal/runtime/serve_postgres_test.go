package runtime

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/frame"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Real-PostgreSQL gate: 50 concurrent reads on one worker must succeed on a
// session pool of at most 4 connections.
func TestServeLoop_ConcurrentPostgresReadsShareBoundedPool(t *testing.T) {
	dsn := os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set KELVO_TEST_WARM_POSTGRES_DSN for the real PostgreSQL gate")
	}
	database := "postgres"
	if u, err := url.Parse(dsn); err == nil && strings.Trim(u.Path, "/") != "" {
		database = strings.Trim(u.Path, "/")
	}

	h := startServe(t, Runner{}, true)
	const n = 50
	now := time.Now().Unix()
	for i := 1; i <= n; i++ {
		req := operations.Request{Version: operations.Version, Kind: operations.QueryRead,
			Connection: operations.ConnectionRef{ID: "warm-postgres", Database: database, Schema: "public"},
			Spec:       operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT pg_backend_pid()::bigint AS pid FROM pg_sleep(0.2)"}}}
		digest, err := operations.Digest(req)
		if err != nil {
			t.Fatal(err)
		}
		h.request(uint32(i), adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: fmt.Sprintf("op-%d", i), RequestSHA256: digest, Request: req,
			Limits: adapter.ProcessLimits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10, TimeoutMS: 30000},
			Source: adapter.ConnectionSpec{Engine: "postgres", DSN: dsn, TenantID: "tenant-a", ConnectionID: "warm-postgres",
				Revision: "revision-1", Database: database, Schema: "public"},
			CredentialsValidUntil: now + 5, ExpiresAt: now + 60})
	}

	payloads := map[uint32]*bytes.Buffer{}
	var mu sync.Mutex
	term := map[uint32]frame.Frame{}
	for len(term) < n {
		f := h.next()
		mu.Lock()
		switch f.Type {
		case frame.Result:
			if payloads[f.Stream] == nil {
				payloads[f.Stream] = &bytes.Buffer{}
			}
			payloads[f.Stream].Write(f.Payload)
		case frame.Receipt, frame.Error:
			term[f.Stream] = f
		}
		mu.Unlock()
	}

	pids := map[int64]bool{}
	for i := 1; i <= n; i++ {
		s := uint32(i)
		if term[s].Type != frame.Receipt {
			t.Fatalf("stream %d failed: %q", i, term[s].Payload)
		}
		rd, err := ipc.NewReader(payloads[s])
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		for rd.Next() {
			col := rd.RecordBatch().Column(0).(*array.Int64)
			for j := 0; j < col.Len(); j++ {
				pids[col.Value(j)] = true
			}
		}
		if err := rd.Err(); err != nil && err != io.EOF {
			t.Fatalf("stream %d: %v", i, err)
		}
		rd.Release()
	}
	if len(pids) == 0 || len(pids) > 4 {
		t.Fatalf("distinct backend pids = %d, want 1..4", len(pids))
	}
	_ = frame.WriteClose(h.in)
	h.wantDone(false)
}
