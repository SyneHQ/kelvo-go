// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

// Embed only the unrelated Store methods; all snapshot access shared with the
// HTTP worker fixture is protected here, including claim and terminal CAS.
type compressionRelayStore struct {
	gatewayStore
	mu             sync.Mutex
	snapshot       Snapshot
	failCommit     bool
	commitAttempts int
	beforeCommit   func()
}

func (s *compressionRelayStore) Get(_ context.Context, id string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != s.snapshot.Job.ID {
		return Snapshot{}, ErrNotFound
	}
	return s.snapshot, nil
}

func (s *compressionRelayStore) CompareAndSwap(_ context.Context, previous Snapshot, next Job) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous.Revision != s.snapshot.Revision || previous.Job.ID != s.snapshot.Job.ID {
		return Snapshot{}, ErrConflict
	}
	if next.State == Succeeded {
		s.commitAttempts++
		if s.beforeCommit != nil {
			s.beforeCommit()
		}
		if s.failCommit {
			return Snapshot{}, ErrConflict
		}
	}
	s.snapshot = Snapshot{Job: next, Revision: previous.Revision + 1}
	return s.snapshot, nil
}

func (s *compressionRelayStore) resultReady(claim string, stats query.Stats) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Job.State != Claimed || claim == "" || claim != s.snapshot.Job.Claim {
		return false
	}
	s.snapshot.Job.State = ResultReady
	s.snapshot.Job.Stats = stats
	s.snapshot.Revision++
	return true
}

func compressedRelayFixture(t *testing.T, compression string) ([]byte, query.Stats) {
	t.Helper()
	limits := query.DefaultLimits()
	limits.ResultCompression = compression
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	var output bytes.Buffer
	sink := worker.NewIPCSink(&output, limits)
	defer sink.Abort()
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	var stats query.Stats
	for batch := 0; batch < 3; batch++ {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for row := 0; row < 256; row++ {
			builder.Field(0).(*array.Int64Builder).Append(int64(batch*256+row) + 9007199254740993)
			if row%11 == 0 {
				builder.Field(1).(*array.StringBuilder).AppendNull()
			} else {
				builder.Field(1).(*array.StringBuilder).Append(strings.Repeat("relay-value-", 64))
			}
		}
		record := builder.NewRecordBatch()
		builder.Release()
		stats.Rows += record.NumRows()
		stats.Bytes += arrowutil.TotalRecordSize(record)
		stats.Batches++
		err := sink.Write(record)
		record.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	stats.WireBytes = sink.EncodedBytes()
	return output.Bytes(), stats
}

func TestGatewayRelaysCompressedIPCOnlyAfterDurableSuccess(t *testing.T) {
	encoded, stats := compressedRelayFixture(t, "lz4_frame")
	uncompressed, _ := compressedRelayFixture(t, "none")
	eos := []byte{255, 255, 255, 255, 0, 0, 0, 0}
	if len(encoded) >= len(uncompressed)/2 || !bytes.HasSuffix(encoded, eos) || stats.WireBytes != int64(len(encoded)) {
		t.Fatal("fixture must be a complete, compressed Arrow stream with exact wire accounting")
	}
	for _, failCommit := range []bool{false, true} {
		name := "success"
		if failCommit {
			name = "commit conflict"
		}
		t.Run(name, func(t *testing.T) {
			limits := query.DefaultLimits()
			limits.ResultCompression = "lz4_frame"
			store := &compressionRelayStore{
				gatewayStore: gatewayStore{policy: Policy{TenantID: "relay", Limits: limits}},
				failCommit:   failCommit,
				snapshot: Snapshot{Revision: 1, Job: Job{
					ID: "compressed-result", TenantID: "relay", State: Assigned,
					Owner: "worker-owner", WorkerID: "worker-one", ExpiresAt: time.Now().Add(time.Minute),
				}},
			}
			output := httptest.NewRecorder()
			store.beforeCommit = func() {
				if !bytes.Equal(output.Body.Bytes(), encoded[:len(encoded)-len(eos)]) {
					t.Error("gateway changed bytes or released EOS before durable success")
				}
			}
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/internal/queries/compressed-result/results" {
					t.Error("unexpected worker result request")
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
				// Fragment writes across IPC messages and the gateway's eight-byte
				// tail. Mark ready before the final bytes and HTTP EOF are visible.
				body := encoded[:len(encoded)-len(eos)]
				for offset := 0; offset < len(body); offset += 113 {
					end := min(offset+113, len(body))
					if _, err := w.Write(body[offset:end]); err != nil {
						t.Errorf("worker fixture write: %v", err)
						return
					}
				}
				if !store.resultReady(r.Header.Get("X-Kelvo-Claim"), stats) {
					t.Error("worker did not receive the gateway's owned claim")
					return
				}
				if _, err := w.Write(eos[:3]); err != nil {
					t.Errorf("worker fixture EOS prefix: %v", err)
					return
				}
				if _, err := w.Write(eos[3:]); err != nil {
					t.Errorf("worker fixture EOS suffix: %v", err)
				}
			}))
			defer upstream.Close()
			endpoint, err := url.Parse(upstream.URL)
			if err != nil {
				t.Fatal(err)
			}
			tenant := gatewayTenant{store: store, workers: map[string]workerEndpoint{
				"worker-one": {url: endpoint, client: upstream.Client()},
			}}
			aborted := false
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						if recovered != http.ErrAbortHandler {
							t.Fatalf("unexpected gateway panic: %v", recovered)
						}
						aborted = true
					}
				}()
				gateway := &Gateway{}
				gateway.results(output, httptest.NewRequest(http.MethodGet, "/v1/queries/compressed-result/results", nil), tenant, "compressed-result", nil)
			}()
			final, err := store.Get(context.Background(), "compressed-result")
			if err != nil {
				t.Fatal(err)
			}
			if output.Code != http.StatusOK || output.Header().Get("Content-Type") != "application/vnd.apache.arrow.stream" || output.Header().Get("Kelvo-Result-Completion") != "durable-eos-v1" {
				t.Fatalf("unexpected result response: status=%d headers=%v", output.Code, output.Header())
			}
			if failCommit {
				// The advertised protocol is not itself success. Even with that
				// header present, a failed commit must withhold final Arrow EOS.
				if !aborted || final.Job.State != Failed || store.commitAttempts != 8 {
					t.Fatalf("failed commit did not abort delivery: aborted=%v state=%s attempts=%d", aborted, final.Job.State, store.commitAttempts)
				}
				if !bytes.Equal(output.Body.Bytes(), encoded[:len(encoded)-len(eos)]) {
					t.Fatal("failed commit released EOS or changed the preceding compressed bytes")
				}
				return
			}
			if aborted || final.Job.State != Succeeded || store.commitAttempts != 1 || final.Job.Stats.WireBytes != int64(len(encoded)) {
				t.Fatalf("successful relay state/accounting changed: aborted=%v job=%+v attempts=%d", aborted, final.Job, store.commitAttempts)
			}
			if !bytes.Equal(output.Body.Bytes(), encoded) {
				t.Fatal("gateway decoded, re-encoded, or changed the compressed IPC bytes")
			}
			reader, err := ipc.NewReader(bytes.NewReader(output.Body.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			batch := 0
			for reader.Next() {
				record := reader.RecordBatch()
				if batch >= 3 || record.NumRows() != 256 {
					t.Fatal("relayed batch shape changed")
				}
				ids, labels := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
				for row := 0; row < 256; row++ {
					if ids.Value(row) != int64(batch*256+row)+9007199254740993 || labels.IsNull(row) != (row%11 == 0) {
						t.Fatal("relayed integer or NULL changed")
					}
					if !labels.IsNull(row) && labels.Value(row) != strings.Repeat("relay-value-", 64) {
						t.Fatal("relayed string changed")
					}
				}
				batch++
			}
			if reader.Err() != nil || batch != 3 {
				t.Fatalf("incomplete relayed IPC: batches=%d error=%v", batch, reader.Err())
			}
		})
	}
}
