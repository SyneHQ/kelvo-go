package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestFilePublicationBindsPrivateRequestAndNeverRetries(t *testing.T) {
	for _, mode := range []string{"commit", "conflict", "wrong-operation", "wrong-hash", "inconsistent-status", "redirect", "lost-response"} {
		t.Run(mode, func(t *testing.T) {
			record, request, _ := operationResolverFixture(t)
			request.Kind = operations.StatementExecute
			request.IdempotencyKey = "file-write"
			request.Connection.Schema = ""
			request.Spec = operations.Spec{Statement: &operations.StatementSpec{SQL: "UPDATE trips SET fare=2", Transaction: operations.TransactionRequired}}
			record.Kind = request.Kind
			record.RequestSHA256, _ = operations.Digest(request)
			p := filesnapshot.Publication{Version: 1, OperationID: record.ID, RequestSHA256: record.RequestSHA256, Original: filesnapshot.Descriptor{Version: 1, Format: "sqlite", Bytes: 8, SHA256: strings.Repeat("a", 64)}, Replacement: filesnapshot.Descriptor{Version: 1, Format: "sqlite", Bytes: 9, SHA256: strings.Repeat("b", 64)}}
			var calls atomic.Int64
			resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/internal/kelvo/operation-file-commit" || r.Header.Get("Content-Type") != "application/vnd.kelvo.file-update" {
					t.Error("wrong private request")
				}
				var size [4]byte
				if _, err := io.ReadFull(r.Body, size[:]); err != nil {
					t.Error(err)
					return
				}
				raw := make([]byte, binary.BigEndian.Uint32(size[:]))
				if _, err := io.ReadFull(r.Body, raw); err != nil {
					t.Error(err)
					return
				}
				var data struct {
					operationResolutionRequest
					SourceRevision string                   `json:"source_revision"`
					Snapshot       filesnapshot.Descriptor  `json:"snapshot"`
					Publication    filesnapshot.Publication `json:"publication"`
				}
				if json.Unmarshal(raw, &data) != nil || data.Grant != record.AuthorityToken || data.Publication != p || data.Snapshot != p.Original || data.OperationID != record.ID {
					t.Error("publication binding lost")
				}
				body, _ := io.ReadAll(r.Body)
				if string(body) != "candidate" {
					t.Error("candidate body lost")
				}
				if mode == "redirect" {
					w.Header().Set("Location", "/other")
					w.WriteHeader(307)
					return
				}
				if mode == "lost-response" {
					return
				}
				reply := filesnapshot.PublicationReceipt{Version: 1, OperationID: p.OperationID, RequestSHA256: p.RequestSHA256, ReplacementSHA256: p.Replacement.SHA256, Committed: mode != "conflict"}
				if mode == "wrong-operation" {
					reply.OperationID = "other"
				}
				if mode == "wrong-hash" {
					reply.ReplacementSHA256 = strings.Repeat("c", 64)
				}
				w.Header().Set("Content-Type", "application/json")
				if mode == "conflict" || mode == "inconsistent-status" {
					w.WriteHeader(http.StatusConflict)
				}
				json.NewEncoder(w).Encode(reply)
			})
			resolver.url += "/internal/kelvo/resolve"
			e := &Executor{connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
			committed, err := e.PublishOperationFile(context.Background(), record, request, strings.Repeat("d", 64), p, bytes.NewBufferString("candidate"))
			if calls.Load() != 1 {
				t.Fatal("request replayed", calls.Load())
			}
			if mode == "commit" {
				if err != nil || !committed {
					t.Fatal(committed, err)
				}
			} else if mode == "conflict" {
				if err != nil || committed {
					t.Fatal(committed, err)
				}
			} else if err == nil {
				t.Fatal("unverified receipt accepted")
			}
		})
	}
}
