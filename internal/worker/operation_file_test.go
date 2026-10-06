// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestOperationFileRequiresExactBytesAndFinalAuthorization(t *testing.T) {
	data := []byte("id,value\n1,private-test-value\n")
	sum := sha256.Sum256(data)
	snapshot := filesnapshot.Descriptor{Version: 1, Format: "csv", Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	for _, mode := range []string{"valid", "missing_verified", "revoked", "expired", "future", "wrong_hash", "truncated", "extra", "header_proof", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			record, request, _ := operationResolverFixture(t)
			record.Kind, request.Kind = operations.QueryRead, operations.QueryRead
			request.Spec = operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT * FROM uploaded"}}
			record.RequestSHA256, _ = operations.Digest(request)
			revision := strings.Repeat("a", 64)
			resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
				var input struct {
					operationResolutionRequest
					SourceRevision string                  `json:"source_revision"`
					Snapshot       filesnapshot.Descriptor `json:"snapshot"`
				}
				if r.URL.Path != "/internal/kelvo/operation-file" || json.NewDecoder(r.Body).Decode(&input) != nil || input.Grant != record.AuthorityToken || input.SourceRevision != revision || input.Snapshot != snapshot || input.OperationID != record.ID || input.Claim != record.Binding.Claim {
					t.Error("file request lost private binding")
					w.WriteHeader(403)
					return
				}
				if mode == "redirect" {
					w.Header().Set("Location", "/other")
					w.WriteHeader(307)
					return
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("X-Kelvo-File-SHA256", snapshot.SHA256)
				if mode == "wrong_hash" {
					w.Header().Set("X-Kelvo-File-SHA256", strings.Repeat("b", 64))
				}
				if mode == "header_proof" {
					w.Header().Set("X-Kelvo-File-Verified", snapshot.SHA256)
				}
				w.Header().Set("Trailer", "X-Kelvo-File-Verified, X-Kelvo-Source-Valid-Until")
				if mode == "header_proof" {
					w.Header().Set("Trailer", "X-Kelvo-Source-Valid-Until")
				}
				body := data
				if mode == "truncated" {
					body = body[:len(body)-1]
				}
				if mode == "extra" {
					body = append(append([]byte{}, body...), 'x')
				}
				_, _ = w.Write(body)
				if mode != "missing_verified" && mode != "revoked" {
					w.Header().Set("X-Kelvo-File-Verified", snapshot.SHA256)
				}
				until := time.Now().Unix() + 5
				if mode == "expired" {
					until = time.Now().Unix()
				}
				if mode == "future" {
					until = time.Now().Unix() + 60
				}
				w.Header().Set("X-Kelvo-Source-Valid-Until", strconv.FormatInt(until, 10))
			})
			resolver.url += "/internal/kelvo/resolve"
			e := &Executor{connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
			var destination bytes.Buffer
			until, err := e.FetchOperationFile(context.Background(), record, request, revision, snapshot, &destination)
			if mode == "valid" {
				if err != nil || until <= time.Now().Unix() || !bytes.Equal(destination.Bytes(), data) {
					t.Fatal("snapshot validation failed", until, err)
				}
			} else if err == nil || until != 0 {
				t.Fatal("unverified source snapshot accepted", until, err)
			}
		})
	}
}

func TestOperationFileCatalogRejectsAuthorityExpansion(t *testing.T) {
	source := catalog.Source{ID: "source_1", Type: "csv", Options: map[string]string{"file_format": "csv", "file_bytes": "8", "file_sha256": strings.Repeat("a", 64)}}
	if _, err := operationFileDescriptor(source, nil); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*catalog.Source){
		func(s *catalog.Source) { s.Path = "/tmp/private" }, func(s *catalog.Source) { s.URLEnv = "KELVO_SOURCE_REQUEST_0_URL" }, func(s *catalog.Source) { s.Options["file_bytes"] = "08" }, func(s *catalog.Source) { s.Options["file_format"] = "parquet" }, func(s *catalog.Source) { s.Federation = &catalog.FederationConfig{} },
	} {
		bad := source
		bad.Options = make(map[string]string, len(source.Options))
		for k, v := range source.Options {
			bad.Options[k] = v
		}
		mutate(&bad)
		if _, err := operationFileDescriptor(bad, nil); err == nil {
			t.Fatal("unbound file catalog accepted")
		}
	}
	if _, err := operationFileDescriptor(source, map[string]string{"EXTRA": "private"}); err == nil {
		t.Fatal("file source received a credential")
	}
}
