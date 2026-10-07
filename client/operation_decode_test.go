// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestReadOperationResultRequiresExactReceiptAndCompleteArrow(t *testing.T) {
	for _, name := range []string{"valid", "rows", "hash", "missing_eos", "trailing_data", "decoded_limit", "wire_limit", "sink_failure"} {
		t.Run(name, func(t *testing.T) {
			request := operations.Request{Version: operations.Version, Kind: operations.QueryRead, Connection: operations.ConnectionRef{ID: "saved-a"}, Spec: operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT value FROM samples"}}}
			data := clientFixtureIPC(t)
			if name == "missing_eos" {
				data = data[:len(data)-8]
			}
			if name == "trailing_data" {
				data = append(data, 1)
			}
			digest := sha256.Sum256(data)
			status := operationFixtureStatus(t, request, string(operations.Completed))
			status.Receipt.Result = &operations.ResultRef{ID: "result-a", Format: "arrow_ipc", SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(data)), Rows: 3}
			if name == "rows" {
				status.Receipt.Result.Rows = 2
			}
			if name == "hash" {
				status.Receipt.Result.SHA256 = strings.Repeat("a", 64)
			}
			var calls atomic.Int64
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodGet {
					t.Error("result delivery attempted source execution")
				}
				if r.URL.Path == "/v1/operations/"+status.ID {
					writeOperationStatus(w, status, http.StatusOK)
					return
				}
				if r.URL.Path != "/v1/operations/"+status.ID+"/results" {
					t.Error("result endpoint changed")
				}
				w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
				_, _ = w.Write(data)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			limits := Limits{MaxRows: c.cfg.MaxRows, MaxDecodedBytes: c.cfg.MaxDecodedBytes, MaxWireBytes: c.cfg.MaxWireBytes}
			if name == "decoded_limit" {
				limits.MaxDecodedBytes = 1
			}
			if name == "wire_limit" {
				limits.MaxWireBytes = int64(len(data) - 1)
			}
			var rows int64
			sink := &clientFixtureSink{write: func(batch arrow.RecordBatch) error {
				if name == "sink_failure" {
					return errors.New("private sink diagnostics")
				}
				rows += batch.NumRows()
				return nil
			}}
			stats, err := c.ReadOperationResult(context.Background(), status, Authority{OperationGrant: operationFixtureGrant(t, request)}, sink, limits)
			if name == "valid" {
				if err != nil || stats.Rows != 3 || rows != 3 || stats.WireBytes != int64(len(data)) {
					t.Fatalf("verified result failed: %+v %v", stats, err)
				}
			} else if err == nil {
				t.Fatal("unverified or over-budget result accepted")
			}
			if name == "wire_limit" && calls.Load() != 0 {
				t.Fatal("receipt exceeding caller limit reached network")
			}
			if len(c.permits) != 0 {
				t.Fatal("result retained admission after delivery")
			}
		})
	}
}

func TestOperationDecoderRetainsAdmissionUntilSinkReturns(t *testing.T) {
	data := clientFixtureIPC(t)
	request, status := operationResultFixture(t, data)
	auth := Authority{OperationGrant: operationFixtureGrant(t, request)}
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/results") {
			w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
			_, _ = w.Write(data)
			return
		}
		writeOperationStatus(w, status, http.StatusOK)
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	entered, unblock := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.DecodeOperationResult(ctx, status, auth, &clientFixtureSink{write: func(arrow.RecordBatch) error { close(entered); <-unblock; return nil }})
		done <- err
	}()
	clientFixtureAwait(t, entered)
	cancel()
	if _, err := c.SubmitOperation(context.Background(), request, auth); err == nil {
		t.Error("cancelled decoder released admission while sink owned a batch")
	}
	close(unblock)
	if err := clientFixtureAwait(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("decode cancellation: %v", err)
	}
	if len(c.permits) != 0 {
		t.Fatal("decoder admission leaked")
	}
}
