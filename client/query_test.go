// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func queryFixtureStatus(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(QueryStatus{ID: "fixture_handle", State: "succeeded", Stats: query.Stats{Rows: 3, Batches: 1, WireBytes: int64(len(data))}})
}
func TestQueryCompletionInStandaloneAndCluster(t *testing.T) {
	data := clientFixtureIPC(t)
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprint(cluster), func(t *testing.T) {
			var calls atomic.Int64
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Authorization") != "Bearer "+clientFixtureToken || r.ProtoMajor != 1 || r.TLS.Version != tls.VersionTLS13 {
					t.Error("unsafe transport")
				}
				switch r.URL.Path {
				case "/v1/queries":
					clientFixtureAccepted(w)
				case "/v1/queries/fixture_handle/results":
					w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
					if cluster {
						w.Header().Set("Kelvo-Result-Completion", "durable-eos-v1")
					}
					_, _ = w.Write(data)
				case "/v1/queries/fixture_handle":
					queryFixtureStatus(w, data)
				default:
					t.Error("unexpected request")
					w.WriteHeader(404)
				}
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			stats, err := c.Query(context.Background(), clientFixtureRequest(), Authority{}, &clientFixtureSink{write: func(batch arrow.RecordBatch) error {
				signed := batch.Column(0).(*array.Int64)
				unsigned := batch.Column(1).(*array.Uint64)
				if signed.Value(0) != math.MaxInt64 || signed.Value(1) != -9007199254740993 || !signed.IsNull(2) || unsigned.Value(0) != math.MaxUint64 {
					t.Error("integer or NULL precision lost")
				}
				return nil
			}})
			if err != nil || stats.Rows != 3 || stats.Batches != 1 || stats.Server.Rows != 3 || stats.WireBytes != int64(len(data)) || calls.Load() != 3 {
				t.Fatalf("query: %+v %v calls=%d", stats, err, calls.Load())
			}
		})
	}
}
func TestQueryRejectsUnverifiedCompletion(t *testing.T) {
	data := clientFixtureIPC(t)
	for _, mode := range []string{"no_eos", "trailing", "bad_completion", "duplicate_completion", "bad_status", "bad_id", "wrong_rows", "wrong_batches", "wrong_bytes", "terminal_error", "ambiguous_stats", "truncated_status", "content_type"} {
		t.Run(mode, func(t *testing.T) {
			var cancels atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/queries":
					clientFixtureAccepted(w)
				case "/v1/queries/fixture_handle/cancel":
					cancels.Add(1)
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"id":"fixture_handle","state":"cancelled"}`)
				case "/v1/queries/fixture_handle/results":
					w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
					payload := data
					switch mode {
					case "no_eos":
						payload = data[:len(data)-8]
					case "trailing":
						payload = append(append([]byte(nil), data...), 0)
					case "bad_completion":
						w.Header().Set("Kelvo-Result-Completion", "unknown")
					case "duplicate_completion":
						w.Header().Add("Kelvo-Result-Completion", "durable-eos-v1")
						w.Header().Add("Kelvo-Result-Completion", "durable-eos-v1")
					case "content_type":
						w.Header().Set("Content-Type", "application/json")
					}
					_, _ = w.Write(payload)
				case "/v1/queries/fixture_handle":
					status := QueryStatus{ID: "fixture_handle", State: "succeeded", Stats: query.Stats{Rows: 3, Batches: 1, WireBytes: int64(len(data))}}
					w.Header().Set("Content-Type", "application/json")
					switch mode {
					case "bad_status":
						status.State = "running"
					case "bad_id":
						status.ID = "other"
					case "wrong_rows":
						status.Stats.Rows++
					case "wrong_batches":
						status.Stats.Batches++
					case "wrong_bytes":
						status.Stats.WireBytes++
					case "terminal_error":
						status.Error = &query.Error{Code: "QUERY_FAILED", Message: "remote-secret-diagnostic"}
					case "ambiguous_stats":
						_, _ = io.WriteString(w, `{"id":"fixture_handle","state":"succeeded","stats":{},"stats":{}}`)
						return
					case "truncated_status":
						_, _ = io.WriteString(w, `{"id":"fixture_handle"`)
						return
					}
					_ = json.NewEncoder(w).Encode(status)
				default:
					t.Error("unexpected request")
					w.WriteHeader(404)
				}
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			_, err := c.Query(context.Background(), clientFixtureRequest(), Authority{}, &clientFixtureSink{})
			clientFixtureError(t, err, "PROTOCOL_ERROR")
			if cancels.Load() != 1 {
				t.Fatal("accepted query did not receive one cleanup attempt")
			}
		})
	}
}
func TestClientTLSAndRedirectRestrictions(t *testing.T) {
	for _, url := range []string{"http://localhost", "https://user:password@localhost", "https://localhost/path", "https://localhost?key=x", "https://localhost#fragment"} {
		if c, err := New(Config{URL: url}); err == nil {
			c.Close()
			t.Fatalf("accepted %s", url)
		}
	}
	for _, cfg := range []*tls.Config{{InsecureSkipVerify: true}, {MaxVersion: tls.VersionTLS12}} {
		if c, err := New(Config{URL: "https://localhost", TLSConfig: cfg}); err == nil {
			c.Close()
			t.Fatal("unsafe TLS accepted")
		}
	}
	for _, mode := range []string{"untrusted", "old_tls", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			var called atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				called.Add(1)
				http.Redirect(w, r, "https://localhost/elsewhere", http.StatusTemporaryRedirect)
			}, map[string]uint16{"untrusted": tls.VersionTLS13, "old_tls": tls.VersionTLS12, "redirect": tls.VersionTLS13}[mode])
			cfg := clientFixtureConfig(t, server)
			if mode == "untrusted" {
				cfg.TLSConfig = nil
			}
			c := clientFixtureClient(t, cfg)
			if _, err := c.Submit(context.Background(), clientFixtureRequest(), Authority{}); err == nil {
				t.Fatal("unsafe response accepted")
			}
			if (mode == "redirect" && called.Load() != 1) || (mode != "redirect" && called.Load() != 0) {
				t.Fatal("TLS rejection or redirect bound failed")
			}
		})
	}
}
func TestAuthorityIsPerRequestAndConflictsFailBeforeNetwork(t *testing.T) {
	const count = 12
	var calls atomic.Int32
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req query.Request
		if json.NewDecoder(r.Body).Decode(&req) != nil || r.Header.Get("X-Kelvo-Delegation") != req.Delegation || r.Header.Get("Authorization") != "Bearer token-"+req.ConnectionID || r.Header.Get("X-Kelvo-Operation-Grant") != "" {
			t.Error("request authority was mixed across calls")
		}
		clientFixtureAccepted(w)
	}, tls.VersionTLS13)
	cfg := clientFixtureConfig(t, server)
	cfg.MaxConcurrent = count
	c := clientFixtureClient(t, cfg)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := clientFixtureRequest()
			req.ConnectionID = fmt.Sprintf("source_%d", i)
			auth := Authority{BearerToken: "token-" + req.ConnectionID, Delegation: "grant-" + req.ConnectionID}
			if _, err := c.Submit(context.Background(), req, auth); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	for _, auth := range []Authority{{Delegation: "different"}, {OperationGrant: "unexpected"}, {InputGrant: "unexpected"}, {BearerToken: "bad\nheader"}} {
		req := clientFixtureRequest()
		req.Delegation = "original"
		if _, err := c.Submit(context.Background(), req, auth); err == nil {
			t.Fatal("conflicting authority accepted")
		}
	}
	if calls.Load() != count {
		t.Fatal("invalid authority reached server")
	}
}
func TestQueryAdmissionCancellationAndControlLease(t *testing.T) {
	data := clientFixtureIPC(t)
	entered := make(chan struct{})
	unblock := make(chan struct{})
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queries":
			clientFixtureAccepted(w)
		case "/v1/queries/fixture_handle/results":
			clientFixtureResult(w, data)
		case "/v1/queries/fixture_handle/cancel":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"fixture_handle","state":"cancelled"}`)
		case "/v1/queries/fixture_handle/connection-lease":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"valid_until": time.Now().Add(5 * time.Second).Unix()})
		default:
			t.Error("unexpected request")
			w.WriteHeader(404)
		}
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.Query(ctx, clientFixtureRequest(), Authority{}, &clientFixtureSink{write: func(arrow.RecordBatch) error { close(entered); <-unblock; return nil }})
		done <- err
	}()
	clientFixtureAwait(t, entered)
	binding := resolver.Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	if _, err := c.ValidateConnectionLease(context.Background(), "fixture_handle", "grant", binding); err != nil {
		t.Fatal("lease callback deadlocked", err)
	}
	cancel()
	_, err := c.Submit(context.Background(), clientFixtureRequest(), Authority{})
	clientFixtureError(t, err, "RESOURCE_EXHAUSTED")
	close(unblock)
	if err := clientFixtureAwait(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if len(c.permits) != 0 {
		t.Fatal("admission leaked")
	}
}

func TestResponsePeerExpiryRejectsStaleOrUnverifiedChains(t *testing.T) {
	now := time.Now()
	leaf := &x509.Certificate{Raw: []byte("leaf"), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
	root := &x509.Certificate{Raw: []byte("root"), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Minute)}
	state := &tls.ConnectionState{HandshakeComplete: true, Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf, root}}}
	if until, ok := responsePeerExpiry(state, now); !ok || !until.Equal(root.NotAfter) {
		t.Fatal("verified chain expiry was not bounded")
	}
	for _, mode := range []string{"expired", "future", "unverified", "wrong_leaf", "old_tls"} {
		t.Run(mode, func(t *testing.T) {
			copyState := *state
			copyRoot := *root
			copyState.VerifiedChains = [][]*x509.Certificate{{leaf, &copyRoot}}
			switch mode {
			case "expired":
				copyRoot.NotAfter = now
			case "future":
				copyRoot.NotBefore = now.Add(time.Minute)
			case "unverified":
				copyState.VerifiedChains = nil
			case "wrong_leaf":
				copyState.VerifiedChains = [][]*x509.Certificate{{root}}
			case "old_tls":
				copyState.Version = tls.VersionTLS12
			}
			if _, ok := responsePeerExpiry(&copyState, now); ok {
				t.Fatal("invalid response chain accepted")
			}
		})
	}
}
func TestLeaseRejectsExtendedCustody(t *testing.T) {
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{"valid_until": time.Now().Add(time.Minute).Unix()})
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	b := resolver.Binding{WorkerID: "worker", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	if _, err := c.ValidateConnectionLease(context.Background(), "query-id", "grant", b); err == nil {
		t.Fatal("extended query custody accepted")
	}
	if _, err := c.ValidateOperationLease(context.Background(), "operation-id", operationFixtureGrant(t, operationFixtureRequest()), b); err == nil {
		t.Fatal("extended operation custody accepted")
	}
}

func TestQuerySubmissionRetainsOnlyUnambiguousHandleOnLostEOF(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprint(duplicate), func(t *testing.T) {
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				raw := `{"id":"fixture_handle","state":"queued"}`
				if duplicate {
					raw = `{"id":"fixture_handle","id":"other","state":"queued"}`
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", fmt.Sprint(len(raw)+10))
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, raw)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			handle, err := c.Submit(context.Background(), clientFixtureRequest(), Authority{})
			clientFixtureError(t, err, "PROTOCOL_ERROR")
			if duplicate && handle.ID != "" {
				t.Fatal("ambiguous handle retained")
			}
			if !duplicate && handle.ID != "fixture_handle" {
				t.Fatal("verified handle lost with envelope EOF")
			}
		})
	}
}
