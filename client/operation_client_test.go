// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

func operationFixtureRequest() operations.Request {
	return operations.Request{Version: operations.Version, Kind: operations.StatementExecute,
		Connection: operations.ConnectionRef{ID: "saved-a", Database: "warehouse", Schema: "public"}, IdempotencyKey: "change-fixture-1",
		Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: "UPDATE sample SET active=true WHERE id=1", Transaction: operations.TransactionRequired}}}
}

func operationFixtureGrant(t *testing.T, request operations.Request) string {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := operations.Digest(request)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	grant, err := operations.SignGrant(operations.GrantClaims{Version: operations.GrantVersion, Issuer: "gateway-fixture", Audience: "kelvo", ClusterTenant: "team-a", ServicePrincipal: "gateway", AppTeam: "app-team-a", Subject: operations.Subject{Kind: "user", ID: "user-a"}, ID: "grant-fixture-1", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(), ConnectionID: request.Connection.ID, Operation: request.Kind, RequestSHA256: digest, Authorization: operations.Authorization{Kind: "trusted_app"}}, private)
	if err != nil {
		t.Fatal(err)
	}
	return grant
}

func operationFixtureStatus(t *testing.T, request operations.Request, state string) operations.Response {
	t.Helper()
	digest, err := operations.Digest(request)
	if err != nil {
		t.Fatal(err)
	}
	status := operations.Response{Version: operations.Version, ID: "operation-fixture-1", RequestSHA256: digest, State: state}
	if state == string(operations.Completed) {
		effect := operations.EffectNone
		if request.Kind.Mutating() {
			effect = operations.EffectCommitted
		}
		status.Receipt = &operations.Receipt{Version: operations.Version, OperationID: status.ID, RequestSHA256: digest, Outcome: operations.Completed, Effect: effect}
	}
	return status
}

func writeOperationStatus(w http.ResponseWriter, status operations.Response, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(status)
}

func assertOperationUncertain(t *testing.T, err error, id, digest string) *OperationUncertainError {
	t.Helper()
	var uncertain *OperationUncertainError
	if !errors.As(err, &uncertain) || uncertain.OperationID != id || uncertain.RequestSHA256 != digest {
		t.Fatalf("operation uncertainty/correlation lost: %v", err)
	}
	if strings.Contains(err.Error(), clientFixtureToken) || strings.Contains(err.Error(), "remote-secret-diagnostic") {
		t.Fatal("operation error disclosed credentials or remote diagnostics")
	}
	return uncertain
}

func TestExecuteOperationSubmitsOnceAndReturnsCommittedReceipt(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	queued := operationFixtureStatus(t, request, "queued")
	running := operationFixtureStatus(t, request, "running")
	completed := operationFixtureStatus(t, request, string(operations.Completed))
	var submissions, polls atomic.Int64
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+clientFixtureToken || r.Header.Get("X-Kelvo-Operation-Grant") != grant || r.Header.Get("X-Kelvo-Delegation") != "" || r.Header.Get("Accept-Encoding") != "identity" || r.ProtoMajor != 1 || r.TLS.Version != tls.VersionTLS13 {
			t.Error("operation left the restricted identity transport")
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/operations":
			submissions.Add(1)
			raw, _ := io.ReadAll(r.Body)
			decoded, err := operations.ParseRequest(raw)
			if err != nil || !reflect.DeepEqual(decoded, request) {
				t.Error("operation request changed in transport")
			}
			writeOperationStatus(w, queued, http.StatusCreated)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/"+queued.ID:
			if polls.Add(1) == 1 {
				writeOperationStatus(w, running, http.StatusOK)
			} else {
				writeOperationStatus(w, completed, http.StatusOK)
			}
		default:
			t.Errorf("unexpected operation request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	status, err := c.ExecuteOperation(context.Background(), request, Authority{OperationGrant: grant})
	if err != nil || !reflect.DeepEqual(status, completed) || submissions.Load() != 1 || polls.Load() != 2 {
		t.Fatalf("operation receipt/call count changed: %+v %v", status, err)
	}
}

func TestOperationLeaseCallbackDoesNotConsumeExecutionAdmission(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	lease := resolver.Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	until := time.Now().Add(5 * time.Second).Unix()
	var called atomic.Int32
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		var received resolver.Binding
		if r.URL.Path != "/v1/operations/operation-a/connection-lease" || r.Method != http.MethodPost || r.Header.Get("X-Kelvo-Operation-Grant") != grant || r.Header.Get("X-Kelvo-Delegation") != "" || json.NewDecoder(r.Body).Decode(&received) != nil || received != lease {
			t.Error("operation lease callback lost proof or endpoint binding")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{"valid_until": until})
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	_, release, err := c.operationContext(context.Background(), Authority{OperationGrant: grant})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	got, err := c.ValidateOperationLease(context.Background(), "operation-a", grant, lease)
	if err != nil || got.Unix() != until || called.Load() != 1 || len(c.permits) != 1 {
		t.Fatal("lease callback blocked on the execution it is resolving")
	}
}

func TestOperationLeaseRejectsInvalidProofAndMalformedResponse(t *testing.T) {
	grant := operationFixtureGrant(t, operationFixtureRequest())
	lease := resolver.Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	for _, name := range []string{"worker", "owner", "claim", "id", "grant", "duplicate", "expired", "unknown", "truncated", "oversize", "content-type"} {
		t.Run(name, func(t *testing.T) {
			var called atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				called.Add(1)
				w.Header().Set("Content-Type", "application/json")
				future := strconv.FormatInt(time.Now().Add(5*time.Second).Unix(), 10)
				body := `{"valid_until":` + future + `}`
				switch name {
				case "duplicate":
					body = `{"valid_until":` + future + `,"valid_until":` + future + `}`
				case "expired":
					body = `{"valid_until":1}`
				case "unknown":
					body = `{"valid_until":` + future + `,"credential":"secret"}`
				case "truncated":
					body = `{"valid_until":`
				case "oversize":
					body = strings.Repeat(" ", 1025)
				case "content-type":
					w.Header().Set("Content-Type", "text/plain")
				}
				_, _ = io.WriteString(w, body)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			proof, id, token := lease, "operation-a", grant
			invalidInput := true
			switch name {
			case "worker":
				proof.WorkerID = "../worker"
			case "owner":
				proof.Owner = "bad"
			case "claim":
				proof.Claim = "bad"
			case "id":
				id = "../escape"
			case "grant":
				token = "invalid"
			default:
				invalidInput = false
			}
			if _, err := c.ValidateOperationLease(context.Background(), id, token, proof); err == nil {
				t.Fatal("invalid lease accepted")
			}
			if invalidInput && called.Load() != 0 {
				t.Fatal("invalid lease proof reached network")
			}
		})
	}
}

func TestOperationLeaseClientShutdownCancelsInflightValidation(t *testing.T) {
	entered := make(chan struct{})
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	grant := operationFixtureGrant(t, operationFixtureRequest())
	done := make(chan error, 1)
	go func() {
		_, err := c.ValidateOperationLease(context.Background(), "operation-a", grant, resolver.Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("validation did not start")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("shutdown accepted a lease")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not cancel callback")
	}
}

func TestOperationCancellationAfterSubmissionRetainsKnownID(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	queued := operationFixtureStatus(t, request, "queued")
	polled := make(chan struct{}, 1)
	var submissions atomic.Int64
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			submissions.Add(1)
			writeOperationStatus(w, queued, http.StatusAccepted)
			return
		}
		polled <- struct{}{}
		<-r.Context().Done()
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.ExecuteOperation(ctx, request, Authority{OperationGrant: grant}); done <- err }()
	clientFixtureAwait(t, polled)
	cancel()
	err := clientFixtureAwait(t, done)
	uncertain := assertOperationUncertain(t, err, queued.ID, queued.RequestSHA256)
	if !errors.Is(err, context.Canceled) || uncertain.IdempotencyKey != request.IdempotencyKey || submissions.Load() != 1 {
		t.Fatal("cancellation lost reconciliation identity or replayed a mutation")
	}
}

func TestLostOperationSubmissionIsNotReplayed(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	queued := operationFixtureStatus(t, request, "queued")
	var submissions atomic.Int64
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		submissions.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		connection.Close()
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	_, err := c.SubmitOperation(context.Background(), request, Authority{OperationGrant: grant})
	uncertain := assertOperationUncertain(t, err, "", queued.RequestSHA256)
	if uncertain.IdempotencyKey != request.IdempotencyKey || submissions.Load() != 1 {
		t.Fatal("unknown submission was retried or lost its idempotency key")
	}
}

func TestOperationResponsesRejectAlteredAndAmbiguousReceipts(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	completed := operationFixtureStatus(t, request, string(operations.Completed))
	raw, _ := json.Marshal(completed)
	for name, mutate := range map[string]func([]byte) []byte{
		"request-digest": func(b []byte) []byte {
			return bytes.ReplaceAll(b, []byte(completed.RequestSHA256), []byte(strings.Repeat("0", 64)))
		},
		"receipt-id": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"operation_id":"operation-fixture-1"`), []byte(`"operation_id":"other-operation"`), 1)
		},
		"duplicate":     func(b []byte) []byte { return append([]byte(`{"id":"duplicate",`), b[1:]...) },
		"unknown-field": func(b []byte) []byte { return append([]byte(`{"diagnostic":"remote-secret-diagnostic",`), b[1:]...) },
		"trailing":      func(b []byte) []byte { return append(b, []byte(` {}`)...) },
		"oversized":     func(b []byte) []byte { return append(b, bytes.Repeat([]byte(" "), operationResponseLimit)...) },
	} {
		t.Run(name, func(t *testing.T) {
			server := clientFixtureServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write(mutate(append([]byte(nil), raw...)))
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			_, err := c.SubmitOperation(context.Background(), request, Authority{OperationGrant: grant})
			assertOperationUncertain(t, err, "", completed.RequestSHA256)
		})
	}
}

func TestOperationLostEnvelopeEOFStillRetainsValidatedHandle(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	queued := operationFixtureStatus(t, request, "queued")
	raw, _ := json.Marshal(queued)
	server := clientFixtureServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(raw)+10))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(raw)
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	_, err := c.SubmitOperation(context.Background(), request, Authority{OperationGrant: grant})
	assertOperationUncertain(t, err, queued.ID, queued.RequestSHA256)
}

func TestCancelOperationReturnsUnknownEffectWithoutRollbackClaim(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	status := operationFixtureStatus(t, request, "queued")
	status.State = string(operations.OutcomeUnknown)
	status.Receipt = &operations.Receipt{Version: operations.Version, OperationID: status.ID, RequestSHA256: status.RequestSHA256, Outcome: operations.OutcomeUnknown, Effect: operations.EffectUnknown, ErrorCode: "OUTCOME_UNKNOWN"}
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/operations/"+status.ID+"/cancel" || r.Header.Get("X-Kelvo-Operation-Grant") != grant {
			t.Error("cancel request changed identity or endpoint")
		}
		writeOperationStatus(w, status, http.StatusOK)
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	got, err := c.CancelOperation(context.Background(), status.ID, status.RequestSHA256, Authority{OperationGrant: grant})
	if err != nil || !reflect.DeepEqual(got, status) {
		t.Fatal("cancel acknowledgement was changed into a rollback or success")
	}
}

func operationResultFixture(t *testing.T, data []byte) (operations.Request, operations.Response) {
	t.Helper()
	request := operations.Request{Version: operations.Version, Kind: operations.ConnectionTest, Connection: operations.ConnectionRef{ID: "saved-a"}}
	status := operationFixtureStatus(t, request, string(operations.Completed))
	digest := sha256.Sum256(data)
	status.Receipt.Result = &operations.ResultRef{ID: "result-fixture-1", SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(data)), Rows: 3, Format: "arrow_ipc"}
	return request, status
}

func TestOperationResultsOnlyReadAndVerifyBeforeSuccessCallback(t *testing.T) {
	data := clientFixtureIPC(t)
	request, completed := operationResultFixture(t, data)
	grant := operationFixtureGrant(t, request)
	var gets, posts atomic.Int64
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			posts.Add(1)
		}
		gets.Add(1)
		if r.Header.Get("X-Kelvo-Operation-Grant") != grant {
			t.Error("result reader lost operation authority")
		}
		if strings.HasSuffix(r.URL.Path, "/results") {
			w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
			_, _ = w.Write(data)
		} else {
			writeOperationStatus(w, completed, http.StatusOK)
		}
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	result, err := c.OpenOperationResult(context.Background(), completed, Authority{OperationGrant: grant})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	markComplete := func() error { calls++; return nil }
	if err := result.Complete(markComplete); err == nil || calls != 0 {
		t.Fatal("result emitted success before EOF verification")
	}
	got, err := io.ReadAll(result)
	if err != nil || !bytes.Equal(got, data) || !result.Verified() || result.Close() != nil {
		t.Fatalf("immutable result verification failed: %v", err)
	}
	if err := result.Complete(markComplete); err != nil || calls != 1 {
		t.Fatal("verified result could not emit success")
	}
	if err := result.Complete(markComplete); err == nil || calls != 1 || posts.Load() != 0 || gets.Load() != 2 {
		t.Fatal("result completion repeated or triggered execution")
	}
}

func TestOperationResultCorruptionAndEarlyCloseNeverEmitSuccess(t *testing.T) {
	data := clientFixtureIPC(t)
	request, completed := operationResultFixture(t, data)
	grant := operationFixtureGrant(t, request)
	for _, mode := range []string{"hash", "truncated", "extra", "early-close"} {
		t.Run(mode, func(t *testing.T) {
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/results") {
					writeOperationStatus(w, completed, http.StatusOK)
					return
				}
				w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
				body := append([]byte(nil), data...)
				switch mode {
				case "hash":
					body[len(body)-1] ^= 1
				case "truncated":
					w.Header().Set("Content-Length", strconv.Itoa(len(body)))
					body = body[:len(body)-1]
				case "extra":
					w.(http.Flusher).Flush()
					body = append(body, 0)
				}
				_, _ = w.Write(body)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			result, err := c.OpenOperationResult(context.Background(), completed, Authority{OperationGrant: grant})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "early-close" {
				_, _ = result.Read(make([]byte, 1))
				err = result.Close()
			} else {
				_, err = io.ReadAll(result)
			}
			if err == nil || result.Verified() {
				t.Fatal("incomplete/corrupt result was verified")
			}
			called := false
			if err := result.Complete(func() error { called = true; return nil }); err == nil || called {
				t.Fatal("incomplete/corrupt result emitted success")
			}
			_ = result.Close()
		})
	}
}

func TestOperationResultsRequireExactFreshCompletedReceipt(t *testing.T) {
	data := clientFixtureIPC(t)
	request, completed := operationResultFixture(t, data)
	grant := operationFixtureGrant(t, request)
	for _, mode := range []string{"running", "altered-result", "wire-limit", "row-limit"} {
		t.Run(mode, func(t *testing.T) {
			fresh := operationFixtureStatus(t, request, "running")
			if mode != "running" {
				_, fresh = operationResultFixture(t, data)
				if mode == "altered-result" {
					fresh.Receipt.Result.ID = "other-result"
				}
			}
			var results atomic.Int64
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/results") {
					results.Add(1)
				}
				writeOperationStatus(w, fresh, http.StatusOK)
			}, tls.VersionTLS13)
			cfg := clientFixtureConfig(t, server)
			if mode == "wire-limit" {
				cfg.MaxWireBytes = 1
			}
			if mode == "row-limit" {
				cfg.MaxRows = 1
			}
			c := clientFixtureClient(t, cfg)
			if result, err := c.OpenOperationResult(context.Background(), completed, Authority{OperationGrant: grant}); err == nil || result != nil || results.Load() != 0 {
				t.Fatal("uncompleted, altered or over-limit receipt reached result endpoint")
			}
		})
	}
}

func TestClientCloseReleasesAbandonedOperationResult(t *testing.T) {
	data := clientFixtureIPC(t)
	request, completed := operationResultFixture(t, data)
	grant := operationFixtureGrant(t, request)
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/results") {
			writeOperationStatus(w, completed, http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	result, err := c.OpenOperationResult(context.Background(), completed, Authority{OperationGrant: grant})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	if err := clientFixtureAwait(t, done); err != nil {
		t.Fatal(err)
	}
	if result.Verified() || result.Close() == nil {
		t.Fatal("client shutdown claimed result completion")
	}
}

func TestOperationClientRejectsInvalidGrantAndNeverFollowsRedirect(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	var redirected, posted atomic.Int64
	destination := clientFixtureServer(t, func(http.ResponseWriter, *http.Request) { redirected.Add(1) }, tls.VersionTLS13)
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		posted.Add(1)
		w.Header().Set("Location", destination.URL+"/v1/operations")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	if _, err := c.SubmitOperation(context.Background(), request, Authority{OperationGrant: "invalid\r\nheader"}); err == nil || posted.Load() != 0 {
		t.Fatal("invalid grant reached the transport")
	}
	_, err := c.SubmitOperation(context.Background(), request, Authority{OperationGrant: grant})
	digest, _ := operations.Digest(request)
	assertOperationUncertain(t, err, "", digest)
	if redirected.Load() != 0 || posted.Load() != 1 {
		t.Fatal("operation redirect exposed authority or replayed submission")
	}
}
