// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func operationRejectionFixture(t *testing.T, request operations.Request, grant string) operations.AdmissionRejection {
	t.Helper()
	digest, err := operations.Digest(request)
	if err != nil {
		t.Fatal(err)
	}
	return operations.AdmissionRejection{Version: 1, Admission: "not_admitted", Code: "RESOURCE_EXHAUSTED",
		RequestSHA256: digest, GrantSHA256: operations.GrantDigest(grant)}
}

func TestOperationAdmissionRejectionIsBoundAndNeverReplayed(t *testing.T) {
	for _, execute := range []bool{false, true} {
		t.Run(strconv.FormatBool(execute), func(t *testing.T) {
			request := operationFixtureRequest()
			grant := operationFixtureGrant(t, request)
			rejection := operationRejectionFixture(t, request, grant)
			var calls atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPost || r.URL.Path != "/v1/operations" {
					t.Error("rejected operation was polled or sent to another endpoint")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(rejection)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			call := c.SubmitOperation
			if execute {
				call = c.ExecuteOperation
			}
			status, err := call(context.Background(), request, Authority{OperationGrant: grant})
			var rejected *OperationRejectedError
			var uncertain *OperationUncertainError
			if !errors.As(err, &rejected) || errors.As(err, &uncertain) || rejected.Code != "RESOURCE_EXHAUSTED" ||
				rejected.RequestSHA256 != rejection.RequestSHA256 || rejected.IdempotencyKey != request.IdempotencyKey || status.ID != "" || calls.Load() != 1 {
				t.Fatalf("definite rejection lost its binding or retried: %+v %v, calls=%d", status, err, calls.Load())
			}
		})
	}
}

func TestOperationAdmissionAmbiguousRejectionStaysUncertain(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	rejection := operationRejectionFixture(t, request, grant)
	raw, _ := json.Marshal(rejection)
	for _, name := range []string{"plain", "ordinary-error", "request-digest", "grant-digest", "version", "marker", "code", "duplicate", "unknown", "trailing", "oversized", "content-type", "duplicate-content-type", "encoding", "lost-eof", "status"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				body := bytes.Clone(raw)
				code := http.StatusTooManyRequests
				w.Header().Set("Content-Type", "application/json")
				switch name {
				case "plain":
					body = []byte("remote-secret-diagnostic")
				case "ordinary-error":
					body = []byte(`{"code":"RESOURCE_EXHAUSTED","message":"remote-secret-diagnostic"}`)
				case "request-digest":
					body = bytes.Replace(body, []byte(rejection.RequestSHA256), []byte(strings.Repeat("0", 64)), 1)
				case "grant-digest":
					body = bytes.Replace(body, []byte(rejection.GrantSHA256), []byte(strings.Repeat("0", 64)), 1)
				case "version":
					body = bytes.Replace(body, []byte(`"version":1`), []byte(`"version":2`), 1)
				case "marker":
					body = bytes.Replace(body, []byte(`"not_admitted"`), []byte(`"queued"`), 1)
				case "code":
					body = bytes.Replace(body, []byte(`"RESOURCE_EXHAUSTED"`), []byte(`"UNAVAILABLE"`), 1)
				case "duplicate":
					body = append([]byte(`{"admission":"not_admitted",`), body[1:]...)
				case "unknown":
					body = append([]byte(`{"diagnostic":"remote-secret-diagnostic",`), body[1:]...)
				case "trailing":
					body = append(body, []byte(` {}`)...)
				case "oversized":
					body = append(body, bytes.Repeat([]byte(" "), operations.MaxAdmissionRejectionBytes)...)
				case "content-type":
					w.Header().Set("Content-Type", "text/plain")
				case "duplicate-content-type":
					w.Header().Add("Content-Type", "application/json")
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				case "lost-eof":
					w.Header().Set("Content-Length", strconv.Itoa(len(body)+10))
				case "status":
					code = http.StatusServiceUnavailable
				}
				w.WriteHeader(code)
				_, _ = w.Write(body)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			_, err := c.SubmitOperation(context.Background(), request, Authority{OperationGrant: grant})
			uncertain := assertOperationUncertain(t, err, "", rejection.RequestSHA256)
			var rejected *OperationRejectedError
			if errors.As(err, &rejected) || uncertain.IdempotencyKey != request.IdempotencyKey || calls.Load() != 1 {
				t.Fatal("ambiguous rejection was treated as definite or replayed")
			}
		})
	}
}

func TestOperationRejectionAfterAdmissionRemainsUncertain(t *testing.T) {
	for _, action := range []string{"execute", "poll", "cancel"} {
		t.Run(action, func(t *testing.T) {
			request := operationFixtureRequest()
			grant := operationFixtureGrant(t, request)
			queued := operationFixtureStatus(t, request, "queued")
			rejection := operationRejectionFixture(t, request, grant)
			var submissions, controls atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/operations" {
					submissions.Add(1)
					writeOperationStatus(w, queued, http.StatusAccepted)
					return
				}
				controls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(rejection)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			var err error
			switch action {
			case "execute":
				_, err = c.ExecuteOperation(context.Background(), request, Authority{OperationGrant: grant})
			case "poll":
				_, err = c.PollOperation(context.Background(), queued.ID, queued.RequestSHA256, Authority{OperationGrant: grant})
			case "cancel":
				_, err = c.CancelOperation(context.Background(), queued.ID, queued.RequestSHA256, Authority{OperationGrant: grant})
			}
			assertOperationUncertain(t, err, queued.ID, queued.RequestSHA256)
			var rejected *OperationRejectedError
			if errors.As(err, &rejected) || controls.Load() != 1 || action == "execute" && submissions.Load() != 1 || action != "execute" && submissions.Load() != 0 {
				t.Fatal("post-admission failure was treated as safe to replay")
			}
		})
	}
}
