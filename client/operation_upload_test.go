// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func inputUploadFixture(t *testing.T, format string, raw []byte) (string, operations.InputRef) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	ref := operations.InputRef{ID: "sealed-fixture", Format: format, Bytes: int64(len(raw)), SHA256: hex.EncodeToString(hash[:])}
	now := time.Now()
	grant, err := operations.SignInputUploadGrant(operations.InputUploadClaims{Version: operations.InputUploadVersion,
		Issuer: "gateway-fixture", Audience: "kelvo", ClusterTenant: "team-a", ServicePrincipal: "gateway", AppTeam: "app-team-a",
		Subject: operations.Subject{Kind: "user", ID: "user-a"}, ID: "input-fixture", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(),
		ConnectionID: "saved-a", Format: format, SHA256: ref.SHA256, Bytes: ref.Bytes}, key)
	if err != nil {
		t.Fatal(err)
	}
	return grant, ref
}

func TestInputUploadBindsExactBytesAndGrantFormat(t *testing.T) {
	for _, format := range []string{"ingestion_batch_v1", "watch_checkpoint_v1"} {
		t.Run(format, func(t *testing.T) {
			raw := []byte(`{"records":[{"id":"one","payload":{"n":9007199254740993}}]}`)
			grant, ref := inputUploadFixture(t, format, raw)
			var uploads atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				uploads.Add(1)
				body, err := io.ReadAll(r.Body)
				if err != nil || string(body) != string(raw) || r.ContentLength != int64(len(raw)) || r.Method != "POST" || r.URL.Path != "/v1/operation-inputs" || r.Header.Get("Content-Type") != "application/octet-stream" || r.Header.Get("Authorization") != "Bearer "+clientFixtureToken || r.Header.Get("X-Kelvo-Operation-Input-Grant") != grant || r.Header.Get("X-Kelvo-Operation-Grant") != "" || r.Header.Get("X-Kelvo-Delegation") != "" || r.Header.Get("Accept-Encoding") != "identity" || r.ProtoMajor != 1 || r.TLS.Version != tls.VersionTLS13 {
					t.Error("sealed input lost exact bytes or restricted upload transport")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(ref)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			got, err := c.UploadOperationInput(context.Background(), Authority{InputGrant: grant}, raw)
			if err != nil || got != ref || uploads.Load() != 1 {
				t.Fatalf("input binding changed: %+v %v", got, err)
			}
		})
	}
}

func TestInputUploadRejectsUnboundRequestsBeforeNetwork(t *testing.T) {
	raw := []byte(`{"sealed":true}`)
	valid, _ := inputUploadFixture(t, "ingestion_batch_v1", raw)
	for _, name := range []string{"empty", "oversize", "body", "grant", "operation-grant", "format", "duplicate-format", "version", "header"} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, _ *http.Request) { requests.Add(1); w.WriteHeader(500) }, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			body, grant := raw, valid
			switch name {
			case "empty":
				body = nil
			case "oversize":
				body = make([]byte, operations.MaxSealedInputBytes+1)
			case "body":
				body = []byte(`{"sealed":false}`)
			case "grant":
				grant = "invalid"
			case "operation-grant":
				grant = operationFixtureGrant(t, operationFixtureRequest())
			default:
				parts := strings.Split(grant, ".")
				index := 1
				if name == "header" {
					index = 0
				}
				decoded, _ := base64.RawURLEncoding.DecodeString(parts[index])
				s := string(decoded)
				switch name {
				case "format":
					s = strings.Replace(s, "ingestion_batch_v1", "native_input_v1", 1)
				case "duplicate-format":
					s = strings.TrimSuffix(s, "}") + `,"format":"ingestion_batch_v1"}`
				case "version":
					s = strings.Replace(s, `"version":1`, `"version":2`, 1)
				case "header":
					s = strings.Replace(s, "kelvo-operation-input+jwt", "kelvo-operation+jwt", 1)
				}
				parts[index] = base64.RawURLEncoding.EncodeToString([]byte(s))
				grant = strings.Join(parts, ".")
			}
			if _, err := c.UploadOperationInput(context.Background(), Authority{InputGrant: grant}, body); err == nil || requests.Load() != 0 {
				t.Fatal("unbound upload reached network")
			}
		})
	}
}

func TestInputUploadRejectsChangedAndMalformedReferences(t *testing.T) {
	raw := []byte(`{"sealed":true}`)
	grant, valid := inputUploadFixture(t, "ingestion_batch_v1", raw)
	for _, name := range []string{"hash", "bytes", "format", "id", "duplicate", "unknown", "truncated", "oversize", "content-type", "encoding", "status", "redirect"} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				ref := valid
				switch name {
				case "hash":
					ref.SHA256 = strings.Repeat("a", 64)
				case "bytes":
					ref.Bytes++
				case "format":
					ref.Format = "watch_checkpoint_v1"
				case "id":
					ref.ID = "../escape"
				}
				body, _ := json.Marshal(ref)
				w.Header().Set("Content-Type", "application/json")
				status := http.StatusCreated
				switch name {
				case "duplicate":
					body = []byte(strings.TrimSuffix(string(body), "}") + `,"id":"second"}`)
				case "unknown":
					body = []byte(strings.TrimSuffix(string(body), "}") + `,"secret":"remote-secret-diagnostic"}`)
				case "truncated":
					body = body[:len(body)-1]
				case "oversize":
					body = []byte(strings.Repeat(" ", 4097))
				case "content-type":
					w.Header().Set("Content-Type", "text/plain")
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				case "status":
					status = http.StatusOK
				case "redirect":
					w.Header().Set("Location", "/should-not-follow")
					status = http.StatusTemporaryRedirect
				}
				w.WriteHeader(status)
				_, _ = w.Write(body)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			_, err := c.UploadOperationInput(context.Background(), Authority{InputGrant: grant}, raw)
			if err == nil || requests.Load() != 1 || strings.Contains(err.Error(), "remote-secret-diagnostic") {
				t.Fatalf("untrusted input reference accepted or replayed: %v", err)
			}
		})
	}
}

func TestInputUploadShutdownCancelsTransfer(t *testing.T) {
	raw := []byte(`{"sealed":true}`)
	grant, _ := inputUploadFixture(t, "ingestion_batch_v1", raw)
	entered := make(chan struct{}, 1)
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		<-r.Context().Done()
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	done := make(chan error, 1)
	go func() {
		_, err := c.UploadOperationInput(context.Background(), Authority{InputGrant: grant}, raw)
		done <- err
	}()
	clientFixtureAwait(t, entered)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := clientFixtureAwait(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("upload ignored shutdown: %v", err)
	}
}
