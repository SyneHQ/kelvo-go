//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type operationAdmissionBackend struct {
	*operationMemoryBackend
	afterWrite func() error
}

func (b *operationAdmissionBackend) Create(ctx context.Context, key string, data []byte) (uint64, error) {
	revision, err := b.operationMemoryBackend.Create(ctx, key, data)
	if err == nil && b.afterWrite != nil {
		err = b.afterWrite()
	}
	return revision, err
}

func (b *operationAdmissionBackend) Update(ctx context.Context, key string, data []byte, expected uint64) (uint64, error) {
	revision, err := b.operationMemoryBackend.Update(ctx, key, data, expected)
	if err == nil && b.afterWrite != nil {
		err = b.afterWrite()
	}
	return revision, err
}

func operationAdmissionStore(t *testing.T, f *operationHTTPFixture, backend ledger.Backend) {
	t.Helper()
	policy := operationStorePolicy(f.policy)
	policy.Shards, policy.SlotsPerShard = 1, 1
	store, err := ledger.New(backend, policy)
	if err != nil {
		t.Fatal(err)
	}
	f.state.store = store
}

func operationAdmissionInputs(t *testing.T, f *operationHTTPFixture, maximum int) {
	t.Helper()
	inputs, err := operationinput.Open(operationinput.Config{MaxInputBytes: operations.MaxRequestBytes,
		Storage: exports.Config{Directory: filepath.Join(t.TempDir(), "limited-inputs"), Tenant: "team-a", MaxEntries: maximum, MaxStoredBytes: 128 << 20, MaxTTL: 6 * time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inputs.Close() })
	f.state.inputs = inputs
}

func TestOperationHTTPFullLedgerReturnsBoundAdmissionRejection(t *testing.T) {
	f := newOperationHTTPFixture(t)
	backend := &operationMemoryBackend{values: map[string]ledger.Entry{}}
	operationAdmissionStore(t, &f, backend)
	operationAdmissionInputs(t, &f, 2)
	first := f.submit(t)
	f.request.IdempotencyKey = "write-2"
	digest, err := operations.Digest(f.request)
	if err != nil {
		t.Fatal(err)
	}
	f.claims.RequestSHA256, f.claims.ID = digest, "grant-2"
	f.grant, err = operations.SignGrant(f.claims, f.key)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := operations.Encode(f.request)
	w := f.call(http.MethodPost, "/v1/operations", body, f.grant)
	var rejection operations.AdmissionRejection
	if w.Code != http.StatusTooManyRequests || operations.DecodeStrict(w.Body.Bytes(), &rejection, operations.MaxAdmissionRejectionBytes) != nil ||
		rejection.ValidateBinding(digest, operations.GrantDigest(f.grant)) != nil || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("ledger admission rejection was not bound: %d %s", w.Code, w.Body.String())
	}
	prior, err := f.state.store.Get(f.context, operationScope(f.claims), first.ID)
	if err != nil || prior.Record.State != ledger.Queued || prior.Record.RequestSHA256 != first.RequestSHA256 {
		t.Fatal("capacity rejection changed existing operation custody")
	}
	priorIdentity := operationInputIdentity(prior.Record.Scope, prior.Record.AuthoritySHA256)
	if _, err := f.state.inputs.Load(f.context, priorIdentity, prior.Record.RequestRef); err != nil {
		t.Fatal("capacity cleanup removed the accepted operation input")
	}
	// Only two input slots exist. A new reservation proves the rejected fresh
	// input was reclaimed; the admitted original is still sealed and readable.
	identity := operationInputIdentity(operationScope(f.claims), operations.GrantDigest(f.grant))
	if _, err := f.state.inputs.PutRequest(f.context, identity, time.Unix(f.claims.ExpiresAt, 0), f.request); err != nil {
		t.Fatalf("rejected input still occupies its reservation: %v", err)
	}
}

func TestOperationHTTPInputCapacityRejectsOnlyCurrentSubmission(t *testing.T) {
	f := newOperationHTTPFixture(t)
	operationAdmissionInputs(t, &f, 1)
	first := f.submit(t)
	body, _ := operations.Encode(f.request)
	w := f.call(http.MethodPost, "/v1/operations", body, f.grant)
	var rejection operations.AdmissionRejection
	if w.Code != http.StatusTooManyRequests || operations.DecodeStrict(w.Body.Bytes(), &rejection, operations.MaxAdmissionRejectionBytes) != nil ||
		rejection.ValidateBinding(first.RequestSHA256, operations.GrantDigest(f.grant)) != nil {
		t.Fatalf("input capacity rejection was not bound: %d %s", w.Code, w.Body.String())
	}
	// A duplicate can reach the input cap before ledger lookup. Rejection does
	// not erase or deny the accepted original, which remains reconcilable.
	prior, err := f.state.store.Get(f.context, operationScope(f.claims), first.ID)
	if err != nil || prior.Record.State != ledger.Queued {
		t.Fatal("input rejection lost accepted duplicate identity")
	}
	if _, err := f.state.inputs.Load(f.context, operationInputIdentity(prior.Record.Scope, prior.Record.AuthoritySHA256), prior.Record.RequestRef); err != nil {
		t.Fatal("input rejection removed accepted duplicate input")
	}
}

func TestOperationHTTPAdmittedFailuresNeverClaimRejection(t *testing.T) {
	for _, mode := range []string{"lost-storage-ack", "audit-completion"} {
		t.Run(mode, func(t *testing.T) {
			f := newOperationHTTPFixture(t)
			backend := &operationAdmissionBackend{operationMemoryBackend: &operationMemoryBackend{values: map[string]ledger.Entry{}}}
			operationAdmissionStore(t, &f, backend)
			writes := 0
			backend.afterWrite = func() error {
				writes++
				if mode == "lost-storage-ack" {
					return errors.New("acknowledgement lost after commit")
				}
				corruptAudit(t, f.g.audit)
				return nil
			}
			if mode == "audit-completion" {
				serviceAudit, err := OpenServiceAudit(serviceAuditConfig(t), "gateway", []string{"team-a"})
				if err != nil {
					t.Fatal(err)
				}
				f.g.audit = serviceAudit
				t.Cleanup(func() { _ = serviceAudit.CloseBounded() })
			}
			body, _ := operations.Encode(f.request)
			w := f.call(http.MethodPost, "/v1/operations", body, f.grant)
			if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "not_admitted") || writes != 1 {
				t.Fatalf("admitted failure claimed rejection or replayed: %d %s, writes=%d", w.Code, w.Body.String(), writes)
			}
			retained := 0
			for _, entry := range backend.values {
				var document struct {
					Records []ledger.Record `json:"records"`
				}
				if json.Unmarshal(entry.Value, &document) != nil {
					t.Fatal("could not inspect committed operation")
				}
				for _, record := range document.Records {
					if record.State != ledger.Queued || record.RequestSHA256 != f.claims.RequestSHA256 {
						t.Fatal("admitted operation lost custody after response failure")
					}
					if _, err := f.state.inputs.Load(f.context, operationInputIdentity(record.Scope, record.AuthoritySHA256), record.RequestRef); err != nil {
						t.Fatal("admitted failure discarded its sealed operation input")
					}
					retained++
				}
			}
			if retained != 1 {
				t.Fatal("failure did not retain exactly one admitted operation")
			}
		})
	}
}
