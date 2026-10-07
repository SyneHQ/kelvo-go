// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package resolver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func snapshotFor(raw []byte) filesnapshot.Descriptor {
	sum := sha256.Sum256(raw)
	return filesnapshot.Descriptor{Version: filesnapshot.Version, Format: "sqlite", Bytes: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])}
}

func TestFileReadRequiresExactBytesAndFinalAuthority(t *testing.T) {
	for _, scenario := range []string{"valid", "wrong bytes", "extra bytes", "truncated bytes", "changed revision", "revoked after delivery", "missing callback"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			f.config.TempDir = t.TempDir()
			payload := []byte("fixture source snapshot")
			input := FileReadRequest{OperationRequest: f.operation, SourceRevision: fixtureAuthorization().Revision, Snapshot: snapshotFor(payload)}
			auths := 0
			f.config.AuthorizeOperation = func(context.Context, OperationRequest, operations.GrantClaims) (Authorization, error) {
				auths++
				a := fixtureAuthorization()
				if scenario == "changed revision" && auths > 1 {
					a.Revision = strings.Repeat("e", 64)
				}
				if scenario == "revoked after delivery" && auths >= 3 {
					return a, ErrInvalid
				}
				return a, nil
			}
			f.config.OpenFile = func(context.Context, FileReadRequest, operations.GrantClaims, Authorization) (io.ReadCloser, error) {
				data := bytes.Clone(payload)
				switch scenario {
				case "wrong bytes":
					data[0] = 'X'
				case "extra bytes":
					data = append(data, 'X')
				case "truncated bytes":
					data = data[:len(data)-1]
				}
				return io.NopCloser(bytes.NewReader(data)), nil
			}
			if scenario == "missing callback" {
				f.config.OpenFile = nil
			}
			w := serveFixture(t, f.config, f.request(t, FileReadPath, input))
			response := w.Result()
			defer response.Body.Close()
			body, _ := io.ReadAll(response.Body)
			if scenario == "valid" {
				if w.Code != http.StatusOK || !bytes.Equal(body, payload) || response.Trailer.Get(FileVerifiedTrailer) != input.Snapshot.SHA256 || response.Trailer.Get(SourceValidUntilTrailer) == "" {
					t.Fatal("valid file delivery lost proof")
				}
			} else if scenario == "revoked after delivery" {
				if response.Trailer.Get(FileVerifiedTrailer) != "" || response.Trailer.Get(SourceValidUntilTrailer) != "" {
					t.Fatal("revoked file delivery acquired valid receipt")
				}
			} else if w.Code < 400 || bytes.Contains(body, []byte("fixture source")) {
				t.Fatal("unverified file bytes escaped")
			}
			entries, err := os.ReadDir(f.config.TempDir)
			if err != nil || len(entries) != 0 {
				t.Fatal("snapshot staging file was retained")
			}
		})
	}
}

type publicationFixture struct {
	authorization Authorization
	commit        func(context.Context, io.Reader) (bool, error)
	closed        bool
}

func (p *publicationFixture) Authorization() Authorization { return p.authorization }
func (p *publicationFixture) Commit(ctx context.Context, r io.Reader) (bool, error) {
	return p.commit(ctx, r)
}
func (p *publicationFixture) Close() error { p.closed = true; return nil }

func commitFixture(t *testing.T) (*handlerFixture, FileCommitRequest, []byte) {
	t.Helper()
	f := newFixture(t)
	f.operation.Operation.Kind = operations.StatementExecute
	f.operation.Operation.Spec = operations.Spec{Statement: &operations.StatementSpec{SQL: "UPDATE records SET value = 1", Transaction: operations.TransactionRequired}}
	f.signOperation(t)
	original := snapshotFor([]byte("original fixture"))
	replacement := []byte("replacement fixture")
	digest, _ := operations.Digest(f.operation.Operation)
	input := FileCommitRequest{OperationRequest: f.operation, SourceRevision: fixtureAuthorization().Revision, Snapshot: original, Publication: filesnapshot.Publication{Version: filesnapshot.Version, OperationID: f.operation.OperationID, RequestSHA256: digest, Original: original, Replacement: snapshotFor(replacement)}}
	return f, input, replacement
}

func commitHTTP(t *testing.T, f *handlerFixture, input FileCommitRequest, payload []byte) *http.Request {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(raw)))
	body := append(append(bytes.Clone(length[:]), raw...), payload...)
	r := httptest.NewRequest(http.MethodPost, FileCommitPath, bytes.NewReader(body))
	r.TLS = f.tls
	r.Header.Set("Content-Type", FileCommitMediaType)
	return r
}

func TestFilePublicationChecksBytesAuthorizationAndCustodyBeforeCommit(t *testing.T) {
	for _, scenario := range []string{"valid", "conflict", "wrong bytes", "extra bytes", "truncated bytes", "wrong snapshot", "wrong request", "revoked while staging", "changed locked revision", "revoked locked lease", "missing lock"} {
		t.Run(scenario, func(t *testing.T) {
			f, input, payload := commitFixture(t)
			f.config.TempDir = t.TempDir()
			expected := bytes.Clone(payload)
			commits, locks, leases, auths := 0, 0, 0, 0
			locked := &publicationFixture{authorization: fixtureAuthorization()}
			locked.commit = func(_ context.Context, r io.Reader) (bool, error) {
				commits++
				data, err := io.ReadAll(r)
				if err != nil || !bytes.Equal(data, expected) {
					t.Fatal("publication did not receive verified bytes")
				}
				return scenario != "conflict", nil
			}
			f.config.LockPublication = func(context.Context, FileCommitRequest, operations.GrantClaims) (PublicationLock, error) {
				locks++
				if scenario == "changed locked revision" {
					locked.authorization.Revision = strings.Repeat("e", 64)
				}
				return locked, nil
			}
			f.config.AuthorizeOperation = func(context.Context, OperationRequest, operations.GrantClaims) (Authorization, error) {
				auths++
				if scenario == "revoked while staging" && auths > 1 {
					return Authorization{}, ErrInvalid
				}
				return fixtureAuthorization(), nil
			}
			l := f.config.Leases.(leaseFixture)
			l.operation = func(context.Context, string, string, Binding) (time.Time, error) {
				leases++
				if scenario == "revoked locked lease" && leases >= 3 {
					return time.Time{}, ErrInvalid
				}
				return time.Now().Add(5 * time.Second), nil
			}
			f.config.Leases = l
			switch scenario {
			case "wrong bytes":
				payload[0] = 'X'
			case "extra bytes":
				payload = append(payload, 'X')
			case "truncated bytes":
				payload = payload[:len(payload)-1]
			case "wrong snapshot":
				input.Snapshot.SHA256 = strings.Repeat("f", 64)
			case "wrong request":
				input.Publication.RequestSHA256 = strings.Repeat("f", 64)
			case "missing lock":
				f.config.LockPublication = nil
			}
			w := serveFixture(t, f.config, commitHTTP(t, f, input, payload))
			if scenario == "valid" || scenario == "conflict" {
				want := http.StatusOK
				if scenario == "conflict" {
					want = http.StatusConflict
				}
				var receipt filesnapshot.PublicationReceipt
				if w.Code != want || commits != 1 || operations.DecodeStrict(w.Body.Bytes(), &receipt, 8192) != nil || !receipt.Matches(input.Publication) || receipt.Committed != (scenario == "valid") {
					t.Fatalf("invalid publication receipt: status %d", w.Code)
				}
			} else if w.Code < 400 || commits != 0 {
				t.Fatal("invalid publication committed")
			}
			if locks > 0 && !locked.closed {
				t.Fatal("publication authority lock was not released")
			}
			entries, err := os.ReadDir(f.config.TempDir)
			if err != nil || len(entries) != 0 {
				t.Fatal("publication staging file was retained")
			}
		})
	}
}

func TestFilePublicationRejectsOversizedPrefixBeforeReadingPayload(t *testing.T) {
	f, _, _ := commitFixture(t)
	f.config.LockPublication = func(context.Context, FileCommitRequest, operations.GrantClaims) (PublicationLock, error) {
		t.Fatal("invalid prefix reached application")
		return nil, ErrInvalid
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], MaxOperationRequestBytes+1)
	r := httptest.NewRequest(http.MethodPost, FileCommitPath, bytes.NewReader(prefix[:]))
	r.TLS = f.tls
	r.Header.Set("Content-Type", FileCommitMediaType)
	if w := serveFixture(t, f.config, r); w.Code != http.StatusBadRequest {
		t.Fatal("oversized publication prefix accepted")
	}
}

type blockingReadCloser struct {
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func (r *blockingReadCloser) Read([]byte) (int, error) {
	r.startOnce.Do(func() { close(r.started) })
	<-r.closed
	return 0, io.ErrClosedPipe
}
func (r *blockingReadCloser) Close() error { r.closeOnce.Do(func() { close(r.closed) }); return nil }

func TestCancellationClosesBlockedBodyAndSnapshotReaders(t *testing.T) {
	for _, scenario := range []string{"request body", "snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			f.config.TempDir = t.TempDir()
			blocked := &blockingReadCloser{started: make(chan struct{}), closed: make(chan struct{})}
			defer blocked.Close()
			var r *http.Request
			if scenario == "request body" {
				r = f.request(t, QueryPath, f.query)
				r.Body = blocked
				r.ContentLength = -1
			} else {
				input := FileReadRequest{OperationRequest: f.operation, SourceRevision: fixtureAuthorization().Revision, Snapshot: snapshotFor([]byte("snapshot"))}
				f.config.OpenFile = func(context.Context, FileReadRequest, operations.GrantClaims, Authorization) (io.ReadCloser, error) {
					return blocked, nil
				}
				r = f.request(t, FileReadPath, input)
			}
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(ctx)
			h, err := NewHandler(f.config)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { h.ServeHTTP(w, r); close(done) }()
			select {
			case <-blocked.started:
			case <-time.After(time.Second):
				t.Fatal("reader was not started")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("cancelled reader pinned handler")
			}
			if w.Code < 400 || len(h.slots) != 0 {
				t.Fatal("cancelled read succeeded or retained admission")
			}
			entries, err := os.ReadDir(f.config.TempDir)
			if err != nil || len(entries) != 0 {
				t.Fatal("cancelled snapshot retained staging file")
			}
		})
	}
}
