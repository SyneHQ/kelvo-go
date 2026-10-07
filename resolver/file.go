// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package resolver

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// PublicationLock holds the application's source and authorization locks until
// Close. Authorization must describe the live records under those locks.
// Commit must atomically compare the current source with Publication.Original
// and publish the already verified candidate, returning false on a conflict.
// It must honor ctx through the atomic publication point. A successful return
// means durable publication, not merely enqueueing a future write.
//
// This interface deliberately requires storage-specific atomicity from the
// application; a lease check alone cannot supply a compare-and-swap or lock its
// membership, source revision, approval, ingestion or watcher records.
type PublicationLock interface {
	Authorization() Authorization
	Commit(context.Context, io.Reader) (bool, error)
	Close() error
}

func (h *Handler) fileRead(w http.ResponseWriter, r *http.Request) {
	if h.config.AuthorizeOperation == nil || h.config.OpenFile == nil {
		deny(w, http.StatusForbidden)
		return
	}
	var input FileReadRequest
	if readJSON(w, r, &input, MaxFileReadRequestBytes) != nil || input.Validate() != nil {
		deny(w, http.StatusBadRequest)
		return
	}
	claims, before, until, err := h.operationCheck(r, input.OperationRequest)
	if err != nil || before.Revision != input.SourceRevision {
		deny(w, http.StatusForbidden)
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), until)
	defer cancel()
	r = r.WithContext(ctx)
	file, err := h.config.OpenFile(ctx, clone(input), clone(claims), before)
	if file != nil {
		defer file.Close()
	}
	if err != nil || file == nil {
		deny(w, http.StatusForbidden)
		return
	}
	// OpenFile's reader must allow Close to interrupt a blocked Read. Retain
	// admission until that read actually returns and staging has been removed.
	stopFileClose := context.AfterFunc(ctx, func() { _ = file.Close() })
	defer stopFileClose()
	// Validate the full snapshot before exposing any bytes. A mistaken storage
	// lookup must not disclose a different source merely because the worker
	// would reject its digest after receiving it.
	verified, err := h.spoolVerified(ctx, file, input.Snapshot)
	if err != nil {
		deny(w, http.StatusForbidden)
		return
	}
	defer os.Remove(verified.Name())
	defer verified.Close()
	_, after, until, err := h.operationCheck(r, input.OperationRequest)
	if err != nil || after.Revision != input.SourceRevision || setDeadline(w, until, false) != nil {
		deny(w, http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set(FileSHA256Header, input.Snapshot.SHA256)
	w.Header().Set("Trailer", FileVerifiedTrailer+", "+SourceValidUntilTrailer)
	w.WriteHeader(http.StatusOK)
	// Flush before copying to preserve the required streaming/trailer response,
	// including snapshots short enough for net/http's automatic content length.
	if err := http.NewResponseController(w).Flush(); err != nil {
		return
	}
	hash := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(w, hash), io.LimitReader(contextReader{ctx, verified}, input.Snapshot.Bytes), make([]byte, 64<<10))
	var extra [1]byte
	extraN, extraErr := io.ReadFull(verified, extra[:])
	if err != nil || n != input.Snapshot.Bytes || extraN != 0 || extraErr != io.EOF || hex.EncodeToString(hash.Sum(nil)) != input.Snapshot.SHA256 || ctx.Err() != nil {
		return
	}
	_, final, until, err := h.operationCheck(r, input.OperationRequest)
	if err != nil || final.Revision != input.SourceRevision {
		return
	}
	w.Header().Set(FileVerifiedTrailer, input.Snapshot.SHA256)
	w.Header().Set(SourceValidUntilTrailer, strconv.FormatInt(until.Unix(), 10))
}

func (h *Handler) spoolVerified(ctx context.Context, source io.Reader, descriptor filesnapshot.Descriptor) (*os.File, error) {
	file, err := os.CreateTemp(h.config.TempDir, "kelvo-snapshot-*")
	if err != nil {
		return nil, ErrInvalid
	}
	bad := func() (*os.File, error) { _ = file.Close(); _ = os.Remove(file.Name()); return nil, ErrInvalid }
	hash := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(file, hash), io.LimitReader(contextReader{ctx, source}, descriptor.Bytes+1), make([]byte, 64<<10))
	if err != nil || n != descriptor.Bytes || hex.EncodeToString(hash.Sum(nil)) != descriptor.SHA256 || ctx.Err() != nil {
		return bad()
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return bad()
	}
	return file, nil
}

func (h *Handler) fileCommit(w http.ResponseWriter, r *http.Request) {
	if h.config.AuthorizeOperation == nil || h.config.LockPublication == nil {
		deny(w, http.StatusForbidden)
		return
	}
	body := http.MaxBytesReader(w, r.Body, filesnapshot.MaxBytes+MaxOperationRequestBytes+4)
	var size [4]byte
	if _, err := io.ReadFull(body, size[:]); err != nil {
		deny(w, http.StatusBadRequest)
		return
	}
	n := binary.BigEndian.Uint32(size[:])
	if n == 0 || n > MaxOperationRequestBytes {
		deny(w, http.StatusBadRequest)
		return
	}
	raw := make([]byte, n)
	defer clear(raw)
	if _, err := io.ReadFull(body, raw); err != nil {
		deny(w, http.StatusBadRequest)
		return
	}
	var input FileCommitRequest
	if operations.DecodeStrict(raw, &input, MaxOperationRequestBytes) != nil || input.Validate() != nil {
		deny(w, http.StatusBadRequest)
		return
	}
	claims, before, until, err := h.operationCheck(r, input.OperationRequest)
	if err != nil || before.Revision != input.SourceRevision {
		deny(w, http.StatusForbidden)
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), until)
	defer cancel()
	r = r.WithContext(ctx)
	if setDeadline(w, until, true) != nil {
		deny(w, http.StatusServiceUnavailable)
		return
	}
	// Spool only a bounded authenticated upload into a private file. No
	// application publication hook sees bytes until length and digest match.
	candidate, err := os.CreateTemp(h.config.TempDir, "kelvo-publication-*")
	if err != nil {
		deny(w, http.StatusServiceUnavailable)
		return
	}
	defer os.Remove(candidate.Name())
	defer candidate.Close()
	hash := sha256.New()
	count, err := io.CopyBuffer(io.MultiWriter(candidate, hash), io.LimitReader(contextReader{ctx, body}, input.Publication.Replacement.Bytes+1), make([]byte, 64<<10))
	if err != nil || count != input.Publication.Replacement.Bytes || hex.EncodeToString(hash.Sum(nil)) != input.Publication.Replacement.SHA256 || ctx.Err() != nil {
		deny(w, http.StatusBadRequest)
		return
	}
	if _, err = candidate.Seek(0, io.SeekStart); err != nil {
		deny(w, http.StatusServiceUnavailable)
		return
	}
	_, current, _, err := h.operationCheck(r, input.OperationRequest)
	if err != nil || current.Revision != input.SourceRevision {
		deny(w, http.StatusForbidden)
		return
	}
	locked, err := h.config.LockPublication(ctx, clone(input), clone(claims))
	if locked != nil {
		defer locked.Close()
	}
	if err != nil || locked == nil {
		deny(w, http.StatusForbidden)
		return
	}
	a := locked.Authorization()
	if !validAuthorization(a) || a.Revision != input.SourceRevision {
		deny(w, http.StatusForbidden)
		return
	}
	// The application now holds revocation/source locks. Recheck the signed
	// request, worker certificate and live Kelvo custody inside those locks.
	claims, err = h.verifyOperation(input.OperationRequest)
	if err != nil {
		deny(w, http.StatusForbidden)
		return
	}
	peer, err := WorkerCertificateExpiry(r.TLS, claims.ClusterTenant, input.WorkerID, time.Now())
	if err != nil {
		deny(w, http.StatusForbidden)
		return
	}
	lease, err := h.config.Leases.ValidateOperationLease(ctx, input.OperationID, input.Grant, input.Binding())
	if err != nil {
		deny(w, http.StatusForbidden)
		return
	}
	until, err = boundUntil(ctx, claims.ExpiresAt, lease, peer, a)
	if err != nil {
		deny(w, http.StatusForbidden)
		return
	}
	commitCtx, commitCancel := context.WithDeadline(ctx, until)
	defer commitCancel()
	committed, err := locked.Commit(commitCtx, candidate)
	if err != nil || commitCtx.Err() != nil {
		deny(w, http.StatusServiceUnavailable)
		return
	}
	status := http.StatusConflict
	if committed {
		status = http.StatusOK
	}
	receipt := filesnapshot.PublicationReceipt{Version: filesnapshot.Version, OperationID: input.OperationID, RequestSHA256: claims.RequestSHA256, ReplacementSHA256: input.Publication.Replacement.SHA256, Committed: committed}
	writeJSON(w, r, status, receipt, until)
}

type contextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.source.Read(p)
	if cancelled := r.ctx.Err(); cancelled != nil {
		return n, cancelled
	}
	return n, err
}
