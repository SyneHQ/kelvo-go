// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// ServeHTTP returns false for unrelated Node routes. Internal calls require
// authenticated gateway mTLS even when a parent handler already checked it.
func (r *ExportRuntime) ServeHTTP(w http.ResponseWriter, req *http.Request) bool {
	if r == nil || !strings.HasPrefix(req.URL.Path, "/internal/exports/") {
		return false
	}
	if req.TLS == nil || len(req.TLS.VerifiedChains) == 0 || len(req.TLS.PeerCertificates) == 0 || req.TLS.PeerCertificates[0] == nil || !hasURI(req.TLS.PeerCertificates[0], GatewayIdentity) {
		r.audit.authenticationDenied()
		http.Error(w, "mutual TLS gateway identity required", http.StatusUnauthorized)
		return true
	}
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	if req.URL.RawQuery != "" || (len(parts) != 4 && len(parts) != 5) || r.ctx.Err() != nil {
		http.NotFound(w, req)
		return true
	}
	w.Header().Set("Cache-Control", "no-store")
	id := parts[2]
	if len(parts) == 4 && parts[3] == "execute" && req.Method == http.MethodPost {
		values := req.Header.Values("X-Kelvo-Claim")
		if len(values) != 1 || len(values[0]) != 32 {
			http.Error(w, "invalid export claim", http.StatusConflict)
			return true
		}
		if req.Body != nil {
			defer req.Body.Close()
		}
		if req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
			http.Error(w, "export execution has no request body", http.StatusBadRequest)
			return true
		}
		s, err := r.ownedExport(req.Context(), id)
		if err != nil || s.Job.State != ExportClaimed || subtle.ConstantTimeCompare([]byte(s.Job.Claim), []byte(values[0])) != 1 {
			http.Error(w, "export unavailable", http.StatusConflict)
			return true
		}
		result, err := r.RunExport(req.Context(), s)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": query.PublicError(err)})
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
		return true
	}
	if len(parts) == 5 && parts[3] == "parts" && req.Method == http.MethodGet {
		r.serveExportPart(w, req, id, parts[4])
		return true
	}
	http.NotFound(w, req)
	return true
}

func (r *ExportRuntime) serveExportPart(w http.ResponseWriter, req *http.Request, id, indexText string) {
	ctx, cancel := context.WithTimeout(req.Context(), r.cfg.Policy.Limits.Timeout)
	defer cancel()
	req = req.WithContext(ctx)
	values := req.Header.Values("X-Kelvo-Export-Receipt")
	index, err := strconv.Atoi(indexText)
	if len(values) != 1 || len(values[0]) != 64 || index < 0 || strconv.Itoa(index) != indexText || err != nil || len(req.Header.Values("Range")) != 0 {
		http.Error(w, "invalid export part request", http.StatusBadRequest)
		return
	}
	s, err := r.store.GetExport(req.Context(), id)
	if err != nil || s.Job.Receipt == nil || subtle.ConstantTimeCompare([]byte(s.Job.Receipt.ReceiptSHA256), []byte(values[0])) != 1 {
		http.NotFound(w, req)
		return
	}
	s, err = r.readyExport(req.Context(), id, s.Job.Receipt)
	if err != nil {
		http.NotFound(w, req)
		return
	}
	op, err := r.audit.begin(req.Context(), s.Job.TenantID, &s.Job.Authority.Principal, audit.ExportResults)
	if err != nil {
		http.Error(w, "audit storage unavailable", http.StatusServiceUnavailable)
		return
	}
	defer op.abort(req.Context())
	part, err := r.OpenPart(req.Context(), s, index)
	if err != nil {
		http.Error(w, "export part unavailable", http.StatusConflict)
		return
	}
	defer part.Close()
	info := s.Job.Receipt.Manifest.Parts[index]
	controller := http.NewResponseController(w)
	deadline := minTime(s.Job.ExpiresAt, time.Now().Add(r.cfg.Policy.Limits.Timeout))
	_ = controller.SetWriteDeadline(deadline)
	stopWrite := make(chan struct{})
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		select {
		case <-part.ctx.Done():
			_ = controller.SetWriteDeadline(time.Now())
		case <-stopWrite:
		}
	}()
	defer func() { close(stopWrite); <-writeDone; _ = controller.SetWriteDeadline(time.Time{}) }()
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.EncodedBytes, 10))
	w.Header().Set("ETag", `"`+info.SHA256+`"`)
	w.Header().Set("Kelvo-Result-Completion", "retained-export-eos-v1")
	w.WriteHeader(http.StatusOK)
	tail := &arrowEOSTail{w: w}
	n, err := io.CopyBuffer(tail, io.LimitReader(part, info.EncodedBytes+1), make([]byte, 32<<10))
	if err != nil || n != info.EncodedBytes || !tail.validEOS() || part.FinalReady() != nil || op.complete(nil) != nil || part.FinalReady() != nil || tail.FlushEOS() != nil {
		panic(http.ErrAbortHandler)
	}
}
