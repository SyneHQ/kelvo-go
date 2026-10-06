// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"mime"
	"net/http"
	"slices"
	"strings"
	"time"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type operationInputRequest struct {
	Scope   operationstore.Scope   `json:"scope"`
	Binding operationstore.Binding `json:"binding"`
}

// OperationInputTLS serves only the dedicated private input listener. Static
// trust is explicit; the ordinary public gateway never accepts these requests.
func OperationInputTLS(config TLSConfig) (*tls.Config, error) {
	if config.Trust != nil || config.IdentityFile != "" || config.ReloadInterval != 0 {
		return nil, errOperationConfig
	}
	server, err := BuildServerTLS(config, GatewayIdentity, "")
	if err != nil {
		return nil, err
	}
	roots, err := loadTLSRoots(config, nil)
	if err != nil {
		return nil, err
	}
	server.ClientAuth, server.ClientCAs, server.SessionTicketsDisabled = tls.RequireAndVerifyClientCert, roots, true
	return server, nil
}

// OperationInputs must be installed only on OperationInputTLS's private server.
// It verifies TLS again, so accidentally exposing the handler fails closed.
func (g *Gateway) OperationInputs() http.Handler { return http.HandlerFunc(g.serveOperationInput) }

func (g *Gateway) serveOperationInput(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		g.err(w, 503, "UNAVAILABLE", "Service unavailable")
		return
	}
	g.handlers.Add(1)
	g.mu.Unlock()
	defer g.handlers.Done()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	stop := context.AfterFunc(g.ctx, cancel)
	defer stop()
	r = r.WithContext(ctx)
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if r.Method != http.MethodPost || len(parts) != 5 || parts[0] != "v1" || parts[1] != "operations" || (parts[4] != "input" && parts[4] != "bulk-input") || r.URL.RawQuery != "" || r.URL.RawPath != "" || !clusterID.MatchString(parts[2]) || !operations.ValidID(parts[3]) {
		g.err(w, 404, "NOT_FOUND", "Not found")
		return
	}
	bulk := parts[4] == "bulk-input"
	expectedState := operationstore.Assigned
	if bulk {
		expectedState = operationstore.Running
	}
	state := g.operations[parts[2]]
	if state == nil {
		g.err(w, 404, "NOT_FOUND", "Operation not found")
		return
	}
	select {
	case g.permits <- struct{}{}:
		defer func() { <-g.permits }()
	default:
		g.err(w, 429, "RESOURCE_EXHAUSTED", "Input capacity unavailable")
		return
	}
	defer r.Body.Close()
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || len(r.Header.Values("Content-Type")) != 1 || media != "application/json" || r.Header.Get("Content-Encoding") != "" {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid input request")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8192))
	var input operationInputRequest
	if err != nil || operations.DecodeStrict(raw, &input, 8192) != nil || input.Scope.Validate() != nil || input.Binding.Validate() != nil || input.Scope.ClusterTenant != parts[2] {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid input request")
		return
	}
	peerUntil, valid := operationWorkerPeer(r.TLS, WorkerIdentity(parts[2], input.Binding.WorkerID), time.Now())
	if !valid {
		g.err(w, 403, "PERMISSION_DENIED", "Worker identity required")
		return
	}
	if err := http.NewResponseController(w).SetWriteDeadline(minTime(peerUntil, time.Now().Add(5*time.Second))); err != nil && !errors.Is(err, http.ErrNotSupported) {
		g.operationError(w, err)
		return
	}
	record, err := state.store.Get(ctx, input.Scope, parts[3])
	if err != nil || record.Record.State != expectedState || record.Record.Binding != input.Binding {
		g.err(w, 409, "CONFLICT", "Input custody unavailable")
		return
	}
	trust, err := operationTrust(g.tenants[parts[2]].store.Policy(), input.Scope.ServicePrincipal)
	if err != nil {
		g.err(w, 403, "PERMISSION_DENIED", "Input access denied")
		return
	}
	claims, err := operations.VerifyGrantClaims(record.Record.AuthorityToken, trust, time.Now())
	if err != nil || operationScope(claims) != input.Scope || claims.RequestSHA256 != record.Record.RequestSHA256 || claims.Operation != record.Record.Kind || !slices.Contains(g.tenants[parts[2]].store.Policy().Access.Principals[input.Scope.ServicePrincipal].Operations, claims.Operation) {
		g.err(w, 403, "PERMISSION_DENIED", "Input access denied")
		return
	}
	reader, ok := g.tenants[parts[2]].store.(workerLeaseReader)
	if !ok {
		g.err(w, 503, "UNAVAILABLE", "Worker custody unavailable")
		return
	}
	workerUntil, err := reader.WorkerLease(ctx, input.Binding.WorkerID, input.Binding.Owner)
	if err != nil {
		g.err(w, 409, "CONFLICT", "Worker custody unavailable")
		return
	}
	payload, err := state.inputs.Load(ctx, operationInputIdentity(input.Scope, record.Record.AuthoritySHA256), record.Record.RequestRef)
	if err != nil {
		g.operationError(w, err)
		return
	}
	defer clear(payload)
	request, err := operations.ParseRequest(payload)
	if err == nil {
		_, err = operations.VerifyGrant(record.Record.AuthorityToken, trust, request, time.Now())
	}
	if err != nil {
		g.err(w, 403, "PERMISSION_DENIED", "Input access denied")
		return
	}
	mediaType := "application/json"
	if bulk {
		ref := request.InputReference()
		if ref == nil || operations.SealedInputOperation(ref.Format) == "" || request.Kind != operations.SealedInputOperation(ref.Format) || ref.Bytes > operations.MaxSealedInputBytes {
			g.err(w, 403, "PERMISSION_DENIED", "Bulk input access denied")
			return
		}
		payload, err = state.inputs.Load(ctx, operationBulkIdentity(input.Scope, *ref), *ref)
		defer clear(payload)
		if err != nil {
			g.operationError(w, err)
			return
		}
		mediaType = "application/octet-stream"
	}
	current, err := state.store.Get(ctx, input.Scope, parts[3])
	now := time.Now()
	if err != nil || current.Record.State != expectedState || current.Record.Binding != input.Binding || current.Record.RequestRef != record.Record.RequestRef || !now.Before(current.Record.LeaseUntil) || !now.Before(workerUntil) || !now.Before(peerUntil) || ctx.Err() != nil {
		g.err(w, 409, "CONFLICT", "Input custody unavailable")
		return
	}
	w.Header().Set("Content-Type", mediaType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func operationWorkerPeer(state *tls.ConnectionState, expected string, now time.Time) (time.Time, bool) {
	if state == nil || !state.HandshakeComplete || state.Version < tls.VersionTLS13 || len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return time.Time{}, false
	}
	leaf := state.PeerCertificates[0]
	if leaf == nil || leaf.IsCA || len(leaf.URIs) != 1 || leaf.URIs[0].String() != expected || !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		return time.Time{}, false
	}
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 || !chain[0].Equal(leaf) {
			continue
		}
		until, valid := leaf.NotAfter, true
		for _, cert := range chain {
			if cert == nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
				valid = false
				break
			}
			until = minTime(until, cert.NotAfter)
		}
		if valid {
			return until, true
		}
	}
	return time.Time{}, false
}
