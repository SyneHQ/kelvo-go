// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package resolver

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// Authorization is an application-owned, current metadata decision. Revision
// must digest every source and authorization record whose change revokes access.
// ValidUntil bounds current membership, API-key, job, approval or session access.
// The handler compares a fresh decision after credentials are loaded.
type Authorization struct {
	Revision   string
	ValidUntil time.Time
}

type QueryMaterial struct {
	Sources []Source
	Secrets map[string]string
	// ValidUntil is mandatory and bounds the actual secret-provider expiry.
	// For static credentials, use the supplied Authorization.ValidUntil.
	ValidUntil time.Time
}

type OperationMaterial struct {
	Source  Source
	Secrets map[string]string
	// ValidUntil is mandatory; it cannot extend the grant, policy or custody.
	ValidUntil time.Time
}

// CompletionAuthority must come from retained application custody, including
// the exact operation/request/grant digests and worker/owner/claim tuple. Its
// expiry may outlive the original grant to permit physical-cleanup reporting.
type CompletionAuthority struct {
	Request       CompletionRequest
	ClusterTenant string
	ValidUntil    time.Time
}

// Config wires application policy and storage into the private handler. Hooks
// are trusted implementation code, must honor context cancellation and must not
// log requests, grants or secrets. Returned material transfers ownership to the
// handler. Every endpoint is disabled unless all of its required hooks exist.
//
// Authorize hooks must consult live application authority, including membership,
// source ownership, source revision, job/approval/ingestion/watch scope and the
// signed database/schema selection. Resolve hooks load only the corresponding
// metadata and secrets. The handler independently verifies signed request scope,
// worker identity, current custody, secret namespaces and response expiry.
type Config struct {
	QueryTrust          []delegation.Trust
	OperationTrust      []operations.GrantTrust
	Leases              LeaseVerifier
	Timeout             time.Duration
	MaxConcurrent       int
	TempDir             string
	AuthorizeQuery      func(context.Context, QueryRequest, delegation.Claims) (Authorization, error)
	ResolveQuery        func(context.Context, QueryRequest, delegation.Claims, Authorization) (QueryMaterial, error)
	AuthorizeOperation  func(context.Context, OperationRequest, operations.GrantClaims) (Authorization, error)
	ResolveOperation    func(context.Context, OperationRequest, operations.GrantClaims, Authorization) (OperationMaterial, error)
	AuthorizeCompletion func(context.Context, CompletionRequest) (CompletionAuthority, error)
	CompleteOperation   func(context.Context, CompletionAuthority) error
	OpenFile            func(context.Context, FileReadRequest, operations.GrantClaims, Authorization) (io.ReadCloser, error)
	LockPublication     func(context.Context, FileCommitRequest, operations.GrantClaims) (PublicationLock, error)
}

// Handler is intended for a dedicated mTLS listener with body/access logging
// disabled. Reverse-proxy identity headers are never accepted as TLS identity.
// Configure the listener with TLS 1.3 and RequireAndVerifyClientCert; the handler
// independently checks its verified chain again before releasing credentials.
type Handler struct {
	config Config
	slots  chan struct{}
	// Cleanup must remain reachable while ordinary callbacks wait on locks
	// held by operations whose workers are reporting physical completion.
	completions chan struct{}
}

func NewHandler(c Config) (*Handler, error) {
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	if c.MaxConcurrent == 0 {
		c.MaxConcurrent = 8
	}
	if c.Timeout < time.Second || c.Timeout > 30*time.Second || c.MaxConcurrent < 1 || c.MaxConcurrent > 64 || c.Leases == nil || len(c.QueryTrust) > 64 || len(c.OperationTrust) > 64 {
		return nil, ErrInvalid
	}
	c.QueryTrust = slices.Clone(c.QueryTrust)
	c.OperationTrust = slices.Clone(c.OperationTrust)
	for i, t := range c.QueryTrust {
		if !validTrust(t.Issuer, t.Audience, t.ClusterTenant, t.ServicePrincipal, t.PublicKey) {
			return nil, ErrInvalid
		}
		c.QueryTrust[i].PublicKey = slices.Clone(t.PublicKey)
	}
	for i, t := range c.OperationTrust {
		if !validTrust(t.Issuer, t.Audience, t.ClusterTenant, t.ServicePrincipal, t.PublicKey) {
			return nil, ErrInvalid
		}
		c.OperationTrust[i].PublicKey = slices.Clone(t.PublicKey)
	}
	return &Handler{config: c, slots: make(chan struct{}, c.MaxConcurrent), completions: make(chan struct{}, c.MaxConcurrent)}, nil
}

func validTrust(issuer, audience, tenant, principal string, key ed25519.PublicKey) bool {
	return issuer != "" && len(issuer) <= 128 && utf8.ValidString(issuer) && !strings.ContainsAny(issuer, "\x00\r\n") && audience != "" && len(audience) <= 128 && utf8.ValidString(audience) && !strings.ContainsAny(audience, "\x00\r\n") && workerPattern.MatchString(tenant) && workerPattern.MatchString(principal) && len(key) == ed25519.PublicKeySize
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if h == nil || h.slots == nil {
		deny(w, http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || r.URL.Fragment != "" {
		deny(w, http.StatusNotFound)
		return
	}
	switch r.URL.Path {
	case QueryPath, OperationPath, CompletionPath, FileReadPath, FileCommitPath:
	default:
		deny(w, http.StatusNotFound)
		return
	}
	if r.TLS == nil || !r.TLS.HandshakeComplete || r.TLS.Version < tls.VersionTLS13 || len(r.TLS.VerifiedChains) == 0 {
		deny(w, http.StatusForbidden)
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	want := "application/json"
	if r.URL.Path == FileCommitPath {
		want = FileCommitMediaType
	}
	if err != nil || media != want || len(r.Header.Values("Content-Type")) != 1 || len(r.Header.Values("Content-Encoding")) != 0 {
		deny(w, http.StatusBadRequest)
		return
	}
	slots := h.slots
	if r.URL.Path == CompletionPath {
		slots = h.completions
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		deny(w, http.StatusTooManyRequests)
		return
	}
	defer r.Body.Close()
	ctx, cancel := context.WithTimeout(r.Context(), h.config.Timeout)
	defer cancel()
	r = r.WithContext(ctx)
	// This also covers wrappers without ResponseController deadline support.
	// Closing an HTTP request body interrupts an in-progress network read.
	stopBodyClose := context.AfterFunc(ctx, func() { _ = r.Body.Close() })
	defer stopBodyClose()
	deadline, _ := ctx.Deadline()
	if setDeadline(w, deadline, true) != nil {
		deny(w, http.StatusServiceUnavailable)
		return
	}
	switch r.URL.Path {
	case QueryPath:
		h.query(w, r)
	case OperationPath:
		h.operation(w, r)
	case CompletionPath:
		h.complete(w, r)
	case FileReadPath:
		h.fileRead(w, r)
	case FileCommitPath:
		h.fileCommit(w, r)
	}
}

func deny(w http.ResponseWriter, status int) {
	http.Error(w, "Connection resolution unavailable", status)
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any, limit int) error {
	if r.ContentLength > int64(limit) {
		return ErrInvalid
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(limit)))
	defer clear(raw)
	if err != nil {
		return ErrInvalid
	}
	if _, ok := dst.(*QueryRequest); ok {
		return delegation.StrictJSONLimit(raw, dst, limit)
	}
	return operations.DecodeStrict(raw, dst, limit)
}

func setDeadline(w http.ResponseWriter, until time.Time, read bool) error {
	c := http.NewResponseController(w)
	if err := c.SetWriteDeadline(until); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if read {
		if err := c.SetReadDeadline(until); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
	}
	return nil
}

// WorkerIdentity returns the only allowed URI SAN for a custody binding.
func WorkerIdentity(tenant, worker string) (string, error) {
	if !workerPattern.MatchString(tenant) || !workerPattern.MatchString(worker) {
		return "", ErrInvalid
	}
	return "spiffe://kelvo/tenant/" + tenant + "/worker/" + worker, nil
}

// WorkerCertificateExpiry rechecks the authenticated leaf identity, explicit
// client-auth use, TLS version and the complete chain at the time of use.
func WorkerCertificateExpiry(state *tls.ConnectionState, tenant, worker string, now time.Time) (time.Time, error) {
	identity, err := WorkerIdentity(tenant, worker)
	if err != nil || state == nil || !state.HandshakeComplete || state.Version < tls.VersionTLS13 || len(state.PeerCertificates) == 0 {
		return time.Time{}, ErrInvalid
	}
	leaf := state.PeerCertificates[0]
	if leaf == nil || leaf.IsCA || len(leaf.URIs) != 1 || leaf.URIs[0] == nil || leaf.URIs[0].String() != identity || !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		return time.Time{}, ErrInvalid
	}
	var best time.Time
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 || chain[0] == nil || !chain[0].Equal(leaf) {
			continue
		}
		until := leaf.NotAfter
		valid := true
		for _, cert := range chain {
			if cert == nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
				valid = false
				break
			}
			if cert.NotAfter.Before(until) {
				until = cert.NotAfter
			}
		}
		if valid && until.After(best) {
			best = until
		}
	}
	if best.IsZero() {
		return time.Time{}, ErrInvalid
	}
	return best, nil
}

// clone gives hooks detached request/claims snapshots so an accidental mutation
// cannot change the envelope, source selection or custody that was verified.
func clone[T any](v T) T {
	raw, _ := json.Marshal(v)
	defer clear(raw)
	var out T
	_ = json.Unmarshal(raw, &out)
	return out
}

func (h *Handler) verifyQuery(input QueryRequest) (delegation.Claims, error) {
	if input.Validate() != nil {
		return delegation.Claims{}, ErrInvalid
	}
	for _, t := range h.config.QueryTrust {
		if c, e := delegation.Verify(input.Delegation, t, input.Query, time.Now()); e == nil {
			return c, nil
		}
	}
	return delegation.Claims{}, ErrInvalid
}

func (h *Handler) verifyOperation(input OperationRequest) (operations.GrantClaims, error) {
	if input.Validate() != nil {
		return operations.GrantClaims{}, ErrInvalid
	}
	for _, t := range h.config.OperationTrust {
		if c, e := operations.VerifyGrant(input.Grant, t, input.Operation, time.Now()); e == nil {
			return c, nil
		}
	}
	return operations.GrantClaims{}, ErrInvalid
}

func validAuthorization(a Authorization) bool {
	return operations.ValidDigest(a.Revision) && a.ValidUntil.Unix() > time.Now().Unix()
}

func boundUntil(ctx context.Context, authorityExpiry int64, lease, peer time.Time, a Authorization) (time.Time, error) {
	now := time.Now()
	// A verifier cannot silently replace short custody with the grant lifetime.
	if ctx.Err() != nil || !validAuthorization(a) || lease.Unix() <= now.Unix() || lease.Unix() > now.Unix()+5 {
		return time.Time{}, ErrInvalid
	}
	until := min(authorityExpiry, lease.Unix(), peer.Unix(), a.ValidUntil.Unix())
	if d, ok := ctx.Deadline(); ok {
		until = min(until, d.Unix())
	}
	if until <= now.Unix() {
		return time.Time{}, ErrInvalid
	}
	return time.Unix(until, 0), nil
}

func (h *Handler) queryCheck(r *http.Request, input QueryRequest) (delegation.Claims, Authorization, time.Time, error) {
	c, err := h.verifyQuery(input)
	if err != nil {
		return c, Authorization{}, time.Time{}, err
	}
	peer, err := WorkerCertificateExpiry(r.TLS, c.ClusterTenant, input.WorkerID, time.Now())
	if err != nil {
		return c, Authorization{}, time.Time{}, err
	}
	lease, err := h.config.Leases.ValidateConnectionLease(r.Context(), input.JobID, input.Delegation, input.Binding())
	if err != nil {
		return c, Authorization{}, time.Time{}, err
	}
	a, err := h.config.AuthorizeQuery(r.Context(), clone(input), clone(c))
	if err != nil {
		return c, a, time.Time{}, err
	}
	until, err := boundUntil(r.Context(), c.ExpiresAt, lease, peer, a)
	return c, a, until, err
}

func (h *Handler) operationCheck(r *http.Request, input OperationRequest) (operations.GrantClaims, Authorization, time.Time, error) {
	c, err := h.verifyOperation(input)
	if err != nil {
		return c, Authorization{}, time.Time{}, err
	}
	peer, err := WorkerCertificateExpiry(r.TLS, c.ClusterTenant, input.WorkerID, time.Now())
	if err != nil {
		return c, Authorization{}, time.Time{}, err
	}
	lease, err := h.config.Leases.ValidateOperationLease(r.Context(), input.OperationID, input.Grant, input.Binding())
	if err != nil {
		return c, Authorization{}, time.Time{}, err
	}
	a, err := h.config.AuthorizeOperation(r.Context(), clone(input), clone(c))
	if err != nil {
		return c, a, time.Time{}, err
	}
	until, err := boundUntil(r.Context(), c.ExpiresAt, lease, peer, a)
	return c, a, until, err
}

func (h *Handler) query(w http.ResponseWriter, r *http.Request) {
	if h.config.AuthorizeQuery == nil || h.config.ResolveQuery == nil {
		deny(w, http.StatusForbidden)
		return
	}
	var input QueryRequest
	if readJSON(w, r, &input, MaxQueryRequestBytes) != nil || input.Validate() != nil {
		deny(w, http.StatusBadRequest)
		return
	}
	claims, before, initialUntil, err := h.queryCheck(r, input)
	if err != nil {
		deny(w, http.StatusForbidden)
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), initialUntil)
	defer cancel()
	r = r.WithContext(ctx)
	material, err := h.config.ResolveQuery(r.Context(), clone(input), clone(claims), before)
	defer clear(material.Secrets)
	if err != nil || validateQueryMaterial(material, claims, input) != nil {
		deny(w, http.StatusForbidden)
		return
	}
	_, after, until, err := h.queryCheck(r, input)
	if err != nil || before.Revision != after.Revision {
		deny(w, http.StatusForbidden)
		return
	}
	until = time.Unix(min(until.Unix(), material.ValidUntil.Unix()), 0)
	response := QueryResponse{Version: Version, DelegationSHA256: delegation.Digest(input.Delegation), ValidUntil: until.Unix(), Sources: material.Sources, Secrets: material.Secrets}
	writeJSON(w, r, http.StatusOK, response, until)
}

func validateQueryMaterial(m QueryMaterial, c delegation.Claims, input QueryRequest) error {
	if m.ValidUntil.Unix() <= time.Now().Unix() || validateSources(m.Sources, m.Secrets) != nil || len(m.Sources) != len(c.Sources) {
		return ErrInvalid
	}
	for i, s := range m.Sources {
		selected := c.Sources[i]
		if s.ID != selected.Alias {
			return ErrInvalid
		}
		if input.Query.Mode == "native" {
			if s.Federation != nil || len(selected.Tables) != 0 {
				return ErrInvalid
			}
			continue
		}
		if s.Federation == nil || s.Federation.MaxScanRows != 0 || s.Federation.MaxScanBytes != 0 || len(selected.Tables) == 0 || !slices.Equal(s.Federation.Tables, selected.Tables) {
			return ErrInvalid
		}
	}
	return nil
}

func (h *Handler) operation(w http.ResponseWriter, r *http.Request) {
	if h.config.AuthorizeOperation == nil || h.config.ResolveOperation == nil {
		deny(w, http.StatusForbidden)
		return
	}
	var input OperationRequest
	if readJSON(w, r, &input, MaxOperationRequestBytes) != nil || input.Validate() != nil {
		deny(w, http.StatusBadRequest)
		return
	}
	claims, before, initialUntil, err := h.operationCheck(r, input)
	if err != nil {
		deny(w, http.StatusForbidden)
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), initialUntil)
	defer cancel()
	r = r.WithContext(ctx)
	material, err := h.config.ResolveOperation(r.Context(), clone(input), clone(claims), before)
	defer clear(material.Secrets)
	if err != nil || material.ValidUntil.Unix() <= time.Now().Unix() || material.Source.ID != "source_1" || material.Source.Federation != nil || validateSources([]Source{material.Source}, material.Secrets) != nil {
		deny(w, http.StatusForbidden)
		return
	}
	_, after, until, err := h.operationCheck(r, input)
	if err != nil || before.Revision != after.Revision {
		deny(w, http.StatusForbidden)
		return
	}
	until = time.Unix(min(until.Unix(), material.ValidUntil.Unix()), 0)
	response := OperationResponse{Version: Version, GrantSHA256: operations.GrantDigest(input.Grant), RequestSHA256: claims.RequestSHA256, SourceRevision: after.Revision, ValidUntil: until.Unix(), Source: material.Source, Secrets: material.Secrets}
	writeJSON(w, r, http.StatusOK, response, until)
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, value any, until time.Time) {
	raw, err := json.Marshal(value)
	defer clear(raw)
	if err != nil || len(raw) > MaxResponseBytes || r.Context().Err() != nil || until.Unix() <= time.Now().Unix() {
		deny(w, http.StatusForbidden)
		return
	}
	if setDeadline(w, until, false) != nil {
		deny(w, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func (h *Handler) complete(w http.ResponseWriter, r *http.Request) {
	if h.config.AuthorizeCompletion == nil || h.config.CompleteOperation == nil {
		deny(w, http.StatusForbidden)
		return
	}
	var input CompletionRequest
	if readJSON(w, r, &input, MaxCompletionBytes) != nil || input.Validate() != nil {
		deny(w, http.StatusBadRequest)
		return
	}
	a, err := h.config.AuthorizeCompletion(r.Context(), input)
	if err != nil || a.Request != input || a.ValidUntil.Unix() <= time.Now().Unix() {
		deny(w, http.StatusForbidden)
		return
	}
	peer, err := WorkerCertificateExpiry(r.TLS, a.ClusterTenant, input.WorkerID, time.Now())
	if err != nil {
		deny(w, http.StatusForbidden)
		return
	}
	until := time.Unix(min(peer.Unix(), a.ValidUntil.Unix()), 0)
	ctx, cancel := context.WithDeadline(r.Context(), until)
	defer cancel()
	if setDeadline(w, until, false) != nil || h.config.CompleteOperation(ctx, a) != nil || ctx.Err() != nil {
		deny(w, http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
