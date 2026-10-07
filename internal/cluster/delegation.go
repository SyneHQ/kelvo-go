// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

// ResolverTrust is operator authority, included in the immutable tenant policy.
type ResolverTrust struct {
	Issuer    string `json:"issuer" yaml:"issuer"`
	Audience  string `json:"audience" yaml:"audience"`
	URL       string `json:"url" yaml:"url"`
	PublicKey string `json:"public_key" yaml:"public_key"`
}

func validateResolverTrust(t ResolverTrust) error {
	key, err := base64.StdEncoding.Strict().DecodeString(t.PublicKey)
	u, e := url.Parse(t.URL)
	if err != nil || len(key) != ed25519.PublicKeySize || base64.StdEncoding.EncodeToString(key) != t.PublicKey || !clusterID.MatchString(t.Issuer) || t.Audience == "" || len(t.Audience) > 128 || strings.ContainsAny(t.Audience, "\x00\r\n") || e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || u.Path != resolver.QueryPath || u.RawPath != "" {
		return errors.New("invalid delegated connection resolver authority")
	}
	return nil
}
func resolverTrust(p Policy, a JobAuthority) (delegation.Trust, error) {
	if p.Access == nil {
		return delegation.Trust{}, delegation.ErrInvalid
	}
	g, ok := p.Access.Principals[a.PrincipalID]
	if !ok || g.Kind != "service" || g.DelegatedResolver == nil || validateResolverTrust(*g.DelegatedResolver) != nil {
		return delegation.Trust{}, delegation.ErrInvalid
	}
	t := g.DelegatedResolver
	key, _ := base64.StdEncoding.Strict().DecodeString(t.PublicKey)
	return delegation.Trust{Issuer: t.Issuer, Audience: t.Audience, ClusterTenant: p.TenantID, ServicePrincipal: a.PrincipalID, PublicKey: ed25519.PublicKey(key)}, nil
}
func baseJobAuthority(a JobAuthority) JobAuthority {
	return JobAuthority{PrincipalID: a.PrincipalID, PrincipalKind: a.PrincipalKind, PolicyVersion: a.PolicyVersion}
}
func delegatedAuthority(p Policy, a JobAuthority, r query.Request, now time.Time) (JobAuthority, delegation.Claims, error) {
	denied := query.NewError("PERMISSION_DENIED", "Query access denied")
	current, ok := authorityForPrincipal(p, a.PrincipalID)
	if !ok || current != baseJobAuthority(a) {
		return JobAuthority{}, delegation.Claims{}, denied
	}
	trust, err := resolverTrust(p, current)
	if err != nil {
		return JobAuthority{}, delegation.Claims{}, denied
	}
	c, err := delegation.Verify(r.Delegation, trust, r, now)
	if err != nil {
		return JobAuthority{}, delegation.Claims{}, denied
	}
	current.DelegationSHA256 = delegation.Digest(r.Delegation)
	current.ApplicationTeam = c.AppTeam
	current.SubjectKind = c.Subject.Kind
	current.SubjectID = c.Subject.ID
	current.SubjectJobID = c.Subject.JobID
	return current, c, nil
}

type delegationHeaderKey struct{}
type delegationCancelKey struct{}

// Only verified service authentication can introduce a delegated request context.
func delegatedHTTPContext(r *http.Request, p Policy) (context.Context, context.CancelFunc, error) {
	noop := func() {}
	values := r.Header.Values(delegation.Header)
	if len(values) == 0 {
		return r.Context(), noop, nil
	}
	if len(values) != 1 || values[0] == "" || len(values[0]) > delegation.MaxTokenBytes || strings.ContainsAny(values[0], " \t\r\n") {
		return nil, noop, delegation.ErrInvalid
	}
	a, ok := jobAuthorityFromContext(r.Context())
	if !ok {
		return nil, noop, delegation.ErrInvalid
	}
	trust, err := resolverTrust(p, a)
	if err != nil {
		return nil, noop, err
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	cleanup := r.Method == http.MethodPost && len(parts) == 4 && parts[0] == "v1" && parts[1] == "queries" && parts[3] == "cancel"
	now := time.Now()
	grace := time.Duration(0)
	if cleanup {
		grace = 5 * time.Second
	}
	c, err := delegation.VerifyClaims(values[0], trust, now.Add(-grace))
	if err != nil {
		return nil, noop, err
	}
	ctx := context.WithValue(r.Context(), delegationHeaderKey{}, values[0])
	if cleanup {
		ctx = context.WithValue(ctx, delegationCancelKey{}, true)
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(c.ExpiresAt, 0).Add(grace))
	return ctx, cancel, nil
}

type connectionLeaseRequest = resolver.Binding
type workerLeaseReader interface {
	WorkerLease(context.Context, string, string) (time.Time, error)
}

// connectionLease attests current durable custody without returning credentials or SQL.
func (g *Gateway) connectionLease(w http.ResponseWriter, r *http.Request, t gatewayTenant, id string) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	defer r.Body.Close()
	raw, err := io.ReadAll(r.Body)
	var input connectionLeaseRequest
	if err != nil || delegation.StrictJSON(raw, &input) != nil || !clusterID.MatchString(input.WorkerID) || !validOwner(input.Owner) || !validClaim(input.Claim) {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid connection lease request")
		return
	}
	s, err := principalSnapshot(r.Context(), t, id)
	if err != nil || s.Job.Authority == nil || s.Job.Authority.DelegationSHA256 == "" {
		g.err(w, 404, "NOT_FOUND", "Query not found")
		return
	}
	p := t.store.Policy()
	now := time.Now()
	j := s.Job
	if j.State != Running || j.WorkerID != input.WorkerID || subtle.ConstantTimeCompare([]byte(j.Owner), []byte(input.Owner)) != 1 || subtle.ConstantTimeCompare([]byte(j.Claim), []byte(input.Claim)) != 1 || !now.Before(j.ExpiresAt) || j.HeartbeatAt.After(now.Add(time.Second)) || !now.Before(j.HeartbeatAt.Add(p.LeaseDuration)) {
		g.err(w, 409, "UNAVAILABLE", "Connection lease is unavailable")
		return
	}
	reader, ok := t.store.(workerLeaseReader)
	if !ok {
		g.err(w, 503, "UNAVAILABLE", "Connection lease is unavailable")
		return
	}
	workerUntil, err := reader.WorkerLease(r.Context(), j.WorkerID, j.Owner)
	if err != nil {
		g.err(w, 409, "UNAVAILABLE", "Connection lease is unavailable")
		return
	}
	_, claims, err := delegatedAuthority(p, *j.Authority, j.Request, now)
	if err != nil {
		g.err(w, 404, "NOT_FOUND", "Query not found")
		return
	}
	until := minTime(now.Add(5*time.Second), minTime(time.Unix(claims.ExpiresAt, 0), minTime(j.ExpiresAt, minTime(j.HeartbeatAt.Add(p.LeaseDuration), workerUntil))))
	// Check the exact state again after custody I/O; cancellation must not turn a
	// delayed store read into a fresh credential lease.
	current, err := principalSnapshot(r.Context(), t, id)
	if err != nil || current.Job.State != Running || current.Job.WorkerID != j.WorkerID || current.Job.Owner != j.Owner || current.Job.Claim != j.Claim || !sameAuthority(current.Job.Authority, j.Authority) || !time.Now().Before(until) || until.Unix() <= time.Now().Unix() {
		g.err(w, 409, "UNAVAILABLE", "Connection lease is unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	g.json(w, 200, resolver.LeaseResponse{ValidUntil: until.Unix()})
}

// WorkerLease reads the existing worker ownership record without renewing it.
func (s *NATSStore) WorkerLease(ctx context.Context, id, owner string) (time.Time, error) {
	if !clusterID.MatchString(id) || s.policy.Workers[id] < 1 || !validOwner(owner) {
		return time.Time{}, ErrConflict
	}
	entry, err := s.kv.Get(ctx, "worker."+id)
	if err != nil {
		return time.Time{}, errors.New("worker custody unavailable")
	}
	var lease workerLease
	if json.Unmarshal(entry.Value(), &lease) != nil || lease.Owner != owner {
		return time.Time{}, ErrConflict
	}
	now := time.Now()
	until := lease.HeartbeatAt.Add(s.policy.LeaseDuration)
	if lease.HeartbeatAt.After(now.Add(time.Second)) || !now.Before(until) {
		return time.Time{}, ErrConflict
	}
	return until, nil
}
