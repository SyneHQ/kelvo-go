// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// PrincipalPolicy grants source access and optional callback-federation row
// and column restrictions. Policies originate only in trusted operator config.
// A change requires draining and reprovisioning the tenant's broker account:
// OpenStore rejects replicas with any different durable policy metadata.
type PrincipalPolicy struct {
	Revision       uint64                    `json:"revision" yaml:"revision"`
	Principals     map[string]PrincipalGrant `json:"principals" yaml:"principals"`
	CatalogBinding *CatalogBinding           `json:"catalog_binding,omitempty" yaml:"catalog_binding,omitempty"`
	KeyAuthority   *KeyAuthorityBinding      `json:"key_authority,omitempty" yaml:"key_authority,omitempty"`
}

type PrincipalGrant struct {
	Operations          []operations.Kind `json:"operations,omitempty" yaml:"operations,omitempty"`
	DelegatedResolver   *ResolverTrust    `json:"delegated_resolver,omitempty" yaml:"delegated_resolver,omitempty"`
	RowColumnPolicy     *access.Policy    `json:"row_column_policy,omitempty" yaml:"row_column_policy,omitempty"`
	Kind                string            `json:"kind" yaml:"kind"`
	NativeSources       []string          `json:"native_sources,omitempty" yaml:"native_sources,omitempty"`
	FederatedSources    []string          `json:"federated_sources,omitempty" yaml:"federated_sources,omitempty"`
	AllowLiteralQueries bool              `json:"allow_literal_queries,omitempty" yaml:"allow_literal_queries,omitempty"`
}

// JobAuthority is attached by the authenticated gateway, never decoded from
// the public query request. It is immutable across every durable transition.
type JobAuthority struct {
	PrincipalID      string `json:"principal_id"`
	PrincipalKind    string `json:"principal_kind"`
	PolicyVersion    string `json:"policy_version"`
	DelegationSHA256 string `json:"delegation_sha256,omitempty"`
	ApplicationTeam  string `json:"application_team,omitempty"`
	SubjectKind      string `json:"subject_kind,omitempty"`
	SubjectID        string `json:"subject_id,omitempty"`
	SubjectJobID     string `json:"subject_job_id,omitempty"`
}

type jobAuthorityKey struct{}
type keyPrincipalID struct{}

func jobAuthorityFromContext(ctx context.Context) (JobAuthority, bool) {
	a, ok := ctx.Value(jobAuthorityKey{}).(JobAuthority)
	return a, ok
}

func validatePrincipalPolicy(p *PrincipalPolicy) error {
	if p == nil {
		return nil
	}
	bad := errors.New("invalid principal source policy")
	if !validCatalogBinding(p.CatalogBinding) || !validKeyAuthorityBinding(p.KeyAuthority) {
		return bad
	}
	if p.Revision == 0 || len(p.Principals) == 0 || len(p.Principals) > 64 {
		return bad
	}
	for id, g := range p.Principals {
		if !clusterID.MatchString(id) || (g.Kind != "user" && g.Kind != "service") {
			return bad
		}
		if g.DelegatedResolver != nil && (g.Kind != "service" || validateResolverTrust(*g.DelegatedResolver) != nil || g.RowColumnPolicy != nil) {
			return bad
		}
		if len(g.Operations) > 16 || (len(g.Operations) != 0 && (g.Kind != "service" || g.DelegatedResolver == nil)) {
			return bad
		}
		seenOperations := map[operations.Kind]bool{}
		for _, kind := range g.Operations {
			if !kind.Valid() || seenOperations[kind] {
				return bad
			}
			seenOperations[kind] = true
		}
		if g.RowColumnPolicy != nil {
			if access.Validate(*g.RowColumnPolicy) != nil {
				return bad
			}
			for source := range g.RowColumnPolicy.Sources {
				if !slices.Contains(g.FederatedSources, source) || slices.Contains(g.NativeSources, source) {
					return bad
				}
			}
		}
		for _, sources := range [][]string{g.NativeSources, g.FederatedSources} {
			if len(sources) > 64 {
				return bad
			}
			seen := make(map[string]bool, len(sources))
			for _, source := range sources {
				if !catalog.ValidID(source) || seen[source] {
					return bad
				}
				seen[source] = true
			}
		}
	}
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > 256<<10 {
		return bad
	}
	return nil
}

// errNativeRestricted answers a native query on a source whose rows or columns
// are restricted for this principal: only federated queries apply the policy.
var errNativeRestricted = query.NewError("UNSUPPORTED", "Row and column policies apply to federated queries only; send this query with mode federated")

func principalPolicyVersion(p Policy) string {
	raw, _ := json.Marshal(struct {
		Tenant string           `json:"tenant"`
		Access *PrincipalPolicy `json:"access"`
	}{p.TenantID, p.Access})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func authorityForPrincipal(p Policy, id string) (JobAuthority, bool) {
	if p.Access == nil || id == "" {
		return JobAuthority{}, false
	}
	grant, ok := p.Access.Principals[id]
	if !ok {
		return JobAuthority{}, false
	}
	return JobAuthority{PrincipalID: id, PrincipalKind: grant.Kind, PolicyVersion: principalPolicyVersion(p)}, true
}

func sameAuthority(a, b *JobAuthority) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func validateJobAuthority(p Policy, a *JobAuthority, request query.Request) error {
	return validateJobAuthorityAt(p, a, request, time.Now())
}

func validateJobAuthorityAt(p Policy, a *JobAuthority, request query.Request, now time.Time) error {
	denied := query.NewError("PERMISSION_DENIED", "Query access denied")
	if p.Access == nil {
		if a != nil || request.Delegation != "" {
			return denied
		}
		return nil
	}
	if a == nil {
		return denied
	}
	current, ok := authorityForPrincipal(p, a.PrincipalID)
	if !ok || current != baseJobAuthority(*a) {
		return denied
	}
	if query.ValidateRequest(request) != nil {
		return denied
	}
	grant := p.Access.Principals[a.PrincipalID]
	if request.Delegation != "" {
		bound, _, err := delegatedAuthority(p, current, request, now)
		if err != nil || bound != *a {
			return denied
		}
		return nil
	}
	if current != *a {
		return denied
	}
	includes := func(allowed []string, id string) bool {
		for _, source := range allowed {
			if source == id {
				return true
			}
		}
		return false
	}
	if request.Mode == "native" {
		if !includes(grant.NativeSources, request.ConnectionID) {
			// Row and column policies are enforced by the federated engine only, so
			// say how to reach a restricted source instead of a bare denial. The
			// principal already knows its own grant; this reveals nothing else.
			if grant.RowColumnPolicy != nil && includes(grant.FederatedSources, request.ConnectionID) {
				if _, restricted := grant.RowColumnPolicy.Sources[request.ConnectionID]; restricted {
					return errNativeRestricted
				}
			}
			return denied
		}
		return nil
	}
	if len(request.Sources) == 0 && !grant.AllowLiteralQueries {
		return denied
	}
	for _, source := range request.Sources {
		if !includes(grant.FederatedSources, source) {
			return denied
		}
	}
	return nil
}

func submissionAuthority(ctx context.Context, p Policy, request query.Request) (*JobAuthority, error) {
	if err := requestAuthorityErr(ctx); err != nil {
		return nil, err
	}
	a, ok := jobAuthorityFromContext(ctx)
	var result *JobAuthority
	if ok {
		result = &a
	}
	header, _ := ctx.Value(delegationHeaderKey{}).(string)
	if header != request.Delegation {
		return nil, query.NewError("PERMISSION_DENIED", "Query access denied")
	}
	if request.Delegation != "" {
		if !ok {
			return nil, query.NewError("PERMISSION_DENIED", "Query access denied")
		}
		bound, _, err := delegatedAuthority(p, a, request, time.Now())
		if err != nil {
			return nil, err
		}
		result = &bound
	}
	if err := validateJobAuthority(p, result, request); err != nil {
		return nil, err
	}
	return result, nil
}

// principalSnapshot returns NOT_FOUND for foreign handles before state, query
// statistics or error information can escape to another user in the tenant.
func principalSnapshot(ctx context.Context, t gatewayTenant, id string) (Snapshot, error) {
	s, err := readSnapshot(ctx, t, id)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err := authorizeSnapshot(ctx, t, s); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}

// readSnapshot reads job id while the request's own key authority is live.
func readSnapshot(ctx context.Context, t gatewayTenant, id string) (Snapshot, error) {
	if err := requestAuthorityErr(ctx); err != nil {
		return Snapshot{}, err
	}
	s, err := t.store.Get(ctx, id)
	if err == nil {
		err = requestAuthorityErr(ctx)
	}
	return s, err
}

// authorizeSnapshot checks that the request's principal may see job s: the
// delegation header, the job's stored authority and, when delegated, the
// grant's signature. It returns the delegation's expiry (Unix seconds, zero when
// none), after which the same job is no longer visible. The signature work is
// what costs CPU; see authorizedJob for avoiding it on repeated reads.
func authorizeSnapshot(ctx context.Context, t gatewayTenant, s Snapshot) (int64, error) {
	a, ok := jobAuthorityFromContext(ctx)
	now := time.Now()
	if grace, allowed := ctx.Value(delegationCancelKey{}).(bool); allowed && grace {
		now = now.Add(-5 * time.Second)
	}
	header, _ := ctx.Value(delegationHeaderKey{}).(string)
	if header != s.Job.Request.Delegation {
		return 0, ErrNotFound
	}
	if t.store.Policy().Access == nil {
		if ok || s.Job.Authority != nil {
			return 0, ErrNotFound
		}
		return 0, nil
	}
	var expires int64
	if ok && s.Job.Request.Delegation != "" {
		bound, claims, err := delegatedAuthority(t.store.Policy(), a, s.Job.Request, now)
		if err != nil {
			return 0, ErrNotFound
		}
		a, expires = bound, claims.ExpiresAt
	}
	if !ok || s.Job.Authority == nil || a != *s.Job.Authority || validateJobAuthorityAt(t.store.Policy(), &a, s.Job.Request, now) != nil {
		return 0, ErrNotFound
	}
	return expires, nil
}

// authorizedJob lets a request that re-reads one job many times (a result
// wait) verify its authority once. The stored authority and delegation of a job
// never change, so after the first full check a re-read needs only the cheap
// parts: the request's key authority, the job's identity, and the delegation's
// expiry. No signature is verified again.
type authorizedJob struct {
	set        bool
	delegation string
	authority  *JobAuthority
	expires    int64
}

// snapshot is principalSnapshot with the verification cached after the first
// success. A changed or expired job falls back to the full check.
func (c *authorizedJob) snapshot(ctx context.Context, t gatewayTenant, id string) (Snapshot, error) {
	if !c.set {
		s, err := readSnapshot(ctx, t, id)
		if err != nil {
			return Snapshot{}, err
		}
		expires, err := authorizeSnapshot(ctx, t, s)
		if err != nil {
			return Snapshot{}, err
		}
		c.set, c.delegation, c.expires = true, s.Job.Request.Delegation, expires
		if s.Job.Authority != nil {
			a := *s.Job.Authority
			c.authority = &a
		}
		return s, nil
	}
	s, err := readSnapshot(ctx, t, id)
	if err != nil {
		return Snapshot{}, err
	}
	header, _ := ctx.Value(delegationHeaderKey{}).(string)
	same := header == s.Job.Request.Delegation && s.Job.Request.Delegation == c.delegation &&
		(s.Job.Authority == nil) == (c.authority == nil) && (c.authority == nil || *s.Job.Authority == *c.authority)
	if same && (c.expires == 0 || time.Now().Unix() < c.expires) {
		return s, nil
	}
	if _, err := authorizeSnapshot(ctx, t, s); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}

// Completion checks the key's current authenticator membership directly.
// AfterFunc cancellation is still used for blocked I/O, but callback scheduling
// cannot extend authority past revocation or the key-document lease deadline.
type keyAuthorizationContext struct{}
type keyAuthorization struct {
	authenticator *gatewayAuthenticator
	key           context.Context
}

func requestAuthorityErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if exp, ok := oidcExpiry(ctx); ok && !time.Now().Before(exp) {
		return context.Canceled
	}
	if auth, ok := ctx.Value(keyAuthorizationContext{}).(keyAuthorization); ok {
		if auth.authenticator == nil || !auth.authenticator.active(auth.key) {
			return context.Canceled
		}
	}
	return nil
}

func (a *gatewayAuthenticator) active(ctx context.Context) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	hash, ok := ctx.Value(keyFingerprint{}).([32]byte)
	if !ok {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := a.keys[hash]
	return !a.closed && time.Now().Before(a.validUntil) && key != nil && key.ctx == ctx && ctx.Err() == nil
}

// executionAuthorityContext derives the selected restrictions from provisioned
// policy, after checking the immutable job binding. Public request fields never
// carry or override these grants.
func executionAuthorityContext(ctx context.Context, p Policy, authority *JobAuthority, request query.Request) (context.Context, error) {
	if err := validateJobAuthority(p, authority, request); err != nil {
		return nil, err
	}
	expected := ""
	if p.Access != nil && p.Access.CatalogBinding != nil {
		if !validCatalogBinding(p.Access.CatalogBinding) {
			return nil, errCatalogAuthority
		}
		expected = p.Access.CatalogBinding.SHA256
	}
	var err error
	ctx, err = worker.WithCatalogAuthority(ctx, expected)
	if err != nil {
		return nil, errCatalogAuthority
	}
	if authority == nil {
		return ctx, nil
	}
	ctx = context.WithValue(ctx, jobAuthorityKey{}, *authority)
	if request.Delegation != "" {
		// The restriction of a delegated read is the one its signed grant carries,
		// verified again here for the exact request. Queries and exports both reach
		// this function, so neither can run the grant unrestricted.
		_, claims, err := delegatedAuthority(p, *authority, request, time.Now())
		if err != nil {
			return nil, err
		}
		policy, restricted, err := delegatedPolicy(claims)
		if err != nil {
			return nil, query.NewError("PERMISSION_DENIED", "Query access denied")
		}
		if restricted {
			return access.WithPolicy(ctx, policy)
		}
	}
	grant := p.Access.Principals[authority.PrincipalID]
	if grant.RowColumnPolicy == nil {
		return ctx, nil
	}
	selected := access.Policy{Sources: make(map[string]access.SourcePolicy)}
	if request.Mode == "native" {
		if _, restricted := grant.RowColumnPolicy.Sources[request.ConnectionID]; restricted {
			return nil, query.NewError("PERMISSION_DENIED", "Query access denied")
		}
		return ctx, nil
	}
	for _, id := range request.Sources {
		if source, restricted := grant.RowColumnPolicy.Sources[id]; restricted {
			selected.Sources[id] = source
		}
	}
	if len(selected.Sources) == 0 {
		return ctx, nil
	}
	return access.WithPolicy(ctx, selected)
}
