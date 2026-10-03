// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// PrincipalPolicy grants access to whole configured sources. Row/column
// restrictions must already be enforced by each source's database credentials.
// A change requires draining and reprovisioning the tenant's broker account:
// OpenStore rejects replicas with any different durable policy metadata.
type PrincipalPolicy struct {
	Revision   uint64                    `json:"revision" yaml:"revision"`
	Principals map[string]PrincipalGrant `json:"principals" yaml:"principals"`
}

type PrincipalGrant struct {
	Kind                string   `json:"kind" yaml:"kind"`
	NativeSources       []string `json:"native_sources,omitempty" yaml:"native_sources,omitempty"`
	FederatedSources    []string `json:"federated_sources,omitempty" yaml:"federated_sources,omitempty"`
	AllowLiteralQueries bool     `json:"allow_literal_queries,omitempty" yaml:"allow_literal_queries,omitempty"`
}

// JobAuthority is attached by the authenticated gateway, never decoded from
// the public query request. It is immutable across every durable transition.
type JobAuthority struct {
	PrincipalID   string `json:"principal_id"`
	PrincipalKind string `json:"principal_kind"`
	PolicyVersion string `json:"policy_version"`
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
	if p.Revision == 0 || len(p.Principals) == 0 || len(p.Principals) > 64 {
		return bad
	}
	for id, g := range p.Principals {
		if !clusterID.MatchString(id) || (g.Kind != "user" && g.Kind != "service") {
			return bad
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
	return nil
}

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
	denied := query.NewError("PERMISSION_DENIED", "Query access denied")
	if p.Access == nil {
		if a != nil {
			return denied
		}
		return nil
	}
	if a == nil {
		return denied
	}
	current, ok := authorityForPrincipal(p, a.PrincipalID)
	if !ok || current != *a {
		return denied
	}
	if query.ValidateRequest(request) != nil {
		return denied
	}
	grant := p.Access.Principals[a.PrincipalID]
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
	if err := validateJobAuthority(p, result, request); err != nil {
		return nil, err
	}
	return result, nil
}

// principalSnapshot returns NOT_FOUND for foreign handles before state, query
// statistics or error information can escape to another user in the tenant.
func principalSnapshot(ctx context.Context, t gatewayTenant, id string) (Snapshot, error) {
	if err := requestAuthorityErr(ctx); err != nil {
		return Snapshot{}, err
	}
	s, err := t.store.Get(ctx, id)
	if err == nil {
		err = requestAuthorityErr(ctx)
	}
	if err != nil {
		return Snapshot{}, err
	}
	a, ok := jobAuthorityFromContext(ctx)
	if t.store.Policy().Access == nil {
		if ok || s.Job.Authority != nil {
			return Snapshot{}, ErrNotFound
		}
		return s, nil
	}
	if !ok || s.Job.Authority == nil || a != *s.Job.Authority || validateJobAuthority(t.store.Policy(), &a, s.Job.Request) != nil {
		return Snapshot{}, ErrNotFound
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
