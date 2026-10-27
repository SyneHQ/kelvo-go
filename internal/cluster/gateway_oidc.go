// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"net/http"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/oidc"
)

type oidcAuthority struct{}

func isOIDCAuthority(ctx context.Context) bool {
	_, ok := ctx.Value(oidcAuthority{}).(bool)
	return ok
}

// newGatewayOIDC is opt-in. Every mapped principal must already exist in the
// tenant's access policy; OIDC never creates identities.
func newGatewayOIDC(ctx context.Context, cfg GatewayConfig) (*oidc.Verifier, error) {
	if cfg.Authentication == nil || cfg.Authentication.OIDC == nil {
		return nil, nil
	}
	var policy *PrincipalPolicy
	for _, tenant := range cfg.Tenants {
		if tenant.Policy.TenantID == cfg.Authentication.OIDC.Tenant {
			policy = tenant.Policy.Access
		}
	}
	if policy == nil {
		return nil, errGatewayAuthConfig
	}
	v, err := oidc.New(ctx, *cfg.Authentication.OIDC)
	if err != nil {
		return nil, errGatewayAuthConfig
	}
	for _, id := range v.MappedPrincipals() {
		if _, ok := policy.Principals[id]; !ok {
			return nil, errGatewayAuthConfig
		}
	}
	return v, nil
}

// oidcAuthenticate is reached only after API-key lookup misses. The returned
// context carries the mapped principal and ends at token expiry or when the
// request finishes, whichever is first. Gateway policy checks then apply
// exactly as for a key bound to that principal.
func (g *Gateway) oidcAuthenticate(r *http.Request, token string) (string, context.Context, bool) {
	principal, exp, err := g.oidc.Verify(r.Context(), token, time.Now())
	if err != nil {
		return "", nil, false
	}
	ctx, cancel := context.WithDeadline(g.ctx, exp)
	context.AfterFunc(r.Context(), cancel)
	ctx = context.WithValue(ctx, keyPrincipalID{}, principal)
	ctx = context.WithValue(ctx, oidcAuthority{}, true)
	return g.oidc.Tenant(), ctx, true
}
