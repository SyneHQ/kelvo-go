// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

const operationGrantHeader = "X-Kelvo-Operation-Grant"

func operationTrust(p Policy, principal string) (operations.GrantTrust, error) {
	a, ok := authorityForPrincipal(p, principal)
	if !ok || p.Operations == nil {
		return operations.GrantTrust{}, operations.ErrInvalid
	}
	trust, err := resolverTrust(p, a)
	if err != nil {
		return operations.GrantTrust{}, operations.ErrInvalid
	}
	return operations.GrantTrust{Issuer: trust.Issuer, Audience: trust.Audience, ClusterTenant: trust.ClusterTenant,
		ServicePrincipal: trust.ServicePrincipal, PublicKey: ed25519.PublicKey(trust.PublicKey)}, nil
}

func operationScope(claims operations.GrantClaims) operationstore.Scope {
	return operationstore.Scope{Issuer: claims.Issuer, ClusterTenant: claims.ClusterTenant,
		ServicePrincipal: claims.ServicePrincipal, AppTeam: claims.AppTeam, SubjectKind: claims.Subject.Kind,
		SubjectID: claims.Subject.ID, SubjectJobID: claims.Subject.JobID, ConnectionID: claims.ConnectionID}
}

func authorizeOperationEnvelope(ctx context.Context, policy Policy, token string) (operations.GrantClaims, error) {
	if err := requestAuthorityErr(ctx); err != nil {
		return operations.GrantClaims{}, err
	}
	a, ok := jobAuthorityFromContext(ctx)
	current, exists := authorityForPrincipal(policy, a.PrincipalID)
	if !ok || !exists || a != current {
		return operations.GrantClaims{}, operations.ErrInvalid
	}
	trust, err := operationTrust(policy, a.PrincipalID)
	if err != nil {
		return operations.GrantClaims{}, err
	}
	claims, err := operations.VerifyGrantClaims(token, trust, time.Now())
	if err != nil || !slices.Contains(policy.Access.Principals[a.PrincipalID].Operations, claims.Operation) {
		return operations.GrantClaims{}, operations.ErrInvalid
	}
	return claims, nil
}

// Status may use a freshly issued grant for the same logical operation. It
// cannot extend the retained attempt's execution deadline or authorize replay.
func operationRecord(ctx context.Context, store *operationstore.Store, claims operations.GrantClaims, id string) (operationstore.Record, error) {
	if err := requestAuthorityErr(ctx); err != nil {
		return operationstore.Record{}, err
	}
	value, err := store.Get(ctx, operationScope(claims), id)
	if err != nil {
		return operationstore.Record{}, err
	}
	if value.Record.RequestSHA256 != claims.RequestSHA256 || value.Record.Kind != claims.Operation {
		return operationstore.Record{}, operationstore.ErrNotFound
	}
	if err := requestAuthorityErr(ctx); err != nil {
		return operationstore.Record{}, err
	}
	return value.Record, nil
}

func operationInputIdentity(scope operationstore.Scope, authorityDigest string) exports.Identity {
	raw, _ := json.Marshal(scope)
	sum := sha256.Sum256(append([]byte("kelvo.operation.input-owner.v1\x00"), raw...))
	return exports.Identity{Owner: "operation-" + hex.EncodeToString(sum[:]), AuthorizationSHA256: authorityDigest}
}

func operationResponse(record operationstore.Record) operations.Response {
	return operations.Response{Version: operations.Version, ID: record.ID, RequestSHA256: record.RequestSHA256,
		State: record.State, Receipt: record.Receipt}
}
