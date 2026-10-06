// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"maps"
	"slices"

	"github.com/SYNEHQ/kelvo-go/internal/access"
)

// clonePolicy detaches the complete mutable policy graph without normalizing
// nil maps or slices. Constructors validate and retain this same owned copy.
// Callers must not mutate input concurrently with construction.
func clonePolicy(p Policy) (Policy, error) {
	p.Workers = maps.Clone(p.Workers)
	p.SourceQuotas = maps.Clone(p.SourceQuotas)
	if p.Operations != nil {
		operations := *p.Operations
		p.Operations = &operations
	}
	if p.Exports != nil {
		exports := *p.Exports
		p.Exports = &exports
	}
	if p.Access == nil {
		return p, nil
	}
	principal := *p.Access
	p.Access = &principal
	principal.Principals = maps.Clone(principal.Principals)
	if principal.CatalogBinding != nil {
		binding := *principal.CatalogBinding
		principal.CatalogBinding = &binding
	}
	if principal.KeyAuthority != nil {
		binding := *principal.KeyAuthority
		principal.KeyAuthority = &binding
	}
	for id, grant := range principal.Principals {
		grant.Operations = slices.Clone(grant.Operations)
		if grant.DelegatedResolver != nil {
			resolver := *grant.DelegatedResolver
			grant.DelegatedResolver = &resolver
		}
		grant.NativeSources = slices.Clone(grant.NativeSources)
		grant.FederatedSources = slices.Clone(grant.FederatedSources)
		if grant.RowColumnPolicy != nil {
			// Validate bounds before recursively copying a caller's row graph.
			// Clone also detaches source/table maps, columns and every predicate.
			rows, err := access.Clone(*grant.RowColumnPolicy)
			if err != nil {
				return Policy{}, err
			}
			grant.RowColumnPolicy = &rows
		}
		principal.Principals[id] = grant
	}
	return p, nil
}
