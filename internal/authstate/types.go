// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package authstate retains local authentication history. It is not a shared
// revocation authority. The operator must retain the volume and protect backups.
package authstate

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
)

const MaxOwners = 8192

var (
	ErrRejected    = errors.New("authentication state admission rejected")
	ErrUnavailable = errors.New("authentication state unavailable")
	ErrUncertain   = errors.New("authentication state durability uncertain")
)

type Owner struct{ TenantID, PrincipalID string }
type Scope struct {
	ID      string
	Tenants []string
}
type Candidate struct {
	Revision       uint64
	DocumentSHA256 [32]byte
	Owners         map[[32]byte]Owner
}

func identifier(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, c := range []byte(value) {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

func copyScope(scope Scope) (Scope, error) {
	if !identifier(scope.ID, 63) || len(scope.Tenants) == 0 || len(scope.Tenants) > 256 {
		return Scope{}, ErrUnavailable
	}
	result := Scope{ID: strings.Clone(scope.ID), Tenants: make([]string, len(scope.Tenants))}
	for i, tenant := range scope.Tenants {
		if !identifier(tenant, 32) {
			return Scope{}, ErrUnavailable
		}
		result.Tenants[i] = strings.Clone(tenant)
	}
	slices.Sort(result.Tenants)
	for i := 1; i < len(result.Tenants); i++ {
		if result.Tenants[i] == result.Tenants[i-1] {
			return Scope{}, ErrUnavailable
		}
	}
	return result, nil
}

func copyCandidate(candidate Candidate, scope Scope) (Candidate, error) {
	if candidate.Revision == 0 || len(candidate.Owners) > MaxOwners {
		return Candidate{}, ErrRejected
	}
	result := Candidate{Revision: candidate.Revision, DocumentSHA256: candidate.DocumentSHA256, Owners: make(map[[32]byte]Owner, len(candidate.Owners))}
	for fingerprint, owner := range candidate.Owners {
		if !identifier(owner.TenantID, 32) || !slices.Contains(scope.Tenants, owner.TenantID) || (owner.PrincipalID != "" && !identifier(owner.PrincipalID, 32)) {
			return Candidate{}, ErrRejected
		}
		result.Owners[fingerprint] = Owner{strings.Clone(owner.TenantID), strings.Clone(owner.PrincipalID)}
	}
	return result, nil
}

func validDirectory(path string) bool {
	return len(path) <= 4096 && !strings.ContainsRune(path, 0) && filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/"
}

// advance builds a detached complete history. Retired owners are never evicted.
func advance(prior, candidate Candidate) (Candidate, bool, error) {
	if candidate.Revision < prior.Revision || (candidate.Revision == prior.Revision && candidate.DocumentSHA256 != prior.DocumentSHA256) {
		return Candidate{}, false, ErrRejected
	}
	for fingerprint, owner := range candidate.Owners {
		old, exists := prior.Owners[fingerprint]
		if (exists && old != owner) || (!exists && candidate.Revision == prior.Revision) {
			return Candidate{}, false, ErrRejected
		}
	}
	if candidate.Revision == prior.Revision {
		return prior, false, nil
	}
	next := Candidate{Revision: candidate.Revision, DocumentSHA256: candidate.DocumentSHA256, Owners: make(map[[32]byte]Owner, len(prior.Owners)+len(candidate.Owners))}
	for fingerprint, owner := range prior.Owners {
		next.Owners[fingerprint] = owner
	}
	for fingerprint, owner := range candidate.Owners {
		next.Owners[fingerprint] = owner
	}
	if len(next.Owners) > MaxOwners {
		return Candidate{}, false, ErrRejected
	}
	return next, true, nil
}
