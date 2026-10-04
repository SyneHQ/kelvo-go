// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authstate"
	"github.com/SYNEHQ/kelvo-go/internal/secrets"
)

// GatewayAuthenticationStateConfig binds restart protection to one retained,
// private directory per gateway replica. It does not coordinate replicas.
type GatewayAuthenticationStateConfig struct {
	Directory string `yaml:"directory"`
	Scope     string `yaml:"scope"`
}

var gatewayAuthStateScope = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

func validGatewayAuthStateConfig(c GatewayAuthenticationStateConfig) bool {
	return gatewayAuthStateScope.MatchString(c.Scope) && len(c.Directory) <= 4096 &&
		filepath.IsAbs(c.Directory) && filepath.Clean(c.Directory) == c.Directory &&
		c.Directory != string(filepath.Separator) && !strings.ContainsRune(c.Directory, 0)
}

type gatewayAuthenticationStore interface {
	Admit(context.Context, authstate.Candidate) error
	Close(context.Context) error
}

type gatewayAuthStateOpener func(context.Context, string, authstate.Scope) (gatewayAuthenticationStore, error)

func gatewayAuthAttemptFresh(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	deadline, bounded := ctx.Deadline()
	return !bounded || time.Now().Before(deadline)
}

func openGatewayAuthState(ctx context.Context, directory string, scope authstate.Scope) (gatewayAuthenticationStore, error) {
	store, err := authstate.Open(ctx, directory, scope)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func gatewayAuthScope(state *GatewayAuthenticationStateConfig, tenants map[string]bool) authstate.Scope {
	scope := authstate.Scope{ID: state.Scope, Tenants: make([]string, 0, len(tenants))}
	for tenant := range tenants {
		scope.Tenants = append(scope.Tenants, tenant)
	}
	slices.Sort(scope.Tenants)
	return scope
}

func gatewayAuthCandidate(set gatewayKeySet) authstate.Candidate {
	candidate := authstate.Candidate{Revision: set.revision, DocumentSHA256: set.digest, Owners: make(map[[32]byte]authstate.Owner, len(set.keys))}
	for hash, tenant := range set.keys {
		candidate.Owners[hash] = authstate.Owner{TenantID: tenant, PrincipalID: set.principals[hash]}
	}
	return candidate
}

// InitializeGatewayAuthState seeds a new state directory without opening any
// broker, database, listener, TLS identity or query engine. Existing state is
// never overwritten. Callers normally obtain cfg through LoadGateway.
func InitializeGatewayAuthState(ctx context.Context, cfg GatewayConfig) error {
	if ctx == nil || validateGatewayAuthentication(&cfg) != nil || cfg.Authentication == nil || cfg.Authentication.State == nil {
		return errGatewayAuthConfig
	}
	tenants := make(map[string]bool, len(cfg.Tenants))
	for _, tenant := range cfg.Tenants {
		id := tenant.Policy.TenantID
		if ValidatePolicy(tenant.Policy) != nil || tenants[id] {
			return errGatewayAuthConfig
		}
		tenants[id] = true
	}
	// One worker owns the potentially uninterruptible private-file read. If the
	// caller leaves, its expired context prevents a later initialization attempt.
	result := make(chan error, 1)
	go func() {
		raw, err := secrets.ReadPrivateDocument(ctx, cfg.Authentication.KeysFile, gatewayKeyFileLimit)
		var set gatewayKeySet
		if err == nil {
			set, err = parseGatewayKeys(raw, tenants, cfg.Authentication.MinRevision)
		}
		clear(raw)
		if err == nil && gatewayAuthAttemptFresh(ctx) {
			err = authstate.Initialize(ctx, cfg.Authentication.State.Directory, gatewayAuthScope(cfg.Authentication.State, tenants), gatewayAuthCandidate(set))
		}
		if err != nil || !gatewayAuthAttemptFresh(ctx) {
			result <- errGatewayAuthUnavailable
			return
		}
		result <- nil
	}()
	select {
	case err := <-result:
		if !gatewayAuthAttemptFresh(ctx) {
			return errGatewayAuthUnavailable
		}
		return err
	case <-ctx.Done():
		return errGatewayAuthUnavailable
	}
}

// Only this loader owns state opening and admission. No filesystem operation
// runs under the authority mutex or on the independent expiration loop.
func (a *gatewayAuthenticator) admitState(ctx context.Context, set gatewayKeySet) error {
	a.mu.Lock()
	store := a.state
	valid := store != nil && gatewayAuthAttemptFresh(ctx) && a.validCandidateLocked(set)
	if !valid {
		a.mu.Unlock()
		a.invalidate()
		return authstate.ErrRejected
	}
	// Rotation revokes removed keys before persistence, but overlapping keys
	// retain their existing deadline and cancellation context until acceptance.
	removed := make(map[[32]byte]*gatewayAuthKey)
	for hash, key := range a.keys {
		if set.keys[hash] != key.tenant || set.principals[hash] != key.principal {
			removed[hash] = key
			delete(a.keys, hash)
		}
	}
	a.admitting = true
	a.mu.Unlock()
	cancelGatewayKeys(removed)
	err := store.Admit(ctx, gatewayAuthCandidate(set))
	a.mu.Lock()
	a.admitting = false
	if !gatewayAuthAttemptFresh(ctx) || (err != nil && !errors.Is(err, authstate.ErrRejected)) {
		a.poisoned = true
	}
	poisoned := a.poisoned
	var revoked map[[32]byte]*gatewayAuthKey
	if poisoned || err != nil {
		revoked, a.keys, a.validUntil = a.keys, nil, time.Time{}
	}
	a.mu.Unlock()
	cancelGatewayKeys(revoked)
	if poisoned {
		return authstate.ErrUncertain
	}
	return err
}
