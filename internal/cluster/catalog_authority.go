// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"encoding/hex"
	"errors"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

// CatalogBinding identifies approved operator definitions, not resolved secrets,
// file contents or a remote database identity. Missing bindings retain legacy
// unbound behavior. Enabling or changing one requires a drained policy cutover.
type CatalogBinding struct {
	Version int    `json:"version" yaml:"version"`
	SHA256  string `json:"sha256" yaml:"sha256"`
}

func validCatalogBinding(binding *CatalogBinding) bool {
	if binding == nil {
		return true
	}
	if binding.Version != 1 || len(binding.SHA256) != 64 || strings.ToLower(binding.SHA256) != binding.SHA256 {
		return false
	}
	_, err := hex.DecodeString(binding.SHA256)
	return err == nil
}

var errCatalogAuthority = errors.New("worker catalog does not match the approved principal catalog")

// ValidateCatalogAuthority checks definitions before the CLI opens source
// providers, snapshot backends, execution probes or broker worker ownership.
// Constructors also bind their own private execution snapshot independently.
func ValidateCatalogAuthority(policy Policy, config catalog.Config) error {
	if policy.Access == nil || policy.Access.CatalogBinding == nil {
		return nil
	}
	binding := policy.Access.CatalogBinding
	if !validCatalogBinding(binding) {
		return errCatalogAuthority
	}
	digest, err := catalog.AuthorityFingerprint(config)
	if err != nil || digest != binding.SHA256 {
		return errCatalogAuthority
	}
	return nil
}

func bindCatalogExecutor(policy Policy, executor *worker.Executor) (*worker.Executor, error) {
	if executor == nil {
		return nil, errCatalogAuthority
	}
	if policy.Access == nil || policy.Access.CatalogBinding == nil {
		return executor, nil
	}
	binding := policy.Access.CatalogBinding
	if !validCatalogBinding(binding) {
		return nil, errCatalogAuthority
	}
	bound, err := executor.WithCatalogBinding(binding.SHA256)
	if err != nil {
		return nil, errCatalogAuthority
	}
	return bound, nil
}
