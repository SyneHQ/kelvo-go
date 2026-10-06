// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"errors"

	"github.com/SYNEHQ/kelvo-go/internal/cluster"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func configureConnectionResolvers(cfg cluster.NodeConfig, executor *worker.Executor) (*worker.Executor, func(), error) {
	bad := errors.New("on-demand connection resolvers must match the provisioned principal policy")
	allowed := make(map[string]string)
	if cfg.Policy.Access != nil {
		for _, grant := range cfg.Policy.Access.Principals {
			if trust := grant.DelegatedResolver; trust != nil {
				if previous, exists := allowed[trust.Issuer]; exists && previous != trust.URL {
					return nil, func() {}, bad
				}
				allowed[trust.Issuer] = trust.URL
			}
		}
	}
	if len(allowed) != len(cfg.ConnectionResolvers) {
		return nil, func() {}, bad
	}
	resolvers := make(map[string]*worker.ConnectionResolver, len(allowed))
	closeAll := func() {
		for _, resolver := range resolvers {
			resolver.Close()
		}
	}
	for issuer, config := range cfg.ConnectionResolvers {
		if expected, exists := allowed[issuer]; !exists || expected != config.URL {
			closeAll()
			return nil, func() {}, bad
		}
		resolver, err := worker.NewConnectionResolver(config, cluster.WorkerIdentity(cfg.Policy.TenantID, cfg.WorkerID))
		if err != nil {
			closeAll()
			return nil, func() {}, err
		}
		resolvers[issuer] = resolver
	}
	bound, err := executor.WithConnectionResolvers(resolvers)
	if err != nil {
		closeAll()
		return nil, func() {}, err
	}
	return bound, closeAll, nil
}
