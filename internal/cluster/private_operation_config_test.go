//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func TestPrivateNodeOperationsRequireProvisionedPrincipalAndBoundedSessions(t *testing.T) {
	for _, mode := range []string{"disabled", "valid", "unknown-principal", "missing-resolver", "sessions", "proof", "resolver-config"} {
		t.Run(mode, func(t *testing.T) {
			f := newOperationHTTPFixture(t)
			trust := f.policy.Access.Principals["api"].DelegatedResolver
			private := worker.PrivateOperationConfig{RouteID: "rabbit-prod", ProxyAddress: "proxy.test:14443", ProxyServerName: "proxy.test", ProxyCAFile: "/pki/ca.pem", ProofPublicKey: trust.PublicKey, TicketPublicKey: trust.PublicKey, MaxSessions: 1, MaxDataConnections: 2, MaxDataPerSession: 2}
			c := NodeConfig{Policy: f.policy, Operations: &OperationNodeConfig{MaxConcurrent: 1, PrivateSources: map[string]worker.PrivateOperationConfig{"api": private}}, ConnectionResolvers: map[string]worker.ConnectionResolverConfig{trust.Issuer: {URL: trust.URL, CAFile: "/pki/ca.pem", CertFile: "/pki/worker.pem", KeyFile: "/pki/worker.key", Timeout: time.Second, MaxConcurrent: 1}}}
			switch mode {
			case "disabled":
				c.Operations.PrivateSources = nil
			case "unknown-principal":
				delete(c.Operations.PrivateSources, "api")
				c.Operations.PrivateSources["other"] = private
			case "missing-resolver":
				clear(c.ConnectionResolvers)
			case "sessions":
				private.MaxSessions = 2
				c.Operations.PrivateSources["api"] = private
			case "proof":
				private.ProofPublicKey = "bad"
				c.Operations.PrivateSources["api"] = private
			case "resolver-config":
				r := c.ConnectionResolvers[trust.Issuer]
				r.MaxConcurrent = 0
				c.ConnectionResolvers[trust.Issuer] = r
			}
			err := validatePrivateNodeOperations(c)
			if (err == nil) != (mode == "disabled" || mode == "valid") {
				t.Fatal("incorrect private startup validation", mode, err)
			}
		})
	}
}
