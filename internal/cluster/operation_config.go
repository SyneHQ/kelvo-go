// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
)

// OperationPolicy partitions retained receipts independently of reusable query
// slots. Changing it requires a drained, explicitly reprovisioned tenant.
type OperationPolicy struct {
	Shards           int           `json:"shards" yaml:"shards"`
	SlotsPerShard    int           `json:"slots_per_shard" yaml:"slots_per_shard"`
	Retention        time.Duration `json:"retention" yaml:"retention"`
	ExecutionTimeout time.Duration `json:"execution_timeout" yaml:"execution_timeout"`
}

type OperationInputConfig struct {
	Directory      string `yaml:"directory"`
	MaxEntries     int    `yaml:"max_entries"`
	MaxStoredBytes int64  `yaml:"max_stored_bytes"`
}

// Inputs live at a private gateway root. Workers fetch exact sealed references
// over the separate mTLS listener. No database credentials are stored here.
type GatewayOperationConfig struct {
	Listen string                          `yaml:"listen"`
	TLS    TLSConfig                       `yaml:"tls"`
	Inputs map[string]OperationInputConfig `yaml:"inputs"`
}

var errOperationConfig = errors.New("operations require explicit retained capacity, principal grants, private input storage and audit")

func operationStorePolicy(p Policy) operationstore.Policy {
	if p.Operations == nil {
		return operationstore.Policy{}
	}
	o := p.Operations
	return operationstore.Policy{Namespace: "gateway", TenantID: p.TenantID, Shards: o.Shards,
		SlotsPerShard: o.SlotsPerShard, Retention: o.Retention, ExecutionTimeout: o.ExecutionTimeout,
		LeaseDuration: p.LeaseDuration, StorageTimeout: 5 * time.Second, MaxCASAttempts: 8}
}

func validateOperationPolicy(p Policy) error {
	if p.Operations == nil {
		if p.Access != nil {
			for _, grant := range p.Access.Principals {
				if len(grant.Operations) != 0 {
					return errOperationConfig
				}
			}
		}
		return nil
	}
	if p.Access == nil || operationStorePolicy(p).Validate() != nil || p.Operations.Shards > 256 || p.Operations.ExecutionTimeout > 5*time.Minute {
		return errOperationConfig
	}
	return nil
}

func validateGatewayOperations(c GatewayConfig) error {
	enabled := 0
	for _, tenant := range c.Tenants {
		if tenant.Policy.Operations != nil {
			enabled++
		}
	}
	if enabled == 0 && c.Operations == nil {
		return nil
	}
	o := c.Operations
	if enabled == 0 || o == nil || c.Authentication == nil || c.Audit == nil || o.Listen == "" || len(o.Inputs) != enabled || o.TLS.CAFile == "" || o.TLS.CertFile == "" || o.TLS.KeyFile == "" || o.TLS.Trust != nil || o.TLS.IdentityFile != "" || o.TLS.ReloadInterval != 0 {
		return errOperationConfig
	}
	var directories []string
	for _, tenant := range c.Tenants {
		input, ok := o.Inputs[tenant.Policy.TenantID]
		if tenant.Policy.Operations == nil {
			if ok {
				return errOperationConfig
			}
			continue
		}
		if !ok || validateOperationPolicy(tenant.Policy) != nil || !filepath.IsAbs(input.Directory) || filepath.Clean(input.Directory) != input.Directory || input.Directory == "/" || len(input.Directory) > 4096 || input.MaxEntries < 1 || input.MaxEntries > 4096 || input.MaxStoredBytes < 1 || input.MaxStoredBytes > 1<<40 {
			return errOperationConfig
		}
		if overlappingExportPaths(input.Directory, c.Audit.Directory) {
			return errOperationConfig
		}
		for _, directory := range directories {
			if overlappingExportPaths(input.Directory, directory) {
				return errOperationConfig
			}
		}
		directories = append(directories, input.Directory)
	}
	return nil
}

func (s *NATSStore) OpenOperations(ctx context.Context, initialize bool) (*operationstore.Store, error) {
	if s == nil || s.policy.Operations == nil {
		return nil, errOperationConfig
	}
	return operationstore.OpenNATS(ctx, s.js, operationStorePolicy(s.policy), s.policy.Replicas, s.nc.MaxPayload(), initialize)
}
