// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"path/filepath"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

// Operations share query resource admission and require kernel containment.
// Results stay on the assigned worker's persistent, private volume.
type OperationNodeConfig struct {
	PrivateSources map[string]worker.PrivateOperationConfig `yaml:"private_sources,omitempty"`
	InputURL       string                                   `yaml:"input_url"`
	TLS            TLSConfig                                `yaml:"tls"`
	Adapter        worker.OperationProcessConfig            `yaml:"adapter"`
	Results        OperationInputConfig                     `yaml:"results"`
	MaxResultBytes int64                                    `yaml:"max_result_bytes"`
	MaxConcurrent  int                                      `yaml:"max_concurrent"`
	MaxDownloads   int                                      `yaml:"max_downloads"`
	PollInterval   time.Duration                            `yaml:"poll_interval"`
}

func validateNodeOperations(c NodeConfig) error {
	if c.Policy.Operations == nil && c.Operations == nil {
		return nil
	}
	o := c.Operations
	if c.Policy.Operations == nil || o == nil || c.Audit == nil || c.Resources == nil || c.Containment == nil || c.ScratchDirectory == "" || c.SandboxPath == "" ||
		!validEndpoint(o.InputURL) || o.TLS.CAFile == "" || o.TLS.CertFile == "" || o.TLS.KeyFile == "" || o.TLS.Trust != nil || o.TLS.IdentityFile != "" || o.TLS.ReloadInterval != 0 ||
		o.MaxConcurrent < 1 || o.MaxConcurrent > c.Policy.Workers[c.WorkerID] || o.MaxConcurrent > c.Resources.MaxConcurrent || o.MaxDownloads < 1 || o.MaxDownloads > 64 ||
		o.PollInterval < 10*time.Millisecond || o.PollInterval > time.Second || o.MaxResultBytes < 1024 || o.MaxResultBytes > operationinput.MaxInputBytes || o.MaxResultBytes > c.Policy.Limits.MaxBytes ||
		o.Results.MaxEntries < 1 || o.Results.MaxEntries > 4096 || o.Results.MaxStoredBytes < 2*o.MaxResultBytes || o.Results.MaxStoredBytes > 1<<40 ||
		!filepath.IsAbs(o.Results.Directory) || filepath.Clean(o.Results.Directory) != o.Results.Directory || o.Results.Directory == "/" || len(o.Results.Directory) > 4096 {
		return errOperationConfig
	}
	for _, directory := range []string{c.ScratchDirectory, c.Audit.Directory} {
		if overlappingExportPaths(o.Results.Directory, directory) {
			return errOperationConfig
		}
	}
	if c.Exports != nil && overlappingExportPaths(o.Results.Directory, c.Exports.Directory) {
		return errOperationConfig
	}
	// The admission pool accounts for both verified download buffers and parent
	// IPC/serialization overhead. Persistent result disk has a separate budget.
	if c.Resources.OverheadMB < 8 || operationDownloadMemory(o) > (c.Resources.MemoryMB-c.Resources.BaselineMB)<<20 {
		return errOperationConfig
	}
	if err := validatePrivateNodeOperations(c); err != nil {
		return err
	}
	return o.Adapter.Validate()
}

func operationDownloadMemory(c *OperationNodeConfig) int64 { return 3*c.MaxResultBytes + (2 << 20) }

func validatePrivateNodeOperations(c NodeConfig) error {
	o := c.Operations
	if o == nil {
		return nil
	}
	total := 0
	for principal, config := range o.PrivateSources {
		trust, err := operationTrust(c.Policy, principal)
		resolver, exists := c.ConnectionResolvers[trust.Issuer]
		if err != nil || !exists || resolver.Validate() != nil || config.Validate() != nil {
			return errOperationConfig
		}
		total += config.MaxSessions
	}
	if len(o.PrivateSources) > 64 || total > o.MaxConcurrent {
		return errOperationConfig
	}
	return nil
}
