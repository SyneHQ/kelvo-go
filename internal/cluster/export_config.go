// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"errors"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

type GatewayExportConfig struct {
	MaxSupervisors int `yaml:"max_supervisors"`
	MaxDownloads   int `yaml:"max_downloads"`
}

type ExportNodeConfig struct {
	Directory          string        `yaml:"directory"`
	MaxEntries         int           `yaml:"max_entries"`
	MaxStoredBytes     int64         `yaml:"max_stored_bytes"`
	MaxConcurrent      int           `yaml:"max_concurrent"`
	MaxDownloads       int           `yaml:"max_downloads"`
	DownloadMemoryMB   int64         `yaml:"download_memory_mb"`
	CleanupInterval    time.Duration `yaml:"cleanup_interval"`
	CleanupMaxRemovals int           `yaml:"cleanup_max_removals"`
}

var exportAuthorizationVersion = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func validateExportPolicy(p Policy) error {
	e := p.Exports
	if e == nil {
		return nil
	}
	bad := errors.New("invalid tenant export policy")
	if p.Access == nil || p.Limits.Validate() != nil || p.LeaseDuration < 5*time.Second || p.LeaseDuration > time.Minute ||
		!exportAuthorizationVersion.MatchString(e.AuthorizationVersion) ||
		e.MaxJobs < 1 || e.MaxJobs > 512 || e.QueueTimeout < time.Second || e.QueueTimeout > time.Hour ||
		e.DefaultTTL < e.QueueTimeout+p.Limits.Timeout+2*p.LeaseDuration || e.DefaultTTL > e.MaxTTL ||
		e.MaxTTL > 30*24*time.Hour || e.Limits.Validate() != nil ||
		e.Limits.MaxRows > p.Limits.MaxRows || e.Limits.MaxDecodedBytes > p.Limits.MaxBytes || e.Limits.MaxEncodedBytes > p.Limits.MaxBytes {
		return bad
	}
	return nil
}

func validateGatewayExports(c GatewayConfig) error {
	enabled := false
	for _, tenant := range c.Tenants {
		if tenant.Policy.Exports != nil {
			enabled = true
			if tenant.Policy.Access == nil || validateExportPolicy(tenant.Policy) != nil {
				return errors.New("exports require a valid principal policy")
			}
		}
	}
	if !enabled && c.Exports == nil {
		return nil
	}
	if !enabled || c.Exports == nil || c.Authentication == nil || c.Exports.MaxSupervisors < 1 || c.Exports.MaxSupervisors > 4096 || c.Exports.MaxDownloads < 1 || c.Exports.MaxDownloads > 4096 {
		return errors.New("exports require principal file authentication and explicit gateway capacity")
	}
	return nil
}

// These are conservative admission reservations, not an RSS bound. Include
// encoded and decoded verification buffers plus schema/metadata headroom;
// worker containment and host headroom remain independent requirements.
func exportBufferMemoryMB(p Policy) int64 {
	l := p.Exports.Limits
	bytes := 4*(l.MaxPartBytes+l.MaxPartDecodedBytes) + (8 << 20)
	return (bytes + (1 << 20) - 1) >> 20
}

func validateNodeExports(c NodeConfig) error {
	if c.Policy.Exports == nil && c.Exports == nil {
		if c.Resources != nil && c.Resources.Export != nil {
			return errors.New("export resource capacity requires an export policy")
		}
		return nil
	}
	if c.Exports == nil || c.Policy.Exports == nil || validateExportPolicy(c.Policy) != nil || c.Resources == nil || !c.Resources.FitsExport(c.Policy.Limits) || c.SandboxPath == "" || c.ScratchDirectory == "" {
		return errors.New("exports require principal policy, sandbox, managed scratch and explicit resources")
	}
	e := c.Exports
	if !filepath.IsAbs(e.Directory) || filepath.Clean(e.Directory) != e.Directory || e.Directory == string(filepath.Separator) || len(e.Directory) > 4096 ||
		overlappingExportPaths(e.Directory, c.ScratchDirectory) || e.MaxEntries < 1 || e.MaxEntries > 4096 ||
		e.MaxStoredBytes < 1 || e.MaxStoredBytes > 1<<50 || e.MaxConcurrent < 1 || e.MaxConcurrent > c.Resources.Export.MaxConcurrent ||
		e.MaxDownloads < 1 || e.MaxDownloads > 64 || e.CleanupInterval < time.Second || e.CleanupInterval > time.Hour ||
		e.CleanupMaxRemovals < 1 || e.CleanupMaxRemovals > 256 || e.DownloadMemoryMB < exportBufferMemoryMB(c.Policy) ||
		e.DownloadMemoryMB > c.Resources.Export.MemoryMB || e.DownloadMemoryMB > c.Resources.MemoryMB-c.Resources.BaselineMB-c.Resources.QueryReserveMemoryMB ||
		c.Resources.OverheadMB < exportBufferMemoryMB(c.Policy) {
		return errors.New("invalid export storage, cleanup or verification budgets")
	}
	if c.Audit != nil && overlappingExportPaths(e.Directory, c.Audit.Directory) {
		return errors.New("export and audit directories must be disjoint")
	}
	return nil
}

func overlappingExportPaths(a, b string) bool {
	a, b = filepath.Clean(a), filepath.Clean(b)
	separator := string(filepath.Separator)
	return a == b || strings.HasPrefix(a, strings.TrimSuffix(b, separator)+separator) || strings.HasPrefix(b, strings.TrimSuffix(a, separator)+separator)
}

// ValidateExportCatalog runs before opening either persistent store. Keeping
// their roots disjoint prevents export cleanup and snapshot maintenance from
// sharing custody of the same files, including object-store metadata caches.
func ValidateExportCatalog(c NodeConfig, catalogue catalog.Config) error {
	if c.Exports != nil && catalogue.Acceleration != nil && overlappingExportPaths(c.Exports.Directory, catalogue.Acceleration.Directory) {
		return errors.New("export and acceleration directories must be disjoint")
	}
	return nil
}
