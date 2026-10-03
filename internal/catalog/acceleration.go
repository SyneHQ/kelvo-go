// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"regexp"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Accelerations belong to one operator-provisioned tenant. Query callers cannot
// supply paths, refresh SQL, source credentials, or a different tenant identity.
type AccelerationConfig struct {
	Directory     string         `json:"directory" yaml:"directory"`
	TenantID      string         `json:"tenant_id" yaml:"tenant_id"`
	Datasets      []Dataset      `json:"datasets" yaml:"datasets"`
	ObjectStorage *ObjectStorage `json:"object_storage,omitempty" yaml:"object_storage,omitempty"`
}

// MultipartConfig bounds immutable snapshot parts and their encoded size.
// Limits.MaxBytes continues to bound the complete snapshot.
type MultipartConfig struct {
	MaxPartBytes int64 `json:"max_part_bytes" yaml:"max_part_bytes"`
	MaxParts     int   `json:"max_parts" yaml:"max_parts"`
}

type Dataset struct {
	Multipart            *MultipartConfig `json:"multipart,omitempty" yaml:"multipart,omitempty"`
	ID                   string           `json:"id" yaml:"id"`
	Query                query.Request    `json:"query" yaml:"query"`
	RefreshInterval      time.Duration    `json:"refresh_interval" yaml:"refresh_interval"`
	MaxAge               time.Duration    `json:"max_age" yaml:"max_age"`
	AuthorizationVersion string           `json:"authorization_version" yaml:"authorization_version"`
	Limits               query.Limits     `json:"limits" yaml:"limits"`
}

var tenantName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

func (c Config) accelerationReferences(id string) bool {
	if c.Acceleration == nil {
		return false
	}
	for _, d := range c.Acceleration.Datasets {
		if d.Query.Mode == "native" && d.Query.ConnectionID == id {
			return true
		}
		for _, source := range d.Query.Sources {
			if source == id {
				return true
			}
		}
	}
	return false
}

func (c Config) Dataset(id string) (Dataset, bool) {
	if c.Acceleration != nil {
		for _, d := range c.Acceleration.Datasets {
			if d.ID == id {
				return d, true
			}
		}
	}
	return Dataset{}, false
}

func (c *Config) validateAcceleration(base string) error {
	a := c.Acceleration
	if a == nil {
		return nil
	}
	if !tenantName.MatchString(a.TenantID) || a.Directory == "" || len(a.Datasets) == 0 || len(a.Datasets) > 64 {
		return errors.New("acceleration requires a tenant, directory and 1 to 64 datasets")
	}
	if !filepath.IsAbs(a.Directory) {
		a.Directory = filepath.Join(base, a.Directory)
	}
	a.Directory = filepath.Clean(a.Directory)
	if a.ObjectStorage != nil {
		if err := a.ObjectStorage.Validate(); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, s := range c.Sources {
		seen[s.ID] = true
	}
	for i := range a.Datasets {
		d := &a.Datasets[i]
		if !ValidID(d.ID) || seen[d.ID] {
			return errors.New("dataset IDs must be unique source identifiers")
		}
		seen[d.ID] = true
		if d.Query.Mode == "" {
			d.Query.Mode = "federated"
		}
		if err := query.ValidateRequest(d.Query); err != nil {
			return errors.New("invalid dataset refresh query")
		}
		if len(d.Query.Parameters) != 0 {
			return errors.New("dataset refresh queries cannot contain unbound parameters")
		}
		if d.MaxAge <= 0 || d.MaxAge > 30*24*time.Hour || d.RefreshInterval < 0 || (d.RefreshInterval > 0 && (d.RefreshInterval < 5*time.Second || d.RefreshInterval > d.MaxAge)) {
			return errors.New("dataset max_age must be positive; refresh_interval must be zero or between 5s and max_age")
		}
		if d.AuthorizationVersion == "" || len(d.AuthorizationVersion) > 128 {
			return errors.New("dataset authorization_version is required and must not exceed 128 characters")
		}
		defaults := query.DefaultLimits()
		if d.Limits.MaxRows == 0 {
			d.Limits.MaxRows = defaults.MaxRows
		}
		if d.Limits.MaxBytes == 0 {
			d.Limits.MaxBytes = defaults.MaxBytes
		}
		if d.Limits.Timeout == 0 {
			d.Limits.Timeout = defaults.Timeout
		}
		if d.Limits.MemoryMB == 0 {
			d.Limits.MemoryMB = defaults.MemoryMB
		}
		if d.Limits.Threads == 0 {
			d.Limits.Threads = defaults.Threads
		}
		if d.Limits.MaxTempMB == 0 {
			d.Limits.MaxTempMB = defaults.MaxTempMB
		}
		if err := d.Limits.Validate(); err != nil {
			return errors.New("invalid dataset refresh resource limits")
		}
		if d.Multipart != nil {
			if d.Multipart.MaxParts < 2 || d.Multipart.MaxParts > 256 || d.Multipart.MaxPartBytes < 1<<20 || d.Multipart.MaxPartBytes > 4<<30 || d.Multipart.MaxPartBytes > d.Limits.MaxBytes {
				return errors.New("invalid multipart snapshot limits")
			}
		}
		if a.ObjectStorage != nil && d.Multipart == nil && d.Limits.MaxBytes > 4<<30 {
			return errors.New("single-object snapshots require max_bytes at most 4 GiB")
		}
		// Refresh inputs must be real registered sources. Disallow dependencies on
		// other accelerated datasets so freshness and permissions stay explicit.
		baseCatalog := Config{Sources: c.Sources}
		ids := d.Query.Sources
		if d.Query.Mode == "native" {
			ids = []string{d.Query.ConnectionID}
		}
		selected, err := baseCatalog.Select(ids)
		if err != nil {
			return errors.New("dataset refresh names an unavailable source")
		}
		if d.Query.Mongo != nil && (len(selected) != 1 || selected[0].Type != "mongodb" || selected[0].Adapter != "") {
			return errors.New("pipeline refreshes require a built-in MongoDB source")
		}
	}
	return nil
}

// Fingerprints invalidate old snapshots when the registered query, source
// configuration, tenant, or operator-controlled authorization version changes.
// Secrets never enter the manifest. Secret/grant changes require a version bump.
func (c Config) DatasetFingerprint(id string) (string, error) {
	d, ok := c.Dataset(id)
	if !ok {
		return "", errors.New("unknown accelerated dataset")
	}
	ids := d.Query.Sources
	if d.Query.Mode == "native" {
		ids = []string{d.Query.ConnectionID}
	}
	sources, err := (Config{Sources: c.Sources}).Select(ids)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(struct {
		Format                    int
		Tenant, ID, Authorization string
		Query                     query.Request
		Sources                   []Source
		ObjectStorage             *ObjectStorage `json:"object_storage,omitempty"`
	}{1, c.Acceleration.TenantID, d.ID, d.AuthorizationVersion, d.Query, sources, c.Acceleration.ObjectStorage})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
