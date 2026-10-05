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

// Bump when the meaning of either nondefault evolution flag changes.
const schemaEvolutionRulesVersion = 1

// SchemaEvolution permits only explicit forward changes between generations.
// An absent or all-false policy preserves the strict schema contract.
type SchemaEvolution struct {
	AddNullableColumns bool `json:"add_nullable_columns" yaml:"add_nullable_columns"`
	SafeWidening       bool `json:"safe_widening" yaml:"safe_widening"`
}

// VerificationLimits bounds one protected verification operation, including
// metadata, payload checksums and repeated footer reads across retained history.
// It does not change snapshot identity or grant an unbounded history scan.
type VerificationLimits struct {
	MaxBytes int64 `json:"max_bytes" yaml:"max_bytes"`
}

func (v VerificationLimits) Validate() error {
	if v.MaxBytes < 1 || v.MaxBytes > 1<<45 {
		return errors.New("verification max_bytes must be between 1 byte and 32 TiB")
	}
	return nil
}

type Dataset struct {
	Scan                 *SnapshotScanLimits `json:"scan,omitempty" yaml:"scan,omitempty"`
	Verification         *VerificationLimits `json:"verification,omitempty" yaml:"verification,omitempty"`
	SchemaEvolution      *SchemaEvolution    `json:"schema_evolution,omitempty" yaml:"schema_evolution,omitempty"`
	Multipart            *MultipartConfig    `json:"multipart,omitempty" yaml:"multipart,omitempty"`
	ID                   string              `json:"id" yaml:"id"`
	Query                query.Request       `json:"query" yaml:"query"`
	RefreshInterval      time.Duration       `json:"refresh_interval" yaml:"refresh_interval"`
	MaxAge               time.Duration       `json:"max_age" yaml:"max_age"`
	AuthorizationVersion string              `json:"authorization_version" yaml:"authorization_version"`
	Limits               query.Limits        `json:"limits" yaml:"limits"`
}

// EffectiveVerificationLimits deliberately supplies no default. Operators must
// opt in to an aggregate allowance before Verify or Inventory reads any objects.
func (d Dataset) EffectiveVerificationLimits() (VerificationLimits, error) {
	if d.Verification == nil {
		return VerificationLimits{}, query.NewError("CONFIGURATION_ERROR", "Protected verification requires explicit verification.max_bytes")
	}
	limits := *d.Verification
	if err := limits.Validate(); err != nil {
		return VerificationLimits{}, query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	return limits, nil
}

func (a AccelerationConfig) validateVerification(d Dataset) error {
	if d.Verification == nil {
		return nil
	}
	if a.ObjectStorage == nil || a.ObjectStorage.ReaderRegistry == nil {
		return errors.New("verification limits require protected object storage")
	}
	return d.Verification.Validate()
}

// ValidateVerification also protects programmatic constructors that do not load
// YAML or create a complete catalog authority snapshot.
func (a AccelerationConfig) ValidateVerification() error {
	for _, dataset := range a.Datasets {
		if err := a.validateVerification(dataset); err != nil {
			return err
		}
	}
	return nil
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
		if err := a.validateVerification(*d); err != nil {
			return err
		}
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
		if _, err := d.EffectiveSnapshotScanLimits(); err != nil {
			return err
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
// configuration, effective schema policy, tenant, or authorization version changes.
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
	// Keep historical strict fingerprints byte-for-byte stable. Explicit strict
	// flags and an absent policy have identical effective behavior.
	var evolution *SchemaEvolution
	var evolutionVersion int
	if d.SchemaEvolution != nil && (d.SchemaEvolution.AddNullableColumns || d.SchemaEvolution.SafeWidening) {
		normalized := *d.SchemaEvolution
		evolution = &normalized
		evolutionVersion = schemaEvolutionRulesVersion
	}
	b, err := json.Marshal(struct {
		Format                    int
		Tenant, ID, Authorization string
		Query                     query.Request
		Sources                   []Source
		ObjectStorage             *ObjectStorage   `json:"object_storage,omitempty"`
		SchemaEvolution           *SchemaEvolution `json:"schema_evolution,omitempty"`
		SchemaEvolutionVersion    int              `json:"schema_evolution_version,omitempty"`
	}{1, c.Acceleration.TenantID, d.ID, d.AuthorizationVersion, d.Query, sources, c.Acceleration.ObjectStorage, evolution, evolutionVersion})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
