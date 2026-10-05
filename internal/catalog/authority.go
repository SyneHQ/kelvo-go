// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

const authorityDomain = "kelvo/catalog-authority/v1\n"

// AuthorityFingerprint identifies operator definitions and credential references,
// never resolved secrets, file contents, or the identity of a remote database.
func AuthorityFingerprint(c Config) (string, error) {
	_, digest, err := AuthoritySnapshot(c)
	return digest, err
}

// AuthoritySnapshot bounds and detaches operator definitions without opening
// files, resolving credentials, or invoking adapter code. Runtime capabilities
// cannot be pinned as operator definitions. Source-specific validation remains
// the catalog loader's responsibility.
//
// The digest sorts unordered definition lists; the returned execution copy keeps
// their original order and representation. In particular, this must not change
// DatasetFingerprint or invalidate snapshots produced from the original catalog.
func AuthoritySnapshot(c Config) (Config, string, error) {
	bad := errors.New("invalid or oversized catalog authority definition")
	if !boundedAuthority(c) {
		return Config{}, "", bad
	}
	raw, err := json.Marshal(c)
	if err != nil || len(raw) > maxConfigBytes {
		return Config{}, "", bad
	}
	var detached Config
	if json.Unmarshal(raw, &detached) != nil {
		return Config{}, "", bad
	}
	canonical := detached
	canonical.Sources = slices.Clone(detached.Sources)
	if canonical.Sources == nil {
		canonical.Sources = []Source{}
	}
	for i := range canonical.Sources {
		s := &canonical.Sources[i]
		s.Type = CanonicalType(s.Type)
		if s.Federation != nil {
			f := *s.Federation
			f.Tables = slices.Clone(f.Tables)
			slices.SortFunc(f.Tables, func(a, b FederationTable) int { return strings.Compare(a.Name, b.Name) })
			s.Federation = &f
		}
	}
	slices.SortFunc(canonical.Sources, func(a, b Source) int { return strings.Compare(a.ID, b.ID) })
	if detached.Acceleration != nil {
		a := *detached.Acceleration
		a.Datasets = slices.Clone(a.Datasets)
		slices.SortFunc(a.Datasets, func(a, b Dataset) int { return strings.Compare(a.ID, b.ID) })
		canonical.Acceleration = &a
	}
	raw, err = json.Marshal(canonical)
	if err != nil || len(raw) > maxConfigBytes {
		return Config{}, "", bad
	}
	h := sha256.New()
	_, _ = h.Write([]byte(authorityDomain))
	_, _ = h.Write(raw)
	return detached, hex.EncodeToString(h.Sum(nil)), nil
}

// Reject unbounded input before JSON allocates or traverses it. Cardinality and
// aggregate text limits bound the encoder's escaping/structural overhead too;
// the final encoded document has its own 1 MiB limit.
func boundedAuthority(c Config) bool {
	budget := authorityBudget{}
	if len(c.Sources) > 256 || !budget.path(c.ExtensionDirectory) {
		return false
	}
	names := make(map[string]bool, len(c.Sources))
	for _, s := range c.Sources {
		if !authorityName(names, s.ID) || !budget.text(s.ID, 63) || !budget.text(s.Type, 63) || s.Type == "" || s.Type == "accelerated" ||
			!budget.text(s.Adapter, 63) || !budget.path(s.Path) || s.LocalSnapshot != nil || s.ObjectSnapshot != nil ||
			s.Object != nil || s.Range != nil || s.Ranges != nil || s.ParquetPaths != nil || len(s.Options) > 16 {
			return false
		}
		for _, ref := range []string{s.DSNEnv, s.URLEnv, s.UsernameEnv, s.PasswordEnv, s.TokenEnv} {
			if !budget.reference(ref) {
				return false
			}
		}
		for key, value := range s.Options {
			if key == "" || !budget.text(key, 64) || !budget.text(value, 4096) {
				return false
			}
		}
		if f := s.Federation; f != nil {
			if len(f.Tables) == 0 || len(f.Tables) > 32 {
				return false
			}
			tables := make(map[string]bool, len(f.Tables))
			for _, table := range f.Tables {
				if !authorityName(tables, table.Name) || !budget.text(table.Name, 63) || !budget.text(table.Database, 128) ||
					!budget.text(table.Schema, 63) || !budget.text(table.Table, 63) || !ValidID(table.Table) {
					return false
				}
			}
		}
	}
	a := c.Acceleration
	if a == nil {
		return true
	}
	if len(a.Datasets) == 0 || len(a.Datasets) > 64 || !budget.path(a.Directory) || a.Directory == "" ||
		!budget.text(a.TenantID, 32) || !tenantName.MatchString(a.TenantID) {
		return false
	}
	if storage := a.ObjectStorage; storage != nil {
		if !budget.text(storage.Provider, 16) || !budget.text(storage.Endpoint, 512) ||
			!budget.text(storage.Bucket, 63) || !budget.text(storage.Prefix, 256) ||
			!budget.text(storage.Region, 64) || !budget.text(storage.Account, 24) {
			return false
		}
		credentials := []ObjectCredentials{storage.ReadCredentials, storage.WriteCredentials}
		if storage.ReaderRegistry != nil {
			credentials = append(credentials, storage.ReaderRegistry.Credentials)
		}
		for _, refs := range credentials {
			for _, ref := range refs.EnvironmentNames() {
				if !budget.reference(ref) {
					return false
				}
			}
		}
		if storage.Validate() != nil {
			return false
		}
	}
	for _, d := range a.Datasets {
		if a.validateVerification(d) != nil || !authorityName(names, d.ID) || !budget.text(d.ID, 63) || !budget.text(d.AuthorizationVersion, 128) ||
			!budget.text(d.Limits.ResultCompression, 16) || !budget.request(d.Query) {
			return false
		}
	}
	return true
}

type authorityBudget struct{ textBytes int }

func (b *authorityBudget) text(value string, maximum int) bool {
	if len(value) > maximum || len(value) > maxConfigBytes-b.textBytes || !utf8.ValidString(value) {
		return false
	}
	b.textBytes += len(value)
	return true
}

func (b *authorityBudget) path(value string) bool {
	return b.text(value, 4096) && (value == "" || (filepath.IsAbs(value) && filepath.Clean(value) == value && !strings.ContainsRune(value, '\x00')))
}

func (b *authorityBudget) reference(value string) bool {
	return b.text(value, 256) && (value == "" || ValidateEnvironment(value) == nil)
}

func (b *authorityBudget) request(r query.Request) bool {
	if len(r.Sources) > 64 || len(r.Parameters) != 0 || !b.text(r.SQL, 64<<10) ||
		!b.text(r.Mode, 16) || !b.text(r.ConnectionID, 63) {
		return false
	}
	names := make(map[string]bool, len(r.Sources))
	for _, id := range r.Sources {
		if !authorityName(names, id) || !b.text(id, 63) {
			return false
		}
	}
	if r.Mongo != nil {
		if len(r.Mongo.Pipeline) > 128 || !b.text(r.Mongo.Collection, 120) {
			return false
		}
		for _, stage := range r.Mongo.Pipeline {
			if len(stage) > 128<<10 || !b.text(string(stage), 128<<10) {
				return false
			}
		}
	}
	return true
}

func authorityName(seen map[string]bool, name string) bool {
	if !ValidID(name) {
		return false
	}
	folded := strings.ToLower(name)
	if seen[folded] {
		return false
	}
	seen[folded] = true
	return true
}
