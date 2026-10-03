// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package exports stores bounded, immutable Arrow results for trusted callers.
// It supplies no HTTP endpoint, job execution, or global authorization service.
package exports

import (
	"crypto/subtle"
	"errors"
	"regexp"
	"time"
)

var (
	ErrInvalid              = errors.New("invalid export request")
	ErrCorrupt              = errors.New("invalid export storage")
	ErrLimit                = errors.New("export resource limit exceeded")
	ErrUnavailable          = errors.New("export unavailable")
	ErrFenced               = errors.New("export writer no longer owns publication")
	ErrBusy                 = errors.New("export storage has active leases")
	ErrClosed               = errors.New("export storage closed")
	ErrSchemaUnbound        = errors.New("export schema is not bound")
	ErrPublicationUncertain = errors.New("export publication durability uncertain; preserve and inspect storage")
	ErrUnsupported          = errors.New("export storage requires Linux")
)

const (
	manifestLimit       = 128 << 10
	stateLimit          = 8 << 10
	schemaLimit         = 1 << 20
	metadataReservation = manifestLimit + schemaLimit + 3*stateLimit
	maximumEntries      = 4096
	maximumParts        = 256
)

var (
	idPattern     = regexp.MustCompile(`^[0-9a-f]{32}$`)
	digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	tenantPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
)

// Config is operator configuration and is persisted at the private root.
// Concurrent processes must use identical limits. Storage counts conservative
// logical-file reservations, not filesystem blocks or whole-process memory.
type Config struct {
	Directory      string        `yaml:"-"`
	Tenant         string        `yaml:"tenant"`
	MaxEntries     int           `yaml:"max_entries"`
	MaxStoredBytes int64         `yaml:"max_stored_bytes"`
	MaxTTL         time.Duration `yaml:"max_ttl"`
}

// Identity must come from current trusted authorization. A matching digest is
// a comparison, not proof that the caller still has globally current access.
type Identity struct {
	Owner               string `yaml:"owner"`
	AuthorizationSHA256 string `yaml:"authorization_sha256"`
}

func (a Identity) valid() bool {
	if len(a.Owner) < 1 || len(a.Owner) > 128 || !digestPattern.MatchString(a.AuthorizationSHA256) {
		return false
	}
	for _, c := range []byte(a.Owner) {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}
func (a Identity) equal(b Identity) bool {
	return subtle.ConstantTimeCompare([]byte(a.Owner), []byte(b.Owner)) == 1 && subtle.ConstantTimeCompare([]byte(a.AuthorizationSHA256), []byte(b.AuthorizationSHA256)) == 1
}

type Limits struct {
	MaxRows             int64  `yaml:"max_rows"`
	MaxEncodedBytes     int64  `yaml:"max_encoded_bytes"`
	MaxDecodedBytes     int64  `yaml:"max_decoded_bytes"`
	MaxPartBytes        int64  `yaml:"max_part_bytes"`
	MaxPartDecodedBytes int64  `yaml:"max_part_decoded_bytes"`
	MaxParts            int    `yaml:"max_parts"`
	Compression         string `yaml:"compression"`
}

func (l Limits) valid() bool {
	return l.MaxRows > 0 && l.MaxRows <= 1<<50 && l.MaxEncodedBytes > 0 && l.MaxEncodedBytes <= 1<<40 &&
		l.MaxDecodedBytes > 0 && l.MaxDecodedBytes <= 1<<40 && l.MaxPartBytes > 0 && l.MaxPartBytes <= 256<<20 && l.MaxPartBytes <= l.MaxEncodedBytes &&
		l.MaxPartDecodedBytes > 0 && l.MaxPartDecodedBytes <= 256<<20 && l.MaxPartDecodedBytes <= l.MaxDecodedBytes &&
		l.MaxParts > 0 && l.MaxParts <= maximumParts && (l.Compression == "" || l.Compression == "none" || l.Compression == "lz4_frame")
}

type Request struct {
	Identity  Identity
	ExpiresAt time.Time
	Limits    Limits
}

type PartInfo struct {
	Index        int    `yaml:"index"`
	Rows         int64  `yaml:"rows"`
	Batches      int64  `yaml:"batches"`
	EncodedBytes int64  `yaml:"encoded_bytes"`
	DecodedBytes int64  `yaml:"decoded_bytes"`
	SHA256       string `yaml:"sha256"`
}

type Manifest struct {
	Version      int        `yaml:"version"`
	ID           string     `yaml:"id"`
	Fence        string     `yaml:"fence"`
	Tenant       string     `yaml:"tenant"`
	Identity     Identity   `yaml:"identity"`
	CreatedAt    time.Time  `yaml:"created_at"`
	ExpiresAt    time.Time  `yaml:"expires_at"`
	SchemaSHA256 string     `yaml:"schema_sha256"`
	Rows         int64      `yaml:"rows"`
	EncodedBytes int64      `yaml:"encoded_bytes"`
	DecodedBytes int64      `yaml:"decoded_bytes"`
	Parts        []PartInfo `yaml:"parts"`
}

func cloneManifest(m Manifest) Manifest { m.Parts = append([]PartInfo(nil), m.Parts...); return m }

type state struct {
	Version        int       `yaml:"version"`
	ID             string    `yaml:"id"`
	Fence          string    `yaml:"fence"`
	Tenant         string    `yaml:"tenant"`
	Identity       Identity  `yaml:"identity"`
	CreatedAt      time.Time `yaml:"created_at"`
	ExpiresAt      time.Time `yaml:"expires_at"`
	Limits         Limits    `yaml:"limits"`
	ReservedBytes  int64     `yaml:"reserved_bytes"`
	SchemaSHA256   string    `yaml:"schema_sha256"`
	Status         string    `yaml:"status"`
	ManifestSHA256 string    `yaml:"manifest_sha256,omitempty"`
}

func (s state) valid(c Config, id string) bool {
	common := s.ID == id && idPattern.MatchString(s.ID) && idPattern.MatchString(s.Fence) && s.Tenant == c.Tenant &&
		s.Identity.valid() && s.Limits.valid() && !s.CreatedAt.IsZero() && s.ExpiresAt.After(s.CreatedAt) && s.ExpiresAt.Sub(s.CreatedAt) <= c.MaxTTL &&
		s.ReservedBytes == s.Limits.MaxEncodedBytes+metadataReservation
	if !common {
		return false
	}
	if s.Version == 1 {
		// Keep the original on-disk validation contract for existing entries.
		return digestPattern.MatchString(s.SchemaSHA256) &&
			(s.Status == "active" || s.Status == "ready" || s.Status == "cancelled") &&
			((s.Status == "ready" && digestPattern.MatchString(s.ManifestSHA256)) || (s.Status != "ready" && (s.ManifestSHA256 == "" || digestPattern.MatchString(s.ManifestSHA256))))
	}
	if s.Version != 2 {
		return false
	}
	switch s.Status {
	case "reserved":
		return s.SchemaSHA256 == "" && s.ManifestSHA256 == ""
	case "active":
		return digestPattern.MatchString(s.SchemaSHA256) && s.ManifestSHA256 == ""
	case "ready":
		return digestPattern.MatchString(s.SchemaSHA256) && digestPattern.MatchString(s.ManifestSHA256)
	case "cancelled":
		return (s.SchemaSHA256 == "" && s.ManifestSHA256 == "") ||
			(digestPattern.MatchString(s.SchemaSHA256) && (s.ManifestSHA256 == "" || digestPattern.MatchString(s.ManifestSHA256)))
	default:
		return false
	}
}

// CleanupResult counts entries actually removed. Busy leases remain charged.
type CleanupResult struct{ Removed, Busy int }
