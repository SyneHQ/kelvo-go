// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
)

var (
	ErrNotFound       = errors.New("reader registry not found")
	ErrConflict       = errors.New("reader registry conditional write conflicted")
	ErrInvalid        = errors.New("invalid reader registry configuration or reference")
	ErrCorrupt        = errors.New("invalid reader registry document")
	ErrBinding        = errors.New("reader registry generation binding mismatch")
	ErrFence          = errors.New("reader registry staging owner mismatch")
	ErrUnsealed       = errors.New("reader registry is not sealed")
	ErrCapacity       = errors.New("reader registry capacity exhausted")
	ErrUnavailable    = errors.New("reader registry unavailable")
	ErrClock          = errors.New("reader registry provider clock is uncertain")
	ErrLost           = errors.New("reader registry lease was lost")
	ErrExpired        = errors.New("reader registry lease expired")
	ErrClosed         = errors.New("reader registry lease closed")
	ErrReleaseUnknown = errors.New("reader registry lease release is uncertain")
)

// Metadata is supplied by the trusted provider adapter. Version is an opaque
// CAS token, never a checksum. ServerTime is authenticated service time.
type Metadata struct {
	Version    string
	Size       int64
	ServerTime time.Time
}

// Store is borrowed and concurrency-safe. Get returns bounded-stream-readable
// current data. CompareAndSwap atomically requires expectedVersion, with ""
// meaning create-if-absent. A returned ErrConflict guarantees no write occurred;
// every other write error may have committed. No raw error text is exposed.
type Store interface {
	Get(context.Context, string) (io.ReadCloser, Metadata, error)
	CompareAndSwap(context.Context, string, string, []byte) (Metadata, error)
}

type Config struct {
	Prefix, Tenant string
	// MaxReaders bounds each remote document. MaxLeases independently bounds
	// this Registry's acquisitions and live or not-yet-quiesced leases across
	// every generation. Neither limit waits or queues when capacity is full.
	MaxReaders, MaxLeases, MaxBytes, MaxAttempts                     int
	LeaseDuration, RenewInterval, OperationTimeout, ClockUncertainty time.Duration
}

func DefaultConfig(prefix, tenant string) Config {
	return Config{Prefix: prefix, Tenant: tenant, MaxReaders: 128, MaxLeases: 128, MaxBytes: 64 << 10, MaxAttempts: 8,
		LeaseDuration: time.Minute, RenewInterval: 15 * time.Second, OperationTimeout: 5 * time.Second, ClockUncertainty: 2 * time.Second}
}

var tenantPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
var datasetPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
var keyPartPattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)

func (c Config) validate() error {
	if !tenantPattern.MatchString(c.Tenant) || len(c.Prefix) == 0 || len(c.Prefix) > 256 ||
		c.MaxReaders < 1 || c.MaxReaders > 128 || c.MaxLeases < 1 || c.MaxLeases > 128 || c.MaxBytes < 1024 || c.MaxBytes > 64<<10 || c.MaxAttempts < 1 || c.MaxAttempts > 32 ||
		c.LeaseDuration < time.Second || c.LeaseDuration > time.Hour || c.OperationTimeout <= 0 || c.OperationTimeout > 5*time.Second ||
		c.ClockUncertainty <= 0 || c.ClockUncertainty > 5*time.Second || c.RenewInterval <= c.OperationTimeout ||
		c.RenewInterval > c.LeaseDuration/3 || c.LeaseDuration <= 4*(c.OperationTimeout+c.ClockUncertainty) ||
		c.LeaseDuration-2*c.ClockUncertainty-c.OperationTimeout <= 2*c.RenewInterval {
		return ErrInvalid
	}
	for _, part := range strings.Split(c.Prefix, "/") {
		if !keyPartPattern.MatchString(part) {
			return ErrInvalid
		}
	}
	return nil
}

// Reference is persisted with the immutable generation's publication metadata.
// Incarnation is a fresh 256-bit nonce; generation names are never reused.
type Reference struct {
	Tenant      string `yaml:"tenant"`
	Dataset     string `yaml:"dataset"`
	Generation  string `yaml:"generation"`
	Incarnation string `yaml:"incarnation"`
}

// ContentSHA256 binds the complete immutable commit identity, including every
// object version, descriptor, schema and authorization fingerprint. The caller
// must compute that complete canonical digest. The registry checks equality of
// the supplied digest; it cannot determine whether the caller omitted content.
type Binding struct {
	Reference     `yaml:",inline"`
	ContentSHA256 string `yaml:"content_sha256"`
}

func hexToken(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func validReference(ref Reference) bool {
	return tenantPattern.MatchString(ref.Tenant) && datasetPattern.MatchString(ref.Dataset) && hexToken(ref.Generation, 32) && hexToken(ref.Incarnation, 64)
}
func randomToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", ErrUnavailable
	}
	return hex.EncodeToString(raw[:]), nil
}

// NewReference allocates identity before any remote write, allowing a caller to
// retain and safely retry an ambiguous Stage using exactly the same reference.
func NewReference(tenant, dataset, generation string) (Reference, error) {
	ref := Reference{Tenant: tenant, Dataset: dataset, Generation: generation}
	var err error
	ref.Incarnation, err = randomToken()
	if err != nil {
		return Reference{}, err
	}
	if !validReference(ref) {
		return Reference{}, ErrInvalid
	}
	return ref, nil
}

func validVersion(version string) bool {
	if len(version) == 0 || len(version) > 4096 {
		return false
	}
	for _, c := range version {
		if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}
