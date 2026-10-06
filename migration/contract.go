// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package migration defines sealed migration plans without database drivers.
package migration

import (
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/operations"
)

const (
	Version        = 1
	Table          = "schema_migrations"
	MaxFiles       = 20
	MaxFileBytes   = 500 << 10
	MaxPlanBytes   = operations.MaxSealedInputBytes
	MaxResultBytes = 16 << 10
)

var (
	ErrInvalid        = errors.New("invalid migration plan")
	ErrConflict       = errors.New("migration version changed")
	ErrDirty          = errors.New("migration version is dirty")
	ErrOutcomeUnknown = errors.New("migration outcome unknown")
	filename          = regexp.MustCompile(`^([0-9]+)_([A-Za-z0-9_]+)\.(up|down)\.sql$`)
)

// State includes dirty so force/recovery cannot race a newer failed migration.
type State struct {
	Version int64 `json:"version"`
	Dirty   bool  `json:"dirty"`
}

func (s State) Validate() error {
	// Keep the public contract portable across signed 32-bit migration versions.
	if s.Version < -1 || s.Version > 2147483647 {
		return ErrInvalid
	}
	return nil
}

type File struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

func ParseName(name string) (version int64, direction string, err error) {
	m := filename.FindStringSubmatch(name)
	if len(name) > 256 || len(m) != 4 {
		return 0, "", ErrInvalid
	}
	v, err := strconv.ParseInt(m[1], 10, 32)
	if err != nil {
		return 0, "", ErrInvalid
	}
	return v, m[3], nil
}

// Files are exact bytes. No remote URL, credentials or filesystem path can be
// loaded by an adapter. Each file is passed whole to the database driver.
type Plan struct {
	Version      int    `json:"version"`
	Expected     State  `json:"expected"`
	Direction    string `json:"direction"`
	Steps        int    `json:"steps,omitempty"`
	ForceVersion *int64 `json:"force_version,omitempty"`
	Files        []File `json:"files"`
}

func (p Plan) Validate() error {
	if p.Version != Version || p.Expected.Validate() != nil || p.Steps < 0 || p.Steps > MaxFiles || len(p.Files) > MaxFiles {
		return ErrInvalid
	}
	switch p.Direction {
	case "up", "down":
		if p.ForceVersion != nil || len(p.Files) == 0 {
			return ErrInvalid
		}
	case "force":
		if p.ForceVersion == nil || (State{Version: *p.ForceVersion}).Validate() != nil || p.Steps != 0 || len(p.Files) != 0 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, f := range p.Files {
		v, direction, err := ParseName(f.Name)
		key := strconv.FormatInt(v, 10) + "/" + direction
		if err != nil || seen[key] || len(f.Content) > MaxFileBytes || !utf8.ValidString(f.Content) || strings.ContainsRune(f.Content, 0) {
			return ErrInvalid
		}
		seen[key] = true
	}
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > MaxPlanBytes {
		return ErrInvalid
	}
	return nil
}

func ParsePlan(raw []byte) (Plan, error) {
	var p Plan
	if operations.DecodeStrict(raw, &p, MaxPlanBytes) != nil || p.Validate() != nil {
		return Plan{}, ErrInvalid
	}
	return p, nil
}

type Result struct {
	Version      int               `json:"version"`
	From         State             `json:"from"`
	To           State             `json:"to"`
	FilesApplied []string          `json:"files_applied"`
	Effect       operations.Effect `json:"effect"`
}

func (r Result) Validate() error {
	if r.Version != Version || r.From.Validate() != nil || r.To.Validate() != nil || len(r.FilesApplied) > MaxFiles {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, f := range r.FilesApplied {
		if _, _, err := ParseName(f); err != nil || seen[f] {
			return ErrInvalid
		}
		seen[f] = true
	}
	switch r.Effect {
	case operations.EffectNone, operations.EffectCommitted, operations.EffectPartial, operations.EffectUnknown:
	default:
		return ErrInvalid
	}
	return nil
}
