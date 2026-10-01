// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import "errors"

// FederationConfig exposes only operator-selected remote tables to DuckDB.
// Scan limits fail on overflow; they never truncate a relation before a join.
type FederationConfig struct {
	Tables       []FederationTable `json:"tables" yaml:"tables"`
	MaxScanRows  int64             `json:"max_scan_rows,omitempty" yaml:"max_scan_rows,omitempty"`
	MaxScanBytes int64             `json:"max_scan_bytes,omitempty" yaml:"max_scan_bytes,omitempty"`
}

type FederationTable struct {
	Name     string `json:"name" yaml:"name"`
	Database string `json:"database" yaml:"database"`
	Table    string `json:"table" yaml:"table"`
}

func (s Source) ValidateFederation() error {
	f := s.Federation
	if f == nil {
		return nil
	}
	if s.Type != "clickhouse" || s.Adapter != "" || s.Object != nil || s.Range != nil {
		return errors.New("custom federation currently requires a built-in ClickHouse source")
	}
	if len(f.Tables) == 0 || len(f.Tables) > 32 || f.MaxScanRows < 0 || f.MaxScanRows > 100000000 || f.MaxScanBytes < 0 || f.MaxScanBytes > 1<<40 || (f.MaxScanBytes > 0 && f.MaxScanBytes < 1024) {
		return errors.New("federation requires 1 to 32 tables and bounded scan limits")
	}
	seen := make(map[string]bool, len(f.Tables))
	for _, table := range f.Tables {
		if !ValidID(table.Name) || !ValidID(table.Database) || !ValidID(table.Table) || seen[table.Name] {
			return errors.New("federation tables require unique aliases and plain database/table identifiers")
		}
		seen[table.Name] = true
	}
	return nil
}
