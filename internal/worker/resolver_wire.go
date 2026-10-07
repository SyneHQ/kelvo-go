// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"maps"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

// These are runtime values, never wire schemas. Every HTTP response is decoded
// strictly into resolver's public contracts before the explicit conversion.
type connectionResolution struct {
	Version          int
	DelegationSHA256 string
	ValidUntil       int64
	Sources          []catalog.Source
	Secrets          map[string]string
}

type operationResolution struct {
	Version        int
	GrantSHA256    string
	RequestSHA256  string
	SourceRevision string
	ValidUntil     int64
	Source         catalog.Source
	Secrets        map[string]string
}

func resolvedSource(s resolver.Source) catalog.Source {
	out := catalog.Source{ID: s.ID, Type: s.Type, DSNEnv: s.DSNEnv, URLEnv: s.URLEnv, UsernameEnv: s.UsernameEnv, PasswordEnv: s.PasswordEnv, TokenEnv: s.TokenEnv, Options: maps.Clone(s.Options)}
	if s.Federation != nil {
		out.Federation = &catalog.FederationConfig{MaxScanRows: s.Federation.MaxScanRows, MaxScanBytes: s.Federation.MaxScanBytes}
		for _, table := range s.Federation.Tables {
			out.Federation.Tables = append(out.Federation.Tables, catalog.FederationTable{Name: table.Name, Table: table.Table, Database: table.Database, Schema: table.Schema})
		}
	}
	return out
}

func resolvedQuery(wire resolver.QueryResponse) connectionResolution {
	out := connectionResolution{Version: wire.Version, DelegationSHA256: wire.DelegationSHA256, ValidUntil: wire.ValidUntil, Secrets: wire.Secrets}
	for _, source := range wire.Sources {
		out.Sources = append(out.Sources, resolvedSource(source))
	}
	return out
}

func resolvedOperation(wire resolver.OperationResponse) operationResolution {
	return operationResolution{Version: wire.Version, GrantSHA256: wire.GrantSHA256, RequestSHA256: wire.RequestSHA256, SourceRevision: wire.SourceRevision, ValidUntil: wire.ValidUntil, Source: resolvedSource(wire.Source), Secrets: wire.Secrets}
}
