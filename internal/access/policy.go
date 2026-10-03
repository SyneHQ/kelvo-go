// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package access enforces trusted row and column grants before Arrow relations
// enter the SQL engine. Policies never come from public query requests.
package access

import (
	"context"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

const (
	MaxSources         = 64
	MaxTables          = 32
	MaxColumns         = 1024
	MaxPredicateNodes  = 1024
	MaxPolicyTextBytes = 64 << 10
)

// Policy contains only restrictions for sources selected by this execution.
// An absent source retains its separately authorized whole-source grant.
type Policy struct {
	Sources map[string]SourcePolicy `json:"sources" yaml:"sources"`
}
type SourcePolicy struct {
	Tables map[string]TablePolicy `json:"tables" yaml:"tables"`
}
type TablePolicy struct {
	Columns []string   `json:"columns" yaml:"columns"`
	Rows    *Predicate `json:"rows,omitempty" yaml:"rows,omitempty"`
	AllRows bool       `json:"all_rows,omitempty" yaml:"all_rows,omitempty"`
}

// Predicate uses exact Arrow values. String comparisons use UTF-8 bytes, not
// database collations. NULL comparisons do not authorize a row; use null checks.
type Predicate struct {
	Kind     string      `json:"kind" yaml:"kind"`
	Column   string      `json:"column,omitempty" yaml:"column,omitempty"`
	Op       string      `json:"op,omitempty" yaml:"op,omitempty"`
	Type     string      `json:"type,omitempty" yaml:"type,omitempty"`
	Value    string      `json:"value,omitempty" yaml:"value,omitempty"`
	Children []Predicate `json:"children,omitempty" yaml:"children,omitempty"`
}

func denied() error { return query.NewError("PERMISSION_DENIED", "Query access denied") }
func unsupported() error {
	return query.NewError("UNSUPPORTED", "Required row or column policy is unsupported")
}
func invalid() error { return query.NewError("CONFIGURATION_ERROR", "Invalid row or column policy") }

type validation struct{ nodes, text, tables int }

func (v *validation) addText(s string) bool {
	v.text += len(s)
	return v.text <= MaxPolicyTextBytes
}
func validColumn(s string) bool {
	return s != "" && s != "*" && len(s) <= 1024 && utf8.ValidString(s) && strings.IndexFunc(s, func(r rune) bool { return r < 32 || r == 127 }) < 0
}

func Validate(p Policy) error {
	if len(p.Sources) == 0 || len(p.Sources) > MaxSources {
		return invalid()
	}
	v := &validation{}
	for id, source := range p.Sources {
		if !catalog.ValidID(id) || !v.addText(id) || len(source.Tables) == 0 {
			return invalid()
		}
		for name, table := range source.Tables {
			v.tables++
			if v.tables > MaxTables || !catalog.ValidID(name) || !v.addText(name) {
				return invalid()
			}
			if err := v.table(table); err != nil {
				return err
			}
		}
	}
	return nil
}
func (v *validation) table(t TablePolicy) error {
	if len(t.Columns) == 0 || len(t.Columns) > MaxColumns || (t.Rows != nil) == t.AllRows {
		return invalid()
	}
	seen := make(map[string]bool, len(t.Columns))
	for _, name := range t.Columns {
		folded := strings.ToLower(name)
		if !validColumn(name) || seen[folded] || !v.addText(name) {
			return invalid()
		}
		seen[folded] = true
	}
	if t.Rows != nil {
		return v.predicate(*t.Rows, 0)
	}
	return nil
}
func (v *validation) predicate(p Predicate, depth int) error {
	v.nodes++
	if depth > 32 || v.nodes > MaxPredicateNodes || !v.addText(p.Column) || !v.addText(p.Value) {
		return invalid()
	}
	if p.Kind == "and" || p.Kind == "or" {
		if len(p.Children) == 0 || len(p.Children) > 256 || p.Column != "" || p.Op != "" || p.Type != "" || p.Value != "" {
			return invalid()
		}
		for _, child := range p.Children {
			if err := v.predicate(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if !validColumn(p.Column) || len(p.Children) != 0 {
		return invalid()
	}
	if p.Kind == "is_null" || p.Kind == "is_not_null" {
		if p.Op != "" || p.Type != "" || p.Value != "" {
			return invalid()
		}
		return nil
	}
	if p.Kind != "comparison" {
		return invalid()
	}
	switch p.Op {
	case "eq", "ne", "lt", "le", "gt", "ge":
	default:
		return invalid()
	}
	if p.Type == "string" {
		if len(p.Value) > 4096 || !utf8.ValidString(p.Value) || strings.ContainsRune(p.Value, 0) {
			return invalid()
		}
		return nil
	}
	if p.Type == "bool" {
		if p.Value != "true" && p.Value != "false" {
			return invalid()
		}
		return nil
	}
	bits, signed, ok := integerType(p.Type)
	if !ok || len(p.Value) > 21 {
		return invalid()
	}
	if signed {
		n, err := strconv.ParseInt(p.Value, 10, bits)
		if err != nil || strconv.FormatInt(n, 10) != p.Value {
			return invalid()
		}
	} else {
		n, err := strconv.ParseUint(p.Value, 10, bits)
		if err != nil || strconv.FormatUint(n, 10) != p.Value {
			return invalid()
		}
	}
	return nil
}
func integerType(name string) (int, bool, bool) {
	switch name {
	case "int8":
		return 8, true, true
	case "int16":
		return 16, true, true
	case "int32":
		return 32, true, true
	case "int64":
		return 64, true, true
	case "uint8":
		return 8, false, true
	case "uint16":
		return 16, false, true
	case "uint32":
		return 32, false, true
	case "uint64":
		return 64, false, true
	default:
		return 0, false, false
	}
}

func clonePredicate(p Predicate) Predicate {
	p.Children = append([]Predicate(nil), p.Children...)
	for i := range p.Children {
		p.Children[i] = clonePredicate(p.Children[i])
	}
	return p
}
func cloneTable(t TablePolicy) TablePolicy {
	t.Columns = append([]string(nil), t.Columns...)
	if t.Rows != nil {
		row := clonePredicate(*t.Rows)
		t.Rows = &row
	}
	return t
}
func clone(p Policy) Policy {
	copy := Policy{Sources: make(map[string]SourcePolicy, len(p.Sources))}
	for id, source := range p.Sources {
		s := SourcePolicy{Tables: make(map[string]TablePolicy, len(source.Tables))}
		for name, table := range source.Tables {
			s.Tables[name] = cloneTable(table)
		}
		copy.Sources[id] = s
	}
	return copy
}
func Clone(p Policy) (Policy, error) {
	if err := Validate(p); err != nil {
		return Policy{}, err
	}
	return clone(p), nil
}

type policyKey struct{}

func WithPolicy(ctx context.Context, p Policy) (context.Context, error) {
	if ctx == nil {
		return nil, invalid()
	}
	copy, err := Clone(p)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, policyKey{}, copy), nil
}
func PolicyFromContext(ctx context.Context) (Policy, bool) {
	p, ok := ctx.Value(policyKey{}).(Policy)
	if !ok {
		return Policy{}, false
	}
	return clone(p), true
}
func Restricted(ctx context.Context) bool { _, ok := ctx.Value(policyKey{}).(Policy); return ok }
func Lookup(ctx context.Context, sourceID, tableAlias string) (TablePolicy, bool, bool) {
	p, ok := ctx.Value(policyKey{}).(Policy)
	if !ok {
		return TablePolicy{}, false, true
	}
	source, restricted := p.Sources[sourceID]
	if !restricted {
		return TablePolicy{}, false, true
	}
	table, allowed := source.Tables[tableAlias]
	return cloneTable(table), true, allowed
}

// ValidateRequest must run before snapshot resolution or credential access in
// the trusted parent. Local accelerated dataset IDs may be unresolved here;
// the child and engine must use ValidateResolvedRequest after lease acquisition.
func ValidateRequest(ctx context.Context, config catalog.Config, request query.Request) error {
	return validateRequest(ctx, config, request, true)
}

// ValidateResolvedRequest rejects unresolved dataset aliases at the worker
// envelope boundary. Every guarded snapshot must carry exact leased provenance.
func ValidateResolvedRequest(ctx context.Context, config catalog.Config, request query.Request) error {
	return validateRequest(ctx, config, request, false)
}

// GuardedSnapshot identifies paths reserved for the private Arrow reader. Never
// register these paths as SQL views or place them in DuckDB's file allowlist.
// Admission and NewSnapshot separately verify the exact dataset grant.
func GuardedSnapshot(ctx context.Context, source catalog.Source) bool {
	return source.LocalSnapshot != nil && Restricted(ctx)
}

func validateRequest(ctx context.Context, config catalog.Config, request query.Request, unresolved bool) error {
	p, active := ctx.Value(policyKey{}).(Policy)
	if !active {
		return nil
	}
	if (request.Mode != "" && request.Mode != "federated") || request.ScanDiagnostics {
		return unsupported()
	}
	sources, err := config.Select(request.Sources)
	if err != nil {
		return denied()
	}
	selected := make(map[string]bool, len(sources))
	names := make(map[string]bool, len(sources))
	for _, source := range sources {
		folded := strings.ToLower(source.ID)
		if names[folded] {
			return denied()
		}
		names[folded] = true
		selected[source.ID] = true
		if source.Type == "accelerated" || source.LocalSnapshot != nil {
			if source.Type == "accelerated" {
				if !unresolved || config.Acceleration == nil || config.Acceleration.ObjectStorage != nil {
					return unsupported()
				}
				if _, exists := config.Dataset(source.ID); !exists || source.Path != "" || source.LocalSnapshot != nil {
					return unsupported()
				}
			} else if source.ValidateLocalSnapshot() != nil || source.LocalSnapshot.SchemaSHA256 == "" {
				return unsupported()
			}
			// A dataset owns one relation in main with the dataset ID as its
			// policy table alias. Refresh-source grants do not imply this grant.
			rule, granted := p.Sources[source.ID]
			if !granted || len(rule.Tables) != 1 {
				return denied()
			}
			if _, granted = rule.Tables[source.ID]; !granted {
				return denied()
			}
			continue
		}
		if source.Federation == nil || source.Adapter != "" || source.Path != "" || source.Object != nil || source.Range != nil || len(source.Ranges) != 0 || len(source.ParquetPaths) != 0 {
			return unsupported()
		}
		aliases := make(map[string]bool, len(source.Federation.Tables))
		foldedAliases := make(map[string]bool, len(source.Federation.Tables))
		for _, table := range source.Federation.Tables {
			folded := strings.ToLower(table.Name)
			if foldedAliases[folded] {
				return denied()
			}
			foldedAliases[folded] = true
			aliases[table.Name] = true
		}
		if restricted, ok := p.Sources[source.ID]; ok {
			for alias := range restricted.Tables {
				if !aliases[alias] {
					return denied()
				}
			}
		}
	}
	for id := range p.Sources {
		if !selected[id] {
			return denied()
		}
	}
	return nil
}
