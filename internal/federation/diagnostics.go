// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"unicode/utf8"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
)

const maxDiagnosticScans = 8
const maxDiagnosticFields = 8
const maxDiagnosticIdentifier = 64
const MaxScanDiagnosticsBytes = 8192

type diagnosticsKey struct{}
type ScanDiagnosticCollector struct {
	mu      sync.Mutex
	scans   []federationapi.ScanDiagnostic
	omitted int64
}

func WithScanDiagnostics(ctx context.Context) (context.Context, *ScanDiagnosticCollector) {
	collector := &ScanDiagnosticCollector{}
	return context.WithValue(ctx, diagnosticsKey{}, collector), collector
}
func scanDiagnosticsFromContext(ctx context.Context) *ScanDiagnosticCollector {
	collector, _ := ctx.Value(diagnosticsKey{}).(*ScanDiagnosticCollector)
	return collector
}

type scanDiagnosticHandle struct {
	collector *ScanDiagnosticCollector
	index     int
}

func diagnosticIdentifier(value string, truncated *bool) string {
	if len(value) > maxDiagnosticIdentifier || !utf8.ValidString(value) || strings.IndexFunc(value, func(r rune) bool { return r < 32 || r == 127 }) >= 0 {
		*truncated = true
		return "[omitted]"
	}
	return value
}
func diagnosticPredicate(filter federationapi.Filter, left *int, truncated *bool) federationapi.PredicateDiagnostic {
	*left--
	result := federationapi.PredicateDiagnostic{Kind: filter.Kind, Column: diagnosticIdentifier(filter.Column, truncated), Operator: filter.Op, Type: filter.Type}
	// Enumerate vocabulary defensively so malformed adapter/planner values cannot
	// smuggle arbitrary text through a field that should contain an operator.
	switch result.Kind {
	case "comparison", "is_null", "is_not_null", "and", "or":
	default:
		result.Kind = "unknown"
		*truncated = true
	}
	switch result.Operator {
	case "", "eq", "ne", "lt", "le", "gt", "ge":
	default:
		result.Operator = "unknown"
		*truncated = true
	}
	switch result.Type {
	case "", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "bool":
	default:
		result.Type = "unknown"
		*truncated = true
	}
	for _, child := range filter.Children {
		if *left == 0 {
			*truncated = true
			break
		}
		result.Children = append(result.Children, diagnosticPredicate(child, left, truncated))
	}
	return result
}
func (c *ScanDiagnosticCollector) begin(source, table string, plan federationapi.ScanPlan, known bool, projectionKind string) *scanDiagnosticHandle {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.scans) >= maxDiagnosticScans {
		c.omitted++
		return nil
	}
	entry := federationapi.ScanDiagnostic{ProjectionKind: projectionKind, ResidualVisibility: "not_observed", CapabilitiesKnown: known, Outcome: "running", Projection: []string{}}
	entry.Source = diagnosticIdentifier(source, &entry.Truncated)
	entry.Table = diagnosticIdentifier(table, &entry.Truncated)
	for _, column := range plan.Columns {
		if len(entry.Projection) >= maxDiagnosticFields {
			entry.Truncated = true
			break
		}
		entry.Projection = append(entry.Projection, diagnosticIdentifier(column, &entry.Truncated))
	}
	left := maxDiagnosticFields
	for _, filter := range plan.Filters {
		if left == 0 {
			entry.Truncated = true
			break
		}
		entry.Predicates = append(entry.Predicates, diagnosticPredicate(filter, &left, &entry.Truncated))
	}
	c.scans = append(c.scans, entry)
	return &scanDiagnosticHandle{collector: c, index: len(c.scans) - 1}
}
func (h *scanDiagnosticHandle) finish(outcome string, rows, bytes, batches, wire int64) {
	if h == nil {
		return
	}
	c := h.collector
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := &c.scans[h.index]
	entry.Outcome = outcome
	entry.Rows = rows
	entry.ArrowBytes = bytes
	entry.Batches = batches
	entry.SourceWireBytes = wire
}

// Snapshot is detached from mutable collector state. The serialized report is
// hard-capped at 8KiB, leaving space in the worker's bounded outcome envelope.
func (c *ScanDiagnosticCollector) Snapshot() federationapi.ScanDiagnostics {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := federationapi.ScanDiagnostics{Version: 1, Scope: "executed_scans", Scans: append([]federationapi.ScanDiagnostic{}, c.scans...), OmittedScans: c.omitted, Truncated: c.omitted > 0}
	for _, scan := range result.Scans {
		result.Truncated = result.Truncated || scan.Truncated
	}
	for {
		data, _ := json.Marshal(result)
		if len(data) <= MaxScanDiagnosticsBytes {
			// JSON copy also detaches nested slices, without unbounded allocations.
			var detached federationapi.ScanDiagnostics
			_ = json.Unmarshal(data, &detached)
			return detached
		}
		result.Scans = result.Scans[:len(result.Scans)-1]
		result.OmittedScans++
		result.Truncated = true
	}
}
