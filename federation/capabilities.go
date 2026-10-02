// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import "errors"

const CapabilityVersion = 1

// CapabilityProvider optionally advertises a trusted adapter's declared scan
// support. Wrappers must forward this method explicitly. Declarations are
// advisory: they are not conformance results or planner negotiation. Every
// required ScanPlan filter must still execute exactly or return ErrUnsupported.
// Missing, invalid or newer declarations are unknown, never implicit support.
type CapabilityProvider interface{ FederationCapabilities() Capabilities }

type Capabilities struct {
	Version        int                    `json:"version"`
	Projection     bool                   `json:"projection"`
	NullPredicates bool                   `json:"null_predicates"`
	Conjunction    bool                   `json:"conjunction"`
	Disjunction    bool                   `json:"disjunction"`
	Comparisons    []ComparisonCapability `json:"comparisons,omitempty"`
}
type ComparisonCapability struct {
	Type      string   `json:"type"`
	Operators []string `json:"operators"`
}

// Validate rejects unbounded or ambiguous declarations. Capability v1 covers
// only the typed scalar predicate vocabulary supported by the current bridge.
func (c Capabilities) Validate() error {
	invalid := errors.New("invalid or unsupported federation capability declaration")
	if c.Version != CapabilityVersion || len(c.Comparisons) > 9 {
		return invalid
	}
	seen := map[string]bool{}
	for _, comparison := range c.Comparisons {
		switch comparison.Type {
		case "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "bool":
		default:
			return invalid
		}
		if seen[comparison.Type] || len(comparison.Operators) == 0 || len(comparison.Operators) > 6 {
			return invalid
		}
		seen[comparison.Type] = true
		ops := map[string]bool{}
		for _, op := range comparison.Operators {
			switch op {
			case "eq", "ne", "lt", "le", "gt", "ge":
			default:
				return invalid
			}
			if ops[op] {
				return invalid
			}
			ops[op] = true
		}
	}
	return nil
}

// InspectCapabilities returns a detached, bounded declaration. A missing or
// broken optional method does not weaken the existing required-filter contract.
func InspectCapabilities(adapter any) (capabilities Capabilities, known bool) {
	defer func() {
		if recover() != nil {
			capabilities = Capabilities{}
			known = false
		}
	}()
	provider, ok := adapter.(CapabilityProvider)
	if !ok {
		return Capabilities{}, false
	}
	declaration := provider.FederationCapabilities()
	if declaration.Validate() != nil {
		return Capabilities{}, false
	}
	result := declaration
	result.Comparisons = make([]ComparisonCapability, len(declaration.Comparisons))
	for i, item := range declaration.Comparisons {
		result.Comparisons[i] = ComparisonCapability{Type: item.Type, Operators: append([]string(nil), item.Operators...)}
	}
	return result, true
}

// ScanDiagnostics reports only executed Go scan boundaries. It does not expose
// DuckDB's full plan, joins, local predicates or estimates. All counts are real
// observed scan counters; missing scans/fields are explicitly marked truncated.
// No diagnostic structure contains SQL, credentials or predicate literal values.
type ScanDiagnostics struct {
	Version      int              `json:"version"`
	Scope        string           `json:"scope"`
	Scans        []ScanDiagnostic `json:"scans"`
	Truncated    bool             `json:"truncated,omitempty"`
	OmittedScans int64            `json:"omitted_scans,omitempty"`
}
type ScanDiagnostic struct {
	ProjectionKind     string                `json:"projection_kind"`
	Source             string                `json:"source"`
	Table              string                `json:"table"`
	Projection         []string              `json:"projection"`
	Predicates         []PredicateDiagnostic `json:"required_source_predicates,omitempty"`
	ResidualVisibility string                `json:"local_residuals"`
	CapabilitiesKnown  bool                  `json:"capabilities_declared"`
	Outcome            string                `json:"outcome"`
	Rows               int64                 `json:"rows"`
	ArrowBytes         int64                 `json:"arrow_bytes"`
	Batches            int64                 `json:"batches"`
	SourceWireBytes    int64                 `json:"source_wire_bytes,omitempty"`
	Truncated          bool                  `json:"truncated,omitempty"`
}
type PredicateDiagnostic struct {
	Kind     string                `json:"kind"`
	Column   string                `json:"column,omitempty"`
	Operator string                `json:"operator,omitempty"`
	Type     string                `json:"type,omitempty"`
	Children []PredicateDiagnostic `json:"children,omitempty"`
}
