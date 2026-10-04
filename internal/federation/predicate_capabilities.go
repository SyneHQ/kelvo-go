// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/apache/arrow-go/v18/arrow"
)

// PredicateCapabilities identifies exposed columns whose effective executor can
// enforce the bridge's complete existing integer/Boolean predicate vocabulary.
// Discovery is already complete; this method never calls an adapter or source.
func (t *Table) PredicateCapabilities() duckbridge.PredicateCapabilities {
	var result duckbridge.PredicateCapabilities
	if t == nil {
		return result
	}
	if t.guard == nil {
		// Custom declarations remain advisory. Snapshot filters also require the
		// local guard; the underlying snapshot producer accepts projections only.
		if t.customDriver != nil || t.snapshot != nil {
			return result
		}
		switch t.dialect {
		case dialectClickHouse, dialectPostgres, dialectMySQL, dialectSQLServer,
			dialectOracle, dialectSnowflake, dialectDatabricks, dialectBigQuery:
		default:
			return result
		}
	}
	schema := t.Schema()
	if schema == nil {
		return result
	}
	for _, field := range schema.Fields() {
		if field.Type == nil {
			continue
		}
		var kind, value string
		switch field.Type.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
			arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
			kind, value = field.Type.Name(), "0"
		case arrow.BOOL:
			kind, value = "bool", "false"
		default:
			continue
		}
		if t.guard == nil {
			// Reuse the compiler's exact type matrix instead of advertising a
			// second matrix that can drift from source SQL behavior.
			if _, err := t.dialect.exactConstant(kind, value, field.Type); err != nil {
				continue
			}
		}
		result.Columns = append(result.Columns, field.Name)
	}
	return result
}
