// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package access

import (
	"bytes"
	"encoding/json"

	"github.com/SYNEHQ/kelvo-go/delegation"
)

// FromDelegated converts the policy of a verified delegation grant and applies
// Validate, so a grant's restrictions are held to exactly the rules of operator
// policies. The delegation package mirrors these types because it is public
// API (see delegation.DelegatedPolicy). The copy is explicit, field by field,
// so a field added to one side cannot silently stop being enforced. Source and
// table keys are the grant's aliases, which are the source IDs and table names
// of the resolved execution catalog. A nil policy is an error: callers decide
// whether a grant carries one before asking.
func FromDelegated(d *delegation.DelegatedPolicy) (Policy, error) {
	if d == nil {
		return Policy{}, invalid()
	}
	p := Policy{Sources: make(map[string]SourcePolicy, len(d.Sources))}
	for id, source := range d.Sources {
		s := SourcePolicy{Tables: make(map[string]TablePolicy, len(source.Tables))}
		for name, table := range source.Tables {
			t := TablePolicy{Columns: append([]string(nil), table.Columns...), AllRows: table.AllRows}
			if table.Rows != nil {
				row := fromDelegatedPredicate(*table.Rows)
				t.Rows = &row
			}
			s.Tables[name] = t
		}
		p.Sources[id] = s
	}
	if err := Validate(p); err != nil {
		return Policy{}, err
	}
	return p, nil
}

func fromDelegatedPredicate(d delegation.DelegatedPredicate) Predicate {
	p := Predicate{Kind: d.Kind, Column: d.Column, Op: d.Op, Type: d.Type, Value: d.Value}
	for _, child := range d.Children {
		p.Children = append(p.Children, fromDelegatedPredicate(child))
	}
	return p
}

// SamePolicy reports whether two policies are identical. It compares canonical
// JSON, which sorts map keys; a policy that fails to encode is never equal.
func SamePolicy(a, b Policy) bool {
	x, errA := json.Marshal(a)
	y, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(x, y)
}
