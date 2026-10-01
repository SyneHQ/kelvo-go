// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query

import (
	"encoding/json"
	"math"
	"testing"
)

func TestTypedParameterFidelity(t *testing.T) {
	r := Request{Parameters: []Parameter{
		{Type: "int64", Value: json.RawMessage(`"9223372036854775807"`)},
		{Type: "uint64", Value: json.RawMessage(`"18446744073709551615"`)},
		{Type: "null", Value: json.RawMessage("null")},
		{Type: "bool", Value: json.RawMessage("true")},
	}}
	v, e := r.Values()
	if e != nil {
		t.Fatal(e)
	}
	if v[0] != int64(math.MaxInt64) || v[1] != uint64(math.MaxUint64) || v[2] != nil || v[3] != true {
		t.Fatalf("lost parameter fidelity: %#v", v)
	}
}
func TestInvalidTypedValuesFail(t *testing.T) {
	for _, p := range []Parameter{
		{Type: "string", Value: json.RawMessage("null")},
		{Type: "bool", Value: json.RawMessage("null")},
		{Type: "float64", Value: json.RawMessage("null")},
		{Type: "int64", Value: json.RawMessage("9223372036854775808")},
		{Type: "uint64", Value: json.RawMessage("-1")},
		{Type: "null", Value: json.RawMessage("1")},
	} {
		if _, e := (Request{Parameters: []Parameter{p}}).Values(); e == nil {
			t.Fatalf("accepted invalid parameter %#v", p)
		}
	}
}
