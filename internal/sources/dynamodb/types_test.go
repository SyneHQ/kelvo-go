// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package dynamodb

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAttributeValueValidation(t *testing.T) {
	for _, raw := range []string{fullDocument, `{}`, `{"empty":{"L":[]},"map":{"M":{}}}`, `{"value":{"N":"-1.25e+100"}}`} {
		if !validDocument(json.RawMessage(raw)) {
			t.Fatalf("rejected valid document: %s", raw)
		}
	}
	for _, raw := range []string{
		`null`, `[]`, `{"a":null}`, `{"a":{}}`, `{"a":{"UNKNOWN":"v"}}`, `{"a":{"S":"v","N":"1"}}`,
		`{"a":{"NULL":false}}`, `{"a":{"NULL":"true"}}`, `{"a":{"BOOL":0}}`, `{"a":{"N":1}}`, `{"a":{"N":"NaN"}}`,
		`{"a":{"B":"bad base64"}}`, `{"a":{"L":["text"]}}`, `{"a":{"M":[]}}`, `{"a":{"SS":[]}}`, `{"a":{"NS":[1]}}`, `{"a":{"BS":["?"]}}`,
		`{"a":{"S":"v"},"a":{"S":"other"}}`, `{"a":{"S":"v","S":"other"}}`, `{"a":{"S":"v"}} {}`,
	} {
		if validDocument(json.RawMessage(raw)) {
			t.Fatalf("accepted invalid document: %s", raw)
		}
	}
	nested := `{"S":"v"}`
	for i := 0; i < 33; i++ {
		nested = `{"L":[` + nested + `]}`
	}
	if validDocument(json.RawMessage(`{"a":` + nested + `}`)) {
		t.Fatal("unbounded nesting accepted")
	}
	if validDocument(json.RawMessage(`{"a":{"N":"` + strings.Repeat("1", 257) + `"}}`)) {
		t.Fatal("unbounded numeric text accepted")
	}
}
