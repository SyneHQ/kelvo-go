// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/query"
	"go.yaml.in/yaml/v3"
)

func TestMongoYAMLPreservesExactValues(t *testing.T) {
	input := `collection: events
pipeline:
  - $match:
      id: 9223372036854775807
      amount:
        $numberDecimal: "12345678901234567890.123456789"
      active: true
      note: null
  - $group:
      _id: "$tenant"
      total:
        $sum: 1
`
	var m query.MongoRequest
	if err := yaml.Unmarshal([]byte(input), &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Pipeline) != 2 || !strings.Contains(string(m.Pipeline[0]), `"id":9223372036854775807`) || !strings.Contains(string(m.Pipeline[0]), `"$numberDecimal":"12345678901234567890.123456789"`) {
		t.Fatalf("lost exact values: %s", m.Pipeline)
	}
	if err := query.ValidateRequest(query.Request{Mode: "native", ConnectionID: "mongo", Mongo: &m}); err != nil {
		t.Fatal(err)
	}
	encoded, err := yaml.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var again query.MongoRequest
	if err := yaml.Unmarshal(encoded, &again); err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(m)
	b, _ := json.Marshal(again)
	if string(a) != string(b) {
		t.Fatal("pipeline YAML round trip changed values")
	}
}

func TestMongoYAMLRejectsAmbiguousValues(t *testing.T) {
	for _, input := range []string{
		"collection: events\npipeline: []\nunknown: true\n",
		"collection: events\ncollection: other\npipeline: []\n",
		"collection: events\npipeline:\n  - $match: {id: 1, id: 2}\n",
		"collection: events\npipeline:\n  - $match: {value: .nan}\n",
		"collection: events\npipeline:\n  - $match: {value: .inf}\n",
		"collection: events\npipeline:\n  - &stage {$match: {}}\n  - *stage\n",
		"collection: events\npipeline:\n  - [1, 2]\n",
		"collection: events\npipeline:\n  - $match: {date: 2026-10-02}\n",
	} {
		var m query.MongoRequest
		if err := yaml.Unmarshal([]byte(input), &m); err == nil {
			t.Fatalf("accepted ambiguous YAML: %s", input)
		}
	}
}
