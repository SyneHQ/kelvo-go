// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package migration

import (
	"encoding/json"
	"strings"
	"testing"
)

func planFixture() Plan {
	return Plan{Version: Version, Expected: State{Version: -1}, Direction: "up", Files: []File{{Name: "1_orders.up.sql", Content: "CREATE TABLE orders(id bigint);"}, {Name: "1_orders.down.sql", Content: "DROP TABLE orders;"}}}
}

func TestPlanRejectsAmbiguousVersionsPathsAndBounds(t *testing.T) {
	for _, mutate := range []func(*Plan){
		func(p *Plan) { p.Files = append(p.Files, File{Name: "01_other.up.sql", Content: "SELECT 1"}) },
		func(p *Plan) { p.Files[0].Name = "../1_orders.up.sql" },
		func(p *Plan) { p.Files[0].Name = "2147483648_orders.up.sql" },
		func(p *Plan) { p.Files[0].Content = strings.Repeat("x", MaxFileBytes+1) },
		func(p *Plan) { p.Files[0].Content = "SELECT '\x00'" },
		func(p *Plan) { p.Expected.Version = -2 },
		func(p *Plan) { p.Direction = "sideways" },
		func(p *Plan) { p.Steps = 21 },
	} {
		p := planFixture()
		mutate(&p)
		if p.Validate() == nil {
			t.Fatal("invalid plan accepted")
		}
	}
}

func TestPlanPreservesExactScriptAndRejectsUnknownFields(t *testing.T) {
	p := planFixture()
	p.Files[0].Content = "-- exact bytes\nCREATE FUNCTION test() RETURNS text LANGUAGE SQL AS $$ SELECT 'a;b' $$;\n"
	raw, _ := json.Marshal(p)
	got, err := ParsePlan(raw)
	if err != nil || got.Files[0].Content != p.Files[0].Content {
		t.Fatal("script changed", err)
	}
	raw = append([]byte(`{"Version":1,`), raw[1:]...)
	if _, err := ParsePlan(raw); err == nil {
		t.Fatal("duplicate field accepted")
	}
}

func TestForceHasNoExecutableFilesAndKeepsDirtyPrecondition(t *testing.T) {
	v := int64(-1)
	p := Plan{Version: Version, Expected: State{Version: 3, Dirty: true}, Direction: "force", ForceVersion: &v}
	if p.Validate() != nil {
		t.Fatal("force rejected")
	}
	p.Files = []File{{Name: "1_orders.up.sql", Content: "DROP TABLE orders"}}
	if p.Validate() == nil {
		t.Fatal("hidden force SQL accepted")
	}
}
