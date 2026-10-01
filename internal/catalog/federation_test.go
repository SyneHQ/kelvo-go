// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import "testing"

func TestFederationSourceContract(t *testing.T) {
	good := Source{ID: "warehouse", Type: "clickhouse", URLEnv: "KELVO_SOURCE_WAREHOUSE_URL", Federation: &FederationConfig{Tables: []FederationTable{{Name: "events", Database: "analytics", Table: "events"}}, MaxScanRows: 10000000, MaxScanBytes: 1 << 30}}
	if err := good.ValidateFederation(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Source){
		func(s *Source) { s.Type = "mongodb" },
		func(s *Source) { s.Adapter = "flightsql" },
		func(s *Source) { s.Federation.Tables = nil },
		func(s *Source) { s.Federation.Tables = append(s.Federation.Tables, s.Federation.Tables[0]) },
		func(s *Source) { s.Federation.Tables[0].Database = "analytics.other" },
		func(s *Source) { s.Federation.Tables[0].Table = "events; DROP TABLE events" },
		func(s *Source) { s.Federation.MaxScanRows = -1 },
		func(s *Source) { s.Federation.MaxScanBytes = 1 },
	} {
		bad := good
		f := *good.Federation
		f.Tables = append([]FederationTable(nil), f.Tables...)
		bad.Federation = &f
		change(&bad)
		if err := bad.ValidateFederation(); err == nil {
			t.Fatal("accepted invalid federation configuration", bad)
		}
	}
}
