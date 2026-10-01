// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import "testing"

func TestCLIRequestAcceptsSQLAndMongoForms(t *testing.T) {
	sql, err := makeRequest("SELECT ?", "federated", "", "source", `[{"type":"int64","value":"9223372036854775807"}]`, "", "")
	if err != nil || sql.Mongo != nil || len(sql.Parameters) != 1 {
		t.Fatalf("SQL request: %+v %v", sql, err)
	}
	mongo, err := makeRequest("", "native", "mongo", "", "[]", "events", `[{"$match":{"id":{"$numberLong":"9223372036854775807"}}}]`)
	if err != nil || mongo.Mongo == nil || len(mongo.Mongo.Pipeline) != 1 || mongo.SQL != "" {
		t.Fatalf("Mongo request: %+v %v", mongo, err)
	}
	empty, err := makeRequest("", "native", "mongo", "", "[]", "events", "")
	if err != nil || empty.Mongo == nil || len(empty.Mongo.Pipeline) != 0 {
		t.Fatal("default empty Mongo pipeline failed")
	}
}

func TestCLIRequestRejectsMixedAndMalformedMongoForms(t *testing.T) {
	for _, args := range [][7]string{
		{"SELECT 1", "native", "mongo", "", "[]", "events", "[]"},
		{"", "native", "mongo", "", "[]", "", "[]"},
		{"", "native", "mongo", "", "[]", "events", "{}"},
		{"", "native", "mongo", "", "[]", "events", "null"},
		{"", "native", "mongo", "", "[]", "events", "["},
		{"", "federated", "", "", "[]", "events", "[]"},
		{"", "native", "mongo", "extra", "[]", "events", "[]"},
		{"", "native", "mongo", "", `[{"type":"int64","value":1}]`, "events", "[]"},
	} {
		if _, err := makeRequest(args[0], args[1], args[2], args[3], args[4], args[5], args[6]); err == nil {
			t.Fatalf("accepted %+v", args)
		}
	}
}
