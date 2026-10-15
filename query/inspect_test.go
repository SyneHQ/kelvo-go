// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query

import (
	"encoding/json"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestInspectRequestValidation(t *testing.T) {
	valid := func() Request {
		return Request{Mode: "native", ConnectionID: "source_1", Inspect: &operations.MetadataSpec{Object: "columns", Target: operations.ObjectRef{Schema: "public", Name: "events"}, Limit: 100}}
	}
	if err := ValidateRequest(valid()); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Request){
		"federated":  func(r *Request) { r.Mode, r.ConnectionID, r.Sources = "federated", "", []string{"source_1"} },
		"sql":        func(r *Request) { r.SQL = "SELECT 1" },
		"parameters": func(r *Request) { r.Parameters = []Parameter{{Type: "int64", Value: json.RawMessage("1")}} },
		"mongo":      func(r *Request) { r.Mongo = &MongoRequest{Collection: "c"} },
		"scan":       func(r *Request) { r.ScanDiagnostics = true },
		"object":     func(r *Request) { r.Inspect.Object = "everything" },
		"connection": func(r *Request) { r.ConnectionID = "" },
	} {
		r := valid()
		change(&r)
		if ValidateRequest(r) == nil {
			t.Fatal(name, "accepted")
		}
	}
}
