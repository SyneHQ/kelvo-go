// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query

import (
	"encoding/json"
	"regexp"
	"strings"
)

var sourceName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

func ValidateRequest(r Request) error {
	if len(r.Delegation) > 32768 || (r.Delegation != "" && (r.Mongo != nil || r.ScanDiagnostics)) {
		return NewError("INVALID_ARGUMENT", "Invalid delegated query")
	}
	if (strings.TrimSpace(r.SQL) == "" && r.Mongo == nil) || len(r.SQL) > 64<<10 || len(r.Parameters) > 1024 || len(r.Sources) > 64 {
		return NewError("INVALID_ARGUMENT", "Query exceeds its request limits")
	}
	if r.Mode != "native" && r.Mode != "federated" {
		return NewError("INVALID_ARGUMENT", "Mode must be native or federated")
	}
	if r.Mode == "native" {
		if !sourceName.MatchString(r.ConnectionID) || len(r.Sources) != 0 {
			return NewError("INVALID_ARGUMENT", "Native queries require one registered connection")
		}
	} else {
		if r.ConnectionID != "" {
			return NewError("INVALID_ARGUMENT", "Federated queries cannot specify connection_id")
		}
		seen := map[string]bool{}
		for _, id := range r.Sources {
			if !sourceName.MatchString(id) || seen[id] {
				return NewError("INVALID_ARGUMENT", "Invalid or duplicate source")
			}
			seen[id] = true
		}
	}
	bytes := len(r.SQL)
	if r.Mongo != nil {
		if r.Mode != "native" || r.SQL != "" || len(r.Parameters) != 0 || len(r.Mongo.Collection) == 0 || len(r.Mongo.Collection) > 120 || len(r.Mongo.Pipeline) > 128 {
			return NewError("INVALID_ARGUMENT", "MongoDB requires a native collection and pipeline, without SQL or parameters")
		}
		bytes += len(r.Mongo.Collection)
		for _, stage := range r.Mongo.Pipeline {
			if !json.Valid(stage) {
				return NewError("INVALID_ARGUMENT", "Invalid MongoDB pipeline")
			}
			bytes += len(stage)
		}
	}
	for _, p := range r.Parameters {
		if len(p.Type) > 16 {
			return NewError("INVALID_ARGUMENT", "Invalid parameter type")
		}
		bytes += len(p.Value)
	}
	if bytes > 128<<10 {
		return NewError("INVALID_ARGUMENT", "Query parameters exceed their size limit")
	}
	_, err := r.Values()
	return err
}
