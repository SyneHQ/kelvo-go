// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package provider defines provider requests without HTTP clients or secrets.
package provider

import (
	"encoding/json"
	"strings"

	"github.com/SYNEHQ/kelvo-go/operations"
)

const ResultFormat = "kelvo_provider_json_v1"

type Query struct {
	Operation string                     `json:"operation,omitempty"`
	Tool      string                     `json:"tool,omitempty"`
	Arguments map[string]json.RawMessage `json:"arguments,omitempty"`
	Resource  string                     `json:"resource,omitempty"`
	Params    map[string]string          `json:"params,omitempty"`
}

func Supported(kind string) bool {
	if SaaS(kind) {
		return true
	}
	switch strings.ToLower(kind) {
	case "salesforce_data360", "salesforce_tableau_next", "ramp", "daloopa", "motherduck", "posthog", "redis":
		return true
	}
	return false
}

func ParseQuery(raw []byte) (Query, error) {
	var q Query
	if operations.DecodeStrict(raw, &q, operations.MaxRequestBytes) != nil || len(q.Arguments) > 128 || len(q.Params) > 128 {
		return q, operations.ErrInvalid
	}
	if q.Operation != "" {
		if q.Operation != "list_tools" || q.Tool != "" || q.Resource != "" || len(q.Arguments) != 0 || len(q.Params) != 0 {
			return q, operations.ErrInvalid
		}
		return q, nil
	}
	if operations.ValidID(q.Tool) && q.Resource == "" && len(q.Params) == 0 {
		return q, nil
	}
	if operations.ValidID(q.Resource) && q.Tool == "" && len(q.Arguments) == 0 {
		return q, nil
	}
	return q, operations.ErrInvalid
}

func ResourceScope(resource string) (string, bool) {
	scope, ok := rampResources[resource]
	return scope, ok
}

func Resources() map[string]string {
	copy := make(map[string]string, len(rampResources))
	for resource, scope := range rampResources {
		copy[resource] = scope
	}
	return copy
}

var rampResources = map[string]string{
	"transactions": "transactions:read", "reimbursements": "reimbursements:read", "bills": "bills:read",
	"vendors": "vendors:read", "users": "users:read", "departments": "departments:read", "locations": "locations:read",
	"entities": "entities:read", "bank_accounts": "bank_accounts:read", "limits": "limits:read", "spend-programs": "spend_programs:read",
}

// ReadOnly uses an explicit provider contract, never remote tool annotations.
// MotherDuck documents query as read-only and query_rw as read-write. Other
// tools, including nested Salesforce execute calls, require mutation authority.
func ReadOnly(kind string, q Query) bool {
	kind = strings.ToLower(kind)
	if !Supported(kind) {
		return false
	}
	if q.Operation == "list_tools" {
		return true
	}
	if kind == "ramp" {
		_, ok := ResourceScope(q.Resource)
		return ok
	}
	return kind == "motherduck" && q.Tool == "query"
}

// Invocation binds the complete JSON object as one exact native parameter.
// Callers choose authority from the saved engine, never an unverified type hint.
func Invocation(kind string, raw []byte) (operations.Kind, *operations.NativeSpec, error) {
	kind = strings.ToLower(kind)
	if kind == "redis" {
		return redisInvocation(raw)
	}
	if kind == "posthog" {
		return posthogInvocation(raw)
	}
	if SaaS(kind) {
		return saasInvocation(kind, raw)
	}
	q, err := ParseQuery(raw)
	if err != nil || !Supported(kind) {
		return "", nil, operations.ErrInvalid
	}
	if kind == "ramp" {
		if q.Operation != "list_tools" {
			if _, ok := ResourceScope(q.Resource); !ok {
				return "", nil, operations.ErrInvalid
			}
		}
	} else if q.Resource != "" {
		return "", nil, operations.ErrInvalid
	}
	k := operations.NativeExecute
	if ReadOnly(kind, q) {
		k = operations.NativeRead
	}
	return k, &operations.NativeSpec{Provider: kind, Command: "query", Parameters: []operations.Parameter{{Type: "json", Value: append(json.RawMessage(nil), raw...)}}, ReturnResult: k == operations.NativeExecute}, nil
}
