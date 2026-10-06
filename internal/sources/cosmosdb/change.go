// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cosmosdb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func (e *Engine) changePlan(sql string) (changePlan, error) {
	plan, err := parseChange(sql)
	if err != nil {
		return plan, err
	}
	if e == nil || e.client == nil {
		return plan, query.NewError("INVALID_ARGUMENT", "Cosmos source is unavailable")
	}
	database, container := e.source.Options["database"], e.source.Options["container"]
	if plan.database == "" {
		plan.database = database
	}
	if plan.database != database || plan.databaseDDL && container != "" || !plan.databaseDDL && container != "" && plan.container != container {
		return plan, query.NewError("PERMISSION_DENIED", "Cosmos mutation differs from the selected namespace")
	}
	if plan.verb != "CREATE" && plan.verb != "DROP" && container == "" {
		return plan, query.NewError("INVALID_ARGUMENT", "Cosmos point writes require a selected container")
	}
	return plan, nil
}

// ValidateStatement performs no network I/O. The adapter calls it for all batch
// slots before sending the first mutation. Account-level throughput changes,
// stored procedures and SQL expressions are deliberately not emulated.
func (e *Engine) ValidateStatement(sql string) error {
	_, err := e.changePlan(sql)
	return err
}

func (e *Engine) ApplyStatement(parent context.Context, sql string) (*int64, error) {
	if parent == nil {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid Cosmos operation context")
	}
	plan, err := e.changePlan(sql)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resource := "dbs/" + plan.database
	if !plan.databaseDDL {
		resource += "/colls/" + plan.container
	}
	if plan.verb == "CREATE" || plan.verb == "DROP" {
		return e.applyResourceChange(ctx, plan, resource)
	}
	paths, err := e.changePartition(ctx, resource)
	if err != nil {
		return nil, err
	}
	if len(plan.partition) != 0 && !samePaths(plan.partition, paths) {
		return nil, query.NewError("INVALID_ARGUMENT", "Cosmos partition paths differ from container metadata")
	}
	partition, err := changePartitionValues(plan, paths)
	if err != nil {
		return nil, err
	}
	headers := map[string]string{
		"x-ms-documentdb-partitionkey":             partition,
		"x-ms-documentdb-contentresponse-on-write": "false",
	}
	method, target, signingResource := http.MethodPost, "/"+resource+"/docs", resource
	var body any = plan.values
	switch plan.verb {
	case "UPSERT":
		headers["x-ms-documentdb-is-upsert"] = "True"
	case "UPDATE", "DELETE":
		id, _ := documentID(plan.where["id"])
		signingResource = resource + "/docs/" + id
		target = "/" + signingResource
		method, body = http.MethodDelete, nil
		if plan.verb == "UPDATE" {
			method = http.MethodPatch
			headers["Content-Type"] = "application/json_patch+json"
			ops := make([]map[string]any, 0, len(plan.fields))
			for _, field := range plan.fields {
				for _, path := range paths {
					if path == "/"+field || strings.HasPrefix(path, "/"+field+"/") {
						return nil, query.NewError("INVALID_ARGUMENT", "Cosmos UPDATE cannot change partition key fields")
					}
				}
				ops = append(ops, map[string]any{"op": "set", "path": "/" + field, "value": plan.values[field]})
			}
			body = map[string]any{"operations": ops}
		}
	}
	status, err := e.sendChange(ctx, method, target, "docs", signingResource, body, headers)
	if err != nil {
		return nil, err
	}
	count := int64(1)
	if (plan.verb == "UPDATE" || plan.verb == "DELETE") && status == http.StatusNotFound {
		count = 0
	} else if !(plan.verb == "INSERT" && status == http.StatusCreated || plan.verb == "UPSERT" && (status == http.StatusOK || status == http.StatusCreated) || plan.verb == "UPDATE" && status == http.StatusOK || plan.verb == "DELETE" && status == http.StatusNoContent) {
		return nil, changeRejected(status)
	}
	return &count, nil
}

func (e *Engine) applyResourceChange(ctx context.Context, plan changePlan, resource string) (*int64, error) {
	kind, id := "colls", plan.container
	if plan.databaseDDL {
		kind, id = "dbs", plan.database
	}
	method, target, signing := http.MethodDelete, "/"+resource, resource
	var body any
	headers := map[string]string{}
	if plan.verb == "CREATE" {
		method = http.MethodPost
		if plan.databaseDDL {
			target, signing = "/dbs", ""
		} else {
			target, signing = "/dbs/"+plan.database+"/colls", "dbs/"+plan.database
		}
		document := map[string]any{"id": id}
		if !plan.databaseDDL {
			kind := "Hash"
			if len(plan.partition) > 1 {
				kind = "MultiHash"
			}
			document["partitionKey"] = map[string]any{"paths": plan.partition, "kind": kind, "version": 2}
		}
		body = document
		if plan.throughput != "" {
			headers["x-ms-offer-throughput"] = plan.throughput
		}
		if plan.autoscale != "" {
			headers["x-ms-cosmos-offer-autopilot-settings"] = `{"maxThroughput":` + plan.autoscale + `}`
		}
	}
	status, err := e.sendChange(ctx, method, target, kind, signing, body, headers)
	if err != nil {
		return nil, err
	}
	count := int64(1)
	if plan.conditional && (plan.verb == "CREATE" && status == http.StatusConflict || plan.verb == "DROP" && status == http.StatusNotFound) {
		count = 0
	} else if !(plan.verb == "CREATE" && status == http.StatusCreated || plan.verb == "DROP" && status == http.StatusNoContent) {
		return nil, changeRejected(status)
	}
	return &count, nil
}

// A successful mutation status acknowledges its effect even if the optional
// document response is oversized or disconnected. Do not turn that into a
// retriable error. No response body is needed for affected-row reporting.
func (e *Engine) sendChange(ctx context.Context, method, path, kind, resource string, body any, headers map[string]string) (int, error) {
	var input io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil || len(data) > 1<<20 {
			return 0, query.NewError("RESOURCE_EXHAUSTED", "Cosmos mutation exceeds its request budget")
		}
		input = bytes.NewReader(data)
	}
	u := *e.client.Origin
	u.Path = path
	request, err := http.NewRequestWithContext(ctx, method, u.String(), input)
	if err != nil {
		return 0, query.NewError("INVALID_ARGUMENT", "Invalid Cosmos mutation request")
	}
	request.GetBody = nil
	date := e.now().UTC().Format(http.TimeFormat)
	authorization := url.QueryEscape("type=aad&ver=1.0&sig=" + e.client.Token)
	if e.source.Options["auth"] == "master_key" {
		authorization = masterAuthorization(method, kind, resource, date, e.key)
	}
	request.Header.Set("Authorization", authorization)
	request.Header.Set("x-ms-date", date)
	request.Header.Set("x-ms-version", "2018-12-31")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := e.client.HTTP.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, query.NewError("QUERY_FAILED", "Cosmos mutation acknowledgement is unavailable")
	}
	response.Body.Close()
	return response.StatusCode, nil
}

func changeRejected(status int) error {
	code := "QUERY_FAILED"
	if status == http.StatusTooManyRequests {
		code = "RESOURCE_EXHAUSTED"
	}
	return query.NewError(code, "Cosmos DB rejected the mutation")
}

func (e *Engine) changePartition(ctx context.Context, resource string) ([]string, error) {
	raw, headers, _, err := e.discoveryGet(ctx, "/"+resource, "colls", resource, "")
	if err != nil {
		return nil, err
	}
	charge, err := singleHeader(headers, "x-ms-request-charge")
	if err != nil || len(charge) > 20 || !chargeNumber.MatchString(charge) {
		return nil, query.NewError("QUERY_FAILED", "Invalid Cosmos partition metadata charge")
	}
	ru, ok := new(big.Rat).SetString(charge)
	if !ok || ru.Cmp(new(big.Rat).SetInt64(e.maxRequestUnits)) > 0 {
		return nil, query.NewError("RESOURCE_EXHAUSTED", "Cosmos partition metadata charge exceeds budget")
	}
	value, err := strictChangeValue(raw)
	if err != nil {
		return nil, query.NewError("QUERY_FAILED", "Invalid Cosmos partition metadata")
	}
	object, ok := value.(map[string]any)
	if !ok || object["id"] != e.source.Options["container"] {
		return nil, query.NewError("QUERY_FAILED", "Cosmos partition metadata identity differs")
	}
	var metadata struct {
		Partition struct {
			Kind  string   `json:"kind"`
			Paths []string `json:"paths"`
		} `json:"partitionKey"`
	}
	if json.Unmarshal(raw, &metadata) != nil || len(metadata.Partition.Paths) < 1 || len(metadata.Partition.Paths) > 3 || metadata.Partition.Kind != "Hash" && metadata.Partition.Kind != "MultiHash" || metadata.Partition.Kind == "Hash" && len(metadata.Partition.Paths) != 1 {
		return nil, query.NewError("UNSUPPORTED", "Cosmos point writes require a supported partition key")
	}
	seen := map[string]bool{}
	for _, path := range metadata.Partition.Paths {
		if !validPartitionPath(path) || seen[path] {
			return nil, query.NewError("UNSUPPORTED", "Unsupported Cosmos partition path")
		}
		seen[path] = true
	}
	return metadata.Partition.Paths, nil
}

func samePaths(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func changePartitionValues(plan changePlan, paths []string) (string, error) {
	values := make([]any, 0, len(paths))
	used := map[string]bool{"id": true}
	for _, path := range paths {
		parts := strings.Split(path[1:], "/")
		var value any
		var err error
		undefined := false
		if plan.verb == "INSERT" || plan.verb == "UPSERT" {
			raw, exists := plan.values[parts[0]]
			if exists {
				value, err = strictChangeValue(raw)
				for _, part := range parts[1:] {
					object, ok := value.(map[string]any)
					if !ok {
						exists = false
						break
					}
					value, exists = object[part]
					if !exists {
						break
					}
				}
			}
			if !exists {
				undefined = true
				value = map[string]any{} // Cosmos undefined partition value.
			}
		} else {
			field := strings.Join(parts, ".")
			raw, exists := plan.where[field]
			if !exists {
				return "", query.NewError("INVALID_ARGUMENT", "Cosmos point writes require every partition predicate")
			}
			used[field] = true
			value, err = strictChangeValue(raw)
		}
		if err != nil {
			return "", err
		}
		switch value.(type) {
		case nil, string, bool, json.Number:
		case map[string]any:
			if !undefined {
				return "", changeSyntax()
			}
		default:
			return "", query.NewError("INVALID_ARGUMENT", "Cosmos partition values must be scalars")
		}
		values = append(values, value)
	}
	if plan.verb == "UPDATE" || plan.verb == "DELETE" {
		for field := range plan.where {
			if !used[field] {
				return "", query.NewError("INVALID_ARGUMENT", "Cosmos point-write predicates must match id and partition keys only")
			}
		}
	}
	encoded, err := json.Marshal(values)
	if err != nil || len(encoded) > 8<<10 {
		return "", query.NewError("RESOURCE_EXHAUSTED", "Cosmos partition header exceeds budget")
	}
	return string(encoded), nil
}
