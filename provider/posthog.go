// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package provider

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// PostHogQuery accepts either HogQL, a query API body, or a project-scoped REST
// call. The saved project ID is inserted by the adapter, never selected here.
type PostHogQuery struct {
	SQL    string          `json:"sql,omitempty"`
	Query  json.RawMessage `json:"query,omitempty"`
	Method string          `json:"method,omitempty"`
	Path   string          `json:"path,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"`
}

var providerProject = regexp.MustCompile(`^[0-9]{1,20}$`)

func ValidProject(id string) bool { return providerProject.MatchString(id) }

// ValidPostHogOrigin allows a saved self-hosted instance but no credentials,
// paths, proxy options or TLS downgrade in the private resolution.
func ValidPostHogOrigin(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil &&
		(u.Path == "" || u.Path == "/") && u.RawPath == "" && u.RawQuery == "" &&
		u.Fragment == "" && u.Opaque == "" && !u.ForceQuery &&
		!strings.ContainsAny(raw, "\\\x00\r\n\t ")
}

// InvocationText preserves the legacy HogQL/QUERY/REST input forms. Every
// normalized request is subsequently validated by both gateway and adapter.
func InvocationText(kind, input string) (operations.Kind, *operations.NativeSpec, error) {
	kind = strings.ToLower(kind)
	if kind == "mongodb" {
		return MongoInvocationText(input)
	}
	if kind == "elasticsearch" {
		return ParseElasticsearchInvocation(input)
	}
	if kind == "redis" {
		if strings.HasPrefix(strings.TrimSpace(input), "{") {
			return Invocation(kind, []byte(input))
		}
		q, err := ParseRedisText(input)
		if err != nil {
			return "", nil, err
		}
		raw, err := json.Marshal(q)
		if err != nil {
			return "", nil, operations.ErrInvalid
		}
		return Invocation(kind, raw)
	}
	if SaaS(kind) {
		if strings.HasPrefix(strings.TrimSpace(input), "{") {
			return Invocation(kind, []byte(input))
		}
		raw, err := json.Marshal(SaaSQuery{SQL: input})
		if err != nil {
			return "", nil, operations.ErrInvalid
		}
		return Invocation(kind, raw)
	}
	if strings.ToLower(kind) != "posthog" {
		return Invocation(kind, []byte(input))
	}
	raw := strings.TrimSpace(input)
	if strings.HasPrefix(raw, "{") {
		return Invocation(kind, []byte(raw))
	}
	first, remainder, _ := strings.Cut(raw, " ")
	var q PostHogQuery
	switch strings.ToUpper(first) {
	case "SELECT", "WITH":
		q.SQL = raw
	case "QUERY":
		q.Query = json.RawMessage(strings.TrimSpace(remainder))
	case "GET", "POST", "PUT", "PATCH", "DELETE":
		q.Method = strings.ToUpper(first)
		path, body, _ := strings.Cut(strings.TrimSpace(remainder), " ")
		q.Path = path
		if strings.TrimSpace(body) != "" {
			q.Body = json.RawMessage(strings.TrimSpace(body))
		}
	default:
		return "", nil, operations.ErrInvalid
	}
	b, err := json.Marshal(q)
	if err != nil {
		return "", nil, operations.ErrInvalid
	}
	return Invocation(kind, b)
}

func ParsePostHog(raw []byte) (PostHogQuery, operations.Kind, error) {
	var q PostHogQuery
	invalid := func() (PostHogQuery, operations.Kind, error) { return q, "", operations.ErrInvalid }
	if operations.DecodeStrict(raw, &q, operations.MaxRequestBytes) != nil {
		return invalid()
	}
	if q.SQL != "" {
		if q.Query != nil || q.Method != "" || q.Path != "" || q.Body != nil || strings.TrimSpace(q.SQL) == "" {
			return invalid()
		}
		return q, operations.NativeRead, nil
	}
	if q.Query != nil {
		if q.Method != "" || q.Path != "" || q.Body != nil {
			return invalid()
		}
		var envelope struct {
			Query map[string]json.RawMessage `json:"query"`
		}
		if operations.DecodeStrict(q.Query, &envelope, operations.MaxRequestBytes) != nil || envelope.Query == nil {
			return invalid()
		}
		var kind string
		if json.Unmarshal(envelope.Query["kind"], &kind) != nil {
			return invalid()
		}
		// Only documented analytical query nodes are read authority. Never
		// accept arbitrary API commands hidden in a nested query wrapper.
		switch kind {
		case "HogQLQuery", "EventsQuery", "PersonsNode", "TrendsQuery", "FunnelsQuery", "RetentionQuery", "PathsQuery", "StickinessQuery":
		default:
			return invalid()
		}
		return q, operations.NativeRead, nil
	}
	project := "1"
	if parts := strings.Split(q.Path, "/"); len(parts) > 3 && ValidProject(parts[3]) {
		project = parts[3]
	}
	if _, err := PostHogPath(q.Path, project); err != nil {
		return invalid()
	}
	switch q.Method {
	case "GET":
		if q.Body != nil {
			return invalid()
		}
		return q, operations.NativeRead, nil
	case "POST", "PUT", "PATCH", "DELETE":
		if q.Body != nil {
			var body map[string]json.RawMessage
			if operations.DecodeStrict(q.Body, &body, operations.MaxRequestBytes) != nil || body == nil {
				return invalid()
			}
		}
		return q, operations.NativeExecute, nil
	}
	return invalid()
}

// PostHogPath only accepts the literal placeholder form. Encoded path separators,
// traversal and cross-project requests fail before credentials are sent.
func PostHogPath(raw, project string) (string, error) {
	prefix := "/api/projects/:project_id/"
	if strings.HasPrefix(raw, "/api/projects/"+project+"/") {
		prefix = "/api/projects/" + project + "/"
	}
	if !ValidProject(project) || !strings.HasPrefix(raw, prefix) || len(raw) > 8192 || strings.ContainsAny(raw, "\\\x00\r\n\t ") {
		return "", operations.ErrInvalid
	}
	u, err := url.Parse(raw)
	if err != nil || u.IsAbs() || u.Host != "" || u.Fragment != "" || u.RawPath != "" || u.Opaque != "" || !strings.HasPrefix(u.Path, prefix) {
		return "", operations.ErrInvalid
	}
	for _, part := range strings.Split(strings.TrimPrefix(u.Path, prefix), "/") {
		if part == "." || part == ".." || strings.ContainsAny(part, ":%") {
			return "", operations.ErrInvalid
		}
	}
	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", operations.ErrInvalid
	}
	for key, v := range values {
		if len(v) != 1 || strings.EqualFold(key, "access_token") || strings.EqualFold(key, "api_key") || strings.EqualFold(key, "project_id") || strings.EqualFold(key, "team_id") {
			return "", operations.ErrInvalid
		}
	}
	u.Path = strings.Replace(u.Path, prefix, "/api/projects/"+project+"/", 1)
	return u.String(), nil
}

func posthogInvocation(raw []byte) (operations.Kind, *operations.NativeSpec, error) {
	_, kind, err := ParsePostHog(raw)
	if err != nil {
		return "", nil, err
	}
	return kind, &operations.NativeSpec{Provider: "posthog", Command: "query", Parameters: []operations.Parameter{{Type: "json", Value: append(json.RawMessage(nil), raw...)}}, ReturnResult: kind == operations.NativeExecute}, nil
}
