package nativereader

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
	"github.com/apache/arrow-go/v18/arrow"
)

func (s *Session) elasticClient(ctx context.Context, rows, bytes int64) (*cloudapi.Client, map[string]string, error) {
	limits, err := s.limits(ctx, rows, bytes)
	if err != nil {
		return nil, nil, err
	}
	token := s.spec.Token
	auth := s.spec.Options["authentication"]
	if s.spec.Username != "" {
		auth = "Basic"
		token = base64.StdEncoding.EncodeToString([]byte(s.spec.Username + ":" + s.spec.Password))
	}
	c, err := cloudapi.NewResolved(s.source, limits, cloudapi.Credentials{URL: s.spec.URL, Token: token, TLS: s.tls})
	return c, map[string]string{"Authorization": auth + " " + token}, err
}

func (s *Session) RunNative(ctx context.Context, n adapter.Native, sink adapter.Sink) (result adapter.NativeResult, err error) {
	result = adapter.NativeResult{Outcome: operations.Rejected, Effect: operations.EffectNone}
	if s == nil || ctx == nil || s.spec.Engine != "elasticsearch" || n.Validate() != nil || n.Spec.Provider != "elasticsearch" || n.Spec.Command != "query" || len(n.Spec.Parameters) != 1 || n.Spec.Parameters[0].Type != "json" {
		return result, adapter.ErrUnsupported
	}
	q, kind, err := provider.ParseElasticsearch(n.Spec.Parameters[0].Value)
	if err != nil || kind != n.Kind || provider.ElasticsearchScope(q, s.spec.Database) != nil {
		return result, adapter.ErrUnsupported
	}
	wants := kind == operations.NativeRead || n.Spec.ReturnResult
	if wants != (sink != nil) {
		return result, adapter.ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		result.Outcome = operations.CancelledBeforeStart
		return result, err
	}
	c, headers, err := s.elasticClient(ctx, n.Limits.MaxRows, n.Limits.MaxBytes)
	if err != nil {
		return result, err
	}
	defer c.Close()
	relative, _ := url.Parse(q.Path)
	destination := *c.Origin
	destination.Path = relative.Path
	destination.RawQuery = relative.RawQuery
	req, err := http.NewRequestWithContext(ctx, q.Method, destination.String(), strings.NewReader(q.Body))
	if err != nil {
		return result, adapter.ErrInvalid
	}
	req.GetBody = nil
	req.Header.Set("Authorization", headers["Authorization"])
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if strings.HasSuffix(relative.Path, "/_bulk") {
		req.Header.Set("Content-Type", "application/x-ndjson")
	}
	var dispatched atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteHeaders: func() { dispatched.Store(true) }}))
	response, err := c.HTTP.Do(req)
	result.Outcome = operations.Failed
	if kind.Mutating() && dispatched.Load() {
		result.Outcome, result.Effect = operations.OutcomeUnknown, operations.EffectUnknown
	}
	if err != nil {
		return result, adapter.ErrInvalid
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if kind.Mutating() && (response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusConflict) {
			result.Outcome, result.Effect = operations.Failed, operations.EffectNone
		}
		return result, adapter.ErrInvalid
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return result, adapter.ErrInvalid
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, min(c.Limit, n.Limits.MaxBytes, 8<<20)+1))
	if err != nil || int64(len(body)) > min(c.Limit, n.Limits.MaxBytes, 8<<20) {
		return result, adapter.ErrLimit
	}
	documents, err := elasticDocuments(body, response.StatusCode)
	if err != nil {
		return result, err
	}
	if kind.Mutating() {
		if len(documents) != 1 {
			return result, adapter.ErrInvalid
		}
		result.Outcome, result.Effect, err = elasticWriteOutcome(relative.Path, documents[0])
		if err != nil {
			return result, err
		}
	}
	if wants {
		result.Stats, err = s.writeDocuments(ctx, documents, n.Limits, sink)
		if err != nil {
			return result, err
		}
	}
	if !kind.Mutating() {
		result.Outcome = operations.Completed
	}
	return result, nil
}

func elasticDocuments(body []byte, status int) ([]json.RawMessage, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		raw, _ := json.Marshal(map[string]int{"status": status})
		return []json.RawMessage{raw}, nil
	}
	if body[0] != '{' && body[0] != '[' {
		raw, _ := json.Marshal(map[string]string{"response": string(body)})
		return []json.RawMessage{raw}, nil
	}
	var raw json.RawMessage
	if provider.DecodeDocument(body, &raw, 8<<20) != nil {
		return nil, adapter.ErrInvalid
	}
	if body[0] == '[' {
		var rows []json.RawMessage
		if json.Unmarshal(raw, &rows) != nil {
			return nil, adapter.ErrInvalid
		}
		for _, row := range rows {
			var object map[string]json.RawMessage
			if json.Unmarshal(row, &object) != nil || object == nil {
				return nil, adapter.ErrInvalid
			}
		}
		return rows, nil
	}
	return []json.RawMessage{raw}, nil
}
func elasticWriteOutcome(path string, raw json.RawMessage) (operations.Outcome, operations.Effect, error) {
	unknown := func() (operations.Outcome, operations.Effect, error) {
		return operations.OutcomeUnknown, operations.EffectUnknown, adapter.ErrInvalid
	}
	var document map[string]json.RawMessage
	if json.Unmarshal(raw, &document) != nil || document == nil {
		return unknown()
	}
	if value, ok := document["error"]; ok && string(value) != "null" {
		return unknown()
	}
	if value, ok := document["acknowledged"]; ok && string(value) != "true" {
		return unknown()
	}
	if strings.HasSuffix(path, "/_bulk") {
		var response struct {
			Errors *bool `json:"errors"`
			Items  []map[string]struct {
				Status int `json:"status"`
			} `json:"items"`
		}
		if json.Unmarshal(raw, &response) != nil || response.Errors == nil || len(response.Items) == 0 || len(response.Items) > 1000 {
			return unknown()
		}
		succeeded, failed := 0, 0
		for _, item := range response.Items {
			if len(item) != 1 {
				return unknown()
			}
			for _, effect := range item {
				if effect.Status >= 200 && effect.Status < 300 {
					succeeded++
				} else if effect.Status >= 400 && effect.Status < 500 {
					failed++
				} else {
					return unknown()
				}
			}
		}
		if *response.Errors != (failed > 0) {
			return unknown()
		}
		if failed > 0 {
			effect := operations.EffectNone
			if succeeded > 0 {
				effect = operations.EffectPartial
			}
			return operations.Failed, effect, adapter.ErrInvalid
		}
		return operations.Completed, operations.EffectCommitted, nil
	}
	if strings.Contains(path, "/_doc/") || strings.Contains(path, "/_create/") || strings.Contains(path, "/_update/") || strings.HasSuffix(path, "/_doc") {
		var result string
		if json.Unmarshal(document["result"], &result) != nil {
			return unknown()
		}
		switch result {
		case "created", "updated", "deleted":
			return operations.Completed, operations.EffectCommitted, nil
		case "noop", "not_found":
			return operations.Completed, operations.EffectNone, nil
		default:
			return unknown()
		}
	}
	if string(document["acknowledged"]) != "true" {
		return unknown()
	}
	return operations.Completed, operations.EffectCommitted, nil
}
func (s *Session) writeDocuments(ctx context.Context, rows []json.RawMessage, limits adapter.Limits, sink adapter.Sink) (stats adapter.QueryStats, err error) {
	started := time.Now()
	defer func() { stats.Elapsed = time.Since(started) }()
	l, err := s.limits(ctx, limits.MaxRows, limits.MaxBytes)
	if err != nil {
		return stats, err
	}
	if int64(len(rows)) > limits.MaxRows {
		return stats, adapter.ErrLimit
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "document", Type: arrow.BinaryTypes.Binary}}, func() *arrow.Metadata {
		m := arrow.MetadataFrom(map[string]string{"kelvo_document_format": provider.ResultFormat})
		return &m
	}())
	writer, err := rowarrow.NewWriter(schema, l, splitSink{next: sink, rows: int64(limits.BatchRows)})
	if err != nil {
		return stats, err
	}
	defer writer.Close()
	for _, raw := range rows {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		if err = writer.Write([]any{[]byte(raw)}); err != nil {
			return stats, err
		}
	}
	result, err := writer.Finish()
	stats.Rows, stats.Bytes = result.Rows, result.Bytes
	return stats, err
}
