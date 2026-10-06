package dynamodb

import (
	"context"
	"encoding/json"
	"regexp"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

var writeStart = regexp.MustCompile(`(?is)^(?:\s|--[^\n]*(?:\n|$)|/\*.*?\*/)*(insert|update|delete)\b`)

func (e *Engine) ValidateStatement(sql string) error {
	if len(sql) == 0 || len(sql) > 8192 || !writeStart.MatchString(sql) {
		return query.NewError("UNSUPPORTED", "DynamoDB requires one PartiQL item mutation")
	}
	return nil
}

// PartiQL writes affect one item. No scan, continuation, or statement replay is
// used to emulate a multi-item mutation.
func (e *Engine) ApplyStatement(parent context.Context, sql string) (*int64, error) {
	if e == nil || parent == nil || len(sql) == 0 || len(sql) > 8192 {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid PartiQL statement")
	}
	client := *e.c
	client.Limit = min(client.Limit, 1<<20)
	if err := e.ValidateStatement(sql); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, e.l.Timeout)
	defer cancel()
	var result struct {
		Next  string            `json:"NextToken"`
		Key   json.RawMessage   `json:"LastEvaluatedKey"`
		Items []json.RawMessage `json:"Items"`
	}
	_, err := client.Do(ctx, "DynamoDB_20120810.ExecuteStatement", map[string]any{"Statement": sql, "ReturnConsumedCapacity": "NONE"}, &result)
	if err != nil {
		return nil, err
	}
	if result.Next != "" || len(result.Key) != 0 || len(result.Items) != 0 {
		return nil, query.NewError("QUERY_FAILED", "Unexpected PartiQL mutation response")
	}
	// An acknowledged conditional update may affect no item; do not invent a count.
	return nil, nil
}
