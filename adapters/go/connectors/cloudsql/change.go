// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cloudsql

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"regexp"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/operations"
)

var cloudStatementID = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)

// Each request is one autocommit statement. A lost, malformed or incomplete
// response remains unknown; no transport or statement replay is performed.
func (s *Session) Execute(ctx context.Context, change adapter.Change) (adapter.ChangeResult, error) {
	result := adapter.ChangeResult{Outcome: "failed"}
	if s == nil || ctx == nil || change.Transaction || change.Role != "" || change.Isolation != "" || len(change.Parameters) != len(change.Statements) {
		return result, adapter.ErrUnsupported
	}
	// Validate every binding before dispatching any statement in the batch.
	for i, parameters := range change.Parameters {
		var err error
		switch s.source.Type {
		case "d1":
			_, err = cloudapi.D1Parameters(parameters)
		case "databricks":
			_, err = cloudapi.DatabricksParameters(parameters)
			if err == nil {
				_, err = cloudapi.DatabricksStatement(change.Statements[i], len(parameters))
			}
		default:
			err = adapter.ErrUnsupported
		}
		if err != nil {
			return result, err
		}
	}
	if err := sqlsession.Validate(s.source.Type, change.Statements, sqlsession.Options{}); err != nil {
		return result, adapter.ErrInvalid
	}
	limits, err := s.cloudLimits(ctx, 1, 64<<10)
	if err != nil {
		return result, err
	}
	client, err := cloudapi.NewResolved(s.source, limits, s.credentials)
	if err != nil {
		return result, err
	}
	defer client.Close()
	client.Limit = min(client.Limit, 64<<10)
	var affected int64
	known := true
	for i, statement := range change.Statements {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		requestCtx, cancel := context.WithTimeout(ctx, limits.Timeout)
		result.Attempted = i + 1
		var count *int64
		switch s.source.Type {
		case "d1":
			count, err = s.executeD1(requestCtx, client, statement, change.Parameters[i])
		case "databricks":
			count, err = s.executeDatabricks(requestCtx, client, statement, change.Parameters[i])
		default:
			err = adapter.ErrUnsupported
		}
		cancel()
		if err != nil {
			result.Outcome = "unknown"
			return result, err
		}
		result.Completed++
		if count == nil || *count < 0 || *count > math.MaxInt64-affected {
			known = false
		} else {
			affected += *count
		}
	}
	result.Outcome = "succeeded"
	if known {
		result.AffectedRows = &affected
	}
	return result, nil
}

func (s *Session) executeD1(ctx context.Context, client *cloudapi.Client, statement string, parameters []operations.Parameter) (*int64, error) {
	bound, err := cloudapi.D1Parameters(parameters)
	if err != nil {
		return nil, err
	}
	path := "/client/v4/accounts/" + s.source.Options["account_id"] + "/d1/database/" + s.source.Options["database_id"] + "/query"
	var response struct {
		Success bool              `json:"success"`
		Errors  []json.RawMessage `json:"errors"`
		Result  []struct {
			Success bool   `json:"success"`
			Error   string `json:"error"`
			Meta    struct {
				Changes *json.Number `json:"changes"`
			} `json:"meta"`
		} `json:"result"`
	}
	if _, _, err := client.Do(ctx, http.MethodPost, path, map[string]any{"sql": statement, "params": bound}, nil, &response); err != nil {
		return nil, err
	}
	if !response.Success || len(response.Errors) != 0 || len(response.Result) != 1 || !response.Result[0].Success || response.Result[0].Error != "" {
		return nil, adapter.ErrInvalid
	}
	if n := response.Result[0].Meta.Changes; n != nil {
		if count, err := n.Int64(); err == nil && count >= 0 {
			return &count, nil
		}
	}
	return nil, nil
}

type databricksChangeResponse struct {
	ID     string `json:"statement_id"`
	Status struct {
		State string `json:"state"`
	} `json:"status"`
}

func (s *Session) executeDatabricks(ctx context.Context, client *cloudapi.Client, statement string, parameters []operations.Parameter) (_ *int64, resultErr error) {
	bound, err := cloudapi.DatabricksParameters(parameters)
	if err != nil {
		return nil, err
	}
	statement, err = cloudapi.DatabricksStatement(statement, len(parameters))
	if err != nil {
		return nil, err
	}
	const endpoint = "/api/2.0/sql/statements"
	body := map[string]any{"statement": statement, "parameters": bound, "warehouse_id": s.source.Options["warehouse_id"], "format": "JSON_ARRAY", "disposition": "INLINE", "wait_timeout": "0s", "row_limit": 1, "byte_limit": 64 << 10}
	for _, key := range []string{"catalog", "schema"} {
		if value := s.source.Options[key]; value != "" {
			body[key] = value
		}
	}
	var response databricksChangeResponse
	if _, _, err := client.Do(ctx, http.MethodPost, endpoint, body, nil, &response); err != nil {
		return nil, err
	}
	id := response.ID
	if !cloudStatementID.MatchString(id) {
		return nil, adapter.ErrInvalid
	}
	defer func() {
		if resultErr != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _, _ = client.Do(cleanup, http.MethodPost, endpoint+"/"+id+"/cancel", nil, nil, nil)
		}
	}()
	for response.Status.State == "PENDING" || response.Status.State == "RUNNING" {
		if err := cloudapi.Poll(ctx); err != nil {
			return nil, err
		}
		var next databricksChangeResponse
		if _, _, err := client.Do(ctx, http.MethodGet, endpoint+"/"+id, nil, nil, &next); err != nil {
			return nil, err
		}
		if next.ID != id {
			return nil, adapter.ErrInvalid
		}
		response = next
	}
	if response.Status.State != "SUCCEEDED" {
		return nil, adapter.ErrInvalid
	}
	return nil, nil
}
