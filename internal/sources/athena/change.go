package athena

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
)

func (e *Engine) changeBody(sql, token string) map[string]any {
	return map[string]any{"ClientRequestToken": token, "QueryString": sql, "WorkGroup": e.s.Options["workgroup"], "QueryExecutionContext": map[string]string{"Database": e.s.Options["database"]}, "ResultConfiguration": map[string]string{"OutputLocation": e.s.Options["output_location"]}, "ResultReuseConfiguration": map[string]any{"ResultReuseByAgeConfiguration": map[string]bool{"Enabled": false}}}
}

// Match the transport's encoded-body budget before any batch statement runs.
func (e *Engine) ValidateStatement(sql string) error {
	if e == nil || len(sql) == 0 || len(sql) > 256<<10 {
		return query.NewError("INVALID_ARGUMENT", "Invalid Athena statement")
	}
	body, err := json.Marshal(e.changeBody(sql, strings.Repeat("x", 64)))
	if err != nil || len(body) > 256<<10 {
		return query.NewError("INVALID_ARGUMENT", "Athena statement exceeds request budget")
	}
	return nil
}

func (e *Engine) ApplyStatement(parent context.Context, sql string) (_ *int64, err error) {
	if e == nil || parent == nil {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid statement")
	}
	if err = e.ValidateStatement(sql); err != nil {
		return nil, err
	}
	client := *e.c
	client.Limit = min(client.Limit, 1<<20)
	ctx, cancel := context.WithTimeout(parent, e.l.Timeout)
	defer cancel()
	var token [32]byte
	if _, err = rand.Read(token[:]); err != nil {
		return nil, err
	}
	body := e.changeBody(sql, hex.EncodeToString(token[:]))
	var started struct {
		ID string `json:"QueryExecutionId"`
	}
	wire, err := client.Do(ctx, "AmazonAthena.StartQueryExecution", body, &started)
	if err != nil {
		return nil, err
	}
	if !executionID.MatchString(started.ID) {
		return nil, query.NewError("QUERY_FAILED", "Invalid query identity")
	}
	defer func() {
		if err != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _ = client.Do(cleanup, "AmazonAthena.StopQueryExecution", map[string]string{"QueryExecutionId": started.ID}, nil)
		}
	}()
	for polls := 0; polls < 1000 && wire <= 8<<20; polls++ {
		var result struct {
			QueryExecution *struct {
				ID     string `json:"QueryExecutionId"`
				Status struct {
					State string `json:"State"`
				} `json:"Status"`
			} `json:"QueryExecution"`
		}
		var n int64
		n, err = client.Do(ctx, "AmazonAthena.GetQueryExecution", map[string]string{"QueryExecutionId": started.ID}, &result)
		wire += n
		if err != nil {
			return nil, err
		}
		if result.QueryExecution == nil || result.QueryExecution.ID != started.ID {
			return nil, query.NewError("QUERY_FAILED", "Query identity changed")
		}
		switch result.QueryExecution.Status.State {
		case "SUCCEEDED":
			return nil, nil
		case "QUEUED", "RUNNING":
			if err = cloudapi.Poll(ctx); err != nil {
				return nil, err
			}
		default:
			return nil, query.NewError("QUERY_FAILED", "Statement completion was not acknowledged")
		}
	}
	return nil, query.NewError("RESOURCE_EXHAUSTED", "Statement acknowledgement budget exceeded")
}
