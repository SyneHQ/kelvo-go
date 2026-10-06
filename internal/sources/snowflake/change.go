package snowflake

import (
	"context"
	"net/http"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/google/uuid"
)

// ApplyStatement executes one caller-authorized autocommit statement. Read
// execution remains guarded separately. A failed acknowledgement is never retried.
func (e *Engine) ApplyStatement(parent context.Context, sql string) (_ *int64, err error) {
	if e == nil || parent == nil || len(sql) == 0 || len(sql) > 1<<20 {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid statement")
	}
	client := *e.client
	client.Limit = min(client.Limit, 1<<20)
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	body := map[string]any{"statement": sql, "timeout": int64((e.limits.Timeout + time.Second - 1) / time.Second), "parameters": map[string]string{"MULTI_STATEMENT_COUNT": "1", "AUTOCOMMIT": "true"}}
	for _, key := range []string{"database", "schema", "warehouse", "role"} {
		if v := e.source.Options[key]; v != "" {
			body[key] = v
		}
	}
	var result response
	status, wire, err := client.Do(ctx, http.MethodPost, endpoint+"?async=true&requestId="+uuid.NewString(), body, e.headers, &result)
	if err != nil {
		return nil, err
	}
	id := result.Handle
	if !handlePattern.MatchString(id) {
		return nil, query.NewError("QUERY_FAILED", "Invalid statement handle")
	}
	defer func() {
		if err != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _, _ = client.Do(cleanup, http.MethodPost, endpoint+"/"+id+"/cancel", nil, e.headers, nil)
		}
	}()
	for polls := 0; status == http.StatusAccepted; polls++ {
		if polls >= 1000 || wire > 8<<20 {
			return nil, query.NewError("RESOURCE_EXHAUSTED", "Statement acknowledgement budget exceeded")
		}
		if err = cloudapi.Poll(ctx); err != nil {
			return nil, err
		}
		var next response
		var n int64
		status, n, err = client.Do(ctx, http.MethodGet, endpoint+"/"+id, nil, e.headers, &next)
		wire += n
		if err != nil {
			return nil, err
		}
		if next.Handle != id {
			return nil, query.NewError("QUERY_FAILED", "Statement handle changed")
		}
		result = next
	}
	if status != http.StatusOK || result.Code != "090001" || len(result.Handles) != 0 {
		return nil, query.NewError("QUERY_FAILED", "Statement completion was not acknowledged")
	}
	return nil, nil
}
