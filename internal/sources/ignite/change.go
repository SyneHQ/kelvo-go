package ignite

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// ApplyStatement receives one validated, parameter-free autocommit statement.
// Ignite SQL DML acknowledges an exact update count through its SQL cursor.
func (e *Engine) ApplyStatement(parent context.Context, statement string) (*int64, error) {
	if parent == nil || e == nil {
		return nil, query.NewError("INVALID_REQUEST", "Invalid Ignite operation")
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	form := url.Values{"cmd": {"qryfldexe"}, "qry": {statement}, "pageSize": {"1"}, "cacheName": {e.cacheName}}
	envelope, _, err := e.client.post(ctx, form, min(e.client.responseLimit, 128<<10))
	if err != nil {
		return nil, err
	}
	var result page
	decodeErr := decode(envelope.Response, &result)
	if result.QueryID != nil && (result.Last == nil || !*result.Last) {
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _, _ = e.client.post(cleanup, url.Values{"cmd": {"qrycls"}, "qryId": {strconv.FormatInt(*result.QueryID, 10)}}, 128<<10)
		}()
	}
	if decodeErr != nil || envelope.Status == nil || *envelope.Status != 0 || !emptyError(envelope.Error) || result.QueryID == nil || result.Last == nil || !*result.Last || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || len(result.Columns) != 1 || result.Columns[0].Type != "java.lang.Long" {
		return nil, query.NewError("QUERY_FAILED", "Ignite returned an invalid statement acknowledgement")
	}
	number, ok := result.Rows[0][0].(json.Number)
	if !ok {
		return nil, query.NewError("QUERY_FAILED", "Ignite omitted its update count")
	}
	count, err := number.Int64()
	if err != nil || count < 0 {
		return nil, query.NewError("QUERY_FAILED", "Ignite returned an invalid update count")
	}
	return &count, nil
}
