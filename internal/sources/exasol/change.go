package exasol

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// ApplyStatement is called only after the operation adapter validates a single
// autocommit statement. Reads keep their separate rollback-only session path.
func (e *Engine) ApplyStatement(parent context.Context, statement string) (*int64, error) {
	if parent == nil || e == nil {
		return nil, query.NewError("INVALID_REQUEST", "Invalid Exasol operation")
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	endpoint := url.URL{Scheme: "wss", Host: net.JoinHostPort(e.config.Host, strconv.Itoa(e.config.Port))}
	conn, response, err := e.dialer.DialContext(ctx, endpoint.String(), nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		return nil, public(ctx, "Could not connect to Exasol")
	}
	defer conn.Close()
	s := &session{conn: conn, responseLimit: min(e.responseLimit, 128<<10), wireLimit: 512 << 10, usable: true}
	if err := s.login(ctx, e.config, e.limits); err != nil {
		return nil, err
	}
	var handle *int64
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		if handle != nil {
			_ = s.exchange(cleanup, map[string]any{"command": "closeResultSet", "resultSetHandles": []int64{*handle}}, nil, true)
		}
		// An acknowledged autocommit remains acknowledged if disconnect fails.
		_ = s.exchange(cleanup, map[string]any{"command": "disconnect"}, nil, true)
	}()
	if err := s.exchange(ctx, map[string]any{"command": "setAttributes", "attributes": map[string]any{"autocommit": true}}, nil, false); err != nil {
		return nil, err
	}
	var applied sessionAttributes
	if err := s.exchange(ctx, map[string]any{"command": "getAttributes"}, &applied, false); err != nil {
		return nil, err
	}
	if applied.Autocommit == nil || !*applied.Autocommit || applied.TimestampUTC == nil || !*applied.TimestampUTC {
		return nil, query.NewError("QUERY_FAILED", "Exasol did not enable autocommit")
	}
	var result results
	if err := s.exchange(ctx, map[string]any{"command": "execute", "sqlText": statement, "attributes": map[string]any{"autocommit": true, "timestampUtcEnabled": true}}, &result, false); err != nil {
		return nil, err
	}
	if len(result.Results) == 1 && result.Results[0].Set != nil {
		handle = result.Results[0].Set.Handle
	}
	if result.Count != 1 || len(result.Results) != 1 || result.Results[0].Kind != "rowCount" || result.Results[0].RowCount == nil || *result.Results[0].RowCount < 0 || result.Results[0].Set != nil {
		return nil, query.NewError("QUERY_FAILED", "Exasol returned an invalid statement acknowledgement")
	}
	return result.Results[0].RowCount, nil
}
