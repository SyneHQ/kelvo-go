package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
)

var unsafeChangeSettings = regexp.MustCompile(`(?i)\b(settings|format|outfile|returning)\b`)

// ClickHouse does not provide a transaction for an arbitrary statement batch.
// Each completed statement is reported, and any uncertain request stops it.
func (s *Session) Execute(ctx context.Context, change adapter.Change) (adapter.ChangeResult, error) {
	result := adapter.ChangeResult{Outcome: "failed"}
	if s == nil || s.client == nil || ctx == nil || change.Transaction || change.Role != "" || change.Isolation != "" {
		return result, adapter.ErrUnsupported
	}
	for _, parameters := range change.Parameters {
		if len(parameters) != 0 {
			return result, adapter.ErrUnsupported
		}
	}
	if err := sqlsession.Validate("clickhouse", change.Statements, sqlsession.Options{}); err != nil {
		return result, err
	}
	for _, statement := range change.Statements {
		if unsafeChangeSettings.MatchString(statement) {
			return result, adapter.ErrUnsupported
		}
	}
	for i, statement := range change.Statements {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		endpoint, err := url.Parse(s.source.URL)
		if err != nil {
			return result, adapter.ErrInvalid
		}
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return result, err
		}
		timeout := 30 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			timeout = min(timeout, time.Until(deadline))
		}
		if timeout <= 0 {
			return result, context.DeadlineExceeded
		}
		settings := endpoint.Query()
		settings.Set("query_id", "kelvo-"+hex.EncodeToString(id))
		settings.Set("wait_end_of_query", "1")
		settings.Set("buffer_size", "65536")
		settings.Set("async_insert", "0")
		settings.Set("mutations_sync", "2")
		settings.Set("max_execution_time", strconv.FormatInt(int64((timeout+time.Second-1)/time.Second), 10))
		endpoint.RawQuery = settings.Encode()
		requestCtx, cancel := context.WithTimeout(ctx, timeout)
		request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint.String(), strings.NewReader(statement))
		if err != nil {
			cancel()
			return result, adapter.ErrInvalid
		}
		request.SetBasicAuth(s.source.Username, s.source.Password)
		request.Header.Set("Content-Type", "text/plain; charset=utf-8")
		request.Header.Set("User-Agent", "kelvo-go")
		result.Attempted = i + 1
		response, err := s.client.Do(request)
		if err != nil {
			cancel()
			result.Outcome = "unknown"
			return result, adapter.ErrInvalid
		}
		count, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 4097))
		closeErr := response.Body.Close()
		requestErr := requestCtx.Err()
		cancel()
		if response.StatusCode != http.StatusOK || readErr != nil || closeErr != nil || count > 4096 || requestErr != nil {
			result.Outcome = "unknown"
			return result, adapter.ErrInvalid
		}
		result.Completed++
	}
	result.Outcome = "succeeded"
	return result, nil
}
