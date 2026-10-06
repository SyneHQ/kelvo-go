package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/migrations"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
)

var _ adapter.MigrationSession = (*Session)(nil)

func (s *Session) MigrationStatus(ctx context.Context) (migration.State, error) {
	runner, err := s.migrationRunner(ctx)
	if err != nil {
		return migration.State{}, err
	}
	return runner.Status(ctx)
}
func (s *Session) ApplyMigration(ctx context.Context, plan migration.Plan) (migration.Result, error) {
	result := migration.Result{Version: 1, From: plan.Expected, To: plan.Expected, Effect: operations.EffectNone, FilesApplied: []string{}}
	if plan.Validate() != nil {
		return result, migration.ErrInvalid
	}
	runner, err := s.migrationRunner(ctx)
	if err != nil {
		return result, err
	}
	return runner.Apply(ctx, plan)
}
func (s *Session) migrationRunner(ctx context.Context) (migrations.ClickHouse, error) {
	if s == nil || s.client == nil || ctx == nil || !migration.ValidServerUUID(s.migrationUUID) {
		return migrations.ClickHouse{}, adapter.ErrUnsupported
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return migrations.ClickHouse{}, err
	}
	id := "kelvo-migration-" + hex.EncodeToString(random)
	bound := false
	exchange := func(ctx context.Context, statement string, rows bool) ([][]json.RawMessage, error) {
		endpoint, err := url.Parse(s.source.URL)
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		settings := endpoint.Query()
		settings.Set("session_id", id)
		settings.Set("session_timeout", "300")
		if bound {
			settings.Set("session_check", "1")
		}
		settings.Set("wait_end_of_query", "1")
		settings.Set("buffer_size", "65536")
		settings.Set("async_insert", "0")
		settings.Set("mutations_sync", "2")
		settings.Set("max_execution_time", "60")
		settings.Set("output_format_json_quote_64bit_integers", "1")
		endpoint.RawQuery = settings.Encode()
		if rows {
			statement += " FORMAT JSONCompact"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(statement))
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		req.SetBasicAuth(s.source.Username, s.source.Password)
		req.Header.Set("Content-Type", "text/plain; charset=utf-8")
		req.Header.Set("User-Agent", "kelvo-go")
		response, err := s.client.Do(req)
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 65537))
		defer clear(raw)
		if err != nil || len(raw) > 65536 || response.StatusCode != http.StatusOK || ctx.Err() != nil {
			return nil, adapter.ErrInvalid
		}
		if !rows {
			if strings.TrimSpace(string(raw)) != "" {
				return nil, adapter.ErrInvalid
			}
			return nil, nil
		}
		var result struct {
			Data      [][]json.RawMessage `json:"data"`
			Rows      uint64              `json:"rows"`
			Exception string              `json:"exception"`
		}
		if json.Unmarshal(raw, &result) != nil || result.Exception != "" || result.Rows != uint64(len(result.Data)) || len(result.Data) > 16 {
			return nil, adapter.ErrInvalid
		}
		return result.Data, nil
	}
	// Creating the random server-local HTTP session and proving its UUID happen
	// in one request. Later session_check=1 never creates a session on failover.
	quote := strings.ReplaceAll(strings.ReplaceAll(s.database, "\\", "\\\\"), "'", "\\'")
	rows, err := exchange(ctx, "SELECT toString(serverUUID()),engine FROM system.databases WHERE name='"+quote+"'", true)
	if err != nil || len(rows) != 1 || len(rows[0]) != 2 {
		return migrations.ClickHouse{}, adapter.ErrUnsupported
	}
	var uuid, engine string
	if json.Unmarshal(rows[0][0], &uuid) != nil || json.Unmarshal(rows[0][1], &engine) != nil || uuid != s.migrationUUID || engine != "Atomic" {
		return migrations.ClickHouse{}, adapter.ErrUnsupported
	}
	bound = true
	return migrations.ClickHouse{Database: s.database, Query: func(ctx context.Context, q string) ([][]json.RawMessage, error) { return exchange(ctx, q, true) }, Exec: func(ctx context.Context, q string) error { _, err := exchange(ctx, q, false); return err }}, nil
}
