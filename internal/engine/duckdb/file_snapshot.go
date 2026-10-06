//go:build duckdb_arrow

package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// NewFileSnapshot accepts one already verified local snapshot. It grants no
// file transport or credential authority; the caller owns those boundaries.
func NewFileSnapshot(source catalog.Source, limits query.Limits) (*Engine, error) {
	if source.Path == "" || source.Adapter != "" || source.Federation != nil || source.LocalSnapshot != nil || source.ObjectSnapshot != nil || source.Object != nil || source.Range != nil || len(source.Ranges) != 0 || len(source.ParquetPaths) != 0 || len(source.Options) != 0 || source.DSNEnv != "" || source.URLEnv != "" || source.TokenEnv != "" || source.UsernameEnv != "" || source.PasswordEnv != "" {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid file snapshot")
	}
	switch source.Type {
	case "csv", "parquet", "json", "jsonl", "duckdb":
	default:
		return nil, query.NewError("UNSUPPORTED", "Unsupported file snapshot format")
	}
	e, err := New(catalog.Config{Sources: []catalog.Source{source}}, limits)
	if err != nil {
		return nil, err
	}
	e.fileSnapshot = true
	return e, nil
}

func attachFileSnapshot(ctx context.Context, raw any, source catalog.Source) error {
	executor, ok := raw.(driver.ExecerContext)
	if !ok {
		return errors.New("trusted file setup unavailable")
	}
	path := "'" + quoteLiteral(source.Path) + "'"
	id := quoteIdentifier(source.ID)
	var statement string
	switch source.Type {
	case "csv":
		// CSV has no declared numeric schema. Preserve exact cell text; callers
		// choose DECIMAL/integer/date casts instead of automatic float rounding.
		statement = "CREATE VIEW " + id + " AS SELECT * FROM read_csv_auto(" + path + ", all_varchar=true, ignore_errors=false, buffer_size=1048576, maximum_line_size=262144, parallel=false, sample_size=2048)"
	case "parquet":
		statement = "CREATE VIEW " + id + " AS SELECT * FROM read_parquet(" + path + ")"
	case "json", "jsonl":
		format := "auto"
		if source.Type == "jsonl" {
			format = "newline_delimited"
		}
		// Opaque JSON text keeps nested values and numeric lexemes intact.
		statement = "CREATE VIEW " + id + " AS SELECT CAST(json AS VARCHAR) AS document FROM read_json_objects(" + path + ", format='" + format + "', ignore_errors=false)"
	case "duckdb":
		statement = "ATTACH " + path + " AS " + id + " (READ_ONLY)"
	default:
		return errors.New("unsupported file source")
	}
	if _, err := executor.ExecContext(ctx, statement, nil); err != nil {
		return err
	}
	if source.Type == "duckdb" {
		_, err := executor.ExecContext(ctx, "USE "+id, nil)
		return err
	}
	return nil
}
