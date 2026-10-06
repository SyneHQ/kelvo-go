package oracle

import (
	"context"
	"database/sql"
	"strings"
)

func Inspect(ctx context.Context, db *sql.DB, engine, database, kind, id string) (Result, error) {
	if strings.ToLower(engine) != "oracle" {
		return Result{}, ErrUnsupported
	}
	return inspectOracle(ctx, db, database, "", kind, id, Limit)
}
