//go:build !duckdb_arrow

package files

import (
	"context"
	"database/sql"
	"github.com/SYNEHQ/kelvo-go/adapter"
)

func (*Session) openDuckMutation(context.Context, string) (*sql.DB, error) {
	return nil, adapter.ErrUnsupported
}
