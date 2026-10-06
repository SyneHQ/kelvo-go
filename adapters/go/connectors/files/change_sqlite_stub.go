//go:build !cgo

package files

import (
	"context"
	"database/sql"
	"github.com/SYNEHQ/kelvo-go/adapter"
)

func (s *Session) openMutation(ctx context.Context, path string) (*sql.DB, error) {
	if s.descriptor.Format == "duckdb" {
		return s.openDuckMutation(ctx, path)
	}
	return nil, adapter.ErrUnsupported
}
