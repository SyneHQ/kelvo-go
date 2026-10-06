//go:build !duckdb_arrow

package sheets

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (*Session) execute(context.Context, adapter.Query, *operations.MetadataSpec, adapter.Sink) (adapter.QueryStats, error) {
	return adapter.QueryStats{}, adapter.ErrUnsupported
}
