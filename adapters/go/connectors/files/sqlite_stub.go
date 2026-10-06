//go:build !cgo

package files

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (*Session) readSQLite(context.Context, adapter.Query, adapter.Sink) (adapter.QueryStats, error) {
	return adapter.QueryStats{}, adapter.ErrUnsupported
}
func (*Session) inspectSQLite(context.Context, operations.MetadataSpec, adapter.Limits, adapter.Sink) (adapter.QueryStats, error) {
	return adapter.QueryStats{}, adapter.ErrUnsupported
}
