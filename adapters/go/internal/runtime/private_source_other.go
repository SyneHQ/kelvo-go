//go:build !linux

package runtime

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func openPrivateSource(context.Context, adapter.ConnectionSpec, operations.Request, *adapter.PrivateTransport) (adapter.Session, func() error, error) {
	return nil, nil, adapter.ErrUnsupported
}
