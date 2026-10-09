//go:build !linux

package worker

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
)

func newPrivateOperationChannel(context.Context, adapter.ProcessRequest, *transportbroker.Session, transportbroker.Binding, int, func()) (*privateOperationChannel, error) {
	return nil, adapter.ErrUnsupported
}
