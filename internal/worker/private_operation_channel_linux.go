//go:build linux

package worker

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/childipc"
)

func newPrivateOperationChannel(ctx context.Context, input adapter.ProcessRequest, session *transportbroker.Session, binding transportbroker.Binding, maxStreams int, release func()) (*privateOperationChannel, error) {
	digest, err := privateInputDigest(input)
	if err != nil || session == nil || release == nil || !session.BoundTo(binding) || !privateInputScope(input, binding) {
		return nil, adapter.ErrInvalid
	}
	server, file, err := childipc.NewPair(ctx, binding.Authority, session.DataDialer(), session.CancellationDialer(), maxStreams)
	if err != nil {
		return nil, err
	}
	return &privateOperationChannel{inputSHA256: digest, server: server, file: file, session: session, binding: binding, release: release}, nil
}
