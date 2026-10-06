// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package runtime

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/nativereader"
)

func openNativeReaderSource(ctx context.Context, input adapter.ProcessRequest) (adapter.Session, error) {
	if input.Validate() != nil || adapter.NativeReaderCapabilities(input.Source.Engine).Supports(input.Request) != nil {
		return nil, adapter.ErrUnsupported
	}
	return nativereader.Open(ctx, input.Source, input.Limits)
}
