// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"errors"
	"github.com/SYNEHQ/kelvo-go/internal/arrowipc"
)

// Keep worker error categories stable while sharing the same allocation-critical
// metadata checks with external Arrow Flight sources.
func validateIPCMetadata(data []byte) (int64, error) {
	n, err := arrowipc.ValidateMessageMetadata(data)
	if errors.Is(err, arrowipc.ErrLimit) {
		return 0, errIPCLimit
	}
	if err != nil {
		return 0, errInvalidIPC
	}
	return n, nil
}
