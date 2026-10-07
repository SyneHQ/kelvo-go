// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query

import "time"

// A source cancellation may need a fresh proxy connection and verified TLS
// handshake. Keep the worker alive through that bounded network cleanup.
// Cancellation is best effort when the source cannot be reached.
const (
	SourceCancellationGrace = 5 * time.Second
	WorkerCancellationGrace = SourceCancellationGrace + time.Second
	WorkerWaitDelay         = WorkerCancellationGrace + time.Second
)
