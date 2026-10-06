// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/ingestion"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// The caller verifies signed claims first. Internal jobs release upstream
// custody only after the runner has proved process and scratch cleanup.
func bindOperationCleanup(sink *operationResultSink, claims operations.GrantClaims, notify func(context.Context) error) {
	if claims.Authorization.Kind == "ingestion" || claims.Authorization.Kind == "watcher" {
		sink.cleaned = notify
	}
}

// Claims must already have passed VerifyGrant against the node policy. This
// binds the sealed batch to that authenticated upstream ingestion run before
// the private resolver can return database credentials.
func authorizeOperationIngestionRun(claims operations.GrantClaims, request operations.Request, payload []byte) error {
	if claims.Authorization.Kind != "ingestion" || request.Kind != operations.IngestionCommit {
		return nil
	}
	proof := claims.Authorization.Ingestion
	if proof == nil {
		return operations.ErrInvalid
	}
	batch, err := ingestion.ParseBatch(payload)
	if err != nil || batch.RunID != proof.RunID {
		return operations.ErrInvalid
	}
	return nil
}
