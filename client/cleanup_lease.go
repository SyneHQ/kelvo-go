// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

var _ resolver.CleanupLeaseVerifier = (*Client)(nil)

// ValidateOperationCleanupLease checks retained cleanup custody through control
// admission. A successful call does not authorize another database operation.
func (c *Client) ValidateOperationCleanupLease(ctx context.Context, id, grant string, binding resolver.CleanupBinding) (resolver.CleanupLeaseResponse, error) {
	var lease resolver.CleanupLeaseResponse
	if !operations.ValidID(id) || binding.Validate() != nil {
		return lease, failure("INVALID_ARGUMENT")
	}
	auth, err := c.authority(Authority{OperationGrant: grant}, "operation")
	if err != nil {
		return lease, err
	}
	ctx, release, err := c.begin(ctx, true)
	if err != nil {
		return lease, err
	}
	defer release()
	raw, _ := json.Marshal(binding)
	response, err := c.do(ctx, http.MethodPost, "/v1/operations/"+id+"/cleanup-lease", auth, raw)
	if err != nil {
		return lease, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return lease, statusFailure(response.StatusCode)
	}
	if err = decodeJSON(response, &lease, 1024); err != nil {
		return resolver.CleanupLeaseResponse{}, err
	}
	if ctx.Err() != nil {
		return resolver.CleanupLeaseResponse{}, contextFailure(ctx)
	}
	if lease.ValidateAt(time.Now()) != nil {
		return resolver.CleanupLeaseResponse{}, failure("PERMISSION_DENIED")
	}
	return lease, nil
}
