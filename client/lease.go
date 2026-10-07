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

var _ resolver.LeaseVerifier = (*Client)(nil)

// ValidateConnectionLease verifies the current worker's custody before an
// application resolver releases credentials. It uses separate control admission.
func (c *Client) ValidateConnectionLease(ctx context.Context, id, grant string, binding resolver.Binding) (time.Time, error) {
	if !handleName.MatchString(id) || grant == "" {
		return time.Time{}, failure("INVALID_ARGUMENT")
	}
	return c.validateLease(ctx, "/v1/queries/"+id+"/connection-lease", Authority{Delegation: grant}, "query", binding)
}

// ValidateOperationLease checks operation custody without consuming execution
// admission. The default bearer token must be allowed to check this tenant.
func (c *Client) ValidateOperationLease(ctx context.Context, id, grant string, binding resolver.Binding) (time.Time, error) {
	if !operations.ValidID(id) {
		return time.Time{}, failure("INVALID_ARGUMENT")
	}
	return c.validateLease(ctx, "/v1/operations/"+id+"/connection-lease", Authority{OperationGrant: grant}, "operation", binding)
}
func (c *Client) validateLease(ctx context.Context, path string, auth Authority, family string, binding resolver.Binding) (time.Time, error) {
	auth, err := c.authority(auth, family)
	if err != nil {
		return time.Time{}, err
	}
	if binding.Validate() != nil {
		return time.Time{}, failure("INVALID_ARGUMENT")
	}
	ctx, release, err := c.begin(ctx, true)
	if err != nil {
		return time.Time{}, err
	}
	defer release()
	raw, _ := json.Marshal(binding)
	response, err := c.do(ctx, http.MethodPost, path, auth, raw)
	if err != nil {
		return time.Time{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return time.Time{}, statusFailure(response.StatusCode)
	}
	var lease resolver.LeaseResponse
	if err = decodeJSON(response, &lease, 1024); err != nil {
		return time.Time{}, err
	}
	if ctx.Err() != nil {
		return time.Time{}, contextFailure(ctx)
	}
	now := time.Now().Unix()
	if lease.ValidUntil <= now || lease.ValidUntil > now+5 {
		return time.Time{}, failure("PERMISSION_DENIED")
	}
	return time.Unix(lease.ValidUntil, 0), nil
}
