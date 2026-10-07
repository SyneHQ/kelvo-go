// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// LookupOperation reads the existing ledger entry after a submission response
// was lost. Absence or expiry remains uncertain and never permits resubmission.
func (c *Client) LookupOperation(ctx context.Context, key, digest string, auth Authority) (operations.Response, error) {
	request := operations.LookupRequest{Version: operations.Version, IdempotencyKey: key, RequestSHA256: digest}
	if request.Validate() != nil {
		return operations.Response{}, failure("INVALID_ARGUMENT")
	}
	ctx, release, err := c.operationControlContext(ctx, auth)
	if err != nil {
		return operations.Response{}, uncertainOperation("", digest, key, err)
	}
	defer release()
	raw, _ := json.Marshal(request)
	response, err := c.doOperation(ctx, http.MethodPost, "/v1/operations/lookup", auth, raw)
	if err != nil {
		return operations.Response{}, uncertainOperation("", digest, key, err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return operations.Response{}, uncertainOperation("", digest, key, statusFailure(response.StatusCode))
	}
	status, err := decodeOperationResponse(response, "", digest)
	if err != nil {
		return status, uncertainOperation(status.ID, digest, key, err)
	}
	return status, nil
}
