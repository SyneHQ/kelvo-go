// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"net/http"
)

// Ready checks a cluster gateway's readiness over the configured verified TLS
// connection. It uses bounded control admission and does not submit a query.
// A successful health snapshot reserves no capacity and does not validate source
// credentials or the caller's permission to execute a particular query.
func (c *Client) Ready(ctx context.Context) error {
	ctx, done, err := c.begin(ctx, true)
	if err != nil {
		return err
	}
	defer done()
	response, err := c.do(ctx, http.MethodGet, "/ready", Authority{BearerToken: c.token}, nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return statusFailure(response.StatusCode)
	}
	var body struct {
		Status string `json:"status"`
	}
	err = decodeJSON(response, &body, 1024)
	if ctx.Err() != nil {
		return contextFailure(ctx)
	}
	if err != nil || body.Status != "ready" {
		return failure("PROTOCOL_ERROR")
	}
	return nil
}
