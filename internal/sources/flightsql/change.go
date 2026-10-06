// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package flightsql

import (
	"context"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	flightSQL "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// ApplyStatement uses Flight SQL's update command, never the result-query path.
// The caller validates authorization and autocommit statement scope first.
func (e *Engine) ApplyStatement(parent context.Context, sql string) (*int64, error) {
	if e == nil || parent == nil || strings.TrimSpace(sql) == "" || len(sql) > 1<<20 || len(e.sources) != 1 {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid statement")
	}
	var ep endpoint
	var token string
	var err error
	if e.resolved != nil {
		ep, err = parseEndpoint(e.resolved.URL)
		if err == nil {
			token, err = validToken(e.resolved.Token)
		}
	} else {
		for _, source := range e.sources {
			ep, err = parseEndpointEnv(source.URLEnv)
			if err == nil {
				token, err = tokenEnv(source.TokenEnv)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	client, err := flightSQL.NewClient(ep.address, nil, nil, grpc.WithTransportCredentials(credentials.NewTLS(e.tlsConfig(ep))), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMessage(e.limits))))
	if err != nil {
		return nil, query.NewError("QUERY_FAILED", "Flight SQL update connection failed")
	}
	defer client.Client.Close()
	count, err := client.ExecuteUpdate(ctx, sql)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, query.NewError("QUERY_FAILED", "Flight SQL update acknowledgement failed")
	}
	if count < -1 {
		return nil, query.NewError("QUERY_FAILED", "Invalid Flight SQL affected row count")
	}
	if count == -1 {
		return nil, nil
	}
	return &count, nil
}
