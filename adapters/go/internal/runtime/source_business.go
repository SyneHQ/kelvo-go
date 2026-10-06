// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package runtime

import (
	"context"
	"maps"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/business"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

func openBusinessSource(ctx context.Context, spec adapter.ConnectionSpec, request operations.Request) (adapter.Session, error) {
	if !provider.Supported(spec.Engine) || spec.DSN != "" || spec.URL != "" && spec.Engine != "posthog" || spec.Password != "" {
		return nil, adapter.ErrInvalid
	}
	registry, err := New(business.Driver{Engine: spec.Engine})
	if err != nil {
		return nil, err
	}
	return registry.Open(ctx, adapter.Connection{TenantID: spec.TenantID, ConnectionID: spec.ConnectionID, Revision: spec.Revision, Engine: spec.Engine, Namespace: spec.Database, Schema: spec.Schema, Token: spec.Token, Username: spec.Username, Endpoint: spec.URL, Options: maps.Clone(spec.Options)}, request)
}
