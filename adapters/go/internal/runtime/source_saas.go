package runtime

import (
	"context"
	"maps"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/saas"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func openSaaSSource(ctx context.Context, spec adapter.ConnectionSpec, request operations.Request) (adapter.Session, error) {
	if err := adapter.ValidateSaaSProcessSource(spec); err != nil {
		return nil, err
	}
	registry, err := New(saas.Driver{Engine: spec.Engine})
	if err != nil {
		return nil, err
	}
	return registry.Open(ctx, adapter.Connection{TenantID: spec.TenantID, ConnectionID: spec.ConnectionID, Revision: spec.Revision, Engine: spec.Engine, Namespace: spec.Database, Token: spec.Token, Username: spec.Username, Endpoint: spec.URL, Options: maps.Clone(spec.Options)}, request)
}
