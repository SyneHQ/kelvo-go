//go:build slim

package runtime

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// The slim build serves warm reads of relational engines only. Any other
// engine is unsupported here; the node then runs it on the full adapter.
func openSource(ctx context.Context, spec adapter.ConnectionSpec, request operations.Request) (adapter.Session, error) {
	connection, err := sourceConnection(spec)
	if err != nil {
		return nil, err
	}
	registry, err := New(relationalDrivers()...)
	if err != nil {
		return nil, err
	}
	return registry.Open(ctx, connection, request)
}

func sourceConnection(spec adapter.ConnectionSpec) (adapter.Connection, error) {
	c := adapter.Connection{TenantID: spec.TenantID, ConnectionID: spec.ConnectionID, Revision: spec.Revision, Engine: spec.Engine, Namespace: spec.Database, Schema: spec.Schema}
	if c.Engine == "postgres" {
		c.Engine = "postgresql"
	}
	for key := range spec.Options {
		if key != "tls_ca_pem" && key != "tls_server_name" {
			return c, adapter.ErrUnsupported
		}
	}
	c, err := relationalConnection(spec, c)
	if err != nil {
		return c, err
	}
	return connectionTLS(spec, c)
}
