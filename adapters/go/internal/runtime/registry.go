// Package runtime owns optional connector selection. Registries are immutable;
// no registry lock is held while a driver opens a network connection.
package runtime

import (
	"context"
	"errors"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type Registry struct{ drivers map[string]adapter.Driver }

func New(drivers ...adapter.Driver) (*Registry, error) {
	registry := &Registry{drivers: make(map[string]adapter.Driver, len(drivers))}
	for _, driver := range drivers {
		if driver == nil {
			return nil, errors.New("nil adapter driver")
		}
		capabilities := driver.Capabilities()
		if err := capabilities.Validate(); err != nil {
			return nil, err
		}
		if _, exists := registry.drivers[capabilities.Engine]; exists {
			return nil, errors.New("duplicate adapter engine")
		}
		registry.drivers[capabilities.Engine] = driver
	}
	return registry, nil
}

// Open verifies advertised support before dialing. This is capability checking,
// not tenant authorization; callers must verify the current operation grant.
func (r *Registry) Open(ctx context.Context, connection adapter.Connection, operation operations.Request) (adapter.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if connection.TenantID == "" || connection.ConnectionID != operation.Connection.ID || connection.Revision == "" ||
		(operation.Connection.Database != "" && connection.Namespace != operation.Connection.Database) ||
		(operation.Connection.Schema != "" && connection.Schema != operation.Connection.Schema) {
		return nil, adapter.ErrInvalid
	}
	driver, exists := r.drivers[connection.Engine]
	if !exists {
		return nil, adapter.ErrUnsupported
	}
	if err := driver.Capabilities().Supports(operation); err != nil {
		return nil, err
	}
	return driver.Open(ctx, connection)
}
