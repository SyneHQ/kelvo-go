package sqlsession

import (
	"context"
	"database/sql"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// PoolFactory belongs to service composition. It selects a pinned driver and
// builds a new pool from the current authorized credentials and namespace.
// It must never return a shared customer pool or use ambient credentials.
type PoolFactory func(context.Context, adapter.Connection) (*sql.DB, error)

type Driver struct {
	engine string
	open   PoolFactory
}

func NewDriver(engine string, open PoolFactory) (*Driver, error) {
	if open == nil || Validate(engine, []string{"UPDATE placeholder SET value=1"}, Options{}) != nil {
		return nil, adapter.ErrInvalid
	}
	return &Driver{engine: engine, open: open}, nil
}

func (d *Driver) Capabilities() operations.Capabilities {
	transactions := []operations.TransactionMode{operations.TransactionAutocommit}
	if d.engine != "clickhouse" {
		transactions = append(transactions, operations.TransactionRequired)
	}
	return operations.Capabilities{Version: operations.Version, Engine: d.engine, Operations: []operations.Capability{{
		Kind: operations.StatementExecute, Transactions: transactions, IsolationLevels: IsolationLevels(d.engine), Roles: true, Idempotency: "none", Cancellation: "best_effort",
		ParameterTypes: []string{"null", "string", "bool", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "decimal128", "decimal256", "binary", "date", "timestamp", "json"},
	}}}
}

func (d *Driver) Open(ctx context.Context, connection adapter.Connection) (adapter.Session, error) {
	if ctx == nil || connection.Engine != d.engine || connection.TenantID == "" || connection.ConnectionID == "" || connection.Revision == "" {
		return nil, adapter.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pool, err := d.open(ctx, connection)
	if err != nil {
		if pool != nil {
			_ = pool.Close()
		}
		return nil, err
	}
	if pool == nil {
		return nil, adapter.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		_ = pool.Close()
		return nil, err
	}
	return &Session{Pool: pool, Engine: d.engine}, nil
}
