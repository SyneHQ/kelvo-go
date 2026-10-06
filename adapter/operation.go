package adapter

import "github.com/SYNEHQ/kelvo-go/operations"

type Limits struct {
	MaxRows   int64
	MaxBytes  int64
	BatchRows int
}

// Invocation is a driver call, not authorization. The dispatcher validates the
// current operation grant and supported capabilities before invoking a session.
type Invocation struct {
	Query     *Query
	Change    *Change
	Metadata  *operations.MetadataSpec
	Ingestion *operations.IngestionSpec
	Native    *Native
	Watch     *operations.WatchSpec
	Migration *operations.MigrationSpec
	Test      bool
}

func FromOperation(request operations.Request, limits Limits) (Invocation, error) {
	pinned, err := operations.Clone(request)
	if err != nil {
		return Invocation{}, err
	}
	request = pinned
	switch request.Kind {
	case operations.ConnectionTest:
		return Invocation{Test: true}, nil
	case operations.MetadataInspect:
		if err := (Query{Statement: "metadata", MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}).Validate(); err != nil {
			return Invocation{}, err
		}
		return Invocation{Metadata: request.Spec.Metadata}, nil
	case operations.IngestionInstall, operations.IngestionState, operations.IngestionCommit:
		return Invocation{Ingestion: request.Spec.Ingestion}, nil
	case operations.MigrationStatus, operations.MigrationApply:
		if request.Spec.Migration.ID != "schema_migrations" || request.Kind == operations.MigrationApply && request.Spec.Migration.Transaction != operations.TransactionAutocommit {
			return Invocation{}, ErrUnsupported
		}
		return Invocation{Migration: request.Spec.Migration}, nil
	case operations.WatchInstall, operations.WatchRead, operations.WatchAck, operations.WatchRemove:
		if request.Spec.Watch.Mode != "native" {
			return Invocation{}, ErrUnsupported
		}
		return Invocation{Watch: request.Spec.Watch}, nil
	case operations.NativeRead, operations.NativeExecute:
		native := &Native{Kind: request.Kind, Spec: *request.Spec.Native, Limits: limits}
		if err := native.Validate(); err != nil {
			return Invocation{}, err
		}
		return Invocation{Native: native}, nil
	case operations.QueryRead:
		query := &Query{Statement: request.Spec.Query.SQL, Parameters: request.Spec.Query.Parameters,
			MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}
		if err := query.Validate(); err != nil {
			return Invocation{}, err
		}
		return Invocation{Query: query}, nil
	case operations.StatementExecute:
		statement := request.Spec.Statement
		change := &Change{Role: statement.Role, Isolation: statement.Isolation, Transaction: statement.Transaction == operations.TransactionRequired}
		items := []operations.BoundStatement{{SQL: statement.SQL, Parameters: statement.Parameters}}
		if statement.Batch != nil {
			items = statement.Batch.Statements
		}
		for _, item := range items {
			change.Statements = append(change.Statements, item.SQL)
			change.Parameters = append(change.Parameters, item.Parameters)
		}
		return Invocation{Change: change}, nil
	default:
		return Invocation{}, ErrUnsupported
	}
}
