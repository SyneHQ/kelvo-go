package cassandra

import (
	"context"
	"errors"
	"strconv"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

type discardSink struct{}

func (discardSink) Schema(*arrow.Schema) error    { return nil }
func (discardSink) Write(arrow.RecordBatch) error { return nil }

func (s *Session) Test(ctx context.Context) error {
	if s == nil || ctx == nil {
		return adapter.ErrInvalid
	}
	stats, err := s.Query(ctx, adapter.Query{Statement: "SELECT release_version FROM system.local LIMIT 1", MaxRows: 1, MaxBytes: 4096, BatchRows: 1}, discardSink{})
	if err == nil && stats.Rows != 1 {
		return errors.New("CQL connection test returned no row")
	}
	return err
}

func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	var stats adapter.QueryStats
	if s == nil || ctx == nil || sink == nil {
		return stats, adapter.ErrInvalid
	}
	statement, parameters, offset, err := s.metadataQuery(spec)
	if err != nil {
		return stats, err
	}
	query := adapter.Query{Statement: statement, MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}
	if err := query.Validate(); err != nil || int64(spec.Limit) > query.MaxRows {
		return stats, adapter.ErrLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backend == nil {
		return stats, adapter.ErrInvalid
	}
	iter := &metadataIterator{iterator: s.backend.Query(ctx, statement, parameters, min(limits.BatchRows, 1024)), namespace: s.namespace, object: spec.Object, skip: offset, limit: spec.Limit}
	return streamCQL(ctx, query, sink, iter)
}

// CQL has no OFFSET. Read at most 20,000 scoped catalog rows, skip a bounded
// prefix, and stream the requested page. No cross-keyspace catalog scan occurs.
func (s *Session) metadataQuery(spec operations.MetadataSpec) (string, []any, int, error) {
	if s.namespace == "" || spec.ObjectKind != "" || spec.Limit < 1 || spec.Limit > 10000 ||
		spec.Target.Catalog != "" && spec.Target.Catalog != s.namespace ||
		spec.Target.Schema != "" && spec.Target.Schema != s.namespace {
		return "", nil, 0, adapter.ErrInvalid
	}
	offset := 0
	if spec.Cursor != "" {
		var err error
		offset, err = strconv.Atoi(spec.Cursor)
		if err != nil || offset < 0 || offset > 10000 || strconv.Itoa(offset) != spec.Cursor {
			return "", nil, 0, adapter.ErrInvalid
		}
	}
	var statement string
	parameters := []any{s.namespace}
	switch spec.Object {
	case "catalogs", "databases", "schemas":
		if spec.Target.Name != "" {
			return "", nil, 0, adapter.ErrInvalid
		}
		statement = "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=?"
	case "tables":
		statement = "SELECT keyspace_name,table_name FROM system_schema.tables WHERE keyspace_name=?"
	case "columns":
		statement = "SELECT keyspace_name,table_name,column_name,type,kind FROM system_schema.columns WHERE keyspace_name=?"
	default:
		return "", nil, 0, adapter.ErrUnsupported
	}
	if spec.Target.Name != "" {
		statement += " AND table_name=?"
		parameters = append(parameters, spec.Target.Name)
	}
	statement += " LIMIT ?"
	parameters = append(parameters, offset+spec.Limit)
	return statement, parameters, offset, nil
}

type metadataIterator struct {
	iterator
	namespace string
	object    string
	skip      int
	limit     int
	position  int
	emitted   int
	err       error
}

func (i *metadataIterator) Columns() []column {
	switch i.object {
	case "catalogs", "databases":
		return []column{{"catalog", "text"}}
	case "schemas":
		return []column{{"catalog", "text"}, {"schema_name", "text"}}
	case "tables":
		return []column{{"catalog", "text"}, {"schema_name", "text"}, {"name", "text"}, {"type", "text"}}
	default:
		return []column{{"schema_name", "text"}, {"table_name", "text"}, {"name", "text"}, {"type", "text"}, {"position", "bigint"}, {"nullable", "text"}}
	}
}

func (i *metadataIterator) Scan(row map[string]any) bool {
	if i.err != nil || i.emitted >= i.limit {
		return false
	}
	for {
		raw := map[string]any{}
		if !i.iterator.Scan(raw) {
			return false
		}
		i.position++
		if raw["keyspace_name"] != i.namespace {
			i.err = adapter.ErrInvalid
			return false
		}
		if i.position <= i.skip {
			continue
		}
		text := func(name string) string {
			v, ok := raw[name].(string)
			if !ok || v == "" {
				i.err = adapter.ErrUnsupported
			}
			return v
		}
		switch i.object {
		case "catalogs", "databases":
			row["catalog"] = i.namespace
		case "schemas":
			row["catalog"], row["schema_name"] = i.namespace, i.namespace
		case "tables":
			row["catalog"], row["schema_name"] = i.namespace, i.namespace
			row["name"], row["type"] = text("table_name"), "BASE TABLE"
		case "columns":
			row["schema_name"], row["table_name"] = i.namespace, text("table_name")
			row["name"], row["type"], row["position"] = text("column_name"), text("type"), int64(i.position)
			row["nullable"] = "YES"
			switch text("kind") {
			case "partition_key", "clustering":
				row["nullable"] = "NO"
			}
		}
		if i.err != nil {
			return false
		}
		i.emitted++
		return true
	}
}

func (i *metadataIterator) Close() error {
	err := i.iterator.Close()
	if i.err != nil {
		return i.err
	}
	return err
}
