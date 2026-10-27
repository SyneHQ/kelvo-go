package cdc

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Postgres captures one publication through one pgoutput logical slot.
// DSN comes from the caller's credential resolution and is never logged or
// included in errors.
type Postgres struct {
	DSN            string
	Slot           string
	Publication    string
	MaxTxnChanges  int           // default 1,000,000; larger transactions fail explicitly
	SnapshotChunk  int           // rows per bootstrap batch; default 10,000
	StatusInterval time.Duration // standby status cadence; default 10s
	MaxTxnBytes    int           // decoded message bytes per transaction; default 256 MiB
	// HeartbeatInterval, when positive, emits a transactional logical message
	// at this cadence while idle so the checkpoint and acknowledged LSN
	// advance past WAL of unpublished tables. Heartbeat commits are
	// checkpointed, then acknowledged; they never reach the sink.
	HeartbeatInterval time.Duration
}

const heartbeatPrefix = "kelvo-cdc-heartbeat"

func (p *Postgres) maxBytes() int {
	if p.MaxTxnBytes > 0 {
		return p.MaxTxnBytes
	}
	return 256 << 20
}

func (p *Postgres) maxTxn() int {
	if p.MaxTxnChanges > 0 {
		return p.MaxTxnChanges
	}
	return 1_000_000
}

func (p *Postgres) connect(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(p.DSN)
	if err != nil {
		return nil, errors.New("cdc: invalid PostgreSQL connection configuration")
	}
	maps.Copy(cfg.RuntimeParams, sessionParams)
	return pgx.ConnectConfig(ctx, cfg)
}

func (p *Postgres) connectReplication(ctx context.Context) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(p.DSN)
	if err != nil {
		return nil, errors.New("cdc: invalid PostgreSQL connection configuration")
	}
	maps.Copy(cfg.RuntimeParams, sessionParams)
	cfg.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, cfg)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Validate checks the publication and every published table: all of
// insert/update/delete/truncate published, no row filter, REPLICA IDENTITY
// DEFAULT with a primary key or USING INDEX, and only exactly-decoded types.
func (p *Postgres) Validate(ctx context.Context) error {
	c, err := p.connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	_, err = p.loadSchemas(ctx, c)
	return err
}

func (p *Postgres) loadSchemas(ctx context.Context, q querier) (map[uint32]Schema, error) {
	var all bool
	err := q.QueryRow(ctx, `SELECT pubinsert AND pubupdate AND pubdelete AND pubtruncate FROM pg_publication WHERE pubname = $1`, p.Publication).Scan(&all)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("cdc: publication %q does not exist", p.Publication)
	}
	if err != nil {
		return nil, err
	}
	if !all {
		return nil, fmt.Errorf("%w: publication %q must publish insert, update, delete and truncate", ErrUnsupported, p.Publication)
	}
	type table struct {
		oid             uint32
		ns, name, ident string
		filter          *string
		attnames        []string
		partitioned     bool
	}
	rows, err := q.Query(ctx, `SELECT c.oid, n.nspname, c.relname, c.relreplident::text, to_jsonb(pt)->>'rowfilter',
	(SELECT array_agg(a ORDER BY a) FROM jsonb_array_elements_text(to_jsonb(pt)->'attnames') a), c.relkind = 'p'
FROM pg_publication_tables pt
JOIN pg_namespace n ON n.nspname = pt.schemaname
JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
WHERE pt.pubname = $1 ORDER BY 2, 3`, p.Publication)
	if err != nil {
		return nil, err
	}
	tables, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (table, error) {
		var t table
		return t, r.Scan(&t.oid, &t.ns, &t.name, &t.ident, &t.filter, &t.attnames, &t.partitioned)
	})
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("cdc: publication %q has no tables", p.Publication)
	}
	out := make(map[uint32]Schema, len(tables))
	for _, t := range tables {
		s := Schema{Namespace: t.ns, Name: t.name, Version: 1, Partitioned: t.partitioned}
		if t.filter != nil {
			return nil, fmt.Errorf("%w: %s has a publication row filter", ErrUnsupported, s.Table())
		}
		if t.ident != "d" && t.ident != "i" {
			return nil, fmt.Errorf("%w: %s REPLICA IDENTITY must be DEFAULT (with a primary key) or USING INDEX", ErrUnsupported, s.Table())
		}
		rows, err := q.Query(ctx, `SELECT a.attname, a.atttypid, a.atttypmod, coalesce(a.attnum = ANY(i.indkey), false)
FROM pg_attribute a
LEFT JOIN pg_index i ON i.indrelid = a.attrelid AND CASE WHEN $2 = 'i' THEN i.indisreplident ELSE i.indisprimary END
WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
ORDER BY a.attnum`, t.oid, t.ident)
		if err != nil {
			return nil, err
		}
		s.Columns, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Column, error) {
			var c Column
			return c, r.Scan(&c.Name, &c.TypeOID, &c.TypeMod, &c.Key)
		})
		if err != nil {
			return nil, err
		}
		if err := checkColumns(s, s.Columns); err != nil {
			return nil, err
		}
		if t.attnames != nil { // PostgreSQL 15+: reject publication column lists
			names := make([]string, len(s.Columns))
			for i, c := range s.Columns {
				names[i] = c.Name
			}
			if slices.Sort(names); !slices.Equal(names, t.attnames) {
				return nil, fmt.Errorf("%w: %s has a publication column list", ErrUnsupported, s.Table())
			}
		}
		if !slices.ContainsFunc(s.Columns, func(c Column) bool { return c.Key }) {
			return nil, fmt.Errorf("%w: %s has no replica identity key", ErrUnsupported, s.Table())
		}
		out[t.oid] = s
	}
	return out, nil
}

func checkColumns(s Schema, cols []Column) error {
	for _, c := range cols {
		if _, ok := supportedTypes[c.TypeOID]; !ok {
			return fmt.Errorf("%w: %s.%s has type oid %d", ErrUnsupported, s.Table(), c.Name, c.TypeOID)
		}
	}
	return nil
}

// DropSlot removes the slot if it exists. It fails if the slot is active.
func (p *Postgres) DropSlot(ctx context.Context) error {
	c, err := p.connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	_, err = c.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, p.Slot)
	return err
}

// Bootstrap is the explicit operator action that (re)starts capture: it drops
// any existing slot, creates a new one exporting its snapshot, delivers every
// published row read in that snapshot as Snapshot batches (the first has
// Reset), and saves a checkpoint at the slot's consistent point. Streaming
// with Run then starts exactly after the snapshot.
func (p *Postgres) Bootstrap(ctx context.Context, sink Sink, store CheckpointStore) error {
	if err := p.Validate(ctx); err != nil {
		return err
	}
	if err := p.DropSlot(ctx); err != nil {
		return err
	}
	q, err := p.connect(ctx)
	if err != nil {
		return err
	}
	defer q.Close(context.Background())
	r, err := p.connectReplication(ctx)
	if err != nil {
		return err
	}
	// The exported snapshot stays valid until r runs another command or closes.
	defer r.Close(context.Background())
	sys, err := pglogrepl.IdentifySystem(ctx, r)
	if err != nil {
		return err
	}
	slot, err := pglogrepl.CreateReplicationSlot(ctx, r, p.Slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		return fmt.Errorf("cdc: create slot: %w", err)
	}
	point, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		return err
	}
	tx, err := q.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(slot.SnapshotName, "'", "''")+"'"); err != nil {
		return err
	}
	schemas, err := p.loadSchemas(ctx, tx)
	if err != nil {
		return err
	}
	chunk := p.SnapshotChunk
	if chunk <= 0 {
		chunk = 10_000
	}
	b := Batch{LSN: uint64(point), Seq: 1, Snapshot: true, Reset: true}
	for _, oid := range slices.Sorted(maps.Keys(schemas)) {
		s := schemas[oid]
		b.Changes = append(b.Changes, Change{Op: OpSchema, Relation: oid, Table: s.Table(), SchemaVersion: s.Version, Schema: &s})
	}
	flush := func() error {
		err := sink.Apply(ctx, b)
		b = Batch{LSN: uint64(point), Seq: b.Seq + 1, Snapshot: true}
		return err
	}
	for _, oid := range slices.Sorted(maps.Keys(schemas)) {
		s := schemas[oid]
		err := readTable(ctx, tx.Conn().PgConn(), s, func(row []any) error {
			b.Changes = append(b.Changes, Change{Op: OpInsert, Relation: oid, Table: s.Table(), SchemaVersion: s.Version, Key: key(s, row), Row: row})
			if len(b.Changes) >= chunk {
				return flush()
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if len(b.Changes) > 0 || b.Reset {
		if err := flush(); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return store.Save(ctx, Checkpoint{Slot: p.Slot, SystemID: sys.SystemID, Timeline: sys.Timeline, LSN: uint64(point), Schemas: schemas})
}

func readTable(ctx context.Context, c *pgconn.PgConn, s Schema, fn func([]any) error) error {
	names := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		names[i] = pgx.Identifier{col.Name}.Sanitize()
	}
	only := "ONLY "
	if s.Partitioned {
		only = ""
	}
	rr := c.ExecParams(ctx, "SELECT "+strings.Join(names, ", ")+" FROM "+only+pgx.Identifier{s.Namespace, s.Name}.Sanitize(), nil, nil, nil, nil)
	for rr.NextRow() {
		vals := rr.Values()
		row := make([]any, len(vals))
		for i, v := range vals {
			if v == nil {
				continue
			}
			d, err := decodeText(s.Columns[i].TypeOID, string(v))
			if err != nil {
				rr.Close()
				return fmt.Errorf("%s.%s: %w", s.Table(), s.Columns[i].Name, err)
			}
			row[i] = d
		}
		if err := fn(row); err != nil {
			rr.Close()
			return err
		}
	}
	_, err := rr.Close()
	return err
}

func key(s Schema, row []any) []any {
	var k []any
	for i, c := range s.Columns {
		if c.Key {
			k = append(k, row[i])
		}
	}
	return k
}

// Run streams committed transactions after the stored checkpoint until ctx
// ends or an error occurs. For each transaction it calls sink.Apply, then
// store.Save, then acknowledges the commit end LSN to PostgreSQL. A restart
// after any failure resumes from the checkpoint; transactions at or below it
// are skipped.
func (p *Postgres) Run(ctx context.Context, sink Sink, store CheckpointStore) error {
	cp, ok, err := store.Load(ctx)
	if err != nil {
		return err
	}
	if !ok || cp.Slot != p.Slot {
		return fmt.Errorf("%w: no checkpoint for slot %q", ErrBootstrapRequired, p.Slot)
	}
	if err := p.checkSlot(ctx, cp.LSN); err != nil {
		return err
	}
	r, err := p.connectReplication(ctx)
	if err != nil {
		return err
	}
	defer r.Close(context.Background())
	sys, err := pglogrepl.IdentifySystem(ctx, r)
	if err != nil {
		return err
	}
	if sys.SystemID != cp.SystemID || sys.Timeline != cp.Timeline {
		return fmt.Errorf("%w: server system %s timeline %d differs from checkpoint system %s timeline %d",
			ErrBootstrapRequired, sys.SystemID, sys.Timeline, cp.SystemID, cp.Timeline)
	}
	pub := strings.ReplaceAll(pgx.Identifier{p.Publication}.Sanitize(), "'", "''")
	err = pglogrepl.StartReplication(ctx, r, p.Slot, pglogrepl.LSN(cp.LSN), pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication, PluginArgs: []string{"proto_version '1'", "publication_names '" + pub + "'", "messages 'true'"},
	})
	if err != nil {
		return slotError(err)
	}
	return p.stream(ctx, r, sink, store, cp)
}

func (p *Postgres) checkSlot(ctx context.Context, lsn uint64) error {
	c, err := p.connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	var plugin, walStatus, reason string
	var confirmed, current pglogrepl.LSN
	err = c.QueryRow(ctx, `SELECT coalesce(plugin, ''), coalesce(wal_status, ''), coalesce(confirmed_flush_lsn, '0/0')::text,
coalesce(to_jsonb(s)->>'invalidation_reason', ''), pg_current_wal_lsn()::text
FROM pg_replication_slots s WHERE slot_name = $1 AND database = current_database()`,
		p.Slot).Scan(&plugin, &walStatus, &confirmed, &reason, &current)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%w: slot %q does not exist", ErrBootstrapRequired, p.Slot)
	case err != nil:
		return err
	case walStatus == "lost" || walStatus == "unreserved" || reason != "":
		return fmt.Errorf("%w: slot %q invalidated (wal_status=%s reason=%s)", ErrBootstrapRequired, p.Slot, walStatus, reason)
	case plugin != "pgoutput":
		return fmt.Errorf("%w: slot %q uses plugin %q", ErrUnsupported, p.Slot, plugin)
	case lsn > uint64(current):
		return fmt.Errorf("%w: checkpoint %s is beyond server WAL %s", ErrBootstrapRequired, pglogrepl.LSN(lsn), current)
	case uint64(confirmed) > lsn:
		return fmt.Errorf("%w: slot %q confirmed %s beyond checkpoint %s", ErrBootstrapRequired, p.Slot, confirmed, pglogrepl.LSN(lsn))
	}
	return nil
}

// slotError maps a slot that became unusable while starting to ErrBootstrapRequired.
func slotError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "55000" || pgErr.Code == "42704" || pgErr.Code == "58P01") {
		return fmt.Errorf("%w: %s", ErrBootstrapRequired, pgErr.Message)
	}
	return err
}

func (p *Postgres) stream(ctx context.Context, r *pgconn.PgConn, sink Sink, store CheckpointStore, cp Checkpoint) error {
	interval := p.StatusInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	schemas := maps.Clone(cp.Schemas)
	if schemas == nil {
		return fmt.Errorf("%w: checkpoint has no schemas", ErrBootstrapRequired)
	}
	acked := cp.LSN
	status := func() error {
		// Only the durably checkpointed position is ever reported.
		return pglogrepl.SendStandbyStatusUpdate(ctx, r, pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(acked)})
	}
	var txn *Batch
	txnBytes := 0
	next := time.Now().Add(interval)
	var hb *pgx.Conn
	defer func() {
		if hb != nil {
			hb.Close(context.Background())
		}
	}()
	aux := func() error { // lazily opened for heartbeats and catalog checks
		if hb != nil {
			return nil
		}
		c, err := p.connect(ctx)
		hb = c
		return err
	}
	beat := time.Now().Add(p.HeartbeatInterval)
	for {
		if !time.Now().Before(next) {
			if err := status(); err != nil {
				return err
			}
			next = time.Now().Add(interval)
		}
		deadline := next
		if p.HeartbeatInterval > 0 {
			if txn == nil && !time.Now().Before(beat) {
				if err := aux(); err != nil {
					return err
				}
				if _, err := hb.Exec(ctx, "SELECT pg_logical_emit_message(true, $1, '')", heartbeatPrefix); err != nil {
					return fmt.Errorf("cdc: heartbeat: %w", err)
				}
				beat = time.Now().Add(p.HeartbeatInterval)
			}
			if beat.Before(deadline) {
				deadline = beat
			}
		}
		rctx, cancel := context.WithDeadline(ctx, deadline)
		msg, err := r.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}
		var data []byte
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			data = m.Data
		case *pgproto3.ErrorResponse:
			return slotError(pgconn.ErrorResponseToPgError(m))
		default:
			return fmt.Errorf("cdc: unexpected replication message %T", msg)
		}
		if len(data) == 0 {
			continue
		}
		switch data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			k, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
			if err != nil {
				return err
			}
			if k.ReplyRequested {
				next = time.Time{}
			}
			continue
		case pglogrepl.XLogDataByteID:
		default:
			continue
		}
		x, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			return err
		}
		lm, err := pglogrepl.Parse(x.WALData)
		if err != nil {
			return err
		}
		if txn != nil {
			if txnBytes += len(x.WALData); txnBytes > p.maxBytes() {
				return fmt.Errorf("%w: transaction exceeds %d bytes", ErrUnsupported, p.maxBytes())
			}
		}
		if _, ok := lm.(*pglogrepl.BeginMessage); !ok && txn == nil {
			if _, ok := lm.(*pglogrepl.OriginMessage); !ok {
				if _, ok := lm.(*pglogrepl.LogicalDecodingMessage); !ok {
					return fmt.Errorf("cdc: %T outside a transaction", lm)
				}
			}
		}
		switch m := lm.(type) {
		case *pglogrepl.BeginMessage:
			txn, txnBytes = &Batch{CommitTime: m.CommitTime}, 0
		case *pglogrepl.RelationMessage:
			s, changed, err := evolve(schemas, m)
			if err != nil {
				return err
			}
			if changed {
				if err := aux(); err != nil {
					return err
				}
				if err := checkAddedDefaults(ctx, hb, m.RelationID, s.Columns[len(schemas[m.RelationID].Columns):]); err != nil {
					return fmt.Errorf("%s: %w", s.Table(), err)
				}
				schemas[m.RelationID] = s
				txn.Changes = append(txn.Changes, Change{Op: OpSchema, Relation: m.RelationID, Table: s.Table(), SchemaVersion: s.Version, Schema: &s})
			}
		case *pglogrepl.InsertMessage:
			err = p.add(txn, schemas, m.RelationID, func(s Schema) ([]Change, error) {
				row, err := decodeTuple(s, m.Tuple)
				return []Change{{Op: OpInsert, Key: key(s, row), Row: row}}, err
			})
		case *pglogrepl.UpdateMessage:
			err = p.add(txn, schemas, m.RelationID, func(s Schema) ([]Change, error) { return updateChanges(s, m) })
		case *pglogrepl.DeleteMessage:
			err = p.add(txn, schemas, m.RelationID, func(s Schema) ([]Change, error) {
				old, err := decodeTuple(s, m.OldTuple)
				return []Change{{Op: OpDelete, Key: key(s, old)}}, err
			})
		case *pglogrepl.TruncateMessage:
			for _, id := range m.RelationIDs {
				if err = p.add(txn, schemas, id, func(Schema) ([]Change, error) { return []Change{{Op: OpTruncate}}, nil }); err != nil {
					break
				}
			}
		case *pglogrepl.CommitMessage:
			end := uint64(m.TransactionEndLSN)
			if end > acked {
				txn.LSN = end
				if len(txn.Changes) == 0 {
					// Heartbeat or emptied transaction: nothing to publish.
				} else if err := sink.Apply(ctx, *txn); err != nil {
					return fmt.Errorf("cdc: apply %s: %w", pglogrepl.LSN(end), err)
				}
				if err := store.Save(ctx, Checkpoint{Slot: p.Slot, SystemID: cp.SystemID, Timeline: cp.Timeline, LSN: end, Schemas: maps.Clone(schemas)}); err != nil {
					return fmt.Errorf("cdc: checkpoint %s: %w", pglogrepl.LSN(end), err)
				}
				acked = end
				if err := status(); err != nil {
					return err
				}
			}
			txn = nil
		case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage, *pglogrepl.LogicalDecodingMessage:
		default:
			return fmt.Errorf("%w: logical message %T", ErrUnsupported, lm)
		}
		if err != nil {
			return err
		}
	}
}

func (p *Postgres) add(txn *Batch, schemas map[uint32]Schema, rel uint32, fn func(Schema) ([]Change, error)) error {
	s, ok := schemas[rel]
	if !ok {
		return fmt.Errorf("%w: change for unknown relation %d", ErrIncompatibleSchema, rel)
	}
	cs, err := fn(s)
	if err != nil {
		return fmt.Errorf("%s: %w", s.Table(), err)
	}
	for _, c := range cs {
		c.Relation, c.Table, c.SchemaVersion = rel, s.Table(), s.Version
		txn.Changes = append(txn.Changes, c)
	}
	// ponytail: whole transaction buffered in memory; stream protocol v2 if transactions exceed this bound.
	if len(txn.Changes) > p.maxTxn() {
		return fmt.Errorf("%w: transaction exceeds %d changes", ErrUnsupported, p.maxTxn())
	}
	return nil
}

func updateChanges(s Schema, m *pglogrepl.UpdateMessage) ([]Change, error) {
	row, err := decodeTuple(s, m.NewTuple)
	if err != nil {
		return nil, err
	}
	newKey := key(s, row)
	if m.OldTuple != nil {
		old, err := decodeTuple(s, m.OldTuple)
		if err != nil {
			return nil, err
		}
		if oldKey := key(s, old); !reflect.DeepEqual(oldKey, newKey) {
			if slices.Contains(row, Unchanged) {
				return nil, fmt.Errorf("%w: key change with unchanged TOAST values", ErrUnsupported)
			}
			return []Change{{Op: OpDelete, Key: oldKey}, {Op: OpInsert, Key: newKey, Row: row}}, nil
		}
	}
	return []Change{{Op: OpUpdate, Key: newKey, Row: row}}, nil
}

func decodeTuple(s Schema, t *pglogrepl.TupleData) ([]any, error) {
	if t == nil || len(t.Columns) != len(s.Columns) {
		return nil, fmt.Errorf("%w: tuple does not match schema version %d", ErrIncompatibleSchema, s.Version)
	}
	row := make([]any, len(t.Columns))
	for i, c := range t.Columns {
		switch c.DataType {
		case pglogrepl.TupleDataTypeNull:
		case pglogrepl.TupleDataTypeToast:
			if s.Columns[i].Key {
				return nil, fmt.Errorf("%w: unchanged key column %s", ErrUnsupported, s.Columns[i].Name)
			}
			row[i] = Unchanged
		case pglogrepl.TupleDataTypeText:
			v, err := decodeText(s.Columns[i].TypeOID, string(c.Data))
			if err != nil {
				return nil, fmt.Errorf("%s: %w", s.Columns[i].Name, err)
			}
			row[i] = v
		default:
			return nil, fmt.Errorf("%w: tuple data type %q", ErrUnsupported, c.DataType)
		}
	}
	return row, nil
}

// evolve accepts a relation message that matches the known schema or only
// appends non-key columns of supported types (a new schema version).
func evolve(schemas map[uint32]Schema, m *pglogrepl.RelationMessage) (Schema, bool, error) {
	old, ok := schemas[m.RelationID]
	if !ok {
		return Schema{}, false, fmt.Errorf("%w: relation %s.%s was not bootstrapped", ErrIncompatibleSchema, m.Namespace, m.RelationName)
	}
	bad := func(why string) (Schema, bool, error) {
		return Schema{}, false, fmt.Errorf("%w: %s: %s", ErrIncompatibleSchema, old.Table(), why)
	}
	if m.Namespace != old.Namespace || m.RelationName != old.Name {
		return bad("renamed to " + m.Namespace + "." + m.RelationName)
	}
	if m.ReplicaIdentity != 'd' && m.ReplicaIdentity != 'i' {
		return bad("replica identity changed")
	}
	cols := make([]Column, len(m.Columns))
	for i, c := range m.Columns {
		cols[i] = Column{Name: c.Name, TypeOID: c.DataType, TypeMod: c.TypeModifier, Key: c.Flags&1 == 1}
	}
	if len(cols) < len(old.Columns) {
		return bad("column removed")
	}
	for i, c := range old.Columns {
		if cols[i] != c {
			return bad(fmt.Sprintf("column %s changed", c.Name))
		}
	}
	if len(cols) == len(old.Columns) {
		return old, false, nil
	}
	added := cols[len(old.Columns):]
	if slices.ContainsFunc(added, func(c Column) bool { return c.Key }) {
		return bad("key column added")
	}
	if err := checkColumns(old, added); err != nil {
		return Schema{}, false, err
	}
	old.Columns, old.Version = cols, old.Version+1
	return old, true, nil
}

// checkAddedDefaults rejects added columns that have, or had, a default:
// PostgreSQL fills existing rows without emitting row changes, so a sink
// could not reproduce them. It reads the current catalog, so it can also
// reject a column whose default was set later (fail closed).
func checkAddedDefaults(ctx context.Context, c *pgx.Conn, rel uint32, added []Column) error {
	for _, col := range added {
		var hasDefault bool
		err := c.QueryRow(ctx, `SELECT coalesce(bool_or(atthasdef OR atthasmissing), true) FROM pg_attribute WHERE attrelid = $1 AND attname = $2`,
			rel, col.Name).Scan(&hasDefault)
		if err != nil {
			return err
		}
		if hasDefault {
			return fmt.Errorf("%w: added column %s has a default that existing rows received without change events", ErrIncompatibleSchema, col.Name)
		}
	}
	return nil
}
