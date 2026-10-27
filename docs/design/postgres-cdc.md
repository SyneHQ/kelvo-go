# PostgreSQL change capture

Status: core package `internal/cdc` with real-PostgreSQL 17 tests. It is not yet wired into
snapshot publication, a worker, configuration or the CLI. Part of
[#29](https://github.com/SyneHQ/kelvo-go/issues/29).

## Contract

`cdc.Postgres` reads one publication through one `pgoutput` logical slot (protocol version 1).
It delivers each source transaction as one `cdc.Batch` to a `cdc.Sink`, in commit order, with the
commit end LSN. For each transaction:

1. `Sink.Apply(batch)` publishes the data durably.
2. `CheckpointStore.Save(checkpoint)` durably records the LSN and the schema versions.
3. The stream reports that LSN to PostgreSQL as written/flushed/applied (`confirmed_flush_lsn`).

The stream reports only a checkpointed LSN, never the server's WAL end. A crash at any point
replays from the checkpoint:

| Crash point | Restart behaviour |
|---|---|
| During apply | Transaction delivered again |
| After apply, before checkpoint | Transaction delivered again; the sink must apply it idempotently |
| After checkpoint, before ACK | PostgreSQL resends; `Run` skips commits at or below the checkpoint |
| After ACK | Resumes after the checkpoint |

The sink contract is narrow so the acceleration snapshot store can implement it later. `Apply`
returns only after the data is durable. It must be idempotent for a repeated batch LSN. Change
values are absolute (row images, key deletes and truncates), so replaying one transaction on the
state it produced leaves that state unchanged. A sink that can publish data and checkpoint
atomically may also implement `CheckpointStore` and remove the replay window.
`FileStore` is the reference store: it writes a temporary file, fsyncs it, renames it over the
checkpoint and fsyncs the directory.

## Snapshot-to-stream handoff

`Bootstrap` is an explicit operator action:

1. Validate the publication and tables.
2. Drop the slot if it exists. This fails while the slot is active.
3. Create the slot with `EXPORT_SNAPSHOT` on a replication connection. Keep that connection idle
   until the snapshot has been read.
4. In a `REPEATABLE READ READ ONLY` transaction, run `SET TRANSACTION SNAPSHOT`. Read the schemas
   and every published row as `Snapshot` batches at the slot's consistent point. The first batch
   has `Reset` set, so the sink discards earlier state.
5. Save a checkpoint at the consistent point.

The snapshot contains exactly the transactions committed before the consistent point. The slot
streams only later commits, so there is no gap. The only duplicates come from idempotent replay.
The concurrent-writer test checks this: four writers insert, update and delete throughout bootstrap
and streaming, then the sink must equal the source.

If bootstrap fails before step 5, there is no checkpoint, so `Run` refuses to start until
bootstrap is repeated.

## Changes, keys and types

- Insert, update and delete become `Change` values. Each carries a `Key` (replica identity
  columns) and, for insert and update, the full `Row` in schema order.
- If an update changes the key, it becomes `delete(old key)` followed by `insert(new row)`. If that
  insert has unchanged TOAST values, it fails with `ErrUnsupported`, because the new row cannot be
  rebuilt.
- An unchanged TOAST column is `cdc.Unchanged`, which is distinct from `nil` (NULL). The sink keeps
  its current value.
- `TRUNCATE` is an explicit `OpTruncate` change for each relation.
- Validation runs before bootstrap and fails with `ErrUnsupported` in these cases:
  - the publication does not publish insert, update, delete and truncate;
  - a table has a row filter;
  - a table's REPLICA IDENTITY is not DEFAULT with a primary key, or USING INDEX (FULL and NOTHING
    are rejected);
  - a column has a type outside the exact set below.
- Types:

  | PostgreSQL type | Go value |
  |---|---|
  | bool | `bool` |
  | bytea | `[]byte` |
  | int2, int4, int8 | `int16`, `int32`, `int64` |
  | float4, float8 | `float32`, `float64` |
  | numeric | `cdc.Numeric` (exact text) |
  | text, varchar, bpchar (padding kept), uuid, json, jsonb | `string` |
  | date, timestamp, timestamptz | `time.Time` in UTC |

  Infinite or BC dates and timestamps fail. Both connections set `DateStyle`, `TimeZone=UTC`,
  `extra_float_digits=3` and `bytea_output=hex`.

## Schema versions

The checkpoint stores each table's schema. A relation message that only appends non-key columns of
supported types increments `Schema.Version`. The new schema appears in `Batch.Schemas` of the
transaction where it takes effect. Any other change stops `Run` with `ErrIncompatibleSchema`:

- a column dropped, retyped or reordered;
- a key change;
- a rename;
- a replica identity change;
- a table that was added to the publication after bootstrap.

Recovery is an explicit bootstrap.

## Unavailable cursor

`Run` returns `ErrBootstrapRequired` and never re-snapshots on its own when:

- there is no checkpoint, or the checkpoint belongs to another slot;
- the slot is missing;
- the slot is invalidated (`wal_status = lost` or `invalidation_reason` set, for example after
  `max_slot_wal_keep_size` removed its WAL);
- `confirmed_flush_lsn` is beyond the checkpoint (for example, a checkpoint restored from an older
  backup).

## Credentials

`Postgres.DSN` is supplied by the caller's existing credential resolution, which names an
environment variable in configuration. The package never logs the DSN or puts it in errors.

## Limits and open work

- Each transaction is buffered in memory and bounded by `MaxTxnChanges` (default 1,000,000).
  Larger transactions fail explicitly. Protocol v2 streaming of in-progress transactions is not
  implemented.
- WAL for unpublished tables is retained while no published commit arrives, because only
  checkpointed commit LSNs are acknowledged. A heartbeat table or `pg_logical_emit_message` is
  needed to bound idle retention.
- Not covered: publication column lists (they fail as a schema mismatch on the first relation
  message), partitioned-table `publish_via_partition_root`, failover slots and primary failover,
  MySQL, and wiring into acceleration snapshots and workers.
- Test with `KELVO_TEST_PG_CDC_DSN` set to a superuser DSN (tested on PostgreSQL 17.11) with
  `wal_level=logical`. The WAL-removal test briefly sets `max_slot_wal_keep_size` with
  `ALTER SYSTEM`, so use a disposable server.
