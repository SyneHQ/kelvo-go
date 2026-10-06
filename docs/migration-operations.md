# Database migrations

The application gateway authorizes `migration.status` and `migration.apply` separately. Kelvo resolves the saved connection only after admission and runs migrations in a contained adapter.

1. Read status and retain the exact version and dirty flag.
2. Submit the migration files, direction and expected state with a stable idempotency key.
3. Read the final operation receipt. Reconcile an unknown outcome before retrying.

Status never initializes or repairs history. A stale version, dirty history or incompatible history table fails closed. `force` changes the recorded version; it does not run or undo SQL.

## Source coordination

| Source | Lock | History |
| --- | --- | --- |
| PostgreSQL | Source advisory lock | Ordinary `schema_migrations` table; views, triggers and RLS denied |
| MySQL / MariaDB | Source named lock | InnoDB history, validated before use |
| SQL Server | Source application lock | Validated history and transaction state |
| CockroachDB | Persistent owner row; serializable acquisition | Ordinary history; trigger-free coordination tables |
| Redshift | Persistent owner row; table lock during acquisition | Ordinary history; live vendor acceptance pending |
| ClickHouse | Persistent lock-table metadata on one pinned server | Atomic database with TinyLog, Log or StripeLog history |

CockroachDB, Redshift and ClickHouse locks have no expiry. A lost response, cancelled script or crashed worker can leave a lock intentionally: the source may still be executing work.

## Recover a retained lock

1. Stop competing migration runners for the saved connection.
2. Inspect source queries, schema jobs, mutations and the affected objects. Wait for completion or cancel and verify the final state.
3. Record the reconciliation result, then remove only the confirmed stale coordination lock as a database operator.
4. Read status again. Use `force` only when the recorded version needs repair.

Never delete a lock because it is old. `force` cannot steal one. Kelvo does not replay uncertain scripts automatically.

## ClickHouse topology

Bind the application team and saved connection to the expected `serverUUID()` in the gateway's analytics YAML:

```yaml
clickhouse_migrations:
  - team_id: team-123
    connection_id: saved-clickhouse
    server_uuid: 11111111-2222-4333-8444-555555555555
```

The adapter verifies that UUID and an Atomic database in one server-local HTTP session. Subsequent requests require that existing session, so a load balancer cannot silently move a write to another server.

`ON CLUSTER`, replicated/distributed/shared DDL, session controls and edits to coordination tables are rejected. This migration path does not coordinate a ClickHouse cluster.

See [database operations](database-operations.md) for worker setup, receipts and retry rules.
