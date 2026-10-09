# Application mode

`kelvo application` runs database operations for one application installation.
It uses local SQLite state. It does not require NATS, a catalog, or Infisical.
The application stores encrypted credentials and owns user authentication.
Kelvo obtains credentials through the existing private resolver protocol.

This mode supports six database engines. It accepts these operations:

| Operation           | Purpose                                                         |
| ------------------- | --------------------------------------------------------------- |
| `connection.test`   | Check a saved connection.                                       |
| `metadata.inspect`  | Read schemas, tables, columns, keys, and relations.             |
| `query.read`        | Execute a read-only query.                                      |
| `statement.execute` | Execute an approved statement when the operator enables writes. |
| `native.read`       | Execute a bounded MongoDB command.                              |
| `native.execute`    | Execute an approved MongoDB command.                            |

| Engine | Protocol | Read | Approved write | Metadata |
| --- | --- | --- | --- | --- |
| PostgreSQL | Verified TLS | SQL | Transaction or autocommit | Schemas, tables, columns, primary keys, foreign keys |
| MySQL | Verified TLS | SQL | Transaction or autocommit | Schemas, tables, columns, primary keys, foreign keys |
| ClickHouse | HTTPS, usually port 8443 | SQL | Autocommit | Database, tables, columns |
| MongoDB | Verified TLS, usually port 27017 | Native commands | Native commands | Database and collections |
| SQLite | Verified file snapshot | SQL | Transaction or autocommit | Main schema, tables, columns, primary keys, foreign keys |
| Oracle | TCPS, usually port 2484 | SQL | Transaction or autocommit | Schemas, tables, columns, primary keys, foreign keys |

The existing adapter validates each transaction mode and statement.
An atomic transaction rejects statements that can commit implicitly.
ClickHouse primary keys do not prove row uniqueness. MongoDB collections have no declared SQL column schema.
The application must not infer editable rows or foreign keys from sampled documents.

`write_mode: disabled` is the default. `write_mode: approved` requires an exact
`approved_change` grant. The mode rejects `trusted_app`, API-key, and job grants.
It has no federation, acceleration, ingestion, or private Rabbit transport.
SQLite is the only supported file format in this mode.

## State and execution

Kelvo stores requests, operation records, receipts, results, and audit records in
private directories. It does not store decrypted credentials. The state volume
must survive container replacement. Only one process can own the operation database.

Each operation must reach durable `running` state before it can resolve credentials.
A restart never replays a running write. If Kelvo cannot prove the result, the
receipt reports `outcome_unknown`. The application must reconcile that receipt.

The application resolver checks the current session, saved connection, grant,
request digest, and Kelvo connection lease. It returns a credential lease for at
most five seconds. This lease permits the initial connection only.

Kelvo runs each operation in a contained adapter process. This mode requires
Linux cgroup v2, managed scratch, and the sandbox launcher. Startup checks containment
with a real child process. Failed cleanup stops new work.

The resolver must implement `/internal/kelvo/resolve-operation` and
`/internal/kelvo/complete-operation`. The configured resolver URL remains
`https://<application>/internal/kelvo/resolve`. The completion callback reports
physical cleanup. It does not report a transaction result.

### SQLite files

The application owns an explicit managed directory and saved relative file paths.
It must reject absolute paths, path escapes, links, and active SQLite sidecar files.
Kelvo receives a size and SHA-256 descriptor. It never receives a host file path.
The signed connection uses database `main` and an empty schema.
Metadata requests can select schema `main`.

The resolver implements `/internal/kelvo/operation-file` and
`/internal/kelvo/operation-file-commit`. Both callbacks use the existing mutual TLS protocol.
Each callback checks the exact grant, request, connection revision, and current operation lease.
The read callback verifies the complete snapshot before it sends bytes.
It confirms the digest and final lease in HTTP trailers.

An approved write changes a private copy in the contained adapter.
The resolver verifies the replacement and compares the current source with the original snapshot.
It publishes the replacement only while source and authorization locks remain held.
A successful response means durable publication. A lost response can leave an unknown write result.
The application must not retry that write.

SQLite metadata uses stable names to group key columns.
`pk_<table>` and `fk_<table>_<id>` are generated names, not stored constraint names.
Composite keys retain their declared column order.

### MongoDB commands

The application uses `native.read` and `native.execute` for MongoDB.
SQL operations are rejected for this engine in application mode.
The native specification selects provider `mongodb` and an explicit command.
It contains one `json` parameter whose value holds the collection and command fields.

Read commands are `find`, `find_one`, `aggregate`, `count`, and `list_indexes`.
Write commands include insert, update, delete, collection, and index operations.
The existing adapter rejects unknown commands, irrelevant fields, and write stages in reads.
It rejects server JavaScript and arbitrary `runCommand` input.
Authentication uses an explicit database name. The default is `admin`.
The driver verifies TLS and disables automatic read and write retries.

Results contain one Arrow binary column named `document`.
Each value contains UTF-8 canonical Extended JSON.
This preserves ObjectIds, integer widths, decimals, dates, and binary values.
Native writes return result data only when the signed request sets `return_result: true`.

## API

Each endpoint requires a service bearer token and `X-Kelvo-Operation-Grant`.
The grant format is version 2. Request and response objects retain version 1.
After grant expiry, lookup, status, cancel, and results accept only the exact
retained grant. Its signature remains required. An expired grant cannot admit
work or obtain credentials. The application must still check the current user.

| Method | Path                                   |
| ------ | -------------------------------------- |
| POST   | `/v1/operations`                       |
| POST   | `/v1/operations/lookup`                |
| GET    | `/v1/operations/{id}`                  |
| GET    | `/v1/operations/{id}/results`          |
| POST   | `/v1/operations/{id}/cancel`           |
| POST   | `/v1/operations/{id}/connection-lease` |

Assign a unique idempotency key to every operation, including reads. Preserve
the request, its digest, and the original signed grant before submission.
If the submission response is lost, use lookup with that key and request digest.
A missing lookup result does not authorize a new submission.

The result endpoint returns Arrow IPC. `Kelvo-Result-Completion: durable-eos-v1`
requires a complete stream and its final end marker. A disconnected stream is
not a completed result. SQL execution remains separate from Arrow delivery.

`GET /healthz` requires the service bearer token. It does not require an operation grant.

## Configuration

Run `kelvo application --config /etc/kelvo/application.yml`.
Use [the example configuration](../examples/application.yml).
Replace the example paths, identities, key, and binary digest before startup.

The listener certificate uses `spiffe://kelvo/gateway`.
The resolver client certificate uses
`spiffe://kelvo/tenant/<instance_id>/worker/application`.
The resolver server certificate uses the existing application resolver identity.

Keep `instance_id`, `app_scope`, signing key, issuer, audience, principal, write
mode, and resolver URL stable while retained state exists. Startup rejects a
changed identity or operation policy. Key rotation needs a controlled state
migration. Do not delete state to recover an uncertain write.

### Capacity

The defaults permit two concurrent operations and retain receipts for one hour.
The ledger holds at most 512 operations. Results use a separate 1 GiB storage budget.
Queries can return at most 4 MiB. Each metadata operation can return at most 1 MiB.
Connection checks and SQL statements reserve no result storage.
Native reads and requested native write results reserve the query result limit.

Kelvo reserves the complete result limit before it obtains database credentials.
This reservation remains until the result expires. The charge includes Arrow,
schema, and manifest overhead. Small results still consume their full reservation.
The default store holds 181 query results or 450 metadata results.
A mix of result types shares the same budget. Active results also count.

Set `max_result_bytes` for queries. Set `max_metadata_bytes` for metadata.
Set `max_stored_bytes` for their combined storage budget, up to 1 GiB.
Set `retention` for the result and receipt lifetime. Its minimum is ten minutes.
`max_retained` limits all operations, including connection checks and rejected operations.
When a limit prevents admission, stop submissions until retained capacity expires.

Storage exhaustion before source execution produces `RESOURCE_EXHAUSTED` with
`effect: none`. It never authorizes a retry of an uncertain write.
Persisted storage settings must remain stable. Startup rejects a changed storage budget.
Preserve existing state when you prepare a separate qualification installation.

## Dependency decision

Application mode adds `modernc.org/sqlite` as a direct dependency. It provides a
durable transaction boundary without a separate broker or a SQLite C library.
The operation store reuses the existing compare-and-swap state machine.
The backend limits each row, shard count, database pages, and retained journal size.
It uses `synchronous=FULL` and an exclusive process lock.

This implementation does not establish production readiness. Release validation
must cover restart recovery, cancellation, disk limits, result integrity, and
the full application-to-database flow on the target Linux host.
