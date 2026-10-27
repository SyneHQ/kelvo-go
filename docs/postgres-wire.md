# PostgreSQL wire protocol (read-only)

Kelvo can accept PostgreSQL clients for read-only federated queries. The listener is **disabled by default**. It has no execution or authorization path of its own: each statement is sent in-process to the same HTTP query handler (`serve`, or the cluster `gateway`) with the client's password as the bearer key. Admission, principal grants, row/column policies, limits and result checks are exactly those of `POST /v1/queries` plus `GET /v1/queries/{id}/results`.

## Enable

```yaml
postgres_wire:
  listen: 0.0.0.0:5432
  tls: {cert_file: /private/kelvo/pg.crt, key_file: /private/kelvo/pg.key}
  max_connections: 64
  idle_timeout: 10m
  statement_timeout: 60s
```

- Gateway: add the block to the gateway YAML.
- Standalone: save it in its own file and run `kelvo serve --postgres-wire-config pgwire.yml`.

All five fields are required. TLS is mandatory; a plaintext startup gets `28000`.

## Connect

| Field | Value |
| --- | --- |
| Host/port | the `listen` address |
| Database | one or more source IDs, comma-separated, e.g. `sales` or `sales,orders`. Every statement runs in `federated` mode with these sources attached |
| User | any name. Identity comes from the key; the user name is not checked |
| Password | the `serve` token, or a gateway tenant/principal API key |
| SSL | `require` or stricter (cleartext password inside TLS) |

| Client | Connection |
| --- | --- |
| psql | `psql "host=kelvo.example port=5432 dbname=sales user=analyst sslmode=require"` (password from `PGPASSWORD`) |
| psycopg / libpq | `postgresql://analyst:$KEY@kelvo.example:5432/sales?sslmode=require` |
| JDBC (DBeaver, Metabase, Superset via JDBC bridges) | `jdbc:postgresql://kelvo.example:5432/sales?sslmode=require` |
| Superset (SQLAlchemy) | `postgresql+psycopg2://analyst:$KEY@kelvo.example:5432/sales?sslmode=require` |
| Grafana | PostgreSQL data source, TLS/SSL mode `require` |
| Power BI | PostgreSQL connector with encrypted connection; Npgsql validates the certificate, so use a trusted certificate |

Use a certificate the clients trust and `sslmode=verify-full` in production.

## Behavior

- Simple and extended protocol: Parse/Bind/Describe/Execute/Sync, named statements and portals, row-limited Execute (`PortalSuspended`), text and binary formats.
- Result types: `int2`, `int4`, `int8` (unsigned Arrow integers widen; `uint64` is `numeric(20,0)`), `numeric(p,s)` with exact typmod, `bool`, `text`, `bytea`, `float4`, `float8`, `date`, `time`, `timestamp`, `timestamptz`, NULLs. Other Arrow types (lists, structs, maps, dictionaries, intervals) fail with `0A000`.
- Parameters: typed `int2/4/8`, `float4/8`, `bool`, text types, `numeric` (cast to the literal's exact `DECIMAL(p,s)`), `date`, `time`, `timestamp`, `timestamptz`, `bytea`. Untyped (`unknown`) parameters are sent as text for DuckDB to cast. Other OIDs fail with `0A000`.
- Read-only: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, DDL, `COPY` and engine commands fail with `25006` before reaching the engine. A simple-query message containing one is rejected entirely. Read statements that contain such a word unquoted (for example a column named `update`) are also rejected; quote the identifier. The engine applies its own one-SELECT check and capability denylist afterwards.
- Session: `BEGIN`/`COMMIT`/`ROLLBACK` are accepted no-ops (no side effects are possible); `SET` is stored locally; `SHOW` answers server settings; `RESET`, `DISCARD`, `DEALLOCATE` are no-ops.
- Catalog: `pg_catalog` and `information_schema` queries run through the same query path with the session's sources attached, so they only see what the key may see, including row/column policy column hiding. Kelvo adds missing PostgreSQL columns and empty relations (policies, publications, triggers, inheritance), reports source views as tables, and replaces `version()`, `current_database()`, `pg_get_userbyid()` and privilege functions. Privilege functions return true: only authorized sources are attached.
- Cancellation: `CancelRequest` cancels the running statement (`57014`) and its query handle. `statement_timeout` also yields `57014`. Connections over `max_connections` get `53300`.
- Fail closed: a statement succeeds only after a complete Arrow stream **and** a terminal `succeeded` state. Truncation, row/byte limits and worker loss are errors. Unauthenticated keys get `28P01`, denied sources `42501`.

## Limits

- Authentication is cleartext password inside TLS. SCRAM-SHA-256 is not offered: Kelvo stores no password verifier for API keys. No session state is allocated before the key is validated.
- Per connection: 1 MiB query text, 256 statements per simple-query message, 256 prepared statements, 256 portals, 64 MiB of buffered portal rows (`54000` above it). At most 64 unauthenticated handshakes run at once, each within 10 seconds; a session slot (`max_connections`) is taken only after the key is validated.
- Each statement uses one query handle and one child execution. The handle is released (`DELETE /v1/queries/{id}`) as soon as its result is read.
- Catalog queries (`pg_catalog`, `information_schema`, statement Describe) are cached per connection for one minute (64 entries). At most 32 fresh catalog executions run per connection per minute; beyond that the query fails with `53400` instead of returning a partial catalog. Many connections can still use many handles; size `max_connections` and `--max-queries` together.
- Extended-protocol Describe of a statement runs it once with NULL parameters and `LIMIT 0`. Portals buffer their encoded rows, bounded by the configured result limits.
- SQL errors from DuckDB return `22023` with a sanitized first-line message.
- The engine rejects any statement whose text mentions a file, network, settings or SQL-string function (`read_*`, `glob`, `query`, `pg_views`, `current_setting`, ...), even inside a literal or comment. Statement keywords such as `'UPDATE'` in literals are allowed; DuckDB's parser rejects them as statements.
- Validated (Linux runner): psql 17 `\d`, `\dt`, `\dv`, `\dn`, `\l` and queries; psycopg 3.2 text and binary; Metabase v0.56 database sync (tables and fields), a native question and an MBQL count question. Known gaps: psql `\d <table>` fails in its publication footer (DuckDB cannot index an array by a `generate_series` row); Metabase field database types show as NULL. DBeaver, Superset, Power BI and Grafana are untested.
