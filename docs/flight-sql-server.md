# Flight SQL server

`kelvo serve` and the cluster gateway can expose a read-only [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) endpoint next to the HTTP API. It is disabled by default.

Each Flight SQL call is an in-process request to the same HTTP query handler, made with the caller's own key. Admission, concurrency, query limits, authorization and execution are the HTTP API's. Flight SQL adds no second policy engine and cannot reach sources the HTTP API would refuse.

## Enable

```sh
export KELVO_TOKEN="$(openssl rand -hex 32)"
bin/kelvo serve --config examples/kelvo.yml --flight-sql-listen 127.0.0.1:50051
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--flight-sql-listen` | empty (disabled) | gRPC listen address |
| `--flight-sql-tls-cert`, `--flight-sql-tls-key` | empty | PEM certificate and key. Required unless the address is a loopback IP. |
| `--flight-sql-max-sessions` | `64` | Keys that may hold tickets, prepared statements or cached metadata at once. At the limit, the least recently used key with no active stream is evicted and its queries are cancelled. |
| `--flight-sql-ticket-ttl` | `1m` | Lifetime of an unclaimed ticket and an idle prepared statement |

HTTP flags such as `--concurrency`, `--max-queries`, `--result-ttl`, `--max-rows`, `--max-bytes` and `--timeout` also bound Flight SQL. Keep `--flight-sql-ticket-ttl` at or below `--result-ttl`.

## Cluster gateway

Add an optional `flight_sql` block to the gateway configuration. When the block is absent, the listener is disabled:

```yaml
flight_sql:
  listen: 0.0.0.0:50051
  tls_cert_file: /etc/kelvo/flight.pem   # required
  tls_key_file: /etc/kelvo/flight.key    # required
  max_sessions: 64                       # default 64
  ticket_ttl: 1m                         # default 1m
  catalog_file: /etc/kelvo/kelvo.yml     # optional, names tables for metadata only
```

The listener wraps the gateway's own query handler. Principal keys, source grants, [row and column policy](row-column-access.md), owner-bound handles and revocation all apply unchanged. The gateway has no source catalog of its own, so set `catalog_file` to the workers' catalog if clients need metadata. Without it, GetTables and GetDbSchemas are empty, and statements attach only the sources named in `x-kelvo-sources`. The file gives no access: each listed table is still probed with the caller's key.

## Authenticate

Use the key the HTTP API accepts:

- bearer: `authorization: Bearer <key>`
- Flight basic authentication: any username, with the key as the password. The handshake returns the same key as a bearer token.

Every call, including GetSqlInfo and GetCatalogs, first validates the key through the HTTP API, before any state is created. A wrong or missing key returns `UNAUTHENTICATED`.

## Connect

Python ADBC (`pip install adbc-driver-flightsql pyarrow`):

```python
import os
import adbc_driver_flightsql.dbapi as flightsql
from adbc_driver_flightsql import DatabaseOptions

with flightsql.connect(
    "grpc://127.0.0.1:50051",  # grpc+tls://host:port with TLS
    db_kwargs={DatabaseOptions.AUTHORIZATION_HEADER.value: "Bearer " + os.environ["KELVO_TOKEN"]},
) as conn, conn.cursor() as cur:
    cur.execute("SELECT region, SUM(amount) AS total FROM sales WHERE id > ? GROUP BY region", parameters=(1,))
    print(cur.fetch_arrow_table())
```

Arrow Flight SQL JDBC driver:

```text
jdbc:arrow-flight-sql://kelvo.example.com:50051/?useEncryption=true&token=<key>
```

On a loopback listener without TLS, use `useEncryption=false`. JDBC basic authentication (`user=...&password=<key>`) also works.

## Statements

- A statement is a federated query. By default it attaches the catalog sources whose IDs appear as identifiers in the SQL. Set the `x-kelvo-sources` call header, for example `sales,orders`, to choose them explicitly. For ADBC, use the `adbc.flight.sql.rpc.call_header.x-kelvo-sources` option.
- Only one statement is accepted. It must start with `SELECT`, `WITH`, `VALUES`, `TABLE` or `FROM`, and be at most 64 KiB. Write, DDL, `COPY`, `ATTACH`, `INSTALL`, `LOAD`, `SET`, `PRAGMA` and transaction keywords outside quotes and comments return `PERMISSION_DENIED`, as do update and ingest commands. This lexical guard is deliberately conservative: double-quote an identifier that is spelled like one of these keywords. Also rejected: a `;` inside a string literal followed by more text, any `$` outside a literal (dollar quoting and `$n` parameters; use `?`), and any string literal containing a backslash.
- Native mode, Substrait, transactions and savepoints return `UNIMPLEMENTED`.
- Prepared statements bind one row of parameters. Integer, unsigned integer, float, string, boolean and NULL parameters map to the HTTP API's typed parameters. Other types return `INVALID_ARGUMENT`.

## Tickets and sessions

`GetFlightInfo` admits the query through the HTTP API and returns one ticket. The ticket:

- is bound to the SHA-256 of the caller's key. Another key gets `NOT_FOUND`.
- can be used once, and expires after `--flight-sql-ticket-ttl`.

`DoGet` re-authorizes because it requests the results with the caller's key. The stream succeeds only if the query's terminal status is `succeeded`. A failed, cancelled, expired or truncated result ends the stream with an error, never with a short successful result. Results are decoded and re-sent batch by batch with gRPC flow control. The engine may still materialize the result first; see the [architecture notes](architecture.md).

`CancelFlightInfo` and client cancellation or deadlines cancel the HTTP query. Each key holds at most 64 tickets. It also holds at most 64 prepared statements and 1 MiB of prepared SQL and parameters; past either limit, its least recently used statement is evicted. A prepared statement expires after one ticket TTL idle, and after ten TTLs regardless of use. An unclaimed ticket that expires cancels its HTTP query. gRPC limits: 2 MiB per received message, 64 concurrent streams per connection, a 10 s connection handshake, and keepalive pings at most every 10 s.

## Metadata

`GetCatalogs` returns `kelvo`. `GetDbSchemas` and `GetTables` list a table only if a `SELECT * FROM <table> LIMIT 0` probe succeeds through the HTTP API with the caller's key. The column list in `include_schema` is the schema that probe returns. File sources appear as `main.<source>`; federation tables appear as `<source>.<name>`. Only file sources (CSV, Parquet, JSON), federation tables and snapshot datasets are listed. The first metadata call for a key runs one probe per listed table and caches the result for one ticket TTL. A catalog with more than 64 such tables returns `RESOURCE_EXHAUSTED`. A table that is denied (`PERMISSION_DENIED` or `NOT_FOUND`) is omitted; any other probe failure fails the whole call with `UNAVAILABLE`, never a partial list. Each probe uses one HTTP query handle, which remains until `--result-ttl`: the HTTP API has no early release path. `GetSqlInfo` reports read-only, no transactions and cancellation support. Key, cross-reference and type-info commands return `UNIMPLEMENTED`.

## Errors

| HTTP API result | gRPC code |
| --- | --- |
| 400 / `INVALID_ARGUMENT` | `INVALID_ARGUMENT` |
| 401 | `UNAUTHENTICATED` |
| 403 / `PERMISSION_DENIED` | `PERMISSION_DENIED` |
| 404, or a foreign or expired ticket | `NOT_FOUND` |
| 429 / `RESOURCE_EXHAUSTED` | `RESOURCE_EXHAUSTED` |
| `CANCELLED` | `CANCELLED` |
| `EXPIRED`, or the client deadline | `DEADLINE_EXCEEDED` |
| `ALREADY_CONSUMED` | `FAILED_PRECONDITION` |
| 503 | `UNAVAILABLE` |
| other 409 terminal failures | `ABORTED` |

## Limits

- A Linux gate (`TestFlightSQLThroughGatewayPrincipals`) runs the arrow-go Flight SQL client over TLS against a production gateway with file-key principals, a real node and a real sandboxed worker. The worker reads a guarded snapshot under a row and column policy. It checks the policy-shaped schema and rows, a hidden column, a principal denied a source, an unknown key, a foreign ticket and per-principal metadata. Job coordination in that gate uses the in-memory store, not a NATS broker. The gate needs the DuckDB bridge build and `KELVO_TEST_SNAPSHOT_BINARY`/`KELVO_TEST_SNAPSHOT_SANDBOX`; otherwise it is skipped.
- The package unit tests use the real HTTP API handler, with a test gateway in front of it.
- The ADBC end-to-end run covered Python ADBC 1.13.0 and pyarrow 26.0.0 on Linux against `serve`. JDBC, ADBC against the gateway and slow-reader behavior were not run.
