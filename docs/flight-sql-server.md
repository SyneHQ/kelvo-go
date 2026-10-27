# Flight SQL server

`kelvo serve` can expose a read-only [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) endpoint next to the HTTP API. It is disabled by default.

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
| `--flight-sql-max-sessions` | `64` | Credentials that may hold tickets or prepared statements at once |
| `--flight-sql-ticket-ttl` | `1m` | Lifetime of an unclaimed ticket and an idle prepared statement |

HTTP flags such as `--concurrency`, `--max-queries`, `--result-ttl`, `--max-rows`, `--max-bytes` and `--timeout` also bound Flight SQL. Keep `--flight-sql-ticket-ttl` at or below `--result-ttl`.

## Authenticate

Use the key the HTTP API accepts:

- bearer: `authorization: Bearer <key>`
- Flight basic authentication: any username, with the key as the password. The handshake returns the same key as a bearer token.

A wrong or missing key returns `UNAUTHENTICATED`.

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
- Only one statement is accepted. It must start with `SELECT`, `WITH`, `VALUES`, `TABLE`, `FROM`, `DESCRIBE`, `SHOW` or `SUMMARIZE`. Write, DDL, `COPY`, `ATTACH`, `INSTALL`, `LOAD`, `SET`, `PRAGMA` and transaction keywords outside quotes and comments return `PERMISSION_DENIED`, as do update and ingest commands. This lexical guard is deliberately conservative: double-quote an identifier that is spelled like one of these keywords. A `;` inside a string literal followed by more text is also rejected.
- Native mode, Substrait, transactions and savepoints return `UNIMPLEMENTED`.
- Prepared statements bind one row of parameters. Integer, unsigned integer, float, string, boolean and NULL parameters map to the HTTP API's typed parameters. Other types return `INVALID_ARGUMENT`.

## Tickets and sessions

`GetFlightInfo` admits the query through the HTTP API and returns one ticket. The ticket:

- is bound to the SHA-256 of the caller's key. Another key gets `NOT_FOUND`.
- can be used once, and expires after `--flight-sql-ticket-ttl`.

`DoGet` re-authorizes because it requests the results with the caller's key. The stream succeeds only if the query's terminal status is `succeeded`. A failed, cancelled, expired or truncated result ends the stream with an error, never with a short successful result. Results are decoded and re-sent batch by batch with gRPC flow control. The engine may still materialize the result first; see the [architecture notes](architecture.md).

`CancelFlightInfo` and client cancellation or deadlines cancel the HTTP query. Each credential holds at most 64 tickets and 64 prepared statements. When `--flight-sql-max-sessions` credentials already hold state, new ones get `RESOURCE_EXHAUSTED`.

## Metadata

`GetCatalogs` returns `kelvo`. `GetDbSchemas` and `GetTables` list a table only if a `SELECT * FROM <table> LIMIT 0` probe succeeds through the HTTP API with the caller's key. The column list in `include_schema` is the schema that probe returns. File sources appear as `main.<source>`; federation tables appear as `<source>.<name>`. Each metadata call runs one bounded probe per matching catalog table, without caching. `GetSqlInfo` reports read-only, no transactions and cancellation support. Key, cross-reference and type-info commands return `UNIMPLEMENTED`.

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

- Only standalone `serve` is wired. Its single token authorizes the whole catalog, as for the HTTP API. The server accepts any `http.Handler` that implements the HTTP query API. Gateway wiring, which brings per-principal grants and row and column policy, is not done yet.
- Unit tests use the arrow-go Flight SQL client against the real HTTP API handler. A test gateway in front of it models per-key grants, owner-bound handles and a column policy. That models the gateway's contract; it does not test the cluster gateway itself.
- The ADBC end-to-end run covered Python ADBC 1.13.0 and pyarrow 26.0.0 on Linux. JDBC, the `grpc+tls` path and slow-reader behavior were not run.
