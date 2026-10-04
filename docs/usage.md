# Usage

Query registered sources and receive Arrow IPC. Use native mode for one database, or DuckDB federation for supported source combinations.

## Quick start

1. Install Go 1.26+ and a C/C++ toolchain. Linux amd64 is the initial validated platform.
2. Build and query the included CSV:

```sh
git clone https://github.com/SYNEHQ/kelvo-go.git
cd kelvo-go
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo

bin/kelvo query --config examples/kelvo.yml --sources sales \
  --sql 'SELECT region, SUM(amount::DECIMAL(18,2)) AS revenue FROM sales GROUP BY region ORDER BY region' \
  --out revenue.arrow
```

3. Read the result with PyArrow:

```python
import pyarrow.ipc as ipc
with ipc.open_stream("revenue.arrow") as result:
    for batch in result:
        print(batch.to_pydict())
```

CLI success atomically replaces the output file; failure preserves an existing file. Statistics go to stderr.

## HTTP API

1. Start the authenticated loopback server:

```sh
export KELVO_TOKEN="$(openssl rand -hex 32)"
bin/kelvo serve --config examples/kelvo.yml
```

2. Submit with the same token:

```sh
curl -sS http://127.0.0.1:8080/v1/queries \
  -H "Authorization: Bearer $KELVO_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"federated","sources":["sales"],"sql":"SELECT * FROM sales"}'
```

3. Use the returned `id`:

| Request | Purpose |
| --- | --- |
| `GET /v1/queries/{id}/results` | Claim and consume Arrow results once |
| `GET /v1/queries/{id}` | Inspect status |
| `POST /v1/queries/{id}/cancel` | Cancel the worker |
| `GET /health` | Liveness, without a token |

Submission creates a TTL handle; execution starts on the first result request. HTTP 429 leaves the handle unconsumed. All query operations require the token.

Check terminal status and complete transport/Arrow framing. HTTP 200 can still end in execution failure; partial data is not a successful result. Exposing the service requires TLS termination and deployment isolation. One `serve` token authorizes the whole catalog.

## Configuration

Default: `kelvo.yml`. Select another `.yml`/`.yaml` with `--config`. Files must contain one mapping, known fields and unique keys, within 1 MiB.

```yaml
extension_directory: /opt/kelvo/extensions
sources:
  - id: warehouse
    type: postgres
    dsn_env: KELVO_POSTGRES_DSN
  - id: orders
    type: mysql
    dsn_env: KELVO_MYSQL_DSN
  - id: events
    type: clickhouse
    url_env: KELVO_CLICKHOUSE_URL
    username_env: KELVO_CLICKHOUSE_USER
    password_env: KELVO_CLICKHOUSE_PASSWORD
```

Store reference names, never credentials. Use read-only database grants and source deadlines. Cluster nodes can resolve [private credential files](operations.md#file-based-source-credential-rotation) in the trusted parent.

PostgreSQL/MySQL use native DSNs. With `duckbridge`, explicit `federation.tables` use the Go connector. Without that registration, federation uses signed DuckDB extensions with their separate connection format. [Relational setup](sources-relational.md)

Provision pinned Linux amd64 extensions before runtime:

```sh
python3 scripts/provision_extensions.py /opt/kelvo/extensions
```

Add `--extensions postgres,mysql,sqlite,httpfs` for object range reads. Runtime checks signatures and never downloads extensions. CSV/Parquet expose a view named by source ID; database attachments expose schemas/tables under their alias.

## Native and federated queries

DuckDB SQL, explicit sources:

```json
{"mode":"federated","sources":["sales"],"sql":"SELECT * FROM sales"}
```

Source SQL, one connection:

```json
{"mode":"native","connection_id":"events","sql":"SELECT count() FROM events"}
```

The [optional bridge](federation.md) supports eight database adapters and [Flight SQL](federation-flight-sql.md) for live joins. Other native results can become [accelerated Parquet aliases](acceleration.md). Native availability alone does not imply federation; independent sources have no shared transaction snapshot.

## Source guides

| Source | Setup and supported behavior |
| --- | --- |
| PostgreSQL and MySQL families | [Relational sources](sources-relational.md) |
| SQL Server and Oracle | [SQL sources](sources-sql.md) |
| Databricks, Snowflake, Cloudflare D1 | [Cloud sources](sources-cloud.md) |
| BigQuery | [Jobs API and authentication](sources-bigquery.md) |
| MongoDB | [Restricted SQL and aggregation](sources-mongodb.md) |
| Trino and Presto | [Statement protocol](sources-trino.md) |
| Elasticsearch | [SQL API](sources-elasticsearch.md) |
| Exasol | [WebSocket SQL and verified TLS](sources-exasol.md) |
| Google Spanner | [Read-only REST SQL](sources-spanner.md) |
| Apache Ignite 2 | [REST SQL fields](sources-ignite.md) |
| Athena and DynamoDB | [Explicit AWS credentials and query APIs](sources-aws.md) |
| Cosmos DB for NoSQL | [Documents, authentication and query budgets](sources-cosmosdb.md) |
| Flight SQL | [Client connections](sources-flight.md) |
| SQLite | [Read-only attachments](sources-sqlite.md) |
| Additional engines | [External adapters](sources-adapters.md) |

ClickHouse: [native connector](../internal/sources/clickhouse/README.md). See the full [coverage matrix](source-coverage.md) for compatible families and validation.

## Parameters

DuckDB uses positional `?` placeholders and ordered typed `parameters`:

```json
{"type":"int64","value":"9223372036854775807"}
```

Types: `string`, `bool`, `int64`, `uint64`, `float64`, `null`. Cast decimal/temporal values in SQL instead of routing them through float. Native support varies; ClickHouse parameters are unsupported.

## Limits

See `kelvo query -h` and `kelvo serve -h` for rows, bytes, duration, memory, threads, scratch, workers and handles.

- Default output: one million rows and 256 MiB. The byte budget applies separately to decoded Arrow buffers and encoded output.
- Limits fail the query; they never silently truncate it. A native driver can allocate a large value before checking output size.
- `--memory-mb` sets DuckDB's managed budget, or ClickHouse's source budget and local Arrow decoder cap. Neither caps worker RSS. Apply host/container limits.

## Opt-in result compression

Enable LZ4 for any public Arrow result path; default is `none`:

```sh
kelvo query --config kelvo.yml --mode native --connection analytics \
  --sql 'SELECT * FROM events' --out events.arrow --result-compression lz4_frame
kelvo serve --config kelvo.yml --result-compression lz4_frame
```

Cluster operators set it in every matching tenant policy:

```yaml
policy:
  limits:
    result_compression: lz4_frame
```

Requests cannot override that policy. Clients need LZ4 Arrow IPC support; the codec is in Arrow metadata, not HTTP `Content-Encoding`.

The node compresses once and the gateway relays bytes; child IPC stays uncompressed. Rows, decoded bytes and encoded bytes remain independently bounded. Raise `--max-rows`/`--max-bytes` explicitly for larger outputs; LZ4 does not increase limits or remove memory costs.

Source compression is separate: ClickHouse offers `options.arrow_compression: lz4_frame`; other connectors retain their own protocols. Measure CPU versus transfer savings. [Micro-VM results](oracle-micro-capacity.md)

## Cluster

NATS dispatches independent tenant queries; one SQL plan stays on one worker. Follow [deployment](../deploy/README.md) and [cluster lifecycle](cluster.md).

```sh
cc -O2 -Wall -Wextra -Werror sandbox/launcher.c -o bin/kelvo-landlock
bin/kelvo cluster-init --config /private/kelvo/bootstrap.yml
bin/kelvo gateway --config /private/kelvo/gateway.yml
bin/kelvo node --config /private/kelvo/node.yml
```

Bootstrap credentials provision resources; gateways/nodes use narrower credentials. `serve` alone does not create tenant isolation. Native export benchmarks do not measure cluster overhead.

## Native cancellation and remote cleanup

On Linux, cancellation, outer deadlines and IPC/sink failures send the worker SIGTERM, allow up to 750ms for cleanup, then SIGKILL remaining group members before reaping the leader. This gives drivers a bounded chance to cancel remote work.

**Local termination does not prove remote work stopped.** Cleanup may exceed that grace or depend on the provider. Configure source deadlines, session expiry and result-storage lifecycle policies. [Recorded PostgreSQL/MySQL cancellation](federation-capacity.md) is scoped evidence; provider-wide remote cleanup still needs live acceptance.
