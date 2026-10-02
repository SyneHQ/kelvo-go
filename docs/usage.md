# Usage

Kelvo executes SQL against an explicit configured source set and returns Arrow IPC. It has two execution modes: DuckDB federation for selected sources, and native execution for one source-specific connector. This guide covers local usage, HTTP lifecycle, source configuration, limits, and cluster commands.

## Quick start

Kelvo requires Go 1.26 or newer, a C/C++ toolchain for cgo linking, and a supported DuckDB binary platform. Linux amd64 is the initial validation target.

```sh
git clone https://github.com/SYNEHQ/kelvo-go.git
cd kelvo-go
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo

bin/kelvo query --config examples/kelvo.yml --sources sales \
  --sql 'SELECT region, SUM(amount::DECIMAL(18,2)) AS revenue FROM sales GROUP BY region ORDER BY region' \
  --out revenue.arrow
```

The output is Arrow IPC, not JSON. Successful CLI exports replace the requested file atomically; failed exports leave an existing file intact. Statistics are written to standard error.

```python
import pyarrow.ipc as ipc
with ipc.open_stream("revenue.arrow") as result:
    for batch in result:
        print(batch.to_pydict())
```

## HTTP API

Start a loopback listener with a token supplied by the environment:

```sh
export KELVO_TOKEN="$(openssl rand -hex 32)"
bin/kelvo serve --config examples/kelvo.yml
```

Submit a federated query with the same token:

```sh
curl -sS http://127.0.0.1:8080/v1/queries \
  -H "Authorization: Bearer $KELVO_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"federated","sources":["sales"],"sql":"SELECT * FROM sales"}'
```

Use the returned `id` with `GET /v1/queries/{id}/results`, inspect `GET /v1/queries/{id}`, or cancel with `POST /v1/queries/{id}/cancel`. Every query operation requires the bearer token; `GET /health` reports liveness.

Submission creates a handle. Execution begins when its results are first requested, and result retrieval is single-consumer. Capacity exhaustion returns HTTP 429 without consuming the handle. Handles expire and cancellation terminates the query worker. Check terminal status after consuming results: an HTTP 200 response may still end with an execution or delivery failure, so partial Arrow data is not a successful analysis.

The listener defaults to loopback. Use authenticated TLS termination and deployment-level filesystem, network, and resource isolation before exposing a service. One token authorizes the configured source catalog; it is not a per-user or row-level policy.

## Configuration

Kelvo reads `kelvo.yml` from the current directory unless `--config` selects another `.yml` or `.yaml` file. Configuration is one YAML mapping with known fields and unique keys, limited to 1 MiB.

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

Catalog entries contain environment-variable names, never credentials. Each database requires a dedicated read-only account with appropriate grants and source-side timeouts.

Native PostgreSQL and MySQL use their DSN environment variables. With the optional
`duckbridge` build, an explicit `federation.tables` registration reuses that native
Go connector for DuckDB queries. Without this section, PostgreSQL/MySQL federation
uses the matching signed DuckDB extension at `extension_directory` and its separate
connection-string format. See [relational sources](sources-relational.md) and
[custom federation](federation.md) for the two paths.

Provision version- and platform-matched signed extensions before runtime. For the validated Linux amd64 target:

```sh
python3 scripts/provision_extensions.py /opt/kelvo/extensions
```

The script writes canonical PostgreSQL, MySQL, and SQLite extension files plus a hash manifest. Add `--extensions postgres,mysql,sqlite,httpfs` to provision `httpfs` for object snapshot range reads. Runtime verifies signatures while loading and does not download extensions. CSV and Parquet files expose a view named by their source ID. DuckDB and SQLite files, plus PostgreSQL and MySQL attachments, expose schemas and tables through the source alias.

## Native and federated queries

Federated requests use DuckDB SQL and an explicit `sources` list:

```json
{"mode":"federated","sources":["sales"],"sql":"SELECT * FROM sales"}
```

Native requests use `mode: "native"`, one `connection_id`, and source-specific SQL:

```json
{"mode":"native","connection_id":"events","sql":"SELECT count() FROM events"}
```

Native streams are not automatically available for live DuckDB cross-source joins. The optional [Go/C++ federation bridge](federation.md) exposes operator-selected ClickHouse tables and reuses its native Arrow connector. [Dataset acceleration](acceleration.md) can materialize a configured native query into a Parquet alias backed by local files or [object storage](object-storage.md) that DuckDB can join. Independent sources do not share an atomic snapshot. See [source coverage](source-coverage.md), [optional adapters](sources-adapters.md), and the source guides below for connector behavior and validation boundaries.

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

## Parameters

DuckDB federation accepts positional `?` parameters. Supply typed objects in the request's `parameters` array, in placeholder order:

```json
{"type":"int64","value":"9223372036854775807"}
```

Supported parameter types are `string`, `bool`, `int64`, `uint64`, `float64`, and explicit `null`. Use SQL casts for decimal and temporal values rather than converting them through floating point. Native parameter support is connector-specific; native ClickHouse parameters are unsupported.

## Limits

`kelvo serve -h` and `kelvo query -h` list controls for result rows, bytes, duration, DuckDB memory, threads, temporary disk, concurrent workers, and retained handles. They are ceilings, not deployment sizing guidance.

Defaults allow one million rows. The 256 MiB byte limit applies separately to cumulative decoded Arrow buffers and encoded output, using the same configured value for each. Limit failures return errors instead of truncating analysis. A native engine can allocate a single large value before Kelvo detects output size.

For federated DuckDB work, `--memory-mb` configures DuckDB memory. For native ClickHouse it configures ClickHouse `max_memory_usage` and Kelvo's local Arrow decoding allocator. Neither is a hard worker-process RSS cap; enforce process or container limits in deployment.

## Opt-in result compression

`query` and `serve` accept `--result-compression lz4_frame` to compress Arrow IPC record buffers. It applies to every result path, including native connectors, federated queries, and queries over accelerated datasets. Compression defaults to `none` for client compatibility. Clients must support LZ4-compressed Arrow IPC; some browser Arrow readers need additional codec support. The stream metadata identifies the codec, without an HTTP `Content-Encoding` header.

```sh
kelvo query --config kelvo.yml --mode native --connection analytics \
  --sql 'SELECT * FROM events' --out events.arrow --result-compression lz4_frame
kelvo serve --config kelvo.yml --result-compression lz4_frame
```

Cluster operators set the same option inside each tenant's `policy.limits` in the bootstrap, gateway, and node configuration. The remaining required policy limits still apply:

```yaml
policy:
  limits:
    result_compression: lz4_frame
```

Compression is part of the provisioned tenant policy, so gateway and node settings must match the stored policy. The per-query request cannot override it. Supported values are `none` and `lz4_frame`; an omitted or empty value means `none`. Unsupported codecs fail configuration validation before execution.

This compresses the public Arrow result once. In cluster mode, the node compresses the result and the gateway relays those bytes; the local worker pipe remains uncompressed. Row limits, decoded Arrow buffer limits, and encoded result byte limits remain enforced independently. Compression uses one codec worker per output stream, but its scratch space is not a whole-process memory limit. Keep operating-system memory limits in place.

Permit larger exports by explicitly raising `--max-rows` and, when necessary, `--max-bytes`; enabling LZ4 does not raise either limit. For example, 80 MiB of decoded columns still needs at least an 80 MiB byte budget even if the encoded stream is much smaller. Keep query memory, temporary disk and worker concurrency sized independently. The [Oracle micro VM measurements](oracle-micro-capacity.md) show the effect of larger result budgets on a specific narrow-row workload.

Source transport remains a separate setting. For example, a ClickHouse source can opt in with `options.arrow_compression: lz4_frame` independently of result compression. Other connectors keep their source-native protocols; enabling result compression does not force LZ4 onto databases that do not support it. Benchmark the workload and network before enabling either option: reduced transfer bytes can cost CPU on a small host.

## Cluster

Cluster mode distributes independent tenant-bound queries to worker pools through NATS JetStream. It does not split one SQL plan across machines. See [cluster lifecycle and failure semantics](cluster.md) and the [tenant deployment example](../deploy/README.md).

Build the Linux launcher with the Go binary:

```sh
cc -O2 -Wall -Wextra -Werror sandbox/launcher.c -o bin/kelvo-landlock
bin/kelvo cluster-init --config /private/kelvo/bootstrap.yml
bin/kelvo gateway --config /private/kelvo/gateway.yml
bin/kelvo node --config /private/kelvo/node.yml
```

Bootstrap uses provisioner credentials. Running gateways and nodes bind existing broker resources with narrower permissions. `serve` remains a simpler single-trust-domain mode and does not create an automatic tenant boundary. Existing ClickHouse throughput measurements predate cluster mode and do not measure cluster overhead.

## Native cancellation and remote cleanup

Connector cleanup runs inside the query worker. A provider error, conversion
failure, or limit detected inside that process can trigger its documented cursor,
session or query cleanup. Direct connector tests validate those paths.

The CLI, HTTP service and cluster executor terminate the worker process group
immediately on caller cancellation, the outer deadline, or a downstream IPC/sink
failure. That can preempt connector cleanup, including Athena StopQueryExecution,
Spanner session deletion, Exasol abort/rollback, and search/grid cursor closure.
Local process termination is therefore not confirmation that remote work stopped.
Configure provider-side query deadlines, cursor/session expiry and result-storage
lifecycle policies. A bounded graceful worker-cancellation protocol and live
remote-cleanup acceptance through that boundary remain future work.
