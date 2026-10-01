# Kelvo Go

An open-source analytics gateway by **SYNEHQ**, built in Go with DuckDB and Apache Arrow.

Kelvo runs SQL against registered data sources and returns typed Arrow batches. DuckDB handles federation; direct ClickHouse queries stay on ClickHouse. The repository is independent of the Rust Spice distribution. Its default branch is `cargo`.

**Status: developer preview.** This first implementation targets one configured trust domain with read-only data access. It does not yet provide shared multi-tenant hosting, CDC, distributed execution, production HA, or a Flight SQL server. No throughput or production-memory guarantee is implied.

## What is implemented

- Registered CSV, Parquet and DuckDB files, plus PostgreSQL/MySQL attachments using approved DuckDB extensions.
- Native ClickHouse queries using its HTTP ArrowStream format.
- Typed parameters for DuckDB queries; native ClickHouse parameters are currently unsupported.
- Arrow IPC file export and authenticated HTTP result delivery.
- Disposable query subprocesses, deadlines, cancellation, bounded admission, single-use result handles and explicit output-limit failures.
- Explicit source selection: only requested configured sources enter the worker.

The pinned DuckDB Go driver performs non-streaming query execution before exposing Arrow batches. Arrow delivery does not make native query memory constant or guarantee early first rows. DuckDB memory/spill settings and output limits complement an external process/container resource boundary.

The initial VM benchmark exported 10 million ClickHouse rows at **1.18–1.48 million rows/s**, including file persistence, with sampled coordinator/worker peaks of roughly **50–58 MiB each**. ClickHouse used a separate 1 GiB query budget inside a 4 GiB container. These are measurements of one native workload; see the [validation record](docs/validation.md) for the exact scope, failed attempts and reproducible fixture.

## Quick start

Requires Go 1.26 or newer, a C/C++ toolchain for cgo linking, and a supported DuckDB binary platform. Linux amd64 is the initial validation target.

```sh
git clone https://github.com/SYNEHQ/kelvo-go.git
cd kelvo-go
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo

bin/kelvo query --config examples/kelvo.yml --sources sales \
  --sql 'SELECT region, SUM(amount::DECIMAL(18,2)) AS revenue FROM sales GROUP BY region ORDER BY region' \
  --out revenue.arrow
```

The output is Arrow IPC, not JSON. Successful CLI exports replace the requested file atomically; failed exports leave existing files intact. Statistics go to stderr. Read with Python:

```python
import pyarrow.ipc as ipc
with ipc.open_stream("revenue.arrow") as result:
    for batch in result:
        print(batch.to_pydict())
```

## HTTP API

```sh
export KELVO_TOKEN="$(openssl rand -hex 32)"
bin/kelvo serve --config examples/kelvo.yml

# In another terminal with the same KELVO_TOKEN:
curl -sS http://127.0.0.1:8080/v1/queries \
  -H "Authorization: Bearer $KELVO_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"federated","sources":["sales"],"sql":"SELECT * FROM sales"}'
```

Use the returned `id` to fetch Arrow at `GET /v1/queries/{id}/results`, inspect `GET /v1/queries/{id}`, or cancel with `POST /v1/queries/{id}/cancel`. Every query operation requires the bearer token. `GET /health` reports liveness.

Submission creates a handle; execution begins on its first result request. Result retrieval is single-consumer. Capacity exhaustion returns 429 without consuming the handle. Handles expire and cancellation terminates the query worker. Check terminal status after consuming HTTP results: a response that started with HTTP 200 can still fail during execution/delivery. Never treat a partial result as a successful analysis.

The listener defaults to loopback. Use authenticated TLS termination and deployment-level filesystem/network/resource isolation before exposing a service. One token authorizes the configured source catalog; tokens are not per-user row or tenant policies.

## Sources and parameters

Kelvo reads `kelvo.yml` from the current directory by default. Use `--config` to select another YAML file; both `.yml` and `.yaml` work. Configuration must contain one YAML mapping, with known fields and unique keys, and fit within 1 MiB.

`examples/kelvo.yml` shows local files. PostgreSQL/MySQL sources use `dsn_env`; ClickHouse uses `url_env`, with optional `username_env` and `password_env`. Configuration contains environment-variable **names**, never credentials. Each database must use a dedicated read-only account with appropriate source grants and timeouts.

```yaml
# Catalog entries name environment variables; they never contain credentials.
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

Install version- and platform-matched signed DuckDB extensions into that directory at provisioning time. For the validated Linux amd64 target, run `python3 scripts/provision_extensions.py /opt/kelvo/extensions`. It writes the canonical `postgres_scanner.duckdb_extension` and `mysql_scanner.duckdb_extension` filenames and an artifact-hash manifest. The runtime verifies signatures when loading and does not download extensions. File sources expose a view named by their ID. Database attachments expose their schemas/tables through the source alias.

Federated requests use DuckDB SQL and `sources`; native requests use `mode: "native"`, one `connection_id`, and ClickHouse SQL. Native result streams are not automatically available to a DuckDB cross-source join. Independent sources do not share an atomic snapshot.

DuckDB parameters are positional `?` placeholders with a `parameters` array:

```json
{"type":"int64","value":"9223372036854775807"}
```

Supported types are `string`, `bool`, `int64`, `uint64`, `float64`, and explicit `null`. Use SQL casts for decimals and temporal types rather than rounding through floating point.

## Resource limits

`kelvo serve -h` and `kelvo query -h` list limits for rows, encoded bytes, query duration, DuckDB memory, threads, temporary disk, concurrent workers and retained handles. These are configurable ceilings, not recommended production sizing.

The default result ceiling is one million rows and 256 MiB. A limit produces an error rather than silently truncating analytical results. A single very large value can be allocated by the native engine before the gateway checks it.

For federated DuckDB queries, `--memory-mb` configures DuckDB's memory budget. For native ClickHouse queries, it configures both ClickHouse's per-query `max_memory_usage` setting and Kelvo's local Arrow decoding allocator budget. Neither use is a hard cap on total worker-process RSS; use deployment-level limits as well.

## Development

```sh
go test -tags duckdb_arrow ./...
go vet -tags duckdb_arrow ./...
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo
```

See [architecture](docs/architecture.md), [contributing](CONTRIBUTING.md), [security](SECURITY.md), and [dependency updates](docs/upgrading-duckdb.md). The [validation record](docs/validation.md) distinguishes executed checks from planned capabilities.

Apache-2.0 licensed. DuckDB and its Go client are MIT licensed; Arrow is Apache-2.0 licensed. See [NOTICE](NOTICE) and [dependency rationale](docs/dependencies.md).
