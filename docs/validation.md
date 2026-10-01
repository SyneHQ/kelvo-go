# Validation — 2026-10-02

Status: a tested developer preview. Cluster, sandbox, connector and failure-handling acceptance is described below. These checks do not certify a production multi-tenant service, establish capacity for every database, or replace deployment-specific security and recovery testing.

## Object snapshots and MongoDB refresh

The [object baseline build](evidence/object-storage-build.json) verifies the
committed source against the tested VM and records binary hashes. Full tagged
Go tests, focused race tests, vet and CLI/launcher builds passed; see
[tests](evidence/tests-object-storage.log) and [race checks](evidence/race-object-storage.log).
The unchanged final baseline binary passed [CLI/HTTP](evidence/acceptance-object-storage.json),
[cluster](evidence/cluster-object-storage.json) and
[14 snapshot checks](evidence/acceleration-object-storage.json). The three-broker
fixture ran on one VM, then its processes were stopped.

[Object protocol acceptance](evidence/object-acceleration.json) passed all 36
checks across private S3, R2, GCS and Azure TLS fixtures. Each selected-column
query transferred 16,984 bytes in two ranges from a roughly 3.15 MB Parquet
object. Reader-only identity, source outage, fresh reader staging, exact values,
failed refresh retention, metadata corruption and read-only publisher separation
were checked. Foreign redirects, ignored ranges and wrong intervals failed
without a completed export; the foreign endpoint received zero requests. These
are protocol fixtures, not real cloud accounts or IAM certification.

Three alternating trials per provider measured median complete query/validation
times of 258–285 ms remotely and 59–63 ms locally. The extra 198–226 ms includes
CLI startup, extension loading, metadata, TLS, range access and export validation;
it is not isolated bridge overhead. A separate generated 32 MiB loopback test
measured 663.95 MB/s over three iterations. Neither result establishes cloud/WAN
throughput or sustained concurrency capacity. Range and copy-buffer limits are
in the [object storage guide](object-storage.md).

The initial direct-DuckDB cloud reader failed the confinement test: pinned
`httpfs` followed foreign redirects and forwarded an S3 session token. That path
was removed. The released path uses a parent-owned Go range reader with redirect
refusal and no cloud credentials in the query process. Object acceptance predates
a later scheduling-only clock-skew fix; both binary identities are retained.
The final code also tests service-clock freshness/scheduling with four-hour host
clock offsets in both directions.

[Live MongoDB 8.0.32 acceptance](evidence/mongodb-acceleration.json) covers YAML
aggregation pipelines, filtered/grouped/empty results, exact int64/Decimal128/BSON
values and DuckDB reads of the resulting local snapshot. `$out` and `$merge`
refreshes fail and preserve the committed generation. BSON remains binary; this
does not infer relational document columns or implement CDC.

## Dataset acceleration

Persistent full-refresh Parquet snapshots are implemented with manual refresh,
local scheduling and tenant-scoped NATS cluster dispatch. DuckDB queries selected
snapshot aliases. [Configuration and operational boundaries](acceleration.md)
cover freshness, grants, types, retention and shared storage.

The [integrated build record](evidence/acceleration-build.json) identifies the
tested Linux binaries and matching VM/local source hashes. Full tagged Go tests,
acceleration/catalog/cluster/worker/HTTP race tests, vet, CLI and sandbox-launcher
builds passed. [Tagged test log](evidence/tests-acceleration.log),
[race log](evidence/race-acceleration.log), [CLI/HTTP regression](evidence/acceptance-acceleration.json)
and [cluster regression](evidence/cluster-acceleration.json) are retained.

The [14-check acceleration acceptance](evidence/acceleration-acceptance.json)
passed exact typed Arrow/Parquet round trips, empty schemas, failed-refresh
retention, source-file outage reads, authorization-version invalidation and
expiry. Real NATS dispatch refreshed two tenants' snapshots; both tenant A
workers read the shared snapshot, scheduled replacements became visible, and
cross-tenant handles remained inaccessible. Linux store race tests also exercise
separate-process writer locking, writer death, readers and generation cleanup.
These are same-host fixtures, not multi-host filesystem or recovery certification.

### Ten-million-row snapshot comparison

The [benchmark script](../scripts/acceleration_benchmark.py) reads four columns
from the first 10 million rows of the existing ClickHouse fixture and creates a
112,829,920-byte Parquet snapshot. Refresh took **2.447 seconds**. Three alternating
query pairs returned 4,096 groups with exactly matching counts, integer sums and
bounds. Median complete CLI query times were **0.201 seconds native** and
**0.240 seconds accelerated**. Native ClickHouse was faster in this comparison.

[Raw evidence](evidence/acceleration-clickhouse.json) records each trial and the
source-unavailable positive/negative controls: snapshot SQL returned the same
result with the registered source endpoint unreachable, while the native query
failed. This demonstrates independent reads after refresh; it does not establish
reduced PostgreSQL/Oracle load or a universal latency advantage.

GNU time reported maximum RSS of 77.9 MiB for refresh, 112.0–114.3 MiB for the
accelerated query trials and 55.8–55.9 MiB for native query trials. These are
GNU time's command/children maxima, not a summed process-tree peak; they exclude
ClickHouse server memory. Source and Kelvo shared the four-CPU, approximately
31-GiB test VM. Engine/client budgets were 512 MiB and two threads; they are not
total-process memory limits. Caches were uncontrolled, only three pairs ran,
and query output contained 4,096 rows rather than 10 million rows. No export,
WAN, cold-cache or sustained-concurrency throughput claim follows from this test.

The benchmark harness's initial timed wait used polling that could add up to
50 ms of observer delay. Final evidence uses a blocking wait with a signal
deadline. No Go source changed for that correction.

No new container image or production deployment was validated for acceleration.
The Compose overlay is provided for operator integration. Incremental refresh,
CDC and per-user row/column policies remain absent. Object snapshots were added in the later validation above.

## Native connector expansion

Six additional native routes are implemented: Exasol, Spanner, Ignite 2, Athena,
DynamoDB, and Cosmos DB for NoSQL. Elasticsearch now supports bound positional
parameters and has live acceptance evidence. Configuration, source dispatch and
worker credential-isolation tests include the new routes. These connectors run
queries at the selected provider; they do not add cross-source DuckDB federation.

The current [integrated build record](evidence/native-expansion-build.json) records
the Go version, kernel, commands, source hashes and binary hashes. Full tagged
Go tests, native/worker/cluster/HTTP race tests, vet, binary and sandbox-launcher
builds, [CLI/HTTP acceptance](evidence/acceptance-native-expansion.json), and
[cluster regression acceptance](evidence/cluster-native-expansion.json) passed on
the dedicated Linux VM. [Tagged tests](evidence/tests-native-expansion.log) and
[race tests](evidence/race-native-expansion.log) are retained. The new connectors
were not throughput-benchmarked, packaged into a newly validated container image,
or deployed to production by this change.

| Connector | Executed checks | Remaining boundary |
| --- | --- | --- |
| Elasticsearch 8.19.0 | [Secured live server](evidence/elasticsearch-native.json): 1,205 rows, pagination, exact int64/NULLs, parameters, limits, index-restricted key, zero open contexts afterward | Package-level acceptance; no cluster or throughput measurement |
| Ignite 2.17.0 | [Authenticated live server](evidence/ignite-native.json): 1,205 rows, exact values, UUID metadata refusal/cast, empty results, failure cleanup, six released cursors | One node behind a verified TLS proxy; no RBAC or sticky-routing acceptance; Ignite 3 unsupported |
| DynamoDB Local 3.1.0 | [Live Local engine](evidence/dynamodb-native.json): complete tagged values, 38-digit decimals, 1,205 rows, empty continuation pages, restrictions/limits, certificate checks | Local does not enforce AWS IAM or prove cloud SigV4 interoperability |
| Exasol | Native WSS protocol fixtures: RSA login, verified TLS, exact DECIMAL values, metadata, batching/fetch, cancellation, rollback acknowledgement, malformed responses and bounded stalls | No live Exasol server; source-side read permissions required |
| Spanner | HTTPS fixtures: source-bound sessions, single-use strong read-only transaction, exact scalar types, cancellation and cleanup | No Google account; materialized REST response limited to 10 MiB; complex types unsupported |
| Athena | Signed HTTPS fixtures: execution/polling, paging/header handling, exact scalar types, cancellation and limits | No AWS account; result files remain in configured S3 storage |
| Cosmos DB for NoSQL | HTTPS fixtures and official HMAC vector: exact JSON, continuation/session headers, empty pages, RU/page budgets, query restrictions | No Azure account; projections/filters only, no distributed aggregates/sorting/query-plan merging |

Direct connector cancellation/cleanup checks run in-process. The outer worker
currently uses immediate process-group termination, which can preempt remote
cleanup on caller cancellation, outer deadlines, or downstream sink failure.
This release does not guarantee provider-side cancellation through the worker
boundary; see [the operational limitation](usage.md#native-cancellation-and-remote-cleanup).

New source guides document unsupported parameters, types and query shapes.
DynamoDB and Cosmos return lossless document bytes in Arrow Binary, not inferred
relational columns. Ignite decimals use annotated exact numeric text because the
REST metadata lacks precision/scale. Exasol bypasses its upstream driver's lossy
number decoding; its rollback-only session does not replace read-only grants.

The first Ignite live scalar test exposed UUID text with binary metadata and
failed safely. The connector now explicitly rejects that mismatch, with a
regression and a validated `CAST(... AS VARCHAR)` path. Protocol fixtures alone
had not exposed this provider behavior.

## Prior cluster and connector expansion

Builds and tests ran on the dedicated Linux amd64 VM, using Go 1.26.8, DuckDB 1.5.6 and Linux `6.12.95+deb12-cloud-amd64`. No local builds were used. The [container image record](evidence/cluster-connectors-image.json) identifies the Docker image and extracted Go/native-launcher binaries; [source hashes](evidence/cluster-connectors-source.json) match the tested VM and local checkout. The CLI, cluster, container and spill acceptance uses those extracted binaries or that image.

- Full `go test -tags duckdb_arrow -p 2 ./...`, tagged `go vet ./...`, and race tests for every native connector, cluster, worker and HTTP package passed. [Tagged test log](evidence/tests-cluster-connectors.log), [race log](evidence/race-cluster-connectors.log). Live database tests are opt-in; a passing default Go suite does not imply that live credentials were available.
- [CLI/HTTP acceptance](evidence/acceptance-cluster-connectors.json) covers YAML, exact decimals, atomic failed exports, authentication, single-consumer Arrow results, limits, cancellation and worker process cleanup.
- [Cluster acceptance](evidence/cluster-acceptance.json) covers tenant isolation, separate worker pools, cross-gateway claims/cancellation, mTLS identities, gateway replacement, one-broker loss with quorum, and worker death without automatic query replay. [NATS store/account tests](evidence/cluster-store-connectors.log) validate durable slots and isolation against three TLS-enabled brokers on the same VM. This is a local failure fixture, not multi-zone HA or backup/restore certification.
- [Container acceptance](evidence/container-acceptance.json) uses the actual Dockerfile image. It verifies nonroot execution, capabilities, no-new-privileges, read-only root, cgroup settings, sandboxed Arrow values and unselected-file denial. Network checks include positive controls and denied cross-tenant/unapproved destination access.
- [DuckDB spill acceptance](evidence/duckdb-spill.json) runs a five-million-row hash-sort/window aggregate with a 64 MiB DuckDB budget and a 1 GiB temporary-data budget. The sandboxed checksum matches the control and actual spill files were observed. Recorded process RSS exceeds the DuckDB budget: that setting is not a total process-memory cap. This fixture returns one aggregate row; it is not an export-throughput benchmark.

### Database acceptance boundaries

| Path | Executed validation | Remaining boundary |
| --- | --- | --- |
| Native PostgreSQL 17.6 and MySQL 8.4 | [Live verified-TLS databases](evidence/relational-acceptance.json): read-only grants/transactions, exact integers/decimals/timestamps/NULLs, parameters, cancellation and limits | MariaDB, CockroachDB, AlloyDB and Redshift share protocol code but were not separately tested |
| MongoDB 8.0.32 | [Live aggregation and restricted SQL](evidence/mongodb-acceptance.json): cursor cleanup, exact BSON, int64/Decimal128 literals, SQL NULL semantics, grouping and empty aggregates | Restricted single-collection SQL; no full SQL dialect, writes or cross-source federation |
| SQLite | [Signed extension fixture](evidence/sqlite-acceptance.json): large int64, text, blob, NULL, read-only access and selected-file boundary | No SQLite concurrency or throughput claim |
| ClickHouse, PostgreSQL/MySQL federation, CSV/Parquet/DuckDB | Initial live and engine evidence below | The original export benchmark predates cluster mode and new connectors |
| Databricks, Snowflake, D1, BigQuery, Elasticsearch, Trino/Presto | HTTPS protocol tests for types, polling/paging, cancellation, origin binding, malformed responses and limits | No live cloud/vendor account acceptance or performance claim |
| SQL Server and Oracle | Driver configuration, conservative read syntax, exact Arrow conversions and request-contract tests | No live vendor server acceptance |
| Flight SQL | Real TLS Flight fixtures including exact batches, allocation/row limits, schema framing, malicious messages, endpoint binding and cancellation | Client only; one result endpoint; no live vendor service or server implementation |
| Optional `dbapi` adapter | HTTPS contract tests plus [container CLI acceptance](evidence/adapter-acceptance.json) for source binding and token handoff | Bounded JSON compatibility; no live deployed gateway/all-backend acceptance |

The [44-engine checklist](source-coverage.md) identifies 24 built-in native routes (including protocol families), three reference file-source routes, and 17 routes requiring an external service. Parquet is additional. Names registered for external adapters do not supply JDBC/vendor drivers. The reference gateway's two SAP entries have incomplete connection builders and need a custom adapter. Metadata browsing, writes, migrations and CDC are separate capabilities.

The MongoDB compiler is pinned to the published [zero-sql commit](https://github.com/SyneHQ/zero-sql/commit/b01a7e87002271a661ebd68060824be013347845); no local module replacement is required. Its new read-only path leaves the legacy write-capable conversion API unchanged. Kelvo deliberately uses only the restricted read-only compiler.

## Initial release checks

The checks and throughput benchmark in this section record the initial release at `45e42f5`. The later YAML configuration change passed the tagged Go tests, vet, build and CLI/HTTP acceptance; it did not rerun that throughput benchmark.

- Full tagged Go tests, vet and binary build passed on Debian 12 / Linux amd64 with Go 1.26.8. HTTP and ClickHouse race tests passed. [Test log](evidence/tests-final.log), [race log](evidence/race-final.log), [build inputs and binary hash](evidence/build-evidence.json).
- The compiled CLI and HTTP server passed exact decimal checks, the README's relative-path configuration, preservation of an existing export on failure, authentication, Arrow delivery, single-consumer results, explicit row-limit failure, queued/active cancellation, and Linux child cleanup after killing the parent. [Acceptance results](evidence/acceptance.json).
- Real PostgreSQL 17.6 and MySQL 8.4 federation returned the expected joined names, exact decimal totals and NULL. Real ClickHouse 26.9.7.9 returned a 100,000,000-row count and ID sum of 4,999,999,950,000,000. CSV aggregation also matched. Signed DuckDB 1.5.6 extensions were provisioned with canonical filenames. These are functional checks, not capacity tests. [Database results](evidence/real-database-results.json).
- Engine tests cover registered CSV, Parquet and DuckDB files, typed parameters, exact decimals, strings, NULLs and denied capabilities. Native adapter tests include fragmented multi-batch streams, dictionary replacement, late source errors, truncation, allocation/output limits and credential redaction.

## Ten-million-row native export

The [public helper and exact fixture](benchmarking.md) select six columns from 10 million of 100 million synthetic ClickHouse rows. Each timed run includes coordinator launch, native source execution, worker/coordinator Arrow delivery, output-file persistence and final caller fsync. Arrow value validation and checksums happen after timing. The output is 659,299,304 bytes (628.76 MiB) in 154 batches; logical Arrow buffers account for 659,232,799 bytes.

| Trial | Export seconds | Million rows/s | Coordinator peak RSS | Worker peak RSS |
| --- | ---: | ---: | ---: | ---: |
| [1](evidence/clickhouse-transfer-trial-1.json) | 6.739 | 1.484 | 55.79 MiB | 50.05 MiB |
| [2](evidence/clickhouse-transfer-trial-2.json) | 8.490 | 1.178 | 57.87 MiB | 49.81 MiB |
| [3](evidence/clickhouse-transfer-trial-3.json) | 8.122 | 1.231 | 57.78 MiB | 51.57 MiB |

Every run passed row count, ID sum (499,995,495,000,000), schema, timestamp-value formula, payload-format and NULL-count checks. File and Arrow-buffer checksums matched across all three runs. Median complete-export throughput was 1.231 million rows/s; encoded throughput ranged from 77.66 to 97.83 MB/s (decimal units).

The VM has four logical CPUs and about 31 GiB RAM. ClickHouse shared that VM with Kelvo and was restricted to 1.5 CPUs and 4 GiB container memory. Kelvo used two query threads, a 1 GiB source-query/Arrow-decoder budget, an 11-million-row ceiling, 1 GiB encoded-byte ceiling and a 120-second timeout. The source cache was warm/uncontrolled. This was a local VM path without browser rendering or WAN transfer.

RSS was sampled every 20 ms and can miss short peaks. Coordinator and worker peaks are separate observations: do not add them, because shared pages can be counted twice. They exclude ClickHouse, the validation client, kernel buffers and filesystem cache. The 1 GiB query budget is distinct from Kelvo's observed roughly 50–58 MiB per-process RSS. These figures do not establish the memory requirements of DuckDB joins, sorts, concurrency or larger values.

The internal execution durations were about 2.89–3.23 seconds and excluded final export persistence. The table uses complete-export time. The Rust reference used a different transport/client path, so these results do not support a Go-versus-Rust speedup claim.

## Failures retained

The first 512 MiB source-query attempt failed after approximately 69 MB of Arrow output. ClickHouse returned memory-limit error 241 after its HTTP 200 response; Kelvo initially misclassified the late text exception as an Arrow allocation limit. That classification is fixed and regression-tested. A repeat with the final binary correctly reported the ClickHouse source memory error. [Initial failure](evidence/clickhouse-transfer-initial-failure.json), [corrected diagnostic](evidence/clickhouse-transfer-512-recheck.json). The successful trials used 1 GiB; 512 MiB did not pass this workload.

An initial benchmark assertion expected ClickHouse DateTime to be an Arrow timestamp. ClickHouse actually emits it as UInt32 Unix seconds; the export succeeded but that harness assertion failed. The helper now validates the documented mapping and exact epoch-second values without an optional NumPy dependency. All successful trials above ran after correction. [Initial harness failure](evidence/benchmark-harness-initial-failure.json), [mapping and explicit timestamp cast](../internal/sources/clickhouse/README.md).

## Remaining acceptance work

Production deployment review, multi-zone broker/storage recovery, sustained tenant concurrency, cold-cache and slow-client/WAN workloads, per-provider live acceptance and broad native-memory profiling remain outstanding. Per-user row/column authorization, token/tenant management APIs, durable result storage, CDC, a Flight SQL server and distributed execution of one SQL plan are not implemented. The pinned DuckDB Go path materializes execution before Arrow delivery; streaming native ClickHouse output does not change that limitation.
