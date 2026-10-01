# Validation — 2026-10-01

Status: a tested developer preview. Cluster, sandbox, connector and failure-handling acceptance is described below. These checks do not certify a production multi-tenant service, establish capacity for every database, or replace deployment-specific security and recovery testing.

## Cluster and connector expansion

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

The [44-engine checklist](source-coverage.md) identifies 18 built-in native routes (including protocol families), three reference file-source routes, and 23 routes requiring an external service. Parquet is additional. Names registered for external adapters do not supply JDBC/vendor drivers. The reference gateway's two SAP entries have incomplete connection builders and need a custom adapter. Metadata browsing, writes, migrations and CDC are separate capabilities.

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
