# Validation evidence

This is recorded developer-preview evidence, tied to specific binaries and fixtures. Use [production status](production-status.md) for current release gates; these results do not certify arbitrary multi-tenant deployments.

Latest: [merged-cargo acceptance](evidence/cargo-b1a0ea5-acceptance.json) passed both CI jobs on `b1a0ea5`, including worker/broker export gates and all 15 notebooks. [Export lifecycle acceptance](export-ci-diagnostics.md) separates these direct passes from the retained historical failure and its unknown cause. [Earlier combined validation](export-validation.md) remains tied to `d144a43`.

The [telemetry comparison](telemetry-overhead.md#recorded-full-comparison) passed all 48 epochs on `93339e7`, with 96 warmups and 96 measured queries. Paired metrics/tracing ratios describe one local million-row fixture; they do not establish deployment capacity or separate source/compute time. Both smoke attempts and full cleanup evidence are retained.

The [pinned worker-capacity gate](node-capacity.md) passed on `b1a0ea5` with observer `fbb9ae8`: five matched metrics on/off pairs, 130/130 workload queries and a separate 3/3 preflight. Exact Arrow results, resource gates, independent reconciliation and both-host cleanup passed. Its warm-cache, worker-only scope excludes the Azure gateway, NATS, source database and SSH tunnels; sustained and deployment capacity remain separate.

The [first `b1a0ea5` attempt](evidence/node-capacity-b1a0ea5-preflight-refusal.json) remains recorded: its source policy refused all three preflight queries. The fresh campaign used bounded source settings, passed 24 live policy checks and retained the same runtime.

## Catalog authority

[Linux acceptance](evidence/catalog-authority.json) on `bcbb3dc` passed nine stages, including all 34 required correctness/race/stub controls and 11 real sandboxed query/export/snapshot controls. It verifies legacy policy compatibility, stale-authority refusal, detached execution definitions and exact Arrow results after attempted source retargeting. Source/bridge inputs and independent cleanup checks passed; fixture-dependent skips remain in the record. No provider or capacity claim.

## Expanded federation and public adapter SDK

The optional Go/C++ bridge supports ClickHouse, PostgreSQL, MySQL, SQL Server, Oracle, Snowflake, BigQuery and Databricks. See the [adapter guide](federation-adapters.md) for namespaces, types and the public `github.com/SYNEHQ/kelvo-go/federation` interface.

| Evidence | Executed scope |
| --- | --- |
| [Build](evidence/federation-expansion.json) | Ordinary/bridge tests and vet; SDK/catalog/federation/bridge/DuckDB/worker race checks with `cgocheck2` on Azure |
| [SQL Server 2022](evidence/federation-sqlserver.json) | 25 native/federated checks: exact types including 100ns timestamps, pushdown, self/cross-source joins, CTE/windows, verified TLS negatives, SELECT grants, namespaces, limits, observed cancellation and recovery |
| Snowflake/BigQuery/Databricks protocol fixtures | HTTPS types/pages/failures/cancellation; real DuckDB joins of fixture results with CSV |
| [External SDK example](evidence/federation-adapter-example.json) | 13 CLI/direct-worker cases: external-module build, CSV join, pushdown/local filters, separate worker and limits without partial export |

Pinned build versions: Go 1.26.8, DuckDB 1.5.6, duckdb-go 2.10506.0 and Arrow Go 18.5.1. Live Oracle TCPS, Snowflake, BigQuery and Databricks acceptance remains pending; protocol fixtures establish neither warehouse grants nor billing behavior.

Integer/Boolean filters can push down. String, decimal, float and temporal predicates, including their NULL checks, remain in DuckDB; unsupported required predicates fail. Private pointer scanners cannot be called from user SQL.

Adapters are trusted compiled-in code responsible for source authorization, verified transport and cleanup. The core checks selection, schema, limits and borrowed batches; the SDK is not an untrusted-plugin sandbox. Reproduce with the [SQL harness](../scripts/federation_sql_acceptance.py) and [SDK harness](../scripts/test_federation_adapter_example.py). SQL fixtures were removed while existing ClickHouse data was preserved. No new throughput claim follows.

## Real CTE analytics, DuckDB and Polars

The [workflow report](analytics-workflow-benchmarks.md) covers rolling KPIs, borough hotspots, route joins and zone momentum over 22,612,607 NYC Taxi trips/265 zones; 22,611,807 trips belong to the common quarter cohort. Exact outputs match ClickHouse references and independent raw-data/window checks.

All 84 measured attempts and 28 separate preflights passed: 36 same-Parquet Azure trials, 24 Azure live-source and 24 Oracle micro live-source trials. The [reconciled summary](evidence/analytics-workflow-summary.json) binds artifacts to trials; [five controls](evidence/analytics-evidence-regression.json) reject tampering/missing attempts. [Runtime fingerprints](evidence/analytics-workflow-runtime.json) identify the reused compression binary and libraries.

| Path | Median range | Interpretation |
| --- | ---: | --- |
| Same-file Kelvo | 0.484–1.497s | Fresh commands include imports/setup/persistence; Polars wins the route workflow |
| Oracle native | 1.480–3.727s | SQL executes in Azure ClickHouse |
| Oracle federation | 33.490–62.275s | Source transfer and local DuckDB computation |

Live runs used a 640 MiB service cap; Azure's federated hotspot reached it and spilled. These are not SQL-kernel comparisons or evidence of sustained concurrency/headroom.

## Oracle micro VM and opt-in result compression

The [micro-VM report](oracle-micro-capacity.md) uses `VM.Standard.E2.1.Micro`, 951 MiB visible RAM, a 640 MiB service cap and ClickHouse on Azure.

| Native export profile | Verified work | Aggregate output rate |
| --- | --- | ---: |
| No result compression | 30 × 1m rows, ten concurrent workers | 242,827 rows/s |
| LZ4 results | 30 × 1m rows, ten concurrent workers | 469,800 rows/s |
| LZ4, 4m-row/128 MiB limits | 30 × 4m rows, three ten-worker cohorts | 443,421 rows/s |

Source LZ4 was enabled in both million-row profiles. All results matched decoded references and complete Arrow/API success. The 120m-row campaign peaked at 423.4 MiB charged cgroup memory with no OOM/task-limit events.

These are narrow three-column native results over SSH forwarding, not direct HTTPS, wide-row federation or sustained entitlement on a burstable shape. The report retains ten-worker federation OOM, sort timeout and slower federated compression results.

[Build checks](evidence/oracle-micro-result-build.json) passed on Azure. [Cluster LZ4 acceptance](evidence/cluster-compression-acceptance.json) checked eight exact typed batches, completion and byte accounting. Compression is opt-in for native/federated/accelerated results; the worker pipe stays uncompressed and limits remain enforced.

## PostgreSQL/MySQL federation and NYC Taxi capacity

The [build record](evidence/federation-capacity-build.json) matches 187 inputs to the tested image. Ordinary/bridge [CI passed at `6a2e94b`](https://github.com/SyneHQ/kelvo-go/actions/runs/36953244744), as did focused [race checks](evidence/race-federation-cancellation.log).

| Evidence | Result and boundary |
| --- | --- |
| [Container controls](evidence/federation-capacity-container.json) | Nonroot, read-only mounts/root, dropped capabilities, CPU/PID/memory, Landlock and network denial with reachable controls; small exact query |
| [Relational acceptance](evidence/federation-relational.json) | 45 checks: exact values, two/three-source joins, self-joins, TLS negatives, grants, selected tables, limits and cancellation |
| [NYC Taxi capacity](federation-capacity.md) | 14 checks; 13 exact result sets; complete 22.6m-row sort/export in 7.738–7.835s (2.886–2.922m rows/s) |
| [Remote exports](evidence/federation-wan.json) | 1m rows in 5.364–8.496s with matching Arrow hashes; SSH forwarding and inner TLS |
| [Cluster load](evidence/federation-cluster-capacity.json) | 224/224 exact jobs; 1/2/4 clients for 120s each; 0.475/0.650/0.683 queries/s; p95 2.69/3.94/7.59s |

PostgreSQL/MySQL require explicit native TLS DSNs and selected tables; the ClickHouse fixture used HTTP. Custom adapters keep DuckDB external access disabled. Million-row PostgreSQL/MySQL joins took about 1.63s each; the three-adapter join took 3.14s. Every fact scan fetched all 22,612,607 rows.

Full-sort workers reached 717–834 MiB sampled RSS despite 128/256 MiB DuckDB budgets; the separate coordinator used about 61 MiB. Large workloads ran as host processes, not in the small container fixture. Do not sum separate peaks or infer a 512 MiB deployment fit. Throughput flattened beyond two clients on the shared four-core host; this is not multi-machine scaling.

The [initial cancellation failure](evidence/federation-relational-pre-cancellation-fix.json) led to bounded cooperative shutdown and source controls. PostgreSQL stopped upstream about 0.203s after cancellation; MySQL stopped 3.154s after launch under its three-second server timeout. Immediate remote cancellation is not guaranteed. The [CA fixture fix](evidence/federation-capacity-fixture-tls.json) corrected key usage without disabling verification.

Owned [cluster](evidence/federation-capacity-cleanup.json) and [relational](evidence/federation-relational-cleanup.json) fixtures were stopped; taxi and original 100m-row ClickHouse data remained. The capacity guide retains scratch, slow-consumer, paced-source and cancellation observations.

## DuckDB custom federation adapter

The initial opt-in Linux amd64 C++ shim connected the Go ClickHouse reader to DuckDB. [Build inputs](evidence/federation-build.json) matched 181 files; the [container record](evidence/federation-container.json) identifies the image/binary. [Tagged tests](evidence/tests-federation.log), [race/strict-cgo checks](evidence/race-federation.log) and vet passed.

The same image passed [CLI/HTTP](evidence/acceptance-federation.json), [NATS store](evidence/cluster-store-federation.log), [10 cluster](evidence/cluster-federation.json) and [14 acceleration checks](evidence/acceleration-federation.json). Container tests covered privilege, filesystem and network controls; these were same-VM fixtures.

[ClickHouse 26.9.7.9](evidence/federation-clickhouse.json) passed 27 checks over an owned million-row table: exact extrema/decimal/NULL, projection/integer predicates, joins, selected-table/global 32-table bounds, cancellation, Landlock and SELECT-only write denial. Failed exports were not published; existing benchmark data remained.

For `row_id >= 999990`, ten rows crossed the source boundary. Selecting only `row_id` used 80 logical/352 body bytes versus 5,244/5,624 with payload. This excludes discovery/headers and proves avoided transfer, not reduced disk scanning. A NULL filter stayed local and fetched all rows; `COUNT(*)` selected the first physical column.

### One-million-row federated export

Each ordered two-column export persisted 16,094,328 bytes with identical exact values/checksum. Timing includes startup, source transfer, ordering, file sync, sampler and active capture proxy; loading/checksum validation is outside.

| Trial | Seconds | Million rows/s | Coordinator RSS | Worker RSS |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0.291 | 3.442 | 58.76 MiB | 157.82 MiB |
| 2 | 0.305 | 3.276 | 58.83 MiB | 150.62 MiB |
| 3 | 0.296 | 3.377 | 57.84 MiB | 152.10 MiB |

Median: 3.377m rows/s; 23 source batches/trial. The four-CPU/~31 GiB VM also hosted ClickHouse/proxy; caches were uncontrolled. DuckDB used two threads/256 MiB, 64 MiB source/output limits and a 2m-row ceiling. Separate 20ms RSS peaks exclude source/proxy/kernel/cache and can miss spikes; do not add them or equate them with the engine budget.

This smaller/narrower workload establishes no speedup over the native benchmark or other engines. DuckDB materializes execution; joins/aggregates/order/LIMIT are not generally pushed down. See the [adapter contract](federation.md) and later capacity scope above.

Early CI found inherited Git discovery skipping a native accessor patch and an ENOENT/ESRCH observer race. Provisioning isolation, content verification and five regression cases corrected these without changing the measured engine.

## Object snapshots and MongoDB refresh

[Build provenance](evidence/object-storage-build.json), [tagged tests](evidence/tests-object-storage.log), [race checks](evidence/race-object-storage.log), vet/builds and [CLI/HTTP](evidence/acceptance-object-storage.json), [cluster](evidence/cluster-object-storage.json), [14 snapshot checks](evidence/acceleration-object-storage.json) passed on the recorded baseline.

[36 object checks](evidence/object-acceleration.json) used private S3/R2/GCS/Azure TLS fixtures: selected columns fetched 16,984 bytes in two ranges from ~3.15 MB objects. Tests covered reader identity, outages, exact data, failed refresh, metadata corruption and redirect/range refusal; foreign endpoints received no requests.

Median full query/validation was 258–285ms remote versus 59–63ms local across three alternating trials/provider. A generated 32 MiB loopback test reached 663.95 MB/s. These include setup/validation and establish neither isolated bridge overhead nor cloud/WAN capacity. See [object limits](object-storage.md).

The initial direct HTTPFS cloud path followed a foreign redirect with an S3 session token and failed confinement. It was removed; the parent Go range reader refuses redirects and keeps cloud credentials out of query children. Later scheduling clock-skew tests cover ±4h offsets; records retain both binary identities.

[MongoDB 8.0.32](evidence/mongodb-acceleration.json) checked pipeline refresh, filtered/grouped/empty results and exact BSON/int64/Decimal128 snapshots. `$out`/`$merge` fail without replacing the committed generation. BSON stays binary; there is no inferred relational schema or CDC.

## Dataset acceleration

[Acceleration](acceleration.md) provides full-refresh Parquet snapshots with manual/local schedules and tenant NATS dispatch. [Build inputs](evidence/acceleration-build.json), [tagged tests](evidence/tests-acceleration.log), [race checks](evidence/race-acceleration.log), vet/builds and [CLI](evidence/acceptance-acceleration.json)/[cluster](evidence/cluster-acceleration.json) regression passed.

[14 acceptance checks](evidence/acceleration-acceptance.json) cover typed/empty round trips, retained snapshots, source outages, authorization/freshness, scheduled two-tenant refresh and shared readers. Separate-process locking/death tests are local fixtures, not multi-host filesystem certification.

### Ten-million-row snapshot comparison

The [script](../scripts/acceleration_benchmark.py) refreshed four ClickHouse columns into 112,829,920 bytes of Parquet in 2.447s. Three alternating pairs returned 4,096 exactly matched groups. Median CLI query: **0.201s native; 0.240s accelerated**. Native ClickHouse was faster.

[Raw evidence](evidence/acceleration-clickhouse.json) includes source-unavailable controls: snapshot queries succeeded while native queries failed. This proves independent reads, not universal speedups or measured PostgreSQL/Oracle load reduction.

GNU time reported command/children maxima: 77.9 MiB refresh, 112.0–114.3 MiB accelerated, 55.8–55.9 MiB native. They are not summed tree peaks and exclude ClickHouse. The shared four-core/~31 GiB VM used two threads/512 MiB budgets, uncontrolled caches and only three pairs. This aggregate-output test establishes no export/WAN/concurrency throughput.

A polling wait that could add 50ms was replaced with blocking timed wait before final evidence; no Go code changed. Later image validation is above. Incremental refresh, CDC and per-user row/column policies remain absent.

## Native connector expansion

Exasol, Spanner, Ignite 2, Athena, DynamoDB and Cosmos DB added native routes; Elasticsearch added parameters/live checks. Native routes execute remotely and do not automatically gain federation.

[Build provenance](evidence/native-expansion-build.json), [tagged tests](evidence/tests-native-expansion.log), [race checks](evidence/race-native-expansion.log), vet/builds and [CLI](evidence/acceptance-native-expansion.json)/[cluster](evidence/cluster-native-expansion.json) acceptance passed on Linux. This milestone added no container, throughput or deployment acceptance.

| Connector | Evidence and boundary |
| --- | --- |
| Elasticsearch 8.19.0 | [Live secured server](evidence/elasticsearch-native.json): 1,205 rows, exact values, paging/limits/restricted key, no open contexts; no throughput/cluster claim |
| Ignite 2.17.0 | [Live authenticated server](evidence/ignite-native.json): 1,205 rows, six released cursors, UUID refusal/cast; one node behind TLS proxy, no RBAC/sticky routing, no Ignite 3 |
| DynamoDB Local 3.1.0 | [Local engine](evidence/dynamodb-native.json): tagged values/38-digit decimals, paging/limits/TLS; no cloud IAM/SigV4 acceptance |
| Exasol | Native WSS fixtures: exact decimals, TLS/login, paging/cancel/rollback; no live server; read grants still required |
| Spanner | HTTPS read-only session/type/cleanup fixtures; no Google account, 10 MiB materialized-response cap, no complex types |
| Athena | Signed HTTPS execution/paging/cancel fixtures; no AWS account; result files remain in configured S3 |
| Cosmos DB | HTTPS/HMAC fixtures, exact documents, continuation and RU/page bounds; no Azure account or distributed aggregate/sort merging |

DynamoDB/Cosmos return Arrow Binary documents. Ignite decimals use exact annotated text; its first live UUID mismatch failed safely and led to explicit rejection plus a verified cast. Exasol avoids lossy driver decoding, but rollback-only sessions do not replace grants.

In-process cancellation does not guarantee provider cleanup through worker termination. See [current cancellation limits](usage.md#native-cancellation-and-remote-cleanup).

## Prior cluster and connector expansion

Linux amd64 checks used Go 1.26.8, DuckDB 1.5.6 and kernel `6.12.95+deb12-cloud-amd64`. The [image](evidence/cluster-connectors-image.json) and [source hashes](evidence/cluster-connectors-source.json) identify extracted binaries used for acceptance.

| Evidence | Scope |
| --- | --- |
| [Tests](evidence/tests-cluster-connectors.log), [race](evidence/race-cluster-connectors.log), vet/build | Full tagged suite; opt-in live tests require their own credentials |
| [CLI/HTTP](evidence/acceptance-cluster-connectors.json) | Types, failed-export atomicity, auth, single consumer, limits/cancel and process cleanup |
| [Cluster](evidence/cluster-acceptance.json), [NATS store/accounts](evidence/cluster-store-connectors.log) | Tenant/mTLS isolation, replacement, one-broker quorum loss and no worker replay; three same-host brokers |
| [Container](evidence/container-acceptance.json) | Nonroot/capabilities/read-only/cgroups/Landlock/network denial with positive controls |
| [Spill](evidence/duckdb-spill.json) | 5m-row sort/window, 64 MiB DuckDB and 1 GiB temp budgets; actual spill and exact checksum; RSS exceeded engine budget |

Spill returned one aggregate row, not an export benchmark. Local broker controls do not certify multi-zone HA or backup recovery.

### Database acceptance boundaries

| Path | Executed validation | Boundary |
| --- | --- | --- |
| PostgreSQL 17.6 / MySQL 8.4 | [Live TLS types/grants/parameters/cancel](evidence/relational-acceptance.json) | Protocol relatives not separately tested |
| MongoDB 8.0.32 | [Live aggregation/restricted SQL](evidence/mongodb-acceptance.json) | Single collection, read-only subset; no cross-source federation |
| SQLite | [Signed extension and exact values](evidence/sqlite-acceptance.json) | No concurrency/throughput claim |
| ClickHouse / relational federation / files | Initial and later evidence above/below | Different milestones use different binaries |
| SQL Server 2022 | [25 live native/federated checks](evidence/federation-sqlserver.json) | Functional acceptance, no new throughput result |
| Oracle | Driver configuration, read syntax, Arrow/request tests | Live Oracle server/TCPS acceptance pending |
| Databricks / Snowflake / D1 / BigQuery / Trino/Presto | HTTPS protocol/type/paging/cancellation fixtures | No live vendor/account acceptance |
| Flight SQL | Real TLS fixtures, exact batches, schema/limits/endpoint/cancel checks | Client only, one result endpoint, no live vendor service |
| Optional `dbapi` | HTTPS contract and [container CLI](evidence/adapter-acceptance.json) | Bounded JSON; no deployed gateway/all-backend acceptance |

Use the [source checklist](source-coverage.md) for current routing. Registered external names do not supply vendor/JDBC drivers. Metadata browsing, writes, migrations and CDC are separate capabilities. The published [zero-sql pin](https://github.com/SyneHQ/zero-sql/commit/b01a7e87002271a661ebd68060824be013347845) supplies only Kelvo's restricted read-only compiler path.

## Initial release checks

These results belong to `45e42f5`. Later YAML changes passed tests/vet/build/CLI acceptance but did **not** rerun this throughput benchmark.

| Evidence | Scope |
| --- | --- |
| [Tests](evidence/tests-final.log), [race](evidence/race-final.log), [build](evidence/build-evidence.json) | Debian 12/amd64, Go 1.26.8; tagged tests/vet/build and HTTP/ClickHouse race |
| [CLI/HTTP](evidence/acceptance.json) | Exact decimals, relative config, failed-export preservation, auth/results/limits/cancel and parent-death cleanup |
| [Live databases](evidence/real-database-results.json) | PostgreSQL/MySQL joins; ClickHouse 100m count and sum 4,999,999,950,000,000; CSV and signed extensions |

Engine/source tests also cover selected files, parameters, exact types, denied capabilities, fragmented/dictionary streams, late errors, truncation, limits and redaction.

## Ten-million-row native export

The [public helper](benchmarking.md) exports six columns from 10m of 100m synthetic ClickHouse rows. Timing includes coordinator launch, source execution, Arrow transfer, output persistence and final fsync; value/checksum validation follows timing.

Output: 659,299,304 encoded bytes in 154 batches; logical Arrow buffers: 659,232,799 bytes.

| Trial | Seconds | Million rows/s | Coordinator RSS | Worker RSS |
| --- | ---: | ---: | ---: | ---: |
| [1](evidence/clickhouse-transfer-trial-1.json) | 6.739 | 1.484 | 55.79 MiB | 50.05 MiB |
| [2](evidence/clickhouse-transfer-trial-2.json) | 8.490 | 1.178 | 57.87 MiB | 49.81 MiB |
| [3](evidence/clickhouse-transfer-trial-3.json) | 8.122 | 1.231 | 57.78 MiB | 51.57 MiB |

All rows, ID sum (499,995,495,000,000), schema, timestamp/payload formulas, NULL counts and checksums matched. Median complete-export throughput: **1.231m rows/s**; encoded rate: 77.66–97.83 MB/s. Internal execution alone took 2.89–3.23s; the table includes persistence.

The four-CPU/~31 GiB VM colocated ClickHouse (1.5 CPUs/4 GiB). Kelvo used two threads, 1 GiB source-query/decoder and byte budgets, an 11m-row limit and 120s timeout. Cache was warm/uncontrolled; there was no browser or WAN.

RSS samples every 20ms can miss peaks. Separate process peaks exclude source/client/kernel/cache and must not be summed. Roughly 50–58 MiB observed RSS is distinct from configured budgets and says nothing about DuckDB joins, sorts or concurrency. Different transport/client benchmarks cannot establish a language/runtime speedup.

## Failures retained

| Failure | Outcome |
| --- | --- |
| [512 MiB attempt](evidence/clickhouse-transfer-initial-failure.json) | Source error 241 after ~69 MB/HTTP 200 was initially misclassified as Arrow allocation; [recheck](evidence/clickhouse-transfer-512-recheck.json) reports source memory correctly. Successful trials used 1 GiB |
| [Timestamp assertion](evidence/benchmark-harness-initial-failure.json) | Export succeeded but harness expected Arrow timestamp; ClickHouse DateTime was UInt32 Unix seconds. Corrected exact mapping preceded successful trials; see [cast/mapping guide](../internal/sources/clickhouse/README.md) |

## Runtime recovery

[Focused recovery checks](runtime-recovery.md) cover heartbeat/result handoff, startup request budgets and bounded scratch lease cleanup. The combined candidate passed race/vet and isolated broker checks. The pinned worker WAN-capacity gate above passed; multi-hour and deployment-specific capacity remain separate.

## Remaining acceptance work

Track current work in [production status](production-status.md): deployment review, multi-zone recovery, provider acceptance and deployment-specific sustained capacity remain separate gates. Callback federation and guarded local/object snapshots have per-user row/column policies; native SQL and coordinated policy lifecycle remain open. Durable federated export jobs/downloads have [combined acceptance evidence](export-validation.md); sustained export capacity and deployment acceptance remain separate. Result caching, CDC, a Flight SQL server and cross-node execution of one SQL plan remain absent. The pinned DuckDB path materializes execution before Arrow delivery.
