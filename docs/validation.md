# Validation — 2026-10-01

Status: a tested developer preview. These measurements describe one native ClickHouse workload on one VM; they do not establish production sizing, all-engine performance, or a multi-tenant security boundary.

The build and benchmark evidence below records the initial release at `45e42f5`. The later YAML configuration change passed the tagged Go tests, vet, build and CLI/HTTP acceptance on the VM; it did not rerun the throughput benchmark.

## Functional checks

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

Production multi-tenant authorization and isolation, Flight SQL, CDC, HA/recovery, durable result storage, additional database adapters, sustained concurrency, cold-cache tests, slow-client/WAN workloads and broad native-memory/spill profiling remain unvalidated or unimplemented. The pinned DuckDB Go path materializes execution before Arrow delivery; the streaming ClickHouse results above do not change that limitation.
