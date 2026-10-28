# Pipeline-scale evidence

Can Kelvo run the analytical pipelines a mid-size e-commerce or logistics company runs: several operational databases, federated joins, snapshot refreshes, dashboards opened by many people at once, and hours of mixed load while parts of the cluster fail? This page records one measured attempt on 2026-10-27. Every answer was checked against an independent reference. The short version: it was correct everywhere it answered, it degraded loudly rather than silently, and on this hardware it saturates at about 3.5 dashboard queries per second.

**Hardware label: noisy shared VM.** Everything (three databases, three NATS brokers, two gateways, three nodes and the load client) ran on one Azure runner, slot 10: 4 vCPU AMD EPYC 7763, 15 GiB RAM, kernel 6.17. Caches were warm and uncontrolled. One timed run per configuration unless stated otherwise.

## Workload model

| Pattern modeled | Where it shows up here |
| --- | --- |
| Shiprocket-style multi-tenant seller platforms on shared MySQL reporting tables | 8 seller tenants via `tenant_id` on `sellers` and `orders`. The restricted principal `a/seller3` sees one tenant's rows and a column subset |
| Swiggy/Zepto-style orders, deliveries and rider events | `orders` (PostgreSQL), `shipments` with promise/pick/deliver times (MySQL), eight tracking pings per shipment (ClickHouse + Parquet) |
| Meesho-style hot vs. cold dashboards | Pings from December 2025 onward are in ClickHouse; older pings are a 153M-row Parquet file. `hot_cold_daily_events` unions the two |
| Netflix-style dashboard fan-out (one page load is about 30–60 queries) | A 50-tile dashboard, each tile a separate query through the gateway |

Tenant scheme: the tenant is a column on shared tables, not one schema per tenant. A deployment that gives each tenant its own MySQL schema would multiply catalog entries and connections. That was not measured.

## Dataset

[`benchmarks/pipelines/generate.py`](../benchmarks/pipelines/generate.py) uses DuckDB to generate every value from `hash(seed, tag, row ordinal)`. Seed `20261027`, scale 20,000,000 orders. [Load record](evidence/pipeline-dataset.json):

| Table | Rows | Parquet MiB | Placed in | Load s |
| --- | ---: | ---: | --- | ---: |
| orders | 20,000,000 | 275.1 | PostgreSQL 17.6 | 63.7 |
| customers | 1,000,000 | 3.4 | PostgreSQL 17.6 | 2.3 |
| shipments | 20,000,000 | 378.2 | MySQL 8.4 | 153.3 |
| sellers | 50,000 | 0.2 | MySQL 8.4 | 0.5 |
| warehouses | 200 | <0.1 | MySQL 8.4 | 0.2 |
| order_items | 50,001,759 | 396.9 | ClickHouse 25.8 | 12.5 |
| events_hot (from 2025-12-01) | 6,771,597 | 86.0 | ClickHouse 25.8 | 3.1 |
| events_cold (before 2025-12-01) | 153,228,403 | 1,942.4 | Parquet file | — |

That is 250.9M rows in total. On disk: PostgreSQL 2.84 GB, MySQL 1.66 GB, ClickHouse 0.99 GB.

**The dataset is smaller than the brief asked for.** The target was about 100M orders and up to 1B events; this run used 20M orders and 160M events. Generation and loading were fast: about 3 minutes to generate and 4 minutes to load. The limits came from elsewhere:

- Federation caps `max_scan_rows` at 100,000,000 per source.
- A full refresh materializes the whole snapshot in memory: about 3.2 GB RSS for 20M orders.
- The runner had a 5-hour lifetime.

At 5x the size, every federated workflow and every snapshot refresh would also be 5x slower than the tables below. That is extrapolation, not measurement.

**Determinism.** Generating twice at full scale with different thread counts gave identical row counts and content checksums for every table (output below). Checksums are order-independent sums of per-row hashes. Parquet bytes are not compared because writer layout varies with thread count.

## Topology and policy

- **Cluster harness:** [`scripts/pipeline_cluster.py`](../scripts/pipeline_cluster.py) reuses `federation_capacity_cluster.py`: three TLS NATS 2.15.0 brokers (R3), two gateways, nodes `a1`/`a2` (tenant a, 2 slots each) and `b1` (tenant b, 1 slot), Landlock query children and mutual TLS.
- **What it adds:**
  - file-backed version-2 gateway keys bound to principals;
  - identical `policy.access` on the gateway, cluster-init and worker policies;
  - per-query limits of 768 MiB DuckDB memory, 2 threads, 600 s timeout and 60M rows;
  - larger NATS account storage, because the job KV store is (`max_queries` + slots) × 512 KiB × three replicas.
- **Sources:** [`scripts/pipeline_fixture.py`](../scripts/pipeline_fixture.py) reuses `federation_relational_acceptance.py` (TLS, read-only principals). It adds an explicit Docker subnet, because Docker 28 refuses `--ip` without one, and raises the containers to 1.5 CPU and 3 GiB.
- **Principals:**
  - `a/ops`: whole sources plus seven snapshot datasets.
  - `a/seller3`: `pg.orders` limited to `tenant_id = 3` and six columns; `my.shipments` limited to four columns.
  - `b/ops`: a second company (a 3PL) that can read only `my.shipments` and `ch.events_hot`.
- **Binary:** built with the pinned DuckDB bridge (`-tags duckdb_arrow,duckbridge`) from `cargo` at `18dda549`. The ordinary build refuses custom federation with `UNSUPPORTED`.

| Artifact | SHA-256 |
| --- | --- |
| `bin/kelvo` (bridge build) | `394b4cc72501b4e91de4b194402836a07e25a2ba9d824b7da1f7141d4f81441d` |
| `bin/kelvo-landlock` | `712e1cd23d1d60e4f17b77c5309d0f3347a0f2ddb31a2a2548a263c1c2430621` |
| `generate.py` | `518e8bc16187ea50d42534f923c57ac0ab82b3ed938e05cf9d07dd79f890221a` |
| `workflows.yml` | `32694d80cbaca108f167accf740383571280bc6bfe694c6f591b9c557e3664a0` |
| Images | `postgres:17.6` `50903ccd…`, `mysql:8.4` `b35575a0…`, `clickhouse/clickhouse-server:25.8` (25.8.33.6) `7eab9ba1…` |

**Cluster mode ran on one host only.** The brief asked for a node on slot 12 joined over the tailnet. Not done: the reused fixture issues loopback-only certificates and its client refuses non-loopback gateways. Slot 12 ran the determinism check only.

## Correctness method

[`workflows.yml`](../benchmarks/pipelines/workflows.yml) defines ten workflows. Each has one DuckDB-dialect SQL text with table placeholders. [`scripts/pipeline_load.py`](../scripts/pipeline_load.py) binds that text to `pg.*`/`my.*`/`ch.*`/`cold` (federated), to `<table>_snap` (snapshots) or to `read_parquet` over the generator output ([DuckDB reference](evidence/pipeline-references.json), computed outside all timings). Native variants are hand-written PostgreSQL or ClickHouse SQL.

Every output column is BIGINT, VARCHAR or DATE. Answers are compared as an order-insensitive SHA-256 over normalized rows. Any difference counts as `EXACT_REFERENCE_MISMATCH`, a failure, and is never retried. The restricted workflow's reference applies `tenant_id = 3` explicitly, while the Kelvo SQL does not filter, so the policy is what produces the match.

## Workflows: native, federated, snapshot

[Live run](evidence/pipeline-workflows-live.json) (native and federated, plus the first snapshot attempt) · [snapshot run](evidence/pipeline-workflows-snapshot.json). The second of two runs is shown. For snapshot mode, "cold" is the first read, which waited for the on-demand refresh.

| Workflow | Sources | Native s | Federated s | Rows fetched (fed.) | Fed. worker RSS MiB | Snapshot cold s | Snapshot warm s | Exact |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| gmv_daily_by_tenant | PG | 1.54 | 19.99 | 20,000,000 | 130 | 0.62¹ | 0.58 | yes |
| sla_breach_by_carrier_week | MySQL × PG | — | 39.85 | 40,000,000 | 255 | 64.25 | 1.25 | yes |
| top_sellers_by_city (window top-N) | PG × MySQL ×2 | — | 22.21 | 20,050,200 | 160 | 0.87 | 0.60 | yes |
| cohort_retention | PG ×2 | 19.62 | 15.35 | 21,000,000 | 901 | 4.74 | 2.48 | yes |
| rider_on_time | CH × MySQL | — | 14.95 | 26,771,597 | 209 | 4.89 | 1.00 | yes |
| hot_cold_daily_events | CH ∪ Parquet | — | 2.38 | 6,771,597² | 165 | 2.47 | 2.49 | yes |
| basket_mix | CH × PG | — | 1.33 | 2,880,027 | 338 | 24.82 | 1.23 | yes |
| cod_rto_by_segment | PG × MySQL × PG | — | 41.43 | 41,000,000 | 529 | 2.45¹ | 2.29 | yes |
| live_rider_pings_hourly | CH | 0.09 | 0.40 | 6,771,597 | 152 | 0.39 | 0.41 | yes |
| seller3_portal (restricted) | PG × MySQL | — | 39.52 | not reported³ | 194 | n/a⁴ | n/a | yes |

¹ The snapshots this query uses had already been refreshed by an earlier query, so no refresh was waited on.
² Federated scan counters cover only ClickHouse. The 153M-row Parquet file is read directly by DuckDB and is not counted.
³ Restricted reads turn off scan diagnostics by design ([row/column access](row-column-access.md)).
⁴ Snapshots need their own guarded policy, which was not configured.

**Results:**

- **Correctness:** 26 of 26 federated and native runs and 18 of 18 snapshot runs from the second pass were exact. All three isolation denials were refused:

  | Check | Result |
  | --- | --- |
  | `seller3` selects the hidden `customer_id` | `PERMISSION_DENIED` |
  | `seller3` queries ClickHouse, which is not granted | 403 |
  | Tenant b queries `pg.orders` | 403 |

- **Native:** PostgreSQL answered the per-tenant GMV in 1.5 s. Kelvo federated took 20 s for the same answer because it fetched all 20M rows.
- **Date predicates were not pushed down.** Timestamp range predicates on PostgreSQL and MySQL were not pushed to the source in these plans: a 3-month filter fetched the whole table. Integer `BETWEEN` on `order_id` was pushed: `basket_mix` fetched 2.9M rows instead of 70M and finished in 1.3 s. This single factor explains most of the federated latency.
- **Snapshots:** warm queries over 20M–50M-row copies took 0.4–2.5 s, 8–35x faster than federated. Their real cost is the refresh.
- **Source load:** sampled DB container CPU per federated run was about 8 core-seconds on PostgreSQL and 10 on MySQL for each full-table pull. Snapshot reads used about 0.

**Refresh durations.** These are full refreshes, measured as the first-read wait, or with `accelerate refresh` run directly:

| Dataset | Rows | Refresh s | Peak RSS |
| --- | ---: | ---: | ---: |
| orders_snap | 20M | 48.4 (cluster), 48.2 (CLI) | 3.2 GB |
| shipments_snap | 20M | about 63 | 2.9 GB sampled |
| order_items_snap | 50M | about 24 | 2.4 GB sampled |
| customers_snap | 1M | 1.5 (CLI) | 0.2 GB |

Spill was not measured. The query statistics expose no spill or temp-bytes counter, and the harness did not sample scratch directories.

## Dashboard fan-out

[Report](evidence/pipeline-fanout.json):

- **Dashboard:** 50 tiles over warm snapshots (tenant GMV, carrier SLA, hot event types, payment mix, top SKUs, zones, segments). Each tile is checked against its own DuckDB reference.
- **Client model:** each viewer opens 6 tiles at a time, alternates gateways, and retries an admission refusal (HTTP 429) up to 3 times with jittered backoff.
- **Capacity:** cluster capacity is 5 execution slots and 64 nonterminal handles for tenant a.

| Viewers | Queries | OK | Failed | 429s retried | OK queries/s | Tile p50 s | p95 s | p99 s | Dashboard load p50 s |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 50 | 50 | 0 | 0 | 3.81 | 1.46 | 2.45 | 2.55 | 13.1 |
| 5 | 250 | 250 | 0 | 0 | 3.50 | 9.07 | 11.52 | 11.89 | 71.3 |
| 20 | 1000 | 389 | 611 | 2,299 | 1.66 | 47.88 | 63.63 | 67.93 | 232.4 |
| 50 | 2500 | 661 | 1,839 | 6,400 | 1.59 | 63.01 | 92.18 | 97.29 | 413.3 |

**Answers:**

- **Correctness:** none of the 1,350 successful tile answers mismatched. Every failure was `RESOURCE_EXHAUSTED` after the retry budget ran out. No wrong answer, no hang and no crash.
- **Saturation:** throughput peaks at about 3.5–3.8 tiles/s, the CPU limit of 5 slots on 4 vCPU with a Landlock child process per query.
- **Overload:** at 20 or more viewers, goodput falls by more than half because refused submissions and retries compete with real work.
- **What viewers see:** one viewer waits 13 s for a full 50-tile page. Five viewers wait about 71 s.

There is no result cache, so identical tiles from different viewers each re-execute.

## Soak with fault injection

**Status: incomplete. No soak evidence file exists.** A 7,200 s soak started at 20:57Z on slot 10. The runner was reclaimed at about 00:37Z, before `docs/evidence/pipeline-soak.json` was copied back, and the replacement host had none of the loaded databases. No rerun was attempted: rebuilding the environment and running a shorter soak would not have fit the remaining time.

**What the soak was running.** The load was mixed:

- 4 dashboard viewers;
- 1 workflow runner (federated and snapshot);
- a 191,858-row result download every 5 s;
- a tenant-b probe every 2 s;
- scheduled refreshes every 5 and 10 minutes.

Faults were scheduled at 900, 1800, 2700/2760, 3600, 4500 and 5400 s.

**Live progress lines.** These are the only record. They were seen on the progress log over SSH and are not backed by a JSON report:

| Elapsed s | Queries | Failed | Fault event |
| ---: | ---: | ---: | --- |
| 900 | — | — | SIGKILL of a running query worker |
| 961 | 1,846 | 1 | — |
| 1800 | — | — | NATS broker `nats-1` SIGTERM and restart |
| 2162 | 4,011 | 2 | — |
| 2700 / 2760 | — | — | SIGKILL of node `a2`, restarted 60 s later on the first attempt |
| 3302 | 5,906 | 4 | — |
| 3601 | — | — | Slow client (1 KiB per 20 ms): stream ended after 254 s without Arrow EOS (`IncompleteRead`) and was correctly counted as a failure |
| 4985 | 8,765 | 4 | — |

The 4,500 s worker kill was scheduled but its log line was not observed. The 5,400 s broker restart and the final summary (answer mismatches, per-fault recovery times, RSS) were never seen.

**Partial reading.** About 83 minutes ran with 4 failures in 8,765 queries, through three injected faults and a node restart. The progress lines count failures but do not break out mismatches. The host load average was about 14 on 4 vCPU, and SSH over the tailnet relay dropped repeatedly. This is not a completed 2-hour soak and is not presented as evidence of recovery behavior.

## Where Kelvo breaks

| Symptom | Failure mode | Seen in |
| --- | --- | --- |
| A full single-part snapshot of a 20M-row table fails after about 39 s with `Acceleration exceeds Parquet row-group metadata limit`. In the cluster, every on-demand read returns `UNAVAILABLE: Accelerated dataset is missing, stale, or requires a refresh`, and node logs say only `Acceleration refresh: Query failed` | Row-group or footer metadata limit at 768 MiB | [Live run](evidence/pipeline-workflows-live.json): 16 snapshot failures. Fixed by `multipart` with 256 MiB parts |
| Refresh memory is not bounded by `memory_mb` | DuckDB materializes before Arrow delivery: peak RSS was about 3.2 GB for 20M rows at both 768 and 2560 MiB settings | CLI refresh with `/usr/bin/time` |
| A single-part refresh at `memory_mb: 2560` returned `Worker unavailable` at the 600 s client timeout | Cause not isolated; run aborted and not used | Operator log only |
| Federated date filters scan whole tables | Timestamp predicates are not pushed to PostgreSQL or MySQL; latency grows with table size, not with the window | 20–41 s federated rows above |
| 20 or more dashboard viewers get page failures | Admission (`RESOURCE_EXHAUSTED`), and retries waste the remaining capacity | Fan-out table |
| Restricted principal cost | Reads skip pushdown and scan diagnostics, so the source sends whole tables | `seller3_portal` 39.5 s |
| Minor diagnostics | Node errors are generic (`Worker catalog cannot be loaded`, `Query failed`). The bad value was `max_scan_rows` > 100,000,000, and the cause was found only by bisecting configuration | Setup |
| Config refused at startup: `refresh reservation exceeds node resources` | A refresh reserves scratch = `max_temp_mb` + `max_bytes` | Setup |

## What this proves

- On 20M–50M-row operational tables across PostgreSQL, MySQL, ClickHouse and Parquet, ten realistic workflows return exact answers through the cluster gateway in native, federated and snapshot modes, including CTEs, windows, top-N, cohorts, SLA rates and a hot/cold union.
- Tenant and principal isolation held under these runs. The restricted principal got only its tenant's rows, and its hidden columns, ungranted sources and the other tenant's sources were refused.
- Overload shows up as explicit admission refusals, not wrong answers.
- For repeated dashboards, snapshots are the right path: 1–2.5 s warm instead of 15–41 s federated, once the refresh is paid.
- Nothing about long-run stability: the soak did not complete (see above).

## What this does not prove

- Multi-host scaling, HA or cross-machine cluster behavior. Everything shared one 4-vCPU VM, and the slot-12 node was not joined.
- The 100M-order / 1B-event scale in the brief. Behavior at 5x is extrapolated.
- Real multi-schema tenancy, real customer data distributions, skew or late-arriving data. The data is synthetic and uniform.
- Spill behavior or memory bounds. RSS was sampled, and spill was not observable.
- Export jobs. The soak's "export" is a large Arrow result download (191,858 rows per request), not the export subsystem.
- Production capacity numbers. These are single runs on a shared, noisy VM with warm caches.

## Reproduce

```sh
python3 benchmarks/pipelines/generate.py --scale 20000000 --out ~/data/sf20m
python3 scripts/pipeline_fixture.py provision && python3 scripts/pipeline_fixture.py load --data ~/data/sf20m
python3 scripts/pipeline_fixture.py sources --data ~/data/sf20m
python3 scripts/pipeline_cluster.py provision
python3 scripts/pipeline_cluster.py start --binary bin/kelvo --source-manifest artifacts/pipeline-private/tenant-sources.json
python3 scripts/pipeline_load.py references --data ~/data/sf20m --report docs/evidence/pipeline-references.json
python3 scripts/pipeline_load.py validate --fixture <relational manifest.json> --report docs/evidence/pipeline-workflows-live.json
python3 scripts/pipeline_load.py fanout --report docs/evidence/pipeline-fanout.json
python3 scripts/pipeline_load.py soak --seconds 7200 --fixture <relational manifest.json> --report docs/evidence/pipeline-soak.json
```

Run under `umask 022` on a Linux VM with Docker 28, Landlock and the bridge build. Credentials stay in ignored `artifacts/*-private` files, and the catalogs refer only to `KELVO_SOURCE_PIPE_*` variable names.
