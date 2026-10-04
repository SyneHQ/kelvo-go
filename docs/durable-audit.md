# Durable local audit

Optional Linux journals retain bounded receipts for cluster operations. Each records effective tenant, service and principal authority, a generated ID, fixed action/outcome/category and timestamps. SQL, rows, parameters, credentials, headers, source names and raw errors are excluded. Receipt IDs identify local operations; they are not cross-service job IDs.

## Configuration and capacity

Give each gateway or worker replica a stable, unique service ID and its own private directory:

```yaml
audit:
  service_id: gateway-one
  directory: /var/lib/kelvo/audit/gateway-one
  max_entries: 65536
  max_pending: 64
  retention: 1h
  write_timeout: 2s
```

Omit `audit` to disable it. All limits are explicit. Configuration loading and `cluster-init` do not open journals. Audit, export and managed scratch directories must be disjoint; keep audit storage separate from query data.

| Setting | Allowed values |
| --- | --- |
| `service_id` | 1–64 lowercase letters, digits or hyphens, beginning with a letter |
| `directory` | Clean absolute path, privately owned by the service UID |
| `max_entries` | 1–1,048,576 retained or active receipts |
| `max_pending` | 1–4,096 queued starts and active receipts combined; no larger than `max_entries` |
| `retention` | Greater than zero through 30 days, from terminal time |
| `write_timeout` | Greater than zero through 30 seconds, including queue and I/O acknowledgement |

Open preallocates **16 KiB + 2 KiB × `max_entries`**, including every terminal frame. Linux `fallocate` must succeed. The example reserves approximately 128 MiB; the maximum approximately 2 GiB. The memory index also scales with capacity.

Size by **receipts/second × retention seconds + active headroom**, not rows. A query can generate several receipts; startup probes and authentication denials also consume capacity. A 65,536-entry journal holds fewer than 19 receipts/second for one hour. Nonexpired completed entries are never overwritten; active receipts never expire. Reopening requires identical limits, service identity and tenant allow-list.

## Recorded boundaries

| Operation | Required order |
| --- | --- |
| Gateway submission/cancellation | Durable start, state mutation, durable outcome, acknowledgement |
| Gateway results | Durable start, local relay, durable outcome, successful job transition, Arrow EOS |
| Worker execution | Durable start, execution and validated IPC completion, durable outcome, result-ready transition, Arrow EOS |
| Direct worker cancellation | Durable start for the verified gateway service role, cancellation, durable outcome, acknowledgement |
| Scheduled refresh | Durable start, extraction/publication/pruning, durable outcome |
| Export submit/cancel/execution/results | Durable start and outcome around the local operation; Ready publication and download EOS require their own final authority checks |
| Authentication denial in a handler | Attempt fixed denial record; still deny when audit storage is unavailable |

Bindings come from authenticated context and provisioned policy. Tenant-only API keys remain `unknown` principals with an empty policy digest. Startup `SELECT 1` probes and scheduled refreshes use the provisioned service identity. Direct worker cancellation identifies the authenticated `gateway` role; its TLS identity cannot distinguish gateway replicas or identify the initiating user.

A successful receipt observes local execution or relay. The later job-state CAS or client delivery can still fail. An external change or immutable publication can also succeed before the terminal audit write fails. These receipts assert neither rollback nor cross-system atomicity.

TLS handshake rejection, pre-authentication HTTP capacity rejection, query-status/diagnostic reads, standalone CLI queries and unsupervised `serve` execution are outside this contract. [Tracing](tracing.md) and [execution history](operations.md#optional-execution-history) remain separate telemetry.

Export-status reads are also outside the journal. Older audit readers reject export action kinds; upgrade shared readers before enabling exports.

## Failure behavior

| Condition | Behavior |
| --- | --- |
| Full retention or pending capacity | Reject new starts; preserve terminal space for admitted work |
| Runtime write/acknowledgement uncertainty | Block new protected work, fail readiness and withhold successful acknowledgements |
| Blocked syscall or close timeout | Keep the same worker, descriptor, writer lock and slot; no replacement writers or retry goroutines |
| Valid start with an all-zero terminal | Recover to `unknown` / `interrupted` |
| Torn/corrupt nonzero frame, generation or checksum | Fail closed; preserve the journal for investigation |
| Valid terminal whose acknowledgement was lost | Preserve its local outcome; it does not prove caller receipt |
| Graceful shutdown with unfinished valid receipts | Complete them as `unknown` / `interrupted` |
| Refresh audit failure | Permanent refresh configuration failure; operator review/reset required |

Terminal cleanup uses a fresh bounded context after query cancellation. Identical completions share a result; conflicting explicit outcomes are rejected. A late successful `fsync` may leave a local success record after the caller received uncertainty.

Runtime timeouts bound the caller's wait, not a kernel syscall. Startup traversal, allocation and recovery are synchronous. Apply service startup timeouts and filesystem monitoring. Local durability depends on storage honoring `fsync`; host/disk loss, malicious effective UID/root, independent database transactions and client receipt are outside the guarantee. There is no replication, archival or regulatory certification claim.

## Operator recovery and reads

1. Drain and stop the writer. Retain the old journal according to the deployment's retention policy.
2. Read bounded pages under the service UID:

```sh
kelvo audit read --directory /var/lib/kelvo/audit/gateway-one --cursor 0 --limit 100
```

3. Follow `next` until `done`. A page scans at most 256 physical slots; empty pages are possible and slots are not chronological. Keep the service stopped throughout pagination.
4. For corruption or changed limits/ownership, investigate and provision a replacement journal. Do not edit its header or automatically delete it.
5. For a stopped refresh, verify the current immutable generation and broker status, repair audit storage, then explicitly reset the refresh job. An already-published generation remains published. Audit, snapshot publication and broker acknowledgement have separate crash boundaries.

Reads refuse an active writer and have no tenant HTTP endpoint. Files must be private, regular, singly linked and owned by the service UID. Paths do not follow symlinks; writable ancestry is rejected except root-owned sticky temporary directories. Checksums detect damage, not deliberate rewriting by a trusted UID.

## Measured cost and validation

On the dedicated Azure x86-64 test VM (4 visible CPUs, Xeon Platinum 8573C, Linux 6.12.95, Go 1.26.8; `GOMAXPROCS=2`), sequential start plus terminal receipts took **12.44–12.76 ms** across three 1,000-operation runs: two `fsync` calls, approximately 10.2 KiB allocated and 58 allocations per receipt. That is about 78–80 sequential receipts/second on that disk, not query or row throughput. Other validation was active on the host.

| Fresh journal | Allocated disk blocks | Additional live Go heap after GC |
| --- | --- | --- |
| 4,096 entries | 8,404,992 bytes | 296,216–303,608 bytes |
| 65,536 entries | 134,234,112 bytes | 4,719,792–4,724,520 bytes |

Opening measurements used four pending reservations and two tenants. They exclude mature retained-entry indexes, queries, surrounding runtime and filesystem cache. RSS deltas included negative values from GC, allocator reuse and lazy pages; they are not a memory ceiling.

[Journal evidence](evidence/durable-audit-journal.json) includes repeated race checks, bounded blocked-I/O/crash fixtures and real `ENOSPC` on disposable 1 MiB tmpfs. [Runtime evidence](evidence/durable-audit-runtime.json) covers gateway/worker/refresh/CLI boundaries and records skipped fixtures. [Service acceptance](evidence/durable-audit-service.json) records two isolated runs through real gateway, worker, NATS and scheduled refresh processes: trusted receipts, restart persistence, damaged-journal rejection and explicit replacement recovery. Its two-row ClickHouse endpoint is an HTTP/Arrow fixture. These checks do not measure throughput, simulate power loss or qualify a storage provider; commands, hashes, resource bounds and failed attempts accompany the evidence.
