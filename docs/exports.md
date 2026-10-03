# Durable exports

Run a federated query once, then download its complete Arrow parts again without rerunning SQL. Exports are opt-in and retained on the worker's persistent disk. [Production gates](production-status.md) track acceptance.

## 1. Enable exports

Start with [principal keys](principal-access.md), tenant-bound workers and mTLS. Legacy tenant tokens cannot enable exports. Add identical `exports` settings inside each tenant policy on the initializer, gateways and workers:

```yaml
exports:
  authorization_version: catalog-v1
  max_jobs: 8
  queue_timeout: 30s
  default_ttl: 15m
  max_ttl: 1h
  limits:
    max_rows: 1000000
    max_encoded_bytes: 67108864
    max_decoded_bytes: 67108864
    max_part_bytes: 8388608
    max_part_decoded_bytes: 8388608
    max_parts: 16
    compression: none
```

Keep these limits within `policy.limits`. Set `compression: lz4_frame` to permit opt-in LZ4; compression never raises decoded limits. Each Arrow batch becomes one part, so choose batch/part limits together.

At the gateway's top level:

```yaml
exports:
  max_supervisors: 8
  max_downloads: 8
```

At each worker's top level, add explicit execution, download and persistent-storage budgets. This example assumes query limits of 512 MiB memory and 256 MiB scratch:

```yaml
scratch_directory: /var/lib/kelvo/scratch
resources:
  max_concurrent: 4
  memory_mb: 2048
  baseline_mb: 128
  overhead_mb: 96
  scratch_mb: 4096
  query_reserve_slots: 1
  query_reserve_memory_mb: 640
  query_reserve_scratch_mb: 256
  export:
    max_concurrent: 2
    memory_mb: 768
    scratch_mb: 1024
exports:
  directory: /var/lib/kelvo/exports
  max_entries: 16
  max_stored_bytes: 805306368
  max_concurrent: 1
  max_downloads: 2
  download_memory_mb: 96
  cleanup_interval: 30s
  cleanup_max_removals: 16
```

Prepare private persistent directories owned by the worker UID. Keep export, scratch, audit and acceleration roots disjoint. Run `cluster-init` with provisioning credentials before starting runtime processes.

Writer overhead and download memory must each cover at least `ceil((4 × (max_part_bytes + max_part_decoded_bytes) + 8 MiB) / MiB)`. These are reservations; enforce host/process limits separately. [Admission](workload-admission.md) · [Containment](process-containment.md)

## 2. Submit and download

Send an authenticated `POST /v1/exports`:

```json
{
  "query": {
    "mode": "federated",
    "sources": ["sales_fast"],
    "sql": "SELECT region, SUM(amount) AS revenue FROM sales_fast GROUP BY region"
  },
  "ttl_seconds": 900,
  "compression": "none"
}
```

The `201` response contains `id`, `state` and `expires_at`. Poll until `ready`; accepted work survives the submit connection closing.

| Action | Route |
| --- | --- |
| Check state | `GET /v1/exports/{id}` |
| Read part metadata | `GET /v1/exports/{id}/manifest` |
| Download one complete Arrow stream | `GET /v1/exports/{id}/parts/{index}` |
| Cancel work or withdraw a ready result | `POST /v1/exports/{id}/cancel` |

Every request needs a current key for the same principal. A replacement key may download an existing ready export; another principal cannot. Download cancellation leaves the export available until cancellation or expiry.

Verify the manifest's byte count/digest, complete HTTP framing and one complete Arrow stream with EOS per part. Parts are independent streams; do not concatenate them. HTTP byte ranges are rejected. A status code, completion header or partial body alone is not success.

## 3. Operate and recover

```text
queued → assigned → claimed → running → stored → ready
```

The worker reserves resources and disk before SQL. `stored` means local commit succeeded; only the initiating gateway can publish `ready` after checking current authority and the exact receipt. Assigned SQL is never replayed automatically.

- Revoking the initiating key stops an active fill. Gateway loss ends supervision when its short authority lease expires; another gateway does not adopt it.
- Ready results survive a worker restart only with the same worker ID and persistent root. They are unavailable while that worker is offline; storage is not replicated or moved automatically.
- Failed/cancelled jobs keep their broker slot until original expiry. Files keep their full storage reservation until safe cleanup; publication uncertainty stays unreadable and needs investigation.
- Existing bytes cannot be recalled. Revocation and withdrawal stop delivery and withhold successful Arrow EOS when detected before final completion.
- Catalog/grant changes require a coordinated policy cutover with a new `authorization_version`. Do not reuse it after changing source authorization.

Upgrade all participating binaries before enabling exports. Older configuration and audit readers reject the new feature. Drain fills/downloads and preserve retained data before a coordinated rollback; do not downgrade a live export root.

[Storage and crash contracts](export-storage.md) · [Cluster lifecycle](cluster.md) · [Audit](durable-audit.md)
