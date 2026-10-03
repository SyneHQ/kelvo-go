# Multipart acceleration

Store a complete dataset generation as ordered immutable Parquet parts. DuckDB reads their explicit list through the existing dataset alias while one active encoder bounds per-part metadata growth.

## Configure

1. Upgrade all readers and writers under the [compatibility rules](#reader-upgrade-and-compatibility).
2. Add `multipart` to the dataset:

```yaml
sources:
  - id: warehouse
    type: clickhouse
    url_env: KELVO_SOURCE_WAREHOUSE_URL
acceleration:
  tenant_id: tenant-a
  directory: /var/lib/kelvo/acceleration
  datasets:
    - id: orders_fast
      query:
        mode: native
        connection_id: warehouse
        sql: SELECT id, region, amount FROM analytics.orders
      authorization_version: orders-readers-v1
      refresh_interval: 15m
      max_age: 1h
      multipart:
        max_part_bytes: 268435456
        max_parts: 64
      limits:
        max_rows: 50000000
        max_bytes: 8589934592
        timeout: 30m
        memory_mb: 512
        threads: 2
        max_temp_mb: 2048
```

3. Refresh, verify and query with the existing commands:

```sh
bin/kelvo accelerate refresh --config kelvo.yml --dataset orders_fast
bin/kelvo accelerate verify --config kelvo.yml --dataset orders_fast
bin/kelvo query --config kelvo.yml --sources orders_fast \
  --sql 'SELECT region, SUM(amount) FROM orders_fast GROUP BY region' \
  --out revenue.arrow
```

The example authorizes work; it is not a sizing recommendation. Omit `multipart` for single-file writes. Parts use private local storage unless `acceleration.object_storage` is configured; then `directory` holds local staging. See [object credentials](object-storage.md) and [refresh/freshness policy](acceleration.md).

## Limits and rotation

| Setting or bound | Behavior |
| --- | --- |
| `max_part_bytes` | Hard encoded limit per part, including its header and footer; 1 MiB–4 GiB, and no greater than dataset `limits.max_bytes`. |
| `max_parts` | Configured ceiling of 2–256; a small or empty result may still produce one part. |
| `limits.max_bytes` | Aggregate encoded snapshot limit across all parts; existing source/result transfer limits still apply independently. The general configuration ceiling is 1 TiB. |
| `limits.max_rows` | Aggregate rows across the entire refresh, up to the existing 100 million configuration ceiling. |
| Rotation target | Estimated uncompressed data reaches half `max_part_bytes`, or a part reaches 1,048,576 rows or its metadata group budget. |
| Row groups and pages | At most 8,192 rows per group, with a memory-dependent uncompressed budget capped at 8 MiB; the encoder targets 64 KiB data pages. Page size is a target, not a separate hard part limit. |
| Metadata | At most 4,096 columns, 1 MiB serialized Arrow schema, 64 MiB Parquet footer input, 64 KiB local/root manifest and 2 MiB remote multipart descriptor. Row-group metadata also has a memory-dependent count budget. |

Rotation at half the byte limit leaves estimated encoding headroom, not a guaranteed fit. Wide rows, compression and footers can still hit hard limits; refresh fails rather than truncates or partially publishes. Oversized uncompressed rows fail. Snappy remains the codec.

`max_parts * max_part_bytes` is an upper bound, not usable-capacity assurance: row/metadata targets may exhaust parts sooner. Total-byte increases do not override other limits.

## Local publication and recovery

One writer owns extraction, finalized footers and the entire generation. Its manifest binds ordered identities, rows, encoded sizes, SHA-256 hashes, totals and schema fingerprint. Every part matches exactly; cross-generation changes follow the configured [schema policy](schema-evolution.md).

Only a complete generation atomically replaces current metadata. Pre-publication errors preserve the previous generation; post-swap sync errors may mean publication happened with uncertain durability.

Queries acquire all selected part leases under metadata protection and retain them through execution, preventing mixed generations within a dataset. Different aliases still acquire independently and share no distributed snapshot.

Acquisition checks metadata, authorization, freshness, permissions and sizes; `verify` checks payload integrity. Verified restore preserves original freshness. Retention normally keeps current, one previous and leased generations. Staging/replacements/old readers can coexist, so `max_bytes` is not a filesystem quota. Recovery/pruning require private trusted storage and exclusive writer coordination; malformed metadata prevents deletion.

## Remote publication and recovery

1. Finalize one local part, verify its rows/schema, hash it and upload under an absent-key condition.
2. Confirm exact version, size and SHA-256 metadata with HEAD before removing staging and starting the next part.
3. Upload/confirm an immutable YAML descriptor binding all ordered parts, totals, identity and schema.
4. Publish its versioned reference/digest through one conditional root update under the renewable writer lease.

Uploads are sequential. Keys derive from configured prefix/tenant/dataset/generation/index; descriptors cannot redirect to arbitrary keys. Failed parts/descriptors expose no partial generation. Ambiguous root writes are reconciled against exact metadata; unresolved outcomes preserve possibly published data.

Acquisition verifies the descriptor's full digest/version/identity/totals and uses at most four concurrent part HEADs. `verify` and restore additionally stream hashes and validate footer rows/schemas. Ordinary reads do not download every payload; restore retains original age.

Root metadata is capped at 64 KiB, keeping current plus at most 16 historical generations, fewer if needed. Descriptors allow 2 MiB, leaves 4 GiB, 256 parts and 1 TiB aggregate. These are format ceilings, not capacity measurements.

Remote pruning never lists/deletes objects. Orphans, descriptors and retired parts may remain indefinitely; metadata eviction frees no payload space. Safety relies on immutable versions and no automatic deletion, not distributed reader leases. Operator cleanup must account for all readers/retained generations.

## Reader upgrade and compatibility

Local single-file manifests use v1; multipart publishes v2. Upgrade every reader/refresher/inventory/restore/prune process before enabling it. New readers accept v1, but old binaries reject v2.

Layout is excluded from the authorization fingerprint, so enabling multipart preserves an authorized old snapshot until replacement is due and succeeds. Removing the setting changes future writes only; retained multipart history remains. **Disabling multipart is not a binary downgrade procedure.**

Remote versions are independent: readers accept v2/v3/v4, and **every writer action emits v4**, including the first lease claim/renewal with multipart disabled or a later failed refresh. V4 supports single objects or descriptors and retains bounded history.

Upgrade all remote readers/writers before any new writer action. After the first lease claim, old-binary rollback needs a separately validated metadata/data recovery plan. Neither disabling multipart nor editing a version number reverses the migration.

## Worker resources and boundaries

The parent alone supplies local `parquet_paths`: unique absolute non-glob paths without conflicting settings. DuckDB gets an explicit list, and Landlock grants exact files, never directories.

Each local part lease consumes one parent descriptor; DuckDB may open more. Across a query, grants are limited to 1,024 files, source path arguments to 512 KiB, and worker JSON input to 2 MiB. Four 256-part datasets consume all file grants. Budget descriptors for runtime/database/coordination sockets too.

Remote `object_ranges` are process-only loopback capabilities bound to query, dataset and part index: at most 256 per source and 1,024 per query. Exact allowlists exclude legacy PostgreSQL/MySQL extension joins that need broader external access; Go adapters preserve the restrictions.

One query bridge shares four upstream requests, 32 MiB maximum ranges and 32 KiB buffers per active request. Excess requests fail rather than queue per part. Exact versions, sizes, digests and response lengths are checked; redirects/full GETs fail. Credentials, keys and provider URLs stay in the parent, and the bridge ends with the query.

Encoder bounds do not cap source memory or total RSS. DuckDB still materializes execution before Arrow delivery; account for input batches, readers, native allocations, caches and verification. Measure RSS, descriptors, disk and latency under load.

Incremental refresh, partition replacement, CDC, remote garbage collection and distributed execution of one query are not provided by file splitting.

## Development validation

| Evidence | Scope |
| --- | --- |
| [Local process acceptance](evidence/multipart-acceptance.json) | Exact 100,000 rows, sandboxed join, formats/restore, limits and corruption |
| [Local large gate](evidence/multipart-large-dataset.json) | 4,456,448 synthetic rows; 138 parts; 4,600,321,235 bytes; full hashes and DuckDB aggregate |
| [Remote acceptance](evidence/object-multipart-acceptance.json) | 20 publication/read/restore/failure checks across four provider-shaped TLS fixtures |
| [Legacy regression](evidence/object-multipart-legacy-acceptance.json) | 36 single-file object checks |
| [Remote large gate](evidence/object-multipart-large-dataset.json) | Same over-4-GiB dataset through immutable fixtures and guarded HTTPFS ranges |

Recorded combined-process peaks were about 216 MiB locally and 457 MiB remotely. They include synthetic producers/verification, exclude filesystem cache and do not measure native ingestion, tenant concurrency or cloud/WAN capacity. Consult the evidence before citing results.

Run expensive gates only on a provisioned test machine with over 4 GiB free for each fixture:

```sh
KELVO_TEST_MULTIPART_LARGE=1 go test -tags duckdb_arrow \
  ./internal/acceleration -run TestMultipartDatasetLargerThanFourGiB \
  -count=1 -timeout=30m -v
```

For the remote layout, also provision matching signed HTTPFS:

```sh
KELVO_TEST_OBJECT_MULTIPART_LARGE=1 \
KELVO_OBJECT_EXTENSION_DIRECTORY=/path/to/approved/extensions \
go test -tags duckdb_arrow ./internal/acceleration \
  -run TestObjectMultipartDatasetLargerThanFourGiB -count=1 -timeout=30m -v
```
