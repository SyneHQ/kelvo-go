# Guarded snapshots

Cluster principals can query an accelerated dataset through a row and column grant. One immutable snapshot is shared by readers; Kelvo filters Arrow batches before they enter DuckDB. SQL keeps the dataset name, such as `main.orders_fast`.

This path requires Linux, the pinned DuckDB bridge build, and a snapshot carrying its original Arrow schema. It supports local storage and [object-backed generations](object-storage.md). Refresh a legacy schema-less snapshot before granting restricted reads. Arbitrary native SQL remains unsupported under a row and column policy.

## Grant the dataset relation

Use the dataset ID for both the source and its sole table policy. Grants for the database used to refresh a dataset do not grant access to its snapshot.

```yaml
federated_sources: [orders_fast]
row_column_policy:
  sources:
    orders_fast:
      tables:
        orders_fast:
          columns: [order_id, amount]
          rows:
            kind: comparison
            column: account_id
            type: int64
            op: eq
            value: "42"
```

With this grant, `SELECT SUM(amount) FROM main.orders_fast` sees only account 42. `account_id` is available to the policy evaluator and absent from the SQL schema. Schema and field metadata are removed. Use the same versioned [principal policy](principal-access.md) on gateways and workers.

Guarded datasets may join other guarded local/object datasets and callback federation tables. Every selected snapshot needs its own dataset policy. Raw file aliases, legacy native attachments and unguarded object readers are rejected.

## Separate scanning from returned results

Set optional `scan` limits on the acceleration dataset in its catalog:

```yaml
acceleration:
  # Keep the existing tenant_id, directory and refresh configuration.
  datasets:
    - id: orders_fast
      # Keep the existing query, refresh_interval, max_age and authorization_version.
      scan:
        max_rows: 5000000
        max_bytes: 1073741824
```

These limits apply to guarded reads; unrestricted snapshot reads retain their existing DuckDB and query limits. They are cumulative raw scan limits per dataset per query, before filtering. Repeated scans and self-joins share the allowance. Bytes count decoded Arrow buffers for requested and policy-only columns. Unprojected columns are not decoded. Snapshot schema discovery and integrity hashing are separate I/O work.

Defaults are **1,000,000 rows and 256 MiB**. Each field defaults independently. Supported bounds are 1–100,000,000 rows and 1 KiB–1 TiB. Changing this allowance does not change stored data identity or require a refresh.

For example, a query output limit of 10 rows can return `COUNT(*)` while this scan allowance processes up to five million raw rows. The query's output bytes, timeout, memory, scratch, admission and process-containment limits still apply. Increasing scan allowances alone does not reserve more memory or storage.

The reader uses sequential Parquet column readers and batches of at most 256 rows. It also checks bounded footer and row-group metadata. Logical decoder-owned Arrow buffers share a cap of half the query memory setting, at most 64 MiB, across snapshot readers. This counter excludes filter masks, selected result buffers, allocator padding and other Go/native memory. Retain worker process containment; the counter is not a process RSS guarantee. DuckDB can still materialize an intermediate join or the final query result.

## Integrity, lifecycle and cost

The trusted worker envelope binds the dataset, generation, ordered part sizes, row counts, digests, original Arrow schema and scan allowance. Public YAML cannot supply this envelope. Local reads require private, worker-owned, single-link, non-writable files without symlinks. Object reads bind each part to its exact parent-minted range capability.

Guarded reads hash every part at relation discovery and again before each scan, including `LIMIT` queries. Each scan also reads projected Parquet columns. This adds disk or network I/O and can consume the query deadline before its first result. Measure actual projections, part sizes, joins and storage before sizing a deployment.

DuckDB receives only guarded callback relations; raw snapshot paths and capability URLs are excluded from its file allowlist. Local readers hold a shared payload lease. Parent generation leases remain held until callbacks and the child close, so refresh can publish while an existing query stays pinned.

Object guards use Go range reads and need no `httpfs` extension. Only the parent holds cloud credentials and checks provider versions. The child accepts exact, uncompressed `206` ranges with matching size and digest identity; redirects, proxies and full-download fallback are disabled. Its 64 KiB cache and 32 KiB hashing buffer share the decoder allocation budget. Cancellation closes active requests.

Full hashes plus pinned immutable versions protect these reads; individual ranges are not independently authenticated against a provider that changes bytes without changing its version. Remote pruning still deletes nothing: process-local reader leases do not make distributed garbage collection safe.

Missing, corrupt, inconsistent or unsupported data fails the query. Raw resource exhaustion fails before delivering the offending batch. Errors during result delivery prevent successful final completion; consuming an incomplete stream is not a successful query.

## Roll out or roll back

Follow the [drained principal-policy cutover](principal-access.md). Deploy supporting binaries before enabling snapshot row and column policies or adding `scan` YAML. Older catalogs use strict unknown-field decoding and reject `scan`; older policy admission also rejects guarded snapshots. The [older-binary control](evidence/guarded-snapshot-rollout.json) verifies that adding only `scan` turns a successful literal-query catalog into an explicit configuration refusal. There is no mixed-version policy compatibility promise for this feature.

Before rollback, stop new admission, drain or cancel jobs, restore a compatible catalog and policy, and then restore the earlier binaries. Do not remove a restrictive grant merely to make an older binary accept it. No snapshot manifest migration or per-principal stored copy is introduced.

Native SQL policies, protected-object export acceptance, result-cache authorization and coordinated live policy updates remain tracked in [#28](https://github.com/SYNEHQ/kelvo-go/issues/28). Public [federated exports](exports.md) are implemented; their complete protected-storage lifecycle is a separate gate.

[Authenticated protected queries](protected-query-acceptance.md) passed the complete gateway, NATS, node and contained-worker path. The gate checks two tenants, differently restricted principals, real multipart CTE joins, four revocation/framing combinations and bounded cleanup. It keeps the HTTP-response and client `Body.Close` checks separate.

[Reader and admission validation](evidence/guarded-snapshot-reader.json) records the focused development checks; production and transport capacity require their separate acceptance evidence.

[Integration evidence](evidence/guarded-snapshot-integration.json) records race/vet checks, real sandbox execution, generation cleanup, key revocation and EOS refusal, plus earlier failed trials and remaining gates.

[Fixture permission regression](evidence/guarded-snapshot-fixture-umask.json) retains the initial CI failure and verifies explicit private test directories under both `0022` and `0077` umasks. The production file-permission checks remain unchanged.

[Object integration](evidence/guarded-object-integration.json) covers full ordinary/bridge suites, race checks, mixed joins, real workers, principal revocation and cleanup. It retains optional skips and links the [exact source](evidence/guarded-object-source.json). [Boundary trials](evidence/guarded-object-boundaries.json), [older-worker refusal](evidence/guarded-object-rollback.json) and [response identity checks](evidence/object-response-identity.json) preserve earlier failures and component evidence. Live-provider and capacity acceptance remain separate.
