# Guarded local snapshots

Cluster principals can query an accelerated dataset through a row and column grant. One immutable snapshot is shared by readers; Kelvo filters Arrow batches before they enter DuckDB. SQL keeps the dataset name, such as `main.orders_fast`.

This path requires Linux, the pinned DuckDB bridge build, local acceleration storage, and a snapshot carrying its original Arrow schema. Refresh a legacy schema-less snapshot before granting restricted reads. Unrestricted reads of existing snapshots keep their previous behavior. Object snapshots and arbitrary native SQL remain unsupported under a row and column policy.

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

Guarded datasets may join other guarded local datasets and callback federation tables. Restricted queries cannot mix raw file aliases, legacy native attachments or object readers. Every selected local snapshot needs its own explicit dataset policy. Unsupported rules or a missing grant fail closed.

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

The trusted worker envelope binds the dataset, generation, ordered part sizes, row counts, digests, original Arrow schema and scan allowance. Public YAML cannot supply this envelope. The child revalidates it and checks private, worker-owned, single-link, non-writable regular files without following symlinks.

Guarded reads hash every part at relation discovery and again before each scan, including `LIMIT` queries. Each full scan also reads projected Parquet columns. This intentionally adds disk reads; it is not a zero-overhead path or a throughput claim. Measure workloads with their actual projection, part sizes, joins, cache state and storage. Integrity checks can consume the query deadline before its first result.

DuckDB receives only guarded callback relations. Raw snapshot paths are excluded from its file allowlist and no raw `read_parquet` view is registered. The Go reader receives exact OS file grants and holds a shared payload lease while reading. Existing parent generation leases remain held until query callbacks and the child have closed. Refreshes may publish a new generation while an existing query stays pinned to its original one.

Missing, corrupt, inconsistent or unsupported data fails the query. Raw resource exhaustion fails before delivering the offending batch. Errors during result delivery prevent successful final completion; consuming an incomplete stream is not a successful query.

## Roll out or roll back

Follow the [drained principal-policy cutover](principal-access.md). Deploy supporting binaries before enabling snapshot row and column policies or adding `scan` YAML. Older catalogs use strict unknown-field decoding and reject `scan`; older policy admission also rejects guarded snapshots. The [older-binary control](evidence/guarded-snapshot-rollout.json) verifies that adding only `scan` turns a successful literal-query catalog into an explicit configuration refusal. There is no mixed-version policy compatibility promise for this feature.

Before rollback, stop new admission, drain or cancel jobs, restore a compatible catalog and policy, and then restore the earlier binaries. Do not remove a restrictive grant merely to make an older binary accept it. No snapshot manifest migration or per-principal stored copy is introduced.

Native SQL policies, object-backed snapshot guards, public export/download integration, result-cache authorization and coordinated live policy updates remain tracked in [#28](https://github.com/SyneHQ/kelvo-go/issues/28).

[Reader and admission validation](evidence/guarded-snapshot-reader.json) records the focused development checks; production and transport capacity require their separate acceptance evidence.

[Integration evidence](evidence/guarded-snapshot-integration.json) records race/vet checks, real sandbox execution, generation cleanup, key revocation and EOS refusal, plus earlier failed trials and remaining gates.

[Fixture permission regression](evidence/guarded-snapshot-fixture-umask.json) retains the initial CI failure and verifies explicit private test directories under both `0022` and `0077` umasks. The production file-permission checks remain unchanged.
