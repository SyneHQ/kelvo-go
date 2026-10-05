# Verify protected snapshots

Check current and retained snapshots without querying the source database. Use a
[protected object namespace](protected-object-readers.md) and explicit read budget.
[Linux acceptance](evidence/protected-verification.json) passed on `c8e4199`, including
race checks and Verify/Inventory over the local TLS fixture.

## Enable

Add this to the dataset configuration, preserving its other limits:

```yaml
verification:
  max_bytes: 2147483648
limits:
  timeout: 2m
```

`max_bytes` covers one command: the current snapshot for `verify`, or the current
snapshot plus up to 16 retained generations for `inventory`. Values must be
positive and at most 32 TiB. There is no default; omission disables these two
protected operations. Refresh, query and status remain available.

The allowance counts root and descriptor bodies, payload checksums, repeated
Parquet footer reads and a conservative byte per range for its internal EOF
probe. Leave headroom beyond the stored payload size. Registry traffic, HTTP/TLS
overhead and transport buffering are outside this allowance; it is neither a
network quota nor a RAM limit.

One `limits.timeout` covers the entire operation. Budget or timeout changes
require the normal protected-runtime restart; they do not change snapshot content
identity or require a fresh namespace.

## Run

```sh
kelvo accelerate verify --config kelvo.yml --dataset trips
kelvo accelerate inventory --config kelvo.yml --dataset trips
```

Successful verification checks immutable versions, checksums, row counts and
schema. The CLI's `ready` field separately checks current catalog compatibility
and freshness. A verified snapshot can still be stale.

Inventory reads one root and only its retained catalog; it never lists the bucket.
`active` marks the current generation, `verified` records integrity, and
`catalog_truncated` warns that older entries have fallen out of the manifest.
An integrity failure can produce `verified: false`. Provider errors, cancellation,
exhausted budgets, lost leases and uncertain cleanup fail the whole operation.

## Ownership and limits

All selected generations are pinned before any descriptor or payload read.
Pins remain held until body reads and closure finish. Cleanup uncertainty returns
an error and retains capacity until ownership is resolved. The CLI emits its
result only after manager/runtime cleanup; cancellation during cleanup suppresses
output too.

Historical restore, migration backup, legacy cutover and remote deletion remain
separate work. Local TLS fixtures do not certify cloud IAM or production capacity.

[Protected readers](protected-object-readers.md) · [Snapshot recovery](snapshot-backup.md) · [Production status](production-status.md)
