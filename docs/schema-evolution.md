# Optional schema evolution

Permit selected forward schema changes during full refresh. Strict matching remains the default, and every batch/part within a generation must always share one exact schema.

## Configure

1. Register the source, such as `warehouse`, under `sources`.
2. Enable either or both flags in the trusted dataset catalog:

```yaml
acceleration:
  tenant_id: tenant-a
  directory: /var/lib/kelvo/acceleration
  datasets:
    - id: orders_fast
      query:
        mode: native
        connection_id: warehouse
        sql: SELECT * FROM analytics.orders
      authorization_version: orders-readers-v1
      refresh_interval: 15m
      max_age: 1h
      schema_evolution:
        add_nullable_columns: true
        safe_widening: true
```

3. Coordinate the new catalog across workers/schedulers and complete a refresh before expecting readiness.

Omit the block, use `{}`, or set both flags false for strict matching. Unknown fields/invalid values fail; callers cannot set policy in query requests.

Policy compares the prior committed schema with a complete new result. It does not cast, fill missing fields, edit SQL, rewrite history or merge schemas. `SELECT *` with nullable additions can expose future source columns: use explicit projections when that is undesirable, and review source/dataset grants.

## Permitted changes

| Policy | Forward change |
| --- | --- |
| Strict | None: order, names, types, nullability and metadata must match |
| `add_nullable_columns` | Append nullable fields after all existing fields |
| `safe_widening` | Widen within signed or unsigned integer families; Float32 → Float64; increase Decimal128 precision to at most 38 with unchanged scale |
| Both | Combine the two changes; no arbitrary replacement |

Int32 → Int64 and Decimal128(12,2) → (18,2) qualify. Signedness changes, integer/float conversion and decimal scale changes do not.

Existing nullability, names, order, timestamp units/zones, nested types and metadata cannot change. Removals or inserted middle columns fail. New fields must meet supported-type, count, metadata, value and resource limits. Widening accepts a fresh schema; it does not convert historical data.

## Publication and failure

The writer lock/lease protects the prior-schema read and directional comparison. Every new batch/part then matches exactly; hashes, footers, row counts and atomic publication remain required for local/remote and single/multipart layouts.

Incompatibility returns `SCHEMA_MISMATCH`; extraction, conversion or resource failure also preserves current data. There is no live-source fallback or partial publication. Retained data can still be unavailable because it is stale or its authorization/configuration differs.

## Configuration identity and rollout

Absent/all-false flags preserve the historical strict fingerprint. Nondefault policies include both flags and internal rules identity `schema_evolution_version: 1`; that version is not a YAML option. Future rule changes require a new identity.

Changing any effective flag, including returning to strict, makes the old generation unavailable under the new catalog until refresh publishes its fingerprint. A fresh generation does not suppress that refresh; readiness may stay false. The prior schema remains the comparison baseline, so policy changes do not authorize incompatible redesigns or reset freshness.

Continue updating `authorization_version` for grant/data-scope changes. Cluster jobs and durable failures use the same fingerprint:

1. Drain old-policy workers before dispatching new jobs; they correctly reject mismatched fingerprints.
2. Roll out matching catalogs to workers and schedulers.
3. Review/reset failed refresh state through operator controls when needed. Policy changes do not bypass admission or retry rules.

## Restore remains strict

Current and target generations must match the active fingerprint, pass integrity checks and have identical schemas. Evolution does not authorize backward narrowing, column removal or restoration from another policy identity.

Restore preserves the original timestamp and may select stale data; it never renews `max_age`. Use a planned migration or new authorized refresh for incompatible changes.

## Recurring conformance gates

[CI](../.github/workflows/ci.yml) includes local multipart/schema-evolution CLI acceptance and focused tagged race checks, alongside ordinary/bridge suites. Expensive over-4-GiB gates and fixture CA installation are excluded from pull-request checks; run them explicitly on a suitable dedicated host.

## Development validation

[Eight-case process acceptance](evidence/schema-evolution-acceptance.json) covers exact values/NULLs, 100,000 rows across 28 parts, policy fingerprints, rejected narrowing and strict restore. Schema mismatch remains a permanent sanitized failure through durable retries, restart and operator reset.

The evidence covers development correctness, not live provider acceptance, a cluster soak, throughput or process-memory sizing.
