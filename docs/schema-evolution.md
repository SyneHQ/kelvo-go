# Optional schema evolution

Acceleration uses strict cross-generation schema matching by default. An operator
can explicitly permit selected forward changes for a dataset without relaxing
integrity checks within a generation. The validation record below covers its
development correctness gates; it is not a production capacity claim.

## Configure

Add either or both independent flags to a dataset in the trusted catalog:

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

This fragment assumes `warehouse` is separately registered under `sources`.
Omit `schema_evolution`, use `{}`, or set both flags to `false` for strict matching.
Unknown fields and invalid flag values fail catalog loading. Query callers cannot
supply an evolution policy in a query request.

The policy applies only when comparing the prior committed Arrow schema with a
complete new refresh. It does not edit SQL, cast values, fill missing columns,
rewrite historical snapshots or merge different schemas into one generation.
Prefer explicit source projections when newly added source columns should not
be exposed automatically. Opting into nullable additions can expose future columns
selected by `SELECT *`; source permissions and dataset authorization remain the
operator's responsibility.

## Permitted changes

| Policy | Allowed forward change |
| --- | --- |
| Strict, the default | No change to field order, names, types, nullability or schema/field metadata. |
| `add_nullable_columns` | Append new nullable fields after the existing ordered fields. Existing fields retain their original contract unless a separate widening flag permits a type change. |
| `safe_widening` | Increase signed integer width within the signed family, or unsigned integer width within the unsigned family; widen Float32 to Float64; increase Decimal128 precision with identical scale and a maximum precision of 38. |
| Both flags | Combine the two changes above. Neither flag enables arbitrary schema replacement. |

For example, Int32 to Int64 is permitted with `safe_widening`; Int32 to UInt64,
Int64 to Float64 and Float64 to Int64 are not. Decimal128(12,2) to Decimal128(18,2)
is permitted, while Decimal128(12,2) to Decimal128(18,3) is not.

Existing field nullability cannot change in either direction. Renames, removals,
reordering, insertion between existing fields, signedness changes, timestamp unit
or timezone changes, nested-type changes and metadata changes remain rejected.
New fields must still satisfy the existing supported Arrow-type, column-count,
metadata, value and resource limits. A nullable declaration does not authorize an
unsupported Parquet type. Widening is a schema compatibility decision about the
fresh source result, not a promise that Kelvo converts old values or historical
snapshots into the new type.

## Publication and failure

The previous schema is read under the dataset writer lock or remote writer lease.
The refresh may proceed only if the directional comparison permits the incoming
schema. Every batch and every part in that refresh must then match the same exact
schema. Per-generation schema hashes, footer checks, row counts, checksums and
atomic publication remain unchanged for local and remote, single-file and
multipart snapshots.

An incompatible change returns `SCHEMA_MISMATCH`; a failed extraction, conversion
or resource check also leaves the current generation intact. There is no fallback
to the live source and no partial publication. A retained snapshot can still become
unavailable because it is stale or its authorization/configuration no longer
matches the active catalog.

## Configuration identity and rollout

An absent policy and explicitly all-false flags keep the historical strict
fingerprint unchanged. A nondefault policy is included in the dataset's
configuration/authorization fingerprint. The two flags and their combinations
have distinct identities. Nondefault fingerprints also include the internal
`schema_evolution_version: 1` rules identity. It is not a YAML setting: changing
which transitions a flag permits requires deliberately updating that identity.
Strict fingerprints omit it to preserve compatibility with existing snapshots.
Changing either effective flag, including tightening a
policy back to strict, makes the old generation unavailable under the new catalog
until a complete refresh publishes the new fingerprint. Required-dataset readiness
can therefore remain false during the transition.

A fresh old generation does not suppress this refresh: scheduling also compares
the fingerprint. The prior generation's schema remains the baseline for the
compatibility check. Policy changes do not erase that baseline, reset freshness,
allow incompatible redesigns or waive source permissions. Continue to change
`authorization_version` when source grants or permitted data change; policy flags
do not substitute for it.

Cluster refresh jobs and their durable failure state are keyed by the same
fingerprint. Coordinate the catalog change across all workers and schedulers.
Drain workers with the old policy before dispatching jobs for the new one; an old
worker correctly rejects a differently fingerprinted job and can record a
configuration failure. Review/reset failed refresh state through the existing
operator controls after the coordinated rollout. A new policy starts a different
configuration identity, not permission to bypass retry or admission controls.

## Restore remains strict

Evolution does not relax restore. Both the current and target generation must
match the active catalog fingerprint, pass integrity verification and have the
same exact schema. A forward-compatible refresh does not make a backward schema
change safe: restoring a narrower type or removing newly added columns can break
current queries. Even unchanged schemas from an older policy can be rejected when
their configuration fingerprints differ.

Restore retains the original generation timestamp and may restore already stale
data. It never renews `max_age`. There is no implicit cast, authorization override
or schema-policy override in pointer restore. Use a separately planned dataset
migration or a new authorized refresh for incompatible redesigns.

## Recurring conformance gates

The CI workflow now includes local multipart and schema-evolution CLI acceptance
after building the binary and Linux sandbox, plus focused tagged race checks for
schema, descriptor, range and multipart engine paths. The ordinary and pinned
bridge suites continue to cover the underlying unit and integration tests.
These commands passed on the dedicated Azure test VM at runtime milestone
`6f748c5`; the updated GitHub workflow has not been pushed or run remotely.

Pull-request checks do not enable the expensive over-4-GiB gates or install
fixture certificate authorities. Those require explicit opt-in on a suitable
dedicated test machine. TLS protocol fixtures use generated local identities;
passing them does not establish acceptance against real cloud-provider accounts.


## Development validation

Runtime milestone `6f748c5` passed the full ordinary and pinned DuckDB bridge
suites, focused race checks across catalog/acceleration/cluster/engine, `go vet`,
a binary build and bridge `cgocheck2`. Publication tests cover local and remote
single-file and multipart paths, pinned generations, rejected mixed schemas,
policy fingerprint transitions and exact-schema restore restrictions.

The [eight-case real-process acceptance](evidence/schema-evolution-acceptance.json)
uses sandboxed CLI refresh/query processes and independently generated Parquet
sources. It checks NULLs, integer values beyond Int32 after widening, exact Arrow
values and original Parquet schemas. The combined-policy case verifies all
100,000 rows across 28 parts. Narrowing, missing independent flags, schema-changing
restore and strict-policy fallback are exercised against complete generations.
The existing six-case local multipart acceptance also passed unchanged.

This validation exposed and fixed an older error-classification gap: the actual
schema-mismatch sentinel was not a typed query error, so durable refresh retries
classified it as unknown. Wrapped encoder errors now retain `SCHEMA_MISMATCH`;
regression checks confirm permanent suppression, no retry scheduling, restart
behavior and operator reset. Public responses retain a fixed sanitized message.

These checks are not live database/provider acceptance, a full cluster soak,
throughput measurements or a general process-memory guarantee.
