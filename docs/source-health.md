# Passive source diagnostics

Track the latest eligible native-query outcome without probe SQL, extra connections or polling. Enable it in node YAML:

```yaml
source_health:
  observation_ttl: 5m
```

TTL accepts 1s–24h. The registry tracks at most 256 configured live source IDs; requests cannot grow it. Omit the block to disable it. Restart clears observations.

## Read the last observed outcome

Call worker `GET /sources` with the verified internal gateway mTLS identity. Tenant API tokens do not grant access; this endpoint is absent from the public gateway.

```json
[
  {
    "id": "orders",
    "state": "failed",
    "category": "access",
    "last_observed": "2026-10-03T12:00:00Z",
    "age_ns": 2000000000
  },
  {"id": "warehouse", "state": "unknown", "category": "none", "age_ns": 0}
]
```

| State/category | Meaning |
| --- | --- |
| `succeeded` / `none` | Last eligible query completed transfer and cleanup within TTL |
| `failed` / `access` | Typed authentication or permission failure |
| `failed` / `unavailable` | Typed source unavailability |
| `failed` / `query` | Query failure; not necessarily an outage |
| `unknown` / `none` | No eligible observation, or TTL expired |

Expired entries retain their timestamp/age. The last outcome does not predict the next query. `HEAD` works, other methods return 405, and responses use `Cache-Control: no-store`.

## Attribution and isolation

Only native calls contribute, including native refresh extraction. A refresh observation covers extraction, not Parquet publication. Federation, snapshots, files and startup probes are excluded.

Admission, missing credentials, parent cancellation/deadlines, startup/cleanup, unsupported types, local limits and transport-only failures do not change state. A decoded typed child failure must remain the final result.

The bounded registry stores no SQL, parameters, errors, credentials, query IDs or tenant identity. Configured source IDs appear only in this protected diagnostic. Snapshots are detached under a short lock; no history accumulates.

Source state never changes `/health` or `/ready`, and remains readable during drain. [Required-dataset readiness](operations.md#dataset-diagnostics-and-required-readiness) is separate.

## Validate the contract

Run on the designated Linux host:

```sh
go test -race ./internal/telemetry ./internal/worker ./internal/cluster \
  -run 'Test(SourceHealth|ExecutorSourceHealth|LoadNodeSourceHealth)' -count=1
```

Checks cover TTL, bounds, concurrency, classification, excluded failures, worker IPC, mTLS and readiness independence. The child is a protocol fixture; this does not prove provider-wide detection or sustained-load overhead.
