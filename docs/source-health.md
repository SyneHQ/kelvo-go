# Passive source diagnostics

Kelvo can retain the latest eligible native-query outcome for each configured
live source on a tenant-bound worker. This is opt-in, bounded and passive: it
runs no probe SQL, opens no extra database connections and creates no polling
goroutine. It is useful for distinguishing a recent access failure from a source
that has not been queried recently.

Enable it in the **node** YAML:

```yaml
source_health:
  observation_ttl: 5m
```

The TTL must be between one second and 24 hours. At most 256 configured live
source IDs are tracked. Duplicate or invalid IDs fail startup; request-supplied
IDs cannot grow the registry. Omitting the configuration disables recording and
the endpoint. Restarting the node clears observations.

## Read the last observed outcome

Worker `GET /sources` requires the existing verified internal gateway mTLS
identity, just like `/datasets` and `/history`. It returns only sources from that
worker's catalog; the worker is bound to one tenant. It is an internal operator
diagnostic, absent from the public gateway API. Tenant API tokens alone do not
grant access. Collect it through authorized internal tooling and retain the
worker's TLS identity checks.

`HEAD` is supported. Other methods return 405. Responses set `Cache-Control:
no-store`; source, tenant or SQL parameters on the URL do not change the scope.

Example response:

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

| State | Category | Meaning |
| --- | --- | --- |
| `succeeded` | `none` | The last eligible native query completed its Arrow transfer and worker cleanup within the observation TTL. |
| `failed` | `access` | A typed native access/authentication error was the final query result. |
| `failed` | `unavailable` | A typed native unavailability error was the final query result. |
| `failed` | `query` | The native query failed; this may be query-specific and does not establish a connection outage. |
| `unknown` | `none` | No eligible observation exists, or the last observation has expired. |

Expired entries retain `last_observed` and its increasing age while their state
returns to `unknown`. A never-observed entry has no timestamp. The TTL measures
observation age, not future reachability. `succeeded` does not promise that the
next query will succeed; `failed` does not imply every query would fail.

## Attribution and isolation

Only `mode: native` calls record observations. Scheduled snapshot refreshes share
the node registry when their extraction uses native mode. Their observation is
about extraction, not subsequent Parquet publication. Snapshot reads, file reads,
federated joins and the startup `SELECT 1` do not refresh source health. A joined
query cannot reliably assign one failure to every participating source.

Source admission failures, parent cancellation/deadlines, missing credentials,
worker startup/cleanup errors, unsupported result types, local resource limits
and transport-only failures do not change the observation. Failure reporting
requires a decoded child outcome whose same typed error remains the final
result. Categories are fixed; SQL, parameters, DSNs, credentials, error messages,
query IDs and tenant identities are never stored in the registry. Configured
source IDs are only returned by this authorized diagnostic and are never added
to metric labels or trace attributes.

Recording holds a short process-local lock. Response snapshots are bounded and
rendered after releasing it. No event history accumulates with query volume.
The endpoint remains available during drain/cleanup; source state never affects
`/health` or `/ready`. An optional source outage must not remove the worker from
service for unrelated sources. Required snapshot readiness remains a separate
[dataset metadata check](operations.md#dataset-diagnostics-and-required-readiness).

## Validate the contract

On the designated Linux build/test host:

```sh
go test -race ./internal/telemetry ./internal/worker ./internal/cluster \
  -run 'Test(SourceHealth|ExecutorSourceHealth|LoadNodeSourceHealth)' -count=1
```

The tests cover TTL boundaries, bounded registry size under unknown request IDs,
concurrent record/snapshot access, detached responses, fixed classifications,
exclusion of parent/transport/federation failures, an actual disposable worker
IPC completion, configuration validation, mTLS denial, catalog scoping and
readiness independence. The disposable child is a protocol fixture; passing it
does not establish provider-wide health detection or sustained-load overhead.
