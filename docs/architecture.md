# Architecture

Kelvo handles source registration, query admission and Arrow delivery. Native queries run at one database; federated queries run in local DuckDB.

[![Kelvo query, acceleration and cluster workflow](../brand/kelvo-architecture.png)](../brand/kelvo-architecture.svg)

## Workflow in text

1. **Submit.** Send SQL, typed parameters and registered source IDs. HTTP `serve` uses one catalog token; the cluster gateway maps tokens to tenants.
2. **Admit.** Apply deadlines and limits. Cluster mode reserves a tenant job slot, queues its ID, and lets a tenant-bound node claim it.
3. **Prepare.** Resolve only selected credentials and pin authorized, fresh snapshot generations. Dataset-only queries receive no original-source credentials.
4. **Execute.** Start a disposable native or DuckDB process. HTTP execution waits for the first, single-consumer result request.
5. **Deliver.** Validate Arrow IPC and output limits. Return uncompressed results by default, or opt into LZ4. Cluster results travel over mTLS, outside NATS.

## Execution and admission boundaries

| Boundary | Contract |
| --- | --- |
| Process | One process per query; a failed process fails its query. No transparent stream replay. |
| Sandbox | Linux cluster workers apply Landlock/seccomp before Go starts threads. Deployment supplies tenant network, PID, memory and disk limits. |
| Admission | Queries and refreshes share configured node budgets; optional reserves protect interactive work. Reservations do not enforce RSS. |
| Source quotas | Bound admitted operations across a tenant's nodes, not database connections. |
| Arrow ownership | `Sink.Write` borrows each batch synchronously. DuckDB iteration and release remain inside `sql.Conn.Raw`. |

**DuckDB completes execution before exposing Arrow batches.** Delivery backpressure does not bound native execution memory. ClickHouse's source query budget and local decoder limit are also not process-RSS caps.

The [native federation bridge](federation.md) projects columns and pushes supported integer/Boolean filters through Go connectors. Other predicates, joins and aggregates run in DuckDB. Independent live sources have no shared transaction snapshot.

## Full refresh to pinned dataset reads

1. Run the catalog's full source query with configured limits.
2. Validate one exact Arrow schema across all batches. Cross-generation changes follow the configured [schema policy](schema-evolution.md).
3. Publish an immutable single-file or [multipart](multipart-acceleration.md) Parquet generation. Failed refreshes preserve the previous generation.
4. Pin a generation after checking its authorization fingerprint and age. Missing, stale or incompatible data fails; there is no automatic live-source fallback.

[Object storage](object-storage.md) serves selected versions through a parent-owned range bridge. Cloud credentials stay in the parent; queries get no listing/deletion access or persistent local cache.

Each node refreshes one dataset at a time. Refresh reservations last through publication and cleanup. Change `authorization_version` after source grants change: copied data cannot inherit later revocations. Freshness measures completed refresh time, not a source transaction watermark.

## Cluster control and result paths

NATS JetStream stores SQL, parameters and lifecycle state; the dispatch queue carries only job IDs. Database credentials and Arrow results stay out of NATS.

One query stays on one worker. After the node records `result_ready`, the gateway commits `succeeded` before releasing Arrow's final end marker. Check both transport completion and [result semantics](cluster.md#results-and-errors); truncation never means success.

Worker loss or client disconnect ends the attempt without automatic SQL replay. Tenant tokens cover the tenant catalog; per-user row/column policies and external identity integration are not implemented.

## Verified local backup and recovery

The Linux [backup command](snapshot-backup.md) verifies and copies a current generation into a **new private root**, preserving identity and original age. It neither overwrites a destination nor switches a running service.

Verify recovery, test a query, then change service configuration explicitly. Stale data remains stale. Retained-generation restore is a separate operation; backup scheduling and failover belong to the operator.

## Saved-connection operations

An application gateway keeps user authentication, connection lookup and decryption. Kelvo owns customer execution through its contained [Go adapters](../adapters/go/README.md) and optional [JDBC runtime](jdbc-runtime.md).

1. The gateway signs an exact request with the saved connection ID.
2. A worker claims the operation and fetches current credentials over private mTLS.
3. The adapter receives credentials on stdin and executes within its resource limits.
4. Kelvo retains the outcome receipt and bounded Arrow result. Status reads never rerun the operation.

Credentials stay out of the queue, ledger, argv and child environment. A lost write acknowledgement requires reconciliation. See [database operations](database-operations.md) for supported capabilities, admission and retry rules.

## Worker operations

- [Metrics and diagnostics](operations.md#diagnostics): protected aggregate state; no SQL, parameters or secrets.
- [History and tracing](tracing.md): optional bounded local telemetry, without full distributed timing.
- [Readiness](operations.md#dataset-diagnostics-and-required-readiness): configured dataset metadata checks; not payload integrity, source health or spare capacity.
- [Drain](operations.md#maintenance): stop admission, fail readiness, finish within the grace period, then cancel remaining work.

## Source references

[DuckDB Go Arrow API](https://github.com/duckdb/duckdb-go/blob/v2.10506.0/arrow.go) · [Driver execution](https://github.com/duckdb/duckdb-go/blob/v2.10506.0/statement.go#L778-L803) · [DuckDB streaming flag](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/capi/pending-c.cpp#L17-L44) · [Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc) · [Parquet](https://parquet.apache.org/docs/file-format/)
