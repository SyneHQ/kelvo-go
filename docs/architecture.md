# Architecture

Kelvo Go owns source registration, query lifecycle, admission and Arrow delivery.
Native connectors execute queries against configured source databases. DuckDB supplies local SQL execution and joins across supported inputs. Optional dataset acceleration supplies persistent Parquet snapshots to DuckDB.
No DataFusion, Drill, LLM/GPU runtime, Kubernetes operator or Hakopod dependency is included.

[![Kelvo architecture: CLI and HTTP requests pass through Go admission and source selection to disposable native or DuckDB workers, then return Arrow results. NATS controls cluster jobs; optional Parquet snapshots feed DuckDB.](../brand/kelvo-architecture.png)](../brand/kelvo-architecture.svg)

## Workflow in text

1. **Submit a request.** The caller supplies SQL or a supported native request, parameters and registered source IDs. The CLI runs locally without a bearer-token layer. HTTP `serve` uses one service token for its configured catalog; the cluster gateway maps each bearer token to a provisioned tenant. Requests cannot supply arbitrary connection credentials or override the cluster tenant.
2. **Admit and claim execution.** The CLI executes immediately. Single-domain `serve` creates a bounded, in-memory TTL handle; its first results request claims a concurrency permit and starts execution. In cluster mode, the gateway reserves a tenant job slot in JetStream KV and queues only its query ID. A tenant-bound node reserves local capacity before pulling the job, assigns ownership atomically, and waits for the gateway's single-consumer result claim before execution. Status and cancellation use the query handle.
3. **Resolve the catalog and selected inputs.** The coordinator applies configured limits and a deadline, selects registered sources, and pins requested accelerated generations after checking their policy and freshness. Only selected source definitions and their explicitly named environment variables enter the disposable query process. Configuration refers to environment-variable names; source identities still require appropriate read-only database grants. Dataset-only queries receive no original-source credentials.
4. **Choose one of two execution modes.** Native mode invokes the selected connector for one configured connection, using that source's query language and capabilities. Federated mode creates a fresh in-memory DuckDB instance. Supported inputs include CSV, Parquet, DuckDB, SQLite, PostgreSQL and MySQL. The optional pinned Go/C++ Arrow scan bridge adds selected-table federation through the Go ClickHouse, PostgreSQL, MySQL, SQL Server, Oracle, Snowflake, BigQuery and Databricks connectors, with supported projection and filter pushdown. Joins and aggregates execute in local DuckDB. Accelerated aliases resolve to Parquet inputs for this same mode; acceleration is a dataset lifecycle, not a third SQL engine. Native connector availability does not imply live DuckDB federation; see the [capability matrix](source-coverage.md#acceleration-and-federation-capabilities).
5. **Deliver typed results.** The engine writes borrowed Arrow batches synchronously into an uncompressed local IPC pipe. The parent validates IPC framing, metadata and delivery limits, then applies public result compression once: uncompressed by default, optionally `lz4_frame`. The CLI finalizes and atomically publishes an Arrow file. HTTP returns `application/vnd.apache.arrow.stream` for PyArrow or another compatible consumer. Cluster result bytes travel directly from the node to the gateway over mTLS, then to the caller. HTTP retrieval is single-consumer; callers must check transport completion and terminal status.

## Cluster control and result paths

[Cluster mode](cluster.md) uses tenant tokens, NATS account isolation, atomic KV admission and worker leases. The job record in tenant KV contains the request, including SQL and parameters; the dispatch queue carries only the query ID. Credentials and Arrow result batches do not enter NATS. NATS also carries lifecycle and cancellation state, while the mTLS connection carries the Arrow result.

One query executes on one node. More nodes serve other independent queries; no SQL plan or intermediate shuffle is distributed between nodes. After execution the node records `result_ready`. The gateway verifies that state and ownership, commits `succeeded`, and only then releases the final Arrow end-of-stream marker. Failed or partial streams do not carry successful completion. Worker loss or client disconnect ends the attempt; there is no transparent stream replay or automatic execution retry.

Single-domain `serve` remains one trust domain. Tenant-wide source grants are not automatic per-user row policies. External identity/KMS integration, an Arrow Flight SQL server (the project includes a Flight SQL client), durable query exports and CDC remain future work.

## Execution boundaries

Each query uses a new process. Federated queries create a fresh DuckDB instance; native queries invoke their selected connector directly. Only selected source definitions and their explicitly named environment variables enter the worker. Cluster nodes launch that process through a native Landlock/seccomp sandbox before Go creates threads. Tenant containers provide PID, memory and network boundaries. Cancellation terminates the query process group; deployment supervision must also bound the process tree when a node is forcibly terminated. A process failure terminates its query; streams are not transparently resumed.

The callback contract is `Executor.Execute(context, Request, Sink)`. A sink borrows each Arrow batch only during its synchronous Write call. The DuckDB adapter keeps execution, record iteration and Release inside the leased `sql.Conn.Raw` callback.

The pinned DuckDB Go v2.10506.0 path uses non-streaming pending execution, then exposes batches. Kelvo bounds returned rows before execution and enforces delivery limits, but native allocations can precede a limit check. First-batch latency and working memory depend on the engine plan. For a native ClickHouse request, the same configured memory limit is passed to ClickHouse as its per-query memory budget and bounds Kelvo's local Arrow decoding allocator; it is still not a worker-process RSS cap.

The [custom federation bridge](federation.md) receives DuckDB optimizer projections
and typed filters, executes supported predicates through Go native connectors,
and lends retained/pinned Arrow batches back through the C Data interface. The
adapters support ClickHouse, PostgreSQL, MySQL, SQL Server, Oracle, Snowflake, BigQuery and Databricks. Trusted contributors can add a source through the [public Go adapter interface](federation-adapters.md). String, decimal, floating-point and temporal filters stay in DuckDB; the bridge advertises source pushdown only for integer/boolean columns. The bridge retains the existing subprocess trust boundary
and uses an opt-in pinned driver accessor patch; no Rust service is introduced.

Live federation reads sources independently. It has no global transaction or comparable cross-source watermark.

## Optional dataset acceleration

[Dataset acceleration](acceleration.md) executes configured refresh queries in the same subprocess boundary and writes immutable Parquet generations. A private tenant store atomically publishes YAML manifests; queries pin only selected generations and receive no original-source credentials for dataset-only reads. Full refreshes can run manually, through a local scheduler, or through a bounded tenant NATS refresh queue. The default backend uses a tenant-specific POSIX volume. The opt-in object backend uses provider-conditional manifest publication and immutable Parquet objects. A parent-owned loopback range bridge serves only selected versions to DuckDB and rejects upstream redirects; cloud reader and writer credentials never enter snapshot query subprocesses. It does not pre-download whole snapshots or maintain a persistent local cache. This dataset lifecycle is separate from the per-query DuckDB instance and the query-handle TTL registry.

The [object backend](object-storage.md) supports S3, R2, Google Cloud Storage and Azure Blob. Acceleration is a developer preview: each full refresh reruns the configured source query and consumes source resources; queries read the committed copy between refreshes. Existing readers retain their pinned generation when a replacement is published. Missing, stale or incompatible snapshots fail acquisition without automatic source fallback. Independent datasets have no global transactional snapshot.

The separate cluster refresh queue carries dataset identifiers and configuration fingerprints, never source SQL, credentials or Arrow data. Each node handles at most one refresh at a time, separately from query admission. Acceleration provides neither incremental loading nor query-result caching. Later CDC must include snapshot/log handoff, durable checkpoints, idempotent replay, deletes, schema changes and recovery from expired source history.

Source references:

- [DuckDB Go Arrow API](https://github.com/duckdb/duckdb-go/blob/v2.10506.0/arrow.go)
- [Driver query execution](https://github.com/duckdb/duckdb-go/blob/v2.10506.0/statement.go#L778-L803)
- [DuckDB streaming flag](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/capi/pending-c.cpp#L17-L44)
- [Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
- [Spice OSS](https://github.com/spiceai/spiceai), architectural reference
