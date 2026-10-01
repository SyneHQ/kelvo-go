# Architecture

Kelvo Go owns source registration, query lifecycle, Arrow delivery and admission.
DuckDB supplies SQL execution and federation. Native ClickHouse executes at the source.
No DataFusion, Drill, LLM/GPU runtime, Kubernetes operator or Hakopod dependency is included.

```mermaid
flowchart LR
    Client[CLI or HTTP client] --> Coordinator[Go coordinator]
    Coordinator --> Worker[Disposable Go query worker]
    Worker --> DuckDB[DuckDB: files and PG/MySQL federation]
    Worker --> ClickHouse[Native ClickHouse ArrowStream]
    Worker --> Arrow[Arrow IPC batches]
    Arrow --> Client
```

Each query uses a new process. Federated queries create a fresh DuckDB instance; native queries invoke their selected connector directly. Only selected source definitions and their explicitly named environment variables enter the worker. Cluster nodes launch that process through a native Landlock/seccomp sandbox before Go creates threads. Tenant containers provide PID, memory and network boundaries. Cancellation terminates the query process group; deployment supervision must also bound the process tree when a node is forcibly terminated. A process failure terminates its query; streams are not transparently resumed.

The callback contract is `Executor.Execute(context, Request, Sink)`. A sink borrows each Arrow batch only during its synchronous Write call. The DuckDB adapter keeps execution, record iteration and Release inside the leased `sql.Conn.Raw` callback.

The pinned DuckDB Go v2.10506.0 path uses non-streaming pending execution, then exposes batches. Kelvo bounds returned rows before execution and enforces delivery limits, but native allocations can precede a limit check. First-batch latency and working memory depend on the engine plan. For a native ClickHouse request, the same configured memory limit is passed to ClickHouse as its per-query memory budget and bounds Kelvo's local Arrow decoding allocator; it is still not a worker-process RSS cap.

Single-domain `serve` uses a bounded in-memory TTL registry and one service token. [Cluster mode](cluster.md) uses per-tenant tokens, NATS account isolation, atomic KV admission, worker leases and mTLS result delivery. It distributes independent queries among tenant-bound workers. External identity/KMS integration, an Arrow Flight SQL server (the project includes a Flight SQL client), durable query exports, CDC and additional native adapters remain future milestones.

[Dataset acceleration](acceleration.md) executes configured refresh queries in the same subprocess boundary and writes immutable Parquet generations. A private tenant store atomically publishes YAML manifests; queries pin only selected generations and receive no original-source credentials for dataset-only reads. Full refreshes can run manually, through a local scheduler, or through a bounded tenant NATS refresh queue. Cluster workers share a tenant-specific POSIX volume; automatic object-store distribution and local caching are not implemented. This dataset lifecycle is separate from the per-query DuckDB instance and the query-handle TTL registry.

Live federation reads sources independently. It has no global transaction or comparable cross-source watermark. Later CDC must include snapshot/log handoff, durable checkpoints, idempotent replay, deletes, schema changes and recovery from expired source history.

Source references:

- [DuckDB Go Arrow API](https://github.com/duckdb/duckdb-go/blob/v2.10506.0/arrow.go)
- [Driver query execution](https://github.com/duckdb/duckdb-go/blob/v2.10506.0/statement.go#L778-L803)
- [DuckDB streaming flag](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/capi/pending-c.cpp#L17-L44)
- [Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
- [Spice OSS](https://github.com/spiceai/spiceai), architectural reference
