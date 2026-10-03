# Worker failure and refresh recovery

Kelvo reports DuckDB's typed native out-of-memory error as
`RESOURCE_EXHAUSTED` with the fixed message `Query exceeded DuckDB memory limit`.
The mapping recognizes the pinned driver's error category, including wrapped
errors. It never searches driver text. Generic I/O errors, Go runtime failures
and externally killed processes retain their existing error handling.

Cluster scheduled refreshes persist these failures as `permanent` / `resource`.
They preserve the previous authorized snapshot and stop automatic retries until
an operator repairs the workload or budget and explicitly resets the status.
See [operations](operations.md#source-refresh-failures-and-recovery). A reset
alone cannot make an oversized query fit.

Worker completion also checks source-quota ownership after IPC delivery and
process cleanup. A cancellation between the final IPC check and completion
returns an error instead of permitting a refresh to publish. The original
execution deadline remains observable even if the quota's child context was
canceled earlier. Parent cancellation, deadlines and existing typed sink/worker
errors retain precedence. Coordination cause strings do not enter public errors.

## Development checks

The focused tests exercise:

- A real nonspillable list aggregate over two million generated integers with a
  16 MB DuckDB limit. The pinned native Arrow API reports its actual OOM category;
  Kelvo delivers no schema or rows and subsequently executes a small query.
- Completed Arrow IPC followed by deterministic source-ownership cancellation at
  the completion decision, with a still-live enclosing context.
- Real sandboxed worker execution, actual IPC and acceleration Manager publication
  with both single-file and multipart local snapshots. A Parquet source controls
  a list aggregate over twenty million integers with a 64 MB managed limit.
- Injected quota cancellation after a parent sink accepts a batch, released
  source/node admission, scratch cleanup, unchanged generation/hash/schema/time,
  permanent resource suppression, explicit reset and exact recovered values.

Build and run these checks on the designated Linux test machine:

```sh
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo
cc -O2 -Wall -Wextra -Werror sandbox/launcher.c -o bin/kelvo-landlock
KELVO_TEST_WORKER_BINARY="$PWD/bin/kelvo" \
KELVO_TEST_SANDBOX_BINARY="$PWD/bin/kelvo-landlock" \
  go test -race -tags duckdb_arrow ./internal/cluster \
  -run TestSandboxedWorkerFailurePreservesSnapshotAndAdmission -count=1
go test -race -tags duckdb_arrow ./internal/worker ./internal/engine/duckdb \
  -run 'Test(WorkerResult|WorkerQuotaLoss|ExecutorQuotaCancellation|ExecutorTypedSink|PublicErrorClassifies|ExecuteNativeMemory)' -count=1
```

The [recorded sandboxed run](evidence/worker-failure-acceptance.json) passed all
four required layout/failure cases against runtime changes through `f3886b7`.
The complete ordinary and bridge suites, affected subsystem race checks, vet,
builds and strict cgo checks also passed on the Azure test VM. CI wiring is
committed separately; this record is local development validation, not a claim
that the unpublished commits have run on hosted CI.

The quota cancellation is injected in the trusted parent. Retry-state tests use
an in-memory compare-and-swap fixture, including reconstruction of its queue
wrapper; they do not simulate a real broker restart. The sandbox launcher is
used, but these checks are not an independent sandbox escape audit. They do not
establish a process RSS bound, cgroup OOM protection, WAN throughput or capacity
under sustained concurrent tenants. The pinned DuckDB path still materializes
execution before Arrow delivery.
