# Worker failure and refresh recovery

Typed DuckDB OOM maps to `RESOURCE_EXHAUSTED` with `Query exceeded DuckDB memory limit`. Classification uses the pinned driver's category, never error-text matching; generic I/O, Go failures and external kills retain existing handling.

1. Inspect the scheduled refresh's permanent/resource failure. Its last authorized snapshot remains available and automatic retries stop across worker restarts.
2. Repair the workload or memory budget.
3. Explicitly [reset refresh status](operations.md#source-refresh-failures-and-recovery). Reset alone cannot make an oversized query fit.

Completion rechecks source-quota ownership after IPC and process cleanup, preventing publication after late ownership loss. Parent cancellation, original deadlines and typed sink/worker errors retain precedence; coordination strings stay private.

## Development checks

Tests exercise real nonspillable DuckDB OOM, completed IPC followed by ownership loss, actual sandbox/refresh publication for both local layouts, admission/scratch release, retained metadata, retry suppression and exact recovery after reset.

Run on the designated Linux test host:

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

The [sandboxed record](evidence/worker-failure-acceptance.json) passed four layout/failure cases through runtime `f3886b7`. Ordinary/bridge suites, affected race checks, vet, builds and strict cgo checks passed on Azure. This is development evidence; CI wiring alone does not establish hosted CI success.

Quota cancellation is injected in the trusted parent; retry state uses in-memory CAS, not a real broker restart. These checks are not an independent sandbox audit, RSS ceiling, cgroup OOM gate, WAN benchmark or sustained tenant test. The pinned DuckDB path materializes execution before Arrow delivery.
