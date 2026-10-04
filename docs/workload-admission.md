# Workload admission

Interactive queries, exports and refreshes share one process budget for slots, memory and scratch. Export and refresh together leave reserved capacity for interactive queries.

Configure the runtime through [durable exports](exports.md). [Persistent storage](export-storage.md) accounts for retained results separately. This page covers the shared Go admission API.

## 1. Set the shared budget

```go
pool, err := admission.New(admission.Limits{
    MaxConcurrent: 8,
    MemoryBytes:   8 << 30,
    ScratchBytes:  32 << 30,
    ReservedSlots:        2,
    ReservedMemoryBytes:  2 << 30,
    ReservedScratchBytes: 4 << 30,
    Classes: map[admission.Class]admission.ClassLimits{
        admission.ClassExport: {
            MaxConcurrent: 2,
            MemoryBytes:   3 << 30,
            ScratchBytes:  12 << 30,
        },
    },
})
if err != nil {
    return err
}
```

- Leave baseline runtime headroom outside this budget.
- Interactive and refresh ceilings default to the global budget. **Exports are disabled until configured** (`ErrDisabled`).
- Each class needs positive slots/memory and nonnegative scratch, all within the global limits. Zero scratch allows only requests that need none.
- Class ceilings share global capacity; adding them creates no extra capacity or guaranteed share.

Here, export and refresh together can use six slots, 6 GiB memory and 28 GiB scratch. Interactive work can use any idle capacity unless given a lower class ceiling.

## 2. Reserve before execution

```go
reservation, err := pool.Acquire(ctx, admission.Request{
    Class:        admission.ClassExport,
    MemoryBytes:  512 << 20,
    ScratchBytes: 2 << 30,
})
if err != nil {
    return err
}
defer reservation.Release() // only after execution and cleanup finish
```

Trusted execution code selects the class. Never accept it from SQL or an untrusted request. All competing work in a process must use the same pool.

| Selector | Meaning |
| --- | --- |
| No class, `Background: false` | Interactive; existing callers unchanged |
| No class, `Background: true` | Refresh; existing callers unchanged |
| Explicit `interactive`, `export` or `refresh` | Selected class; `false` is the default flag |
| Explicit class with `Background: true` | Allowed only for `refresh` |
| Unknown class or conflicting flags | `ErrInvalid` |

Requests need positive memory and nonnegative scratch. Impossible requests return `ErrOversize`; `TryAcquire` returns `ErrBusy` when capacity is occupied. `Acquire` waits for capacity or cancellation. Bound upstream queues; this pool promises no queue limit or fairness.

## 3. Keep reservations until cleanup

Cancellation removes waiters, not active reservations. Stop execution and finish cleanup before `Release`, which is safe to call concurrently or repeatedly.

`Drain` rejects new work and wakes waiters without canceling active work. `Wait` accepts a grace deadline; expiry leaves active reservations charged. `Snapshot` returns detached global, background and per-class counters. Changing a snapshot cannot reconfigure the pool.

Admission is accounting, not RSS enforcement, disk quotas or tenant isolation. Reserve native memory, Arrow/driver buffers and publication scratch conservatively. DuckDB may materialize before Arrow delivery; backpressure does not bound its query memory.

## Validate on the Linux test host

```sh
GOMAXPROCS=2 go test -race -count=10 -p 1 ./internal/admission
GOMAXPROCS=2 go vet -p 1 ./internal/admission
```

Tests cover shared/class limits, legacy callers, disabled exports, cancellation, drain, overflow and concurrent accounting. They do not establish end-to-end export availability or production capacity.
