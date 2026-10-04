# Reader acquisition and custody lifetimes

Kelvo's internal reader registry can separate request cancellation from the
lifetime of a confirmed snapshot pin. [Protected object snapshots](protected-object-readers.md)
use this API through a shared owner. Remote garbage collection remains disabled;
[issue 14](https://github.com/SYNEHQ/kelvo-go/issues/14) tracks the remaining gates.

## API contract

```go
func (r *Registry) AcquireWithLifetime(
    acquireCtx, custodyCtx context.Context, binding Binding,
) (*Lease, error)

func (l *Lease) Quiesced() <-chan struct{}
```

Both contexts must be non-nil. During acquisition, either context can cancel
the operation; providers also see the earlier deadline. The Registry reserves
capacity before creating cancellation work and joins any started cancellation
callback before returning or transferring a successful pin.

After confirmed acquisition, renewal follows `custodyCtx`. Ending `acquireCtx`
does not stop that pin's renewal. The returned lease retains no acquisition
context values unless the caller also supplied that context as `custodyCtx`.
Custody cancellation, renewal loss and the conservative provider-clock cutoff
still cancel `Lease.Context()` and fail `Check()`.

Existing `Acquire(ctx, binding)` delegates with both contexts equal, preserving
its request-cancellation behavior. Both entry points share the same Registry
capacity across datasets and generations. A Registry must be reused for its
configured scope; constructing one per query bypasses that bound.

## Cancellation is not cleanup proof

| Signal | What it establishes |
| --- | --- |
| Acquisition context ends | Acquisition must stop; a previously confirmed pin can continue renewing under its custody context. |
| `Lease.Context().Done()` | The pin no longer authorizes further consumption. It does not prove that existing consumers or provider work stopped. |
| `Close()` returns `ErrReleaseUnknown` | Cleanup or remote release is uncertain. Local capacity stays charged while provider work remains. |
| `Quiesced()` closes | Local renewal, watchdog and provider work have joined and the Registry slot has returned. It does not prove remote release succeeded or a native child exited. |

An ambiguous acquisition can leave a remote pin but returns no usable lease.
An uncooperative provider retains acquisition capacity until its actual method,
body cleanup and cancellation callback return. Closing a successful lease is
bounded; `Quiesced()` may remain open after that bounded call returns.

The [private owner and guard](reader-owner.md) preserve the execution deadline
and retain pins through preparation, ranges, the process tree and scratch cleanup.
Final completion checks ownership again. Missing metadata, expiry, cancellation,
restart and local quiescence never authorize deletion.

## Frozen-source validation

[The receipt](evidence/reader-custody.json) records source revision
`858cd7a1374d1c0526ebcee2fc70991da193680d`. All **44 required top-level tests**
passed with race detection and no failures or skips, producing **235 pass events
including subtests**. The reader registry contributed 37 tests/136 events and
the immutable binding regressions 7 tests/99 events. `go vet` passed for both
packages.

The new checks cover renewal after acquisition cancellation/deadline, context
value separation, earlier custody deadlines, cancellation during reads and
committed writes, cancellation-callback joins, stalled provider Read/Close and
once-only cleanup under concurrent custody loss.

The offline Linux amd64 run used Go 1.26.8 and the cached native bridge through
an external module file. All 948 source files, bridge inputs and module files
remained unchanged. The service enforced one CPU, 3 GiB memory, no swap, 256
tasks, non-root execution, zero capabilities and a private network namespace
containing only loopback. Its exact service and cgroup were removed afterward.
These limits describe the validation environment, not production sizing or
throughput. Fixture tests do not establish live-provider compatibility,
production consumer safety or garbage-collection readiness.

[Reader registry and remaining gates](durable-reader-registry.md) · [Object-store components](reader-objectstore.md) · [Production roadmap](production-roadmap.md)
