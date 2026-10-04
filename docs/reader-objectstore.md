# Object-backed reader registry components

Kelvo has two tested internal components for protecting remote snapshot readers:
an adapter from `objectstore.Client` to the reader registry, and a canonical
binding for immutable snapshot metadata. [Protected object snapshots](protected-object-readers.md)
use both for publication and contained queries. Remote deletion remains disabled;
[issue 14](https://github.com/SYNEHQ/kelvo-go/issues/14) tracks the remaining work.

## Registry storage adapter

`readerlease.NewObjectStore(client, prefix, tenant)` borrows a trusted provider
client and accepts only keys with this layout:

```text
<prefix>/<tenant>/<dataset>/reader-leases/<32-lowercase-hex-generation>.yml
```

Foreign scopes, traversal and other layouts fail before provider I/O. Reads use
the exact current key, materialize at most one 64 KiB control document, and
verify the complete length and SHA-256 before exposing bytes. Query results do
not pass through this adapter. Metadata must contain a bounded positive size,
valid opaque version, checksum and nonzero provider time.

Conditional writes copy the caller's bytes before provider access. An empty
expected version means atomic create-if-absent; other writes require the exact
CAS version. Only definite provider absence on reads and definite conditional
conflict on writes map to those outcomes. Joined, cyclic or custom-`Is` errors
fail closed. An uncertain write never establishes that no write occurred.

Cancellation closes the provider body, and cleanup joins both the read and
close callback before returning ownership. A provider that ignores cancellation
therefore retains the Registry's capacity slot until it actually stops. The
adapter never calls client `Close`, `Head`, listing or deletion methods.

The client must authenticate service time and honor current reads, atomic
conditions and cancellation. A timestamp field alone does not prove that trust.
The protected runtime uses dedicated parent-only registry rights and reuses a
bounded Registry/provider owner across queries.

See [object client and upload lifetime](object-upload-lifetime.md) for input
ownership and shutdown behavior. The built-in shutdown extension passed its
offline validation gate; live-provider acceptance remains open.

## Immutable snapshot binding

`acceleration.objectReaderBinding` validates and hashes a versioned binary
encoding of the storage location, tenant, dataset, generation, incarnation,
single-object or multipart layout, checksum, object/descriptor version,
descriptor bounds, row/byte counts, schema, configuration fingerprint and
freshness instant. Length-prefixed strings and fixed-width integers make field
boundaries unambiguous. UTC seconds plus nanoseconds preserve supported
timestamps without `UnixNano` range loss.

The caller must verify the immutable object or multipart descriptor before
sealing this binding. A verified descriptor checksum transitively binds its
parts. The binding does not authenticate a user, grant data access, register a
pin or publish a manifest. Current principal and row-policy authorization remain
separate checks.

## Validation and remaining gates

[The frozen-source receipt](evidence/reader-objectstore.json) covers commit
`480faf83f4b44bb6f5028ed7d6f3c147bd1e16ed`: all **36 required top-level tests**
passed with race detection, producing **213 pass events including subtests**,
with no failures or skips. `go vet` passed for both packages. The reader registry
contributed 29 tests/114 events and the binding helper 7 tests/99 events.

The offline Linux amd64 run used Go 1.26.8 and the pinned cached native bridge
through an external module file. Its complete 941-file source snapshot, bridge
inputs and effective module files were unchanged. The isolated service enforced
one CPU, 3 GiB memory, no swap, 256 tasks, non-root execution, zero capabilities
and a private network namespace with only loopback. The exact service unloaded
and its cgroup disappeared afterward. These are validation limits, not a
production sizing or throughput claim.

This historical receipt covers the adapter and binding components.
[Runtime acceptance](protected-object-readers.md#validation-and-remaining-work)
is separate. Remaining [lifecycle gates](durable-reader-registry.md#integration-gates-still-open)
include providers, maintenance readers and legacy cutover. Retirement and
deletion need their own reviewed protocol.

[Reader registry](durable-reader-registry.md) · [Object storage](object-storage.md) · [Production roadmap](production-roadmap.md)
