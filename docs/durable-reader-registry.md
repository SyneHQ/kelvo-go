# Durable reader registry

`internal/readerlease` provides durable remote snapshot reader pins.
[Protected object snapshots](protected-object-readers.md) connect it to v5
publication and contained queries. Remote deletion remains disabled;
[issue 14](https://github.com/SYNEHQ/kelvo-go/issues/14) tracks the remaining gates.

## What the package provides

- A separate generation-specific CAS document, so reader renewal does not
  contend with the writer fence in `current.yaml`.
- Staging and sealing under a caller-held writer fence, with a random
  incarnation and immutable-content digest checked on acquisition and renewal.
- Conservative local deadlines derived from authenticated provider time,
  round-trip duration and configured clock uncertainty. An independent watchdog
  cancels readers even when renewal stalls.
- Strict, bounded YAML with canonical scalar types, exact fields and no aliases
  or duplicate keys. Fractional or coerced fencing sequences are rejected.
- Bounded local capacity across generations. A provider operation that ignores
  cancellation retains its slot until it actually exits, including after a
  bounded `Close` returns an uncertain result.

| Default bound | Value |
| --- | --- |
| Readers per remote generation document | 128 |
| In-flight acquisitions and unclosed/non-quiesced leases per Registry | 128 |
| Encoded document | 64 KiB |
| CAS attempts per operation | 8 |
| Lease / renewal interval | 60 seconds / 15 seconds |
| Provider operation timeout / clock uncertainty | 5 seconds / 2 seconds |

These are protocol defaults, not production capacity measurements. Callers must
reuse the Registry for its configured scope and close leases even after
cancellation. Creating one Registry per request would bypass the local bound.

The provider adapter must supply current reads, atomic conditional writes,
authenticated service time and cancellation-aware I/O. An opaque object version
is a CAS token, not a content checksum. Missing or uncertain provider metadata
fails closed. Ambiguous writes can leave retained pins and never grant a reader
permission to start.

The [object-store adapter and immutable binding components](reader-objectstore.md)
have focused race/vet coverage. The node reuses one immutable runtime, with
dedicated registry credentials and separate data/registry transports.

The [acquisition and custody lifetime API](reader-custody.md) also has focused
race/vet coverage. It separates request cancellation from confirmed-pin renewal
and exposes local quiescence without claiming consumer cleanup or remote release.
The [owner/guard](reader-owner.md) retains that custody through query cleanup.

## A pin does not authorize access

The caller must compute a complete canonical immutable-commit digest; the
registry checks the supplied value's equality and cannot detect omitted fields.
The existing commit `Fingerprint` describes source/dataset configuration.
Per-request principal and row-policy permissions still need separate checks.
Immutable authorization provenance in a binding does not grant current access.

## Integration gates still open

| Reviewable slice | Required behavior and acceptance |
| --- | --- |
| Runtime acceptance | [#118](https://github.com/SYNEHQ/kelvo-go/issues/118): verify publication, acquisition order, contained queries and cleanup against the frozen source. |
| Live providers | Validate exact-key CAS, service time, cancellation and credential rotation against each supported object provider. |
| Maintenance readers | Status and previous-schema reads use guards. Verification, inventory, historical restore and migration backup refuse protected namespaces until integrated. |
| Legacy cutover | Protected mode requires a fresh namespace and v5 bindings. Legacy namespaces remain non-collecting until an explicit, fenced cutover. |

Remote garbage collection needs a separate reviewed retirement protocol that
prevents new readers and handles writer races, incomplete catalogs and uncertain
node/process cleanup. An empty or expired registry does not prove that a native
child stopped reading. No absence, timeout or restart authorizes deletion.

## Validation evidence

[The receipt](evidence/durable-reader-registry.json) records source commit
`50fd0ce86e284441b1a90da7d26194724904cc1f`, the exact nine-file package/module
inventory and unchanged hashes before and after testing. On Linux amd64 with
Go 1.26.8, all **19 required top-level tests and 68 pass events including
subtests** passed with no failures or skips; `go vet` also passed.

The test service enforced one CPU, 1 GiB memory, no swap, 128 tasks, non-root
execution, no capabilities and a private network namespace containing only
loopback. Its owned service was unloaded and its cgroup removed afterward.
Those are test limits, not a sizing recommendation or throughput claim.

An initial helper preflight failed before Go ran because it inspected a
host-mounted sysfs interface view. The corrected helper used the service's
`/proc/self/net/dev` view in fresh scratch space. The receipt retains that
failure and cleanup evidence; the package source did not change.

Reproduce the package checks on a disposable Linux host with the pinned Go
toolchain and dependencies already cached:

```sh
GOMAXPROCS=1 GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off \
  go test -mod=readonly -race -p 1 -count=1 -timeout=90s ./internal/readerlease
GOMAXPROCS=1 GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off \
  go vet -mod=readonly -p 1 ./internal/readerlease
```

These checks use a fault-injecting CAS fixture. They do not establish live
provider compatibility, complete reader-path integration or production
garbage-collection safety.

[Object storage](object-storage.md) · [Production roadmap](production-roadmap.md)
