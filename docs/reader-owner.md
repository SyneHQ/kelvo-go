# Bounded reader owners and guards

Kelvo's tested **private reader state machine** retains custody during
cancellation and uncertain cleanup. Resource attachment uses fixtures.
Production consumers remain unwired; there is no YAML switch or remote garbage
collection.

## Ownership and bounds

One node budget spans overlapping owner replacements. Owners copy immutable
storage and dataset policy with credential references; their configuration
retains neither mutable catalogs nor request contexts.

| Private component limit | Value |
| --- | ---: |
| Concurrent owner records, including unresolved replacements | 2 |
| Unfinished guards across all owners | 128 |
| Reserved acquisitions and retained pins across all owners | 128 |
| Bindings reserved together per guard | 1–64 |
| Consumer registrations over one guard's entire lifetime | 4 |

Reservations precede acquisition and cancellation callbacks. Owner replacement
does not reset them; partial failure and stalled cleanup keep the entire guard
reservation charged. These are component bounds, not production sizing.

## Cancellation, cleanup and results

`Check` and `HoldConsumer` refuse incomplete acquisition. Acquisition starts
once, and late returned pins remain owned. Each consumer callback completes only
its own token, once, without provider I/O; completed tokens cannot be reused.

Request cancellation and owner drain stop execution while custody continues
until actual acquisition and consumer cleanup. Observed failures stay recorded
after provider recovery. Owner drain also prevents success at final publication.

Bounded `Close` returns uncertainty on timeout, retaining clients and capacity.
Repeated calls create no new cleanup work. `Quiesced` reports joined local work
and returned capacity; it does not erase failure or prove remote release or
native-process exit. Production callers must supply real consumer cleanup proof.

## Validation and remaining work

[The receipt](evidence/reader-owner.json) records frozen source
`43af914669b61c7f809c56b304dbda5460c2f4a3`: **67 required top-level tests**,
**290 pass events including subtests**, no failures or skips, passing race
detection and `go vet`. Inputs stayed unchanged and service cleanup passed.

[Issue 14](https://github.com/SYNEHQ/kelvo-go/issues/14) still requires production
resource construction, dedicated registry credentials, guarded resolution and
ranges, subprocess/scratch cleanup transfer, versioned manifests and maintenance
readers. Retirement and deletion require a separate reviewed protocol.

[Custody API](reader-custody.md) · [Reader registry gates](durable-reader-registry.md#integration-gates-still-open) · [Production roadmap](production-roadmap.md)
