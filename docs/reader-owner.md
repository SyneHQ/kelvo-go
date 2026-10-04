# Bounded reader owners and guards

Kelvo's private reader state machine retains custody during cancellation and
uncertain cleanup. [Protected object snapshots](protected-object-readers.md)
connect it to contained queries, ranges, process trees and scratch cleanup.
Remote garbage collection remains disabled.

## Ownership and bounds

The private budget supports two overlapping owners. The current node runtime
uses one immutable owner and requires a restart for replacement. Owners copy
storage and dataset policy with credential references; their configuration
retains neither mutable catalogs nor request contexts.

| Private component limit | Value |
| --- | ---: |
| Concurrent owner records, including unresolved replacements | 2 |
| Unfinished guards across all owners | 128 |
| Reserved acquisitions and retained pins across all owners | 128 |
| Bindings reserved together per guard | 1–64 |
| Consumer registrations over one guard's entire lifetime | 4 |

Reservations precede acquisition and cancellation callbacks. Partial failure
and stalled cleanup keep the entire guard reservation charged. The component's
replacement path shares that budget; node hot replacement is unsupported.
These are component bounds, not production sizing.

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
native-process exit. The protected node supplies separate completion tokens for
preparation, range handlers, the verified-empty process tree and scratch cleanup.

## Validation and remaining work

[The receipt](evidence/reader-owner.json) records frozen source
`43af914669b61c7f809c56b304dbda5460c2f4a3`: **67 required top-level tests**,
**290 pass events including subtests**, no failures or skips, passing race
detection and `go vet`. Inputs stayed unchanged and service cleanup passed.

That receipt covers the owner component before runtime integration.
[Issue 118](https://github.com/SYNEHQ/kelvo-go/issues/118) tracks protected runtime
acceptance. [Issue 14](https://github.com/SYNEHQ/kelvo-go/issues/14) retains
maintenance readers, legacy cutover and provider acceptance. Retirement and
deletion require a separate reviewed protocol.

[Custody API](reader-custody.md) · [Reader registry gates](durable-reader-registry.md#integration-gates-still-open) · [Production roadmap](production-roadmap.md)
