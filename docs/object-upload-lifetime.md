# Object upload input lifetime

Kelvo's S3, R2, GCS-compatible and Azure upload clients retain a borrowed input
until the HTTP transport has closed its request body and every admitted read has
finished. Go permits transport body closure after `Client.Do` returns, including
on errors. Waiting for that completion prevents a staging file or registry
payload from being reused while HTTP can still read it.

`Put` never closes the caller's reader. Its bounded body serializes reads; body
`Close` promptly seals new reads without waiting on an active read. A separate
join waits for actual completion. The response body closes before that join,
including when the server responds before consuming the upload. Cancellation
does not fabricate completion: a stalled read or missing transport close retains
the invocation. Callers must wait for `Put` before reusing or closing the input.

Size validation, conditional writes, signing order, redirect policy and existing
retry behavior remain unchanged. No replay body or retry loop is added. Empty
uploads retain `http.NoBody`. A signing failure closes the unsubmitted body
locally because HTTP never acquired it.

The [validation receipt](evidence/object-upload-lifetime.json) records frozen
commit `5f06d0ac9f965b71e840dae79d4104936ac3bce4`: all **22 top-level objectstore tests**
passed under race detection, with **426 pass events including subtests**, and
`go vet` passed. A separate control restored only the old S3/Azure implementations
and failed the exact early-ownership assertion; build errors, races and timeouts
were rejected as evidence. Tests include delayed closure, concurrent reads,
cancellation and real TLS servers returning early responses.

This offline gate does not certify live providers or client-wide quiescence.
[Reader-owner integration and provider acceptance](reader-objectstore.md#validation-and-remaining-gates)
remain open; remote deletion remains disabled.
