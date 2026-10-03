# Object client lifetime

Kelvo's built-in S3, R2, GCS-compatible and Azure clients give `Close` this
[contract](../internal/objectstore/client.go):

1. Stop admitting calls, before upload `Seek` or network access.
2. Cancel every admitted request, including requests with returned bodies.
3. Wait for method cleanup, body reads, body closure and cancellation callbacks;
   then close idle connections. Concurrent `Close` callers wait together.

Close `Get` and `GetRange` bodies, even after EOF. Late responses remain owned
until cleanup finishes. Reads serialize around the complete response wrapper.

The [lifetime implementation](../internal/objectstore/lifetime.go) tracks
outstanding calls and bodies without an independent admission bound.
`MaxConnsPerHost` limits transport connections, not this bookkeeping. An
uncooperative `Seek`, read or transport close keeps shutdown waiting; cancellation
cannot substitute for completion.

[Backend Close](../internal/acceleration/object_store.go) joins its read client.
Separate writer clients, staging and node reader-owner cleanup remain outside
that guarantee. Completion covers Kelvo-owned I/O and body callbacks; it does
not promise every internal HTTP transport goroutine has exited. Remote deletion
remains disabled.

## Upload input

`Put` borrows the caller's reader until transport body closure and every admitted
read finish. It never closes that reader. Response cleanup runs before the join;
empty uploads keep `http.NoBody`. Wait for `Put` before reusing its input.

## Validation

The [shutdown receipt](evidence/object-client-lifetime-2322954.json) covers
`2322954`: 49 top-level tests and 582 pass events passed under race detection,
plus vet. Separate controls restoring the earlier provider or backend failed
their exact premature-return assertions. Source and helper hashes stayed
unchanged; the owned service and cgroup were removed.

The earlier [upload gate on `5f06d0a`](evidence/object-upload-lifetime.json) is
retained. Neither gate certifies production reader ownership, live providers
or capacity.
