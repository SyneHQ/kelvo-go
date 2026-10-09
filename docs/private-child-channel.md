# Private source child channel

The Linux `childipc` package transfers database streams through inherited file descriptors for opt-in [private operations](private-operation-transport.md). Complete deployment qualification before enabling traffic.

1. The trusted parent validates the original grant, source binding, execution lease and worker identity.
2. The parent creates one channel for one source authority and one admitted child.
3. The launcher inherits the descriptor. The parent closes its duplicate after process start.
4. `Serve` checks the kernel sender PID against the actual child PID for every request.
5. The parent opens each physical connection through its configured broker dialer. The child receives only a stream descriptor.

Requests contain a protocol version and a purpose. Children cannot submit routes, sources, endpoints, proofs or additional descriptors. Cancellation requires an explicit parent capability.

The channel allows 1–64 active streams. IPC writes remain limited to two seconds. A private data-open response can wait for the configured resolver timeout plus the transport setup budget, at most 32 seconds. The trusted parent sets this bound; the operation deadline can shorten it. Ordinary opens keep their two-second default.

The parent retains physical connection ownership until cleanup completes. A cleanup deadline or physical close error must prevent a successful completion receipt.

Use an execution-cleanup context for the server and a separate operation context for data opens. Cancelling SQL stops pending data resolution. It must not revoke the parent's separate cleanup authority.

Azure tests cover a real child process, stream integrity, incorrect PID/source, malformed messages, incoming descriptor cleanup, capacity, cancellation denial and delayed/failed physical cleanup. Complete [deployment acceptance](private-transport-activation.md) for each deployed parent and adapter pair.
