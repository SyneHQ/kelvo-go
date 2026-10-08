# Private source child channel

The Linux `childipc` package transfers database streams through inherited file descriptors. It is not wired into production workers yet.

1. The trusted parent validates the original grant, source binding, execution lease and worker identity.
2. The parent creates one channel for one source authority and one admitted child.
3. The launcher inherits the descriptor. The parent closes its duplicate after process start.
4. `Serve` checks the kernel sender PID against the actual child PID for every request.
5. The parent opens each physical connection through its configured broker dialer. The child receives only a stream descriptor.

Requests contain a protocol version and a purpose. Children cannot submit routes, sources, endpoints, proofs or additional descriptors. Cancellation requires an explicit parent capability.

The channel allows 1–64 active streams. Each setup exchange has a two-second deadline. The parent retains physical connection ownership until cleanup completes. A cleanup deadline or physical close error must prevent a successful completion receipt.

Use an execution-cleanup context for the server. A cancelled SQL request must not remove the parent's ability to open a cancellation connection.

Azure tests cover a real child process, stream integrity, incorrect PID/source, malformed messages, incoming descriptor cleanup, capacity, cancellation denial and delayed/failed physical cleanup. Worker integration and paired database tests remain required.
