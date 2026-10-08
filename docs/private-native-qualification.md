# Native private-transport qualification

The opt-in Linux helper tests Kelvo's dormant broker and CONNECT opener against
an actual Rabbit server, customer client and disposable PostgreSQL instance.
It does not enable private routing in Kelvo.

The paired Rabbit fixture checks:

- A CTE and 100,000 exact rows, including nulls, through native pgx and source TLS.
- Foreign tenant/source denial before a customer connection opens.
- Verification of the original database hostname.
- A separate TLS CancelRequest and confirmation that the owned backend stopped.
- Joined broker cleanup and exact customer connection counts.

The helper is compiled from
`internal/transportbroker/rabbitconnect` using `go test -c`. Rabbit's
`TestKelvoPrivateNativePostgres` receives its path through
`RABBIT_KELVO_NATIVE_HELPER`. Both normal and race builds must pass.

Only the test coordinator sends generated fixture keys and socket-only database
addresses through bounded stdin. The helper has no production issuer endpoint
and must never receive customer credentials.

Cancellation uses a second ordinary v1 connection with spare capacity. This
does **not** qualify cancellation under saturation, v2 reservations, child IPC,
production source routing or remote compute termination after revocation.
Those remain separate activation gates.
