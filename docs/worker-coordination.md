# Worker coordination failures

A failed worker lease renewal stops that owner permanently. Readiness becomes
unavailable, active work is fenced, and the process exits with a failure status.
A supervisor can start a new owner after shutdown; the old owner cannot resume.

The CLI emits a bounded diagnostic before its usual terminal error:

```text
KELVO_COORDINATION {"stage":"worker_renew_read","reason":"deadline_exceeded"}
Worker coordination lease lost
```

| Reason | Check |
| --- | --- |
| `deadline_exceeded`, `timeout` | Broker latency, CPU throttling and connectivity during the renewal window. |
| `authorization`, `permission_denied` | Broker credentials and account permissions. |
| `tls_verification`, `tls_protocol` | CA, certificate names and TLS configuration. |
| `owner_conflict`, `lease_expired`, `revision_conflict` | Competing worker identities or a lease that was not renewed in time. |
| `connection_closed`, `no_servers`, `connection_refused`, `connection_reset` | Broker availability and network reachability. |
| `invalid_lease`, `missing_lease`, `api_error`, `other` | Broker health and retained worker metadata; preserve the evidence. |

Stages identify connection setup, lease claim or renewal, and the failing read
or write. Diagnostics contain no broker messages, addresses, identities or credentials.
A timed-out write may have committed. Diagnostics never authorize a retry or
prove that a database mutation did not run.

Run the optional outage test against a **fresh, isolated** cluster fixture:

```sh
KELVO_TEST_COORDINATION_OUTAGE=1 go test ./internal/cluster \
  -run '^TestNATSWorkerOutageFencesOwnerAndRecoversOnlyWithNewOwner$' \
  -count=1 -timeout=60s
```

Use the `KELVO_TEST_NATS_*` variables from `scripts/cluster_fixture.py`. Run it
separately from other store tests. It interrupts only its own forwarding socket,
checks permanent fencing and a new owner's recovery, and never queries a customer database.
