# Worker coordination failures

A failed worker lease renewal stops that owner permanently. Readiness becomes
unavailable, active work is fenced, and the process exits with a failure status.
A supervisor can start a new owner after shutdown; the old owner cannot resume.

The gateway's `/ready` also requires at least one healthy configured worker for
every tenant, plus healthy reconciliation, authentication and audit state.
It checks workers over their verified mTLS connections in one background sweep,
starting every five seconds or after the previous sweep finishes. At most eight
probes run at once, each with a two-second deadline. Public
readiness requests read the cache only and reveal no tenant or worker identities.
For up to eight configured endpoints, success expires seven seconds after the
probe starts. Larger fleets use `W = ceil(endpoints / 8) * 2s` and a freshness
bound of `max(5s, W) + W` (32 seconds for 64 endpoints). This covers a worker
probed early, then late in consecutive sweeps. Larger fleets trade slower failure
detection for bounded probe overhead. Certificate expiry can shorten this bound;
stalled sweeps still expire. Startup, drain and shutdown fail closed.
This is a **health snapshot, not a capacity
reservation**: a subsequent query can still queue or be rejected.

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
