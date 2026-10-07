# Independent application example

This separate Go module uses public Kelvo packages without runtime or database-driver
imports. Its small policy schema belongs to the application.
`authority` serves private resolver callbacks over TLS 1.3 with verified worker
client certificates. The handler verifies the exact signed request, worker URI SAN,
and live gateway lease. Policy is reloaded before credential lookup and checked
again afterward. `exercise` is a trusted backend that signs configured requests
locally; the authority never reads its signing seed. There is no grant-issuing HTTP endpoint.

The example permits native SQL reads and one configured transactional insert.
Callers cannot supply arbitrary SQL, source URLs, or credentials. Other operations,
federation, file, ingestion, watcher, and cleanup callbacks fail closed. Integrate
your own membership store and secret provider, and retain restrictive database privileges.

## Setup

Build Kelvo from the revision pinned in this module; the `v0.1.0-preview.1`
notebook binary predates this integration. See the [deployment guide](../../deploy/README.md).

Provision the cluster, gateway principal, grant trust, worker mTLS identity, database
TLS trust, and private resolver URL separately. Worker identity must be
`spiffe://kelvo/tenant/<cluster_tenant>/worker/<worker_id>`. Use an HTTPS gateway origin;
resolver paths are `/internal/kelvo/resolve` and `/internal/kelvo/resolve-operation`.
Topology and authentication settings do not belong in query bodies.

Copy `app.example.yaml` and `policy.example.yaml` to private `app.yaml` and `policy.yaml`,
then configure paths, identities, database, and SQL. Paths are relative to their containing
file. Apply `fixture.sql` with a database administrator; use restricted identities at runtime.
PostgreSQL DSNs require `sslmode=verify-full`. Set `tls_ca_file` for explicit CA trust
or use operator-configured system trust; hostname and certificate verification remain enabled.

Set the variables named by `public_key_env` (base64 Ed25519 public key) and
`bearer_token_env` (gateway principal token). `signing_seed_file` contains a base64
32-byte Ed25519 seed. Credential descriptors contain `revision` and exactly one of
`dsn_file` or `dsn_env`; seed and DSN files require mode `0600`. Keep secrets and journals private.
For rotation, publish an immutable descriptor and DSN file, then atomically replace
the policy's `credentials_file` and matching `revision`. Incomplete rotations fail closed.

## Run

Use the published dependency pin in `go.mod`, with `GOWORK=off` and no local `replace`:

```sh
GOWORK=off go test ./...
GOWORK=off go build -o authority ./cmd/authority
GOWORK=off go build -o exercise ./cmd/exercise
./authority -config app.yaml -audit-file private/authority-audit.json
```

In another process with the configured environment:

```sh
./exercise -config app.yaml -team team-a -subject alice -connection saved-a \
  -run-id smoke-a-001 -journal private/team-a-journal.json
./exercise -config app.yaml -team team-b -subject bob -connection saved-b \
  -run-id smoke-b-001 -journal private/team-b-journal.json
```

Reports verify mutation commit, operation-result integrity, analytical status and complete
Arrow delivery, and cancellation. Both read paths verify integer `9007199254740993` exactly.
Use `-mode read` for operation and analytical reads; `-mode query` for analytical reads only.
The private journal durably records intent, exact request, stable idempotency key, and
original grant before the single mutation submission. Re-running it uses lookup/status
only. Missing ledger entries, expired grants, or uncertain effects never permit resubmission.
Retain journals for operator reconciliation; use a new run ID and journal only for a new mutation.

## Prove revocation

Retain a policy snapshot only for the exercise process; keep the authority on live policy.
Remove membership or change ownership in the live file, then query using the snapshot.
`-mode query -expect-denied` requires explicit remote `PERMISSION_DENIED`.
For revocation evidence, also require an increased authority `query_policy_denied`
counter and no additional `credential_lookups`. A generic transport failure cannot
prove revocation.
Audit counters contain no identifiers or secrets; policy-load and credential failures are
counted separately from policy denial. Snapshots are adversarial fixtures, never production policy.
End-to-end evidence requires a real gateway, running worker, and database fixture.
