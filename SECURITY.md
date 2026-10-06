# Security

Kelvo is a developer preview. Report vulnerabilities through [GitHub private reporting](https://github.com/SyneHQ/kelvo-go/security/advisories/new). Supported-version and response-time guarantees are not yet established.

## Deploy with these boundaries

1. Give each tenant its own worker pool, container identity, snapshot namespace and NATS account. Do not mix unrelated tenants in one `serve` catalog.
2. Use read-only accounts for analytics and separate, scoped permissions for authorized writes. Restrict mounts and outbound network access; SQL functions can access more than tables.
3. Enforce host/container CPU, memory, PID and disk limits. Admission reservations and Arrow output limits do not cap process memory.
4. Protect broker storage and backups: NATS contains SQL, parameters and job metadata. It excludes database credentials and Arrow results.

Cluster mode requires TLS 1.3 and Linux Landlock ABI 3+. The native launcher installs filesystem/syscall restrictions before Go starts threads and fails startup without the required support. Landlock does not restrict network destinations or memory use. Keep secrets out of allowed runtime/library directories.

See the [deployment checklist](deploy/README.md) and [cluster model](docs/cluster.md).

## Credentials and authorization

| Control | Requirement |
| --- | --- |
| Principal access | [User/service keys](docs/principal-access.md) bind source grants and handle ownership. [Row/column policies](docs/row-column-access.md) cover registered callback tables; restricted native SQL and snapshots fail closed. External identity and enrollment remain open. |
| NATS | Separate accounts, no cross-account imports/exports, restricted users and storage limits. Gateway/node credentials are trusted control-plane identities. |
| Source secrets | Catalogs hold environment-variable references. The parent resolves only selected secrets and forwards values to the selected child. |
| Cloud secrets | Optional [AWS, Azure and GCP mappings](docs/cloud-secrets.md) keep expiring provider credentials in the trusted parent. Live IAM/rotation acceptance is unrun. |
| Audit | Optional [durable local receipts](docs/durable-audit.md) protect specified cluster operations. No SQL/row retention, replication or cross-service atomicity guarantee. |
| Credential files | Optional [private files](docs/operations.md#file-based-source-credential-rotation) rotate credentials for new processes. Paths stay in the parent; file errors never fall back to environment values. |
| API keys | Optional [revisioned key files](docs/gateway-key-rotation.md) fail closed. Update every replica and restart floor; revocation is not globally atomic. |
| TLS | [Identity](docs/tls-identity-rotation.md) and [trust](docs/tls-trust-rotation.md) rotation require operator propagation; trust policy also needs a configured restart floor. |

Native relational connectors require verified TLS and operator-provisioned CA trust. Unsupported private wallets/CA paths are rejected. Legacy PostgreSQL/MySQL attachments use temporary redacted DuckDB secrets.

## Saved-connection operations

[Database operations](docs/database-operations.md) require separate signed authority;
ordinary query grants cannot authorize writes. A leased worker fetches current
credentials over private mTLS and sends them to its verified adapter through stdin.
Credentials stay out of broker records, argv and child environment. Unknown write
outcomes require reconciliation; status reads never replay source operations.

## Protect accelerated copies

- Keep tenant snapshots private. Apply encryption, storage quotas, backup access controls and retired-copy deletion policies.
- Only trusted refresh processes publish. Query children receive selected immutable files or short-lived range capabilities; cloud credentials stay in the parent.
- Use separate object reader/writer credentials. Redirects, changed versions, unselected keys and full-download fallbacks are rejected.
- Restrict process visibility and network access within each tenant. Loopback capabilities rely on that boundary.
- Increment `authorization_version` after source grants or credentials change. A copied snapshot cannot inherit database revocations automatically.

See [acceleration](docs/acceleration.md) and [object storage](docs/object-storage.md).

## Native federation

The opt-in [Go/C++ bridge](docs/federation.md) registers operator-selected tables in disposable DuckDB instances. Pointer-bearing views must never persist. Required predicates are applied exactly or rejected; scan admission is bounded.

Bridge adapters perform I/O in Go with DuckDB external access disabled. PostgreSQL/MySQL configurations without custom registrations retain signed-extension behavior. Native code stays inside the worker boundary and still needs deployment isolation.

Never put credentials, DSNs or customer data in source, logs, fixtures or public reports.
