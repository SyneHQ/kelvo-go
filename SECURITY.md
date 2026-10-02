# Security

Kelvo Go is a developer preview. `serve` has one configured trust domain. Cluster mode adds tenant authentication, durable admission, tenant-bound worker pools, TLS 1.3 and a native Linux filesystem sandbox. It requires deployment-enforced tenant container/network/resource boundaries; process isolation and SQL filters alone are insufficient.

Use least-privilege database accounts, dedicated tenant worker/container identities, restricted mounts, outbound network policy and CPU/memory/PID/disk quotas. SQL can invoke functions beyond ordinary table reads; database permissions and network restrictions remain necessary. Do not attach unrelated tenants to one worker pool or `serve` catalog. See [deployment controls](deploy/README.md).

Cluster worker startup fails if Landlock ABI 3 or newer is unavailable. The native launcher applies restrictions before the Go runtime creates threads. It allows selected source files, approved extensions, public runtime libraries/CA roots and that query's scratch directory; it denies other file contents and dangerous syscalls. Landlock does not impose network destination rules or process-memory quotas. Keep secrets out of publicly allowed runtime directories.

Each tenant must have a separate NATS account without cross-account imports/exports, restricted users and storage limits. Gateway and node credentials are trusted control-plane credentials for that tenant. NATS holds SQL, parameters and job metadata, so broker storage/backup access and encryption belong to the security boundary. Arrow results and registered database credentials do not enter NATS. API tokens authorize the whole tenant catalog; per-user row/column policies and external identity/KMS integration remain future work.

Source secrets are resolved from explicitly configured environment-variable names in the allowed source namespace. Legacy PostgreSQL/MySQL extension attachments use temporary redacted DuckDB secrets instead of embedding credentials in metadata paths; custom Go adapters resolve only their selected source credentials. Custom credential-file mounts require explicit future support; the sandbox fails closed rather than allowing all private configuration paths. Never include secrets or customer data in issues.

Accelerated datasets are durable copies of source data. Give each tenant a separate private snapshot volume or object-storage namespace and matching catalog identity. Only trusted refresh processes may publish snapshots; query subprocesses receive selected immutable files or short-lived loopback range capabilities. Object readers and publishers use separate explicitly named environment credentials. Provider redirects, full-download fallbacks, unselected keys and changed object versions are rejected. Cloud credentials remain in the trusted parent, and no object listing or deletion API is exposed. Object query capabilities are private to the tenant process/network boundary; restrict same-tenant process visibility and network access. See [object storage](docs/object-storage.md). Increment the required `authorization_version` when source credentials or grants change, because a copied dataset cannot inherit database revocations automatically. Apply encryption, storage quotas, backup access controls and retired-copy deletion policies at deployment. NATS refresh envelopes contain dataset identifiers and fingerprints only. See [acceleration boundaries](docs/acceleration.md).

Use GitHub private vulnerability reporting on this repository for sensitive reports. Supported-version and response-time guarantees have not yet been established for this preview.

The optional [native federation bridge](docs/federation.md) compiles a version-pinned
C++ Arrow shim and a scoped native-connection driver accessor. It registers only
operator-selected tables in a disposable database; pointer-bearing views must
never persist. Required source predicates are applied exactly or rejected, and
scan admission is bounded per query. This native code remains inside the tenant
worker boundary and does not replace filesystem, network or cgroup controls.
Its ClickHouse, PostgreSQL and MySQL adapters perform source I/O in Go while
DuckDB external access remains disabled. PostgreSQL/MySQL configurations without
custom table registrations retain the legacy signed-extension behavior. Native
relational credentials require verified TLS and operator-provisioned system CA
trust; caller-selected trust files and insecure TLS modes are not accepted.
