# Tenant container deployment example

This example distributes independent queries across tenant-bound worker nodes. Tenant A has two nodes; tenant B has one. Each node runs disposable query subprocesses inside its own container. A query is executed by one node; this does not distribute one SQL plan across the cluster.

These files are an operator starting point, not a production certification. The limits below are an explicit small acceptance profile, not a sizing recommendation. Production acceptance still needs the actual sources, tenant concurrency, failure recovery, workload sizes, network policy and container metrics.

## Host and service prerequisites

- Linux with the Landlock ABI required by `sandbox/launcher.c`, enabled for unprivileged processes, and a container seccomp profile allowing the Landlock calls. The launcher checks support and fails closed. A container image cannot upgrade the host kernel. Do not disable the sandbox, use `--privileged`, or set `seccomp=unconfined` to bypass a failed startup probe.
- Docker Engine with cgroup CPU/memory/PID enforcement and Docker Compose v2. Avoid rootless configurations that lack the configured resource controls.
- A TLS-enabled, persistent NATS JetStream cluster. The sample policies use three replicas, so at least three healthy JetStream members are required. Broker deployment, storage backups and quorum failure handling belong to the operator.
- A different NATS account for each tenant, with no cross-tenant imports/exports and no application access to the system account. Provision separate gateway and worker users within each account. Kelvo reuses its stream/bucket names within accounts, so subject prefixes alone are not the isolation boundary. Apply storage/connection quotas to each account.
- Allow at least four streams per tenant account: the existing metadata, job and dispatch streams plus the bounded acceleration refresh stream. `cluster-init` provisions the latter; existing installations must increase quotas before rerunning bootstrap.
- Source database credentials with database-enforced read-only access. Every node has only its tenant's catalog, credentials and mounted data. A connection identifier is resolved within that catalog.

Build the image on the designated Linux build host:

```sh
docker build --platform linux/amd64 --build-arg VERSION=dev -t kelvo-go:dev .
```

For the optional Linux amd64 ClickHouse federation bridge, add `--build-arg DUCKBRIDGE=1`. See [its build and operational boundaries](../docs/federation.md).

The build includes the Go executable and the native pre-exec launcher. The runtime uses UID/GID `65532`, has no build toolchain and installs only runtime libraries, CA roots and timezone data. Build context rules omit private configs, certificates, artifacts and repository history. For a released deployment, publish a reviewed image and set `KELVO_IMAGE` to its immutable digest.

## Network boundary

Create separate internal networks once, using names not shared with unrelated services:

```sh
docker network create --internal kelvo-tenant-a
docker network create --internal kelvo-tenant-b
```

The Compose file consumes these as external networks and does not create an unrestricted tenant bridge. Each worker joins only its tenant network. Worker ports are not published. The gateway joins both tenant networks plus an ingress network; IP forwarding is disabled in its namespace. Only the gateway's TLS API is published, on loopback by default.

Attach the provisioned broker endpoints to each internal network under the DNS names used in the YAML, with certificates matching those names. The examples use `nats:4222`; adjust them to the real endpoints. All discovered NATS cluster endpoints must also be reachable on that tenant's network.

Attach only the matching tenant's source services to its network. Remote databases need an operator-controlled egress proxy or network policy that permits only approved database destinations and DNS. Do not solve remote access by attaching workers to the ingress network, host networking, or a shared unrestricted bridge. Landlock does not restrict destination IP addresses; this network boundary is a separate required control.

## Private configuration and identities

Keep the live configuration outside the checkout. Copy the YAML files from `examples/` to an absolute private directory and adjust the sources, endpoints and budgets. Set these non-secret Compose inputs in the invoking environment:

```text
KELVO_PRIVATE_DIR=/absolute/private/kelvo
KELVO_TENANT_A_DATA_DIR=/absolute/tenant-a/data
KELVO_TENANT_B_DATA_DIR=/absolute/tenant-b/data
KELVO_EXTENSION_DIR=/absolute/approved/duckdb-extensions
KELVO_IMAGE=kelvo-go:dev
```

The sample CSV source expects `sales.csv` in each tenant's own data directory. Remove unused database entries from the catalogs. Mount approved `postgres_scanner.duckdb_extension` and `mysql_scanner.duckdb_extension` binaries that match the pinned DuckDB version/platform. Verify their provenance and checksums before deployment. Runtime auto-installation is disabled; the image does not fetch extensions.

The private directory must contain:

| File or directory | Purpose |
|---|---|
| `bootstrap.yml`, `bootstrap.env` | Provisioning policy and separate admin credentials; mounted only for initialization |
| `gateway.yml` | Tenant policies, node endpoints and secret environment references |
| `worker-a1.yml`, `worker-a2.yml`, `worker-b1.yml` | One fixed tenant and node identity each |
| `tenant-a-catalog.yml`, `tenant-b-catalog.yml` | Separate source registries |
| `gateway.env` | Tenant API tokens and the gateway's NATS passwords |
| `tenant-a.env`, `tenant-b.env` | That tenant's worker NATS password and source credentials |
| `gateway-tls/` | `api.crt`, `api.key`, `client.crt`, `client.key`, `worker-ca.crt`, `nats-ca.crt` |
| `worker-a1-tls/`, `worker-a2-tls/`, `worker-b1-tls/` | Each node's `server.crt`, `server.key`, `gateway-ca.crt`, `nats-ca.crt` |

`gateway.env` supplies `KELVO_TENANT_A_TOKEN`, `KELVO_TENANT_B_TOKEN`, `KELVO_TENANT_A_NATS_PASSWORD` and `KELVO_TENANT_B_NATS_PASSWORD`. Use independent random API tokens and NATS passwords of at least 32 characters; NATS passwords must match their provisioned users. Tenant worker env files supply their own NATS password under the same configured variable name, and only the source credentials needed by their catalog.

New source environment references use `KELVO_SOURCE_*`, such as `KELVO_SOURCE_POSTGRES_DSN`. The worker forwards only registered source references into the query subprocess. Broker passwords, API tokens and TLS credentials are not part of that allowed namespace. Do not name control-plane secrets `KELVO_SOURCE_*`.

Files mounted into a service must be readable by container UID `65532`. Protect private keys with mode `0400` or `0600` and private directories with `0700`, owned by that UID. Protect environment files on the host with mode `0600`. Do not print a resolved Compose configuration containing loaded environment files; use `docker compose config --quiet` for validation.

The gateway's API certificate needs the DNS names used by clients. Its worker-client certificate needs client-auth usage and URI SAN `spiffe://kelvo/gateway`. Each node certificate needs server-auth usage, its service DNS SAN, and its exact URI SAN:

```text
worker-a1: spiffe://kelvo/tenant/tenant-a/worker/a1
worker-a2: spiffe://kelvo/tenant/tenant-a/worker/a2
worker-b1: spiffe://kelvo/tenant/tenant-b/worker/b1
```

Use the appropriate CA trust bundles in the mounted paths. Worker mTLS validates both certificate trust and the expected URI identity. Never use an insecure TLS flag or a shared worker certificate across tenants.

Custom source TLS CA/client-key files, PostgreSQL service files and password files outside the runtime CA roots are not currently added to the query sandbox's path allowlist. Such connections fail closed. Use source configurations compatible with the allowed system roots until explicit per-source credential-file provisioning is implemented; do not broaden the sandbox to the whole private directory.

## Initialize and start

Provision the broker accounts and TLS identities before starting Kelvo. Every copy of a tenant policy must match, including node capacities and query limits. The initializer records that policy in the tenant's account; a mismatch is rejected. Use the separate bootstrap files with provisioner usernames and `KELVO_TENANT_A_BOOTSTRAP_PASSWORD` / `KELVO_TENANT_B_BOOTSTRAP_PASSWORD`. Running service credentials must not have stream creation/update permissions.

```sh
docker compose -f deploy/compose.yml config --quiet
docker compose -f deploy/compose.yml --profile operations run --rm cluster-init
docker compose -f deploy/compose.yml up -d gateway worker-a1 worker-a2 worker-b1
docker compose -f deploy/compose.yml ps
```

Node startup performs a sandboxed query probe. A failed probe must be investigated; repeated restart failures are not a healthy deployment. The example retries a failed service only three times. Review logs without exposing private environment files or source DSNs.

The launcher permits its own `/proc/self/cgroup` membership file and the exact numeric CPU/memory limit files needed by DuckDB initialization. It does not allow a `/proc` or cgroup directory, process environments, process maps, or cgroup control writes. Assign the process to its container/cgroup before launching it. The acceptance CSV query uses a 256 MiB DuckDB budget inside a 512 MiB container; the same query exhausted a 64 MiB DuckDB budget because of the CSV reader's allocations. These configured budgets are not measured idle RSS or a production sizing recommendation.

The reference worker profile caps the whole tenant-node container at 2 CPUs, 1536 MiB with no extra swap, and 256 PIDs. Its `/tmp` is a 768 MiB noexec/nosuid/nodev tmpfs. Root and mounted sources are read-only; all Linux capabilities are dropped; no-new-privileges is enabled. Individual query limits are 512 MiB of DuckDB-managed memory, two execution threads, 256 MiB of engine temporary data, 30 seconds, one million returned rows and 128 MiB of returned Arrow data. Each sample node admits one query at a time.

Tmpfs consumes memory charged to the container. These budgets do not mean DuckDB RSS is capped at 512 MiB, or that its result is produced incrementally: the pinned Go driver materializes execution before exposing Arrow batches. For disk-heavy joins/sorts, replace tmpfs only with a tenant-dedicated scratch filesystem carrying an enforced host/project quota and noexec/nosuid/nodev mount flags. Engine spill settings alone are not a host disk quota. Increase concurrency only after accounting for all simultaneous native engines, Arrow buffers and temporary storage.

Container limits bound a tenant node as a whole. They do not create a separate cgroup for each query. Multiple nodes provide capacity for independent queries; one larger container is not a substitute for measured admission limits or failure-domain redundancy.

## Acceptance before a deployment claim

Verify with real container processes and actual source databases:

1. Tenant A queries are assigned to both `a1` and `a2`; a query runs on only one of them. Tenant B uses only `b1`.
2. Tenant tokens cannot read, fetch, cancel or address the other tenant's query IDs. A mismatched worker TLS identity is rejected.
3. A query cannot read an unrelated file, parent/control-plane credentials or `/proc`; a selected network source does not widen filesystem access.
4. Workers cannot contact other tenant networks or unapproved external destinations. Broker credentials cannot access another tenant's account or JetStream state.
5. Cancellation and node loss produce explicit terminal failures. Incomplete Arrow output is not accepted as a successful result. Assigned work is not silently re-executed after worker loss.
6. CPU/memory/PID/tmp limits are present in live container inspection. Measure peak container memory, native-source memory, first-row latency, sustained throughput and behavior at the configured concurrency.
7. Broker failover, gateway restart, policy mismatch, full storage and expired jobs behave as documented. Validate the chosen storage/backup recovery process separately.

Current evidence belongs in the repository's validation record. Do not infer production HA, isolation or throughput from image construction, config validation or unit tests alone.

References: [Landlock](https://docs.kernel.org/userspace-api/landlock.html), [Docker Compose service controls](https://docs.docker.com/reference/compose-file/services/), [NATS account isolation](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/accounts), [DuckDB untrusted SQL guidance](https://duckdb.org/docs/stable/operations_manual/securing_duckdb/overview).

For optional persistent dataset acceleration, follow the [acceleration guide](../docs/acceleration.md) and apply [acceleration.compose.yml](acceleration.compose.yml). It adds separate writable tenant snapshot mounts. Cluster refresh consumers also require the additional NATS permissions listed in that guide; source query subprocesses still receive read-only access only to selected snapshot files.

The optional [native federation bridge](docs/federation.md) compiles a version-pinned
C++ Arrow shim and a scoped native-connection driver accessor. It registers only
operator-selected tables in a disposable database; pointer-bearing views must
never persist. Required source predicates are applied exactly or rejected, and
scan admission is bounded per query. This native code remains inside the tenant
worker boundary and does not replace filesystem, network or cgroup controls.
