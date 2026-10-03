# Tenant container deployment example

The sample runs tenant A on two nodes and tenant B on one. Each query stays on one node. These are acceptance defaults, not production sizing or certification.

## Host and service prerequisites

1. Use Linux with unprivileged Landlock ABI 3+ and a container seccomp profile allowing its calls. Startup fails closed; do not bypass it with privileged/unconfined containers.
2. Use Docker/Compose v2 with enforced CPU, memory and PID limits. Check rootless configurations actually enforce them.
3. Provision TLS-enabled persistent JetStream. The sample requires **three healthy members** for three replicas.
4. Give each tenant a separate NATS account, no cross-account imports/exports or application access to the system account, separate gateway/worker users and account quotas.
5. Allow **five streams per tenant account**, or **six with source quotas**. Update quotas before rerunning bootstrap.
6. Provide read-only database grants and only the matching tenant's catalog, credentials and data.

Build on the designated Linux host:

```sh
docker build --platform linux/amd64 --build-arg VERSION=dev -t kelvo-go:dev .
```

Add `--build-arg DUCKBRIDGE=1` for the [optional federation bridge](../docs/federation.md). The image includes the native launcher, runs as UID/GID 65532 and has no build toolchain. For releases, set `KELVO_IMAGE` to a reviewed immutable digest.

## Network boundary

Create isolated tenant networks:

```sh
docker network create --internal kelvo-tenant-a
docker network create --internal kelvo-tenant-b
```

Workers join only their tenant network and publish no ports. The gateway joins tenant and ingress networks with forwarding disabled; its TLS API publishes on loopback by default.

Attach matching broker/source endpoints with certificate-valid DNS names; every discovered NATS member must be reachable. Remote databases need approved egress/DNS policy or a controlled proxy. Never attach workers to ingress, host networking or an unrestricted shared bridge to gain access. Landlock does not enforce destination IP policy.

## Private configuration and identities

Copy example YAML into a private directory outside the checkout. Set these non-secret Compose inputs:

```text
KELVO_PRIVATE_DIR=/absolute/private/kelvo
KELVO_TENANT_A_DATA_DIR=/absolute/tenant-a/data
KELVO_TENANT_B_DATA_DIR=/absolute/tenant-b/data
KELVO_EXTENSION_DIR=/absolute/approved/duckdb-extensions
KELVO_IMAGE=kelvo-go:dev
```

Keep `sales.csv` in each tenant's data directory, or replace/remove that source. Provision signed, version/platform-matched extensions; runtime downloads are disabled.

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

- Use independent random API tokens and NATS passwords of at least 32 characters. Match passwords to provisioned users.
- Name source secrets `KELVO_SOURCE_*`; never use that namespace for broker/API/TLS secrets. Only selected source values enter child environments.
- Service files must be readable by UID 65532: private directories 0700, keys 0400/0600. Host environment files use 0600. Validate with `docker compose config --quiet` to avoid printing secrets.
- For [live API-key rotation](../docs/gateway-key-rotation.md), mount the containing directory read-only, remove all tenant `token_env` fields, and update every replica/restart floor.

The API certificate needs client-facing DNS SANs. Gateway worker-client identity requires client-auth EKU and `spiffe://kelvo/gateway`. Worker certificates require server-auth EKU, service DNS SAN and their exact URI:

```text
worker-a1: spiffe://kelvo/tenant/tenant-a/worker/a1
worker-a2: spiffe://kelvo/tenant/tenant-a/worker/a2
worker-b1: spiffe://kelvo/tenant/tenant-b/worker/b1
```

Use matching CA bundles; never disable verification or share identities across tenants. Custom source wallets/CA/key files are not automatically granted sandbox access. Use supported system trust; never mount the entire private directory into the query sandbox.

## Initialize and start

1. Provision broker accounts and TLS identities.
2. Match each tenant policy across bootstrap, gateways and nodes, including capacities and limits.
3. Initialize with separate provisioner credentials, then start:

```sh
docker compose -f deploy/compose.yml config --quiet
docker compose -f deploy/compose.yml --profile operations run --rm cluster-init
docker compose -f deploy/compose.yml up -d gateway worker-a1 worker-a2 worker-b1
docker compose -f deploy/compose.yml ps
```

Running credentials must not create/update streams. Investigate failed sandbox startup probes; the example retries only three times.

| Sample worker limit | Value |
| --- | --- |
| Container | 2 CPUs, 1536 MiB, no extra swap, 256 PIDs |
| Scratch | 768 MiB tmpfs, noexec/nosuid/nodev |
| Query | 512 MiB DuckDB memory, 2 threads, 256 MiB temp, 30s |
| Output | 1 million rows, 128 MiB Arrow |
| Concurrency | 1 query per node |

Root/source mounts are read-only, capabilities are dropped, and no-new-privileges is enabled. Tmpfs counts toward container memory. Disk-backed scratch needs its own tenant quota and mount restrictions.

DuckDB materializes before Arrow delivery; its memory setting is not RSS. The acceptance CSV query needed a 256 MiB managed budget inside 512 MiB; a 64 MiB budget failed. Neither is idle memory or production sizing evidence.

This Compose profile limits the whole node, not each query. Size aggregate work and failure domains separately. The launcher exposes only selected cgroup membership/limit files, not process environments or writable controls.

## Acceptance before a deployment claim

1. Verify A uses only `a1/a2`, B only `b1`, and each query has one owner.
2. Reject cross-tenant query access and incorrect mTLS identities.
3. Deny unrelated files, parent secrets and unapproved network destinations.
4. Confirm broker credentials cannot cross account boundaries.
5. Cancel/kill processes: require explicit failures, no silent replay and no successful partial Arrow result.
6. Inspect live CPU/memory/PID/disk limits; measure source and worker memory, latency and concurrency.
7. Test broker loss, gateway restart, full storage, expired jobs and backup recovery.

Record results in [validation](../docs/validation.md). Image builds and unit tests alone do not establish deployment isolation, HA or throughput.

For snapshots, apply [acceleration.compose.yml](acceleration.compose.yml) and the [acceleration permissions](../docs/acceleration.md#tenant-and-cluster-operation). Pre-create separate private tenant volumes.

[Landlock](https://docs.kernel.org/userspace-api/landlock.html) · [Compose controls](https://docs.docker.com/reference/compose-file/services/) · [NATS accounts](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/accounts) · [DuckDB security](https://duckdb.org/docs/stable/operations_manual/securing_duckdb/overview)
