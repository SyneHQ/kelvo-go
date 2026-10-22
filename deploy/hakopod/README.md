# Kelvo on Hakopod (single node `hakopod-server`, TEST tenant)

Layout, validated with `hakopod validate --dir deploy/hakopod`:

| Path | Purpose |
|---|---|
| `network.toml` | Virtual network `kelvo-net`, segment `gateway`, granted to `syne` and `kelvo` |
| `node-profiles.toml` | Operator-only binding `kelvo-node`; not an application file |
| `kelvo/hakopod.toml` | Application: `nats`, `cluster-init` (job), `gateway`, `node`. Gateway, node and bootstrap YAML are inline `content` blocks (TEST tenant `kelvo-test`, worker `n1`, one NATS member so `replicas: 1`) |
| `node-entrypoint.sh` | Root bootstrap of the `hakopod-node` image: Landlock/cgroup v2 checks, delegated cgroup subtree, fail-closed egress rules, drop to uid 65532 |
| `node-entrypoint_test.sh` | Dry-run test of the entrypoint (`sh deploy/hakopod/node-entrypoint_test.sh`) |

Images come from `.github/workflows/hakopod-image.yml` (push to `cargo`, tags `v*`): `ghcr.io/synehq/kelvo-go` (gateway, `runtime` target) and `ghcr.io/synehq/kelvo-go-node` (`hakopod-node` target). The job summary prints both digests. Put them in `kelvo/hakopod.toml`, replacing the all-zero placeholder digests (3 occurrences of the kelvo images, 1 for NATS: pin a `nats:2.15.x` digest yourself).

## Requirements

Hakopod 0.1.0-alpha.64 or later (node profiles with `run_as_root`, and sidecars). The `node` service relies on both: the binding in `node-profiles.toml` lets the node container start as root (to delegate its cgroup subtree and install the egress rules before dropping to 65532), and grants the `tailscale` sidecar `NET_ADMIN`, `NET_RAW`, `/dev/net/tun` and root.

## Runbook

### 1. Operator steps on the server

1. Check the kernel (docs/node-profiles.md):
   ```sh
   cat /sys/kernel/security/lsm            # must include landlock
   stat -fc %T /sys/fs/cgroup              # cgroup2fs
   cat /sys/fs/cgroup/cgroup.controllers   # cpu memory pids
   ```
2. Edit `node-profiles.toml` (project, environment), install it as `/etc/hakopod/node-profiles.toml` owned by root or the server user, not group/other writable.
3. Set `HAKOPOD_NODE_PROFILES_FILE=/etc/hakopod/node-profiles.toml` (or `[node_profiles] file`) and restart `hakopod-server`.
4. Keep `kelvo` in an application of its own: its namespace becomes pod-security `privileged`. The node container runs privileged on `hakopod-server`, which also runs the control plane and other applications: a Kelvo escape would reach all of them. Kelvo's Landlock and cgroup containment are the boundary for query children.
5. Create the virtual network from `network.toml` (Networks, Create network, import). The `syne` application must also declare a network with `virtual_network = "kelvo-net"` and `segment = "gateway"` on db-api.

### 2. Fixed egress through Tailscale

The `tailscale` sidecar in `kelvo/hakopod.toml` joins the tailnet and uses the Oracle VM as its exit node. The route change happens in the pod's network namespace only; the host and other applications keep their own routes.

1. On the Oracle VM: `sudo tailscale set --advertise-exit-node`, enable `net.ipv4.ip_forward=1` (and IPv6 forwarding if used), and approve the exit node in the Tailscale admin console (or an `autoApprovers` ACL).
2. Create a Tailscale auth key (reusable off, ephemeral on, tagged, for example `tag:kelvo`), and allow `tag:kelvo` to use the exit node in the ACL.
3. In Hakopod, create the application secret `tailscale-authkey` with that key. Never commit it.
4. Replace `REPLACE_ORACLE_EXIT_NODE` in `TS_EXTRA_ARGS` with the exit node's tailnet IP or name, and pin the `tailscale/tailscale` image digest you have reviewed.
5. `KELVO_ALLOW_CIDRS` in the node's env lists the cluster CIDRs that stay direct (k3s defaults `10.42.0.0/16` pods, `10.43.0.0/16` services). Check them on the server before deploying.

Fail-closed: the node entrypoint installs iptables and ip6tables rules for uid 65532 (the node and its query children): loopback, `KELVO_ALLOW_CIDRS` and `tailscale0` only, everything else rejected. If the sidecar or the exit node is down, database connections fail instead of leaving from the server's own address.

### 3. Kelvo TLS material

`scripts/cluster_fixture.py` only makes 2-day loopback certificates for tests; do not use it here. Create a private CA and leaf certificates with openssl in a private directory (never committed). Every leaf needs `extendedKeyUsage=serverAuth,clientAuth`; the CA needs `basicConstraints=critical,CA:true` and `keyUsage=critical,keyCertSign,cRLSign`.

| Secret (Hakopod name) | Content | SAN |
|---|---|---|
| `kelvo-ca-crt` | CA certificate; used for NATS, worker and gateway trust | |
| `kelvo-nats-crt` / `kelvo-nats-key` | NATS server | `DNS:nats` |
| `kelvo-api-crt` / `kelvo-api-key` | Gateway API | `DNS:gateway` plus client-facing names |
| `kelvo-gateway-client-crt` / `-key` | Gateway worker client | `URI:spiffe://kelvo/gateway` |
| `kelvo-node-crt` / `kelvo-node-key` | Node server | `DNS:node`, `URI:spiffe://kelvo/tenant/kelvo-test/worker/n1` |

Example for the node (repeat per row with its own SAN):

```sh
openssl req -new -newkey rsa:2048 -nodes -keyout node.key -out node.csr -subj /CN=kelvo-node
printf 'subjectAltName=DNS:node,URI:spiffe://kelvo/tenant/kelvo-test/worker/n1\nextendedKeyUsage=serverAuth,clientAuth\n' > node.ext
openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out node.crt -days 90 -extfile node.ext
```

The worker ID `n1` and tenant `kelvo-test` must match the URI SAN exactly. Create these as Hakopod secrets in the UI (one PEM per secret), along with: `kelvo-api-token`, `kelvo-nats-provisioner-password`, `kelvo-nats-gateway-password`, `kelvo-nats-worker-password` (independent random values, 32+ characters), and `kelvo-source-postgres-dsn` (read-only grant on the test database).

### 4. Deploy

1. `hakopod validate --dir deploy/hakopod`, then plan the network, then the application, and review the plan. Do not skip the review.
2. Deploy. Order is enforced by `depends_on`: `nats`, then `cluster-init` (job, `retries = 1`, must be idempotent for an unchanged policy; it re-runs on every new revision), then `gateway` and `node`.
3. Check the node logs: `cgroup root ... (containment.root must be .../jobs)`. If that root differs from `/sys/fs/cgroup/jobs` in the inline `node.yml`, edit `containment.root` and redeploy. `kelvo node --check-config` can be run with `args` for a dry check.

### 5. Verify

1. Gateway `/ready` returns 200 and the node reports ready (sandbox probe, identity claim).
2. After section 2 exists: from inside the node pod, run as uid 65532 a fetch of an IP echo service and require the Oracle VM's public IP. Also check the database sees the Oracle IP.
3. Fail-closed: stop tailscaled in the sidecar (`hakopod terminal kelvo --service node --container tailscale`); new DB connections from the node must fail (REJECT), not leave from the host IP.
4. Canary: point db-api (application `syne`, joined to `kelvo-net/gateway`) at `https://gateway:8443` using the test token and run one query on the `pg` source. Confirm `from_applications = ["syne/db-api"]` blocks other callers.

### 6. Rollback

Roll the application back to the previous revision in Hakopod (it creates a new revision; data volumes `nats-data` and `kelvo-state` are retained). Or suspend the `node` service. Remove `network.toml` grants and the node profile binding only after detaching services and deploying that change. A pending partial deployment of `cluster-init` does not undo JetStream streams.

## Unverified

- Nothing was run on a cluster: pod start, the node profile and sidecar grants, the entrypoint on a real kernel, cgroup delegation under Hakopod, the Tailscale sidecar with `KUBERNETES_SERVICE_HOST` hidden, and the egress rules.
- The entrypoint is tested only with its dry-run mode and shellcheck; the actual cgroup/chown/setpriv calls and that `node` can manage `jobs` are untested.
- Docker images were not built; the `hakopod-node` target and the workflow have not run.
- The hakopod validator checks schema only. Single-file secret mount modes with `fs_group = 65532` and `containment.root` are assumptions.
- Egress via Tailscale, fail-closed behaviour and the Oracle IP checks are design only.
- NATS config (`accounts`, env-interpolated passwords), `cluster-init` against a single member, and `kelvo node --check-config` of the inline YAML were not run.
