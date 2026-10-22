# Kelvo on Hakopod (single node `hakopod-server`, TEST tenant)

Layout, validated with `hakopod validate --dir deploy/hakopod`:

| Path | Purpose |
|---|---|
| `network.toml` | Virtual network `kelvo-net`, segment `gateway`, granted to `syne` and `kelvo` |
| `node-profiles.toml` | Operator-only binding `kelvo-node`; not an application file |
| `kelvo/hakopod.toml` | Application: `nats`, `cluster-init` (job), `gateway`, `node`. Gateway, node and bootstrap YAML are inline `content` blocks (TEST tenant `kelvo-test`, worker `n1`, one NATS member so `replicas: 1`) |
| `node-entrypoint.sh` | Root bootstrap of the `hakopod-node` image: Landlock/cgroup v2 checks, delegated cgroup subtree, drop to uid 65532 |
| `node-entrypoint_test.sh` | Dry-run test of the entrypoint (`sh deploy/hakopod/node-entrypoint_test.sh`) |

Images come from `.github/workflows/hakopod-image.yml` (push to `cargo`, tags `v*`): `ghcr.io/synehq/kelvo-go` (gateway, `runtime` target) and `ghcr.io/synehq/kelvo-go-node` (`hakopod-node` target). The job summary prints both digests. Put them in `kelvo/hakopod.toml`, replacing the all-zero placeholder digests (3 occurrences of the kelvo images, 1 for NATS: pin a `nats:2.15.x` digest yourself).

## Blocking issue to resolve first

Hakopod pods are created with `runAsNonRoot: true`, uid/gid 10001, and `run_as_user` must be a positive UID (`internal/cluster/deploy.go`, `internal/spec/workloads.go`). The node profile only adds `privileged` and seccomp (`internal/cluster/node_profile.go`). The entrypoint needs root to create and chown the cgroup subtree, and it refuses to run otherwise. As Hakopod stands, the `node` pod will not start (or the entrypoint exits "must start as root"). Hakopod needs a node-profile option that lets the profiled container run as uid 0. This was not tested.

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
4. Keep `kelvo` in an application of its own: its namespace becomes pod-security `privileged`. The node `hakopod-server` should run nothing else important.
5. Create the virtual network from `network.toml` (Networks, Create network, import). The `syne` application must also declare a network with `virtual_network = "kelvo-net"` and `segment = "gateway"` on db-api.

### 2. Fixed egress through Tailscale: pending Hakopod sidecar support

Not implemented yet. The syntax is not final, so `kelvo/hakopod.toml` has no sidecar section, and as written the node's database traffic leaves from the host's own address (network `kelvo-egress`). Do not use real databases until this exists. Design:

- Oracle VM: `tailscale up --advertise-exit-node`, enable IP forwarding, then approve the exit node in the Tailscale admin console.
- Auth key: create a reusable-or-single-use, tagged, ephemeral key in Tailscale. Store it as a Hakopod secret (for example `kelvo-ts-authkey`) in the UI. Never commit it.
- A Tailscale sidecar in the node pod (the pod's network namespace is shared with the Kelvo container), kernel mode, state on an emptyDir/tmpfs (no persistent state), started with `tailscale up --auth-key file:... --exit-node=<oracle> --exit-node-allow-lan-access --hostname=kelvo-node --accept-dns=false`. `--exit-node-allow-lan-access` keeps pod and service CIDRs (NATS, gateway, resolver) reachable directly. The route change applies to the pod only, never the host.
- The sidecar waits until the exit node is active (bounded wait, fail the pod otherwise) and installs fail-closed rules in the shared netns, for uid 65532 only:
  ```sh
  iptables  -A OUTPUT -m owner --uid-owner 65532 -o lo -j ACCEPT
  iptables  -A OUTPUT -m owner --uid-owner 65532 -d <pod/service CIDRs> -j ACCEPT
  iptables  -A OUTPUT -m owner --uid-owner 65532 -o tailscale0 -j ACCEPT
  iptables  -A OUTPUT -m owner --uid-owner 65532 -j REJECT
  ip6tables -A OUTPUT -m owner --uid-owner 65532 -j REJECT   # plus a loopback ACCEPT first
  ```
  The sidecar needs NET_ADMIN and /dev/net/tun; this is Hakopod's to define. The rules live in the sidecar, not in the Kelvo image.
- Hakopod's private-destination egress blocks apply independently; check that Oracle and Tailscale control-plane traffic is permitted for the sidecar.

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
3. Fail-closed (after section 2): stop tailscaled in the sidecar; new DB connections from the node must fail (REJECT), not leave from the host IP.
4. Canary: point db-api (application `syne`, joined to `kelvo-net/gateway`) at `https://gateway:8443` using the test token and run one query on the `pg` source. Confirm `from_applications = ["syne/db-api"]` blocks other callers.

### 6. Rollback

Roll the application back to the previous revision in Hakopod (it creates a new revision; data volumes `nats-data` and `kelvo-state` are retained). Or suspend the `node` service. Remove `network.toml` grants and the node profile binding only after detaching services and deploying that change. A pending partial deployment of `cluster-init` does not undo JetStream streams.

## Unverified

- Nothing was run on a cluster: pod start, the node profile, the entrypoint on a real kernel, cgroup delegation under Hakopod, and the root-user blocker above.
- The entrypoint is tested only with its dry-run mode and shellcheck; the actual cgroup/chown/setpriv calls and that `node` can manage `jobs` are untested.
- Docker images were not built; the `hakopod-node` target and the workflow have not run.
- The hakopod validator checks schema only. Single-file secret mount modes with `fs_group = 65532` and `containment.root` are assumptions.
- Egress via Tailscale, fail-closed behaviour and the Oracle IP checks are design only.
- NATS config (`accounts`, env-interpolated passwords), `cluster-init` against a single member, and `kelvo node --check-config` of the inline YAML were not run.
