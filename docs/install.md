# Install Kelvo

Three ways to run open-source Kelvo, from quickest to closest to production:

| Path | What you get | Time |
| --- | --- | --- |
| [Run with Docker](#run-with-docker) | One standalone `kelvo serve`, sample CSV, API key from an env file | ~15 min, most of it the first image build |
| [Install with Helm](#install-with-helm) | Cluster mode: gateway, tenant-bound node(s), NATS JetStream | ~30 min |
| [Release binaries](#release-binaries) | `kelvo` and `kelvo-landlock` for linux/amd64 and linux/arm64 | — |

All paths need Linux with Landlock ABI 3+ (kernel 6.2+ with `landlock` in
`/sys/kernel/security/lsm`). Kelvo fails closed where the sandbox is
unavailable. This is a developer preview: read [production status](production-status.md)
before choosing a deployment.

## Images

Tagged releases (`v*`) publish multi-arch (linux/amd64, linux/arm64) images:

| Image | Dockerfile target | Use |
| --- | --- | --- |
| `ghcr.io/synehq/kelvo` | `runtime` | `serve`, gateway, node, `cluster-init` |
| `ghcr.io/synehq/kelvo-worker` | `worker` | Nodes that run [database operations](database-operations.md) |

Pin by digest (`ghcr.io/synehq/kelvo@sha256:...`); the release notes list both digests.
Until the first release built by `.github/workflows/release.yml` exists, build
locally: `docker build --target runtime -t kelvo:runtime .` (see [container images](container-images.md)).

## Run with Docker

Needs Docker with Compose v2.24+ and, for the query script, Python 3 with `pyarrow`.

```sh
git clone https://github.com/SYNEHQ/kelvo-go.git && cd kelvo-go
(umask 077; echo "KELVO_TOKEN=$(openssl rand -hex 32)" > deploy/quickstart/kelvo.env)  # mode 0600
docker compose -f deploy/quickstart/compose.yml up -d          # pulls ghcr.io/synehq/kelvo:latest
# no release image yet, or want this checkout:  ... up -d --build
curl -sS http://127.0.0.1:8080/health
```

The server listens on loopback port 8080 (`KELVO_PORT` changes it), runs as
UID 65532 with a read-only root filesystem, no capabilities and the Landlock
query sandbox, and serves `deploy/quickstart/data/sales.csv` as source `sales`.
`kelvo.env` is git-ignored; the token is the API key for every request.

Query with curl (submit, then claim the Arrow result once):

```sh
set -a; . deploy/quickstart/kelvo.env; set +a
id=$(curl -sS http://127.0.0.1:8080/v1/queries \
  -H "Authorization: Bearer $KELVO_TOKEN" -H 'Content-Type: application/json' \
  -d '{"mode":"federated","sources":["sales"],"sql":"SELECT region, SUM(amount) AS revenue FROM sales GROUP BY region"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -sS http://127.0.0.1:8080/v1/queries/$id/results -H "Authorization: Bearer $KELVO_TOKEN" -o revenue.arrow
curl -sS http://127.0.0.1:8080/v1/queries/$id -H "Authorization: Bearer $KELVO_TOKEN"   # state: succeeded
```

Or with Python:

```sh
pip install pyarrow
python3 deploy/quickstart/query.py
python3 deploy/quickstart/query.py "SELECT product, SUM(quantity) AS units FROM sales GROUP BY 1 ORDER BY 2 DESC"
```

Add your own sources to `deploy/quickstart/kelvo.yml` ([source types](source-coverage.md));
put credentials in `kelvo.env` and reference them by variable name. Stop with
`docker compose -f deploy/quickstart/compose.yml down`.

The quickstart is a single process with one token for the whole catalog and
plain HTTP on loopback. Do not expose it; use cluster mode for shared access.

## Install with Helm

The chart in [`deploy/helm/kelvo`](../deploy/helm/kelvo) runs [cluster mode](cluster.md)
for one tenant: a gateway Deployment, one Deployment per node identity, a
`cluster-init` Job and, by default, a bundled single-member NATS JetStream
StatefulSet. Every option is documented in
[`values.yaml`](../deploy/helm/kelvo/values.yaml).

1. Create the namespace and the two secrets the chart references. The chart
   never creates secrets; `make-secrets.sh` makes a private CA, the TLS identities
   (including the SPIFFE URIs) and random credentials for an evaluation cluster:

   ```sh
   kubectl create namespace kelvo
   DIR=./kelvo-pki sh deploy/helm/kelvo/scripts/make-secrets.sh kelvo kelvo default n1
   ```

   For your own PKI, create `kelvo-tls` and `kelvo-secrets` with the keys listed
   under `tls` and `secrets` in `values.yaml`.

2. Install:

   ```sh
   helm install kelvo deploy/helm/kelvo -n kelvo
   # add  --set image.digest=sha256:...  to pin the release image
   kubectl -n kelvo rollout status deploy/kelvo-gateway
   ```

   Gateway and node pods restart a few times until `cluster-init` has created
   the tenant's streams; that is expected.

### Query through the gateway

```sh
kubectl -n kelvo port-forward svc/kelvo-gateway 8443:8443 &
T=$(cat kelvo-pki/tenant-token)
curl -sS --cacert kelvo-pki/ca.crt https://localhost:8443/ready
id=$(curl -sS --cacert kelvo-pki/ca.crt https://localhost:8443/v1/queries \
  -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  -d '{"mode":"federated","sources":["sample"],"sql":"SELECT region, SUM(amount) AS revenue FROM sample GROUP BY region"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -sS --cacert kelvo-pki/ca.crt https://localhost:8443/v1/queries/$id/results -H "Authorization: Bearer $T" -o result.arrow
```

Accept a cluster result only as described in [results and errors](cluster.md#results-and-errors).

### Chart design choices

- **NATS:** bundled single-member StatefulSet (`nats.bundled: true`) with TLS,
  one account and the gateway/worker/provisioner users Kelvo needs, so the chart
  has no subchart dependency. JetStream `replicas` is 1. For durability run a
  3-member cluster yourself, set `nats.bundled: false`, `nats.external.url` and
  `nats.external.replicas: 3`, and provision matching account users
  (see [deploy/README.md](../deploy/README.md)).
- **Probes:** gateway readiness is `GET /ready` (broker reconciliation plus every
  node's `/ready`), liveness `GET /health`. The node listener accepts only the
  gateway's mTLS client certificate, so node probes are TCP checks and the
  gateway carries node readiness.
- **Security:** UID/GID 65532, `runAsNonRoot`, `readOnlyRootFilesystem`, all
  capabilities dropped, no privilege escalation, `seccompProfile: RuntimeDefault`,
  memory-backed `/tmp` and no service-account token. Set resource requests and
  limits for your workload (`gateway.resources`, `nodeDefaults.resources`).
- **Containment:** cgroup v2 [process containment](process-containment.md) needs a
  privileged root bootstrap and is not supported by this chart; nodes run with the
  Landlock sandbox only. The pod memory limit bounds the whole node.
- **NATS storage:** `nats.storage.size` is immutable after install (StatefulSet
  volume claim template); resize the PVC directly if the storage class allows it.
  NATS monitoring listens on loopback only; its probes run inside the pod.
- **NetworkPolicy:** optional (`networkPolicy.enabled`): clients to gateway,
  gateway to nodes, Kelvo pods to NATS; `networkPolicy.nodeEgressTo` limits node
  egress to your sources; with an external NATS, list its address there as well.

Not covered: multiple tenants, gateway key rotation, acceleration, exports,
database operations (use the `kelvo-worker` image and node config from
[database operations](database-operations.md)). Policy changes require the
drain and reprovisioning described in [cluster mode](cluster.md).

## Release binaries

Each GitHub release carries `kelvo-linux-{amd64,arm64}`,
`kelvo-landlock-linux-{amd64,arm64}`, the image SBOMs and `SHA256SUMS`. The
binaries are copied out of the release `runtime` image; `kelvo` needs glibc,
`libstdc++6` and `libgomp1` (Debian bookworm or newer).

```sh
sha256sum -c --ignore-missing SHA256SUMS
gh attestation verify kelvo-linux-amd64 -R SYNEHQ/kelvo-go
```

## Verify a release

Images are signed keylessly with cosign from the release workflow (the index
and every platform image), each platform image carries an SPDX SBOM
attestation, and the index carries a GitHub build-provenance attestation. The
public `:vX.Y.Z` and `:latest` tags are added only after signing and attestation
succeed; `:latest` moves only for the highest stable `vX.Y.Z` tag.

```sh
TAG=v0.2.0
IMG=ghcr.io/synehq/kelvo@sha256:<digest-from-release-notes>
ID="https://github.com/SyneHQ/kelvo-go/.github/workflows/release.yml@refs/tags/$TAG"
cosign verify "$IMG" --certificate-identity "$ID" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
# Per-platform SBOM: verify against the platform digest
# (docker buildx imagetools inspect "$IMG" lists them).
cosign verify-attestation --type spdxjson ghcr.io/synehq/kelvo@sha256:<amd64-digest> \
  --certificate-identity "$ID" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com > /dev/null
gh attestation verify "oci://$IMG" -R SyneHQ/kelvo-go
```

`--certificate-identity` must match the signing workflow ref exactly, including
the repository's case as GitHub reports it (`gh repo view SyneHQ/kelvo-go --json nameWithOwner`).
Alternatively use `--certificate-github-workflow-repository SyneHQ/kelvo-go`
with `--certificate-identity-regexp` and the same OIDC issuer.

A manual run of the Release workflow (`workflow_dispatch`) is a dry run: it
builds both images for both architectures, their SBOMs and the binaries, and
pushes nothing. Tag pushes must match `vMAJOR.MINOR.PATCH[-PRERELEASE]` or the
workflow stops before any push.

### Maintainer setup

Repository settings the workflow relies on (configure them before the first tag):

1. An environment named `release` with required reviewers. The `publish` and
   `release` jobs, the only ones that sign, attest, tag images or write the
   GitHub release, wait for that approval.
2. A tag ruleset for `v*` that restricts creation, update and deletion to maintainers.
