# Container images

Build native Linux images from the same reviewed checkout:

```sh
docker build --target runtime -t kelvo:runtime .
docker build --target worker -t kelvo:worker .
```

| Target | Contains |
| --- | --- |
| `runtime` (default) | Kelvo and the Landlock launcher |
| `worker` | Runtime plus the optional Go database-operation adapter |

The worker target builds its adapter against the same SDK source. JDBC runtimes
and vendor JARs remain separately provisioned. Add `--build-arg DUCKBRIDGE=1`
when the Kelvo binary needs the native federation bridge.

## Configure operations

Read and verify the adapter checksum from your built worker image:

```sh
docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true --entrypoint /bin/sh kelvo:worker \
  -ec 'cd /usr/local/bin; sha256sum -c /usr/share/kelvo/kelvo-adapter-go.sha256; cat /usr/share/kelvo/kelvo-adapter-go.sha256'
```

Put that digest in the node configuration:

```yaml
operations:
  adapter:
    binary: /usr/local/bin/kelvo-adapter-go
    sha256: <digest-from-this-image>
```

The adapter is a child executable started by Kelvo, not a separate HTTP service.
Pin the deployed image by registry digest and update the adapter checksum with it.

Both targets default to UID/GID `65532`. Use a read-only root filesystem,
drop capabilities, disable privilege escalation and provision private writable
scratch/result volumes. Set explicit CPU, memory and process limits.

The image does not grant cgroup delegation or change the containment backend.
Operation workers still require the [existing containment setup](process-containment.md),
[on-demand resolver](on-demand-connections.md) and [operation configuration](database-operations.md).

## Verify packaging

On an isolated Linux host with Docker, Landlock ABI 3+ and `pyarrow==25.0.1`:

```sh
python3 scripts/worker_image_acceptance.py \
  --runtime-image kelvo:runtime --worker-image kelvo:worker \
  --output artifacts/worker-image.json
```

The check verifies packaged hashes, nonroot/read-only execution, real DuckDB
Arrow values, Landlock denial and adapter reads from an unlinked fd6 snapshot.
It rejects altered snapshot bytes and checks container cleanup. It uses synthetic
data without network access; it does not qualify cluster operation or vendor accounts.
