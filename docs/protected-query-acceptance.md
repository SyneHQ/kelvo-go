# Protected query acceptance

The Linux gate runs authenticated queries through the gateway, a private NATS broker, a node and its contained worker. Snapshots come from real protected-runtime publication into a local TLS/CAS object fixture.

[Retained acceptance](evidence/protected-queries.json) passed all 11 VM stages on `15bd93f`: 8 existing worker cases and 10 authenticated-query cases. It includes the first trial's fixture errors, their fix and independently verified cleanup.

The first hosted CI attempt caught an older broker-prefix count assertion. Its correction passed 37 targeted Python controls on the VM; the evidence retains both results separately.

| Check | What the gate verifies |
| --- | --- |
| Publication | Separate tenant namespaces; single-file and two-part Parquet snapshots |
| Analysis | Exact filtered CTE joins and aggregates; integers, decimals, timestamps and nulls |
| Authorization | Hidden columns and metadata; foreign handles return 404; stale, forged and unsupported requests fail closed |
| Revocation | Remove the initiating key after durable success; withhold public Arrow completion across LZ4/plain and length/chunked transport |
| Cleanup | Cancel a held range; retain a query during a 50 ms drain deadline while registry release stalls; finish cleanup after release |

## Run on a disposable Linux test machine

Use the pinned Go/DuckDB toolchain, cgroup v2 delegation, the sandbox launcher and a checksum-verified NATS 2.15.0 binary. The runner creates its own TLS keys, scoped broker accounts and private storage.

```sh
mkdir -m 700 artifacts/protected-query-private
python3 -B scripts/containment_acceptance.py \
  --protected-objects --protected-queries \
  --nats-server /absolute/path/to/nats-server \
  --nats-sha256 "$NATS_BINARY_SHA256" \
  --artifact-root "$PWD/artifacts/protected-query-private/run" \
  --report artifacts/protected-query.json
```

The delegated service shares 1 CPU, 1 GiB RAM and 256 tasks across its broker, test supervisor and worker processes, with a 10-minute runtime cap. Compilation happens outside that service; bound the calling build job separately. The retained VM run uses an additional 2 CPU / 8 GiB build job.

Successful evidence requires every named worker/query case, unchanged source and binaries, a gracefully reaped broker, and removal of the owned service and cgroup. Missing, repeated, failed or skipped cases reject the report. CI runs the same gate.

## Scope

The TLS fixture checks exact keys, versions, conditional writes and tenant roles. It does not certify cloud signatures, IAM, throughput or memory sizing.

A held HTTP response is separate from a blocked client `Body.Close`. The retained VM validation also runs named lower-level tests for delayed Close, pin retention and eventual capacity handback. The integrated drain check proves its recorded interval; it does not promise that every node permit stays held indefinitely during provider cleanup.

Revocation is local to the gateway replica. A job may already be durably successful when its final bytes are withheld. The gate does not make revocation atomic with publication or recall bytes already received.

[Protected snapshots](protected-object-readers.md) · [Principal access](principal-access.md) · [Key rotation](gateway-key-rotation.md)
