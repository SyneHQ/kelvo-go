# Protected export acceptance

This Linux gate follows protected snapshots through authenticated export fill,
durable local parts and repeat downloads. [Linux acceptance](evidence/protected-exports.json)
passed all 15 stages on `dfe788c`, with independent source, binary and cleanup checks.
The evidence retains a corrected Python control failure and a later monitor stall
whose cause remains unknown; the accepted run used unchanged resource guards.

| Check | What the gate verifies |
| --- | --- |
| Analysis | Two tenants, real single-file/two-part snapshots, filtered CTE joins, aggregates and exact Arrow values |
| Access | Foreign handles, hidden data, stale authority and unsupported requests fail closed |
| Reuse | A replacement key for the same principal downloads identical bytes without another fill or source read |
| Revocation | Withhold readiness or final download bytes across plain/LZ4 and length/chunked transport |
| Failure | Lost completion is not replayed; cancellation and stalled registry release retain custody until cleanup ends |

## Run on a disposable Linux host

Use the pinned Go/DuckDB toolchain, cgroup v2 delegation, sandbox launcher and a
checksum-verified NATS 2.15.0 binary. The runner creates private TLS credentials,
separate tenant accounts and its own storage.

```sh
mkdir -m 700 artifacts/protected-export-private
python3 -B scripts/containment_acceptance.py \
  --protected-objects --protected-exports \
  --nats-server /absolute/path/to/nats-server \
  --nats-sha256 "$NATS_BINARY_SHA256" \
  --artifact-root "$PWD/artifacts/protected-export-private/run" \
  --report artifacts/protected-export.json
```

The service shares 1 CPU, 1 GiB RAM and 256 tasks across the broker, supervisor and
workers, with a 10-minute cap. Bound the calling build job separately. Run the
query and export gates as separate invocations.

Acceptance requires all 12 export cases, the existing worker gates, unchanged
source/binary hashes, a gracefully reaped broker and removed service/cgroup.
Missing, duplicated, skipped or failed cases reject the report.

## Scope

Source pins cover export fill. Ready downloads read durable local parts; they do
not reread protected snapshots. Revoking one key does not revoke a still-authorized
replacement key for the same principal or recall bytes already delivered.

The TLS fixture does not certify cloud IAM, coordinated revocation, throughput or
memory sizing. The drain case proves its recorded 50 ms interval. LZ4 checks cover
the requested codec, decoded values, digests and framing.

[Exports](exports.md) · [Protected snapshots](protected-object-readers.md) · [Query acceptance](protected-query-acceptance.md)
