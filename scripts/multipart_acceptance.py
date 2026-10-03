#!/usr/bin/env python3
"""Local multipart snapshot acceptance using real Kelvo subprocesses on the test VM.

Uses only private generated data. No broker or cloud credentials are needed.
Compiles nothing; requires bin/kelvo, bin/kelvo-landlock and PyArrow.
"""
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import traceback
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from acceleration_acceptance import Acceptance, ROOT, BIN, require, write_config, write_private, parse_status


class MultipartAcceptance(Acceptance):
    def run(self):
        self.stage = "multipart_fixture"
        require((ROOT / "bin/kelvo-landlock").is_file(), "sandbox launcher unavailable")
        rows = 100000
        source = self.directory / "source.parquet"
        schema = pa.schema([("id", pa.int64()), ("label", pa.string())])
        with pq.ParquetWriter(source, schema) as writer:
            for start in range(0, rows, 4096):
                ids = list(range(start, min(rows, start + 4096)))
                labels = [hashlib.sha512(str(i).encode()).hexdigest() for i in ids]
                writer.write_table(pa.table({"id": pa.array(ids, type=pa.int64()), "label": labels}, schema=schema))
        source.chmod(0o600)
        dim = self.directory / "dim.csv"
        write_private(dim, "bucket,category\n0,even\n1,odd\n")
        limits = {"max_rows": 200000, "max_bytes": 64 << 20, "timeout": "1m", "memory_mb": 256, "threads": 2, "max_temp_mb": 256}
        dataset = {"id": "many", "query": {"mode": "federated", "sources": ["raw"], "sql": "SELECT id, label FROM raw"}, "refresh_interval": "0s", "max_age": "1h", "authorization_version": "acceptance-v1", "limits": limits}
        config = {"sources": [{"id": "raw", "type": "parquet", "path": str(source)}, {"id": "dim", "type": "csv", "path": str(dim)}], "acceleration": {"directory": str(self.directory / "snapshots"), "tenant_id": "tenant-a", "datasets": [dataset]}}
        path = self.directory / "catalog.yml"
        write_config(path, config)
        self.stage = "single_to_multipart"
        initial = self.status(path, "many", command="refresh")
        dataset["multipart"] = {"max_part_bytes": 1 << 20, "max_parts": 128}
        write_config(path, config)
        refreshed = self.status(path, "many", command="refresh")
        require(refreshed["generation"] != initial["generation"] and refreshed["fingerprint"] == initial["fingerprint"], "storage transition changed authorization identity")
        directory = self.directory / "snapshots" / "tenant-a" / "many"
        parts = sorted(directory.glob(refreshed["generation"] + "*.parquet"))
        require(1 < len(parts) <= 128, "refresh did not publish multiple bounded parts")
        require(all(p.stat().st_size <= 1 << 20 for p in parts), "encoded part ceiling exceeded")
        require(sum(p.stat().st_size for p in parts) == refreshed["bytes"], "generation byte accounting differs from parts")
        require(sum(pq.ParquetFile(p).metadata.num_rows for p in parts) == rows, "row boundaries lost or duplicated rows")
        verified = self.status(path, "many", command="verify")
        require(verified["generation"] == refreshed["generation"], "verification changed generation")
        self.record("multipart_generation_bounds_and_full_verification", rows=rows, parts=len(parts), encoded_bytes=refreshed["bytes"], max_part_bytes=1 << 20)

        self.stage = "sandboxed_multipart_join"
        def query_rows():
            output = self.directory / (uuid.uuid4().hex + ".arrow")
            self.cli(["query", "--config", path, "--mode", "federated", "--sources", "many,dim", "--sql", "SELECT d.category, CAST(count(*) AS BIGINT) AS n, CAST(sum(m.id) AS BIGINT) AS total, CAST(sum(length(m.label)) AS BIGINT) AS chars FROM many m JOIN dim d ON m.id % 2 = d.bucket GROUP BY d.category ORDER BY d.category", "--out", output, "--sandbox", ROOT / "bin/kelvo-landlock"], timeout=60)
            with pa.ipc.open_stream(output) as reader:
                return reader.read_all().to_pylist()
        expected = [{"category": name, "n": rows // 2, "total": sum(range(parity, rows, 2)), "chars": rows // 2 * 128} for parity, name in ((0, "even"), (1, "odd"))]
        require(query_rows() == expected, "sandboxed join across parts returned wrong rows")
        self.record("sandboxed_cross_source_join_reads_all_parts_exactly")
        full_output = self.directory / "all-rows.arrow"
        self.cli(["query", "--config", path, "--mode", "federated", "--sources", "many", "--sql", "SELECT id, label FROM many ORDER BY id", "--out", full_output, "--sandbox", ROOT / "bin/kelvo-landlock"], timeout=60)
        seen = 0
        with pa.ipc.open_stream(full_output) as reader:
            for batch in reader:
                for row in batch.to_pylist():
                    require(row["id"] == seen and row["label"] == hashlib.sha512(str(seen).encode()).hexdigest(), "multipart content differs from original source")
                    seen += 1
        require(seen == rows, "full multipart scan lost rows")
        self.record("sandboxed_multipart_full_values_match_source", checked_rows=seen)

        self.stage = "mixed_format_rollback"
        first, _ = self.cli(["accelerate", "restore", "--config", path, "--dataset", "many", "--generation", initial["generation"], "--expected-generation", refreshed["generation"]])
        require(parse_status(first)["refreshed_at"] == initial["refreshed_at"], "rollback relabeled original freshness")
        require(query_rows() == expected, "single-file rollback changed values")
        self.cli(["accelerate", "restore", "--config", path, "--dataset", "many", "--generation", refreshed["generation"], "--expected-generation", initial["generation"]])
        require(query_rows() == expected, "multipart rollback changed values")
        self.record("single_and_multipart_generations_restore_without_freshness_changes")

        self.stage = "failed_refresh_preserves_generation"
        limited = copy.deepcopy(config)
        limited["acceleration"]["datasets"][0]["multipart"]["max_parts"] = 2
        write_config(path, limited)
        self.status(path, "many", command="refresh", success=False)
        write_config(path, config)
        require(self.status(path, "many")["generation"] == refreshed["generation"], "failed part limit published an incomplete generation")
        require(query_rows() == expected, "failed refresh damaged current generation")
        self.record("part_count_exhaustion_preserves_complete_previous_generation")

        self.stage = "all_part_integrity"
        damaged = parts[len(parts) // 2]
        original = damaged.read_bytes()
        damaged.chmod(0o600)
        mutated = bytearray(original)
        mutated[len(mutated) // 2] ^= 1
        damaged.write_bytes(mutated)
        damaged.chmod(0o400)
        try:
            self.status(path, "many", command="verify", success=False)
        finally:
            damaged.chmod(0o600)
            damaged.write_bytes(original)
            damaged.chmod(0o400)
        require(self.status(path, "many", command="verify")["generation"] == refreshed["generation"], "fixture repair did not restore integrity")
        self.record("nonfirst_part_corruption_fails_full_verification")


def main():
    acceptance = MultipartAcceptance(False)
    outcome = {"passed": False, "checked_at": datetime.now(timezone.utc).isoformat(), "checks": acceptance.checks, "scope": "Local real-process multipart correctness; no remote storage or capacity claim"}
    code = 1
    try:
        require(BIN.is_file(), "built binary unavailable")
        acceptance.run()
        outcome["passed"] = True
        code = 0
    except BaseException as error:
        outcome["failure"] = {"stage": acceptance.stage, "type": type(error).__name__}
        write_private(acceptance.directory / "failure.log", traceback.format_exc())
        print("Multipart acceptance failed at " + acceptance.stage + "; diagnostics retained privately", file=sys.stderr)
    finally:
        acceptance.cleanup()
        (ROOT / "docs/evidence/multipart-acceptance.json").write_text(json.dumps(outcome, indent=2) + "\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
