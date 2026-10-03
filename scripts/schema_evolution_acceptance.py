#!/usr/bin/env python3
"""Real-process schema evolution acceptance on the provisioned Linux test VM.

Requires built bin/kelvo, bin/kelvo-landlock and PyArrow. Builds/downloads nothing.
Mutates generated Parquet source files while preserving query/config identity,
then observes CLI publication, Arrow values, Parquet types, restore and fencing.
Does not import or reproduce Kelvo's schema compatibility implementation.
"""

from datetime import datetime, timezone
import hashlib
import json
import platform
import signal
import sys
import time
import traceback
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from acceleration_acceptance import (
    Acceptance, BIN, ROOT, EOS, parse_status, require, write_config, write_private,
)


EVIDENCE = ROOT / "docs/evidence/schema-evolution-acceptance.json"
SANDBOX = ROOT / "bin/kelvo-landlock"


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_table(rows, wide=False, added=False):
    # Int32 boundary values, NULLs and >Int32 values after widening exercise actual
    # Arrow data semantics independently of the policy comparator's allow list.
    edge = (1 << 40) if wide else (1 << 31) - 1
    values = [-edge, edge, None] + list(range(3, rows))
    fields = [pa.field("row_id", pa.int64()),
              pa.field("amount", pa.int64() if wide else pa.int32()),
              pa.field("label", pa.string())]
    arrays = [pa.array(range(rows), type=pa.int64()),
              pa.array(values, type=fields[1].type),
              pa.array([hashlib.sha512(str(i).encode()).hexdigest()
                        if i % 7 else None for i in range(rows)])]
    if added:
        fields.append(pa.field("note", pa.string(), nullable=True))
        arrays.append(pa.array([None if i % 2 else "new-field" for i in range(rows)]))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


class SchemaEvolutionAcceptance(Acceptance):
    def fixture(self, name, policy, multipart=False):
        source = self.directory / (name + ".parquet")
        path = self.directory / (name + ".yml")
        dataset = {
            "id": "evolved", "query": {"mode": "federated", "sources": ["raw"],
                                         "sql": "SELECT * FROM raw"},
            "refresh_interval": "0s", "max_age": "1h",
            "authorization_version": "acceptance-v1",
            "limits": {"max_rows": 200000, "max_bytes": 64 << 20, "timeout": "1m",
                       "memory_mb": 256, "threads": 2, "max_temp_mb": 256},
        }
        if policy is not None:
            dataset["schema_evolution"] = policy
        if multipart:
            dataset["multipart"] = {"max_part_bytes": 1 << 20, "max_parts": 128}
        config = {"sources": [{"id": "raw", "type": "parquet", "path": str(source)}],
                  "acceleration": {"directory": str(self.directory / (name + "-snapshots")),
                                   "tenant_id": "acceptance", "datasets": [dataset]}}
        write_config(path, config)
        return source, path, config

    def write_source(self, source, table):
        pq.write_table(table, source, row_group_size=4096)
        source.chmod(0o600)

    def refresh(self, path, success=True):
        stdout, stderr = self.cli(["accelerate", "refresh", "--config", path,
                              "--dataset", "evolved", "--sandbox", SANDBOX],
                             success=success, timeout=90)
        if not success:
            require(stderr.strip() == b"Accelerated dataset schema changed",
                    "schema rejection lost its sanitized public error classification")
        return parse_status(stdout) if success else None

    def check_values(self, path, table, generation):
        output = self.directory / (uuid.uuid4().hex + ".arrow")
        _, stderr = self.cli(["query", "--config", path, "--mode", "federated",
                              "--sources", "evolved", "--sql",
                              "SELECT * FROM evolved ORDER BY row_id", "--out", output,
                              "--sandbox", SANDBOX, "--memory-mb", "256", "--threads", "2"],
                             timeout=90)
        with output.open("rb") as stream:
            stream.seek(-len(EOS), 2)
            require(stream.read() == EOS, "query export missing successful Arrow EOS")
        with pa.ipc.open_stream(output) as reader:
            actual = reader.read_all()
        require(actual.equals(table, check_metadata=False),
                "sandboxed query changed typed values, column order or NULLs")
        stats = json.loads(stderr)
        require(stats.get("backend") == "duckdb", "query did not execute in DuckDB")
        selected = stats.get("accelerations", [])
        require(len(selected) == 1 and selected[0]["generation"] == generation,
                "query selected the wrong snapshot generation")

    def preserved(self, path, before, expected):
        current = self.status(path, "evolved")
        for field in ("generation", "fingerprint", "sha256", "rows", "bytes", "refreshed_at"):
            require(current[field] == before[field], "rejected operation changed snapshot identity")
        require(self.status(path, "evolved", command="verify")["generation"] == before["generation"],
                "rejected operation damaged committed payloads")
        self.check_values(path, expected, before["generation"])

    def run_case(self, name, policy, wide, added, accepted, multipart=False):
        self.stage = name
        rows = 100000 if multipart else 8
        source, path, config = self.fixture(name, policy, multipart)
        original = source_table(rows)
        changed = source_table(rows, wide=wide, added=added)
        self.write_source(source, original)
        first = self.refresh(path)
        self.check_values(path, original, first["generation"])
        self.write_source(source, changed)
        next_snapshot = self.refresh(path, success=accepted)
        if not accepted:
            self.preserved(path, first, original)
            self.record(name, expected="rejected", layout="single", checked_rows=rows)
            return
        require(next_snapshot["generation"] != first["generation"], "accepted evolution was not published")
        require(next_snapshot["fingerprint"] == first["fingerprint"],
                "source drift unexpectedly changed static config identity")
        self.check_values(path, changed, next_snapshot["generation"])
        self.status(path, "evolved", command="verify")
        parts = sorted((self.directory / (name + "-snapshots") / "acceptance/evolved").glob(
            next_snapshot["generation"] + "*.parquet"))
        require(len(parts) > 1 if multipart else len(parts) == 1, "unexpected generation layout")
        require(sum(pq.ParquetFile(part).metadata.num_rows for part in parts) == rows,
                "snapshot footer rows do not match source")
        for part in parts:
            require(pq.read_schema(part).equals(changed.schema, check_metadata=False),
                    "stored part schema differs from independently generated source")
            if multipart:
                require(part.stat().st_size <= 1 << 20, "multipart encoded ceiling exceeded")
        require(sum(part.stat().st_size for part in parts) == next_snapshot["bytes"],
                "stored byte accounting differs from published snapshot")
        # Restore compares two independently verified generations with the same
        # configuration fingerprint; failure therefore cannot be policy fencing.
        self.cli(["accelerate", "restore", "--config", path, "--dataset", "evolved",
                  "--generation", first["generation"],
                  "--expected-generation", next_snapshot["generation"]], success=False)
        self.preserved(path, next_snapshot, changed)
        self.write_source(source, original)
        self.refresh(path, success=False)
        self.preserved(path, next_snapshot, changed)
        self.record(name, expected="published_then_regression_and_restore_rejected",
                    layout="multipart" if multipart else "single", checked_rows=rows,
                    parts=len(parts), encoded_bytes=next_snapshot["bytes"])

    def strict_fallback(self):
        self.stage = "explicit_strict_fallback"
        source, path, config = self.fixture(self.stage, {"add_nullable_columns": True})
        original = source_table(8)
        self.write_source(source, original)
        permissive = self.refresh(path)
        config["acceleration"]["datasets"][0]["schema_evolution"] = {
            "add_nullable_columns": False, "safe_widening": False}
        write_config(path, config)
        # Refresh unchanged schema establishes the stricter policy's fingerprint.
        strict = self.refresh(path)
        require(strict["fingerprint"] != permissive["fingerprint"],
                "tightening policy did not change catalog identity")
        self.write_source(source, source_table(8, added=True))
        self.refresh(path, success=False)
        self.preserved(path, strict, original)
        del config["acceleration"]["datasets"][0]["schema_evolution"]
        write_config(path, config)
        self.preserved(path, strict, original)
        self.record("explicit_strict_fallback_and_omitted_policy_equivalence", checked_rows=8)

    def run(self):
        require(SANDBOX.is_file(), "built sandbox launcher unavailable")
        self.run_case("default_strict_rejects_addition", None, False, True, False)
        self.run_case("default_strict_rejects_widening", None, True, False, False)
        self.run_case("nullable_addition", {"add_nullable_columns": True}, False, True, True)
        self.run_case("integer_widening", {"safe_widening": True}, True, False, True)
        self.run_case("addition_flag_does_not_enable_widening", {"add_nullable_columns": True}, True, False, False)
        self.run_case("widening_flag_does_not_enable_addition", {"safe_widening": True}, False, True, False)
        self.run_case("combined_multipart_evolution", {"add_nullable_columns": True, "safe_widening": True},
                      True, True, True, multipart=True)
        self.strict_fallback()


def main():
    acceptance = SchemaEvolutionAcceptance(False)
    started = time.monotonic()
    outcome = {"schema_version": 1, "passed": False,
               "checked_at": datetime.now(timezone.utc).isoformat(),
               "checks": acceptance.checks,
               "runtime": {"os": platform.system(), "architecture": platform.machine()},
               "scope": "Local real-process DuckDB/Arrow schema evolution correctness with sandboxed refresh/query and multipart files; no remote-provider, concurrency, throughput or production-readiness claim"}
    def interrupted(signum, frame):
        raise InterruptedError("acceptance interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    code = 1
    try:
        require(BIN.is_file(), "built binary unavailable")
        outcome["runtime"]["binary_sha256"] = digest_file(BIN)
        require(SANDBOX.is_file(), "built sandbox launcher unavailable")
        outcome["runtime"]["sandbox_sha256"] = digest_file(SANDBOX)
        acceptance.run()
        require(digest_file(BIN) == outcome["runtime"]["binary_sha256"], "binary changed during acceptance")
        require(digest_file(SANDBOX) == outcome["runtime"]["sandbox_sha256"], "sandbox changed during acceptance")
        outcome["passed"] = True
        code = 0
    except BaseException as error:
        outcome["failure"] = {"stage": acceptance.stage, "type": type(error).__name__}
        write_private(acceptance.directory / "failure.log", traceback.format_exc())
        print("Schema evolution acceptance failed at " + acceptance.stage + "; diagnostics retained privately", file=sys.stderr)
    finally:
        try:
            acceptance.cleanup()
        except BaseException as error:
            outcome["passed"] = False
            outcome["cleanup_failure"] = {"type": type(error).__name__}
            code = 1
        outcome["elapsed_seconds"] = round(time.monotonic() - started, 3)
        EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE.write_text(json.dumps(outcome, indent=2) + "\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
