#!/usr/bin/env python3
"""Prepare private analytical benchmark manifests from public YAML on Linux.

Installs, provisions and executes nothing. Preparation hashes local inputs
before timing; validation preparation only indexes finished reports/artifacts.
Source credentials are never read. Requires PyYAML in the invoking interpreter.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import stat
import sys

VERSIONS = {"duckdb": "1.5.6", "polars": "1.44.2", "pyarrow": "25.0.1"}
WORKFLOWS = {"daily_rolling_kpis", "borough_hour_hotspots", "route_economics", "monthly_zone_momentum"}
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
LABEL = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,95}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
UNIT = re.compile(r"kelvo-analytics-[0-9a-f]{24}\.service\Z")
MAX_BYTES = 64 << 20


class PreparationError(Exception):
    """Carries only fixed, public diagnostic codes."""


def require(condition, code):
    if not condition:
        raise PreparationError(code)


def file_path(value, private=False):
    path = Path(value)
    require(path.is_absolute() and not path.is_symlink(), "file_requires_absolute_regular_path")
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode), "file_requires_absolute_regular_path")
    if private:
        require(info.st_uid == os.getuid() and not info.st_mode & 0o077, "file_must_be_owned_and_private")
    return path.resolve()


def directory_path(value, private=False):
    path = Path(value)
    require(path.is_absolute() and path.is_dir() and not path.is_symlink(), "directory_requires_absolute_existing_path")
    if private:
        info = path.stat()
        require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700, "directory_must_be_owned_mode_0700")
    return path.resolve()


def read_json(path):
    path = file_path(path)
    require(path.stat().st_size <= 8 << 20, "json_input_exceeds_limit")
    return json.loads(path.read_text())


def fingerprint(path):
    value = hashlib.sha256()
    with file_path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def save(path, value):
    require(not path.exists() and not path.is_symlink(), "generated_file_already_exists")
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def load_inputs(args):
    import yaml

    root = directory_path(args.root)
    directory_path(root / "private", private=True)
    config_path = file_path(args.config)
    require(config_path.stat().st_size <= 65536, "yaml_config_exceeds_limit")
    config = yaml.safe_load(config_path.read_text())
    require(isinstance(config, dict) and set(config) <= {"schema_version", "machine", "profile", "panels", "connection"}, "invalid_public_config")
    require(type(config.get("schema_version")) is int and config["schema_version"] == 1, "invalid_config_version")
    require(isinstance(config.get("machine"), str) and LABEL.fullmatch(config["machine"]), "invalid_machine_label")
    profile = config.setdefault("profile", "standard")
    require(profile in ("standard", "oracle-micro"), "unknown_resource_profile")
    panels = config.setdefault("panels", ["live"] if profile == "oracle-micro" else ["snapshot", "live"])
    require(isinstance(panels, list) and panels and len(set(panels)) == len(panels)
            and set(panels) <= {"snapshot", "live"}, "invalid_panels")
    require(profile != "oracle-micro" or panels == ["live"], "oracle_micro_profile_is_live_only")
    require(IDENTIFIER.fullmatch(config.setdefault("connection", "taxi")), "invalid_connection_identifier")
    definitions_path = file_path(root / "benchmarks/ride-hailing/workflows.yml")
    definitions = yaml.safe_load(definitions_path.read_text())
    specs = copy.deepcopy(definitions["workloads"])
    require(set(specs) == WORKFLOWS, "all_four_workflows_required")
    for spec in specs.values():
        require(set(spec["sql"]) == {"duckdb", "clickhouse"}, "both_sql_dialects_required")
        for dialect, relative in list(spec["sql"].items()):
            path = file_path(root / relative)
            require(path.is_relative_to(root / "benchmarks/ride-hailing") and path.stat().st_size <= 128 << 10, "invalid_sql_file")
            spec["sql"][dialect] = path.read_text()
    provenance_path = root / "datasets/provenance.json"
    provenance = read_json(provenance_path)
    require(set(provenance["datasets"]) == {"trips", "zones"}, "invalid_dataset_provenance")
    dataset = {}
    for name, count in (("trips", 22612607), ("zones", 265)):
        source = provenance["datasets"][name]
        require(source["file"] == name + ".parquet" and source["rows"] == count
                and SHA256.fullmatch(source["sha256"]), "unexpected_dataset_contract")
        dataset[name] = {"paths": [str(root / "datasets" / source["file"])], "rows": count, "sha256": [source["sha256"]]}
    return root, config, specs, dataset, {"workflow_contract": fingerprint(definitions_path), "dataset_provenance": fingerprint(provenance_path)}


def prepare(args, root, config, specs, dataset, contract_hashes):
    panels = config["panels"]
    target = Path(args.output_directory) if args.output_directory else root / "private/manifests"
    directory_path(target.parent, private=True)
    require(target.is_absolute() and not target.exists() and not target.is_symlink(), "output_directory_must_be_new")
    binary, launcher = root / "bin/kelvo", root / "bin/kelvo-landlock"
    for path in (binary, launcher):
        require(os.access(file_path(path), os.X_OK), "required_binary_not_executable")
    code_hashes = {**contract_hashes, "kelvo": fingerprint(binary), "sandbox_launcher": fingerprint(launcher)}
    for name in ("analytics_library_baseline", "analytics_native_references", "analytics_workflow_benchmark",
                 "prepare_analytics_dataset", "validate_analytics_workflows", "federation_micro_benchmark", "federation_capacity"):
        code_hashes[name] = fingerprint(root / "scripts" / (name + ".py"))
    environment = database = live_catalog = None
    if "live" in panels or args.source_database or args.environment_file:
        require(args.source_database and IDENTIFIER.fullmatch(args.source_database) and args.environment_file, "source_database_and_private_environment_required")
        database = args.source_database
        environment = file_path(args.environment_file, private=True)  # Never read credential values.
    if "live" in panels:
        live_catalog = file_path(args.live_catalog or root / "catalog-live.yml")
        code_hashes["live_catalog"] = fingerprint(live_catalog)
    file_catalog = None
    if "snapshot" in panels:
        file_catalog = file_path(args.file_catalog or root / "catalog-files.yml")
        code_hashes["file_catalog"] = fingerprint(file_catalog)
        for source in dataset.values():
            require(fingerprint(Path(source["paths"][0])) == source["sha256"][0], "normalized_dataset_hash_mismatch")
        python = root / "venv/bin/python"
        require(python.is_absolute() and python.is_file() and os.access(python, os.X_OK), "venv_python_not_executable")
        # Preserve the venv invocation path even when it is a symlink.
    manifests = {}
    if file_catalog:
        manifests["library-manifest.json"] = {"version": 1, "dataset": dataset, "versions": VERSIONS,
            "limits": {"threads": 2, "memory_mb": 1024, "max_temp_mb": 2048, "max_rows": 100000,
                       "max_bytes": MAX_BYTES, "result_compression": "lz4_frame"}, "workloads": specs}
    if environment:
        manifests["reference-manifest.json"] = {"environment_file": str(environment), "database": database, "workflows": specs}
    input_hashes = {name: source["sha256"][0] for name, source in dataset.items()}

    def kelvo_case(key, spec, mode, snapshot=False):
        connection = config["connection"]
        relations = {name: database + "." + name if mode == "native" else name if snapshot else connection + "." + name for name in ("trips", "zones")}
        sql = spec["sql"]["clickhouse" if mode == "native" else "duckdb"].format(**relations)
        memory = 1024 if snapshot else 512 if mode == "native" else 128
        engine = "kelvo-snapshot" if snapshot else "kelvo-" + mode
        argv = [str(binary), "query", "--config", str(file_catalog if snapshot else live_catalog),
            "--sandbox", str(launcher), "--mode", mode, "--connection" if mode == "native" else "--sources",
            ",".join(spec["inputs"]) if snapshot else connection, "--sql", sql, "--out", "{output}",
            "--memory-mb", str(memory), "--threads", "2", "--temp-mb", "2048", "--max-rows", "100000",
            "--max-bytes", str(MAX_BYTES), "--timeout", "180s" if snapshot else "300s", "--result-compression", "lz4_frame"]
        case = {"id": key + "-" + engine, "panel": "snapshot" if snapshot else "live", "engine": engine,
                "workflow": key, "argv": argv, "input_hashes": input_hashes if snapshot else contract_hashes,
                "code_hashes": {**code_hashes, "sql": hashlib.sha256(sql.encode()).hexdigest()}}
        if not snapshot:
            case["environment_file"] = str(environment)
        if "expected_rows" in spec["output"]:
            case["expected_rows"] = spec["output"]["expected_rows"]
        return case

    for panel in panels:
        cases = []
        for key, spec in specs.items():
            if panel == "live":
                cases.extend(kelvo_case(key, spec, mode) for mode in ("native", "federated"))
                continue
            cases.append(kelvo_case(key, spec, "federated", True))
            for engine in ("duckdb", "polars"):
                cases.append({"id": key + "-" + engine, "panel": panel, "engine": engine, "workflow": key,
                    "argv": [str(python), str(root / "scripts/analytics_library_baseline.py"), "--engine", engine,
                        "--manifest", str(target / "library-manifest.json"), "--workload", key,
                        "--output", "{output}", "--metrics", "{metrics}", "--scratch", "{scratch}"],
                    "input_hashes": input_hashes, "code_hashes": {**code_hashes,
                        "sql": hashlib.sha256(spec["sql"]["duckdb"].encode()).hexdigest()}})
        for stage, trials in (("preflight", 1), ("measure", 3)):
            profile = config["machine"] + "-" + panel + "-" + stage
            require(LABEL.fullmatch(profile), "profile_label_too_long")
            manifests[panel + "-" + stage + ".json"] = {"schema_version": 1, "profile": profile, "trials": trials,
                "limits": {"memory_max_mib": 1536 if panel == "snapshot" else 640, "threads": 2, "tasks_max": 96,
                    "temp_mib": 2048, "max_output_bytes": MAX_BYTES, "timeout_seconds": 180 if panel == "snapshot" else 300}, "cases": cases}
    packages = {}
    for package in (*VERSIONS, "PyYAML"):
        try:
            packages[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            packages[package] = None
    evidence = {"schema_version": 1, "machine": config["machine"], "profile": config["profile"], "panels": panels,
        "python_version": platform.python_version(), "installed_package_versions": packages, "required_engine_versions": VERSIONS,
        "generator_sha256": fingerprint(Path(__file__).resolve()), "code_hashes": code_hashes,
        "input_hashes": input_hashes, "local_input_hashes_verified": "snapshot" in panels,
        "scope": "Preparation only, outside trial timing. Code and executable hashes are observed; live-source data hashes remain declared provenance. No performance or correctness validation is performed."}
    target.mkdir(mode=0o700)
    for name, value in manifests.items():
        save(target / name, value)
    save(target / "preparation-evidence.json", evidence)
    print(json.dumps({"state": "prepared", "manifests": sorted(manifests), "validation": "not_started"}))


def validation(args, root, _config, specs, dataset, _hashes):
    require(LABEL.fullmatch(args.name), "invalid_validation_name")
    references = directory_path(args.references_directory or root / "references", private=True)
    workflows = copy.deepcopy(specs)
    for name, spec in workflows.items():
        spec["reference"] = {"path": str(file_path(references / (name + ".arrow"))), "engine": "clickhouse"}
    results, labels, units = [], set(), set()
    for label, report_path, runs_path in args.run:
        require(LABEL.fullmatch(label) and label not in labels, "invalid_or_duplicate_run_label")
        labels.add(label)
        report = read_json(Path(report_path))
        require(report.get("schema_version") == 1 and report.get("finished_at") and report.get("state") in
                ("execution_complete_validation_pending", "failed", "interrupted"), "measurement_report_not_finished")
        require(isinstance(report["schedule"], list) and 0 < len(report["schedule"]) <= 320, "invalid_measurement_schedule")
        root_runs = directory_path(runs_path)
        cases = {case["id"]: case for case in report["cases"]}
        require(len(cases) == len(report["cases"]), "duplicate_report_case")
        attempts = {item["sequence"]: item for item in report["results"]}
        require(len(attempts) == len(report["results"]), "duplicate_attempt_sequence")
        wanted = {item["unit"] for item in attempts.values()}
        require(len(wanted) == len(attempts) and not wanted & units and all(UNIT.fullmatch(unit) for unit in wanted), "duplicate_or_invalid_attempt_unit")
        units.update(wanted)
        indexed = {}
        for index, path in enumerate(root_runs.glob("analytics-*/*/measurement.json")):
            require(index < 20000, "too_many_retained_measurements")
            measurement = read_json(path)
            unit = measurement.get("unit")
            if unit in wanted:
                require(unit not in indexed, "duplicate_retained_unit")
                indexed[unit] = (path.parent, measurement)
        seen = set()
        for scheduled in report["schedule"]:
            sequence, trial, case_id = (scheduled[key] for key in ("sequence", "trial", "case"))
            require(type(sequence) is int and sequence > 0 and sequence not in seen and type(trial) is int and 1 <= trial <= 5, "invalid_schedule_identity")
            seen.add(sequence)
            case = cases[case_id]
            require(LABEL.fullmatch(case_id) and LABEL.fullmatch(case["engine"]) and case["workflow"] in WORKFLOWS, "invalid_case_contract")
            identity = label + "-" + case_id
            require(LABEL.fullmatch(identity), "combined_case_label_too_long")
            result = {"case": identity, "workflow": case["workflow"], "engine": case["engine"], "trial": trial, "path": None, "execution_state": "failed"}
            if sequence in attempts:
                attempt = attempts[sequence]
                require(attempt["trial"] == trial and all(attempt[key] == case[key] for key in ("id", "workflow", "engine", "panel")), "attempt_differs_from_schedule")
                require(attempt["unit"] in indexed, "retained_attempt_measurement_missing")
                directory, measurement = indexed[attempt["unit"]]
                require(measurement == attempt, "retained_measurement_differs_from_report")
                artifact = directory / "result.arrow"
                if artifact.exists() or artifact.is_symlink():
                    artifact = file_path(artifact)
                    require(artifact.is_relative_to(root_runs), "artifact_outside_runs_directory")
                    recorded = attempt.get("artifact_validation", {}).get("sha256")
                    if recorded:
                        require(SHA256.fullmatch(recorded) and fingerprint(artifact) == recorded, "retained_artifact_fingerprint_mismatch")
                        result["path"] = str(artifact)
                    # A failed attempt may have no authenticated output (for
                    # example an oversized partial). Keep that attempt failed.
                if attempt["trial_state"] == "execution_and_byte_checks_passed" and attempt["execution_state"] == "succeeded":
                    require(result["path"] is not None, "successful_attempt_artifact_missing")
                    result["execution_state"] = "succeeded"
            results.append(result)
        require(set(attempts) <= seen, "unscheduled_attempt_in_report")
        require(not report.get("error") or len(attempts) < len(seen)
                or any(item["trial_state"] != "execution_and_byte_checks_passed" for item in attempts.values()), "unrepresented_campaign_failure")
    require(0 < len(results) <= 1000, "invalid_validation_result_count")
    require(len({(item["case"], item["trial"]) for item in results}) == len(results), "duplicate_validation_identity")
    save(root / "private" / ("validation-" + args.name + ".json"), {"schema_version": 1, "dataset": dataset,
        "workflows": workflows, "results": results})
    print(json.dumps({"state": "prepared", "listed_attempts": len(results), "failed_or_unattempted": sum(item["execution_state"] == "failed" for item in results), "validation": "not_started"}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare", help="Generate private library, reference and 1/3-trial harness manifests")
    for name in ("source-database", "environment-file", "live-catalog", "file-catalog", "output-directory"):
        prepare_parser.add_argument("--" + name)
    validate_parser = commands.add_parser("validation", help="Index completed local or imported campaigns for independent validation")
    validate_parser.add_argument("--name", required=True)
    validate_parser.add_argument("--references-directory")
    validate_parser.add_argument("--run", action="append", nargs=3, required=True, metavar=("LABEL", "REPORT", "RUNS_ROOT"))
    args = parser.parse_args()
    require(sys.platform.startswith("linux"), "manifest_preparation_requires_linux")
    os.umask(0o077)
    inputs = load_inputs(args)
    (prepare if args.command == "prepare" else validation)(args, *inputs)


if __name__ == "__main__":
    try:
        main()
    except PreparationError as error:
        print(json.dumps({"state": "failed", "error": str(error)}), file=sys.stderr)
        raise SystemExit(1)
    except Exception:
        print('{"state":"failed","error":"manifest_preparation_failed_check_private_inputs"}', file=sys.stderr)
        raise SystemExit(1)
