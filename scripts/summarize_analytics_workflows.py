#!/usr/bin/env python3
"""Reconcile analytical campaign JSON with separately validated Arrow artifacts.

This offline, standard-library-only program reads JSON, never Arrow or database
inputs. Campaign LABEL=path namespaces a measurement id as LABEL-id in validator
results. Failed and unstarted scheduled trials remain in the output ledger.
Timing distributions include only verified successful complete-process times.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import statistics
import sys

LABEL = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,191}\Z")
VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
ERROR = re.compile(r"[a-z][a-z0-9_]{0,127}\Z")
MAX_JSON_BYTES = 64 << 20
STAGES = ("import_seconds", "setup_seconds", "compute_serialize_seconds", "teardown_seconds",
          "query_to_first_arrow_batch_seconds", "in_process_wall_seconds")
RUNTIME_NUMBERS = ("threads", "memory_budget_profile_mb", "engine_memory_limit_mb", "configured_temp_mb",
                   "polars_ooc_memory_budget_mb", "polars_ooc_disk_budget_mb")
LIMIT_KEYS = {"memory_max_mib", "threads", "tasks_max", "temp_mib", "max_output_bytes", "timeout_seconds",
              "memory_swap_max_bytes", "cpu_weight", "cpu_quota_percent", "cpu_quota_period_usec", "trials",
              "disk_reserve_bytes", "minimum_scratch_headroom_bytes", "log_max_bytes_per_stream", "sample_seconds",
              "status_poll_seconds"}


class SummaryError(Exception):
    """Carries fixed public codes, never arbitrary diagnostics or input paths."""


def require(condition, code):
    if not condition:
        raise SummaryError(code)


def number(value, positive=False, integer=False):
    if type(value) not in ((int,) if integer else (int, float)):
        return None
    if not math.isfinite(value) or value < 0 or (positive and value <= 0):
        return None
    return value


def mapping(value):
    return value if isinstance(value, dict) else {}


def label(value):
    require(isinstance(value, str) and LABEL.fullmatch(value) is not None, "invalid_public_identity")
    return value


def digest(value):
    return value if isinstance(value, str) and DIGEST.fullmatch(value) is not None else None


def codes(value):
    if not isinstance(value, list):
        return ["invalid_error_code_list"] if value is not None else []
    return sorted({entry if isinstance(entry, str) and ERROR.fullmatch(entry) else "unrecognized_error_code" for entry in value})


def stamp(value):
    try:
        return datetime.fromisoformat(value).isoformat() if isinstance(value, str) else None
    except ValueError:
        return None


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def distinct_object(pairs):
    output = {}
    for key, value in pairs:
        require(key not in output, "duplicate_json_object_key")
        output[key] = value
    return output


def read_input(value):
    path = Path(value)
    require(path.is_absolute() and not path.is_symlink(), "input_requires_absolute_regular_file")
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= MAX_JSON_BYTES, "input_type_or_size_invalid")
    data = path.read_bytes()
    require(len(data) <= MAX_JSON_BYTES, "input_byte_limit_exceeded")
    value = json.loads(data, object_pairs_hook=distinct_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(SummaryError("nonfinite_json_number")))
    require(isinstance(value, dict) and type(value.get("schema_version")) is int and value["schema_version"] == 1,
            "unsupported_report_schema")
    return value, {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def hash_mapping(value):
    require(isinstance(value, dict) and len(value) <= 64, "invalid_provenance_hash_mapping")
    result = {}
    for key, entry in value.items():
        label(key)
        require(digest(entry) is not None, "invalid_provenance_hash")
        result[key] = entry
    return result


def versions(value):
    if not isinstance(value, dict):
        return {}
    return {key: entry for key, entry in value.items() if key in ("duckdb", "polars", "pyarrow", "kelvo", "clickhouse", "python")
            and isinstance(entry, str) and VERSION.fullmatch(entry)}


def checks_passed(value):
    value = mapping(value)
    checks = value.get("checks")
    return value.get("state") == "passed" and isinstance(checks, dict) and bool(checks) and all(entry is True for entry in checks.values())


def dataset_metadata(report):
    result = {}
    for name, source in mapping(report.get("dataset")).items():
        if name not in ("trips", "zones") or not isinstance(source, dict):
            continue
        files = source.get("files", [])
        result[name] = {"rows": number(source.get("rows"), integer=True),
            "hashes_verified": source.get("hashes_verified") is True,
            "schema_verified": source.get("schema_verified") is True,
            "files": [{"sha256": digest(item.get("sha256")), "bytes": number(item.get("bytes"), integer=True),
                       "rows": number(item.get("rows"), integer=True)} for item in files if isinstance(item, dict)]}
    return result


def validation_context(report, workflow):
    reference = mapping(mapping(report.get("references")).get(workflow))
    reference_canonical = mapping(reference.get("canonical"))
    reference_file = mapping(reference.get("file"))
    metadata = dataset_metadata(report)
    dataset_valid = set(metadata) == {"trips", "zones"} and all(source["hashes_verified"] and source["schema_verified"]
        and source["files"] and all(item["sha256"] for item in source["files"]) for source in metadata.values())
    passed = (report.get("state") in ("passed", "failed") and stamp(report.get("finished_at")) is not None
        and checks_passed(report.get("raw_audit")) and checks_passed(report.get("independent_expectations"))
        and dataset_valid and reference.get("state") == "passed" and not reference.get("errors")
        and checks_passed(reference.get("invariants")) and digest(reference_canonical.get("canonical_value_sha256")) is not None
        and digest(reference_file.get("sha256")) is not None
        and reference_file.get("sha256") == reference_canonical.get("arrow_ipc_sha256")
        and number(reference_file.get("bytes"), positive=True, integer=True) is not None
        and reference_file.get("bytes") == reference_canonical.get("arrow_ipc_bytes")
        and number(reference_canonical.get("rows"), positive=True, integer=True) is not None)
    return {"passed": bool(passed), "dataset": metadata, "versions": versions(report.get("versions")),
        "validator_sha256": digest(report.get("validator_sha256")),
        "canonical_implementation_sha256": digest(report.get("canonical_implementation_sha256")),
        "reference_canonical_value_sha256": digest(reference_canonical.get("canonical_value_sha256")),
        "reference_rows": number(reference_canonical.get("rows"), integer=True)}


def index_validations(paths):
    index, sources, seen_files = {}, [], set()
    duplicates = 0
    for path in paths:
        report, file_info = read_input(path)
        if file_info["sha256"] in seen_files:
            duplicates += len(report.get("results", []))
            continue
        seen_files.add(file_info["sha256"])
        source_id = len(sources) + 1
        results = report.get("results")
        require(isinstance(results, list), "validation_results_must_be_array")
        source = {"index": source_id, **file_info, "state": report.get("state") if report.get("state") in ("passed", "failed", "running") else "unknown",
            "all_passed": report.get("all_passed") is True, "results": len(results),
            "started_at": stamp(report.get("started_at")), "finished_at": stamp(report.get("finished_at")),
            "manifest_sha256": digest(report.get("manifest_sha256")), "validator_sha256": digest(report.get("validator_sha256")),
            "canonical_implementation_sha256": digest(report.get("canonical_implementation_sha256")),
            "dataset": dataset_metadata(report), "versions": versions(report.get("versions")), "errors": codes(report.get("errors", []))}
        sources.append(source)
        within_file = set()
        for entry in results:
            require(isinstance(entry, dict), "invalid_validation_result")
            for key in ("case", "workflow", "engine"):
                label(entry.get(key))
            require(number(entry.get("trial"), positive=True, integer=True) is not None, "invalid_validation_trial")
            identity = (entry["case"], entry["trial"])
            require(identity not in within_file, "duplicate_validation_identity_within_report")
            within_file.add(identity)
            context = validation_context(report, entry["workflow"])
            if identity in index:
                prior = index[identity]
                require(canonical_json(prior["entry"]) == canonical_json(entry)
                        and canonical_json(prior["context"]) == canonical_json(context), "conflicting_duplicate_validation_identity")
                prior["source_indices"].append(source_id)
                duplicates += 1
            else:
                index[identity] = {"entry": entry, "context": context, "source_indices": [source_id], "used": False}
    return index, sources, duplicates


def distribution(values):
    present = [value for value in values if value is not None]
    return {"available": len(present), "missing": len(values) - len(present),
            "median": statistics.median(present) if present else None,
            "min": min(present) if present else None, "max": max(present) if present else None}


def peak(values):
    measured = distribution(values)
    return {"value": measured["max"], "available": measured["available"], "missing": measured["missing"]}


def source_counters(query):
    query = mapping(query)
    scans = query.get("federation")
    scans = scans if isinstance(scans, list) else []
    if scans:
        invocations = [number(mapping(scan).get("scans"), integer=True) for scan in scans]
        output = {"scope": "sum_of_recorded_federation_scan_counters", "recorded_scan_entries": len(scans),
                  "reported_scan_invocations": sum(invocations) if all(value is not None for value in invocations) else None}
        for source, target in (("rows_fetched", "rows_fetched"), ("arrow_bytes_fetched", "logical_arrow_bytes"),
                               ("source_wire_bytes", "encoded_response_body_bytes")):
            values = [number(mapping(scan).get(source), integer=True) for scan in scans]
            output[target] = sum(values) if all(value is not None for value in values) else None
            output[target + "_scans_with_counter"] = sum(value is not None for value in values)
        return output
    return {"scope": "top_level_response_counter_without_recorded_federation_scans", "recorded_scan_entries": 0,
            "reported_scan_invocations": None,
            "rows_fetched": None, "logical_arrow_bytes": None,
            "encoded_response_body_bytes": number(query.get("source_wire_bytes"), integer=True)}


def metrics(measurement):
    sampled = mapping(measurement.get("metrics"))
    accounting = mapping(measurement.get("systemd_accounting"))
    driver = mapping(measurement.get("driver_metrics"))
    memory = number(sampled.get("cgroup_memory_peak_bytes"), integer=True)
    memory_source = "cgroup_memory_peak" if memory is not None else None
    if memory is None:
        memory = number(accounting.get("MemoryPeak"), integer=True)
        memory_source = "systemd_memory_peak" if memory is not None else None
    cpu = number(accounting.get("CPUUsageNSec"))
    cpu_source = "systemd_final_cpu_usage" if cpu is not None else None
    if cpu is not None:
        cpu /= 1e9
    else:
        cpu = number(mapping(mapping(sampled.get("cgroup_last_observed")).get("cpu.stat")).get("usage_usec"))
        if cpu is not None:
            cpu /= 1e6
            cpu_source = "cgroup_last_observed_cpu_usage"
    return {"process_seconds": number(measurement.get("process_seconds_including_wrapper_and_fsync"), positive=True),
        "orchestration_wall_seconds": number(measurement.get("wall_seconds_including_service_start_and_status_poll")),
        "sampled_combined_process_rss_bytes": number(mapping(sampled.get("sampled_rss_bytes_peak")).get("combined"), integer=True),
        "charged_cgroup_memory_peak_bytes": memory, "charged_cgroup_memory_source": memory_source,
        "sampled_cgroup_memory_current_peak_bytes": number(sampled.get("cgroup_memory_current_bytes_sampled_peak"), integer=True),
        "service_cpu_seconds": cpu, "service_cpu_source": cpu_source,
        "sampled_scratch_allocated_bytes": number(mapping(sampled.get("scratch_files_bytes_peak")).get("allocated_bytes"), integer=True),
        "sampled_scratch_logical_bytes": number(mapping(sampled.get("scratch_files_bytes_peak")).get("logical_bytes"), integer=True),
        "maximum_sample_gap_seconds": number(sampled.get("maximum_sample_gap_seconds")),
        "sampling_errors": codes(sampled.get("sampling_errors", [])),
        "host_steal_fraction": number(mapping(sampled.get("host_cpu_delta")).get("steal_fraction")),
        "source": source_counters(measurement.get("query_stats")),
        "library_stages_seconds": {key: number(driver.get(key)) for key in STAGES if key in driver},
        "runtime": {"versions": versions(driver.get("versions")),
                    "result_compression": driver.get("result_compression") if driver.get("result_compression") in ("none", "lz4_frame") else None,
                    **{key: number(driver.get(key)) for key in RUNTIME_NUMBERS if key in driver}}}


def attempt(campaign_label, case, scheduled, measurement, validation_index):
    joined_name = campaign_label + "-" + case["id"]
    output = {"sequence": scheduled["sequence"], "trial": scheduled["trial"], "validation_case": joined_name,
              "recorded": measurement is not None, "state": "failed", "timing_eligible": False, "errors": []}
    evidence = validation_index.get((joined_name, scheduled["trial"]))
    if evidence is not None:
        require(not evidence["used"], "validation_identity_matched_multiple_campaign_attempts")
        evidence["used"] = True
        output["validation_source_indices"] = evidence["source_indices"]
    if measurement is None:
        output["state"] = "missing"
        output["errors"].append("scheduled_measurement_missing")
        output["matching_validation_present"] = evidence is not None
        return output
    output["metrics"] = metrics(measurement)
    output["measurement_errors"] = codes(measurement.get("errors", []))
    output["final_status_uncertain"] = measurement.get("final_status_uncertain") is True
    output["oom_event_observed"] = measurement.get("oom_event_observed") is True
    output["oom_kill_observed"] = measurement.get("oom_kill_observed", measurement.get("oom_observed")) is True
    output["task_limit_event_observed"] = measurement.get("task_limit_event_observed") is True
    cleanup = mapping(measurement.get("unit_cleanup"))
    output["cleanup"] = {"cgroup_empty": cleanup.get("cgroup_empty") if type(cleanup.get("cgroup_empty")) is bool else None,
                         "stop_exit_status": number(cleanup.get("stop_exit_status"), integer=True)}
    execution_ok = (measurement.get("execution_state") == "succeeded"
        and measurement.get("trial_state") == "execution_and_byte_checks_passed"
        and measurement.get("process_exit_status_verified") is True and measurement.get("service_limits_verified") is True
        and cleanup.get("cgroup_empty") is True and cleanup.get("stop_exit_status") == 0
        and not measurement.get("errors"))
    output["execution_checks_passed"] = bool(execution_ok)
    if not execution_ok:
        output["errors"].append("measurement_execution_checks_failed")
    byte_evidence = mapping(measurement.get("artifact_validation"))
    measured_hash = digest(byte_evidence.get("sha256"))
    output["output_bytes"] = number(byte_evidence.get("bytes"), integer=True)
    output["measured_artifact_sha256"] = measured_hash
    if evidence is None:
        output["matching_validation_present"] = False
        output["errors"].append("matching_validation_missing")
        return output
    entry, context = evidence["entry"], evidence["context"]
    named_inputs = {name for name in ("trips", "zones") if name in case["input_hashes"]}
    if case["panel"] == "snapshot":
        named_inputs = {"trips", "zones"}
    if named_inputs:
        input_binding = all(len(context["dataset"].get(name, {}).get("files", [])) == 1
            and case["input_hashes"].get(name) == context["dataset"][name]["files"][0]["sha256"] for name in named_inputs)
        output["input_provenance_binding"] = "matched_declared_parquet_hashes" if input_binding else "mismatch"
        if not input_binding:
            output["errors"].append("declared_parquet_inputs_differ_from_validated_snapshot")
    else:
        output["input_provenance_binding"] = "live_source_provenance_declared_not_recomputed_by_summarizer"
    identities_ok = entry.get("workflow") == case["workflow"] and entry.get("engine") == case["engine"]
    output["matching_validation_present"] = identities_ok
    if not identities_ok:
        output["errors"].append("validation_workflow_or_engine_mismatch")
    if entry.get("execution_state") != measurement.get("execution_state"):
        output["errors"].append("validation_execution_state_mismatch")
    artifact = mapping(entry.get("artifact"))
    canonical = mapping(artifact.get("canonical"))
    file_info = mapping(artifact.get("file"))
    validated_hash = digest(file_info.get("sha256"))
    hash_match = measured_hash is not None and measured_hash == validated_hash
    size_match = output["output_bytes"] is not None and output["output_bytes"] == number(file_info.get("bytes"), integer=True)
    canonical_file_match = (validated_hash is not None and canonical.get("arrow_ipc_sha256") == validated_hash
        and number(file_info.get("bytes"), positive=True, integer=True) is not None
        and canonical.get("arrow_ipc_bytes") == file_info["bytes"])
    output["artifact_sha256_matches_decoded_file"] = hash_match
    output["artifact_byte_count_matches_decoded_file"] = size_match
    output["canonical_reader_fingerprint_matches_validated_file"] = canonical_file_match
    output["validation_errors"] = codes(entry.get("errors", []))
    output["artifact_validation_errors"] = codes(artifact.get("errors", []))
    output["decoded_output_rows"] = number(canonical.get("rows"), integer=True)
    output["canonical_value_sha256"] = digest(canonical.get("canonical_value_sha256"))
    canonical_ok = (output["canonical_value_sha256"] is not None and canonical_file_match
        and number(output["decoded_output_rows"], positive=True, integer=True) is not None
        and number(context["reference_rows"], positive=True, integer=True) is not None
        and output["canonical_value_sha256"] == context["reference_canonical_value_sha256"]
        and output["decoded_output_rows"] == context["reference_rows"])
    value_ok = (identities_ok and context["passed"] and entry.get("state") == "passed" and not entry.get("errors")
        and entry.get("matches_direct_clickhouse_canonical_values") is True and artifact.get("state") == "passed"
        and not artifact.get("errors") and checks_passed(artifact.get("invariants")) and canonical_ok)
    output["value_checks_passed"] = bool(value_ok)
    if not value_ok:
        output["errors"].append("separate_value_validation_failed")
    if not hash_match or not size_match:
        output["errors"].append("measured_artifact_not_bound_to_validated_file")
    if not canonical_file_match:
        output["errors"].append("validated_fingerprint_disagrees_with_canonical_reader")
    if byte_evidence.get("state") != "byte_integrity_observed" or byte_evidence.get("arrow_eos_present") is not True:
        output["errors"].append("measurement_byte_integrity_not_established")
    if output["metrics"]["process_seconds"] is None:
        output["errors"].append("positive_complete_process_time_unavailable")
    output["timing_eligible"] = not output["errors"]
    output["state"] = "passed" if output["timing_eligible"] else "failed"
    return output


def case_summary(case, attempts):
    recorded = [item for item in attempts if item["recorded"]]
    verified = [item for item in attempts if item["timing_eligible"]]
    runtime_sets = {canonical_json(item["metrics"]["runtime"]) for item in verified}
    value_hashes = {item["canonical_value_sha256"] for item in verified}
    row_counts = sorted({item["decoded_output_rows"] for item in verified})
    errors = []
    if len(runtime_sets) > 1:
        errors.append("runtime_versions_or_settings_changed_between_verified_trials")
    if len(value_hashes) > 1 or len(row_counts) > 1:
        errors.append("validated_values_changed_between_verified_trials")
    aggregate_eligible = verified if not errors else []
    measured = [item["metrics"] for item in recorded]
    stage_names = sorted({name for item in aggregate_eligible for name in item["metrics"]["library_stages_seconds"]})
    output = {**case, "planned": len(attempts), "recorded": len(recorded), "missing": len(attempts) - len(recorded),
        "verified": len(verified), "failed_recorded": len(recorded) - len(verified), "errors": errors,
        "all_planned_verified": bool(attempts) and len(verified) == len(attempts) and not errors,
        "latency_statistics_scope": "verified_complete_process_times_only",
        "process_seconds": distribution([item["metrics"]["process_seconds"] for item in aggregate_eligible]),
        "output_rows": row_counts[0] if len(row_counts) == 1 else None, "observed_verified_row_counts": row_counts,
        "encoded_output_bytes_all_recorded_attempts": distribution([item.get("output_bytes") for item in recorded]),
        "resource_statistics_scope": "all_recorded_attempts_including_failures",
        "peak_sampled_combined_process_rss_bytes": peak([item["sampled_combined_process_rss_bytes"] for item in measured]),
        "peak_charged_cgroup_memory_bytes": peak([item["charged_cgroup_memory_peak_bytes"] for item in measured]),
        "charged_cgroup_memory_sources": sorted({item["charged_cgroup_memory_source"] for item in measured if item["charged_cgroup_memory_source"]}),
        "peak_sampled_scratch_allocated_bytes": peak([item["sampled_scratch_allocated_bytes"] for item in measured]),
        "maximum_sample_gap_seconds": peak([item["maximum_sample_gap_seconds"] for item in measured]),
        "service_cpu_seconds_all_recorded_attempts": distribution([item["service_cpu_seconds"] for item in measured]),
        "source_observations_all_recorded_attempts": {name: distribution([item["source"][name] for item in measured])
            for name in ("rows_fetched", "logical_arrow_bytes", "encoded_response_body_bytes")},
        "runtime_variants_among_verified_trials": [json.loads(value) for value in sorted(runtime_sets)],
        "library_stages_seconds_verified_trials": {name: distribution([item["metrics"]["library_stages_seconds"].get(name) for item in aggregate_eligible]) for name in stage_names},
        "attempts": attempts}
    output["headline_comparison_eligible"] = output["all_planned_verified"]
    return output


def summarize_campaign(campaign_label, report, file_info, validations):
    profile = label(report.get("profile"))
    require(isinstance(report.get("cases"), list) and bool(report["cases"]), "campaign_cases_required")
    require(isinstance(report.get("schedule"), list) and bool(report["schedule"]), "campaign_schedule_required")
    require(isinstance(report.get("results"), list), "campaign_results_required")
    declared_trials = number(mapping(report.get("limits")).get("trials"), positive=True, integer=True)
    require(declared_trials is not None and declared_trials <= 1000, "campaign_declared_trial_count_invalid")
    cases = {}
    for source in report["cases"]:
        require(isinstance(source, dict), "invalid_campaign_case")
        case = {key: label(source.get(key)) for key in ("id", "workflow", "engine", "panel")}
        require(case["id"] not in cases, "duplicate_campaign_case")
        case.update({"input_hashes": hash_mapping(source.get("input_hashes", {})),
                     "code_hashes": hash_mapping(source.get("code_hashes", {})),
                     "executable_sha256": digest(source.get("executable_sha256"))})
        cases[case["id"]] = case
    schedule, sequences = {}, set()
    for entry in report["schedule"]:
        require(isinstance(entry, dict) and entry.get("case") in cases, "invalid_scheduled_case")
        require(number(entry.get("trial"), positive=True, integer=True) is not None
                and number(entry.get("sequence"), positive=True, integer=True) is not None, "invalid_schedule_identity")
        identity = (entry["case"], entry["trial"])
        require(identity not in schedule and entry["sequence"] not in sequences, "duplicate_schedule_identity")
        schedule[identity] = {key: entry[key] for key in ("sequence", "trial", "case")}
        sequences.add(entry["sequence"])
    required_schedule = {(name, trial) for name in cases for trial in range(1, declared_trials + 1)}
    require(set(schedule) == required_schedule and sequences == set(range(1, len(schedule) + 1)), "schedule_inconsistent_with_declared_cases_and_trials")
    measurements, unexpected = {}, []
    for entry in report["results"]:
        require(isinstance(entry, dict), "invalid_measurement_result")
        name = label(entry.get("id"))
        require(number(entry.get("trial"), positive=True, integer=True) is not None, "invalid_measurement_trial")
        identity = (name, entry["trial"])
        require(identity not in measurements, "duplicate_measurement_identity")
        measurements[identity] = entry
        if identity not in schedule:
            unexpected.append({"id": name, "trial": entry["trial"], "errors": ["measurement_not_in_declared_schedule"],
                "measurement_errors": codes(entry.get("errors", [])), "metrics": metrics(entry),
                "measured_artifact_sha256": digest(mapping(entry.get("artifact_validation")).get("sha256")),
                "output_bytes": number(mapping(entry.get("artifact_validation")).get("bytes"), integer=True)})
            continue
        require(entry.get("sequence") == schedule[identity]["sequence"], "measurement_sequence_differs_from_schedule")
        require(all(entry.get(key) == cases[name][key] for key in ("workflow", "engine", "panel")), "measurement_metadata_differs_from_case")
    summaries = []
    for name, case in cases.items():
        planned = sorted((entry for key, entry in schedule.items() if key[0] == name), key=lambda item: item["sequence"])
        attempts = [attempt(campaign_label, case, entry, measurements.get((name, entry["trial"])), validations) for entry in planned]
        summaries.append(case_summary(case, attempts))
    limits = {key: value for key, value in mapping(report.get("limits")).items() if key in LIMIT_KEYS and number(value) is not None}
    filesystem = mapping(report.get("filesystem"))
    host = mapping(report.get("host"))
    output = {"label": campaign_label, "profile": profile, "input": file_info,
        "source_state": report.get("state") if report.get("state") in ("running", "failed", "interrupted", "execution_complete_validation_pending") else "unknown",
        "source_all_execution_and_byte_checks_passed": report.get("all_execution_and_byte_checks_passed") is True,
        "started_at": stamp(report.get("started_at")), "finished_at": stamp(report.get("finished_at")),
        "harness_sha256": digest(report.get("harness_sha256")), "shared_sampler_sha256": digest(report.get("shared_sampler_sha256")),
        "limits": limits, "host": {key: value for key, value in host.items() if
            (key == "logical_cpus" and number(value, positive=True, integer=True) is not None)
            or (key in ("machine", "kernel") and isinstance(value, str) and VERSION.fullmatch(value))},
        "filesystem": {"type": filesystem.get("type") if isinstance(filesystem.get("type"), str) and VERSION.fullmatch(filesystem["type"]) else None,
            "capacity_scope": "shared_workdir_filesystem_including_all_retained_trials_and_other_files"
                if filesystem.get("capacity_scope") == "shared_workdir_filesystem_including_all_retained_trials_and_other_files" else None,
            "per_trial_scratch_limit_enforcement": "sampled_soft_limit"
                if filesystem.get("per_trial_scratch_limit_enforcement") == "sampled_soft_limit" else None,
            **{key: number(filesystem.get(key), integer=True) for key in ("capacity_bytes", "available_bytes_at_start")}},
        "planned": len(schedule), "recorded": len(report["results"]), "missing": sum(item["missing"] for item in summaries),
        "verified": sum(item["verified"] for item in summaries), "failed_recorded": sum(item["failed_recorded"] for item in summaries) + len(unexpected),
        "complete_declared_schedule": set(measurements) == set(schedule),
        "unexpected_measurements": unexpected, "cases": summaries, "errors": []}
    if not output["complete_declared_schedule"]:
        output["errors"].append("campaign_schedule_not_fully_reconciled")
    if report.get("state") != "execution_complete_validation_pending" or report.get("all_execution_and_byte_checks_passed") is not True:
        output["errors"].append("campaign_report_contains_failure_or_is_not_complete")
    if "error" in report:
        output["campaign_error_codes"] = codes([report["error"]])
        output["errors"].append("campaign_report_error_present")
    output["all_planned_verified"] = (not output["errors"] and bool(summaries) and all(case["all_planned_verified"] for case in summaries))
    return output


def write_output(path, value):
    temporary = path.with_name(path.name + ".tmp-" + secrets.token_hex(6))
    try:
        with temporary.open("x") as stream:
            json.dump(value, stream, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Example: --campaign azure-snapshot=/ABS/snapshot.json
--campaign azure-live=/ABS/live.json --validation /ABS/validated-measured.json
--output /ABS/new-summary.json
Each campaign label must be the exact validator case prefix before a hyphen.
Identical repeated validation entries are deduplicated; conflicting entries or
contexts for the same case/trial are rejected. Unused validation entries are
listed separately, so preflight results cannot silently enter measured medians.
Exit 0 means all supplied campaigns completed every declared verified trial.
Exit 1 also covers valid summaries containing failed or missing attempts.
No Arrow libraries, source connections, benchmark processes, or VM calls occur.
''')
    parser.add_argument("--campaign", action="append", required=True, metavar="LABEL=ABS_JSON")
    parser.add_argument("--validation", action="append", required=True, metavar="ABS_JSON")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        require(args.output.is_absolute() and args.output.parent.is_dir() and not args.output.exists()
                and not args.output.is_symlink(), "output_must_be_new_absolute_file")
        require(1 <= len(args.campaign) <= 64 and 1 <= len(args.validation) <= 64, "input_report_count_out_of_bounds")
    except SummaryError as error:
        parser.error(str(error))
    summary = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
        "state": "failed", "all_planned_verified": False, "errors": [], "campaigns": [],
        "summarizer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "definitions": {
            "process_seconds": "Verified systemd process start/exit duration including wrapper, interpreter/imports, output and fsync; no orchestration-time substitution.",
            "partial_success": "A case may show a successful-only timing distribution while planned, failed and missing counts remain visible. Headline eligibility requires every planned trial verified.",
            "resources": "Maximum observations across all recorded attempts, including failures; missing counters stay null. RSS is simultaneous sampled process sum; charged cgroup memory is a separate kernel/systemd high-water measure.",
            "source": "Sum every recorded federation scan, including repeated scans. Never add top-level source counters to scan totals. Without scans, only the available top-level response-body counter is reported. Missing scan counters make their sum unavailable.",
            "units": "Durations are seconds and byte fields are bytes. Reported engine *_mb settings retain their original units.",
            "scope": "Campaigns, cases, profiles and input/code/runtime configurations remain separate. Only explicit campaign schedules contribute timings; unused validation entries are listed separately.",
            "trust": "This offline report binds measurement IPC hashes to separately validated files. It does not reopen Arrow artifacts, rerun raw audits, or authenticate report provenance beyond recording input SHA256 values."}}
    try:
        validations, sources, duplicates = index_validations(args.validation)
        summary["validation_inputs"] = sources
        summary["identical_validation_entries_deduplicated"] = duplicates
        names, report_hashes = set(), set()
        for argument in args.campaign:
            require("=" in argument, "campaign_requires_label_equals_path")
            name, path = argument.split("=", 1)
            label(name)
            require(name not in names, "duplicate_campaign_label")
            names.add(name)
            report, file_info = read_input(path)
            require(file_info["sha256"] not in report_hashes, "same_campaign_report_supplied_twice")
            report_hashes.add(file_info["sha256"])
            summary["campaigns"].append(summarize_campaign(name, report, file_info, validations))
        summary["unused_validation_entries"] = [{"case": key[0], "trial": key[1], "workflow": evidence["entry"]["workflow"],
            "engine": evidence["entry"]["engine"], "state": evidence["entry"].get("state") if evidence["entry"].get("state") in ("passed", "failed") else "unknown",
            "validation_source_indices": evidence["source_indices"]} for key, evidence in sorted(validations.items()) if not evidence["used"]]
        summary["counts"] = {key: sum(campaign[key] for campaign in summary["campaigns"])
                             for key in ("planned", "recorded", "missing", "verified", "failed_recorded")}
        summary["all_planned_verified"] = bool(summary["campaigns"]) and all(campaign["all_planned_verified"] for campaign in summary["campaigns"])
    except SummaryError as error:
        summary["errors"].append(str(error))
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        summary["errors"].append("input_report_or_output_data_invalid")
    summary["state"] = "passed" if summary["all_planned_verified"] and not summary["errors"] else "failed"
    write_output(args.output, summary)
    print(json.dumps({"state": summary["state"], "counts": summary.get("counts"), "errors": summary["errors"]}))
    return 0 if summary["state"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
