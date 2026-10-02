#!/usr/bin/env python3
"""Regression checks against recorded analytical JSON evidence; no Arrow needed.

Inputs are read-only. Mutations are copies used solely to test whether evidence
tampering, missing attempts and failed resource observations are handled safely.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import sys
import tempfile

sys.dont_write_bytecode = True

import summarize_analytics_workflows as summary


def check(condition, name):
    if not condition:
        raise AssertionError(name)


def changed_hash(value):
    return ("1" if value[0] == "0" else "0") + value[1:]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True, type=Path)
    parser.add_argument("--validation", required=True, type=Path)
    parser.add_argument("--label", default="azure-snapshot")
    args = parser.parse_args()
    args.campaign, args.validation = args.campaign.resolve(strict=True), args.validation.resolve(strict=True)
    campaign, campaign_info = summary.read_input(args.campaign)
    validation, _ = summary.read_input(args.validation)
    completed = []

    def summarize(value):
        # The reconciliation API consumes validation identities. Each scenario
        # gets a fresh index, including all unrelated real validation entries.
        index, _, _ = summary.index_validations([args.validation])
        return summary.summarize_campaign(args.label, value, campaign_info, index)

    baseline = summarize(campaign)
    check(baseline["planned"] == baseline["recorded"] == baseline["verified"] == 36
          and baseline["missing"] == baseline["failed_recorded"] == 0
          and baseline["all_planned_verified"], "recorded_36_attempt_baseline_must_pass")
    check(all(case["headline_comparison_eligible"] and case["process_seconds"]["available"] == 3
              for case in baseline["cases"]), "baseline_requires_three_verified_trials_per_case")
    completed.append("recorded_36_attempt_baseline")
    selected = campaign["results"][0]

    def selected_case(result):
        return next(case for case in result["cases"] if case["id"] == selected["id"])

    def selected_attempt(result):
        return next(item for item in selected_case(result)["attempts"] if item["trial"] == selected["trial"])

    tampered = copy.deepcopy(campaign)
    artifact = tampered["results"][0]["artifact_validation"]
    artifact["sha256"] = changed_hash(artifact["sha256"])
    observed = summarize(tampered)
    check(observed["planned"] == 36 and observed["verified"] == 35 and not observed["all_planned_verified"],
          "tampered_artifact_must_reduce_verified_count")
    check(not selected_attempt(observed)["timing_eligible"]
          and not selected_attempt(observed)["artifact_sha256_matches_decoded_file"]
          and not selected_case(observed)["headline_comparison_eligible"], "tampered_artifact_must_not_enter_headline")
    completed.append("artifact_sha_tampering_rejected")

    missing = copy.deepcopy(campaign)
    missing["results"].pop(0)
    observed = summarize(missing)
    check(observed["planned"] == 36 and observed["recorded"] == observed["verified"] == 35
          and observed["missing"] == 1 and not observed["all_planned_verified"], "missing_attempt_must_remain_in_total")
    check(selected_attempt(observed)["state"] == "missing"
          and not selected_case(observed)["headline_comparison_eligible"], "missing_attempt_must_not_disappear")
    completed.append("scheduled_missing_attempt_retained")

    failed = copy.deepcopy(campaign)
    failed["state"], failed["all_execution_and_byte_checks_passed"] = "failed", False
    attempt = failed["results"][0]
    attempt.update({"execution_state": "failed", "trial_state": "failed", "errors": ["injected_test_failure"],
                    "oom_event_observed": True, "oom_kill_observed": True})
    # Inject a new maximum into a failed attempt. This makes a success-only
    # resource aggregation visibly wrong without asserting a timing formula.
    memory = 1_048_576 + max(case["peak_charged_cgroup_memory_bytes"]["value"] or 0 for case in baseline["cases"])
    rss = 1_048_576 + max(case["peak_sampled_combined_process_rss_bytes"]["value"] or 0 for case in baseline["cases"])
    attempt.setdefault("metrics", {})["cgroup_memory_peak_bytes"] = memory
    attempt["metrics"].setdefault("sampled_rss_bytes_peak", {})["combined"] = rss
    observed = summarize(failed)
    case, failed_attempt = selected_case(observed), selected_attempt(observed)
    check(observed["planned"] == 36 and observed["failed_recorded"] == 1 and observed["verified"] == 35
          and not observed["all_planned_verified"], "failed_attempt_must_remain_failed")
    check(case["peak_charged_cgroup_memory_bytes"]["value"] == memory
          and case["peak_sampled_combined_process_rss_bytes"]["value"] == rss
          and failed_attempt["oom_kill_observed"], "failed_resource_peak_must_be_retained")
    check(case["process_seconds"]["available"] == 2 and not case["headline_comparison_eligible"]
          and not failed_attempt["timing_eligible"], "successful_subset_must_not_be_a_complete_headline")
    completed.append("failed_resource_peak_retained_without_headline")

    conflicting = copy.deepcopy(validation)
    target = next(item for item in conflicting["results"]
                  if item["case"] == args.label + "-" + selected["id"] and item["trial"] == selected["trial"])
    canonical = target["artifact"]["canonical"]
    canonical["canonical_value_sha256"] = changed_hash(canonical["canonical_value_sha256"])
    with tempfile.TemporaryDirectory(prefix="kelvo-evidence-regression-") as directory:
        duplicate = Path(directory) / "conflicting-validation.json"
        with duplicate.open("x") as stream:
            json.dump(conflicting, stream, allow_nan=False)
        try:
            summary.index_validations([args.validation, duplicate])
        except summary.SummaryError as error:
            check(str(error) == "conflicting_duplicate_validation_identity", "duplicate_must_fail_for_conflicting_identity")
        else:
            raise AssertionError("conflicting_duplicate_validation_was_accepted")
    completed.append("conflicting_duplicate_validation_rejected")
    print(json.dumps({"state": "passed", "fixture_attempts": 36, "checks": completed,
                      "scope": "JSON regression checks only; no benchmarks or Arrow validation executed"}))


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, summary.SummaryError) as error:
        print(json.dumps({"state": "failed", "check": str(error)}), file=sys.stderr)
        raise SystemExit(1)
