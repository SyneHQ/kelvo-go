#!/usr/bin/env python3
"""Measure real closed-loop federation jobs on the isolated Linux fixture.

Every submission executes a fresh two-source join. Exact native reference
values, successful Arrow EOS, tenant binding, and source scan statistics are
checked. This script neither builds software nor changes fixture limits.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import pyarrow as pa

from federation_capacity import QUERIES, canonical_arrow
from federation_capacity_cluster import DIR, matches, process_records, read_private

ROOT = Path(__file__).resolve().parents[1]
EOS = b"\xff\xff\xff\xff\0\0\0\0"
TERMINAL = {"succeeded", "failed", "cancelled"}


class Failure(Exception):
    def __init__(self, code, status=None):
        super().__init__(code)
        self.code, self.status = code, status


def safe_code(value, fallback="UNKNOWN"):
    return value if isinstance(value, str) and re.fullmatch(r"[A-Z_]{1,64}", value) else fallback


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    low = int(index)
    return ordered[low] + (ordered[min(low + 1, len(ordered) - 1)] - ordered[low]) * (index - low)


class Client:
    def __init__(self, manifest):
        self.manifest = manifest
        environment = read_private(manifest["environment_file"])
        self.tokens = {tenant: environment[name] for tenant, name in manifest["token_envs"].items()}
        context = ssl.create_default_context(cafile=manifest["ca_file"])
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=context))
        for address in manifest["gateway_urls"]:
            parsed = urllib.parse.urlsplit(address)
            if parsed.scheme != "https" or parsed.hostname != "127.0.0.1":
                raise Failure("INVALID_FIXTURE_GATEWAY")

    def open(self, tenant, gateway, path, body=None, headers=None, timeout=135):
        request = urllib.request.Request(self.manifest["gateway_urls"][gateway] + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + self.tokens[tenant],
                     "Content-Type": "application/json", **(headers or {})})
        try:
            return self.opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            try:
                payload = json.loads(error.read(65536))
                code = safe_code(payload.get("error", {}).get("code"), "HTTP_ERROR")
            except (ValueError, AttributeError):
                code = "HTTP_ERROR"
            raise Failure(code, error.code) from None

    def json(self, tenant, gateway, path, body=None, **kwargs):
        with self.open(tenant, gateway, path, body, **kwargs) as response:
            return json.load(response)

    def cancel(self, tenant, gateway, identifier):
        try:
            self.json(tenant, gateway, "/v1/queries/" + identifier + "/cancel", {}, timeout=5)
        except (Failure, OSError):
            pass

    def denied(self, tenant, gateway, path, headers=None):
        try:
            with self.open(tenant, gateway, path, headers=headers) as response:
                response.read(65536)
        except Failure as failure:
            if failure.status == 404:
                return
            raise
        raise Failure("TENANT_HANDLE_EXPOSED")


def isolation(client):
    evidence = []
    for index, tenant in enumerate(("a", "b")):
        other = "b" if tenant == "a" else "a"
        identifier = client.json(tenant, index, "/v1/queries",
            {"mode": "federated", "sources": ["tenant_marker"], "sql": "SELECT tenant FROM tenant_marker"})["id"]
        path = "/v1/queries/" + identifier
        try:
            for suffix in ("", "/results"):
                client.denied(other, 1 - index, path + suffix, {"X-Kelvo-Tenant": tenant})
            with client.open(tenant, 1 - index, path + "/results") as response:
                data = response.read(1 << 20)
            if not data.endswith(EOS):
                raise Failure("MISSING_ARROW_EOS")
            with pa.ipc.open_stream(io.BytesIO(data)) as reader:
                if reader.read_all().to_pylist() != [{"tenant": tenant}]:
                    raise Failure("TENANT_MARKER_MISMATCH")
            if client.json(tenant, index, path)["state"] != "succeeded":
                raise Failure("TENANT_MARKER_NOT_SUCCESSFUL")
            for suffix in ("", "/results"):
                client.denied(other, index, path + suffix, {"X-Kelvo-Tenant": tenant})
            evidence.append({"tenant": tenant, "synthetic_marker": tenant,
                "cross_gateway": True, "cross_tenant_before_after_denied": True})
        finally:
            client.cancel(tenant, index, identifier)
    return evidence


def admission_control(client, maximum):
    """Fill bounded unconsumed handles; do not confuse this with query rate."""
    held = []
    try:
        for _ in range(maximum):
            held.append(client.json("a", 0, "/v1/queries", {"mode": "federated",
                "sources": ["tenant_marker"], "sql": "SELECT tenant FROM tenant_marker"})["id"])
        try:
            unexpected = client.json("a", 1, "/v1/queries", {"mode": "federated",
                "sources": ["tenant_marker"], "sql": "SELECT tenant FROM tenant_marker"})
            held.append(unexpected["id"])
            raise Failure("ADMISSION_LIMIT_NOT_ENFORCED")
        except Failure as failure:
            if failure.status != 429 or failure.code != "RESOURCE_EXHAUSTED":
                raise
        identifier = client.json("b", 1, "/v1/queries", {"mode": "federated",
            "sources": ["tenant_marker"], "sql": "SELECT tenant FROM tenant_marker"})["id"]
        try:
            with client.open("b", 0, "/v1/queries/" + identifier + "/results") as response:
                data = response.read(1 << 20)
            with pa.ipc.open_stream(io.BytesIO(data)) as reader:
                if not data.endswith(EOS) or reader.read_all().to_pylist() != [{"tenant": "b"}]:
                    raise Failure("ADMISSION_TENANT_CONTROL_FAILED")
        finally:
            client.cancel("b", 0, identifier)
        return {"tenant_a_retained_nonterminal_handles": len(held), "overflow_http_status": 429,
            "overflow_error": "RESOURCE_EXHAUSTED", "tenant_b_query_succeeded": True,
            "scope": "Separate bounded admission negative control; excluded from sustained throughput."}
    finally:
        for identifier in held:
            client.cancel("a", 0, identifier)
        deadline = time.monotonic() + 10
        pending = set(held)
        while pending and time.monotonic() < deadline:
            for identifier in list(pending):
                try:
                    if client.json("a", 0, "/v1/queries/" + identifier)["state"] in TERMINAL:
                        pending.remove(identifier)
                except Failure as failure:
                    if failure.status == 404:
                        pending.remove(identifier)
                    else:
                        raise
            if pending:
                time.sleep(0.05)
        if pending:
            raise Failure("ADMISSION_CONTROL_DID_NOT_DRAIN")


def proc(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
        if fields[0] in ("Z", "X"):
            return None
        return {"pid": pid, "ppid": int(fields[1]), "start": int(fields[19]),
            "ticks": int(fields[11]) + int(fields[12]),
            "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE")}
    except (OSError, ValueError, IndexError):
        return None


class Sampler:
    def __init__(self, fixture, interval=0.1):
        self.interval, self.stop_event = interval, threading.Event()
        self.roots = {}
        for item in process_records():
            if matches(item) and not item["name"].endswith("-launcher"):
                role = "broker" if item["name"].startswith("nats-") else "gateway" if item["name"].startswith("gateway") else "node"
                self.roots[item["pid"]] = role
        self.roots[os.getpid()] = "load_client"
        if fixture:
            source_manifest = json.loads(Path(fixture).read_text())
            for kind, name in source_manifest["containers"].items():
                result = subprocess.run(["sudo", "-n", "docker", "inspect", "--format", "{{.State.Pid}}", name],
                    capture_output=True, text=True, timeout=10)
                if result.returncode or not result.stdout.strip().isdigit():
                    raise Failure("SOURCE_PROCESS_UNAVAILABLE")
                self.roots[int(result.stdout.strip())] = "source_" + kind
        self.records, self.peaks, self.samples = {}, defaultdict(int), 0
        self.max_workers, self.current_workers = 0, 0
        self.thread = threading.Thread(target=self.run, daemon=True)

    def sample(self):
        all_processes = {}
        for path in Path("/proc").iterdir():
            if path.name.isdigit():
                item = proc(int(path.name))
                if item:
                    all_processes[item["pid"]] = item
        selected = {pid: role for pid, role in self.roots.items() if pid in all_processes}
        changed = True
        while changed:
            changed = False
            for pid, item in all_processes.items():
                if pid not in selected and item["ppid"] in selected:
                    parent_role = selected[item["ppid"]]
                    selected[pid] = "query_worker" if parent_role in ("node", "query_worker") else parent_role
                    changed = True
        totals, workers = defaultdict(int), 0
        for pid, role in selected.items():
            item = all_processes[pid]
            key = (pid, item["start"])
            if key not in self.records:
                self.records[key] = {"role": role, "first": item["ticks"] if self.samples == 0 else 0,
                                     "last": item["ticks"], "peak_rss_bytes": 0}
            record = self.records[key]
            record["last"] = item["ticks"]
            record["peak_rss_bytes"] = max(record["peak_rss_bytes"], item["rss_bytes"])
            totals[role] += item["rss_bytes"]
            workers += role == "query_worker"
        for role, value in totals.items():
            self.peaks[role] = max(self.peaks[role], value)
        self.max_workers = max(self.max_workers, workers)
        self.current_workers = workers
        self.samples += 1

    def run(self):
        while not self.stop_event.wait(self.interval):
            self.sample()

    def start(self):
        self.sample()
        if self.current_workers:
            raise Failure("PREEXISTING_QUERY_WORKERS")
        self.thread.start()

    def drain(self):
        self.stop_event.set()
        self.thread.join()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            self.sample()
            if not self.current_workers:
                return True
            time.sleep(0.05)
        return False

    def stop(self):
        self.stop_event.set()
        self.thread.join()
        self.sample()
        cpu = defaultdict(float)
        for item in self.records.values():
            cpu[item["role"]] += max(0, item["last"] - item["first"]) / os.sysconf("SC_CLK_TCK")
        return {"interval_seconds": self.interval, "samples": self.samples,
            "sampled_cpu_core_seconds_by_role": dict(cpu),
            "simultaneous_peak_rss_bytes_by_role": dict(self.peaks),
            "largest_query_worker_rss_bytes": max((item["peak_rss_bytes"] for item in self.records.values()
                if item["role"] == "query_worker"), default=0),
            "observed_query_worker_processes": sum(item["role"] == "query_worker" for item in self.records.values()),
            "maximum_simultaneously_observed_query_workers": self.max_workers,
            "query_workers_alive_at_final_sample": self.current_workers,
            "limitations": "Sampled RSS and process CPU are lower bounds; short-lived workers and final CPU ticks can be missed. Shared RSS pages are counted per process. Source service samples include any background work."}


def workloads():
    result = []
    for reference, old in (("zone_join", "taxi.zones"), ("large_hash_join", "taxi.taxi_sample")):
        for adapter in ("pg", "my"):
            result.append({"name": reference + "_" + adapter, "reference": reference,
                "sources": ["taxi", adapter], "sql": QUERIES[reference]["sql"].replace(old, adapter + old[4:])})
    return result


def export_reference(client, reference, destination):
    request = {"mode": "federated", "sources": ["taxi"], "sql": QUERIES["ordered_million"]["sql"]}
    identifier = client.json("a", 0, "/v1/queries", request)["id"]
    started = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix="kelvo-final-wire-reference-") as directory:
            path = Path(directory) / "reference.arrow"
            with client.open("a", 1, "/v1/queries/" + identifier + "/results") as response, path.open("wb") as output:
                size = 0
                while True:
                    block = response.read(65536)
                    if not block:
                        break
                    size += len(block)
                    if size > 64 << 20:
                        raise Failure("EXPORT_REFERENCE_TOO_LARGE")
                    output.write(block)
            actual = canonical_arrow(path)
            for key in ("rows", "canonical_value_sha256", "integer_sums", "null_counts"):
                if actual[key] != reference[key]:
                    raise Failure("EXPORT_REFERENCE_MISMATCH")
            status = client.json("a", 0, "/v1/queries/" + identifier)
            if status["state"] != "succeeded":
                raise Failure("EXPORT_REFERENCE_NOT_SUCCESSFUL")
            fixture = {"request": request, "expected_wire_bytes": actual["arrow_ipc_bytes"],
                "expected_wire_sha256": actual["arrow_ipc_sha256"], "expected_rows": actual["rows"],
                "canonical_value_sha256": actual["canonical_value_sha256"],
                "binary_sha256": client.manifest["binary_sha256"]}
            destination.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(descriptor, "w") as output:
                json.dump(fixture, output, indent=2)
                output.write("\n")
            return {**fixture, "elapsed_with_verification_seconds": time.perf_counter() - started,
                    "stats": status.get("stats", {})}
    finally:
        client.cancel("a", 0, identifier)


def execute(client, tenant, gateway, spec, reference, folder, sequence):
    started = time.perf_counter()
    detail = {"tenant": tenant, "workload": spec["name"], "submit_gateway": gateway,
        "result_gateway": 1 - gateway, "sequence": sequence, "success": False}
    identifier = None
    path = Path(folder) / (str(threading.get_ident()) + ".arrow")
    try:
        identifier = client.json(tenant, gateway, "/v1/queries", {"mode": "federated",
            "sources": spec["sources"], "sql": spec["sql"]})["id"]
        detail["submit_seconds"] = time.perf_counter() - started
        endpoint = "/v1/queries/" + identifier
        with client.open(tenant, 1 - gateway, endpoint + "/results") as response, path.open("wb") as output:
            size = 0
            while True:
                block = response.read(65536)
                if not block:
                    break
                if not size:
                    detail["first_body_read_seconds"] = time.perf_counter() - started
                size += len(block)
                if size > 1 << 20:
                    raise Failure("AGGREGATE_RESULT_TOO_LARGE")
                output.write(block)
        detail["result_seconds"] = time.perf_counter() - started
        # A different client can immediately reuse terminal NATS slots. A 404
        # here is counted separately; successful EOS already certifies commit.
        try:
            status = client.json(tenant, gateway, endpoint)
            detail["state"] = status["state"]
            detail["stats"] = status.get("stats", {})
            if status["state"] != "succeeded":
                raise Failure(safe_code((status.get("error") or {}).get("code"), "NON_SUCCESS_STATUS"))
        except Failure as failure:
            if failure.status != 404:
                raise
            detail["status_reused_before_read"] = True
        actual = canonical_arrow(path)
        for key in ("rows", "canonical_value_sha256", "integer_sums", "null_counts"):
            if actual[key] != reference[key]:
                raise Failure("EXACT_REFERENCE_MISMATCH")
        scans = [item for item in detail.get("stats", {}).get("federation", []) if item["scans"] > 0]
        if not detail.get("status_reused_before_read") and (
                {item["source"] for item in scans} != set(spec["sources"]) or
                any(item["rows_fetched"] <= 0 for item in scans)):
            raise Failure("SOURCE_SCAN_EVIDENCE_MISSING")
        detail.update(success=True, rows=actual["rows"], canonical_value_sha256=actual["canonical_value_sha256"],
                      wire_bytes=actual["arrow_ipc_bytes"])
    except Failure as failure:
        detail["error_class"], detail["http_status"] = failure.code, failure.status
    except (OSError, ValueError, KeyError, AssertionError, pa.ArrowException):
        detail["error_class"] = "CLIENT_OR_INCOMPLETE_STREAM"
    finally:
        if identifier and not detail["success"]:
            try:
                status = client.json(tenant, gateway, "/v1/queries/" + identifier)
                detail["state"], detail["stats"] = status["state"], status.get("stats", {})
                source_error = safe_code((status.get("error") or {}).get("code"), "")
                if source_error:
                    detail["upstream_error_class"] = source_error
            except (Failure, OSError, ValueError):
                pass
            client.cancel(tenant, gateway, identifier)
            deadline, drained = time.monotonic() + 5, False
            while time.monotonic() < deadline:
                try:
                    status = client.json(tenant, gateway, "/v1/queries/" + identifier, timeout=3)
                    if status["state"] in TERMINAL:
                        drained = True
                        break
                except Failure as failure:
                    if failure.status == 404:
                        drained = True
                        break
                except (OSError, ValueError):
                    break
                time.sleep(0.05)
            detail["cancelled_handle_drained"] = drained
            if not drained:
                detail["cleanup_error_class"] = "CANCEL_DID_NOT_DRAIN"
        detail["latency_seconds"] = time.perf_counter() - started
        path.unlink(missing_ok=True)
    return detail


def summarize(jobs, duration):
    success = [item for item in jobs if item["success"]]
    in_window = [item for item in success if item["completed_at_seconds"] <= duration]
    source_totals = {}
    for item in jobs:
        for scan in item.get("stats", {}).get("federation", []):
            if scan["scans"] <= 0:
                continue
            key = scan["source"] + "." + scan["table"]
            total = source_totals.setdefault(key, {field: 0 for field in
                ("scans", "rows_fetched", "arrow_bytes_fetched", "batches_fetched")})
            for field in total:
                total[field] += scan[field]
    return {"attempted_jobs": len(jobs), "successful_jobs": len(success),
        "successful_completions_in_window": len(in_window), "successful_completions_per_second": len(in_window) / duration,
        "error_classes": dict(Counter(item["error_class"] for item in jobs if not item["success"])),
        "cleanup_error_classes": dict(Counter(item["cleanup_error_class"] for item in jobs if item.get("cleanup_error_class"))),
        "source_scan_totals": source_totals,
        "status_reused_before_read": sum(bool(item.get("status_reused_before_read")) for item in jobs),
        "successful_latency_seconds_p50": percentile([item["latency_seconds"] for item in success], 0.5),
        "successful_latency_seconds_p95": percentile([item["latency_seconds"] for item in success], 0.95),
        "all_attempt_latency_seconds_p50": percentile([item["latency_seconds"] for item in jobs], 0.5),
        "all_attempt_latency_seconds_p95": percentile([item["latency_seconds"] for item in jobs], 0.95)}


def trial(manifest, references, clients, duration, source_fixture):
    specs = workloads()
    start_event = threading.Event()
    started = 0.0
    stop = threading.Event()
    sampler = Sampler(source_fixture)
    with tempfile.TemporaryDirectory(prefix="kelvo-cluster-load-") as folder:
        def loop(index):
            client, results, sequence = Client(manifest), [], 0
            start_event.wait()
            while not stop.is_set() and time.perf_counter() - started < duration:
                tenant = ("a", "b")[(index + sequence) % 2]
                spec = specs[(index + sequence // 2) % len(specs)]
                began = time.perf_counter() - started
                result = execute(client, tenant, (index + sequence // 3) % 2, spec,
                                 references[spec["reference"]], folder, sequence)
                result.update(client=index, started_at_seconds=began,
                              completed_at_seconds=time.perf_counter() - started)
                results.append(result)
                sequence += 1
                if result.get("error_class") in ("EXACT_REFERENCE_MISMATCH", "SOURCE_SCAN_EVIDENCE_MISSING"):
                    stop.set()
                if result.get("cleanup_error_class"):
                    stop.set()
                if not result["success"]:
                    # Bound rejection retry load while preserving every error.
                    stop.wait(0.05)
            return results
        sampler.start()
        try:
            with ThreadPoolExecutor(max_workers=clients) as pool:
                pending = [pool.submit(loop, index) for index in range(clients)]
                started = time.perf_counter()
                start_event.set()
                jobs = [item for future in pending for item in future.result()]
        finally:
            drained = sampler.drain()
            resources = sampler.stop()
            elapsed = time.perf_counter() - started
    jobs.sort(key=lambda item: item["started_at_seconds"])
    result = {"clients": clients, "submission_window_seconds": duration, "elapsed_including_drain_seconds": elapsed,
        **summarize(jobs, duration), "resources": resources, "jobs": jobs,
        "by_tenant": {tenant: summarize([item for item in jobs if item["tenant"] == tenant], duration) for tenant in ("a", "b")},
        "by_workload": {spec["name"]: summarize([item for item in jobs if item["workload"] == spec["name"]], duration) for spec in specs}}
    result["all_query_workers_drained"] = drained
    result["passed"] = bool(jobs) and all(item["success"] for item in jobs) and not stop.is_set() and drained
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DIR / "capacity-manifest.json")
    parser.add_argument("--references", type=Path, default=ROOT / "artifacts/capacity-private/reference-results.json")
    parser.add_argument("--source-fixture", type=Path, help="relational fixture manifest.json for source process sampling")
    parser.add_argument("--clients", nargs="+", type=int, default=[1, 2, 4])
    parser.add_argument("--duration", type=float, default=120)
    parser.add_argument("--phase", choices=("exploratory", "final"), required=True)
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--skip-admission-control", action="store_true")
    parser.add_argument("--wan-reference-output", type=Path)
    parser.add_argument("--reference-only", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not set(args.clients) <= {1, 2, 4} or not 1 <= args.duration <= 120:
        parser.error("clients must be 1, 2, 4 and duration must be between 1 and 120 seconds")
    if args.reference_only and not args.wan_reference_output:
        parser.error("--reference-only requires --wan-reference-output")
    manifest = json.loads(args.manifest.read_text())
    if manifest.get("state") != "ready" or manifest.get("binary_sha256") != args.expected_binary_sha256:
        raise Failure("FIXTURE_BINARY_IDENTITY_MISMATCH")
    references = json.loads(args.references.read_text())
    client = Client(manifest)
    for gateway in (0, 1):
        client.json("a", gateway, "/ready", timeout=5)
    evidence = {"phase": args.phase, "checked_at": datetime.now(timezone.utc).isoformat(),
        "passed": False, "suite_complete": False,
        "requested_clients": args.clients, "requested_duration_seconds": args.duration,
        "binary_sha256": manifest["binary_sha256"], "sandbox_sha256": manifest["sandbox_sha256"],
        "limits": manifest["limits"], "tenant_policy": manifest["tenant_policy"],
        "gateway_limits": manifest["gateway_limits"], "total_slots": manifest["total_slots"],
        "workloads": workloads(), "trials": [], "tenant_isolation": isolation(client),
        "method": "Closed-loop clients alternate tenants and real CH+PG/MySQL joins. Submit through one gateway; consume through the other. Every result is decoded and checked against independent exact native reference hashes. No cached result substitution.",
        "retention": "Terminal durable slots are immediately reusable in NATSStore.Submit; max_queries16 per tenant remains unchanged. Post-result status reuse is counted separately.",
        "limitations": ["One window per concurrency; no confidence interval or production sizing claim.",
            "Client result validation and new HTTP/TLS connections are included in closed-loop throughput and latency.",
            "In-flight queries may finish after the submission window; only completions within it contribute to the per-second rate.",
            "Source datasets are identical public taxi copies; synthetic tenant markers and cross-tenant handle denial independently check tenant binding."]}
    def save():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    if not args.skip_admission_control:
        evidence["admission_control"] = admission_control(client, manifest["tenant_policy"]["max_queries"])
    if args.wan_reference_output:
        evidence["ordered_export_reference"] = export_reference(client, references["ordered_million"], args.wan_reference_output)
    if args.reference_only:
        evidence["passed"] = True
        evidence["suite_complete"] = True
        evidence["scope"] = "Preflight and WAN reference only; no sustained trial requested."
        evidence["requested_clients"] = []
        save()
        print(json.dumps({"passed": True, "binary_sha256": manifest["binary_sha256"],
            "wan_reference": str(args.wan_reference_output),
            "wire_bytes": evidence["ordered_export_reference"]["expected_wire_bytes"],
            "wire_sha256": evidence["ordered_export_reference"]["expected_wire_sha256"]}), flush=True)
        return
    save()
    for count in args.clients:
        result = trial(manifest, references, count, args.duration, args.source_fixture)
        evidence["trials"].append(result)
        evidence["suite_complete"] = len(evidence["trials"]) == len(args.clients)
        evidence["passed"] = evidence["suite_complete"] and all(item["passed"] for item in evidence["trials"])
        save()
        print(json.dumps({key: result[key] for key in ("clients", "passed", "attempted_jobs", "successful_jobs",
            "successful_completions_per_second", "successful_latency_seconds_p50", "successful_latency_seconds_p95", "error_classes")}), flush=True)
        if not result["passed"]:
            break
    if not evidence.get("passed"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
