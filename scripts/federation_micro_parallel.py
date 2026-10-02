#!/usr/bin/env python3
"""Run bounded, synchronized HTTP export cohorts through a private SSH forward.

Uses the adjacent federation_micro_http.py for the private manifest contract and
SSH/HTTP setup helpers. No response files are saved. Threads do not use SIGALRM:
a controller closes their sockets at operation/cohort deadlines, and body reads
have a five-second idle timeout. Server worker concurrency is measured separately.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time

import federation_micro_http as transport


OPERATION_SECONDS = 180
COHORT_SECONDS = 210
IDLE_SECONDS = 5
ERROR_CODES = {"RESOURCE_EXHAUSTED", "UNAVAILABLE", "QUERY_FAILED", "DEADLINE_EXCEEDED",
               "CANCELLED", "INTERNAL", "UNAUTHENTICATED", "INVALID_ARGUMENT",
               "NOT_FOUND", "ALREADY_CONSUMED", "PERMISSION_DENIED"}


class OperationError(Exception):
    """Fixed public code, optionally accompanied by safe numeric HTTP status."""

    def __init__(self, code, phase=None, status=None, server_code=None):
        super().__init__(code)
        self.phase, self.status, self.server_code = phase, status, server_code


class Job:
    def __init__(self, index, case, round_number):
        self.index, self.case, self.round = index, case, round_number
        self.cancel = threading.Event()
        self.deadline, self.handle, self.done = None, None, False
        self.reason = None
        self.intervals = []


class CohortControl:
    def __init__(self, jobs):
        self.jobs, self.lock = jobs, threading.Lock()
        self.epoch, self.started_at = time.perf_counter(), transport.utc_now()
        self.sockets, self.active = {}, {"submit": 0, "results": 0, "status": 0, "cancel": 0}
        self.peaks, self.total_peak = dict(self.active), 0
        self.start_barrier = threading.Barrier(len(jobs) + 1, action=self.start_clock)
        self.results_barrier = threading.Barrier(len(jobs))
        self.cohort_expired = False

    def start_clock(self):
        self.epoch, self.started_at = time.perf_counter(), transport.utc_now()

    def request_started(self, job, phase, raw_socket, end):
        with self.lock:
            self.sockets[job.index] = {"socket": raw_socket, "end": end, "phase": phase, "expired": False}
            self.active[phase] += 1
            self.peaks[phase] = max(self.peaks[phase], self.active[phase])
            self.total_peak = max(self.total_peak, sum(self.active.values()))
        return time.perf_counter()

    def request_finished(self, job, phase, raw_socket, started):
        with self.lock:
            if self.sockets.get(job.index, {}).get("socket") is raw_socket:
                self.sockets.pop(job.index, None)
            self.active[phase] -= 1
            job.intervals.append({"phase": phase, "start_offset_seconds": started - self.epoch,
                                  "end_offset_seconds": time.perf_counter() - self.epoch})

    @staticmethod
    def close_socket(raw_socket):
        try:
            raw_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            raw_socket.close()
        except OSError:
            pass

    def abort(self, reason, only=None):
        sockets = []
        with self.lock:
            for job in self.jobs:
                if job.done or (only is not None and job is not only):
                    continue
                if not job.cancel.is_set():
                    job.reason = reason
                    job.cancel.set()
                # A cohort deadline also interrupts cancellation/status I/O
                # belonging to jobs whose operation deadline already expired.
                if job.index in self.sockets:
                    sockets.append(self.sockets[job.index]["socket"])
        for raw_socket in sockets:
            self.close_socket(raw_socket)
        self.start_barrier.abort()
        self.results_barrier.abort()

    def check_deadlines(self):
        now = time.perf_counter()
        overdue = []
        with self.lock:
            for job in self.jobs:
                request = self.sockets.get(job.index)
                if request and not request["expired"] and now >= request["end"]:
                    request["expired"] = True
                    overdue.append(request["socket"])
                    if request["phase"] != "cancel" and not job.cancel.is_set():
                        job.reason = "http_request_deadline_exceeded"
                        job.cancel.set()
        for raw_socket in overdue:
            self.close_socket(raw_socket)
        if now - self.epoch >= COHORT_SECONDS and not self.cohort_expired:
            self.cohort_expired = True
            self.abort("cohort_deadline_exceeded")
        for job in self.jobs:
            if job.deadline is not None and now >= job.deadline and not job.done and not job.cancel.is_set():
                self.abort("operation_deadline_exceeded", only=job)


class ParallelClient:
    def __init__(self, port, token, control):
        self.port, self.token, self.control = port, token, control

    @staticmethod
    def remaining(job, end, allow_cancel):
        if job.cancel.is_set() and not allow_cancel:
            raise OperationError(job.reason or "operation_cancelled")
        remaining = end - time.perf_counter()
        if remaining <= 0:
            raise OperationError("operation_deadline_exceeded")
        return remaining

    @contextmanager
    def response(self, job, phase, path, body=None, end=None, allow_cancel=False):
        end = job.deadline if end is None else end
        connection = http.client.HTTPConnection("127.0.0.1", self.port,
            timeout=min(IDLE_SECONDS, self.remaining(job, end, allow_cancel)))
        raw_socket, measured_start = None, None
        try:
            connection.connect()
            raw_socket = connection.sock
            measured_start = self.control.request_started(job, phase, raw_socket, end)
            # The first schema/HTTP headers can legitimately wait for query work.
            raw_socket.settimeout(self.remaining(job, end, allow_cancel))
            headers = {"Authorization": "Bearer " + self.token, "Connection": "close"}
            payload = None if body is None else json.dumps(body, separators=(",", ":")).encode()
            if payload is not None:
                headers["Content-Type"] = "application/json"
            connection.request("GET" if body is None else "POST", path, body=payload, headers=headers)
            response = connection.getresponse()
            try:
                yield response, raw_socket, end
            finally:
                response.close()
        finally:
            connection.close()
            if measured_start is not None:
                self.control.request_finished(job, phase, raw_socket, measured_start)

    def blocks(self, job, response, raw_socket, end, allow_cancel=False):
        while True:
            if response.isclosed():
                break
            raw_socket.settimeout(min(IDLE_SECONDS, self.remaining(job, end, allow_cancel)))
            block = response.read1(65536)
            if not block:
                break
            yield block
        if response.length not in (None, 0):
            raise OperationError("incomplete_http_body")

    def read_json(self, job, response, raw_socket, end, allow_cancel=False):
        data = bytearray()
        for block in self.blocks(job, response, raw_socket, end, allow_cancel):
            if len(data) + len(block) > transport.MAX_JSON:
                raise OperationError("json_response_exceeds_limit")
            data.extend(block)
        return json.loads(data)

    def error_code(self, job, response, raw_socket, end):
        try:
            data = self.read_json(job, response, raw_socket, end)
            code = data.get("error", {}).get("code") if isinstance(data, dict) and isinstance(data.get("error"), dict) else None
            return code if code in ERROR_CODES else None
        except (OperationError, OSError, ValueError, http.client.HTTPException):
            return None

    def cancel_handle(self, job):
        with self.response(job, "cancel", "/v1/queries/" + job.handle + "/cancel", {},
                           end=time.perf_counter() + 5, allow_cancel=True) as (response, raw_socket, end):
            status = response.status
            data = self.read_json(job, response, raw_socket, end, allow_cancel=True)
        state = data.get("state") if isinstance(data, dict) else None
        result = {"http_status": status,
                  "terminal_state": state if state in ("cancelled", "succeeded", "failed", "expired") else None}
        if status != 200 or result["terminal_state"] is None:
            result["error"] = "cancellation_not_confirmed"
        return result


def run_operation(client, job):
    control, case = client.control, job.case
    result = {"client": job.index + 1, "round": job.round, "case": case["name"],
        "mode": case["request"]["mode"], "state": "failed", "accepted": False,
        "submit_http_status": None, "results_http_status": None}
    digest, size, tail, first_64k = hashlib.sha256(), 0, b"", None
    started = time.perf_counter()
    try:
        control.start_barrier.wait(timeout=10)
        started = time.perf_counter()
        job.deadline = started + OPERATION_SECONDS
        result["start_offset_seconds"] = started - control.epoch
        submission_error = None
        try:
            with client.response(job, "submit", "/v1/queries", case["request"]) as (response, raw_socket, end):
                result["submit_http_status"] = response.status
                if response.status != 201:
                    raise OperationError("submission_rejected", "submit", response.status,
                                         client.error_code(job, response, raw_socket, end))
                created = client.read_json(job, response, raw_socket, end)
                handle = created.get("id") if isinstance(created, dict) else None
                if not isinstance(handle, str) or not transport.HANDLE.fullmatch(handle):
                    raise OperationError("invalid_query_handle", "submit")
                job.handle, result["accepted"] = handle, True
        except (OperationError, OSError, ValueError, http.client.HTTPException) as error:
            submission_error = error
        result["submission_seconds"] = time.perf_counter() - started
        # Failed submissions still join this barrier, so every accepted request
        # starts its single result GET in the same synchronized phase.
        control.results_barrier.wait(timeout=max(0.01, job.deadline - time.perf_counter()))
        if submission_error is not None:
            raise submission_error
        results_started = time.perf_counter()
        result["results_start_offset_seconds"] = results_started - control.epoch
        with client.response(job, "results", "/v1/queries/" + job.handle + "/results") as (response, raw_socket, end):
            result["results_http_status"] = response.status
            if response.status != 200:
                raise OperationError("result_request_rejected", "results", response.status,
                                     client.error_code(job, response, raw_socket, end))
            if response.getheader("Content-Type", "").split(";", 1)[0].strip() != "application/vnd.apache.arrow.stream":
                raise OperationError("unexpected_result_content_type", "results")
            for block in client.blocks(job, response, raw_socket, end):
                size += len(block)
                digest.update(block)
                tail = (tail + block)[-8:]
                if first_64k is None and size >= 65536:
                    first_64k = time.perf_counter() - started
                if size > case["expected_wire_bytes"]:
                    raise OperationError("result_exceeds_reference_bytes", "results")
        result["export_seconds"] = time.perf_counter() - started
        result["complete_offset_seconds"] = time.perf_counter() - control.epoch
        result["results_seconds"] = time.perf_counter() - results_started
        result["wire_matches_reference"] = (size == case["expected_wire_bytes"] and
            digest.hexdigest() == case["expected_wire_sha256"])
        status_started = time.perf_counter()
        with client.response(job, "status", "/v1/queries/" + job.handle,
                end=min(job.deadline, status_started + 10)) as (response, raw_socket, end):
            if response.status != 200:
                raise OperationError("final_status_request_failed", "status", response.status)
            final = client.read_json(job, response, raw_socket, end)
        result["final_state_check_seconds"] = time.perf_counter() - status_started
        state = final.get("state") if isinstance(final, dict) else None
        result["api_final_state"] = state if state in ("queued", "running", "streaming", "succeeded", "failed", "cancelled", "expired") else "unknown"
        rows = final.get("stats", {}).get("rows") if isinstance(final, dict) and isinstance(final.get("stats"), dict) else None
        result["api_reported_rows"] = rows if transport.bounded_integer(rows, 0, 100_000_000) else None
        if state != "succeeded":
            raise OperationError("query_did_not_succeed")
        if tail != transport.EOS:
            raise OperationError("arrow_eos_missing")
        if not result["wire_matches_reference"]:
            raise OperationError("wire_reference_mismatch")
        if result["api_reported_rows"] != case["expected_rows"]:
            raise OperationError("api_rows_reference_mismatch")
        result["state"] = "completed"
        result["reference_matched_rows"] = case["expected_rows"]
    except threading.BrokenBarrierError:
        result["error"] = job.reason or "synchronization_barrier_broken"
    except OperationError as error:
        result["error"] = job.reason or str(error)
        if error.phase is not None:
            result["error_phase"] = error.phase
        if error.status is not None:
            result["error_http_status"] = error.status
        if error.server_code is not None:
            result["server_error_code"] = error.server_code
    except (socket.timeout, TimeoutError):
        result["error"] = job.reason or "http_socket_timeout"
    except (OSError, http.client.HTTPException):
        result["error"] = job.reason or "connection_closed_reset_or_http_failure"
        result["transport_disconnected"] = not job.cancel.is_set()
        result["connection_closed_by_controller"] = job.cancel.is_set()
    except ValueError:
        result["error"] = job.reason or "invalid_response_json"
    finally:
        result.update({"attempt_seconds_before_cleanup": time.perf_counter() - started,
            "first_64k_seconds": first_64k, "wire_bytes_read": size,
            "wire_sha256": digest.hexdigest(), "arrow_eos_present": tail == transport.EOS})
        if job.handle is not None and result["state"] != "completed":
            try:
                result["cancel"] = client.cancel_handle(job)
            except (OperationError, OSError, ValueError, http.client.HTTPException):
                result["cancel"] = {"error": "cancel_request_failed"}
        result["finished_offset_seconds"] = time.perf_counter() - control.epoch
        result["request_intervals"] = job.intervals
        with control.lock:
            job.done = True
    return result


def quantile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lo, hi = math.floor(position), math.ceil(position)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def run_cohort(port, token, cases, clients, round_number):
    jobs = [Job(index, cases[((round_number - 1) * clients + index) % len(cases)], round_number)
            for index in range(clients)]
    control = CohortControl(jobs)
    client = ParallelClient(port, token, control)
    pool = ThreadPoolExecutor(max_workers=clients, thread_name_prefix="kelvo-export")
    futures, interrupted, synchronization_failed = [], False, False
    try:
        futures = [pool.submit(run_operation, client, job) for job in jobs]
        control.start_barrier.wait(timeout=10)
        while not all(future.done() for future in futures):
            control.check_deadlines()
            wait(futures, timeout=0.05)
    except KeyboardInterrupt:
        interrupted = True
        control.abort("interrupted")
    except threading.BrokenBarrierError:
        synchronization_failed = True
        control.abort("start_barrier_failed")
    finally:
        # Continue enforcing every active request deadline during cancellation,
        # including after an interrupt. Socket inactivity timeouts alone do not
        # bound a peer that slowly trickles response headers.
        while not all(future.done() for future in futures):
            control.check_deadlines()
            wait(futures, timeout=0.05)
        pool.shutdown(wait=True, cancel_futures=True)
    elapsed = time.perf_counter() - control.epoch
    results = []
    for job, future in zip(jobs, futures):
        try:
            results.append(future.result())
        except Exception:
            results.append({"client": job.index + 1, "round": round_number, "case": job.case["name"],
                "state": "failed", "error": "client_thread_failed", "wire_bytes_read": 0,
                "accepted": job.handle is not None})
    successes = [result for result in results if result["state"] == "completed"]
    rows = sum(result["reference_matched_rows"] for result in successes)
    wire_bytes = sum(result["wire_bytes_read"] for result in successes)
    latency = [result["export_seconds"] for result in successes]
    return {"round": round_number, "started_at": control.started_at, "finished_at": transport.utc_now(),
        "cohort_wall_seconds_including_verification_and_cleanup": elapsed,
        "clients": clients, "accepted_query_handles": sum(bool(result["accepted"]) for result in results),
        "result_http_200": sum(result.get("results_http_status") == 200 for result in results),
        "http_429_count": sum(429 in (result.get("submit_http_status"), result.get("results_http_status")) for result in results),
        "transport_disconnect_count": sum(bool(result.get("transport_disconnected")) for result in results),
        "failed_after_http_200": sum(result.get("results_http_status") == 200 and result["state"] != "completed" for result in results),
        "reference_matched_exports": len(successes), "failed_exports": len(results) - len(successes),
        "total_reference_matched_rows": rows, "total_reference_matched_bytes": wire_bytes,
        "total_bytes_read_including_failed_exports": sum(result["wire_bytes_read"] for result in results),
        "aggregate_reference_rows_per_second": rows / elapsed,
        "aggregate_reference_bytes_per_second": wire_bytes / elapsed,
        "successful_export_latency_seconds": {"count": len(latency), "p50": quantile(latency, 0.5),
            "p95": quantile(latency, 0.95), "minimum": min(latency, default=None), "maximum": max(latency, default=None)},
        "maximum_inflight_client_http_requests": control.total_peak,
        "maximum_inflight_client_http_requests_by_phase": control.peaks,
        "cohort_deadline_exceeded": control.cohort_expired, "interrupted": interrupted,
        "synchronization_failed": synchronization_failed,
        "oom_status": "unverified_by_client_requires_server_cgroup_evidence", "results": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Manifest format is the same as federation_micro_http.py. Default: 10 clients
x 3 rounds = 30 jobs total. Cases are assigned round-robin across client slots/rounds;
provide a one-case manifest for a homogeneous cohort. POST submission and GET
results each have a separate synchronization barrier. No request is retried.
Each operation is bounded to 180 seconds; the cohort controller deadline is 210s.
Server concurrency/admission, query limits and OOM evidence are provisioned and
measured separately. A 429 is a measured rejection, not a successful export.
""")
    for name in ("ssh-target", "identity", "manifest", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    identity, output = Path(args.identity).expanduser(), Path(args.output)
    if sys.platform not in ("darwin", "linux"):
        parser.error("requires macOS or Linux")
    if not transport.bounded_integer(args.clients, 1, 10) or not transport.bounded_integer(args.rounds, 1, 3):
        parser.error("clients must be 1-10 and rounds 1-3")
    if not re.fullmatch(r"(?:[A-Za-z0-9_.-]+@)?[A-Za-z0-9_.-]+", args.ssh_target) or args.ssh_target.startswith("-"):
        parser.error("invalid SSH target")
    if not identity.is_absolute() or not identity.is_file() or not args.manifest.startswith("/") or "\0" in args.manifest or len(args.manifest) > 4096:
        parser.error("identity and remote manifest must be valid absolute paths")
    if not output.is_absolute() or output.exists() or output.is_symlink() or not output.parent.is_dir():
        parser.error("output must be a new absolute file in an existing directory")
    report = {"schema_version": 1, "started_at": transport.utc_now(), "state": "running", "cohorts": [],
        "client_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "transport_helper_sha256": hashlib.sha256(Path(transport.__file__).read_bytes()).hexdigest(),
        "clients_per_cohort": args.clients, "planned_rounds": args.rounds,
        "operation_deadline_seconds": OPERATION_SECONDS, "cohort_deadline_seconds": COHORT_SECONDS,
        "body_read_idle_timeout_seconds": IDLE_SECONDS,
        "transport": "HTTP on loopback through a dedicated SSH tunnel; no HTTP TLS",
        "limitations": [
            "In-flight client HTTP requests do not establish actual server worker overlap; correlate server process/cgroup evidence.",
            "POST 201 creates a query handle. GET 200 starts an Arrow response and does not establish successful completion.",
            "OOM cannot be diagnosed from a client disconnect alone; correlate failures with server memory.events and systemd Result.",
            "Throughput includes only exports matching reference bytes/EOS and final API state/rows; its denominator includes the whole cohort and cleanup.",
            "Export latency includes submission, the results barrier and full body hashing; final API status checks are recorded separately.",
            "P50/P95 use linear interpolation over successful export durations only. Failed attempts and 429 responses remain in individual results.",
            "The provisioner attests that reference Arrow outputs were independently decoded and use the same binary as the server.",
            "Client result bodies are hashed and discarded. No Arrow values are decoded locally, and no response file is saved.",
            "There are no retries during the measured cohort. Server health is checked between cohorts, outside throughput timing.",
            "SSH encryption and buffering are included; these measurements are not direct public HTTP/HTTPS capacity."]}
    transport.write_report(output, report)
    ssh = ["ssh", "-i", str(identity), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=15", "-o", "ControlMaster=no", "-o", "ControlPath=none", "-o", "ForkAfterAuthentication=no"]
    tunnel, interrupted = None, False
    previous = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        command = shlex.join(["python3", "-c", transport.FETCH_MANIFEST, args.manifest])
        fetched = subprocess.run(ssh + [args.ssh_target, command], stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=30, check=False)
        if fetched.returncode or len(fetched.stdout) > transport.MAX_JSON:
            raise transport.ClientError("private_manifest_fetch_failed")
        manifest = transport.validate_manifest(json.loads(fetched.stdout))
        report["binary_sha256"] = manifest["binary_sha256"]
        report["cases"] = [{**{key: case[key] for key in ("name", "expected_wire_sha256", "expected_wire_bytes",
            "expected_rows", "canonical_value_sha256")}, "mode": case["request"]["mode"]} for case in manifest["cases"]]
        with socket.socket() as available:
            available.bind(("127.0.0.1", 0))
            port = available.getsockname()[1]
        tunnel = subprocess.Popen(ssh + ["-N", "-T", "-o", "ExitOnForwardFailure=yes", "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=2", "-L", f"127.0.0.1:{port}:127.0.0.1:{manifest['port']}", args.ssh_target],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        health_client = transport.LoopbackClient(port, manifest["token"])
        report["bearer_authentication_probe_http_status"] = transport.wait_for_service(health_client, tunnel)
        for round_number in range(1, args.rounds + 1):
            if tunnel.poll() is not None:
                raise transport.ClientError("ssh_forward_exited")
            with transport.deadline(5):  # Main thread only, outside cohort workers.
                if health_client.json("/health", timeout=5) != {"status": "ok"}:
                    raise transport.ClientError("server_unhealthy_before_cohort")
            cohort = run_cohort(port, manifest["token"], manifest["cases"], args.clients, round_number)
            report["cohorts"].append(cohort)
            transport.write_report(output, report)
            print(json.dumps({"round": round_number, "matched_exports": cohort["reference_matched_exports"],
                              "failed_exports": cohort["failed_exports"], "http429": cohort["http_429_count"]}), flush=True)
            if cohort["interrupted"]:
                raise KeyboardInterrupt
            if cohort["synchronization_failed"]:
                raise transport.ClientError("stopped_after_synchronization_failure")
            if any(result.get("error") == "client_thread_failed" for result in cohort["results"]):
                raise transport.ClientError("stopped_after_client_thread_failure")
            if cohort["cohort_deadline_exceeded"]:
                raise transport.ClientError("stopped_after_cohort_deadline")
            if any(result.get("cancel", {}).get("error") for result in cohort["results"]):
                raise transport.ClientError("stopped_after_unconfirmed_cancellation")
            try:
                with transport.deadline(5):
                    healthy = health_client.json("/health", timeout=5) == {"status": "ok"}
            except (transport.ClientError, OSError, ValueError, http.client.HTTPException):
                healthy = False
            cohort["server_healthy_after_cohort"] = healthy
            transport.write_report(output, report)
            if not healthy:
                raise transport.ClientError("stopped_after_server_became_unhealthy")
        report["state"] = "completed"
    except KeyboardInterrupt:
        interrupted = True
        report["error"] = "interrupted"
    except transport.ClientError as error:
        report["error"] = str(error)
    except (OSError, ValueError, subprocess.TimeoutExpired, http.client.HTTPException):
        report["error"] = "setup_transport_or_response_failed"
    finally:
        if tunnel is not None:
            tunnel.terminate()
            try:
                tunnel.wait(timeout=5)
            except subprocess.TimeoutExpired:
                tunnel.kill()
                tunnel.wait(timeout=5)
            report["owned_ssh_process_exit_status"] = tunnel.returncode
        signal.signal(signal.SIGTERM, previous)
        report["finished_at"] = transport.utc_now()
        if "error" in report:
            report["state"] = "interrupted" if interrupted else "failed"
        results = [result for cohort in report["cohorts"] for result in cohort["results"]]
        matched = [result for result in results if result["state"] == "completed"]
        wall = sum(cohort["cohort_wall_seconds_including_verification_and_cleanup"] for cohort in report["cohorts"])
        rows = sum(result["reference_matched_rows"] for result in matched)
        wire_bytes = sum(result["wire_bytes_read"] for result in matched)
        latencies = [result["export_seconds"] for result in matched]
        report["summary"] = {"attempted_jobs": len(results), "reference_matched_exports": len(matched),
            "failed_exports": len(results) - len(matched), "total_reference_matched_rows": rows,
            "total_reference_matched_bytes": wire_bytes, "sum_cohort_wall_seconds": wall,
            "aggregate_reference_rows_per_second": rows / wall if wall else None,
            "aggregate_reference_bytes_per_second": wire_bytes / wall if wall else None,
            "successful_export_latency_p50_seconds": quantile(latencies, 0.5),
            "successful_export_latency_p95_seconds": quantile(latencies, 0.95),
            "http_429_count": sum(cohort["http_429_count"] for cohort in report["cohorts"]),
            "transport_disconnect_count": sum(cohort["transport_disconnect_count"] for cohort in report["cohorts"]),
            "maximum_inflight_client_http_requests": max((cohort["maximum_inflight_client_http_requests"]
                for cohort in report["cohorts"]), default=0)}
        report["all_exports_matched_reference"] = (report["state"] == "completed" and
            len(results) == args.clients * args.rounds and all(result["state"] == "completed" for result in results))
        if report["state"] == "completed" and not report["all_exports_matched_reference"]:
            report["state"] = "completed_with_failures"
        transport.write_report(output, report)
    return 0 if report["all_exports_matched_reference"] else 1


if __name__ == "__main__":
    sys.exit(main())
