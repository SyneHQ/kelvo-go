#!/usr/bin/env python3
"""Exact-result client for the isolated native/federated Oracle node profiles.

Run on the separate Linux gateway host with PyArrow. Profile results have no
resource claim until joined with the matching node_capacity_worker report.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import ssl
import threading
import time
import urllib.error
import urllib.request

import pyarrow as pa

import operational_acceptance as ops


def response_framing(headers):
    ops.require(headers.get_all('Kelvo-Result-Completion') == ['durable-eos-v1'], 'DURABLE_COMPLETION_REQUIRED')
    transfer, length = headers.get_all('Transfer-Encoding'), headers.get_all('Content-Length')
    ops.require(not (transfer and length), 'AMBIGUOUS_HTTP_FRAMING')
    if transfer is not None:
        ops.require(len(transfer) == 1 and transfer[0].lower() == 'chunked', 'INVALID_HTTP_TRANSFER_ENCODING')
        return None
    ops.require(length is not None and len(length) == 1 and re.fullmatch(r'[0-9]+', length[0]) is not None, 'HTTP_COMPLETION_FRAMING_REQUIRED')
    count = int(length[0])
    ops.require(8 <= count <= 33554432, 'ENCODED_RESULT_BOUND')
    return count


def success_counter(raw):
    values = re.findall(rb'(?m)^kelvo_jobs_completed_total\{kind="query",outcome="success"\} ([0-9]+)$', raw)
    ops.require(len(values) == 1 and len(values[0]) <= 20, 'SUCCESS_METRICS_COUNTER_REQUIRED')
    value = int(values[0])
    ops.require(value <= (1 << 64) - 1, 'SUCCESS_METRICS_COUNTER_REQUIRED')
    return value


def verify_success_delta(before, after, successful_cases):
    # A fresh node performs one instrumented SELECT 1 startup probe before its
    # readiness endpoint opens. Keep that observation separate from the workload.
    ops.require(type(before) is int and before == 1, 'STARTUP_METRICS_COUNT_MISMATCH')
    ops.require(type(after) is int and type(successful_cases) is int and successful_cases >= 0
                and after - before == successful_cases, 'SUCCESS_METRICS_COUNT_MISMATCH')


def request(opener, url, token=None, body=None, timeout=5):
    headers = {} if token is None else {'Authorization': 'Bearer ' + token}
    if body is not None:
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, headers=headers, data=None if body is None else json.dumps(body).encode())
    try:
        with opener.open(req, timeout=timeout) as response:
            raw = response.read(65537)
            ops.require(len(raw) <= 65536, 'CONTROL_RESPONSE_BOUND')
            return response.status, raw
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read(65537)


def until(callback, timeout, interval=.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if callback():
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(interval)
    raise ops.AcceptanceError('OBSERVATION_DEADLINE')


def run_case(directory, output, config, context, item, assigned_timeout=30, barrier=None):
    name, body, reference_name = item
    gateway = config['gateway']
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), ops.StrictHTTPSHandler(context=context))
    case = {'name': name, 'passed': False}
    query_id = None
    total = 0
    started = time.monotonic()
    try:
        if barrier is not None:
            barrier.wait(timeout=10)
        started = time.monotonic()
        code, raw = request(opener, gateway + '/v1/queries', config['token'], body)
        ops.require(code == 201, 'QUERY_SUBMISSION_FAILED')
        submitted = time.monotonic()
        case['submission_seconds'] = submitted - started
        query_id = json.loads(raw)['id']
        status_url = gateway + '/v1/queries/' + query_id
        def assigned():
            code, raw = request(opener, status_url, config['token'])
            ops.require(code == 200, 'QUERY_HANDLE_LOST')
            state = json.loads(raw)['state']
            ops.require(state not in ('failed', 'cancelled'), 'QUERY_ASSIGNMENT_FAILED')
            return state == 'assigned'
        until(assigned, assigned_timeout, .25 if barrier is not None else .1)
        dispatched = time.monotonic()
        case['assignment_wait_seconds'] = dispatched - submitted
        req = urllib.request.Request(status_url + '/results', headers={'Authorization': 'Bearer ' + config['token']})
        total = 0
        checksum = hashlib.sha256()
        tail = b''
        with opener.open(req, timeout=75) as response, (output / (name + '.arrow')).open('xb') as result:
            ops.require(response.status == 200, 'RESULT_RESPONSE_FAILED')
            expected_length = response_framing(response.headers)
            first = response.read(1)
            case['first_byte_seconds'] = time.monotonic() - started
            block = first
            while block:
                total += len(block)
                ops.require(total <= 33554432, 'ENCODED_RESULT_BOUND')
                checksum.update(block)
                result.write(block)
                tail = (tail + block)[-8:]
                block = response.read(64 << 10)
        ops.require(expected_length is None or total == expected_length, 'INCOMPLETE_HTTP_CONTENT_LENGTH')
        elapsed = time.monotonic() - started
        ops.require(tail == ops.EOS, 'INCOMPLETE_ARROW_RESULT')
        with pa.memory_map(str(output / (name + '.arrow'))) as data:
            actual = pa.ipc.open_stream(data).read_all()
            ops.require(data.tell() == total, 'TRAILING_RESULT_BYTES')
        with pa.memory_map(str(directory / 'references' / (reference_name + '.arrow'))) as data:
            expected = pa.ipc.open_stream(data).read_all()
        ops.require(actual.schema.names == expected.schema.names and [field.type for field in actual.schema] == [field.type for field in expected.schema], 'RESULT_TYPES_MISMATCH')
        ops.require(actual.num_rows == expected.num_rows and actual.cast(expected.schema).equals(expected), 'EXACT_RESULT_MISMATCH')
        def completed():
            code, raw = request(opener, status_url, config['token'])
            return code == 200 and json.loads(raw)['state'] == 'succeeded'
        until(completed, 10)
        case.update(passed=True, output_rows=actual.num_rows, encoded_bytes=total, body_sha256=checksum.hexdigest(),
                    complete_seconds=elapsed, dispatch_observation_seconds=dispatched - started, reference_sha256=hashlib.sha256((directory / 'references' / (reference_name + '.arrow')).read_bytes()).hexdigest(),
                    arrow_types=[str(field.type) for field in actual.schema], exact_values_and_nulls=True, durable_completion=True)
    except Exception as error:
        case['failure'] = str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__
        case['failed_after_seconds'] = time.monotonic() - started
        case['partial_encoded_bytes'] = total
        diagnostic = {'query_id': query_id, 'failure': case['failure']}
        if isinstance(error, urllib.error.HTTPError):
            case['http_status'] = error.code
            diagnostic['error_body'] = error.read(65537).decode(errors='replace')
        if query_id is not None:
            code, status = request(opener, gateway + '/v1/queries/' + query_id, config['token'])
            diagnostic['status_http'] = code
            diagnostic['status_body'] = status.decode(errors='replace')
            if code == 200:
                value = json.loads(status)
                case['terminal_state_observed'] = value.get('state')
                if isinstance(value.get('error'), dict):
                    case['query_error_code'] = value['error'].get('code')
            (output / (name + '-diagnostics.json')).write_text(json.dumps(diagnostic, indent=2) + '\n')
            request(opener, gateway + '/v1/queries/' + query_id + '/cancel', config['token'], {})
    return case


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--metrics', choices=('enabled', 'disabled'), required=True)
    parser.add_argument('--queued-operations', type=int, choices=(0, 10), default=0)
    args = parser.parse_args()
    ops.require(re.fullmatch(r'[a-z0-9-]{1,48}', args.profile), 'INVALID_PROFILE')
    directory = args.directory.resolve(strict=True)
    private = directory / 'client.json'
    ops.require(private.is_file() and not private.is_symlink() and not private.stat().st_mode & 0o077, 'PRIVATE_CLIENT_INPUT_REQUIRED')
    config = json.loads(private.read_text())
    context = ssl.create_default_context(cafile=str(directory / 'ca.pem'))
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), ops.StrictHTTPSHandler(context=context))
    worker_context = ssl.create_default_context(cafile=str(directory / 'ca.pem'))
    worker_context.minimum_version = ssl.TLSVersion.TLSv1_3
    worker_context.load_cert_chain(str(directory / 'gateway.pem'), str(directory / 'gateway.key'))
    worker = urllib.request.build_opener(urllib.request.ProxyHandler({}), ops.StrictHTTPSHandler(context=worker_context))
    gateway, node = config['gateway'], config['node']
    output = directory / 'client-profiles' / args.profile
    output.mkdir(parents=True, mode=0o700)
    input_paths = {'client_script': Path(__file__), 'protocol_reader': Path(ops.__file__),
                   'private_control': private, 'projection_reference': directory / 'references/projection.arrow',
                   'join_reference': directory / 'references/join.arrow'}
    inputs = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in input_paths.items()}
    report = {'schema': 1, 'metrics_enabled': args.metrics == 'enabled', 'cases': [], 'passed': False,
              'inputs': inputs, 'queued_operations_requested': args.queued_operations,
              'timing_scope': 'Submission through complete Arrow response at the Azure client; SQL, dispatch, verified TLS and Oracle-Azure SSH transport included. Value decoding is outside timing. Source/gateway/NATS/SSH resources excluded from separate Oracle node cgroup.'}
    try:
        until(lambda: request(worker, node + '/ready')[0] == 200, 30)
        until(lambda: request(opener, gateway + '/ready')[0] == 200, 20)
        expected_metrics = 200 if args.metrics == 'enabled' else 404
        code, initial_metrics = request(worker, node + '/metrics')
        ops.require(code == expected_metrics, 'METRICS_ROUTE_MODE_MISMATCH')
        if args.metrics == 'enabled':
            initial_successes = success_counter(initial_metrics)
            verify_success_delta(initial_successes, initial_successes, 0)
            report['metrics_successes_before_workload'] = initial_successes
        cases = [
            ('native_million', {'mode': 'native', 'connection_id': 'warehouse', 'sql': config['projection_sql']}, 'projection'),
            ('federated_million', {'mode': 'federated', 'sources': ['warehouse'], 'sql': 'SELECT trip_id,pickup_zone_id,fare_cents FROM warehouse.taxi_sample ORDER BY trip_id'}, 'projection'),
            ('federated_cte_join', {'mode': 'federated', 'sources': ['warehouse'], 'sql': "WITH trips AS (SELECT pickup_zone_id,fare_cents FROM warehouse.taxi_sample), zones AS (SELECT zone_id,borough FROM warehouse.zones) SELECT coalesce(z.borough,'Unmapped') AS borough, count(*)::BIGINT AS trips, sum(t.fare_cents)::DECIMAL(38,0) AS fare_total FROM trips t LEFT JOIN zones z ON t.pickup_zone_id=z.zone_id GROUP BY borough ORDER BY borough"}, 'join'),
        ]
        report['cases'] = [run_case(directory, output, config, context, item) for item in cases]
        if args.queued_operations:
            ops.require(all(case['passed'] for case in report['cases']), 'PREFLIGHT_CASES_REQUIRED')
            barrier = threading.Barrier(args.queued_operations + 1)
            burst = []
            for number in range(args.queued_operations):
                _, body, reference = cases[number % 2]
                burst.append((f'queued_million_{number+1:02d}', body, reference))
            with ThreadPoolExecutor(max_workers=args.queued_operations) as pool:
                futures = [pool.submit(run_case, directory, output, config, context, item, 240, barrier) for item in burst]
                barrier.wait(timeout=10)
                report['cases'].extend(future.result() for future in futures)
            report['queued_burst'] = {'concurrent_submissions': args.queued_operations, 'worker_execution_permits': 1,
                                      'native_operations': 5, 'federated_operations': 5,
                                      'rows_per_operation': 1000000,
                                      'scope': 'Concurrent queued submissions; at most one query executes on this node at a time.'}
        code, raw = request(worker, node + '/metrics')
        ops.require(code == expected_metrics, 'METRICS_ROUTE_MODE_CHANGED')
        if args.metrics == 'enabled':
            final_successes = success_counter(raw)
            report['metrics_successes_after_workload'] = final_successes
            verify_success_delta(initial_successes, final_successes, sum(case['passed'] for case in report['cases']))
            report['metrics_success_delta_verified'] = True
        report['metrics_route_verified'] = True
        report['inputs_unchanged'] = inputs == {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in input_paths.items()}
        report['passed'] = report['inputs_unchanged'] and len(report['cases']) == 3 + args.queued_operations and all(case['passed'] for case in report['cases'])
    except Exception as error:
        report['failure'] = str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__
    (output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'metrics_enabled': report['metrics_enabled'], 'cases': [{'name': case['name'], 'passed': case['passed'], 'failure': case.get('failure')} for case in report['cases']], 'failure': report.get('failure')}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
