#!/usr/bin/env python3
"""Reconcile complete, matched metrics profiles before summarizing capacity."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

import node_capacity_worker as worker


CASE_NAMES = ['native_million', 'federated_million', 'federated_cte_join'] + [f'queued_million_{i:02d}' for i in range(1, 11)]


def require(value, code):
    if not value:
        raise ValueError(code)


def finite(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def distribution(values):
    require(bool(values) and all(finite(value) for value in values), 'INVALID_DISTRIBUTION')
    ordered = sorted(values)
    return {'samples': len(values), 'median': statistics.median(values),
            'p95_nearest_rank': ordered[math.ceil(.95 * len(values)) - 1], 'maximum': ordered[-1]}


def reconcile(entries, binary_sha256):
    require(re.fullmatch(r'[0-9a-f]{64}', binary_sha256) is not None, 'INVALID_BINARY_IDENTITY')
    require(len(entries) >= 10 and len(entries) <= 40 and len(entries) % 2 == 0, 'FIVE_COMPLETE_PAIRS_REQUIRED')
    groups, seen, identity = {}, set(), None
    for profile, resources, client in entries:
        require(isinstance(profile, str) and re.fullmatch(r'[a-z0-9-]{1,48}', profile), 'INVALID_PROFILE')
        require(profile not in seen, 'DUPLICATE_PROFILE')
        seen.add(profile)
        prefix, _, mode = profile.rpartition('-')
        require(prefix and mode in ('enabled', 'disabled'), 'INVALID_PROFILE_MODE')
        require(resources.get('passed') is True and worker.valid_profile(resources), 'INVALID_RESOURCE_PROFILE')
        for name, expected in (('memory_bytes', 640 << 20), ('swap_bytes', 0), ('tasks', 128), ('cpu_weight', 20)):
            value = resources.get('resource_budget', {}).get(name)
            require(type(value) is int and value == expected, 'RESOURCE_BUDGET_MISMATCH')
        require(resources.get('binary_sha256') == binary_sha256, 'UNEXPECTED_BINARY')
        enabled = mode == 'enabled'
        require(resources.get('metrics_enabled') is enabled and client.get('metrics_enabled') is enabled, 'METRICS_MODE_MISMATCH')
        require(client.get('passed') is True and client.get('inputs_unchanged') is True and client.get('metrics_route_verified') is True, 'INCOMPLETE_CLIENT_PROFILE')
        if enabled:
            require(client.get('metrics_success_delta_verified') is True, 'METRICS_DELTA_REQUIRED')
            for name, expected in (('metrics_successes_before_workload', 1), ('metrics_successes_after_workload', 14)):
                require(type(client.get(name)) is int and client[name] == expected, 'METRICS_DELTA_MISMATCH')
        require(type(client.get('queued_operations_requested')) is int and client['queued_operations_requested'] == 10, 'TEN_QUEUED_OPERATIONS_REQUIRED')
        cases = client.get('cases', [])
        require([case.get('name') for case in cases] == CASE_NAMES, 'EXACT_WORKLOAD_REQUIRED')
        for case in cases:
            expected_rows = 8 if case['name'] == 'federated_cte_join' else 1_000_000
            require(case.get('passed') is True and case.get('exact_values_and_nulls') is True and case.get('durable_completion') is True, 'UNCERTIFIED_RESULT')
            require(type(case.get('output_rows')) is int and case['output_rows'] == expected_rows, 'ROW_COUNT_MISMATCH')
            expected_types = ['string', 'int64', 'decimal128(38, 0)'] if case['name'] == 'federated_cte_join' else ['int64', 'int32', 'int64']
            require(case.get('arrow_types') == expected_types, 'ARROW_TYPES_MISMATCH')
            require(type(case.get('encoded_bytes')) is int and 8 <= case['encoded_bytes'] <= 33554432, 'INVALID_RESULT_BYTES')
            for name in ('submission_seconds', 'assignment_wait_seconds', 'first_byte_seconds', 'complete_seconds', 'dispatch_observation_seconds'):
                require(finite(case.get(name)), 'INVALID_QUERY_TIMING')
            require(0 < case['complete_seconds'] and case['first_byte_seconds'] <= case['complete_seconds'], 'INVALID_COMPLETION_TIMING')
            for name in ('reference_sha256', 'body_sha256'):
                require(isinstance(case.get(name), str) and re.fullmatch(r'[0-9a-f]{64}', case[name]), 'INVALID_RESULT_IDENTITY')
        burst = client.get('queued_burst', {})
        for name, expected in (('concurrent_submissions', 10), ('worker_execution_permits', 1), ('native_operations', 5), ('federated_operations', 5), ('rows_per_operation', 1000000)):
            require(type(burst.get(name)) is int and burst[name] == expected, 'INVALID_BURST_SCOPE')
        worker_inputs, client_inputs = resources.get('inputs', {}), client.get('inputs', {})
        require(set(client_inputs) == {'client_script', 'protocol_reader', 'private_control', 'projection_reference', 'join_reference'}, 'CLIENT_INPUTS_INCOMPLETE')
        files = worker_inputs.get('files', {})
        require(files and client_inputs and all(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) for value in [*files.values(), *client_inputs.values()]), 'INVALID_INPUT_IDENTITY')
        require(worker_inputs.get('sha256') == hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest(), 'INVALID_INPUT_MANIFEST')
        require(files.get('bin/kelvo') == binary_sha256, 'INPUT_BINARY_MISMATCH')
        current = (resources.get('normalized_config_sha256'), worker_inputs, client_inputs,
                   [(case['name'], case['output_rows'], case['arrow_types'], case['reference_sha256']) for case in cases])
        require(isinstance(current[0], str) and re.fullmatch(r'[0-9a-f]{64}', current[0]), 'INVALID_NORMALIZED_CONFIG')
        require(identity is None or identity == current, 'PROFILES_NOT_MATCHED')
        identity = current
        for name in ('rss_node_peak_bytes', 'rss_workers_peak_bytes', 'rss_combined_peak_bytes', 'memory_peak_bytes', 'minimum_host_available_bytes'):
            value = resources['resources'].get(name)
            require(type(value) is int and value >= 0, 'INVALID_RESOURCE_COUNTER')
        require(finite(resources['resources'].get('host_steal_fraction')) and resources['resources']['host_steal_fraction'] <= 1, 'INVALID_STEAL_COUNTER')
        require(mode not in groups.setdefault(prefix, {}), 'DUPLICATE_PAIRED_MODE')
        groups[prefix][mode] = (resources, client)
    require(all(set(pair) == {'disabled', 'enabled'} for pair in groups.values()), 'UNPAIRED_PROFILE')
    output = {'schema': 1, 'passed': True, 'pairs': len(groups), 'binary_sha256': binary_sha256,
              'normalized_config_sha256': identity[0], 'worker_input_sha256': identity[1]['sha256'],
              'client_inputs': identity[2], 'modes': {},
              'scope': 'One executor, three serial queries then ten concurrent queued submissions per profile. Service cgroup includes observer; source, gateway, NATS and SSH excluded. Sample p95 is nearest rank, not a confidence bound.'}
    for mode in ('disabled', 'enabled'):
        profiles = [pair[mode] for pair in groups.values()]
        summary = {'profiles': len(profiles), 'cases': {}, 'resources': {}}
        for name in ('native_million', 'federated_million', 'federated_cte_join', 'queued_million'):
            cases = [case for _, client in profiles for case in client['cases'] if case['name'] == name or (name == 'queued_million' and case['name'].startswith('queued_million_'))]
            summary['cases'][name] = {field: distribution([case[field] for case in cases]) for field in ('submission_seconds', 'assignment_wait_seconds', 'first_byte_seconds', 'complete_seconds')}
            if name != 'federated_cte_join':
                summary['cases'][name]['rows_per_second_including_queue'] = distribution([case['output_rows'] / case['complete_seconds'] for case in cases])
        for name in ('rss_node_peak_bytes', 'rss_workers_peak_bytes', 'rss_combined_peak_bytes', 'memory_peak_bytes', 'minimum_host_available_bytes', 'host_steal_fraction'):
            summary['resources'][name] = distribution([resource['resources'][name] for resource, _ in profiles])
        output['modes'][mode] = summary
    return output


def read_json(path):
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 2 << 20, 'BOUNDED_REPORT_REQUIRED')
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--expected-binary-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    entries = []
    for entry in read_json(args.directory / 'summary.json'):
        profile = entry.get('profile')
        require(entry.get('passed') is True and isinstance(profile, str) and re.fullmatch(r'[a-z0-9-]{1,48}', profile), 'INCOMPLETE_PROFILE')
        directory = args.directory / profile
        require(not directory.is_symlink(), 'REGULAR_PROFILE_REQUIRED')
        entries.append((profile, read_json(directory / 'worker.json'), read_json(directory / 'client.json')))
    result = reconcile(entries, args.expected_binary_sha256)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as out:
        json.dump(result, out, indent=2)
        out.write('\n')
    print(json.dumps({'passed': result['passed'], 'pairs': result['pairs']}))


if __name__ == '__main__':
    main()
