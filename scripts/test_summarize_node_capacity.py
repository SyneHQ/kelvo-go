import copy
import hashlib
import json
import unittest

import summarize_node_capacity as summary
from test_node_capacity_worker import valid as valid_worker


BINARY = 'a' * 64


def profiles():
    entries = []
    for pair in range(1, 6):
        for mode in ('disabled', 'enabled'):
            worker = valid_worker()
            worker.update(passed=True, metrics_enabled=mode == 'enabled', binary_sha256=BINARY,
                          normalized_config_sha256='b' * 64,
                          resource_budget={'memory_bytes': 640 << 20, 'swap_bytes': 0, 'tasks': 128, 'cpu_weight': 20})
            files = {'bin/kelvo': BINARY}
            worker['inputs'] = {'files': files, 'sha256': hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
            worker['resources'].update(memory_peak_bytes=10000, minimum_host_available_bytes=100000, host_steal_fraction=.02)
            client = {'passed': True, 'metrics_enabled': mode == 'enabled', 'metrics_route_verified': True,
                      'inputs_unchanged': True, 'queued_operations_requested': 10,
                      'inputs': {name: 'd' * 64 for name in ('client_script', 'protocol_reader', 'private_control', 'projection_reference', 'join_reference')},
                      'queued_burst': {'concurrent_submissions': 10, 'worker_execution_permits': 1, 'native_operations': 5, 'federated_operations': 5, 'rows_per_operation': 1000000}, 'cases': []}
            if mode == 'enabled':
                client.update(metrics_success_delta_verified=True, metrics_successes_before_workload=1, metrics_successes_after_workload=14)
            for name in summary.CASE_NAMES:
                join = name == 'federated_cte_join'
                client['cases'].append({'name': name, 'passed': True, 'exact_values_and_nulls': True,
                                        'durable_completion': True, 'output_rows': 8 if join else 1000000,
                                        'arrow_types': ['string', 'int64', 'decimal128(38, 0)'] if join else ['int64', 'int32', 'int64'],
                                        'encoded_bytes': 840, 'submission_seconds': .1, 'assignment_wait_seconds': .3,
                                        'first_byte_seconds': .5, 'complete_seconds': 1., 'dispatch_observation_seconds': .4,
                                        'reference_sha256': 'c' * 64, 'body_sha256': 'e' * 64})
            entries.append((f'pair{pair:02d}-{mode}', worker, client))
    return entries


class CapacitySummaryControls(unittest.TestCase):
    def test_requires_complete_matched_pairs(self):
        result = summary.reconcile(profiles(), BINARY)
        self.assertEqual(result['pairs'], 5)
        self.assertEqual(result['modes']['enabled']['cases']['queued_million']['complete_seconds']['samples'], 50)
        for entries in (profiles()[:-1], profiles()[:8], profiles() + [profiles()[0], profiles()[1]]):
            with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)

    def test_resource_success_does_not_override_failed_or_incomplete_delivery(self):
        for field, value in [('passed', False), ('durable_completion', False), ('output_rows', 999999), ('output_rows', 1000000.), ('encoded_bytes', True), ('first_byte_seconds', float('nan')), ('complete_seconds', 0), ('arrow_types', ['string'])]:
            entries = profiles()
            entries[0][2]['cases'][0][field] = value
            with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)
        entries = profiles()
        entries[0][2]['cases'].pop()
        with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)

    def test_modes_inputs_binary_and_resource_budgets_cannot_drift(self):
        for location, field, value in [('worker', 'metrics_enabled', 0), ('worker', 'binary_sha256', 'f' * 64), ('worker', 'normalized_config_sha256', 'f' * 64), ('client', 'inputs_unchanged', False), ('client', 'queued_operations_requested', 10.)]:
            entries = profiles()
            entries[0][1 if location == 'worker' else 2][field] = value
            with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)
        for category, field, value in [('resource_budget', 'memory_bytes', 1 << 30), ('resource_budget', 'swap_bytes', False), ('resources', 'memory_peak_bytes', -1), ('resources', 'host_steal_fraction', True), ('resources', 'host_steal_fraction', 1.1)]:
            entries = profiles()
            entries[0][1][category][field] = value
            with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)
        entries = profiles()
        entries[0][2]['inputs']['client_script'] = 'f' * 64
        with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)

    def test_zero_oom_cleanup_and_complete_sampling_remain_required(self):
        entries = profiles()
        entries[0][1]['resources']['memory_events']['oom_kill'] = 1
        with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)
        entries = profiles()
        entries[0][1]['scratch_directories_remaining'] = 1
        with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)
        entries = profiles()
        entries[0][1]['resources']['maximum_sample_gap_seconds'] = 3
        with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)

    def test_metrics_evidence_requires_startup_probe_and_workload_delta(self):
        for field, value in [('metrics_success_delta_verified', False), ('metrics_successes_before_workload', 0),
                             ('metrics_successes_before_workload', True), ('metrics_successes_after_workload', 13),
                             ('metrics_successes_after_workload', 14.0)]:
            entries = profiles()
            entries[1][2][field] = value
            with self.assertRaises(ValueError): summary.reconcile(entries, BINARY)


if __name__ == '__main__':
    unittest.main()
