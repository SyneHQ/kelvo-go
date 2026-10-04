"""Pure formatter controls. Synthetic data only; no benchmark or network runs."""
import copy
import hashlib
import json
import math
import unittest
import format_summary as f


def stats(values):
    values = sorted(values)
    mean = sum(values) / 6
    return {'count': 6, 'median': (values[2] + values[3]) / 2, 'mean': mean,
        'sample_stddev': math.sqrt(sum((value - mean) ** 2 for value in values) / 5),
        'min': values[0], 'max': values[-1],
        'iqr': (values[3] + .75 * (values[4] - values[3])) - (values[1] + .25 * (values[2] - values[1]))}


def fixture():
    revision, unit = 'a' * 40, 'kelvo-telemetry-overhead-0123456789ab'
    def cleanup(name):
        return {'unit': name, 'passed': True, 'owned_service_removed': True, 'owned_cgroup_removed': True,
            'fields': {'LoadState': 'not-found', 'ActiveState': 'inactive', 'SubState': 'dead', 'MainPID': '0'}}
    files = {'main.go': 'b' * 64}
    source = {'base_revision': revision, 'verified': True, 'files': files, 'file_count': 1,
        'sha256': hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
    control_unit = 'kelvo-telemetry-controls-0123456789ab'
    receipt = {'passed': True, 'mode': 'full', 'revision': revision, 'unit': unit,
        'collection_ssh_exit_code': 0, 'cleanup': cleanup(unit),
        'service_exit': {'revision': revision, 'mode': 'full', 'unit': unit, 'systemd_run_exit_code': 0},
        'local_launch': {'revision': revision, 'mode': 'full', 'unit': unit, 'ssh_exit_code': 0},
        'controls': {'passed': True, 'revision': revision, 'unit': control_unit, 'systemd_run_exit_code': 0,
            'controls_sha256': 'c' * 64, 'staging_json_sha256': 'd' * 64, 'cleanup': cleanup(control_unit)},
        'trust_preparation': {key: 'e' * 64 for key in ('system_bundle_before_sha256', 'prepared_bundle_sha256', 'collector_certificate_sha256')},
        'enforced_isolation': {'memory_bytes': 6 << 30, 'swap_bytes': 0, 'tasks': 512,
            'cpu_period': 100000, 'cpu_quota': 200000, 'non_root': True, 'private_network': True},
        'inner_receipt_sha256': 'f' * 64, 'staging_json_sha256': 'd' * 64,
        'runner_receipt_sha256': 'a' * 64, 'runner_sha256': 'a' * 64, 'service_exit_sha256': 'b' * 64,
        'python_identity': {'executable': '/fixture/python3', 'executable_sha256': 'c' * 64, 'version': '3.12.0 fixture',
            'packages': {name: {'version': '1.0', 'metadata_sha256': 'd' * 64, 'record_sha256': 'e' * 64}
                         for name in ('pyarrow', 'PyYAML')}}}
    receipt.update({key: True for key in f.OUTER_CHECKS})
    inner = {'passed': True, 'mode': 'full', 'unit': unit, 'schema_version': 1, 'pairs': 6, 'rows': f.ROWS,
        'operation_budget_seconds': 2400, 'hard_watchdog_seconds': 3600, 'source': source,
        'binary_sha256': '1' * 64, 'sandbox_sha256': '2' * 64, 'collector_sha256': '3' * 64,
        'workload_sha256': '4' * 64, 'data_sha256': '5' * 64, 'prerequisite_smoke_sha256': '6' * 64,
        'prior_receipt_sha256': '7' * 64, 'helper_sha256': {'harness.py': '8' * 64, 'workloads.py': '9' * 64},
        'epochs': [], 'comparison': []}
    inner.update({key: True for key in ('brokers_exited', 'source_unchanged', 'data_unchanged',
        'binary_unchanged', 'sandbox_unchanged', 'collector_unchanged', 'helpers_unchanged', 'workload_unchanged',
        'independent_service_cleanup_required', 'prerequisite_smoke_reconciled', 'prerequisite_smoke_unchanged')})
    receipt['runtime'] = {'source': source, 'binary_sha256': {'kelvo': '1' * 64, 'kelvo-landlock': '2' * 64, 'collector': '3' * 64},
        'effective_module_files': {'mod_sha256': 'a' * 64, 'sum_sha256': 'b' * 64},
        'shared_inputs': {'driver_tree_sha256': 'c' * 64, 'headers_tree_sha256': 'd' * 64, 'driver_patch_marker_sha256': 'e' * 64},
        'otlp_public_receipt_sha256': 'f' * 64, 'original_validation_sha256': 'a' * 64, 'nats_sha256': 'b' * 64}
    receipt['inner_receipt'] = inner
    for contrast, (left, right) in enumerate(f.CONTRASTS):
        for pair in range(6):
            order = (left, right) if pair % 2 == 0 else (right, left)
            for position, variant in enumerate(order):
                observations = []
                for offset, workload in enumerate(('aggregate', 'transfer')):
                    baseline = 1.0 + contrast / 10 + pair / 100 + offset / 5
                    duration = baseline if variant == left else baseline * (1.01 + pair / 100)
                    observations.append({'workload': workload, 'receive_seconds': duration,
                        'answer': {'rows': 10 if workload == 'aggregate' else f.ROWS, 'batches': 1, 'wire_bytes': 128,
                            'exact_values': True, 'exact_types': True, 'explicit_eos': True, 'no_trailing_bytes': True}})
                full_trace = variant == 'trace_all'
                collector = {'summary_sha256': '7' * 64, 'ledger_sha256': '8' * 64,
                    'requests': 3 if full_trace else 0, 'received_bytes': 1000 if full_trace else 0,
                    'accepted_spans': 47 if full_trace else 0,
                    'observed_submission_spans': 4 if full_trace else 0,
                    'observed_query_spans': 4 if full_trace else 0,
                    'observed_independent_startup_query_spans': 1 if full_trace else 0,
                    'duplicate_span_ids': 0, 'observed_missing_parent_links': 0,
                    'sdk_offered_spans': None, 'sdk_lost_spans': None,
                    'loss_scope': 'Offered denominator unavailable; fractional sampling and absent parents are not proof of exporter loss.'}
                inner['epochs'].append({'contrast': contrast, 'pair': pair, 'position': position,
                    'variant': variant, 'metrics': f.VARIANTS[variant][0], 'tracing_ratio': f.VARIANTS[variant][1],
                    'passed': True, 'cleanup_failures': [], 'observed_live_descendants_after_cleanup': 0,
                    'collector': collector,
                    'warmups': copy.deepcopy(observations), 'measured': copy.deepcopy(observations)})
        for offset, workload in enumerate(('aggregate', 'transfer')):
            pairs = []
            for pair in range(6):
                baseline = 1.0 + contrast / 10 + pair / 100 + offset / 5
                variant = baseline * (1.01 + pair / 100)
                pairs.append({'pair': pair, 'left_seconds': baseline, 'right_seconds': variant,
                    'delta_seconds': variant - baseline, 'ratio': variant / baseline})
            inner['comparison'].append({'contrast': [left, right], 'workload': workload, 'pairs': pairs,
                'left': stats([v['left_seconds'] for v in pairs]), 'right': stats([v['right_seconds'] for v in pairs]),
                'delta': stats([v['delta_seconds'] for v in pairs]), 'ratio': stats([v['ratio'] for v in pairs])})
    return receipt


class FormatterTests(unittest.TestCase):
    def test_two_tables_with_all_four_comparisons(self):
        markdown = f.format_receipt(fixture())
        for label in f.LABELS:
            self.assertEqual(markdown.count('| ' + label + ' |'), 2)
        self.assertEqual(markdown.count('| Comparison |'), 2)
        self.assertIn('Warmups are excluded', markdown)
        self.assertIn('1.035x', markdown)
        self.assertIn('1025.000', markdown)
        self.assertIn('one million source rows, ten groups returned', markdown)
        self.assertIn('not confidence intervals or statistical significance', markdown)
        self.assertIn('do not establish production capacity', markdown)

    def test_refuses_smoke_failed_or_unreconciled_receipts(self):
        for change in ({'mode': 'smoke'}, {'passed': False}, {'host_trust_unchanged': False},
                       {'controls_reconciled': False}, {'collection_ssh_exit_code': True}):
            with self.subTest(change=change), self.assertRaises(f.ReceiptError):
                f.format_receipt(dict(fixture(), **change))
        value = fixture()
        value['cleanup']['owned_cgroup_removed'] = False
        with self.assertRaises(f.ReceiptError):
            f.format_receipt(value)

    def test_rejects_incomplete_or_inconsistent_epochs(self):
        cases = {
            'missing': lambda v: v['epochs'].pop(),
            'duplicate': lambda v: v['epochs'].__setitem__(1, copy.deepcopy(v['epochs'][0])),
            'missing_workload': lambda v: v['epochs'][0]['measured'].pop(),
            'wrong_rows': lambda v: v['epochs'][0]['measured'][1]['answer'].update(rows=2),
            'eos': lambda v: v['epochs'][0]['measured'][1]['answer'].update(explicit_eos=False),
            'nan': lambda v: v['epochs'][0]['measured'][1].update(receive_seconds=float('nan')),
            'boolean_duration': lambda v: v['epochs'][0]['measured'][1].update(receive_seconds=True),
            'cleanup': lambda v: v['epochs'][0].update(cleanup_failures=['worker']),
            'variant': lambda v: v['epochs'][0].update(metrics=True),
            'summary': lambda v: v['comparison'][0]['ratio'].update(median=0.5),
            'pair': lambda v: v['comparison'][0]['pairs'][0].update(ratio=0.5),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name), self.assertRaises(f.ReceiptError):
                value = fixture()
                mutate(value['inner_receipt'])
                f.format_receipt(value)

    def test_duplicate_json_fields_are_rejected(self):
        with self.assertRaises(f.ReceiptError):
            json.loads('{"passed":false,"passed":true}', object_pairs_hook=f.unique_object)

    def test_missing_or_mismatched_provenance_is_rejected(self):
        cases = {
            'missing_helpers': lambda v: v['inner_receipt'].pop('helper_sha256'),
            'helper_keys': lambda v: v['inner_receipt']['helper_sha256'].pop('workloads.py'),
            'helper_hash': lambda v: v['inner_receipt']['helper_sha256'].update({'workloads.py': 'invalid'}),
            'missing_python': lambda v: v.pop('python_identity'),
            'missing_package': lambda v: v['python_identity']['packages'].pop('PyYAML'),
            'package_hash': lambda v: v['python_identity']['packages']['pyarrow'].update(record_sha256='invalid'),
            'missing_stage': lambda v: v.pop('staging_json_sha256'),
            'stage_mismatch': lambda v: v['controls'].update(staging_json_sha256='0' * 64),
            'missing_runner': lambda v: v.pop('runner_receipt_sha256'),
            'runner_mismatch': lambda v: v.update(runner_receipt_sha256='0' * 64),
            'service_hash': lambda v: v.update(service_exit_sha256=True),
            'prior_hash': lambda v: v['inner_receipt'].pop('prior_receipt_sha256'),
            'module_hash': lambda v: v['runtime']['effective_module_files'].pop('sum_sha256'),
            'shared_hash': lambda v: v['runtime']['shared_inputs'].update(driver_tree_sha256='invalid'),
            'otlp_hash': lambda v: v['runtime'].pop('otlp_public_receipt_sha256'),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name), self.assertRaises(f.ReceiptError):
                value = fixture()
                mutate(value)
                f.format_receipt(value)

    def test_missing_or_inconsistent_collector_evidence_is_rejected(self):
        cases = {
            'missing_counter': lambda v: v.pop('requests'),
            'boolean_counter': lambda v: v.update(accepted_spans=True),
            'negative_counter': lambda v: v.update(received_bytes=-1),
            'unbounded_counter': lambda v: v.update(requests=129),
            'disabled_exports': lambda v: v.update(requests=1),
            'span_count': lambda v: v.update(observed_submission_spans=1),
            'unknown_denominator': lambda v: v.update(sdk_offered_spans=0),
            'unknown_loss': lambda v: v.update(sdk_lost_spans=0),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name), self.assertRaises(f.ReceiptError):
                value = fixture()
                mutate(value['inner_receipt']['epochs'][0]['collector'])
                f.format_receipt(value)
        for change in ({'observed_independent_startup_query_spans': 0}, {'accepted_spans': 46}, {'accepted_spans': 52}):
            with self.subTest(change=change), self.assertRaises(f.ReceiptError):
                value = fixture()
                full = next(epoch for epoch in value['inner_receipt']['epochs'] if epoch['variant'] == 'trace_all')
                full['collector'].update(change)
                f.format_receipt(value)


if __name__ == '__main__':
    unittest.main()
