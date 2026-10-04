"""Pure receipt controls for future VM execution; no fixture or network starts."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import harness as h


def epochs(contrasts, pairs):
    result = []
    for contrast, (left, right) in enumerate(contrasts):
        for pair in range(pairs):
            order = (left, right) if pair % 2 == 0 else (right, left)
            for position, variant in enumerate(order):
                observations = []
                for workload in h.SQL:
                    observations.append({'workload': workload, 'receive_seconds': 1.0,
                        'answer': {'rows': 10 if workload == 'aggregate' else h.ROWS,
                                   'batches': 1, 'wire_bytes': 128, 'exact_values': True,
                                   'exact_types': True, 'explicit_eos': True, 'no_trailing_bytes': True}})
                collector = {key: 0 for key in ('requests', 'received_bytes', 'accepted_spans',
                    'observed_submission_spans', 'observed_query_spans',
                    'observed_independent_startup_query_spans', 'duplicate_span_ids',
                    'observed_missing_parent_links')}
                collector.update(summary_sha256='a' * 64, ledger_sha256='b' * 64)
                result.append({'contrast': contrast, 'pair': pair, 'position': position,
                    'variant': variant, 'metrics': h.VARIANTS[variant][0],
                    'tracing_ratio': h.VARIANTS[variant][1], 'passed': True,
                    'warmups': copy.deepcopy(observations), 'measured': copy.deepcopy(observations),
                    'cleanup_failures': [], 'observed_live_descendants_after_cleanup': 0,
                    'collector': collector})
    return result


def receipt():
    value = {'schema_version': 1, 'mode': 'smoke', 'passed': True, 'pairs': 1,
             'rows': h.ROWS, 'unit': 'kelvo-telemetry-overhead-0123456789ab',
             'operation_budget_seconds': h.SMOKE_SECONDS, 'hard_watchdog_seconds': 900,
             'source': {'base_revision': 'a' * 40, 'sha256': 'b' * 64},
             'binary_sha256': 'c' * 64, 'sandbox_sha256': 'd' * 64,
             'collector_sha256': 'e' * 64, 'helper_sha256': {'harness.py': 'f' * 64, 'workloads.py': 'a' * 64},
             'workload_sha256': 'b' * 64, 'data_sha256': 'c' * 64,
             'epochs': epochs(h.CONTRASTS[:1], 1)}
    value['smoke_checks'] = h.smoke_summary(value['epochs'])
    for key in ('brokers_exited', 'source_unchanged', 'data_unchanged', 'binary_unchanged',
                'sandbox_unchanged', 'collector_unchanged', 'helpers_unchanged',
                'workload_unchanged', 'independent_service_cleanup_required'):
        value[key] = True
    return value


class ReceiptTests(unittest.TestCase):
    def test_campaign_guard_ignores_service_descriptions(self):
        owned = 'kelvo-telemetry-overhead-0123456789ab'
        output = (f'  {owned}.service loaded active running '
            '/home/syenuser/kelvo-analytics-8d3bb8bc/venv/bin/python -B /fixture/run.py\n'
            'ssh.service loaded active running description mentions kelvo-sustained-deadbeef.service '
            'and kelvo-telemetry-overhead-aaaaaaaaaaaa.service\n\n')
        h.validate_running_units(output, owned)

    def test_campaign_guard_rejects_actual_performance_units(self):
        owned = 'kelvo-telemetry-overhead-0123456789ab'
        base = f'{owned}.service loaded active running telemetry fixture\n'
        for unit in ('kelvo-sustained-deadbeef.service', 'kelvo-analytics-live.service',
                     'kelvo-oracle-capacity-test.service', 'kelvo-query-benchmark-test.service'):
            with self.subTest(unit=unit), self.assertRaisesRegex(RuntimeError, '^OTHER_PERFORMANCE_CAMPAIGN_RUNNING$'):
                h.validate_running_units(base + f'{unit} loaded active running fixture\n', owned)

    def test_campaign_guard_requires_exact_owned_telemetry_unit(self):
        owned = 'kelvo-telemetry-overhead-0123456789ab'
        other = 'kelvo-telemetry-overhead-aaaaaaaaaaaa.service loaded active running telemetry fixture\n'
        for output in ('', other, f'{owned}.service loaded active running telemetry fixture\n' + other,
                       f'ssh.service loaded active running description mentions {owned}.service\n'):
            with self.subTest(output=output), self.assertRaisesRegex(RuntimeError, '^OTHER_TELEMETRY_EXPERIMENT_RUNNING$'):
                h.validate_running_units(output, owned)

    def test_smoke_receipt_hash_and_size_are_checked_before_use(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'smoke.json'
            raw = json.dumps(receipt()).encode()
            path.write_bytes(raw)
            checksum = hashlib.sha256(raw).hexdigest()
            self.assertEqual(h.read_smoke_receipt(path, checksum), receipt())
            path.write_bytes(raw + b'\n')
            with self.assertRaisesRegex(RuntimeError, '^SMOKE_RECEIPT_HASH$'):
                h.read_smoke_receipt(path, checksum)
            with path.open('wb') as stream:
                stream.truncate(h.SMOKE_RECEIPT_BYTES + 1)
            with self.assertRaisesRegex(RuntimeError, '^SMOKE_RECEIPT_BOUND$'):
                h.read_smoke_receipt(path, checksum)

    def test_exact_smoke_counts_without_comparison(self):
        smoke = receipt()
        h.check_smoke_receipt(smoke, copy.deepcopy(smoke))
        self.assertEqual(smoke['smoke_checks']['epochs'], 2)
        self.assertEqual(smoke['smoke_checks']['warmup_queries'], 4)
        self.assertEqual(smoke['smoke_checks']['measured_queries'], 4)
        self.assertEqual(smoke['smoke_checks']['verified_result_rows'], {'aggregate': 40, 'transfer': 4_000_000})
        self.assertNotIn('comparison', smoke)

    def test_smoke_rejects_partial_duplicate_failed_or_invalid_result(self):
        def duplicate_epoch(value):
            value['epochs'][1] = copy.deepcopy(value['epochs'][0])
        def duplicate_workload(value):
            value['epochs'][0]['measured'][1] = copy.deepcopy(value['epochs'][0]['measured'][0])
        cases = {
            'partial': lambda v: v['epochs'].pop(),
            'extra': lambda v: v['epochs'].append(copy.deepcopy(v['epochs'][0])),
            'duplicate_epoch': duplicate_epoch,
            'duplicate_workload': duplicate_workload,
            'failed_epoch': lambda v: v['epochs'][0].update(passed=False),
            'wrong_rows': lambda v: v['epochs'][0]['measured'][1]['answer'].update(rows=999_999),
            'wrong_values': lambda v: v['epochs'][0]['warmups'][0]['answer'].update(exact_values=False),
            'export_traffic': lambda v: v['epochs'][0]['collector'].update(requests=1),
            'missing_collector': lambda v: v['epochs'][0].pop('collector'),
            'cleanup_failure': lambda v: v['epochs'][0].update(cleanup_failures=['worker']),
            'live_descendant': lambda v: v['epochs'][0].update(observed_live_descendants_after_cleanup=1),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                value = receipt()
                mutate(value)
                with self.assertRaises(RuntimeError):
                    h.check_smoke_receipt(value, receipt())

    def test_full_prerequisite_requires_matching_inputs_and_cleanup(self):
        for key in ('source', 'binary_sha256', 'sandbox_sha256', 'collector_sha256',
                    'helper_sha256', 'workload_sha256', 'data_sha256'):
            with self.subTest(identity=key):
                smoke, current = receipt(), receipt()
                current[key] = None
                with self.assertRaisesRegex(RuntimeError, '^SMOKE_INPUT_IDENTITY_MISMATCH$'):
                    h.check_smoke_receipt(smoke, current)
        for key in ('brokers_exited', 'source_unchanged', 'data_unchanged', 'binary_unchanged',
                    'sandbox_unchanged', 'collector_unchanged', 'helpers_unchanged', 'workload_unchanged'):
            with self.subTest(cleanup=key):
                smoke = receipt()
                smoke[key] = False
                with self.assertRaisesRegex(RuntimeError, '^SMOKE_CLEANUP_OR_INTEGRITY$'):
                    h.check_smoke_receipt(smoke, receipt())
        for update in ({'mode': 'full'}, {'passed': False}, {'comparison': []}, {'schema_version': True}):
            with self.subTest(update=update):
                smoke = receipt()
                smoke.update(update)
                with self.assertRaisesRegex(RuntimeError, '^PASSED_SMOKE_REQUIRED$'):
                    h.check_smoke_receipt(smoke, receipt())

    def test_full_summary_keeps_exact_48_epoch_contract(self):
        full = epochs(h.CONTRASTS, h.PAIRS)
        self.assertEqual(len(full), 48)
        self.assertEqual(len(h.summarize(full)), 8)
        with self.assertRaises(RuntimeError):
            h.summarize(epochs(h.CONTRASTS[:1], 1))
        for mutate in (lambda v: v.pop(), lambda v: v.append(copy.deepcopy(v[0])),
                       lambda v: v[0].update(passed=False), lambda v: v.__setitem__(1, copy.deepcopy(v[0]))):
            invalid = copy.deepcopy(full)
            mutate(invalid)
            with self.assertRaises(RuntimeError):
                h.summarize(invalid)

    def test_full_summary_rechecks_answers_configuration_and_cleanup(self):
        cases = {
            'rows': lambda v: v[0]['measured'][1]['answer'].update(rows=999_999),
            'types': lambda v: v[0]['warmups'][0]['answer'].update(exact_types=False),
            'eos': lambda v: v[0]['measured'][0]['answer'].update(explicit_eos=False),
            'values': lambda v: v[0]['warmups'][1]['answer'].update(exact_values=False),
            'trailing': lambda v: v[0]['measured'][1]['answer'].update(no_trailing_bytes=False),
            'missing_workload': lambda v: v[0]['measured'].pop(),
            'duplicate_workload': lambda v: v[0]['warmups'].__setitem__(1, copy.deepcopy(v[0]['warmups'][0])),
            'cleanup': lambda v: v[0].update(cleanup_failures=['worker']),
            'descendant': lambda v: v[0].update(observed_live_descendants_after_cleanup=1),
            'metrics': lambda v: v[0].update(metrics=True),
            'tracing': lambda v: v[0].update(tracing_ratio=0),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                value = epochs(h.CONTRASTS, h.PAIRS)
                mutate(value)
                with self.assertRaises(RuntimeError):
                    h.summarize(value)


if __name__ == '__main__':
    unittest.main()
