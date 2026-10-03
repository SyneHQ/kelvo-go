import copy
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import node_capacity_worker as worker

TEMPLATE = '''metrics:
  enabled: @METRICS@
scratch_directory: '@SCRATCH@'
containment:
  root: '@GROUP@'
  state_directory: '@STATE@'
'''


def valid():
    return {'interrupted': False, 'node_exit_code': 0, 'forced_node_kill': False, 'stop_file_observed': True,
            'config_unchanged': True, 'binary_unchanged': True, 'inputs_unchanged': True,
            'owned_service_removed': True, 'owned_cgroup_removed': True, 'service_exit_code': 0,
            'scratch_directories_remaining': 0, 'containment_records_remaining': 0, 'elapsed_seconds': 10.,
            'resources': {'samples': 100, 'rss_node_peak_bytes': 1000, 'rss_workers_peak_bytes': 1000, 'rss_combined_peak_bytes': 2000,
                          'max_workers': 1, 'scratch_peak_bytes': 0, 'live_owned_processes_after_shutdown': 0, 'read_errors': [],
                          'maximum_sample_gap_seconds': .1, 'memory_events': {'oom': 0, 'oom_kill': 0, 'max': 0},
                          'pids_events': {'max': 0}, 'cpu_stat': {'usage_usec': 10, 'user_usec': 5, 'system_usec': 5}}}


class CapacityControls(unittest.TestCase):
    def test_digest_uses_bounded_reads_and_preserves_digest(self):
        payload = b'bounded identity\x00' * 180000
        reads = []

        class BoundedReader(io.BytesIO):
            def read(self, size=-1):
                self_test.assertGreater(size, 0)
                self_test.assertLessEqual(size, 1 << 20)
                reads.append(size)
                return super().read(size)

        self_test = self
        with mock.patch.object(Path, 'open', return_value=BoundedReader(payload)):
            self.assertEqual(worker.digest(Path('fixture')), hashlib.sha256(payload).hexdigest())
        self.assertGreater(len(reads), 2)

    def test_shutdown_and_last_sample_precede_identity_verification(self):
        events = []

        class Process:
            returncode = None
            stopping = False
            polls = 0

            def poll(self):
                if self.stopping:
                    self.polls += 1
                    if self.polls == 3:
                        self.returncode = 0
                return self.returncode

            def send_signal(self, _):
                events.append('stop')
                self.stopping = True

        process = Process()

        class Sampler:
            def sample(self):
                self_test.assertIsNone(process.returncode)
                events.append('sample')

            def final(self):
                self_test.assertEqual(process.returncode, 0)
                events.append('final')
                return {'live_owned_processes_after_shutdown': 0, 'captured': True}

        report = {}
        self_test = self

        def verify():
            self.assertEqual(process.returncode, 0)
            self.assertEqual(events[-1], 'final')
            self.assertTrue(report['resources']['captured'])
            events.append('verify')

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(worker.time, 'sleep'):
            profile = Path(directory)
            (profile / 'scratch').mkdir()
            (profile / 'containment').mkdir()
            worker.finish_observation(process, Sampler(), profile, report, verify)
        self.assertEqual(events, ['stop', 'sample', 'sample', 'final', 'verify'])
        self.assertEqual(report['node_exit_code'], 0)

    def test_surviving_worker_or_failed_final_sample_blocks_hashing(self):
        for failure in ('live_worker', 'raised_sample', 'recorded_sample'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                profile = Path(directory)
                (profile / 'scratch').mkdir()
                (profile / 'containment').mkdir()
                process = mock.Mock(returncode=0)
                process.poll.return_value = 0
                sampler = mock.Mock()
                sampler.final.return_value = {'live_owned_processes_after_shutdown': int(failure == 'live_worker'),
                                              'read_errors': ['OSError'] if failure == 'recorded_sample' else []}
                verify = mock.Mock()
                report = {}
                if failure == 'raised_sample':
                    sampler.final.side_effect = OSError('sample unavailable')
                    with self.assertRaises(OSError):
                        worker.finish_observation(process, sampler, profile, report, verify)
                else:
                    worker.finish_observation(process, sampler, profile, report, verify)
                    self.assertEqual(report['failure'], 'OWNED_PROCESS_SURVIVED_SHUTDOWN' if failure == 'live_worker' else 'RESOURCE_OBSERVATION_FAILED')
                verify.assert_not_called()

    def test_final_identity_mutation_and_read_errors_remain_failures(self):
        for changed in ('config', 'binary', 'catalog', 'missing_binary'):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / 'bin').mkdir()
                (root / 'scripts').mkdir()
                for name in ('catalog.yml', 'node-template.yml', 'node-environment.json', 'bin/kelvo', 'bin/kelvo-landlock'):
                    (root / name).write_bytes(b'original')
                config, binary = root / 'rendered-node.yml', root / 'bin/kelvo'
                config.write_bytes(b'original')
                config_hash, binary_hash, before = worker.digest(config), worker.digest(binary), worker.input_identity(root)
                target = {'config': config, 'binary': binary, 'catalog': root / 'catalog.yml', 'missing_binary': binary}[changed]
                if changed == 'missing_binary':
                    target.unlink()
                else:
                    target.write_bytes(b'changed')
                report = {}
                worker.verify_final_inputs(root, config, config_hash, binary, binary_hash, before, report)
                self.assertEqual(report['failure'], 'INPUT_IDENTITY_VERIFICATION_FAILED')
                self.assertIs(report['config_unchanged'], changed != 'config')
                self.assertIs(report['binary_unchanged'], changed not in ('binary', 'missing_binary'))
                self.assertIs(report['inputs_unchanged'], changed == 'config')
                if changed == 'missing_binary':
                    self.assertEqual({item['check'] for item in report['identity_verification_errors']}, {'binary_unchanged', 'inputs_unchanged'})
                report['failure'] = 'EARLIER_PROCESS_FAILURE'
                worker.verify_final_inputs(root, config, config_hash, binary, binary_hash, before, report)
                self.assertEqual(report['failure'], 'EARLIER_PROCESS_FAILURE')

    def test_boolean_mode_applied_and_other_fields_match(self):
        off, off_hash = worker.render_config(TEMPLATE, Path('/group1'), Path('/state1'), Path('/scratch1'), False)
        on, on_hash = worker.render_config(TEMPLATE, Path('/group2'), Path('/state2'), Path('/scratch2'), True)
        self.assertIs(worker.yaml.safe_load(off)['metrics']['enabled'], False)
        self.assertIs(worker.yaml.safe_load(on)['metrics']['enabled'], True)
        self.assertEqual(off_hash, on_hash)

    def test_missing_duplicate_unknown_or_quoted_metrics_marker_fails(self):
        for text in (TEMPLATE.replace('@METRICS@', 'true'), TEMPLATE + '# @METRICS@\n', TEMPLATE + 'unknown: "@UNKNOWN@"\n', TEMPLATE.replace('@METRICS@', '"@METRICS@"')):
            with self.assertRaises(worker.ops.AcceptanceError):
                worker.render_config(text, Path('/g'), Path('/s'), Path('/t'), False)

    def test_incomplete_or_gapped_samples_fail(self):
        self.assertTrue(worker.valid_profile(valid()))
        for key, value in (('samples', 19), ('samples', True), ('maximum_sample_gap_seconds', 2.1), ('maximum_sample_gap_seconds', float('nan')), ('rss_workers_peak_bytes', 0), ('read_errors', ['ERROR'])):
            report = valid()
            report['resources'][key] = value
            self.assertFalse(worker.valid_profile(report))
        report = valid()
        report['elapsed_seconds'] = 4.9
        self.assertFalse(worker.valid_profile(report))

    def test_corrupt_counters_cleanup_and_input_mutation_fail(self):
        for category, fields in (('memory_events', ('oom', 'oom_kill', 'max')), ('pids_events', ('max',)), ('cpu_stat', ('usage_usec', 'user_usec', 'system_usec'))):
            for field in fields:
                for value in (-1, False, '0', None):
                    report = valid()
                    report['resources'][category][field] = value
                    self.assertFalse(worker.valid_profile(report))
                report = valid()
                del report['resources'][category][field]
                self.assertFalse(worker.valid_profile(report))
        for field in ('config_unchanged', 'binary_unchanged', 'inputs_unchanged', 'owned_service_removed', 'owned_cgroup_removed'):
            report = valid()
            report[field] = False
            self.assertFalse(worker.valid_profile(report))


if __name__ == '__main__':
    unittest.main()
