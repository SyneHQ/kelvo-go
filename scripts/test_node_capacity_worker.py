import copy
from pathlib import Path
import unittest

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
