import copy
import hashlib
import io
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
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


def launch_args():
    return SimpleNamespace(unit='kelvo-micro-node-123456abcdef', profile='off-1', metrics='disabled',
                           runtime=60, expires_at=int(time.time()) + 3600)


def owned_status(intent):
    command = worker.inside_command(intent)
    return {'LoadState': 'loaded', 'Description': 'Kelvo capacity ' + intent['token'],
            'User': intent['account'], 'Transient': 'yes', 'KillMode': 'control-group',
            'SendSIGKILL': 'yes', 'Restart': 'no',
            'RuntimeMaxUSec': worker.format_systemd_seconds(intent['runtime'] + worker.RUNTIME_GRACE_SECONDS),
            'TimeoutStopUSec': worker.format_systemd_seconds(worker.STOP_SECONDS),
            'ControlGroup': '/system.slice/' + intent['unit'] + '.service',
            'ExecStart': '{ path=' + command[0] + ' ; argv[]=' + ' '.join(command)
                         + ' ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=42 ; code=(null) ; status=0/0 }'}


def persist_intent(root, intent):
    profile = root / 'profiles' / intent['profile']
    profile.mkdir(mode=0o700, parents=True)
    worker.create_private_json(profile / 'launch-intent.json', intent)
    return profile


class FakeService:
    def __init__(self, failure=None, before_launch=None):
        self.current = {'LoadState': 'not-found'}
        self.exists = False
        self.events = []
        self.failure = failure
        self.before_launch = before_launch

    def status(self, unit):
        self.events.append('status')
        return dict(self.current)

    def group_exists(self, unit):
        return self.exists

    def launch(self, intent, log):
        self.events.append('launch')
        if self.before_launch:
            self.before_launch(intent)
        self.current, self.exists = owned_status(intent), True
        if self.failure:
            raise self.failure
        return 0

    def stop(self, unit):
        self.events.append('stop')
        self.current, self.exists = {'LoadState': 'not-found'}, False


class LaunchControls(unittest.TestCase):
    def test_symlinked_virtual_environment_python_keeps_its_invocation_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            launcher = root / 'venv/bin/python3'
            launcher.parent.mkdir(parents=True)
            launcher.symlink_to(worker.sys.executable)
            with mock.patch.object(worker.sys, 'executable', str(launcher)):
                intent = worker.new_intent(launch_args(), root, int(time.time()))
            self.assertEqual(intent['python'], str(launcher))
            self.assertNotEqual(intent['python'], str(launcher.resolve(strict=True)))
            self.assertEqual(worker.inside_command(intent)[0], str(launcher))
            self.assertIn(str(launcher), worker.systemd_command(intent))

    def test_explicit_unit_and_independent_hard_expiry_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            intent = worker.new_intent(launch_args(), root, int(time.time()))
            command = worker.systemd_command(intent)
            self.assertIn('--unit=kelvo-micro-node-123456abcdef', command)
            for argument in ('--property=RuntimeMaxSec=80', '--property=TimeoutStopSec=15',
                             '--property=KillMode=control-group', '--property=SendSIGKILL=yes', '--property=Restart=no',
                             '--property=MemoryMax=640M', '--property=MemorySwapMax=0', '--property=TasksMax=128'):
                self.assertIn(argument, command)
            self.assertLess(intent['runtime'] + worker.RUNTIME_GRACE_SECONDS + worker.STOP_SECONDS, intent['runtime'] + 90)
            self.assertEqual(command[-len(worker.inside_command(intent)):], worker.inside_command(intent))

    def test_invalid_unit_expiry_and_ambiguous_paths_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root, now = Path(directory).resolve(), int(time.time())
            for key, value in (('unit', 'ssh'), ('unit', 'kelvo-micro-node-123456abcdef.service'),
                               ('expires_at', now + 99), ('expires_at', float('inf')), ('expires_at', True)):
                args = launch_args()
                setattr(args, key, value)
                with self.subTest(key=key, value=value), self.assertRaises(worker.ops.AcceptanceError):
                    worker.new_intent(args, root, now)
            for name in ('space path', 'semi;colon'):
                unsafe = root / name
                unsafe.mkdir()
                with self.assertRaises(worker.ops.AcceptanceError):
                    worker.new_intent(launch_args(), unsafe, now)

    def test_intent_is_exclusive_private_and_fsynced_before_launch(self):
        events = []
        original = os.fsync

        def observe_fsync(descriptor):
            events.append('directory' if worker.stat.S_ISDIR(os.fstat(descriptor).st_mode) else 'file')
            original(descriptor)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()

            def inspect(intent):
                profile = root / 'profiles' / intent['profile']
                self.assertEqual(worker.read_intent(profile), intent)
                self.assertEqual((profile / 'launch-intent.json').stat().st_mode & 0o777, 0o600)
                self.assertEqual(events[-2:], ['file', 'directory'])
                before = (profile / 'launch-intent.json').read_bytes()
                with self.assertRaises(FileExistsError):
                    worker.create_private_json(profile / 'launch-intent.json', {'reused': True})
                self.assertEqual((profile / 'launch-intent.json').read_bytes(), before)

            service = FakeService(before_launch=inspect)
            with mock.patch.object(worker.os, 'fsync', side_effect=observe_fsync):
                worker.launch_profile(launch_args(), root, service)
            self.assertIn('stop', service.events)

    def test_interrupted_launch_without_started_receipt_still_cleans_exact_service(self):
        for error in (KeyboardInterrupt(), worker.subprocess.TimeoutExpired('systemd-run', 150), OSError('launch lost')):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                service = FakeService(failure=error)
                report = worker.launch_profile(launch_args(), root, service)
                self.assertFalse((root / 'profiles/off-1/started.json').exists())
                self.assertIs(report['owned_service_removed'], True)
                self.assertIs(report['owned_cgroup_removed'], True)
                self.assertFalse(report['passed'])
                self.assertEqual(service.events.count('stop'), 1)

    def test_existing_profile_or_unit_is_never_adopted_or_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            service = FakeService()
            service.current, service.exists = {'LoadState': 'loaded'}, True
            report = worker.launch_profile(launch_args(), root, service)
            self.assertEqual(report['failure'], 'FRESH_SERVICE_REQUIRED')
            profile = root / 'profiles/off-1'
            before = (profile / 'report.json').read_bytes()
            with self.assertRaises(FileExistsError):
                worker.launch_profile(launch_args(), root, service)
            self.assertEqual((profile / 'report.json').read_bytes(), before)
            self.assertNotIn('launch', service.events)
            self.assertNotIn('stop', service.events)

    def test_every_live_identity_mismatch_refuses_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            intent = worker.new_intent(launch_args(), root, int(time.time()))
            persist_intent(root, intent)
            for key in worker.SERVICE_PROPERTIES:
                with self.subTest(key=key):
                    service = FakeService()
                    service.current, service.exists = owned_status(intent), True
                    service.current[key] = 'foreign-or-malformed'
                    report = worker.cleanup_owned_launch(intent, service)
                    self.assertFalse(report['owned_service_removed'])
                    self.assertNotIn('stop', service.events)
            service = FakeService()
            service.current, service.exists = owned_status(intent), True
            service.current['ExecStart'] += ' { path=/other ; argv[]=/other ; }'
            self.assertIn('cleanup_failure', worker.cleanup_owned_launch(intent, service))
            self.assertNotIn('stop', service.events)

    def test_same_name_race_never_stops_the_colliding_service(self):
        class Collision(FakeService):
            def launch(self, intent, log):
                self.events.append('launch')
                self.current, self.exists = owned_status(intent), True
                self.current['Description'] = 'another invocation'
                return 1

        with tempfile.TemporaryDirectory() as directory:
            service = Collision()
            report = worker.launch_profile(launch_args(), Path(directory).resolve(), service)
            self.assertEqual(report['cleanup_failure'], 'SERVICE_OWNERSHIP_MISMATCH')
            self.assertNotIn('stop', service.events)

    def test_missing_service_requires_missing_cgroup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            intent = worker.new_intent(launch_args(), root, int(time.time()))
            persist_intent(root, intent)
            service = FakeService()
            service.exists = True
            report = worker.cleanup_owned_launch(intent, service)
            self.assertTrue(report['owned_service_removed'])
            self.assertFalse(report['owned_cgroup_removed'])
            self.assertNotIn('stop', service.events)

    def test_cleanup_precedes_unwritable_final_report(self):
        with tempfile.TemporaryDirectory() as directory:
            service = FakeService(failure=KeyboardInterrupt())

            def cannot_write(path, report):
                self.assertEqual(service.events.count('stop'), 1)
                self.assertTrue(report['owned_cgroup_removed'])
                raise OSError('receipt unavailable')

            with self.assertRaises(OSError):
                worker.launch_profile(launch_args(), Path(directory).resolve(), service, write_report=cannot_write)

    def test_failed_intent_fsync_preserves_evidence_without_launching(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            service = FakeService()
            original = os.fsync

            def fail_file_fsync(descriptor):
                if worker.stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise OSError('intent not durable')
                original(descriptor)

            with mock.patch.object(worker.os, 'fsync', side_effect=fail_file_fsync):
                report = worker.launch_profile(launch_args(), root, service, write_report=lambda path, report: None)
            self.assertFalse(report['passed'])
            self.assertTrue((root / 'profiles/off-1/launch-intent.json').exists())
            self.assertNotIn('launch', service.events)
            self.assertNotIn('stop', service.events)

    def test_stale_intent_blocks_activation_but_matching_expired_service_is_cleaned(self):
        with tempfile.TemporaryDirectory() as directory:
            root, now = Path(directory).resolve(), int(time.time())
            intent = worker.new_intent(launch_args(), root, now)
            profile = root / 'profiles/off-1'
            profile.mkdir(mode=0o700, parents=True)
            intent['created_at'], intent['expires_at'] = now - 300, now - 1
            worker.create_private_json(profile / 'launch-intent.json', intent)
            with self.assertRaises(worker.ops.AcceptanceError):
                worker.read_intent(profile, now=now)
            service = FakeService()
            service.current, service.exists = owned_status(intent), True
            report = worker.cleanup_launch(profile, service)
            self.assertTrue(report['owned_service_removed'])
            self.assertTrue(report['owned_cgroup_removed'])
            self.assertEqual(service.events.count('stop'), 1)

    def test_default_expiry_allows_bounded_startup_delay(self):
        with tempfile.TemporaryDirectory() as directory:
            root, now = Path(directory).resolve(), int(time.time())
            args = launch_args()
            args.expires_at = None
            intent = worker.new_intent(args, root, now)
            worker.validate_intent(intent, root / 'profiles/off-1', now=now + 20)
            with self.assertRaises(worker.ops.AcceptanceError):
                worker.validate_intent(intent, root / 'profiles/off-1', now=now + 26)

    def test_inside_requires_matching_fresh_unclaimed_intent_and_actual_cgroup(self):
        with tempfile.TemporaryDirectory() as directory:
            root, now, args = Path(directory).resolve(), int(time.time()), launch_args()
            intent = worker.new_intent(args, root, now)
            profile = root / 'profiles/off-1'
            profile.mkdir(mode=0o700, parents=True)
            service = FakeService()
            service.current, service.exists = owned_status(intent), True
            group = '0::/system.slice/' + intent['unit'] + '.service'
            with self.assertRaises(FileNotFoundError):
                worker.claim_launch(args, profile, service, group, now)
            worker.create_private_json(profile / 'launch-intent.json', intent)
            changed = copy.copy(args)
            changed.metrics = 'enabled'
            for candidate, cgroup, clock in ((changed, group, now), (args, '0::/foreign', now), (args, group, now + 61)):
                with self.assertRaises(worker.ops.AcceptanceError):
                    worker.claim_launch(candidate, profile, service, cgroup, clock)
                self.assertFalse((profile / 'launch-claimed.json').exists())
            worker.claim_launch(args, profile, service, group, now)
            with self.assertRaises(FileExistsError):
                worker.claim_launch(args, profile, service, group, now)

    def test_malformed_manager_output_is_rejected(self):
        for output in ('', 'LoadState=loaded\n', 'LoadState=not-found\nLoadState=loaded\n',
                       'permission denied\n', 'LoadState=not-found\nUnknown=value\n'):
            with self.subTest(output=output), self.assertRaises(worker.ops.AcceptanceError):
                worker.parse_service_status(output)

    def test_cleanup_fences_delayed_registration_before_worker_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root, now, args = Path(directory).resolve(), int(time.time()), launch_args()
            intent = worker.new_intent(args, root, now)
            profile = persist_intent(root, intent)
            service = FakeService()
            report = worker.cleanup_launch(profile, service)
            self.assertTrue(report['launch_closed'])
            self.assertTrue(report['owned_service_removed'])
            self.assertTrue(report['owned_cgroup_removed'])
            # StartTransientUnit was already sent but only becomes visible now.
            service.current, service.exists = owned_status(intent), True
            with self.assertRaisesRegex(worker.ops.AcceptanceError, 'LAUNCH_INTENT_CLOSED'):
                worker.claim_launch(args, profile, service, '0::/system.slice/' + intent['unit'] + '.service', now)
            self.assertFalse((profile / 'launch-claimed.json').exists())
            self.assertFalse((profile / 'started.json').exists())
            # Repeated controller cleanup is idempotent and stops that service.
            self.assertTrue(worker.cleanup_launch(profile, service)['owned_cgroup_removed'])
            self.assertEqual(service.events.count('stop'), 1)

    def test_closed_marker_fsync_failure_keeps_cleanup_uncertain_but_stops_owned_service(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            intent = worker.new_intent(launch_args(), root, int(time.time()))
            profile = persist_intent(root, intent)
            service = FakeService()
            service.current, service.exists = owned_status(intent), True
            with mock.patch.object(worker.os, 'fsync', side_effect=OSError('cancel not durable')):
                report = worker.cleanup_launch(profile, service)
            self.assertFalse(report['launch_closed'])
            self.assertFalse(report['owned_service_removed'])
            self.assertFalse(report['owned_cgroup_removed'])
            self.assertEqual(service.events.count('stop'), 1)
            self.assertTrue((profile / 'launch-closed.json').exists())
            # Retry must revalidate and fsync the existing private marker.
            with mock.patch.object(worker.os, 'fsync', wraps=os.fsync) as fsync:
                report = worker.cleanup_launch(profile, service)
                self.assertEqual(fsync.call_count, 2)
            self.assertTrue(report['launch_closed'])
            self.assertTrue(report['owned_service_removed'])

    def test_any_closed_directory_entry_blocks_claim_and_malformed_marker_fails_cleanup(self):
        for kind in ('directory', 'symlink', 'fifo', 'wrong_hash'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root, now, args = Path(directory).resolve(), int(time.time()), launch_args()
                intent = worker.new_intent(args, root, now)
                profile = persist_intent(root, intent)
                closed = profile / 'launch-closed.json'
                if kind == 'directory':
                    closed.mkdir()
                elif kind == 'symlink':
                    closed.symlink_to(profile / 'missing')
                elif kind == 'fifo':
                    os.mkfifo(closed, mode=0o600)
                else:
                    worker.create_private_json(closed, {'schema': 1, 'intent_sha256': '0' * 64})
                service = FakeService()
                with self.assertRaisesRegex(worker.ops.AcceptanceError, 'LAUNCH_INTENT_CLOSED'):
                    worker.claim_launch(args, profile, service, '0::/system.slice/' + intent['unit'] + '.service', now)
                self.assertFalse(worker.cleanup_launch(profile, service)['owned_service_removed'])
                self.assertFalse((profile / 'launch-claimed.json').exists())

    def test_closure_during_claim_is_rechecked_before_return(self):
        with tempfile.TemporaryDirectory() as directory:
            root, now, args = Path(directory).resolve(), int(time.time()), launch_args()
            intent = worker.new_intent(args, root, now)
            profile = persist_intent(root, intent)

            class CloseDuringStatus(FakeService):
                def status(self, unit):
                    worker.close_launch(intent)
                    return owned_status(intent)

            service = CloseDuringStatus()
            service.exists = True
            with self.assertRaisesRegex(worker.ops.AcceptanceError, 'LAUNCH_INTENT_CLOSED'):
                worker.claim_launch(args, profile, service, '0::/system.slice/' + intent['unit'] + '.service', now)
            self.assertTrue((profile / 'launch-claimed.json').exists())
            self.assertFalse((profile / 'started.json').exists())


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
