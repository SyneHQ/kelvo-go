#!/usr/bin/env python3
"""Synthetic runner checks: no subprocesses, containers, trust changes or builds."""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import relational_acceptance as gate


PACKAGE = 'github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative'
PRIVATE = 'private-password private-user private-server SELECT secret_column'


def pass_events():
    events = []
    for root in ('TestPostgresLiveNativeTLS', 'TestMySQLLiveNativeTLS'):
        for suffix in ('', '/typed_access_and_refresh',
                       '/typed_access_and_refresh/authentication',
                       '/typed_access_and_refresh/permission'):
            events.append({'Package': PACKAGE, 'Test': root + suffix, 'Action': 'pass'})
    events.append({'Package': PACKAGE, 'Action': 'pass'})
    return '\n'.join(json.dumps(item) for item in events) + '\n'


class RelationalAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / 'evidence.json'
        self.commands = []
        patcher = mock.patch.object(gate, 'PRIVATE_ROOT', Path(self.temp.name) / 'artifacts' / 'relational-private')
        patcher.start()
        self.addCleanup(patcher.stop)

    def fake_run(self, argv, **kwargs):
        self.commands.append(list(argv))
        if argv[0] == 'openssl':
            # These are inert placeholders for subsequent permission operations.
            # No key, certificate, or external command is generated or executed.
            if '-keyout' in argv:
                Path(argv[argv.index('-keyout') + 1]).write_text('placeholder')
        data = b''
        if argv[:3] == ['sudo', '-n', 'docker']:
            command = argv[3:]
            if command[:2] == ['network', 'inspect']:
                data = json.dumps([{'IPAM': {'Config': [{'Subnet': '172.30.0.0/24'}]}}]).encode()
            elif command[:1] == ['inspect']:
                data = json.dumps([{'Image': 'sha256:synthetic-fixture'}]).encode()
        if argv[0] == 'synthetic-go':
            return subprocess.CompletedProcess(argv, 0, pass_events(), '')
        return subprocess.CompletedProcess(argv, 0, data, b'')

    def invoke(self, runner=None):
        stdout = io.StringIO()
        with mock.patch.object(sys, 'argv', ['relational_acceptance.py', '--go', 'synthetic-go',
                                             '--output', str(self.output)]), \
                mock.patch.object(gate.subprocess, 'run', side_effect=runner or self.fake_run), \
                contextlib.redirect_stdout(stdout):
            result = gate.main()
        return result, json.loads(stdout.getvalue())

    def assert_private_absent(self, report):
        encoded = json.dumps(report)
        for value in ('private-password', 'private-user', 'private-server', 'secret_column'):
            self.assertNotIn(value, encoded)

    def cleanup_commands(self):
        return [command[3:] for command in self.commands
                if command[:4] == ['sudo', '-n', 'docker', 'rm']
                or command[:5] == ['sudo', '-n', 'docker', 'network', 'rm']]

    def test_existing_output_is_refused_before_any_provisioning(self):
        self.output.write_text('{"passed": true, "old": true}\n')
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            self.invoke()
        self.assertEqual(error.exception.code, 2)
        self.assertEqual(self.commands, [])
        self.assertEqual(json.loads(self.output.read_text()), {'passed': True, 'old': True})

    def test_dangling_output_symlink_is_refused_before_provisioning(self):
        self.output.symlink_to(Path(self.temp.name) / 'absent')
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            self.invoke()
        self.assertEqual(error.exception.code, 2)
        self.assertEqual(self.commands, [])
        self.assertTrue(self.output.is_symlink())

    def test_success_evidence_is_emitted_only_after_all_owned_cleanup(self):
        def run(argv, **kwargs):
            self.assertFalse(self.output.exists())
            return self.fake_run(argv, **kwargs)
        result, report = self.invoke(run)
        self.assertEqual(result, 0)
        self.assertTrue(report['passed'])
        self.assertTrue(report['live_tests_passed'])
        self.assertTrue(report['cleanup_passed'])
        self.assertEqual(len(self.cleanup_commands()), 3)
        self.assertEqual(json.loads(self.output.read_text()), report)

    def test_failed_test_run_writes_sanitized_failure_after_cleanup(self):
        def run(argv, **kwargs):
            result = self.fake_run(argv, **kwargs)
            if argv[0] == 'synthetic-go':
                return subprocess.CompletedProcess(argv, 1, PRIVATE, PRIVATE)
            return result
        result, report = self.invoke(run)
        self.assertEqual(result, 1)
        self.assertFalse(report['passed'])
        self.assertFalse(report['live_tests_passed'])
        self.assertTrue(report['cleanup_passed'])
        self.assertEqual(len(self.cleanup_commands()), 3)
        self.assert_private_absent(report)
        self.assertEqual(json.loads(self.output.read_text()), report)

    def test_setup_exception_is_sanitized_and_attempted_network_is_cleaned(self):
        def run(argv, **kwargs):
            result = self.fake_run(argv, **kwargs)
            if argv[3:5] == ['network', 'create']:
                raise subprocess.TimeoutExpired(argv, 30, output=PRIVATE)
            return result
        result, report = self.invoke(run)
        self.assertEqual(result, 1)
        self.assertFalse(report['passed'])
        self.assertTrue(report['cleanup_passed'])
        self.assertEqual(len(self.cleanup_commands()), 1)
        self.assertEqual(self.cleanup_commands()[0][:2], ['network', 'rm'])
        self.assert_private_absent(report)
        self.assertEqual(json.loads(self.output.read_text()), report)

    def test_cleanup_failure_cannot_pass_and_remaining_cleanup_is_attempted(self):
        for failure in ('nonzero', 'exception'):
            with self.subTest(failure=failure):
                self.output = Path(self.temp.name) / (failure + '.json')
                self.commands = []
                def run(argv, **kwargs):
                    result = self.fake_run(argv, **kwargs)
                    if argv[3:4] == ['rm'] and argv[-1].endswith('-mysql'):
                        self.assertFalse(self.output.exists())
                        if failure == 'exception':
                            raise RuntimeError(PRIVATE)
                        return subprocess.CompletedProcess(argv, 1, b'', PRIVATE.encode())
                    return result
                result, report = self.invoke(run)
                self.assertEqual(result, 1)
                self.assertFalse(report['passed'])
                self.assertTrue(report['live_tests_passed'])
                self.assertFalse(report['cleanup_passed'])
                self.assertEqual(report['cleanup_failures'], ['container removal failed'])
                cleanup = self.cleanup_commands()
                self.assertEqual(len(cleanup), 3)
                self.assertTrue(cleanup[0][-1].endswith('-mysql'))
                self.assertTrue(cleanup[1][-1].endswith('-pg'))
                self.assertEqual(cleanup[2][:2], ['network', 'rm'])
                self.assert_private_absent(report)
                self.assertEqual(json.loads(self.output.read_text()), report)

    def test_private_diagnostics_redact_fixture_secrets_and_retain_test_details(self):
        fixture_secrets = []
        def run(argv, **kwargs):
            result = self.fake_run(argv, **kwargs)
            if argv[0] == 'synthetic-go':
                environment = kwargs['env']
                dsns = [value for key, value in environment.items()
                        if key.startswith('KELVO_TEST_') and key.endswith('_DSN')]
                fixture_secrets.extend(dsns)
                # Extract both generated passwords from MySQL DSNs to prove that
                # separately printed passwords are redacted as well as full DSNs.
                passwords = [dsn.split(':', 1)[1].split('@tcp(', 1)[0]
                             for dsn in dsns if '@tcp(' in dsn]
                fixture_secrets.extend(passwords)
                output = 'assertion failed: useful private diagnostic\n' + '\n'.join(dsns + passwords)
                return subprocess.CompletedProcess(argv, 1, output, PRIVATE)
            return result
        result, report = self.invoke(run)
        self.assertEqual(result, 1)
        self.assertEqual(report['failure_stage'], 'source_tests')
        self.assertTrue(report['diagnostics_retained'])
        self.assertEqual(len(report['missing_tests']), 8)
        self.assert_private_absent(report)
        directories = list(gate.PRIVATE_ROOT.iterdir())
        self.assertEqual(len(directories), 1)
        self.assertEqual(directories[0].stat().st_mode & 0o777, 0o700)
        logs = list(directories[0].glob('*.log'))
        self.assertTrue(logs)
        diagnostic = ''.join(path.read_text() for path in logs)
        self.assertIn('useful private diagnostic', diagnostic)
        self.assertIn('[REDACTED]', diagnostic)
        for value in fixture_secrets:
            self.assertNotIn(value, diagnostic)
        for path in logs:
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_private_diagnostics_have_a_total_byte_limit(self):
        with mock.patch.object(gate, 'DIAGNOSTIC_LIMIT', 127):
            diagnostic = gate.PrivateDiagnostics('bounded-test', ['password'])
            diagnostic.record('password ' + 'a' * 200)
            diagnostic.record('second output that must not be retained')
        logs = list(diagnostic.directory.glob('*.log'))
        self.assertEqual(sum(path.stat().st_size for path in logs), 127)
        self.assertEqual(len(logs), 1)
        self.assertNotIn('password', logs[0].read_text())

    def test_unknown_child_name_cannot_leak_into_public_failed_test_names(self):
        def run(argv, **kwargs):
            result = self.fake_run(argv, **kwargs)
            if argv[0] == 'synthetic-go':
                event = {'Package': PACKAGE, 'Action': 'fail',
                         'Test': 'TestMySQLLiveNativeTLS/' + PRIVATE}
                return subprocess.CompletedProcess(argv, 1, pass_events() + json.dumps(event), '')
            return result
        result, report = self.invoke(run)
        self.assertEqual(result, 1)
        self.assertEqual(report['failed_tests'], ['TestMySQLLiveNativeTLS'])
        self.assert_private_absent(report)

    def test_package_only_pass_cannot_claim_required_live_tests(self):
        def run(argv, **kwargs):
            result = self.fake_run(argv, **kwargs)
            if argv[0] == 'synthetic-go':
                return subprocess.CompletedProcess(argv, 0,
                    json.dumps({'Package': PACKAGE, 'Action': 'pass'}) + '\n', '')
            return result
        result, report = self.invoke(run)
        self.assertEqual(result, 1)
        self.assertFalse(report['passed'])
        self.assertFalse(report['live_tests_passed'])
        self.assertTrue(report['cleanup_passed'])


if __name__ == '__main__':
    unittest.main()
