#!/usr/bin/env python3
"""Bounded Oracle node profile; private prepared inputs, no source provisioning.

Run on Linux only. The external controller runs queries on a separate host and
creates this profile's stop file after delivery. No telemetry, log or report
contains environment values. Every profile owns one transient service.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import re
import signal
import subprocess
import sys
import time
import uuid

import yaml

import containment_acceptance as identity
import operational_acceptance as ops

MIB = 1 << 20


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def private_json(path):
    info = path.lstat()
    ops.require(path.is_file() and not path.is_symlink() and info.st_uid == os.getuid() and not info.st_mode & 0o077 and info.st_size <= MIB, "PRIVATE_INPUT_REQUIRED")
    return json.loads(path.read_text())


def save(path, data):
    temporary = path.with_suffix('.pending')
    with temporary.open('w') as output:
        json.dump(data, output, indent=2)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def counters(path):
    return {key: int(value) for key, value in (line.split() for line in path.read_text().splitlines())}


def host_cpu():
    fields = Path('/proc/stat').read_text().splitlines()[0].split()[1:]
    return {'total': sum(int(value) for value in fields[:8]), 'steal': int(fields[7])}


def host_available():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return int(values['MemAvailable'].split()[0]) * 1024


def render_config(template, group, state, scratch, enabled):
    for marker in ('@GROUP@', '@STATE@', '@SCRATCH@', '@METRICS@'):
        ops.require(template.count(marker) == 1, 'CONFIG_TEMPLATE_INCOMPLETE')
    rendered = template.replace('@GROUP@', str(group)).replace('@STATE@', str(state)).replace('@SCRATCH@', str(scratch)).replace('@METRICS@', 'true' if enabled else 'false')
    ops.require(re.search(r'@[A-Z_]+@', rendered) is None, 'CONFIG_TEMPLATE_INCOMPLETE')
    parsed = yaml.safe_load(rendered)
    ops.require(isinstance(parsed, dict) and parsed.get('metrics', {}).get('enabled') is enabled, 'METRICS_MODE_NOT_APPLIED')
    normalized = json.loads(json.dumps(parsed))
    normalized['metrics']['enabled'] = '<compared-mode>'
    normalized['scratch_directory'] = '<profile-scratch>'
    normalized['containment']['root'] = '<profile-group>'
    normalized['containment']['state_directory'] = '<profile-state>'
    return rendered, hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def input_identity(directory):
    names = ['catalog.yml', 'node-template.yml', 'node-environment.json', 'bin/kelvo', 'bin/kelvo-landlock']
    names.extend(str(path.relative_to(directory)) for path in sorted((directory / 'scripts').glob('*.py')))
    files = {name: digest(directory / name) for name in names}
    return {'files': files, 'sha256': hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}


def nonnegative_integer(value):
    return type(value) is int and value >= 0


class Samples:
    def __init__(self, group, scratch, binary):
        self.group, self.scratch, self.binary = group, scratch, str(binary).encode()
        self.owned = set()
        self.value = {'samples': 0, 'read_errors': [], 'disappeared': 0, 'rss_node_peak_bytes': 0,
                      'rss_workers_peak_bytes': 0, 'rss_combined_peak_bytes': 0, 'max_workers': 0,
                      'memory_current_peak_bytes': 0, 'memory_peak_bytes': 0, 'scratch_peak_bytes': 0,
                      'minimum_host_available_bytes': host_available(), 'maximum_sample_gap_seconds': 0.}
        self.before_cpu = host_cpu()
        self.last = None

    def sample(self):
        value, now = self.value, time.monotonic()
        if self.last is not None:
            value['maximum_sample_gap_seconds'] = max(value['maximum_sample_gap_seconds'], now - self.last)
        self.last = now
        value['samples'] += 1
        try:
            value['memory_current_peak_bytes'] = max(value['memory_current_peak_bytes'], int((self.group / 'memory.current').read_text()))
            value['memory_peak_bytes'] = max(value['memory_peak_bytes'], int((self.group / 'memory.peak').read_text()))
            value['memory_events'] = counters(self.group / 'memory.events')
            value['cpu_stat'] = counters(self.group / 'cpu.stat')
            value['pids_events'] = counters(self.group / 'pids.events')
            value['minimum_host_available_bytes'] = min(value['minimum_host_available_bytes'], host_available())
            pids = set()
            for current, _, _ in os.walk(self.group):
                try:
                    pids.update(int(pid) for pid in (Path(current) / 'cgroup.procs').read_text().split())
                except FileNotFoundError:
                    value['disappeared'] += 1
            node_rss = worker_rss = workers = 0
            for pid in pids:
                try:
                    process = Path('/proc') / str(pid)
                    fields = (process / 'stat').read_text().rsplit(') ', 1)[1].split()
                    if fields[0] in ('Z', 'X', 'x'):
                        continue
                    arguments = (process / 'cmdline').read_bytes().split(b'\0')
                    if len(arguments) < 2 or arguments[0] != self.binary or arguments[1] not in (b'node', b'worker'):
                        continue
                    self.owned.add((pid, fields[19]))
                    ops.require(len(self.owned) <= 10000, 'PROCESS_INVENTORY_BOUND')
                    rss = int(fields[21]) * os.sysconf('SC_PAGE_SIZE')
                    if arguments[1] == b'node':
                        node_rss += rss
                    else:
                        worker_rss += rss
                        workers += 1
                except FileNotFoundError:
                    value['disappeared'] += 1
            value['rss_node_peak_bytes'] = max(value['rss_node_peak_bytes'], node_rss)
            value['rss_workers_peak_bytes'] = max(value['rss_workers_peak_bytes'], worker_rss)
            value['rss_combined_peak_bytes'] = max(value['rss_combined_peak_bytes'], node_rss + worker_rss)
            value['max_workers'] = max(value['max_workers'], workers)
            size = 0
            for current, dirs, files in os.walk(self.scratch, followlinks=False):
                dirs[:] = [name for name in dirs if not (Path(current) / name).is_symlink()]
                for name in files:
                    try:
                        size += (Path(current) / name).lstat().st_size
                    except FileNotFoundError:
                        pass
            value['scratch_peak_bytes'] = max(value['scratch_peak_bytes'], size)
            ops.require(size <= 512 * MIB, 'SCRATCH_BOUND_EXCEEDED')
        except Exception as error:
            value['read_errors'].append(str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__)

    def final(self):
        self.sample()
        after = host_cpu()
        total, steal = after['total'] - self.before_cpu['total'], after['steal'] - self.before_cpu['steal']
        self.value['host_steal_fraction'] = steal / total if total else None
        self.value['observed_worker_identities'] = len(self.owned)
        self.value['live_owned_processes_after_shutdown'] = sum(ops.proc_identity(pid) == (pid, started) for pid, started in self.owned)
        self.value['sampling_scope'] = 'RSS: observed node and worker processes across delegated descendants. Cgroup: node, disposable children and Python observer; excludes source, gateway, NATS and SSH. Shared RSS pages can be counted twice; sampling can miss short peaks.'
        return self.value


def valid_profile(report):
    try:
        resources = report['resources']
        if not all(nonnegative_integer(resources[key]) for key in ('samples', 'rss_node_peak_bytes', 'rss_workers_peak_bytes', 'rss_combined_peak_bytes', 'max_workers', 'scratch_peak_bytes', 'live_owned_processes_after_shutdown')):
            return False
        for category, required in (('memory_events', ('oom', 'oom_kill', 'max')), ('pids_events', ('max',)), ('cpu_stat', ('usage_usec', 'user_usec', 'system_usec'))):
            if not all(nonnegative_integer(resources[category][key]) for key in required):
                return False
        gap, elapsed = resources['maximum_sample_gap_seconds'], report['elapsed_seconds']
        if not (type(gap) in (int, float) and math.isfinite(gap) and 0 <= gap <= 2 and type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 5):
            return False
        return (report['interrupted'] is False and report.get('failure') is None and type(report['node_exit_code']) is int and report['node_exit_code'] == 0
                and report['forced_node_kill'] is False and report['stop_file_observed'] is True
                and report['config_unchanged'] is True and report['binary_unchanged'] is True
                and report['inputs_unchanged'] is True
                and report['owned_service_removed'] is True and report['owned_cgroup_removed'] is True
                and type(report['service_exit_code']) is int and report['service_exit_code'] == 0
                and type(report['scratch_directories_remaining']) is int and report['scratch_directories_remaining'] == 0
                and type(report['containment_records_remaining']) is int and report['containment_records_remaining'] == 0 and resources['samples'] >= 20
                and resources['rss_node_peak_bytes'] > 0 and resources['rss_workers_peak_bytes'] > 0
                and resources['max_workers'] == 1 and not resources['read_errors']
                and resources['live_owned_processes_after_shutdown'] == 0
                and resources['memory_events']['oom'] == resources['memory_events']['oom_kill'] == 0
                and resources['pids_events']['max'] == 0 and resources['scratch_peak_bytes'] <= 512 * MIB)
    except (KeyError, TypeError):
        return False


def inside(args, directory, profile):
    group = Path('/sys/fs/cgroup/system.slice') / (args.unit + '.service')
    report = {'interrupted': False, 'metrics_enabled': args.metrics == 'enabled', 'forced_node_kill': False}
    process = sampler = None
    try:
        ops.require(Path('/proc/self/cgroup').read_text().strip() == '0::/system.slice/' + args.unit + '.service', 'OWNED_SERVICE_REQUIRED')
        ops.require(identity.zero_capabilities(Path('/proc/self/status').read_text()), 'ZERO_CAPABILITIES_REQUIRED')
        for name, expected in (('memory.max', 640 * MIB), ('memory.swap.max', 0), ('pids.max', 128), ('cpu.weight', 20)):
            ops.require(int((group / name).read_text()) == expected, 'SERVICE_RESOURCE_LIMIT_MISMATCH')
        supervisor = group / 'supervisor'
        supervisor.mkdir()
        (supervisor / 'cgroup.procs').write_text(str(os.getpid()))
        (group / 'cgroup.subtree_control').write_text('+cpu +memory +pids')
        (group / 'jobs').mkdir()
        scratch, state = profile / 'scratch', profile / 'containment'
        scratch.mkdir(mode=0o700)
        state.mkdir(mode=0o700)
        config = profile / 'node.yml'
        before_inputs = input_identity(directory)
        template = (directory / 'node-template.yml').read_text()
        rendered, normalized_hash = render_config(template, group / 'jobs', state, scratch, args.metrics == 'enabled')
        report['normalized_config_sha256'] = normalized_hash
        report['inputs'] = before_inputs
        ops.cf.write(config, rendered)
        config_hash = digest(config)
        env = private_json(directory / 'node-environment.json')
        ops.require(all(key == 'KELVO_NATS_A1' or re.fullmatch(r'KELVO_SOURCE_[A-Z0-9_]{1,100}', key) for key in env), 'UNEXPECTED_ENVIRONMENT_REFERENCE')
        ops.require(all(isinstance(value, str) and '\0' not in value and len(value) <= 65536 for value in env.values()), 'INVALID_ENVIRONMENT_VALUE')
        env.update(PATH='/usr/local/bin:/usr/bin:/bin', HOME=str(profile), TMPDIR=str(scratch), GOMAXPROCS='1', LANG='C.UTF-8')
        binary = directory / 'bin/kelvo'
        binary_hash = digest(binary)
        sampler = Samples(group, scratch, binary)
        with (profile / 'node.log').open('w') as log:
            process = subprocess.Popen([str(binary), 'node', '--config', str(config), '--drain-timeout', '8s'], env=env, cwd=profile, stdout=log, stderr=subprocess.STDOUT)
            started = time.monotonic()
            save(profile / 'started.json', {'pid': process.pid, 'unit': args.unit, 'metrics_enabled': args.metrics == 'enabled', 'config_sha256': config_hash, 'binary_sha256': binary_hash})
            while process.poll() is None and not (profile / 'stop').exists() and time.monotonic() - started < args.runtime:
                sampler.sample()
                if sampler.value['read_errors']:
                    raise ops.AcceptanceError('RESOURCE_OBSERVATION_FAILED')
                time.sleep(.1)
            report['stop_file_observed'] = (profile / 'stop').is_file()
            report['elapsed_seconds'] = time.monotonic() - started
            report['config_unchanged'] = digest(config) == config_hash
            report['binary_unchanged'] = digest(binary) == binary_hash
            report['inputs_unchanged'] = input_identity(directory) == before_inputs
            report['config_sha256'], report['binary_sha256'] = config_hash, binary_hash
    except BaseException as error:
        report['interrupted'] = isinstance(error, (KeyboardInterrupt, InterruptedError))
        report['failure'] = str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__
    finally:
        if process is not None:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                deadline = time.monotonic() + 12
                while process.poll() is None and time.monotonic() < deadline:
                    if sampler is not None:
                        sampler.sample()
                    time.sleep(.1)
                if process.poll() is None:
                    report['forced_node_kill'] = True
                    process.kill()
                    process.wait(timeout=3)
            report['node_exit_code'] = process.returncode
        if sampler is not None:
            report['resources'] = sampler.final()
            report['scratch_directories_remaining'] = len(list((profile / 'scratch').glob('kelvo-worker-*')))
            report['containment_records_remaining'] = sum(path.name != '.kelvo-containment.lock' for path in (profile / 'containment').iterdir())
        save(profile / 'inside.json', report)
    return 0 if not report.get('failure') and report.get('node_exit_code') == 0 else 1


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--metrics', choices=('enabled', 'disabled'), required=True)
    parser.add_argument('--runtime', type=int, default=600)
    parser.add_argument('--inside', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--unit', help=argparse.SUPPRESS)
    args = parser.parse_args()
    ops.require(sys.platform == 'linux' and os.getuid() != 0 and re.fullmatch(r'[a-z0-9-]{1,48}', args.profile) and 60 <= args.runtime <= 1200, 'BOUNDED_NONROOT_LINUX_PROFILE_REQUIRED')
    directory = args.directory.resolve(strict=True)
    profile = directory / 'profiles' / args.profile
    if args.inside:
        return inside(args, directory, profile)
    profile.mkdir(mode=0o700, parents=True)
    report = {'schema': 1, 'metrics_enabled': args.metrics == 'enabled', 'resource_budget': {'memory_bytes': 640 * MIB, 'swap_bytes': 0, 'tasks': 128, 'cpu_weight': 20}}
    unit = 'kelvo-micro-node-' + uuid.uuid4().hex[:12]
    command = ['sudo', '-n', 'systemd-run', '--unit=' + unit, '--uid=' + pwd.getpwuid(os.getuid()).pw_name,
               '--property=Delegate=yes', '--property=MemoryMax=640M', '--property=MemorySwapMax=0', '--property=TasksMax=128',
               '--property=CPUWeight=20', '--property=NoNewPrivileges=yes', '--property=CapabilityBoundingSet=', '--property=AmbientCapabilities=',
               '--collect', '--wait', '--pipe', sys.executable, str(Path(__file__).resolve()), '--inside', '--directory', str(directory),
               '--profile', args.profile, '--metrics', args.metrics, '--runtime', str(args.runtime), '--unit', unit]
    try:
        with (profile / 'service.log').open('w') as log:
            report['service_exit_code'] = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=args.runtime + 90).returncode
        if (profile / 'inside.json').is_file():
            report.update(json.loads((profile / 'inside.json').read_text()))
    except BaseException as error:
        report['failure'] = type(error).__name__
        report['interrupted'] = isinstance(error, (KeyboardInterrupt, InterruptedError))
    finally:
        report.update(identity.cleanup_owned(unit))
        report['passed'] = valid_profile(report)
        save(profile / 'report.json', report)
    print(json.dumps({key: report.get(key) for key in ('passed', 'metrics_enabled', 'failure', 'node_exit_code', 'service_exit_code')}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
