#!/usr/bin/env python3
"""Run an explicit telemetry smoke or matched experiment in a bounded Linux service.

No SSH, package installation or compilation. Read PLAN.md before scheduling.
"""
import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

sys.dont_write_bytecode = True
import workloads as workload_definition
from workloads import ROWS, MAX_BODY, SQL, generate, require, verify

PAIRS = 6
VARIANTS = {'metrics_off': (False, None), 'metrics_on': (True, None),
            'trace_zero': (True, 0), 'trace_tenth': (True, .1), 'trace_all': (True, 1)}
CONTRASTS = [('metrics_off', 'metrics_on'), ('metrics_on', 'trace_zero'),
             ('metrics_on', 'trace_tenth'), ('metrics_on', 'trace_all')]
MAX_SECONDS = 2400
SMOKE_SECONDS = 600
LOG_BYTES = 512 << 10
SMOKE_RECEIPT_BYTES = 4 << 20


def digest(path):
    with Path(path).open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def save(path, value):
    temporary = path.with_suffix('.pending')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    os.replace(temporary, path)


def helper_hashes():
    return {'harness.py': digest(Path(__file__)),
            'workloads.py': digest(Path(workload_definition.__file__))}


def workload_hash():
    definition = {'rows': ROWS, 'max_body_bytes': MAX_BODY, 'sql': SQL,
                  'implementation_sha256': digest(Path(workload_definition.__file__))}
    return hashlib.sha256(json.dumps(definition, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def read_smoke_receipt(path, expected_hash):
    require(path.is_file() and not path.is_symlink() and 0 < path.stat().st_size <= SMOKE_RECEIPT_BYTES,
            'SMOKE_RECEIPT_BOUND')
    raw = path.read_bytes()
    require(0 < len(raw) <= SMOKE_RECEIPT_BYTES and hashlib.sha256(raw).hexdigest() == expected_hash,
            'SMOKE_RECEIPT_HASH')
    return json.loads(raw)


def counters(path):
    result = {}
    for line in path.read_text().splitlines():
        key, value = line.split()
        require(value.isdigit(), 'INVALID_COUNTER')
        result[key] = int(value)
    return result


def parent_cpu(proc):
    raw = Path(f'/proc/{proc.pid}/stat').read_text()
    fields = raw.rsplit(') ', 1)[1].split()
    require(proc.poll() is None and len(fields) >= 22, 'PARENT_EXITED')
    # /proc fields 14/15, excluding waited-for child CPU fields 16/17.
    return (fields[19], (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'))


def removed(unit):
    require(re.fullmatch(r'kelvo-[a-z0-9-]+(?:\.service)?', unit), 'INVALID_PRIOR_UNIT')
    unit = unit.removesuffix('.service')
    raw = subprocess.check_output(['systemctl', 'show', unit + '.service', '-p', 'LoadState',
        '-p', 'ActiveState', '-p', 'MainPID'], text=True, timeout=10)
    fields = dict(line.split('=', 1) for line in raw.splitlines())
    return (fields == {'LoadState': 'not-found', 'ActiveState': 'inactive', 'MainPID': '0'}
            and not (Path('/sys/fs/cgroup/system.slice') / (unit + '.service')).exists())


def validate_running_units(output, owned_unit):
    # Descriptions may contain interpreter paths or other unit names.
    units = {line.split(maxsplit=1)[0] for line in output.splitlines() if line.strip()}
    require(not any(re.fullmatch(r'kelvo-(?:sustained|analytics|.*capacity|.*benchmark)[a-z0-9.-]*\.service', unit)
                    for unit in units), 'OTHER_PERFORMANCE_CAMPAIGN_RUNNING')
    require({unit for unit in units if re.fullmatch(r'kelvo-telemetry-overhead-[0-9a-f]{12}\.service', unit)}
            == {owned_unit + '.service'}, 'OTHER_TELEMETRY_EXPERIMENT_RUNNING')


def guard(args):
    require(sys.platform == 'linux' and os.geteuid() != 0, 'DEDICATED_NONROOT_LINUX_REQUIRED')
    require(re.fullmatch(r'kelvo-telemetry-overhead-[0-9a-f]{12}', args.unit), 'OWNED_UNIT_REQUIRED')
    relative = '/system.slice/' + args.unit + '.service'
    require(Path('/proc/self/cgroup').read_text().strip() == '0::' + relative, 'EXACT_SERVICE_REQUIRED')
    group = Path('/sys/fs/cgroup') / relative.lstrip('/')
    require(int((group / 'memory.max').read_text()) == 6 << 30
            and int((group / 'memory.swap.max').read_text()) == 0
            and int((group / 'pids.max').read_text()) == 512, 'SERVICE_MEMORY_TASK_LIMITS')
    quota, period = map(int, (group / 'cpu.max').read_text().split())
    require(quota == 2 * period, 'SERVICE_CPU_LIMIT')
    require(socket.if_nameindex() == [(1, 'lo')] and os.readlink('/proc/self/ns/net') != args.host_network_namespace,
            'PRIVATE_NETWORK_REQUIRED')
    properties = subprocess.check_output(['systemctl', 'show', args.unit + '.service', '-p', 'RuntimeMaxUSec',
        '-p', 'TimeoutStopUSec', '-p', 'KillMode', '-p', 'SendSIGKILL'], text=True, timeout=10)
    require(dict(line.split('=', 1) for line in properties.splitlines()) == {
        'RuntimeMaxUSec': '15min' if args.mode == 'smoke' else '1h',
        'TimeoutStopUSec': '20s', 'KillMode': 'control-group', 'SendSIGKILL': 'yes'}, 'SERVICE_WATCHDOG')
    prior = json.loads(args.prior_receipt.read_text())
    require(prior.get('owned_service_removed') is True and prior.get('owned_cgroup_removed') is True
            and type(prior.get('unit')) is str and prior['unit'].removesuffix('.service') == args.prior_unit.removesuffix('.service')
            and type(prior.get('service_exit_code')) is int and removed(args.prior_unit), 'PRIOR_CAMPAIGN_NOT_CLEARED')
    units = subprocess.check_output(['systemctl', 'list-units', '--type=service', '--state=running',
        '--no-legend', '--plain', '--full'], text=True, timeout=10)
    validate_running_units(units, args.unit)
    for path, expected in ((args.binary, args.binary_sha256), (args.sandbox, args.sandbox_sha256),
                           (args.collector, args.collector_sha256)):
        require(path.is_file() and not path.is_symlink() and path.stat().st_mode & 0o022 == 0
                and os.access(path, os.X_OK) and digest(path) == expected, 'FROZEN_BINARY_REQUIRED')
    require(digest(Path('/etc/ssl/certs/ca-certificates.crt')) == args.trust_bundle_sha256, 'COLLECTOR_SYSTEM_TRUST_REQUIRED')
    require(not args.output.exists() and not args.output.is_symlink(), 'FRESH_OUTPUT_REQUIRED')
    if args.mode == 'full':
        smoke = read_smoke_receipt(args.smoke_receipt, args.smoke_receipt_sha256)
        require(type(smoke) is dict and type(smoke.get('unit')) is str
                and re.fullmatch(r'kelvo-telemetry-overhead-[0-9a-f]{12}', smoke['unit'])
                and smoke['unit'] != args.unit and removed(smoke['unit']), 'SMOKE_SERVICE_NOT_CLEARED')
    return group


class Process:
    def __init__(self, command, env, directory, name):
        self.proc = subprocess.Popen([str(x) for x in command], env=env, cwd=directory,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        self.overflow = False
        self.failure = False
        self.path = directory / (name + '.log')
        def drain():
            try:
                kept = 0
                with self.path.open('xb') as target:
                    while block := self.proc.stdout.read(65536):
                        allowed = block[:max(0, LOG_BYTES - kept)]
                        target.write(allowed)
                        kept += len(allowed)
                        self.overflow |= len(allowed) != len(block)
            except Exception:
                self.failure = True
        self.thread = threading.Thread(target=drain, daemon=True)
        self.thread.start()

    def stop(self):
        forced = False
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            forced = True
            self.proc.kill()
            code = self.proc.wait(timeout=3)
        self.thread.join(timeout=3)
        require(not forced and code == 0 and not self.thread.is_alive() and not self.failure
                and not self.overflow, 'PROCESS_CLEANUP_OR_LOG_BOUND')
        require(b'Tracing shutdown did not complete' not in self.path.read_bytes(), 'TRACING_FLUSH_FAILED')


class Experiment:
    def __init__(self, args, group):
        self.args, self.group = args, group
        self.started = time.monotonic()
        self.max_seconds = SMOKE_SECONDS if args.mode == 'smoke' else MAX_SECONDS
        self.worker_restart_after = 0
        args.output.mkdir(mode=0o700)
        sys.path.insert(0, str(args.repo / 'scripts'))
        import cluster_fixture as cf
        import operational_acceptance as ops
        import sustained_acceptance as sustained
        import containment_acceptance as identity
        import yaml
        self.cf, self.ops, self.sustained, self.identity, self.yaml = cf, ops, sustained, identity, yaml
        # Same strict framing checks as existing acceptance, with this fixed
        # experiment's larger transfer allowance. This process runs no other campaign.
        self.ops.MAX_BODY = MAX_BODY
        self.source = identity.source_manifest(args.repo)
        require(self.source['base_revision'] == args.revision
                and self.source == json.loads(args.build_source.read_text()), 'MATCHED_BUILD_SOURCE_REQUIRED')
        self.fixture = args.output / 'cluster'
        cf.DIR = self.fixture
        self.data = args.output / 'million.parquet'
        self.expected = generate(self.data)
        self.data_hash = digest(self.data)
        self.report = {'schema_version': 1, 'mode': args.mode, 'unit': args.unit,
            'passed': False, 'source': self.source, 'binary_sha256': args.binary_sha256,
            'sandbox_sha256': args.sandbox_sha256, 'collector_sha256': args.collector_sha256,
            'helper_sha256': helper_hashes(), 'workload_sha256': workload_hash(),
            'prior_receipt_sha256': digest(args.prior_receipt), 'data_sha256': self.data_hash,
            'rows': ROWS, 'pairs': 1 if args.mode == 'smoke' else PAIRS,
            'operation_budget_seconds': self.max_seconds,
            'hard_watchdog_seconds': 900 if args.mode == 'smoke' else 3600,
            'epochs': [], 'kernel': os.uname().release, 'machine': os.uname().machine,
            'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'),
            'scope': ('Fixture correctness smoke only; raw durations are diagnostics, not benchmark or overhead evidence.'
                      if args.mode == 'smoke' else 'Whole-metrics and tracing experiment; no capacity or pure-compute claim.')}
        self.brokers = []
        save(args.output / 'receipt.json', self.report)

    def deadline(self):
        remaining = self.max_seconds - (time.monotonic() - self.started)
        require(remaining > 0, 'HARNESS_OPERATION_BUDGET')
        return remaining

    def call(self, path, body=None, port=14440, worker=False, timeout=35):
        timeout = min(timeout, self.deadline())
        data = None if body is None else json.dumps(body).encode()
        headers = {'Content-Type': 'application/json'}
        if not worker:
            headers['Authorization'] = 'Bearer ' + self.env['KELVO_TOKEN_A']
        request = urllib.request.Request(f'https://127.0.0.1:{port}' + path, data=data, headers=headers)
        with (self.worker_opener if worker else self.opener).open(request, timeout=timeout) as response:
            raw, headers = self.ops.read_http_response(response)
            self.deadline()
            return response.status, raw, headers

    def ready(self, proc, port, worker=False):
        until = time.monotonic() + 20
        while time.monotonic() < until:
            self.deadline()
            require(proc.proc.poll() is None, 'STARTUP_PROCESS_EXITED')
            try:
                status = self.call('/ready', port=port, worker=worker,
                                   timeout=min(2, max(.001, until - time.monotonic())))[0]
                require(time.monotonic() < until, 'STARTUP_TIMEOUT')
                if status == 200:
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(.05)
        raise RuntimeError('STARTUP_TIMEOUT')

    def setup(self):
        self.cf.provision(str(self.args.nats_archive))  # supplied pinned archive only; no download
        self.brokers = json.loads((self.fixture / 'pids.json').read_text())
        self.env = {'PATH': '/usr/bin:/bin', 'HOME': str(self.args.output), 'TMPDIR': str(self.args.output),
                    'GOMAXPROCS': '1', **json.loads((self.fixture / 'environment.json').read_text())}
        self.env['KELVO_OTLP_FIXTURE_TOKEN'] = os.urandom(32).hex()
        client = ssl.create_default_context(cafile=str(self.fixture / 'ca.pem'))
        client.minimum_version = ssl.TLSVersion.TLSv1_3
        worker = ssl.create_default_context(cafile=str(self.fixture / 'ca.pem'))
        worker.minimum_version = ssl.TLSVersion.TLSv1_3
        worker.load_cert_chain(self.fixture / 'gateway.pem', self.fixture / 'gateway.key')
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), self.ops.StrictHTTPSHandler(context=client))
        self.worker_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), self.ops.StrictHTTPSHandler(context=worker))
        self.node = self.yaml.safe_load((self.fixture / 'a1.yml').read_text())
        self.gateway = self.yaml.safe_load((self.fixture / 'gateway1.yml').read_text())
        tenant = self.gateway['tenants'][0]
        tenant['workers'] = [entry for entry in tenant['workers'] if entry['id'] == 'a1']
        tenant['policy']['workers'] = {'a1': 1}
        tenant['policy']['limits'].update(max_rows=ROWS, max_bytes=MAX_BODY, timeout='30s', memory_mb=256, max_temp_mb=256)
        tenant['policy']['job_ttl'] = '2m'
        self.node['policy'] = copy.deepcopy(tenant['policy'])
        self.gateway['tenants'] = [tenant]
        self.node['sandbox_path'] = str(self.args.sandbox)
        self.node['resources'] = {'max_concurrent': 1, 'memory_mb': 1024, 'baseline_mb': 128, 'overhead_mb': 224, 'scratch_mb': 512}
        catalog = self.args.output / 'catalog.yml'
        catalog.write_text(self.yaml.safe_dump({'sources': [{'id': 'sample', 'type': 'parquet', 'path': str(self.data)}]}))
        self.node['catalog_file'] = str(catalog)
        initialization = copy.deepcopy(self.gateway)
        initialization['tenants'][0]['nats'].update(username='aadmin', password_env='KELVO_NATS_AADMIN')
        init = self.args.output / 'init.yml'
        init.write_text(self.yaml.safe_dump(initialization))
        with (self.args.output / 'init.log').open('xb') as log:
            subprocess.run([str(self.args.binary), 'cluster-init', '--config', str(init)], env=self.env,
                           stdout=log, stderr=log, check=True, timeout=30)

    def query(self, workload, processes):
        self.deadline()
        sampler = self.sustained.LockedSamples({k: p.proc for k, p in processes.items()})
        stop = threading.Event()
        errors = []
        peak = 0
        def sample():
            nonlocal peak
            try:
                while not stop.is_set():
                    sampler.sample()
                    peak = max(peak, int((self.group / 'memory.current').read_text()))
                    stop.wait(.05)
            except Exception:
                errors.append('RESOURCE_SAMPLE_FAILED')
        before_cpu = {k: parent_cpu(p.proc) for k, p in processes.items()}
        before_service = counters(self.group / 'cpu.stat')
        before_events = counters(self.group / 'memory.events')
        thread = threading.Thread(target=sample, daemon=True)
        thread.start()
        started = time.monotonic()
        try:
            code, raw, _ = self.call('/v1/queries', {'mode': 'federated', 'sources': ['sample'], 'sql': SQL[workload]})
            require(code == 201, 'QUERY_SUBMISSION')
            identifier = json.loads(raw)['id']
            code, body, headers = self.call('/v1/queries/' + identifier + '/results')
            elapsed = time.monotonic() - started
            after_cpu = {k: parent_cpu(p.proc) for k, p in processes.items()}
            after_service = counters(self.group / 'cpu.stat')
            require(code == 200 and headers.get('kelvo-result-completion') == 'durable-eos-v1', 'DURABLE_RESULT')
        finally:
            stop.set()
            thread.join(timeout=3)
        require(not thread.is_alive() and not errors, 'SAMPLER_STOP_OR_READ')
        evidence = sampler.evidence()
        self.epoch_owned.update(sampler.owned)
        require(evidence['samples'] > 0 and evidence['process_tree_rss_available']
                and not evidence['read_errors'], 'PROCESS_SAMPLE_UNAVAILABLE')
        after_events = counters(self.group / 'memory.events')
        require(all(after_events.get(k, 0) == before_events.get(k, 0) for k in ('oom', 'oom_kill', 'max')), 'MEMORY_LIMIT_EVENT')
        require(all(after_cpu[k][0] == before_cpu[k][0] for k in before_cpu), 'PARENT_IDENTITY_CHANGED')
        validation_started = time.monotonic()
        answer = verify(body, workload, self.expected)
        self.deadline()
        return {'workload': workload, 'receive_seconds': elapsed,
            'validation_seconds_excluded': time.monotonic() - validation_started,
            'parent_cpu_seconds': {k: after_cpu[k][1] - before_cpu[k][1] for k in before_cpu},
            'service_cpu_seconds': (after_service['usage_usec'] - before_service['usage_usec']) / 1e6,
            'service_throttled_usec': after_service.get('throttled_usec', 0) - before_service.get('throttled_usec', 0),
            'sampled_service_charged_peak_bytes': peak, 'process_sampling': evidence, 'answer': answer}

    def collector_evidence(self, directory, ratio):
        summary = json.loads((directory / 'collector-summary.json').read_text())
        raw = (directory / 'collector-events.jsonl').read_bytes()
        require(len(raw) <= 8 << 20, 'COLLECTOR_LEDGER_BOUND')
        spans = [json.loads(line) for line in raw.splitlines()]
        require(len(spans) <= 2048 and len(spans) == summary['accepted_spans'], 'COLLECTOR_ACCEPTED_COUNT')
        violations = ('privacy_violations', 'plan_source_privacy_violations', 'cap_violations',
                      'protocol_violations', 'auth_violations', 'write_violations', 'connections_rejected')
        require(all(type(summary.get(k)) is int and summary[k] == 0 for k in violations), 'COLLECTOR_VIOLATION_OR_MISSING_COUNTER')
        require(all(type(summary.get(k)) is int and 0 <= summary[k] <= bound for k, bound in
            (('requests', 128), ('bytes', 8 << 20), ('spans', 2048), ('accepted_spans', 2048))), 'COLLECTOR_COUNTER_BOUND')
        roles = summary.get('roles')
        require(type(roles) is list and len(roles) == 3, 'COLLECTOR_ROLES')
        role_keys = {'requests', 'spans', 'accepted', 'accepted_spans', 'stalled', 'unavailable', 'active_stalls'}
        require(all(type(role) is dict and set(role) == role_keys
            and all(type(value) is int and value >= 0 for value in role.values())
            and role['stalled'] == role['unavailable'] == role['active_stalls'] == 0
            and role['accepted'] == role['requests'] and role['accepted_spans'] == role['spans'] for role in roles), 'COLLECTOR_ROLE_COUNTERS')
        require(all(sum(role[k] for role in roles) == summary[k] for k in ('requests', 'spans', 'accepted_spans')),
                'COLLECTOR_TOTAL_COUNTERS')
        ids = {(s['trace_id'], s['span_id']) for s in spans}
        duplicates = len(spans) - len(ids)
        missing_parents = sum(bool(s['parent_id']) and set(s['parent_id']) != {'0'}
                              and (s['trace_id'], s['parent_id']) not in ids for s in spans)
        require(duplicates == 0, 'DUPLICATE_SPANS')
        if ratio in (None, 0):
            require(not spans and summary['requests'] == 0, 'DISABLED_TRACE_TRAFFIC')
        submission_traces = {s['trace_id'] for s in spans if s['name'] == 'kelvo.cluster.submit'}
        roots = sum(s['name'] == 'kelvo.cluster.submit' for s in spans)
        queries = sum(s['name'] == 'kelvo.query' and s['trace_id'] in submission_traces for s in spans)
        startup_queries = [s for s in spans if s['name'] == 'kelvo.query' and not s['parent_id']]
        if ratio == 1:
            # NewNode executes one independent readiness probe before serving.
            require(roots == 4 and queries == 4 and len(startup_queries) == 1
                    and missing_parents == 0, 'FULL_SAMPLE_TOPOLOGY_GAP')
            known_traces = submission_traces | {s['trace_id'] for s in startup_queries}
            require(all(s['trace_id'] in known_traces for s in spans), 'UNRELATED_FULL_SAMPLE_TRACE')
            for root in [s for s in spans if s['name'] == 'kelvo.cluster.submit']:
                validate_tree([s for s in spans if s['trace_id'] == root['trace_id']], root, False)
            validate_tree([s for s in spans if s['trace_id'] == startup_queries[0]['trace_id']], startup_queries[0], True)
        return {'requests': summary['requests'], 'received_bytes': summary['bytes'], 'accepted_spans': len(spans),
            'observed_submission_spans': roots, 'observed_query_spans': queries,
            'observed_independent_startup_query_spans': len(startup_queries),
            'duplicate_span_ids': duplicates, 'observed_missing_parent_links': missing_parents,
            'sdk_offered_spans': None, 'sdk_lost_spans': None,
            'loss_scope': 'Offered denominator unavailable; fractional sampling and absent parents are not proof of exporter loss.',
            'summary_sha256': digest(directory / 'collector-summary.json'), 'ledger_sha256': digest(directory / 'collector-events.jsonl')}

    def epoch(self, variant, workload_order, index):
        self.deadline()
        # NewNode probes before claiming a1's durable five-second lease.
        # Wait explicitly after the prior process exits; no startup retries.
        cooldown_started = time.monotonic()
        time.sleep(max(0, self.worker_restart_after - cooldown_started))
        cooldown_seconds = time.monotonic() - cooldown_started
        self.deadline()
        directory = self.args.output / f'epoch-{index:03}'
        directory.mkdir(mode=0o700)
        metrics, ratio = VARIANTS[variant]
        configurations = {'worker': copy.deepcopy(self.node), 'gateway1': copy.deepcopy(self.gateway)}
        configurations['worker']['metrics'] = {'enabled': metrics}
        for role, config in configurations.items():
            config.pop('tracing', None)
            if ratio is not None:
                config['tracing'] = {'endpoint': f'https://127.0.0.1:14318/v1/traces/{role}',
                    'token_env': 'KELVO_OTLP_FIXTURE_TOKEN', 'sample_ratio': ratio,
                    'queue_size': 64, 'export_timeout': '2s'}
            (directory / (role + '.yml')).write_text(self.yaml.safe_dump(config))
        (directory / 'mode').write_text('accept')
        (directory / 'deny.json').write_text(json.dumps([self.env['KELVO_OTLP_FIXTURE_TOKEN'], SQL['aggregate'], SQL['transfer'], str(self.data)]))
        all_processes, applications = {}, {}
        self.epoch_owned = {}
        baseline_cpu = counters(self.group / 'cpu.stat')['usage_usec']
        started = time.monotonic()
        result = {'variant': variant, 'metrics': metrics, 'tracing_ratio': ratio, 'warmups': [], 'measured': [], 'passed': False,
                  'worker_lease_cooldown_seconds_excluded': cooldown_seconds}
        try:
            all_processes['collector'] = Process([self.args.collector, '--listen', '127.0.0.1:14318',
                '--cert', self.args.collector_cert, '--key', self.args.collector_key, '--mode-file', directory / 'mode',
                '--deny-file', directory / 'deny.json', '--events', directory / 'collector-events.jsonl',
                '--summary', directory / 'collector-summary.json'], self.env, directory, 'collector')
            # Authenticate readiness over system-root TLS before starting exporters.
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                urllib.request.HTTPSHandler(context=ssl.create_default_context()))
            until = time.monotonic() + 10
            while True:
                self.deadline()
                require(time.monotonic() < until, 'COLLECTOR_STARTUP_TIMEOUT')
                try:
                    request = urllib.request.Request('https://127.0.0.1:14318/fixture/status',
                        headers={'Authorization': 'Bearer ' + self.env['KELVO_OTLP_FIXTURE_TOKEN']})
                    with opener.open(request, timeout=min(2, self.deadline(), max(.001, until - time.monotonic()))) as response:
                        require(response.status == 200, 'COLLECTOR_READINESS')
                    require(time.monotonic() < until, 'COLLECTOR_STARTUP_TIMEOUT')
                    break
                except OSError:
                    require(time.monotonic() < until, 'COLLECTOR_STARTUP_TIMEOUT')
                    time.sleep(.05)
            for role, command, port, worker in [('worker', 'node', 14443, True), ('gateway1', 'gateway', 14440, False)]:
                p = Process([self.args.binary, command, '--config', directory / (role + '.yml'), '--drain-timeout', '8s'],
                            self.env, directory, role)
                all_processes[role] = applications[role] = p
                self.ready(p, port, worker)
            result['startup_seconds'] = time.monotonic() - started
            for workload in workload_order:
                result['active_observation'] = {'phase': 'warmup', 'workload': workload}
                result['warmups'].append(self.query(workload, applications))
            for workload in workload_order:
                result['active_observation'] = {'phase': 'measured', 'workload': workload}
                result['measured'].append(self.query(workload, applications))
            result.pop('active_observation', None)
            result['passed'] = True
        except Exception as error:
            result['failure_type'] = type(error).__name__
            result['failure_category'] = str(error) if re.fullmatch(r'[A-Z_]+', str(error)) else 'UNCLASSIFIED'
        finally:
            failures = []
            for role in ('gateway1', 'worker', 'collector'):
                if role in all_processes:
                    try:
                        all_processes[role].stop()
                    except Exception:
                        failures.append(role)
                    finally:
                        if role == 'worker' and all_processes[role].proc.poll() is not None:
                            self.worker_restart_after = time.monotonic() + 6
            result['cleanup_failures'] = failures
            result['passed'] &= not failures
            result['observed_live_descendants_after_cleanup'] = sum(
                self.ops.proc_identity(pid) == identity for pid, identity in self.epoch_owned.items())
            result['passed'] &= result['observed_live_descendants_after_cleanup'] == 0
            result['epoch_seconds_including_startup_warmups_validation_flush'] = time.monotonic() - started
            result['epoch_service_cpu_seconds_inclusive'] = (counters(self.group / 'cpu.stat')['usage_usec'] - baseline_cpu) / 1e6
            result['campaign_cumulative_charged_peak_bytes'] = int((self.group / 'memory.peak').read_text())
        if result['passed']:
            try:
                result['collector'] = self.collector_evidence(directory, ratio)
            except Exception as error:
                result['passed'] = False
                result['failure_type'] = type(error).__name__
                result['failure_category'] = str(error) if re.fullmatch(r'[A-Z_]+', str(error)) else 'COLLECTOR_RECONCILIATION'
        return result

    def run(self):
        try:
            if self.args.mode == 'full':
                smoke = read_smoke_receipt(self.args.smoke_receipt, self.args.smoke_receipt_sha256)
                check_smoke_receipt(smoke, self.report)
                require(removed(smoke['unit']), 'SMOKE_SERVICE_NOT_CLEARED')
                self.report['prerequisite_smoke_sha256'] = self.args.smoke_receipt_sha256
                self.report['prerequisite_smoke_unit'] = smoke['unit']
                self.report['prerequisite_smoke_reconciled'] = True
            self.setup()
            contrasts = CONTRASTS[:1] if self.args.mode == 'smoke' else CONTRASTS
            pairs = 1 if self.args.mode == 'smoke' else PAIRS
            for contrast, (left, right) in enumerate(contrasts):
                for pair in range(pairs):
                    order = (left, right) if pair % 2 == 0 else (right, left)
                    workloads = ('aggregate', 'transfer') if pair % 2 == 0 else ('transfer', 'aggregate')
                    for variant in order:
                        result = self.epoch(variant, workloads, len(self.report['epochs']))
                        result.update(contrast=contrast, pair=pair, position=order.index(variant))
                        self.report['epochs'].append(result)
                        save(self.args.output / 'receipt.json', self.report)
                        require(result['passed'], 'FAILED_EPOCH_NO_RETRY')
            if self.args.mode == 'smoke':
                self.report['smoke_checks'] = smoke_summary(self.report['epochs'])
            else:
                self.report['comparison'] = summarize(self.report['epochs'])
            self.deadline()
            self.report['passed'] = True
        finally:
            cleanup = True
            if not self.brokers and (self.fixture / 'pids.json').is_file():
                self.brokers = json.loads((self.fixture / 'pids.json').read_text())
            for item in self.brokers:
                if self.cf.broker_alive(item):
                    identity = self.ops.proc_identity(item['pid'])
                    if identity:
                        self.ops.signal_owned(identity, signal.SIGTERM)
            until = time.monotonic() + 10
            while any(self.cf.broker_alive(item) for item in self.brokers) and time.monotonic() < until:
                time.sleep(.05)
            cleanup = not any(self.cf.broker_alive(item) for item in self.brokers)
            self.report['brokers_exited'] = cleanup
            self.report['source_unchanged'] = self.identity.source_manifest(self.args.repo) == self.source
            self.report['data_unchanged'] = digest(self.data) == self.data_hash
            self.report['binary_unchanged'] = digest(self.args.binary) == self.args.binary_sha256
            self.report['sandbox_unchanged'] = digest(self.args.sandbox) == self.args.sandbox_sha256
            self.report['collector_unchanged'] = digest(self.args.collector) == self.args.collector_sha256
            self.report['helpers_unchanged'] = helper_hashes() == self.report['helper_sha256']
            self.report['workload_unchanged'] = workload_hash() == self.report['workload_sha256']
            if self.args.mode == 'full':
                self.report['prerequisite_smoke_unchanged'] = digest(self.args.smoke_receipt) == self.args.smoke_receipt_sha256
            self.report['passed'] &= cleanup and all(self.report[k] for k in (
                'source_unchanged', 'data_unchanged', 'binary_unchanged', 'sandbox_unchanged',
                'collector_unchanged', 'helpers_unchanged', 'workload_unchanged'))
            if self.args.mode == 'full':
                self.report['passed'] &= self.report.get('prerequisite_smoke_reconciled') is True and self.report['prerequisite_smoke_unchanged']
            self.report['independent_service_cleanup_required'] = True
            save(self.args.output / 'receipt.json', self.report)
        require(self.report['passed'], 'EXPERIMENT_FAILED')


def validate_tree(events, root, startup):
    require(root['parent_id'] == '', 'TRACE_ROOT_PARENT')
    successful = {'kelvo.kind': 'query', 'kelvo.outcome': 'success'}
    phases = {'kelvo.phase.' + name for name in ('validation', 'node_admission', 'source_admission',
                                               'prepare', 'execution_delivery', 'cleanup')}
    query_spans = [s for s in events if s['name'] == 'kelvo.query']
    require(len(query_spans) == 1, 'TRACE_QUERY_CARDINALITY')
    query_span = query_spans[0]
    phase_spans = [s for s in events if s['name'] in phases]
    require(len(phase_spans) == 6 and {s['name'] for s in phase_spans} == phases
            and all(s['parent_id'] == query_span['span_id'] and s['role'] == 'worker'
                    and s['attributes'] == {} for s in phase_spans), 'TRACE_QUERY_PHASES')
    if startup:
        require(len(events) == 7 and root == query_span and query_span['role'] == 'worker'
                and query_span['attributes'] == successful, 'STARTUP_TRACE_SHAPE')
        return
    required = {'kelvo.cluster.submit': 'gateway1', 'kelvo.cluster.dispatch': 'worker',
                'kelvo.cluster.relay': 'gateway1', 'kelvo.query': 'worker'}
    for name, role in required.items():
        selected = [s for s in events if s['name'] == name]
        require(len(selected) == 1 and selected[0]['role'] == role and selected[0]['attributes'] == successful,
                'DURABLE_TRACE_OPERATION')
        if selected[0] is not root:
            require(selected[0]['parent_id'] == root['span_id'], 'DURABLE_TRACE_PARENT')
    waits = [s for s in events if s['name'] == 'kelvo.cluster.result_wait']
    require(len(waits) <= 1 and all(s['role'] == 'gateway1' and s['parent_id'] == root['span_id']
            and s['attributes'] == successful for s in waits), 'CONDITIONAL_RESULT_WAIT')
    require(len(events) == 10 + len(waits), 'UNEXPECTED_TRACE_SPAN')


def distribution(values):
    require(len(values) == PAIRS and all(math.isfinite(v) for v in values), 'MATCHED_SAMPLE_COUNT')
    quartiles = statistics.quantiles(values, n=4, method='inclusive')
    return {'count': len(values), 'median': statistics.median(values), 'mean': statistics.mean(values),
        'sample_stddev': statistics.stdev(values), 'min': min(values), 'max': max(values), 'iqr': quartiles[2] - quartiles[0]}


def validate_epochs(epochs, contrasts, pairs):
    require(type(epochs) is list and len(epochs) == len(contrasts) * pairs * 2
            and all(type(e) is dict and e.get('passed') is True for e in epochs), 'COMPLETE_PASSED_EPOCHS_REQUIRED')
    observed = set()
    for epoch in epochs:
        contrast, pair, position = (epoch.get(k) for k in ('contrast', 'pair', 'position'))
        require(type(contrast) is int and 0 <= contrast < len(contrasts)
                and type(pair) is int and 0 <= pair < pairs
                and type(position) is int and position in (0, 1), 'EPOCH_COORDINATES')
        coordinates = (contrast, pair, position)
        require(coordinates not in observed, 'DUPLICATE_EPOCH')
        observed.add(coordinates)
        left, right = contrasts[contrast]
        order = (left, right) if pair % 2 == 0 else (right, left)
        require(epoch.get('variant') == order[position], 'MATCHED_VARIANT_ORDER')
        metrics, ratio = VARIANTS[epoch['variant']]
        require(type(epoch.get('metrics')) is bool and epoch['metrics'] == metrics
                and 'tracing_ratio' in epoch and (epoch['tracing_ratio'] is None if ratio is None
                    else type(epoch['tracing_ratio']) in (int, float) and epoch['tracing_ratio'] == ratio),
                'EPOCH_VARIANT_CONFIGURATION')
        require(epoch.get('cleanup_failures') == []
                and type(epoch.get('observed_live_descendants_after_cleanup')) is int
                and epoch['observed_live_descendants_after_cleanup'] == 0, 'EPOCH_CLEANUP')
        for phase in ('warmups', 'measured'):
            entries = epoch.get(phase)
            require(type(entries) is list and len(entries) == len(SQL)
                    and all(type(entry) is dict for entry in entries)
                    and {entry.get('workload') for entry in entries} == set(SQL), 'EXACT_WORKLOADS_REQUIRED')
            require(all(type(entry.get('receive_seconds')) in (int, float)
                        and math.isfinite(entry['receive_seconds']) and entry['receive_seconds'] > 0
                        for entry in entries), 'INVALID_RECEIVE_DURATION')
            for entry in entries:
                answer = entry.get('answer')
                require(type(answer) is dict and all(answer.get(key) is True
                    for key in ('exact_values', 'exact_types', 'explicit_eos', 'no_trailing_bytes')),
                    'RESULT_INTEGRITY')
                require(type(answer.get('rows')) is int and answer['rows'] == (10 if entry['workload'] == 'aggregate' else ROWS)
                    and type(answer.get('batches')) is int and 0 < answer['batches'] <= 16_384
                    and type(answer.get('wire_bytes')) is int and 0 < answer['wire_bytes'] <= MAX_BODY, 'RESULT_COUNTS')


def smoke_summary(epochs):
    validate_epochs(epochs, CONTRASTS[:1], 1)
    for epoch in epochs:
        collector = epoch.get('collector')
        require(type(collector) is dict and all(type(collector.get(key)) is int and collector[key] == 0
            for key in ('requests', 'received_bytes', 'accepted_spans', 'observed_submission_spans',
                        'observed_query_spans', 'observed_independent_startup_query_spans',
                        'duplicate_span_ids', 'observed_missing_parent_links')), 'SMOKE_UNEXPECTED_TRACE_TRAFFIC')
        require(all(type(collector.get(key)) is str and re.fullmatch(r'[0-9a-f]{64}', collector[key])
            for key in ('summary_sha256', 'ledger_sha256')), 'SMOKE_COLLECTOR_EVIDENCE')
    return {'passed': True, 'epochs': 2, 'warmup_queries': 4, 'measured_queries': 4,
            'source_rows': ROWS, 'workloads_per_epoch': sorted(SQL),
            'verified_result_rows': {'aggregate': 40, 'transfer': 4 * ROWS},
            'collector_present_each_epoch': True, 'tracing_absent_no_exports': True,
            'evidence_kind': 'fixture_correctness_only'}


def check_smoke_receipt(smoke, current):
    require(type(smoke) is dict and smoke.get('mode') == 'smoke' and smoke.get('passed') is True
            and all(type(smoke.get(key)) is int and smoke[key] == value for key, value in (
                ('schema_version', 1), ('pairs', 1), ('rows', ROWS),
                ('operation_budget_seconds', SMOKE_SECONDS), ('hard_watchdog_seconds', 900)))
            and type(smoke.get('unit')) is str and re.fullmatch(r'kelvo-telemetry-overhead-[0-9a-f]{12}', smoke['unit'])
            and 'comparison' not in smoke,
            'PASSED_SMOKE_REQUIRED')
    require(smoke.get('smoke_checks') == smoke_summary(smoke.get('epochs')), 'SMOKE_CHECKS_MISMATCH')
    identities = ('source', 'binary_sha256', 'sandbox_sha256', 'collector_sha256',
                  'helper_sha256', 'workload_sha256', 'data_sha256')
    require(all(key in smoke and key in current and smoke[key] == current[key] for key in identities),
            'SMOKE_INPUT_IDENTITY_MISMATCH')
    require(all(smoke.get(key) is True for key in ('brokers_exited', 'source_unchanged', 'data_unchanged',
            'binary_unchanged', 'sandbox_unchanged', 'collector_unchanged', 'helpers_unchanged',
            'workload_unchanged', 'independent_service_cleanup_required')), 'SMOKE_CLEANUP_OR_INTEGRITY')


def summarize(epochs):
    validate_epochs(epochs, CONTRASTS, PAIRS)
    result = []
    for contrast, (left, right) in enumerate(CONTRASTS):
        for workload in SQL:
            paired = []
            for pair in range(PAIRS):
                entries = [e for e in epochs if e['contrast'] == contrast and e['pair'] == pair]
                require(len(entries) == 2 and {e['variant'] for e in entries} == {left, right}, 'UNPAIRED_EPOCH')
                times = {e['variant']: next(t['receive_seconds'] for t in e['measured'] if t['workload'] == workload) for e in entries}
                paired.append({'pair': pair, 'left_seconds': times[left], 'right_seconds': times[right],
                    'delta_seconds': times[right] - times[left], 'ratio': times[right] / times[left]})
            result.append({'contrast': [left, right], 'workload': workload, 'pairs': paired,
                'left': distribution([p['left_seconds'] for p in paired]), 'right': distribution([p['right_seconds'] for p in paired]),
                'delta': distribution([p['delta_seconds'] for p in paired]), 'ratio': distribution([p['ratio'] for p in paired])})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute-after-campaign', action='store_true', required=True)
    parser.add_argument('--mode', choices=('smoke', 'full'), required=True)
    parser.add_argument('--smoke-receipt', type=Path)
    parser.add_argument('--smoke-receipt-sha256')
    for name in ('repo', 'binary', 'sandbox', 'collector', 'collector-cert', 'collector-key', 'nats-archive',
                 'prior-receipt', 'build-source', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    for name in ('revision', 'unit', 'prior-unit', 'host-network-namespace', 'trust-bundle-sha256',
                 'binary-sha256', 'sandbox-sha256', 'collector-sha256'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    require((args.smoke_receipt is not None and type(args.smoke_receipt_sha256) is str
            and re.fullmatch(r'[0-9a-f]{64}', args.smoke_receipt_sha256)) if args.mode == 'full'
            else args.smoke_receipt is None and args.smoke_receipt_sha256 is None, 'MODE_PREREQUISITE_ARGUMENTS')
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    os.umask(0o077)
    experiment = Experiment(args, guard(args))
    experiment.run()


if __name__ == '__main__':
    main()
