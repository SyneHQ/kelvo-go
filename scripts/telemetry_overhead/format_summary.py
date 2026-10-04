"""Format a reconciled full telemetry run. No execution or benchmark launch."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics

PAIRS = 6
ROWS = 1_000_000
MAX_BODY = 96 << 20
MAX_RECEIPT = 16 << 20
VARIANTS = {'metrics_off': (False, None), 'metrics_on': (True, None),
            'trace_zero': (True, 0), 'trace_tenth': (True, .1), 'trace_all': (True, 1)}
CONTRASTS = [('metrics_off', 'metrics_on'), ('metrics_on', 'trace_zero'),
             ('metrics_on', 'trace_tenth'), ('metrics_on', 'trace_all')]
LABELS = ['Metrics: off to on', 'Tracing: off to 0%', 'Tracing: off to 10%', 'Tracing: off to 100%']
OUTER_CHECKS = ('helpers_reconciled', 'prerequisites_reconciled', 'runner_passed', 'controls_reconciled',
    'python_reconciled', 'isolation_reconciled', 'harness_log_reconciled', 'inner_reconciled',
    'host_trust_unchanged', 'prepared_trust_reconciled', 'service_exit_reconciled',
    'local_launch_reconciled', 'local_stage_reconciled', 'local_trust_reconciled', 'reconciled')
COLLECTOR_COUNTERS = {'requests': 128, 'received_bytes': 8 << 20, 'accepted_spans': 2048,
    'observed_submission_spans': 4, 'observed_query_spans': 4,
    'observed_independent_startup_query_spans': 1, 'duplicate_span_ids': 0,
    'observed_missing_parent_links': 2048}
LOSS_SCOPE = 'Offered denominator unavailable; fractional sampling and absent parents are not proof of exporter loss.'


class ReceiptError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise ReceiptError(code)


def number(value, positive=False):
    return type(value) in (int, float) and math.isfinite(value) and (value > 0 if positive else True)


def sha(value):
    return type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value) is not None


def hash_inventory(value, keys, code):
    require(type(value) is dict and set(value) == set(keys) and all(sha(checksum) for checksum in value.values()), code)


def python_identity(value):
    require(type(value) is dict and set(value) == {'executable', 'executable_sha256', 'version', 'packages'}
            and type(value['executable']) is str and value['executable'].startswith('/')
            and 0 < len(value['executable']) <= 4096 and sha(value['executable_sha256'])
            and type(value['version']) is str and 0 < len(value['version']) <= 4096
            and type(value['packages']) is dict and set(value['packages']) == {'pyarrow', 'PyYAML'}, 'PYTHON_IDENTITY')
    for package in value['packages'].values():
        require(type(package) is dict and set(package) == {'version', 'metadata_sha256', 'record_sha256'}
                and type(package['version']) is str and 0 < len(package['version']) <= 256
                and sha(package['metadata_sha256']) and sha(package['record_sha256']), 'PYTHON_PACKAGE_IDENTITY')


def collector_evidence(value, ratio):
    require(type(value) is dict and set(value) == set(COLLECTOR_COUNTERS) | {
        'summary_sha256', 'ledger_sha256', 'sdk_offered_spans', 'sdk_lost_spans', 'loss_scope'}
        and sha(value['summary_sha256']) and sha(value['ledger_sha256'])
        and value['sdk_offered_spans'] is None and value['sdk_lost_spans'] is None
        and value['loss_scope'] == LOSS_SCOPE, 'EPOCH_COLLECTOR_EVIDENCE')
    require(all(type(value[key]) is int and 0 <= value[key] <= bound for key, bound in COLLECTOR_COUNTERS.items()),
            'EPOCH_COLLECTOR_COUNTERS')
    roots = sum(value[key] for key in ('observed_submission_spans', 'observed_query_spans',
                                     'observed_independent_startup_query_spans'))
    require(roots <= value['accepted_spans'] and value['observed_missing_parent_links'] <= value['accepted_spans'],
            'EPOCH_COLLECTOR_SPAN_COUNTS')
    if ratio in (None, 0):
        require(all(value[key] == 0 for key in COLLECTOR_COUNTERS), 'DISABLED_TRACE_TRAFFIC')
    if ratio == 1:
        require(value['observed_submission_spans'] == value['observed_query_spans'] == 4
                and value['observed_independent_startup_query_spans'] == 1
                and value['observed_missing_parent_links'] == 0
                and 47 <= value['accepted_spans'] <= 51
                and value['requests'] > 0 and value['received_bytes'] > 0, 'FULL_SAMPLE_TOPOLOGY')


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'DUPLICATE_JSON_KEY')
        result[key] = value
    return result


def read_receipt(path):
    require(path.is_file() and not path.is_symlink() and 0 < path.stat().st_size <= MAX_RECEIPT, 'RECEIPT_FILE_BOUND')
    with path.open('rb') as source:
        raw = source.read(MAX_RECEIPT + 1)
    require(0 < len(raw) <= MAX_RECEIPT, 'RECEIPT_BYTES_BOUND')
    return json.loads(raw, object_pairs_hook=unique_object, parse_constant=lambda _: (_ for _ in ()).throw(ReceiptError('NONFINITE_JSON')))


def cleanup(value, unit):
    require(type(value) is dict and value.get('unit') == unit and value.get('passed') is True
            and value.get('owned_service_removed') is True and value.get('owned_cgroup_removed') is True,
            'CLEANUP_NOT_RECONCILED')
    fields = value.get('fields', {})
    require(all(fields.get(key) == expected for key, expected in
        (('LoadState', 'not-found'), ('ActiveState', 'inactive'), ('SubState', 'dead'), ('MainPID', '0'))), 'UNIT_NOT_REMOVED')


def source_identity(value, revision):
    require(type(value) is dict and value.get('base_revision') == revision and value.get('verified') is True
            and type(value.get('files')) is dict and type(value.get('file_count')) is int
            and value['file_count'] == len(value['files']) > 0
            and all(type(name) is str and sha(checksum) for name, checksum in value['files'].items()), 'SOURCE_IDENTITY')
    canonical = json.dumps(value['files'], sort_keys=True, separators=(',', ':')).encode()
    require(value.get('sha256') == hashlib.sha256(canonical).hexdigest(), 'SOURCE_MANIFEST_HASH')


def distribution(values):
    require(len(values) == PAIRS and all(number(value) for value in values), 'EXACT_PAIRED_SAMPLES')
    quartiles = statistics.quantiles(values, n=4, method='inclusive')
    return {'count': PAIRS, 'median': statistics.median(values), 'mean': statistics.mean(values),
        'sample_stddev': statistics.stdev(values), 'min': min(values), 'max': max(values), 'iqr': quartiles[2] - quartiles[0]}


def same_statistics(actual, expected):
    if type(expected) is dict:
        return type(actual) is dict and set(actual) == set(expected) and all(same_statistics(actual[k], v) for k, v in expected.items())
    if type(expected) is list:
        return type(actual) is list and len(actual) == len(expected) and all(same_statistics(a, b) for a, b in zip(actual, expected))
    if type(expected) is int:
        return type(actual) is int and actual == expected
    if type(expected) is float:
        return number(actual) and math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-15)
    return type(actual) is type(expected) and actual == expected


def validate(receipt):
    require(type(receipt) is dict and receipt.get('passed') is True and receipt.get('mode') == 'full'
            and all(receipt.get(key) is True for key in OUTER_CHECKS), 'PASSED_RECONCILED_FULL_REQUIRED')
    revision, unit = receipt.get('revision'), receipt.get('unit')
    require(type(revision) is str and re.fullmatch(r'[0-9a-f]{40}', revision)
            and type(unit) is str and re.fullmatch(r'kelvo-telemetry-overhead-[0-9a-f]{12}', unit), 'RUN_IDENTITY')
    require(type(receipt.get('collection_ssh_exit_code')) is int and receipt['collection_ssh_exit_code'] == 0, 'COLLECTION_EXIT')
    require(all(sha(receipt.get(key)) for key in ('staging_json_sha256', 'runner_receipt_sha256', 'runner_sha256',
            'service_exit_sha256', 'inner_receipt_sha256'))
            and receipt['runner_receipt_sha256'] == receipt['runner_sha256'], 'OUTER_RECEIPT_IDENTITIES')
    python_identity(receipt.get('python_identity'))
    for field, exit_key in (('service_exit', 'systemd_run_exit_code'), ('local_launch', 'ssh_exit_code')):
        value = receipt.get(field, {})
        require(value.get('revision') == revision and value.get('mode') == 'full' and value.get('unit') == unit
                and type(value.get(exit_key)) is int and value[exit_key] == 0, 'LAUNCH_EXIT')
    cleanup(receipt.get('cleanup'), unit)
    controls = receipt.get('controls', {})
    control_unit = 'kelvo-telemetry-controls-' + unit.rsplit('-', 1)[1]
    require(controls.get('passed') is True and controls.get('revision') == revision and controls.get('unit') == control_unit
            and type(controls.get('systemd_run_exit_code')) is int and controls['systemd_run_exit_code'] == 0
            and sha(controls.get('controls_sha256'))
            and controls.get('staging_json_sha256') == receipt['staging_json_sha256'], 'CONTROLS_NOT_RECONCILED')
    cleanup(controls.get('cleanup'), control_unit)
    trust = receipt.get('trust_preparation', {})
    require(type(trust) is dict and set(trust) == {'system_bundle_before_sha256', 'prepared_bundle_sha256', 'collector_certificate_sha256'}
            and all(sha(value) for value in trust.values()), 'TRUST_IDENTITY')
    isolation = receipt.get('enforced_isolation', {})
    require(isolation.get('memory_bytes') == 6 << 30 and isolation.get('swap_bytes') == 0 and isolation.get('tasks') == 512
            and type(isolation.get('cpu_period')) is int and isolation['cpu_period'] > 0
            and isolation.get('cpu_quota') == 2 * isolation['cpu_period']
            and isolation.get('non_root') is True and isolation.get('private_network') is True, 'FIXTURE_RESOURCE_PROFILE')
    inner = receipt.get('inner_receipt', {})
    require(inner.get('passed') is True and inner.get('mode') == 'full' and inner.get('unit') == unit
            and type(inner.get('schema_version')) is int and inner['schema_version'] == 1
            and type(inner.get('pairs')) is int and inner['pairs'] == PAIRS
            and type(inner.get('rows')) is int and inner['rows'] == ROWS
            and inner.get('operation_budget_seconds') == 2400 and inner.get('hard_watchdog_seconds') == 3600
            and sha(receipt.get('inner_receipt_sha256')), 'INNER_RUN_IDENTITY')
    require(all(inner.get(key) is True for key in ('brokers_exited', 'source_unchanged', 'data_unchanged',
        'binary_unchanged', 'sandbox_unchanged', 'collector_unchanged', 'helpers_unchanged', 'workload_unchanged',
        'independent_service_cleanup_required', 'prerequisite_smoke_reconciled', 'prerequisite_smoke_unchanged'))
        and sha(inner.get('prerequisite_smoke_sha256')) and sha(inner.get('prior_receipt_sha256')),
        'INNER_INTEGRITY_OR_SMOKE_PREREQUISITE')
    hash_inventory(inner.get('helper_sha256'), ('harness.py', 'workloads.py'), 'INNER_HELPER_IDENTITIES')
    source_identity(inner.get('source'), revision)
    runtime = receipt.get('runtime', {})
    require(runtime.get('source') == inner['source'] and runtime.get('binary_sha256') == {
        'kelvo': inner.get('binary_sha256'), 'kelvo-landlock': inner.get('sandbox_sha256'), 'collector': inner.get('collector_sha256')}
        and all(sha(inner.get(key)) for key in ('binary_sha256', 'sandbox_sha256', 'collector_sha256', 'workload_sha256', 'data_sha256')),
        'RUNTIME_BINARY_IDENTITY')
    hash_inventory(runtime.get('effective_module_files'), ('mod_sha256', 'sum_sha256'), 'RUNTIME_MODULE_IDENTITIES')
    hash_inventory(runtime.get('shared_inputs'), ('driver_tree_sha256', 'headers_tree_sha256', 'driver_patch_marker_sha256'),
                   'RUNTIME_SHARED_IDENTITIES')
    require(all(sha(runtime.get(key)) for key in ('otlp_public_receipt_sha256', 'original_validation_sha256', 'nats_sha256')),
            'RUNTIME_PREREQUISITE_IDENTITIES')
    epochs = inner.get('epochs')
    require(type(epochs) is list and len(epochs) == 48, 'EXACT_48_EPOCHS_REQUIRED')
    observed, times = set(), {}
    for epoch in epochs:
        require(type(epoch) is dict and epoch.get('passed') is True, 'FAILED_EPOCH')
        contrast, pair, position = (epoch.get(key) for key in ('contrast', 'pair', 'position'))
        require(type(contrast) is int and 0 <= contrast < 4 and type(pair) is int and 0 <= pair < PAIRS
                and type(position) is int and position in (0, 1), 'EPOCH_COORDINATES')
        coordinate = (contrast, pair, position)
        require(coordinate not in observed, 'DUPLICATE_EPOCH')
        observed.add(coordinate)
        order = CONTRASTS[contrast] if pair % 2 == 0 else CONTRASTS[contrast][::-1]
        variant = epoch.get('variant')
        require(variant == order[position], 'VARIANT_ORDER')
        metrics, ratio = VARIANTS[variant]
        require(type(epoch.get('metrics')) is bool and epoch['metrics'] == metrics and 'tracing_ratio' in epoch
                and (epoch['tracing_ratio'] is None if ratio is None else number(epoch['tracing_ratio']) and epoch['tracing_ratio'] == ratio), 'VARIANT_CONFIGURATION')
        require(epoch.get('cleanup_failures') == [] and type(epoch.get('observed_live_descendants_after_cleanup')) is int
                and epoch['observed_live_descendants_after_cleanup'] == 0, 'EPOCH_CLEANUP')
        collector_evidence(epoch.get('collector'), ratio)
        for phase in ('warmups', 'measured'):
            entries = epoch.get(phase)
            require(type(entries) is list and len(entries) == 2 and all(type(item) is dict for item in entries)
                    and {item.get('workload') for item in entries} == {'aggregate', 'transfer'}, 'EXACT_WORKLOADS')
            for item in entries:
                require(number(item.get('receive_seconds'), positive=True), 'FINITE_POSITIVE_DURATION')
                answer = item.get('answer', {})
                require(all(answer.get(key) is True for key in ('exact_values', 'exact_types', 'explicit_eos', 'no_trailing_bytes'))
                        and type(answer.get('rows')) is int and answer['rows'] == (10 if item['workload'] == 'aggregate' else ROWS)
                        and type(answer.get('batches')) is int and 0 < answer['batches'] <= 16384
                        and type(answer.get('wire_bytes')) is int and 0 < answer['wire_bytes'] <= MAX_BODY, 'RESULT_INTEGRITY')
                if phase == 'measured':
                    times[contrast, pair, variant, item['workload']] = item['receive_seconds']
    comparisons = []
    for contrast, (left, right) in enumerate(CONTRASTS):
        for workload in ('aggregate', 'transfer'):
            pairs = []
            for pair in range(PAIRS):
                baseline, variant = times[contrast, pair, left, workload], times[contrast, pair, right, workload]
                pairs.append({'pair': pair, 'left_seconds': baseline, 'right_seconds': variant,
                    'delta_seconds': variant - baseline, 'ratio': variant / baseline})
            comparisons.append({'contrast': [left, right], 'workload': workload, 'pairs': pairs,
                'left': distribution([item['left_seconds'] for item in pairs]),
                'right': distribution([item['right_seconds'] for item in pairs]),
                'delta': distribution([item['delta_seconds'] for item in pairs]),
                'ratio': distribution([item['ratio'] for item in pairs])})
    require(same_statistics(inner.get('comparison'), comparisons), 'COMPARISON_DOES_NOT_MATCH_EPOCHS')
    return revision, comparisons


def format_receipt(receipt):
    revision, comparisons = validate(receipt)
    lines = ['Local fixture: one worker, one million Parquet source rows, and a total service quota of 2 CPU / 6 GiB.', '',
        'Warmups are excluded. Timings cover submission through complete body receipt; client Arrow validation is excluded. '
        'Tracing comparisons keep metrics enabled. Ratios are variant/baseline; above 1 means slower.', '']
    for workload, title in (('aggregate', 'Aggregate CTE: one million source rows, ten groups returned'),
                            ('transfer', 'Transfer CTE: one million ordered rows returned')):
        lines += [f'**{title}**', '',
            '| Comparison | Baseline median (ms) | Variant median (ms) | Median paired ratio | Ratio IQR | Paired ratio min–max |',
            '|---|---:|---:|---:|---:|---:|']
        for label, contrast in zip(LABELS, CONTRASTS):
            item = next(value for value in comparisons if value['workload'] == workload and value['contrast'] == list(contrast))
            ratio = item['ratio']
            lines.append(f"| {label} | {item['left']['median'] * 1000:.3f} | {item['right']['median'] * 1000:.3f} | "
                f"{ratio['median']:.3f}x | {ratio['iqr']:.3f} | {ratio['min']:.3f}x–{ratio['max']:.3f}x |")
        lines.append('')
    lines += ['Six matched pairs per comparison; IQR is the 75th–25th percentile spread of paired ratios. '
              'Ratios are raw observations; IQR/min–max describe spread, not confidence intervals or statistical significance. '
              '48 epochs, 96 measured queries and 96 warmups. These fixture measurements do not establish production capacity.', '',
              f'[Measured runtime {revision[:12]}](https://github.com/SYNEHQ/kelvo-go/commit/{revision}).', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        rendered = format_receipt(read_receipt(args.input))
    except (ReceiptError, KeyError, TypeError, ValueError, AttributeError, OverflowError):
        parser.error('Receipt rejected: requires a complete, reconciled full run with matching paired statistics.')
    with args.output.open('x') as output:
        output.write(rendered)


if __name__ == '__main__':
    main()
