#!/usr/bin/env python3
"""Read-only independent timestamp calendar/lock/contract audit; stdout JSON only.

Run with Python >=3.11. No Rust compilation, CLI execution, network, or corpus
mutation. The frozen-store portion inspects retained receipts, not a fresh run.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import platform
import re
import sys
import tomllib


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def leap(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def before_year(year):
    y = year - 1
    return 365 * y + y // 4 - y // 100 + y // 400


PATTERN = re.compile(r'(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)', re.ASCII)


def independent_timestamp(text):
    m = PATTERN.fullmatch(text)
    require(m is not None, 'strict timestamp spelling: ' + text)
    y, month, day, hour, minute, second = map(int, m.groups()[:6])
    months = [31, 29 if leap(y) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    require(1 <= month <= 12 and 1 <= day <= months[month - 1], 'calendar: ' + text)
    require(hour < 24 and minute < 60 and second < 60, 'clock: ' + text)
    fraction = int((m[7] or '')[:6].ljust(6, '0'))
    zone = m[8]
    offset = 0
    if zone != 'Z':
        zh, zm = int(zone[1:3]), int(zone[4:])
        require(zh < 24 and zm < 60, 'offset: ' + text)
        offset = (1 if zone[0] == '+' else -1) * (zh * 3600 + zm * 60)
    days = before_year(y) - before_year(1970) + sum(months[:month - 1]) + day - 1
    micros = (days * 86400 + hour * 3600 + minute * 60 + second - offset) * 1_000_000 + fraction
    canonical_zone = 'Z' if offset == 0 else zone
    canonical = text[:19] + (f'.{fraction:06}' if fraction else '') + canonical_zone
    return micros, canonical


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo-root', type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    root = args.repo_root.resolve()
    evidence = root / '.local/evidence/hyperliquid-integration-review'
    vector_path = root / 'docs/evidence/timestamp-backend-20261020/compatibility-vectors.json'
    lock_before = evidence / 'timestamp-lock-before.toml'
    contract_before = evidence / 'timestamp-contract-hashes-before-01.json'
    vectors = json.loads(vector_path.read_text())
    contract_hashes = json.loads(contract_before.read_text())
    sources = ['Cargo.toml', 'Cargo.lock', 'crates/event-pulse/src/wire.rs',
               'crates/event-pulse-capture/src/fixture_v4.rs', 'crates/datasets/src/export.rs',
               'crates/event-pulse/tests/timestamp_backend_compatibility.rs', 'deny.toml']
    watched = [vector_path, lock_before, contract_before] + [root / p for p in sources] + [root / p for p in contract_hashes]
    start_hashes = {str(p.relative_to(root)): sha(p) for p in watched}
    require(sha(vector_path) == 'b157a0effdcb92f8e187aa2954ec713826ef3a58ec09fe2331b2e25175ae3dc7', 'legacy vector bytes changed')
    require(len(vectors['parse']) == 302 and len(vectors['nanos']) == 17, 'vector counts')
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    checked = stdlib_checked = 0
    minimum = (before_year(0) - before_year(1970)) * 86400 * 1_000_000
    upper_exclusive = (before_year(10000) - before_year(1970)) * 86400 * 1_000_000
    for row in vectors['parse']:
        if not row['accepted']:
            continue
        micros, canonical = independent_timestamp(row['input'])
        require(canonical == row['canonical'] and micros == row['utc_micros'], 'accepted legacy vector: ' + row['input'])
        require((row['utc_canonical'] is not None) == (minimum <= micros < upper_exclusive), 'UTC calendar boundary: ' + row['input'])
        if row['utc_canonical'] is not None:
            u, spelling = independent_timestamp(row['utc_canonical'])
            require(u == micros and spelling == row['utc_canonical'] and spelling.endswith('Z'), 'UTC instant/canonical mismatch')
        if not canonical.startswith('0000'):
            delta = datetime.datetime.fromisoformat(canonical.replace('Z', '+00:00')) - epoch
            require(delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds == micros, 'stdlib epoch mismatch')
            stdlib_checked += 1
        checked += 1
    require(checked == 274 and stdlib_checked == 272, 'accepted independent counts')
    for row in vectors['nanos']:
        n = row['input']
        sec, frac = divmod(n, 1_000_000_000)
        d = epoch + datetime.timedelta(seconds=sec)
        head = f'{d.year:04}-{d.month:02}-{d.day:02}T{d.hour:02}:{d.minute:02}:{d.second:02}'
        expected = head + ('.' + f'{frac:09}'.rstrip('0') if frac else '') + 'Z'
        require(expected == row['export_utc'], 'shortest nanosecond formatter')
        require(n // 1000 == row['utc_micros'] == independent_timestamp(row['canonical'])[0], 'negative floor/extrema')
    old = tomllib.loads(lock_before.read_text())
    new = tomllib.loads((root / 'Cargo.lock').read_text())
    key = lambda p: (p['name'], p['version'], p.get('source'))
    old_registry = {key(p): p for p in old['package'] if p.get('source', '').startswith('registry+')}
    new_registry = {key(p): p for p in new['package'] if p.get('source', '').startswith('registry+')}
    removed = sorted(set(old_registry) - set(new_registry))
    expected_removed = {('deranged', '0.4.0'), ('num-conv', '0.1.0'), ('powerfmt', '0.2.0'),
                        ('time', '0.3.41'), ('time-core', '0.1.4'), ('time-macros', '0.2.22')}
    require({p[:2] for p in removed} == expected_removed and len(removed) == 6, 'exact removed packages')
    require(not set(new_registry) - set(old_registry), 'unexpected registry addition')
    require(len(new_registry) == 263 and all(p == old_registry[k] for k, p in new_registry.items()), 'complete registry records changed')
    require(len(contract_hashes) == 17, 'contract baseline count')
    for path, expected in contract_hashes.items():
        require(sha(root / path) == expected, 'frozen contract changed: ' + path)
    # Pin v1 evidence and compare the exact relocated test block separately
    # from all production bytes. Ignore only trailing LF separators at EOF.
    v1_helper = evidence / 'timestamp-independent-audit.py'
    v1_result = evidence / 'timestamp-independent-audit.json'
    require(sha(v1_helper) == 'c17e1a40d6fe011afd7a35075738ba06cf0b7c9851d0d5575451ca414e15ba30', 'v1 helper changed')
    require(sha(v1_result) == 'e9f124448da8fda69c1e065a2e94d7cbf5fa8aa7ae65773539a47d404a208684', 'v1 result changed')
    pre_move = evidence / 'timestamp-export-before-test-move.rs'
    require(sha(pre_move) == '90efa1eef23a1ea62e8fc25d9f5e2b2fcc5571cce7ec96de27ce1bdddae7813a', 'retained pre-move export source changed')
    old_source = pre_move.read_text()
    final_source = (root / 'crates/datasets/src/export.rs').read_text()
    marker = '#[cfg(test)]\nmod timestamp_tests {'
    require(old_source.count(marker) == final_source.count(marker) == 1, 'timestamp test module multiplicity')
    old_start = old_source.index(marker)
    old_end = old_source.index('fn bounded_file(', old_start)
    new_start = final_source.index(marker)
    old_block = old_source[old_start:old_end].rstrip('\n')
    final_block = final_source[new_start:].rstrip('\n')
    require(old_block == final_block, 'relocated test module changed')
    require((old_source[:old_start] + old_source[old_end:]).rstrip('\n') == final_source[:new_start].rstrip('\n'), 'production bytes changed during test relocation')
    relocation = {'mechanical_test_module_relocation': True, 'production_bytes_equal_except_EOF_LF_separators': True,
                  'test_module_bytes_equal_except_EOF_LF_separators': True,
                  'pre_move_sha256': sha(pre_move), 'final_export_sha256': sha(root / 'crates/datasets/src/export.rs'),
                  'v1_helper_sha256': sha(v1_helper), 'v1_result_sha256': sha(v1_result)}
    # These are inspections of parent-executed logs; no tests are rerun here.
    parent_logs = {}
    for name, count, ignored in [('timestamp-workspace-tests-01.log', 1316, 56),
                                 ('timestamp-msrv-core-01.log', 451, 0),
                                 ('timestamp-msrv-compat-final-01.log', 4, 0),
                                 ('timestamp-msrv-dataset-final-01.log', 20, 0)]:
        log = evidence / name
        groups = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log.read_text())
        totals = {'passed': sum(int(g[0]) for g in groups), 'failed': sum(int(g[1]) for g in groups),
                  'ignored': sum(int(g[2]) for g in groups), 'result_groups': len(groups)}
        require(totals['passed'] == count and totals['failed'] == 0 and totals['ignored'] == ignored, 'parent retained log totals: ' + name)
        parent_logs[name] = {'sha256': sha(log), **totals}
    audit_after = evidence / 'timestamp-audit-after-01.json'
    security = json.loads(audit_after.read_text())
    require(security['vulnerabilities']['found'] is False and security['vulnerabilities']['count'] == 0 and security['warnings'] == {}, 'parent retained RustSec result')
    parent_logs[audit_after.name] = {'sha256': sha(audit_after), 'reported_vulnerabilities': 0, 'reported_warnings': 0}
    for name in ('timestamp-clippy-02.log', 'timestamp-docs-01.log', 'timestamp-deny-01.log'):
        parent_logs[name] = {'sha256': sha(evidence / name), 'scope': 'retained_parent_log_hash_only'}
    frozen = {'scope': 'retained_receipt_inspection_only_no_cli_reexecution'}
    receipt_path = evidence / 'timestamp-frozen-store-read-02.json'
    if receipt_path.exists():
        inspect_path = evidence / 'timestamp-frozen-inspect-final-01.json'
        snapshot_path = evidence / 'timestamp-frozen-store-before-01.json'
        receipt = json.loads(receipt_path.read_text())
        inspection = json.loads(inspect_path.read_text())
        require(sha(inspect_path) == receipt['inspect_output_sha256'], 'retained inspector receipt hash')
        require(receipt['reader_source_sha256'] == sha(root / 'crates/datasets/src/export.rs'), 'final reader source binding')
        require(sha(snapshot_path) == receipt['before_snapshot_sha256'], 'frozen inventory receipt hash')
        require(receipt['verified_rows'] == inspection['verified_rows'] == 2666, 'frozen rows')
        require(receipt['decision'] == inspection['manifest']['quality']['decision'] == 'validated-not-qualified', 'frozen decision')
        require(inspection['admitted'] is False and inspection['manifest']['quality']['supported_profiles'] == [], 'frozen admission')
        require(len(receipt['all_profiles_refused']) == 5 and all(p['exit'] == 1 for p in receipt['all_profiles_refused']), 'retained profile denials')
        require(receipt['original22files_hash_size_mtime_ctime_mode_unchanged'] is True, 'retained immutability assertion')
        frozen.update({'present': True, 'receipt_sha256': sha(receipt_path), 'inspect_sha256': sha(inspect_path),
                       'before_snapshot_sha256': sha(snapshot_path), 'verified_rows': 2666, 'admitted': False,
                       'decision': receipt['decision'], 'five_profile_denials_recorded': True,
                       'original_22_file_immutability': 'reported_by_parent_receipt_not_independently_reexecuted'})
    else:
        frozen['present'] = False
    require(start_hashes == {str(p.relative_to(root)): sha(p) for p in watched}, 'input bytes changed during independent audit')
    print(json.dumps({'schema': 'marketfeed.timestamp_independent_audit.v2', 'passed': True,
                      'helper_sha256': sha(Path(__file__)), 'runtime': {'executable': sys.executable, 'version': platform.python_version()},
                      'scope': 'independent calendar/lock/contract verification; no Rust tests, builds, workloads, network or economic runs',
                      'calendar': {'legacy_parse_vectors': 302, 'accepted_integer_gregorian_epochs': checked,
                                   'also_python_datetime_epochs': stdlib_checked, 'nanosecond_strings_and_floor_cases': 17,
                                   'rejected_vectors': 'not independently executed; reviewed Rust boundary separately'},
                      'lock': {'unchanged_complete_registry_records': 263, 'removed': [list(p) for p in removed], 'added_registry_records': 0},
                      'existing_contract_files_unchanged': 17, 'contract_file_sha256': contract_hashes,
                      'input_sha256': start_hashes, 'frozen_store_receipt': frozen,
                      'module_relocation': relocation, 'parent_executed_evidence_inspection': parent_logs,
                      'qualification': False, 'owner_workspace_msrv_test_status': 'not asserted by this audit'}, indent=2))


if __name__ == '__main__':
    main()
