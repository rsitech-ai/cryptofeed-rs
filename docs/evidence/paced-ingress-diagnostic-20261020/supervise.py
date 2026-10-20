#!/usr/bin/env python3
"""Run each registered local arm once, with an external process-group deadline.

No network, exchange capture, strategy, or dataset admission. The harness verdict
is provisional until audit.py reconciles the stored bytes and outputs.
"""
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    repo, fixture, binary, output = map(lambda p: Path(p).resolve(), sys.argv[1:])
    plan_path = Path(__file__).with_name('plan.json')
    plan = json.loads(plan_path.read_text())
    assert sha(fixture) == plan['fixture_sha256'], 'registered fixture changed'
    output.mkdir()  # Never overwrite or silently rerun an attempt.
    execution = {
        'schema': 'marketfeed.paced_ingress_execution.v1',
        'plan_sha256': sha(plan_path), 'fixture_sha256': sha(fixture),
        'binary_sha256': sha(binary), 'build_profile': 'debug',
        'rustc': subprocess.check_output(['rustc', '--version'], cwd=repo, text=True).strip(),
        'platform': platform.platform(), 'machine': platform.machine(),
        'logical_cpus': os.cpu_count(), 'arm_order': plan['order'],
        'free_bytes_before': os.statvfs(output).f_bavail * os.statvfs(output).f_frsize,
        'source_sha256': {p: sha(repo / p) for p in (
            'crates/daemon/examples/paced_shared_feed.rs',
            'crates/recording/src/shared_feed.rs',
            'crates/recording/tests/shared_feed.rs',
        )},
        'supervisor_sha256': sha(Path(__file__)),
        'qualified_dataset': False,
        'independent_storage_audit_required': True,
    }
    (output / 'execution.json').write_text(json.dumps(execution, indent=2) + '\n')
    receipts = []
    for arm in plan['arms']:
        name = arm['name']
        mode = 'durable' if arm['durable'] else 'baseline'
        started = time.monotonic_ns()
        with (output / (name + '.stdout.json')).open('xb') as stdout, \
                (output / (name + '.stderr.log')).open('xb') as stderr:
            process = subprocess.Popen(
                [str(binary), str(fixture), mode, str(output / name), str(arm['delay_us'])],
                cwd=repo, stdout=stdout, stderr=stderr, start_new_session=True,
            )
            timed_out = False
            try:
                code = process.wait(timeout=plan['watchdog_seconds_per_arm'])
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, signal.SIGKILL)
                code = process.wait(timeout=5)
        receipt = {'arm': name, 'exit': code, 'watchdog_fired': timed_out,
                   'wall_ns': time.monotonic_ns() - started,
                   'stdout_sha256': sha(output / (name + '.stdout.json')),
                   'stderr_sha256': sha(output / (name + '.stderr.log'))}
        receipts.append(receipt)
        (output / (name + '.receipt.json')).write_text(json.dumps(receipt, indent=2) + '\n')
        print(json.dumps(receipt), flush=True)
        # Preserve every failed attempt. Do not try another normal arm after an
        # unexpected failure; the deliberately slow third arm must fail visibly.
        expected_exit = 1 if name == 'slow-failure' else 0
        if timed_out or code != expected_exit:
            break
    (output / 'receipts.json').write_text(json.dumps(receipts, indent=2) + '\n')


if __name__ == '__main__':
    main()
