"""Run relevant frozen native units directly; never compile."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

binary = Path('E:/persarb/nautilus_trader/target/nextest/deps/nautilus_hyperliquid-1fcbc36810deefe5.exe')
out = Path('E:/persarb/_tmp/entropy101-validation')
runs = []
for name, test_filter in [
    ('scope-unit', 'execution_scope::tests::'),
    ('ingress-unit', 'prepared_private_ingress'),
    ('signed-unit', 'prepared_signed_binding_is_verified_before_queue_and_rejection_never_writes'),
    ('handler-unit', 'prepared_send_wait_does_not_block_private_raw_frame_processing'),
    ('funds-provenance-unit', 'latest_private_funds_are_separate_from_http_provenance'),
]:
    argv = [str(binary), test_filter, '--test-threads=1', '--nocapture']
    started = time.monotonic()
    completed = subprocess.run(argv, cwd='E:/persarb/worktrees/entropy-account', stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, timeout=90)
    log = out / f'reviewer-native-{name}.log'
    log.write_bytes(completed.stdout)
    content = completed.stdout.decode('utf-8', errors='replace')
    match = re.search(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', content)
    run = {'name': name, 'argv': argv, 'seconds': round(time.monotonic() - started, 3),
           'exit_code': completed.returncode, 'log': str(log),
           'log_sha256': hashlib.sha256(completed.stdout).hexdigest()}
    if match:
        run.update(status=match[1], passed=int(match[2]), failed=int(match[3]), ignored=int(match[4]))
    runs.append(run)
    (out / 'reviewer-native-lib-results.json').write_text(json.dumps({
        'binary': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
        'runs': runs}, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(run), flush=True)
    if completed.returncode or not match or int(match[2]) == 0:
        raise RuntimeError('Independent native library filter failed')
print('INDEPENDENT_LIB_SUBSET_PASS', sum(run['passed'] for run in runs), flush=True)
