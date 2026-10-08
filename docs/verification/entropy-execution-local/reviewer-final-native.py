"""Directly check small final-main native filters without rebuilding."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

out = Path('E:/persarb/_tmp/entropy101-validation')
deps = Path('E:/persarb/nautilus_trader/target/nextest/deps')
peer = deps / 'entropy_account-69bad49fe9dc76e8.exe'
ws = deps / 'websocket-6b79a96a3b9ad5a0.exe'
cases = [
    ('private-funds', peer, 'newer_private_funds_decline_limits_new_risk_before_http_refresh', False, 2),
    ('owned-cancel', peer, 'execution::owned_cancel_uses_exact_cached_order_and_dedicated_io_asset', True, 1),
    ('ordinary-subscribe', ws, 'test_subscribe_user_events', True, 1),
    ('ordinary-restoration', ws, 'test_subscription_restoration_tracking', True, 1),
    ('ordinary-reconnect', ws, 'test_request_reconnect_replays_subscriptions', True, 1),
]
runs = []
identities = {str(binary): hashlib.sha256(binary.read_bytes()).hexdigest() for binary in (peer, ws)}
for name, binary, test_filter, exact, expected in cases:
    argv = [str(binary), test_filter, '--test-threads=1', '--nocapture'] + (['--exact'] if exact else [])
    completed = subprocess.run(argv, cwd='E:/persarb/worktrees/entropy-account', stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, timeout=45)
    log = out / f'reviewer-final-native-{name}.log'
    log.write_bytes(completed.stdout)
    match = re.search(rb'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', completed.stdout)
    assert completed.returncode == 0 and match and int(match[2]) == expected, completed.stdout.decode(errors='replace')
    runs.append({'name': name, 'argv': argv, 'exit_code': completed.returncode, 'passed': expected,
                 'failed': int(match[3]), 'ignored': int(match[4]),
                 'log_sha256': hashlib.sha256(completed.stdout).hexdigest()})
    print(json.dumps(runs[-1]), flush=True)
assert identities == {str(binary): hashlib.sha256(binary.read_bytes()).hexdigest() for binary in (peer, ws)}
(out / 'reviewer-final-native.json').write_text(json.dumps({'binaries': identities, 'runs': runs}, indent=2) + '\n', encoding='utf-8')
print('FINAL_NATIVE_SUBSET_PASS', sum(run['passed'] for run in runs))
