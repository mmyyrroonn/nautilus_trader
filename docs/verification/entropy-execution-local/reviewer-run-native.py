"""Independently execute frozen native cases without building or reading credentials."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path('E:/persarb/worktrees/entropy-account')
OUT = Path('E:/persarb/_tmp/entropy101-validation')
PEER = Path('E:/persarb/nautilus_trader/target/nextest/deps/entropy_account-69bad49fe9dc76e8.exe')
LIB = PEER.with_name('nautilus_hyperliquid-1fcbc36810deefe5.exe')

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

manifest = json.loads((OUT / 'tested-inputs.json').read_text(encoding='utf-8-sig'))
verification = []
for name, expected in manifest['inputs'].items():
    path = ROOT / name
    actual = {'sha256': digest(path), 'bytes': path.stat().st_size}
    verification.append({'path': name, **actual, 'matches': actual == expected})
if not all(item['matches'] for item in verification):
    raise RuntimeError('Source identity differs from root tested-inputs.json')

filters = [
    ('factory-partials-close', 'factory_actual_partial_fills_fees_and_owned_ioc_close_update_engine_and_portfolio'),
    ('partial-cancel', 'partial_fill_then_cancel_preserves_actual_position_and_rejects_oversized_close'),
    ('owned-cancel', 'owned_cancel_uses_exact_cached_order_and_dedicated_io_asset'),
    ('unknown-restart', 'missing_ack_and_unknown_oid_retain_budget_and_restart_never_resends'),
    ('projection-restart', 'durable_actual_fill_does_not_claim_native_cache_restored_after_fresh_factory_restart'),
    ('private-funds', 'newer_private_funds_decline_limits_new_risk_before_http_refresh'),
    ('account-deadline', 'query_account_total_deadline_bounds_private_verification_and_preserves_unknown'),
    ('unsupported-private', 'unsupported_private_economic_frames_revoke_trust_before_new_risk'),
    ('economic-conflict', 'same_raw_trade_identity_with_changed_economics_is_a_conflict_even_after_terminal'),
    ('unprojectable-fill', 'contradictory_or_unprojectable_actual_fill_blocks_without_native_position'),
    ('double-spend', 'concurrent_orders_cannot_spend_one_balance_twice'),
]
result = {'peer': {'path': str(PEER), 'sha256': digest(PEER)},
          'lib': {'path': str(LIB), 'sha256': digest(LIB)},
          'source_inputs': verification, 'runs': []}
(OUT / 'reviewer-native-identity.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
for name, test_filter in filters:
    argv = [str(PEER), test_filter, '--test-threads=1', '--nocapture']
    started = time.monotonic()
    completed = subprocess.run(argv, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=150)
    log = OUT / f'reviewer-native-{name}.log'
    log.write_bytes(completed.stdout)
    content = completed.stdout.decode('utf-8', errors='replace')
    match = re.search(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', content)
    run = {'name': name, 'argv': argv, 'exit_code': completed.returncode,
           'seconds': round(time.monotonic() - started, 3), 'log': str(log), 'log_sha256': digest(log)}
    if match:
        run.update(status=match[1], passed=int(match[2]), failed=int(match[3]), ignored=int(match[4]))
    result['runs'].append(run)
    (OUT / 'reviewer-native-results.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({key: run[key] for key in ('name', 'exit_code', 'seconds', 'passed', 'failed', 'ignored') if key in run}), flush=True)
    if completed.returncode or not match or int(match[2]) == 0:
        sys.exit(1)
print('INDEPENDENT_NATIVE_SUBSET_PASS', sum(run['passed'] for run in result['runs']), flush=True)
