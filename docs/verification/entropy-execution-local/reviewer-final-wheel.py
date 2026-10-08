"""Verify final wheel/installation/source identity and normal Python factory tests."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import zipfile

root = Path('E:/persarb/worktrees/entropy-account')
out = Path('E:/persarb/_tmp/entropy101-validation')
wheel_dir = Path('E:/persarb/_tmp/entropy101-wheel-nextest-20261008')
wheel = wheel_dir / 'nautilus_trader-2.0.0rc4-cp312-cp312-win_amd64.whl'

def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def git(*args):
    return subprocess.check_output(['git', *args], cwd=root).decode().strip()

provenance = json.loads((wheel_dir / 'native-provenance.json').read_text(encoding='utf-8-sig'))
installed = json.loads((out / 'installed-native.json').read_text(encoding='utf-8-sig'))
binding = json.loads((out / 'binding-inputs.json').read_text(encoding='utf-8-sig'))
inputs = json.loads((out / 'tested-inputs.json').read_text(encoding='utf-8-sig'))
identity = provenance['declared_native']
assert git('status', '--porcelain') == ''
assert git('rev-parse', 'HEAD') == identity['commit']
assert git('rev-parse', 'HEAD^{tree}') == identity['tree']
assert identity == provenance['build']['started_source'] == provenance['build']['finished_source']
assert identity['dirty_count'] == 0 and identity['dirty'] == []
assert identity['cargo_lock_sha256'] == sha(root / 'Cargo.lock')
canonical = {key: identity[key] for key in ['commit', 'tree', 'tracked_diff_sha256', 'untracked_sha256', 'cargo_lock_sha256']}
fingerprint = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
assert fingerprint == provenance['source_fingerprint_sha256']
assert provenance['source_binding'] == 'verified'
assert sha(wheel) == provenance['wheel']['sha256'] == installed['wheel']['sha256']
assert sha(wheel) == '99d6351fc0e8ac3322d97808c6dc5da5a01eb1981ad8184a7fc73927cade94a1'
assert installed['strict_source_binding_passed'] is True
assert sha(wheel_dir / 'native-provenance.json') == installed['native_provenance']['sha256']
assert sha(Path(provenance['declared_evidence']['path'])) == provenance['declared_evidence']['sha256']
for key, filename in [('builder_sha256', 'scripts/adapter-evidence/build_native.py'),
                      ('maturin_config_sha256', 'python/pyproject.toml'), ('python_uv_lock_sha256', 'python/uv.lock')]:
    assert provenance['build'][key] == sha(root / filename)

import nautilus_trader
import nautilus_trader._libnautilus as native

package = Path(nautilus_trader.__file__).parent
pyd = Path(native.__file__)
assert str(package).startswith(str(root / '.venv'))
assert str(pyd) == installed['installed']['native_module']
objects = []
stubs = []
with zipfile.ZipFile(wheel) as archive:
    for record in provenance['embedded_native_objects']:
        with archive.open(record['path']) as stream:
            actual = hashlib.file_digest(stream, 'sha256').hexdigest()
        assert actual == record['sha256'] == sha(package.parent / record['path'])
        objects.append(record)
    for record in provenance['embedded_adapter_stubs']:
        actual = hashlib.sha256(archive.read(record['path'])).hexdigest()
        assert actual == record['sha256'] == sha(package.parent / record['path'])
        stubs.append(record)
    hl = 'nautilus_trader/adapters/hyperliquid/__init__.pyi'
    assert hashlib.sha256(archive.read(hl)).hexdigest() == sha(root / 'python' / hl)

source_checks = []
for filename, expected in inputs['inputs'].items():
    actual = sha(root / filename)
    same = actual == expected['sha256']
    if not same:
        assert filename in binding['final_projection_inputs']
        assert actual == binding['final_projection_inputs'][filename]
        assert filename == 'crates/adapters/hyperliquid/src/python/factories.rs'
    source_checks.append({'path': filename, 'sha256': actual, 'default_input_matches': same})
for filename, expected in binding['final_projection_inputs'].items():
    assert sha(root / filename) == expected
result = {'source_commit': identity['commit'], 'source_tree': identity['tree'],
          'source_fingerprint_sha256': fingerprint, 'wheel_sha256': sha(wheel),
          'installed_pyd': str(pyd), 'installed_pyd_sha256': sha(pyd),
          'native_objects_verified': objects, 'adapter_stubs_verified': stubs,
          'source_checks': source_checks, 'python_projection': binding,
          'provenance_sha256': sha(wheel_dir / 'native-provenance.json')}
(out / 'reviewer-final-artifact.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
print(json.dumps({'artifact_verified': True, 'default_input_matches': sum(x['default_input_matches'] for x in source_checks),
                  'projection_deltas': sum(not x['default_input_matches'] for x in source_checks),
                  'verified_adapter_stubs': len(stubs), 'pyd_sha256': sha(pyd)}), flush=True)
argv = [sys.executable, '-m', 'pytest', '-q', '-o', 'pythonpath=', '--import-mode=importlib',
        str(root / 'python/tests/unit/test_entropy_execution_config.py'),
        str(root / 'python/tests/unit/test_entropy_account_config.py')]
started = time.monotonic()
completed = subprocess.run(argv, cwd=out, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
log = out / 'reviewer-final-python.log'
log.write_bytes(completed.stdout)
content = completed.stdout.decode('utf-8', errors='replace')
match = re.search(r'(\d+) passed in ([\d.]+)s', content)
assert completed.returncode == 0 and match and int(match[1]) == 27, content
result['python_tests'] = {'argv': argv, 'exit_code': completed.returncode, 'passed': int(match[1]),
                          'seconds': round(time.monotonic() - started, 3), 'log_sha256': sha(log)}
(out / 'reviewer-final-artifact.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
print(json.dumps(result['python_tests']), flush=True)
