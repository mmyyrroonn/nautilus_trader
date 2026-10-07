# Python config readback acceptance

Date: 2026-10-07. Scope: [Issue 87](https://github.com/mmyyrroonn/nautilus_trader/issues/87).
These results are credential-free offline checks on Windows amd64 and CPython 3.12.9.

## Candidate identity

The controlled repository builder produced an actual installed wheel from a clean checkout:

- Code commit: `ab28d70ff4c65f15e9780af2669c2b5517a2f02f`.
- Code tree: `eecd33dfbda677c595c28cc78ccee4645792a1ea`.
- Native dirty count: **0**; pre/post-build source fingerprints match.
- Source fingerprint: `804bd26c20a3a057806b125caa4e9a23b34f0045d08c9a8a4210cb0e7e95df79`.
- Wheel: `nautilus_trader-2.0.0rc4-cp312-cp312-win_amd64.whl`.
- Wheel SHA-256: `990bd128098d6766fb42c3593754bf6d2a8af3371beeef0d649b6c82f860d69c`.
- Provenance SHA-256: `0debc808a889d7343e7d31fe308701f3ffe93dd4c64f58c2f6dccb4742ea846c`.
- Installed binary SHA-256: `cd0506545b06036185ccd92eff16d3a30f42d333a8b218a5c71b10d9748d937a`.

Builder: `scripts/adapter-evidence/build_native.py`, locked/offline `nextest` profile, Rust
1.98.0, maturin 1.15.0 and uv 0.12.6. The application installer verified this exact wheel,
embedded binary/stub hashes, source fingerprint and Ondo shutdown diagnostics capability.
The validation environment is `E:\persarb\worktrees\issue87-config-contracts\.venv`.
This report is a later documentation commit; the tested implementation is the code commit above.

Local artifacts are under `E:\persarb\.backpack-artifacts\issue87-config-contracts`:
`wheel/native-provenance.json`, `installed.json`, `generator-junit.xml` and `runtime-junit.xml`.
The wheel is an offline acceptance candidate; a published release and Linux validation are separate.

## Accepted contracts

- Ondo's generated factory stub now includes `production_shutdown_diagnostics`. An unbound native
  factory returns `None`, with no account session opened.
- `OndoExecutionClientConfig.execution_envelope` returns an optional frozen envelope snapshot.
  All 18 non-sensitive envelope fields have read-only properties. Eight financial fields return
  `decimal.Decimal`; tests compare their complete decimal tuples, including scale, using distinct
  exact inputs. Identifiers, sides, counts, deadlines and the flat-start requirement also round-trip.
  The existing constructor validation and immutable native ceilings remain active.
- Aster data and execution configs expose read-only `has_proxy_url` booleans. The raw `proxy_url`
  property is removed. Python representations and validation errors do not reveal synthetic proxy
  authentication or signing inputs, and both native config `Debug` implementations redact proxies.
  A native regression proves the underlying transport configuration still receives its proxy.
- Blockchain's generated constructor retains keyword-only order from `allowed_token_pairs` through
  `verification`. DataActor, ExecutionAlgorithm and Strategy configs expose `**_kwargs: typing.Any`
  with no default, matching native keyword capture; arbitrary keyword values remain accepted.
- The four historical scanner correspondence entries are resolved separately: the exact
  `rust_decimal::Decimal` path maps to the existing Decimal binding; IB's two SymbologyMethod
  annotations and Ondo's envelope annotation use explicit Rust imports. Nondefault native Decimal
  and enum readback passed. These entries were scanner limitations, not evidence of API defects.
  Arbitrary qualified domain homonyms still fail closed; no adapter whitelist was added.
- Current main introduced a genuine additional Backpack gap in its optional
  `economic_state_directory` constructor parameter. Its missing getter is now present. A nondefault
  path round-trips read-only without creating the directory. This is independent of the already
  accepted exported-wrapper scanner correction in PR 83.

Generated `...` establishes default **presence**, not literal equality. Existing negative fixtures
still detect missing constructors/getters, required Optional parameters, order/kind drift, ambiguous
exports, incompatible types, constant readback and unequal concrete defaults. Native runtime checks
separately verify exact source defaults where available; this work makes no broader claim about
elided defaults across all stubs. Keyword-only required parameters cannot suppress preceding
positional defaults. The same generator correction updates `ParquetDataCatalog.extend_file_name`.

## Results

- Full `python/tests/unit/test_generate_stubs.py`: **145 passed**, zero failures/errors/skips.
- Installed-wheel config contracts plus existing common/example config tests: **37 passed**,
  including all 15 new readback/privacy/metadata cases; zero failures/errors/skips.
- Aster/Ondo native config tests with the Python feature: **34 passed**.
- Ondo production-envelope/lifecycle unit tests with the Python feature: **24 passed**.
- Full exported config source inventory: zero diagnostics; the inventory assertion remains active.
- Scoped nightly Rust formatting, Ruff lint/format, PyO3 public names/conventions and
  `git diff --check`: passed.

The initial current-source run on the older installed wheel was **136 passed / 4 failed**.
Its failures included the retained Aster/Ondo contracts, the new Backpack directory getter and two
new Backpack methods absent from that old binary. The clean rebuilt wheel resolves those results.
The historical **137 passed / 3 retained failures** in Issue 72 remains historical evidence.
There are no retained generator failures in this candidate and no test was skipped or weakened.

Reproduction, using the installed candidate interpreter from outside the Python source directory:

```powershell
$py = 'E:\persarb\worktrees\issue87-config-contracts\.venv\Scripts\python.exe'
$root = 'E:\persarb\worktrees\issue87-config-contracts'
Set-Location E:\persarb
& $py -m pytest --noconftest -p no:cacheprovider -o addopts='' -o pythonpath='' `
    --import-mode=importlib "$root\python\tests\unit\test_generate_stubs.py" -q
& $py -m pytest --noconftest -p no:cacheprovider -o addopts='' -o pythonpath='' `
    --import-mode=importlib "$root\python\tests\unit\test_config_readback_contracts.py" `
    "$root\python\tests\unit\common\test_configs.py" `
    "$root\python\tests\unit\trading\test_example_configs.py" -q
```

## Independent repository hook limitations

The full repository hook gate is **not claimed green**:

- `make` is unavailable on this Windows host. The full `prek 0.5.0 run --all-files` attempt fails
  when `python-test-collection` launches `make pytest-collect-fast`.
- The full `cargo +nightly fmt` invocation exceeds the Windows command-line length limit. Scoped
  formatting/checks cover every changed Rust file. Formatting unrelated pre-existing files was
  reverted instead of adding it to this change.
- The global testing-conventions hook reports 15 pre-existing `#[test]` uses in Backpack/Ondo
  sources and tests, including `tests/python_loopback.rs`, `tests/python_account.rs`,
  `tests/execution_client.rs` and Ondo `tests/http_client.rs`. These are present in the original
  `7ccfbd7426` base and are not the new config regression, which uses `#[rstest]`.
- Git Bash initially used the Windows Python alias and backslash-separated ripgrep paths. A local
  venv `python3` alias and a slash path separator fix the tool environment. The complete PyO3
  conventions hook then passes, including its secret-safe error-helper check.

These limitations remain separate from the passing generator and installed-wheel contracts.
No venue connection, credential discovery, live order or mainnet operation occurred.
