# Entropy io account acceptance, 2026-10-08

Native issue [100](https://github.com/mmyyrroonn/nautilus_trader/issues/100) delivers an explicit
**read-only** io account view. It does not enable Entropy trading, establish a funded live account,
or replace the bounded execution and economic receipts required by issues 101 and 102.

## Baseline and candidate

Implementation began at the then-current native main `5dca5481db9d602ec4f250696830e7173f00dd31`,
after application issue 42 merged through [PR 45](https://github.com/mmyyrroonn/Nautilus-Perps/pull/45).
The independent worktree is `E:\persarb\worktrees\entropy-account`, branch `feature/entropy-account`.
Both original dirty checkouts remain intact.

The wheel was built from clean source commit `f9b1dda025c06f1306b7f1a66ed7bf731ddc1325`,
tree `b541ba11a2937da611678cd48f2f14c071dd9a80`. Evidence documents are added afterward and do not
change that candidate's identity. The verified pre/post source fingerprint is
`d080b1f9e9eab2c79625946e8ed4a816e72366913f0086fad391277a525ef29c`.

| Artifact | Identity |
| --- | --- |
| Wheel | `nautilus_trader-2.0.0rc4-cp312-cp312-win_amd64.whl` |
| Wheel SHA-256 | `ba8aba6499f45bb69627f644629cecdd110045b6e342e521de8fbc4d362a2b34` |
| Installed native object SHA-256 | `1c46aebbeb6e99c8adb2af3fdfdbb009a4a0dcb20ef42973a1db9ab4734d8296` |
| Generated Hyperliquid stub SHA-256 | `dafda61f40591fb3580d37b50772dcc5f16b60e23f620d816413a9fb82de9c83` |
| Profile | Existing `nextest` profile, local validation candidate; not a published release |
| Interpreter | `E:\persarb\worktrees\entropy-account\.venv\Scripts\python.exe`, CPython 3.12.9 |
| Tools | uv 0.12.6, rustc 1.98.0, maturin 1.15.0 |

The controlled build completed at `2026-10-08T11:28:41.022237+00:00`. Source binding, wheel ABI,
embedded binaries/stubs, and installed native objects were independently checked by the installer.
The selected interpreter used the exact wheel through an explicit local override; the application's
existing formal candidate lock and environment were not promoted to this development wheel.
See [native provenance](entropy-account-local/native-provenance.json),
[installed identity](entropy-account-local/installed-native.json), and
[raw artifact manifest](entropy-account-local/manifest.json). The
[independent review](entropy-account-review-20261008.md) returned PASS for this read-only scope.

## Delivered facts and boundaries

The [official contract research](entropy-account-contract-20261008.md) distinguishes venue facts
from adapter policy. Fourteen public documentation/SDK sources were captured and rehashed, without
querying an actual user. Raw sources remain at `E:\persarb\_tmp\issue100-official-contract-20261008`;
their URLs, original capture times, byte lengths, and hashes are in
[public provenance](entropy-account-public/provenance.json).

The supported scope requires a direct verified user matching the signer, dedicated AccountId,
explicit `dex="io"`, canonical USDC, exact `disabled` abstraction, and legacy abstraction `false`.
Standard accounting is an explicit inference from the reviewed mode/SDK contract. Other modes,
agent/vault/subaccount routing, other collateral, and cross-margin io positions are unsupported.

Full `marginSummary` provides isolated-inclusive equity and used margin. Raw balance, equity,
withdrawable, used, free, signed positions, and cross-only maintenance are separate exact facts.
Negative equity/free is preserved. Total isolated maintenance remains unknown; no fictional
margin balance is emitted. Default perpetual and spot wealth cannot fund this io view.

Trust needs complete scoped REST verification, all private subscription acknowledgements, matching
full io WS positions/collateral, and the actual native connection epoch. Reader receipt time and
optional venue WS source time remain distinct. Every observation checks age, current transport,
and epoch. Failed refreshes retain prior facts while revoking trust and observed flatness.
Reconnect requires new private-stream evidence and an explicit fresh `QueryAccount`.

All io submit/modify/cancel/list/batch and staged actions remain refused, as do legacy global
order/fill/position/mass reconciliation paths. Global private execution reports cannot contaminate
the dedicated io account. `flat` describes observed positions, not pending orders, funds, or
permission to trade. A detached factory snapshot is diagnostic evidence, not a durable permission.
Rust callers must use `HyperliquidExecutionClientFactory::new()` or `::default()`; the old unit
literal/const construction changes. Python construction and the 18 existing positional arguments
remain compatible. One factory observes its most recently created client.

## Validation

| Check | Observed result |
| --- | --- |
| Full Hyperliquid library/integration suite | 1076 passed, 12 live tests ignored |
| Final owned native HTTP/WS peer suite | 46 passed in 1.20 s |
| Independent normal Rust factory peer case | 1 passed in 0.22 s |
| Installed-wheel Python config/readback/stub regression | 170 passed in 21.19 s |
| Production-only Clippy with `-D warnings` | Passed |
| Scoped nightly rustfmt, Ruff, Git whitespace check | Passed |
| Native Python stub generation | Completed; only seven necessary Hyperliquid stub lines changed |

The synthetic peer drives normal native execution clients and the normal execution factory with
actual HTTP/WS connections. It validates default/spot wealth versus empty io, isolated accounting,
negative facts, unsupported modes, wrong identities/collateral, missing fields, stale sources,
acknowledgements without current state, silence, physical reconnect, delayed/conflicting positions,
mode/collateral changes, wrong QueryAccount IDs, and global order/fill report isolation.
Valid cached io order attempts produce native denials with zero exchange writes. Shutdown leaves
zero active peer connections and revokes the newest snapshot; previously returned copies stay detached.
Each peer uses explicitly synthetic identity and data. It does not establish live venue behavior.

Python validation uses the installed wheel and normal `LiveNode.builder().add_exec_client().build()`.
A valid io configuration builds successfully without running the node or inventing an account proof.
Four invalid configurations fail with their specific scope/bounds/account-ID reasons. Frozen readback,
opaque signing inputs, empty-factory diagnostics, and legacy positional order are checked.

The relevant commands were:

```powershell
cargo test -p nautilus-hyperliquid --lib --tests --profile nextest --locked --offline --config "build.warnings='warn'"
cargo test -p nautilus-hyperliquid --test entropy_account --profile nextest --locked --offline --config "build.warnings='warn'"
cargo clippy -p nautilus-hyperliquid --lib --profile nextest --locked --offline --config "build.warnings='warn'" -- -D warnings
# NAUTILUS_STUB_PROFILE=nextest, PYTHONUTF8=1, exact CPython base directory on PATH
.venv/Scripts/python.exe python/generate_stubs.py
.venv/Scripts/python.exe scripts/adapter-evidence/build_native.py --python E:/persarb/worktrees/entropy-account/.venv/Scripts/python.exe --native-root E:/persarb/worktrees/entropy-account --output-dir E:/persarb/_tmp/entropy100-wheel-nextest-20261008 --profile nextest
.venv/Scripts/python.exe -m pytest --import-mode=importlib python/tests/unit/test_entropy_account_config.py python/tests/unit/test_config_readback_contracts.py python/tests/unit/test_generate_stubs.py -q
```

The [command record](entropy-account-local/commands.json) retains the installer invocation;
the underlying build command is also retained in provenance.
Checks are scoped to this change; no real credentials, venue orders, transfers, borrowing,
leverage changes, or account-mode actions were used.

## Corrected failures and outstanding checks

The first peer harness missed normal client startup and used a six-decimal Money display expectation;
both were corrected. Further peer expectations were corrected to require an actual new stream epoch
before reconnect reconciliation, distinguish REST age from WS receive age, and expect failed startup
when subscription ACKs/full state are missing. Earlier logs remain in the artifact manifest.
The normal factory test was added after the full adapter run, so the final peer count is 46 versus
45 in that earlier adapter run; the two totals are reported separately.

Adapter regressions exposed public WS reconnect event-order compatibility and a logging test's
undrained subscription error. These were fixed before the successful full regression. Public data
clients keep `Reconnected` first and do not receive private account proof messages.

`make format` and `make pre-commit` were attempted but cannot execute because this Windows host has
no `make`. Equivalent scoped formatting/lint checks are recorded; this is not a full pre-commit pass.
Clippy including tests is blocked by three existing Windows `doc_markdown` findings in baseline
`crates/persistence/src/parquet.rs:656`. The production-only Hyperliquid check passed without disabling
Rust lint rules. Only Cargo's known MSVC import-library notice uses the recorded per-command warning
exception. Unrelated baseline code and workflow/rule files were not changed.

Stub generation first failed under the default Windows text encoding, then required the selected
CPython DLL directory after Conda paths were removed. Both environment causes were corrected.
The release stub bin's fat-LTO compilation was deliberately interrupted after a long optimization
phase; it is not reported as a successful release check. The existing nextest profile generated the
actual stubs and installed candidate. Ruff normalized generated line endings without editing types.

Actual private WS shape/timing, mode transitions, live io balances, real funding or trading
permissions, and combined execution/economics remain unobserved. Read-only live observation and
bounded entry/close acceptance are reserved for the later application issue 43 under its explicit
permissions. These gaps do not turn synthetic passes into live acceptance.
