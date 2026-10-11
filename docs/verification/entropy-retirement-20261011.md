# Entropy specialization retirement

Date: 2026-10-11.

Entropy's `io:` instruments are HIP-3 builder-deployed perpetuals supported by the existing
Hyperliquid adapter. Like `xyz:` instruments, they use common metadata discovery, asset ID
mapping, order submission/modification/cancellation, and DEX-scoped order/position queries.
A standard new HIP-3 instrument does not require a deployer-specific adapter.

The official [HIP-3 specification](https://hyperliquid.gitbook.io/hyperliquid-docs/hyperliquid-improvement-proposals-hips/hip-3-builder-deployed-perpetuals)
states that trading uses the unified HyperCore API. The
[asset ID specification](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids)
defines the builder-perpetual asset mapping. Before the specialization, commit `5dca5481db`
already loaded every perpetual DEX and routed submit, modify, cancel, and reconciliation
through this generic implementation.

This conclusion covers the common trading actions and order/position routing. The ordinary
client's `AccountState` remains its existing primary/default perp margin plus spot view;
it does not provide the retired specialization's independent io balance/margin proof or
attest to every DEX account mode.

The retired work implemented additional account proofs, execution admission, economic
receipts, journals, and bounded startup/recovery acceptance. These controls were project
requirements, not protocol prerequisites for connecting to Entropy. The retirement restores
the ordinary Hyperliquid client/configuration path; it does not claim a completed live trade
or prove a particular wallet's funding or permissions.

## Reversal scope

The local review branch `revert/entropy-specialization-20261011`, based on `be060164f2`,
reverses code, configuration, tests, Python interfaces, generated interface artifacts, and
current integration guidance introduced by these commits, in reverse dependency order:

| Commit       | Retired addition                                                           |
| ------------ | -------------------------------------------------------------------------- |
| `be060164f2` | Bounded io warm prerequisites.                                             |
| `5db2761590` | Sealed io startup source support.                                          |
| `aadad78fab` | Bounded warm io account recovery.                                          |
| `667f424e12` | Bounded fresh-flat io startup.                                             |
| `9ff73d1205` | Aster selected-scope admission used as an Entropy acceptance prerequisite. |
| `fff2aa6808` | Entropy economic receipts and recovery.                                    |
| `511ff91d04` | Entropy execution and owned recovery.                                      |
| `95fab86a26` | Scoped Entropy account proofs.                                             |

The generated Hyperliquid and Aster stubs are restored by reversing their original generated changes.
No new hand-written stub logic is introduced. The now-unused prepared WebSocket admission
infrastructure introduced for Entropy execution and its associated tests/dev-dependencies
are included in the reversal.

The reversal was initially prepared locally without commits or pushes. The user then
authorized committing, opening pull requests, and merging both repositories. Publication
status is tracked by the corresponding pull requests and the publication records in
[native #120](https://github.com/mmyyrroonn/nautilus_trader/issues/120) and
[application #43](https://github.com/mmyyrroonn/Nautilus-Perps/issues/43).
No new wheel was built or installed as part of this reversal.

## Retained work

- Existing generic HIP-3 discovery, execution, and reconciliation.
- `9644fd657e`: common Hyperliquid fast order-book subscription support and regression tests.
- The ordinary Aster adapter and the generic fill-chronology fix introduced alongside
  the Entropy selected-scope prerequisite in `9ff73d1205` (PR #110).
- The independent Windows socket2 getter compatibility fix in `crates/network/src/net.rs`
  introduced alongside `511ff91d04`.
- All pre-existing Backpack and Ondo work.
- All historical `docs/verification/entropy-*`, `io-*`, and Aster evidence, including the
  `.gitattributes` byte-preservation rules.

## Historical evidence

The preserved Entropy/io and Aster prerequisite contracts, acceptance reports, public/local receipts, frozen source
captures, and commands describe the implementation at their recorded commits. They remain
historical evidence only. Their dedicated policy/configuration APIs are retired and must not
be used as current integration instructions. The current usage guide is
[`docs/integrations/hyperliquid.md`](../integrations/hyperliquid.md#hip-3-builder-deployed-perpetuals).

## Issue disposition

On 2026-10-11, native issues #100, #101, #102, #109, #111, #113, #115, #117, and #120
and application issues #42 and #43 were updated with this retirement decision.
All eleven specialization issues are closed. The previously open
[native #120](https://github.com/mmyyrroonn/nautilus_trader/issues/120) and
[application #43](https://github.com/mmyyrroonn/Nautilus-Perps/issues/43) were closed
as `not_planned`; earlier completed records retain their historical status and text.
The mixed application epic #38 now continues Variational only. Application #26/#29
and independent Aster error-classification issue #119 remain open under their own scopes.

The initial issue updates distinguished the then-local, uncommitted reversal from remote
`main` and the installed wheel. Subsequent publication records track the authorized
commits and merges separately from the unchanged installed wheel. Exact selected working files,
SHA-256 manifests, tracked patches, and prior issue bodies are preserved under
`E:/persarb/archives/entropy-retirement-20261011`. The unmerged application execution
files and Hyperliquid warm-retry WIP were archived before targeted removal; Aster #119
working changes were retained.

## Verification

Source comparison confirms that Hyperliquid retains only the generic fast-book changes
relative to the pre-specialization baseline, with no dedicated Entropy account/execution/
economic APIs. The Aster selected-scope prerequisite and its Binance ingress observer
extension are also removed. The generated Hyperliquid and Aster stubs match their
pre-specialization artifacts.
The following local commands were run from
`E:/persarb/worktrees/entropy-retirement-native`. Cargo reused the existing
`E:/persarb/nautilus_trader/target` build cache and used `--offline --locked`.

```powershell
$env:CARGO_TARGET_DIR = 'E:/persarb/nautilus_trader/target'
$env:CARGO_BUILD_WARNINGS = 'allow'
cargo test --offline --locked -p nautilus-hyperliquid --lib --test data_client --test websocket --test exec_client --test http --test dispatch --test catalog
cargo +nightly fmt -p nautilus-hyperliquid -p nautilus-aster -p nautilus-binance -p nautilus-network -- --check
& E:/persarb/nautilus_trader/.venv/Scripts/ruff.exe check python/nautilus_trader/adapters/hyperliquid/__init__.pyi python/nautilus_trader/adapters/aster/__init__.pyi
& E:/persarb/nautilus_trader/.venv/Scripts/ruff.exe format --check python/nautilus_trader/adapters/hyperliquid/__init__.pyi python/nautilus_trader/adapters/aster/__init__.pyi
$env:PYTHONUTF8 = '1'
& E:/persarb/nautilus_trader/.venv/Scripts/python.exe python/generate_docstrings.py
$env:PYO3_PYTHON = 'E:/persarb/nautilus_trader/.venv/Scripts/python.exe'
cargo check --offline --locked -p nautilus-hyperliquid -p nautilus-aster --features nautilus-hyperliquid/python,nautilus-aster/python
cargo test --offline --locked -p nautilus-network -p nautilus-aster --lib --test exec_client -- --quiet
& 'E:/persarb/nautilus_trader/target/debug/deps/exec_client-b5c0f05c13e976ed.exe' test_reconnect_flat_after_real_close_preserves_trade_ids_and_fees_once --exact --nocapture
cargo test --offline --locked -p nautilus-network --lib -- --quiet
& E:/persarb/nautilus_trader/.venv/Scripts/python.exe -B scripts/check-markdown-tables.py docs/integrations/hyperliquid.md docs/verification/entropy-retirement-20261011.md
git diff --check
git diff --cached --check
```

Hyperliquid results: 753 library tests, 46 data-client tests, 40 dispatch tests,
102 execution-client tests, 46 HTTP tests, and 53 WebSocket tests passed:
**1,040 passed, 1 ignored**. The catalog target has no tests without its optional feature.
This includes the existing generic HIP-3 routing and the preserved fast-book behavior.
The affected-crate nightly format check, stub Ruff checks, and whitespace checks passed.
The official docstring generator succeeded with **0 comments updated**. This is a
docstring check, not a full stub generation; the restored generated stubs were instead
compared directly with their original `5dca5481db` artifacts.
The Hyperliquid and Aster Python-feature binding compile check passed.

Aster's initial complete reversal passed **326 library tests** and **97 execution tests**,
but failed `test_reconnect_flat_after_real_close_preserves_trade_ids_and_fees_once` at the
open-position cache assertion. This was investigated before publication rather than
accepted as an unrelated baseline failure.

Clean-source comparisons established the cause:

- Pre-specialization `5dca5481db`: the original test passed 7 of 10 independent test
  processes and failed 3 at the same flat-position assertion.
- Pre-reversal main `be060164f2`: both parameterized regression cases passed.
- PR #110 had also fixed ordinary Aster recovery by sorting real trades and order bundles
  chronologically. Reversing that entire change removed this generic fix. Hash iteration
  could then deliver a reduce-only close before its opening fill and leave the recovered
  position open.

The final reversal retains that generic sorting and its two regression cases, including
the case where order IDs oppose real fill chronology. It still removes selected-scope
admission and every Entropy-specific prerequisite. The defect existed before the
specialization, was fixed in main, and was reintroduced by the initial reversal; it must
not be described as a failure already present in the current main.

The clean worktree hashes, commands, logs, and all repeated outcomes are preserved in
`E:/persarb/archives/entropy-retirement-20261011/aster-baseline`. The initial main command
using `--exact` matched zero tests; a direct invocation after shared-target reuse was also
invalid as a main comparison. Both are excluded from the evidence above; the valid main
comparison rebuilt from its clean tree and executed both parameterized cases.

After retaining the fix, the focused two-case run passed. The complete Aster rerun
`cargo test --offline --locked -p nautilus-aster --lib --test exec_client -- --quiet`
passed **326 library tests and 99 execution tests (425 total, zero failures)**.
Source hashes were unchanged during both runs; affected-crate formatting and whitespace
checks also passed after the fix was retained.

The initial combined Cargo batch stopped before running network tests, so network was run separately.
The separate network library run passed **583 tests** with no failures or ignored tests.

The Markdown table normalization hook was run for both edited documents. The first
offline Markdown lint attempt lacked the npm dependency `uc.micro` (`ENOTCACHED`).
The dependency was subsequently fetched; `markdownlint-cli2@0.21.0` passed for both documents.

Both `make format` and `make pre-commit` were attempted before opening the pull request,
but this Windows environment has no `make` executable. The targeted format, Ruff,
docstring, table, and whitespace checks above are the available local checks; they are
not a claimed pass of the full pre-commit suite.

The first Cargo invocation encountered the repository's existing Windows MSVC import-library
notice under `build.warnings = "deny"`. The local `CARGO_BUILD_WARNINGS=allow` override matches
the existing Windows adapter build procedure in `scripts/adapter-evidence/build_native.py`;
Rust lint flags remain unchanged. The first docstring invocation used Windows' GBK default
and failed to read a UTF-8 Rust source; setting `PYTHONUTF8=1` resolved it. A workspace-wide
`cargo fmt --all -- --check` also exceeded the Windows command-length limit; the affected
crates were checked successfully with the targeted nightly command above.
