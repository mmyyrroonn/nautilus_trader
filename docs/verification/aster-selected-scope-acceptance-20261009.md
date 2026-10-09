# Aster selected-scope acceptance

Native issue 109 delivers conservative selected-instrument admission through the ordinary Aster
execution client and factory. This acceptance covers synthetic native peers and an actual installed
Windows wheel. The contract is in [the scope guide](aster-selected-scope-contract-20261009.md).
The Entropy application remains blocked on a separate Hyperliquid io startup reconciliation gap;
this document does not claim successful application two-leg execution.

The branch began at freshly fetched `main@fff2aa6808afb76acf274a6a972322b5ac34d830`.
The clean implementation was built with unchanged pre/post source fingerprints. Documentation and
evidence packaging commits are separate from that implementation and do not replace its identity.

## Frozen artifact identities

| Item                                    | Identity                                                           |
| --------------------------------------- | ------------------------------------------------------------------ |
| Implementation commit                   | `f5246dc34bf73f58dba8cf94bcabc7e4aac1144a`                         |
| Implementation tree                     | `4e1dcf22aef587d7675d8468d15a631f4a1f7ecb`                         |
| Source fingerprint SHA256               | `d7b7300a6eace724ce80b1e8cbbe930b7fd295d7ec0e00ff11913f560b812fcf` |
| Wheel SHA256                            | `da0cc0df4fc70cefe52c80ddb6c5514c1566c73a7cac32e460ca5d59298b72cf` |
| Embedded/installed native binary SHA256 | `62eaea08f84ae9fa7cc21e374d38fc8fc6b4d877d6a24c4e85919618de139138` |
| Generated Aster stub SHA256             | `3dc13e6d8cfca549f5d0396effbd3c14069075666f9b3cbb544332161fb179bf` |

The actual artifact is
`E:/persarb/_tmp/entropy109-wheel-nextest-20261009/nautilus_trader-2.0.0rc4-cp312-cp312-win_amd64.whl`.
It uses CPython 3.12.9, uv 0.12.6, Rust 1.98.0, maturin 1.15.0 and the `nextest` profile.
Normal strict installation selected the explicit native-proof and application execution interpreters.
The application's formal candidate lock and original workspace interpreter were not changed.

[Provenance](aster-selected-scope-local/native-provenance.json) records build inputs, features,
tools, clean source identities and all embedded binary/stub hashes.
[Manifest](aster-selected-scope-local/manifest.json) hashes raw commands, failures, reviews and
temporary reproduction procedures. [Commands](aster-selected-scope-local/commands.json) preserve
actual argv, directories, UTC intervals and exits. Large binaries are identified rather than committed.

## Actual validation

| Check                                                              | Actual result                                                                  |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| Aster, Binance and Hyperliquid library/integration regression      | 3093 passed, 12 existing ignored, zero failures                                |
| Final selected-scope native peer group within that suite           | 35 passed                                                                      |
| Independent final native executables                               | 35 selected-scope and 2 ordinary open/close cases passed                       |
| Installed Python account/execution/economic/config/stub regression | 242 passed                                                                     |
| Independent installed wheel ABI behavior                           | 14 checks passed; binary and 23 adapter stubs match archive/provenance         |
| Actual installed Aster factory/LiveNode/QueryAccount/teardown      | Passed without orders; independently rerun with unchanged source hashes        |
| Affected Aster/Binance library and test Clippy, `-D warnings`      | Passed                                                                         |
| Official stub generation                                           | Passed; affected Rust source bytes unchanged                                   |
| Scoped nightly Rust format and Python Ruff                         | Passed                                                                         |
| `make format` and `make pre-commit`                                | Could not launch: make unavailable, capture exit 127                           |
| Whole-workspace formatting invocation                              | Could not launch its oversized Windows argument list; scoped formatting passed |

The 3093 native cases comprise Aster library 358, ordinary execution 99 and selected peers 35;
Binance library 977, futures 233 and spot 147; Hyperliquid library 865, data 46, dispatch 40,
owned execution 92, ordinary execution 102, HTTP 46 and WebSocket 53. The twelve ignored tests are
pre-existing live/soak cases and one account-registration test. They were not executed or counted
as passing. Reviewer subsets and earlier overlapping reruns are not additional distinct cases.

The complete regression ran from 03:12:08 through 03:13:58 UTC on 2026-10-09. It did not enable
Python features. Official generation subsequently left all 24 scoped source inputs byte-identical;
only the new Python test's line endings were normalized by Ruff before the clean implementation
commit. Actual feature-enabled wheel tests cover the resulting PyO3 binding and generated stub.

The independent final Rust report is
[boundary validation](aster-selected-scope-local/reviewer-native-final-boundary-validation.md.txt).
[Wheel ABI review](aster-selected-scope-local/reviewer-wheel-abi-report.md.txt) verifies actual
site-packages imports, loaded signatures, immutable policy, native rejection paths, no-client getter
and exact installed bytes. It checks manifest consistency; the reviewer did not independently
recompute the Git tree.

The independently repeated ordinary Aster node used `reconciliation=True`, normal data/execution
factories and one ordinary `QueryAccount`. Its initial generation 4 diagnostic was degraded, and the
query produced generation 6 with Ready/trusted, live stream, verified mode, complete recovery and
no chronology debt. Explicit `SNDKUSD1-PERP.ASTER` quantity was `0`; original USD1 total/free were
`100`. Remote snapshot time stayed null. Normal stop/dispose returned the getter to `None`, left
zero active sockets and made zero order actions. Actual loopback requests, responses, frames and
before/after runner/verifier/peer hashes are retained in the local evidence directory. This read-only
installed-wheel run does not replace the native submit/refusal/fault peer tests.

## Preserved failures and boundaries

Initial compile, raw-message callback, Money precision, Clippy, formatting and peer-test failures
remain in the raw archive. Two initial peer assertions were corrected after checking native behavior:
an empty working-order set does not require all-orders history, and successful HTTP submission does
not invent an Accepted event before the actual private stream.

An actual recovered close/open ordering defect was first observed in the full regression. A bounded
diagnostic probe stopped on its sixth attempt's failure after five successful executions; it was
not a retry-until-pass acceptance. Original grouped recovery used unsorted hash-map iteration and
published the real close before its earlier open. The clean-main comparison, actual report sequence
and analysis are retained. The fix orders separated native bundles by actual time, adds reverse-OID
ordinary cases, and refuses ambiguous or late optional-scope history with sticky chronology debt.
No fees/trade IDs were deleted, test events sorted, engine state manually patched or Unknown debt
cleared to obtain acceptance.

The scope proof never establishes run ownership, durable cold Cache restoration, whole-account
flatness, historical PnL for arbitrary interleaved default history or actual funding completeness.
Existing dependency advisories in issues 88 and 107 remain deployment limitations. Windows kernel
isolation is not verified; numeric loopback peers are functional protocol tests.

Application issue 43's seven initial actual installed-native scenarios all failed before strategy
entry because Hyperliquid `generate_mass_status` explicitly refuses an io account scope. A further
fresh paired run confirmed zero entries/fills and retained traffic. This requires a separate native
dependency from the next latest main. Node reconciliation and account scoping were not disabled.
No real account, real order, transfer, leverage change or mainnet acceptance was performed.
