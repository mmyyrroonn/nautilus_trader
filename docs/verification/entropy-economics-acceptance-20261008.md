# Native Entropy Economic Acceptance

Native issue 102 provides bounded actual economic reporting through the ordinary Hyperliquid
execution factory. This acceptance covers original public protocol evidence, credential-free
synthetic native peers and an actual installed wheel. It does not claim real private settlements,
application two-leg execution or mainnet acceptance; those remain application issue 43.

The branch started from freshly fetched main `511ff91d04983afb706eb20989be0b400e6299b9`.
The clean implementation commit below was built and installed with identical pre/post source
fingerprints and zero dirty files. Later commits only format documentation and package evidence;
they are not relabeled as the wheel's source.

## Frozen identities

| Item                                 | Identity                                                           |
| ------------------------------------ | ------------------------------------------------------------------ |
| Implementation commit                | `95c87d4e70d92ec31072a30df3d764d75172b7b3`                         |
| Implementation tree                  | `8ed0387e434187098309e22d52f2b7d900d55722`                         |
| Source fingerprint                   | `56238a5bd653594cb354d871c5c8c8415e7120b12e56891616ca5bda12b61149` |
| Wheel SHA256                         | `be8d46c2d0b5f197852086b8818b4a17172aa000416512d73893c6041c129aed` |
| Embedded and installed binary SHA256 | `bdb2c149ab91d12aa0462afc99106424232158f5f6197abbea533156ebf61c6d` |
| Hyperliquid generated stub SHA256    | `016760dd07b4e7498769088ce6a7ac85100ba3fcaf07a0abd33e8e51614d7cfd` |
| Cargo.lock SHA256                    | `a802008d4363de0a4e28dd3b14990d0d79f95ef8a89431c5936fc31e1215463a` |
| Python uv.lock SHA256                | `5bf016c9fa06ee5ecad7d3d346e148793a205bb186e56bfb5d30106a57be7a55` |

The controlled build uses CPython 3.12.9, uv 0.12.6, maturin 1.15.0, Rust 1.98.0 and the
`nextest` profile. The actual wheel is
`E:/persarb/_tmp/entropy102-wheel-nextest-20261008/nautilus_trader-2.0.0rc4-cp312-cp312-win_amd64.whl`.
Only the isolated native validation interpreter was changed. The application formal candidate
lock, wheel and interpreter were not changed by this issue.

[Local manifest](entropy-economics-local/manifest.json) hashes every archived acceptance artifact.
[Provenance](entropy-economics-local/native-provenance.json) preserves clean build identities,
features, toolchain, original command and all embedded binary/stub hashes.
[Installed identity](entropy-economics-local/installed-native.json) reports strict source binding.
[Public manifest](entropy-economics-public/manifest.json) maps fifteen original protocol captures
to unchanged data bytes, original URLs and actual capture intervals. The pinned SDK license is
also retained. Historical preparation notes are preserved and do not override the final
[API guide](entropy-economics-api-20261008.md).

## Acceptance criteria

1. **Official economic contract:** actual REST `userFunding` and `userNonFundingLedgerUpdates`,
   user/private funding and ledger streams, signed amounts, account ownership, source timestamps,
   weak identity, inclusive pagination and Unknown retention are documented from official sources.
   Requests bind the actual verified user and do not invent a DEX selector.
2. **Native observation/recovery:** normal startup and `QueryAccount` subscribe and recover within
   explicit finite pages, records, response bytes, elapsed time and requested historical window.
   Original envelopes and item lexemes precede monetary normalization. Attribution requires
   independently verified exact io metadata/account context and matching reader generation/epoch.
3. **Actual versus estimated cash:** exact string amounts feed separate funding and actual fee
   totals. Individual fees include negative rebates; builder fees are already included. No rate
   multiplied by current position, public funding prediction, additional commission or invented
   balance adjustment enters actual amounts. Raw ledger components are not fabricated trade income.
4. **Scope and diagnostics:** default/xyz/foreign/unknown assets, wrong owner, unsupported mode,
   collateral and numeric monetary literals remain Unknown. Exact duplicates, changed payloads,
   weak replay/REST overlap and stream gaps remain visible. Funds trust stays independent of valid
   metadata attribution; failure or source replacement revokes context.
5. **Bounded native report:** one exclusive checkpoint contains durable raw observations, local
   immutable receipts, report output and consumed IDs. No missing-field `FundingSettlement`,
   caller ACK, silent eviction, generic all-account ledger or private transfer/borrowing action.
6. **Consumer/restart proof:** source phase A and native report phase B are distinct atomic writes.
   Restart before B retains pending observations; restart after B restores identical receipts and
   exact totals. Coverage is finite, empty is not zero lifetime cash and incomplete recovery stays
   diagnostic. Missing initialized checkpoint, tamper, capacity and filesystem failure fail closed.
7. **Actual native peers:** positive/negative/explicit-zero cash, tiny decimals, distinct events at
   the same time, 500-row inclusive overlap, saturation, weak multiplicity, conflict before/after B,
   failed user/mode/token verification, malformed fills, real native fee application, restart and
   consumer write failures are exercised through normal native factories/engines.
8. **Installed Python delivery:** generated optional policy and three normal factory methods work
   with actual installed config/factory/LiveNode builders; legacy constructor suffix/defaults and
   existing execution/account interfaces remain compatible. The factory lifetime, exclusive lease,
   invalid policies and source-bound binary/stub identity are independently checked.

## Actual local validation

| Check                                                         | Result                                                                       |
| ------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Full Hyperliquid library and integration tests                | 1,244 passed; 12 existing ignored, not executed.                             |
| Normal account/owned peers within that suite                  | 92 passed, including 22 new economic cases.                                  |
| Independent direct native executable subset                   | 41 passed; zero failures or ignored.                                         |
| Installed Python account/execution/economic/config/stub suite | 209 passed in 18.38 seconds.                                                 |
| Independent installed builder/ABI/policy/lifetime subset      | 51 passed; zero failures or ignored.                                         |
| Production library Clippy with `-D warnings`                  | Passed.                                                                      |
| Test-target Clippy with `-D warnings`                         | Blocked by existing Windows persistence/parquet.rs documentation lints.      |
| Official stub generation and Ruff                             | Passed; 44 generated stub files formatted with LF, one adapter stub changed. |
| Nightly Rust format check                                     | Passed.                                                                      |
| Authored Markdown and table checks                            | Passed after required table padding.                                         |
| `make format` and `make pre-commit`                           | Could not launch: make unavailable; capture wrapper exit 127.                |

The full 1,244 cases comprise library 865, data 46, dispatch 40, owned peers 92, execution 102,
HTTP 46 and WebSocket 53. The twelve ignored cases are eleven pre-existing live/soak cases and
one pre-existing account-registration test with its approximately thirty-second timeout.
Focused reviewer subsets are additional executions, not additional distinct full-suite cases.
Exact commands, UTC intervals, actual exits and full output are in
[commands.json](entropy-economics-local/commands.json) and the hashed raw files.

The native command did not enable the Python feature. Twenty-four final validation inputs match
the actual native tested bytes exactly. The official generator separately changed three Python
wrapper doc comments/formatting and emitted the new stub; the final actual wheel validates those
feature-enabled bytes. [Binding inputs](entropy-economics-local/binding-inputs.json) records this
distinction rather than claiming all twenty-five original inputs are byte-identical.

Independent [native review](entropy-economics-local/reviews/reviewer-native-final.txt) and
[wheel review](entropy-economics-local/reviews/reviewer-wheel-final.txt) retain actual input,
executable, installed binary and twenty-three adapter stub identities and selected executions.
The whole repository pre-commit suite is not claimed to pass when make cannot launch; the actual
available equivalent checks and unchanged Windows Clippy limitation are explicit above.

## Preserved failures and practical limits

Raw evidence includes initial compilation errors, Windows read-only post-rename flushing failure,
the real position-changing fee-attribution failure, a real three-second historical quota deadline,
an intermediate peer compilation failure and reviewer input preflight refusals. The final fee fix
establishes independent metadata attribution while funds stay untrusted. The positive recovery
fixture explicitly declares a supported thirty-second budget without changing production quotas
or deadlines. The expected-origin fix rejects another reader generation even at the same epoch.
Formatting/check-command errors and their actual corrections are retained as well.

`persist_economics()` does not reopen execution, credit account funds, apply another commission or
restore a cold native cache. Native application and retention remain Unknown. Exact strong facts
already present in phase A can preserve an independently recovered current account proof; report
receipts or totals never authorize that decision. Unresolved weak/ledger/conflict facts may continue
to close readiness. A later conflict preserves immutable historical receipts/totals while exposing
Unknown identity, not currently spendable cash.

The consumer has explicit cumulative source/receipt/coverage/checkpoint limits and stops without
eviction when exhausted. It is a finite report rather than an incremental continuous ledger. WS
and HTTP acceptance bounds apply after existing transport reception; they are not a proof of a
smaller socket allocation or a global bounded process queue. Windows process restart/file flush
is exercised, but generic directory power-loss durability and adversarial rewriting of every owner
and checksum are not claimed. Real venue hash uniqueness/retention and actual private settlements
remain unobserved.

The unchanged dependency advisories tracked in native issues 88 and 107 remain deployment gates.
This issue changes neither dependency lockfile nor audit exclusions. Actual PR CI results are
recorded separately on the PR and issue; local success does not imply audit or mainnet acceptance.
