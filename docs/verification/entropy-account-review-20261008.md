# Independent Entropy account review, 2026-10-08

**PASS for native issue 100's read-only io account scope.** No blocking source or candidate-artifact
finding remains. The reviewed implementation is suitable for the user's authorized merge into the
fork's main branch, with the observation and compatibility limits below. This verdict does not
accept io execution, recovery, economic receipts, or a real-account deployment.

## Reviewed candidate

The production, tests, integration documentation, and generated stubs were reviewed at commit
`f9b1dda025c06f1306b7f1a66ed7bf731ddc1325`, directly based on main
`5dca5481db9d602ec4f250696830e7173f00dd31`. Before this report was created, independent identity
capture confirmed a clean checkout and matched the wheel's pre-build and post-build identities.
The later addition of acceptance documents does not change the reviewed production source.

| Identity | Independently verified value |
| --- | --- |
| Source tree | `b541ba11a2937da611678cd48f2f14c071dd9a80` |
| Source fingerprint SHA256 | `d080b1f9e9eab2c79625946e8ed4a816e72366913f0086fad391277a525ef29c` |
| Wheel SHA256 | `ba8aba6499f45bb69627f644629cecdd110045b6e342e521de8fbc4d362a2b34` |
| Installed native object SHA256 | `1c46aebbeb6e99c8adb2af3fdfdbb009a4a0dcb20ef42973a1db9ab4734d8296` |
| Generated Hyperliquid stub SHA256 | `dafda61f40591fb3580d37b50772dcc5f16b60e23f620d816413a9fb82de9c83` |
| Build profile | Existing `nextest` profile; CPython 3.12, Windows AMD64 |

The wheel is `E:/persarb/_tmp/entropy100-wheel-nextest-20261008/nautilus_trader-2.0.0rc4-cp312-cp312-win_amd64.whl`.
The imported native object was in the worktree's `.venv/Lib/site-packages/nautilus_trader/`, rather
than another checkout. All 23 embedded adapter-stub hashes were recalculated; the embedded
Hyperliquid stub also matched the committed source bytes. The provenance's `source_binding=verified`
and installed evidence's strict-source-binding result were checked against actual source, wheel,
and installed bytes, rather than accepted as standalone declarations.

## Source and safety conclusions

The io path obtains balances exclusively from an explicit actual-user `dex=io` request. It requires
the exact supported abstraction results, a direct signer matching that user, and canonical USDC
metadata resolved by token index. It does not inherit primary or spot wealth. Missing position
arrays, malformed quantities, unsupported margin modes, and unknown identities remain errors.
`balance`, `equity`, `used`, `free`, and `withdrawable` retain their distinct meanings. Exact raw
decimals, including negative equity/free, are preserved; native Money balances derive free by
checked subtraction without clamping. Isolated total maintenance is explicitly unknown.

Trust additionally requires acknowledged private subscriptions, complete correctly routed io
state, current transport availability and connection ownership, and bounded REST/receive ages.
HTTP completion checks its captured version and native connection epoch. Private frames and ACKs
carry their originating reader epoch and are filtered again by execution. Socket-callback receive
time is retained through queues. Optional WS source time expires independently; absent source time
remains null. The initial private snapshot is checked against newly verified metadata, including
zero-size positions, and against REST coin/signed-size facts. Floating valuation and margin fields
are deliberately not required to match atomically between REST and WS.

These material review findings were resolved before this verdict:

- Initial REST-flat/private-nonflat evidence can no longer publish a trusted flat account; unknown
  zero-size private coins cannot bypass metadata verification. A conflict needs matching fresh REST
  evidence before recovery.
- Private failures clear complete-stream evidence. ACKs alone, old source timestamps, stale receive
  timestamps, and old connection epochs cannot restore trust. Failed HTTP refreshes retain previous
  facts while revoking trust and flatness.
- A wrong `QueryAccount.account_id` is rejected before requests or emission. io requires a dedicated
  AccountId matching the execution core; applications must keep account IDs unique within a node.
- Global order/fill messages are ignored for the read-only io account. Manual AccountState emission
  and legacy order/fill/position/mass reports are refused, preventing primary/spot activity from
  entering its dedicated ledger through those routes.
- io submit/list/bracket/modify/cancel/cancel-all/batch-cancel paths are refused, and staged restoration
  cannot dispatch io orders. Default execution also refuses io writes. The earlier asynchronous
  admission/queued-send proposal was removed: issue 101 must independently establish bounded
  execution and recovery before enabling writes.

The normal factory exposes the same live diagnostic state as its created client, returns detached
JSON, and revokes trust after shutdown. It tracks the most recently created client; retain a factory
per observed client and verify address/dex in its result. The stateful Rust factory requires
`::new()` or `::default()`; its old unit-struct literal and const construction are incompatible.
Python factory construction and all 18 legacy configuration positional parameters remain compatible;
the two new options are appended.

## Independent verification

Selected native cases were executed directly, avoiding shared Cargo build locks. The peer tests use
owned loopback HTTP/WS sockets and synthetic identities; their stop helper asserts zero exchange/post
writes and closed sessions. No real account, key file, or external private endpoint was accessed.

| Independently executed coverage | Result |
| --- | --- |
| Negative free/equity, zero and positive funds, actual SubmitOrder denial | 4 passed |
| Foreign WS user/dex | 2 passed |
| Initial position conflict and unknown zero-size coin | 2 passed |
| Physical reconnect and fresh scoped reconciliation | 1 passed |
| Global execution stream cannot emit io ledger events | 1 passed |
| Wrong native AccountId query | 1 passed |
| Private position conflict and restoration | 1 passed |
| Floating REST/WS financial values | 1 passed |
| Optional WS source expiration despite fresh receive time | 1 passed |
| Normal Rust factory, exact detached JSON, shutdown trust revocation | 1 passed |
| Installed-wheel Entropy Python configuration/factory cases | 10 passed in 0.44s |

The final 13-case peer rerun used binary SHA256
`e2a9728593d2ddc8a48f4ee2f77577d59ab086e347940a2cdd7f5147b4403989`.
The additional factory case used the newer 46-case peer binary SHA256
`8425e7c5618e5c47aa4292b0c44f82ee9b8bb8d8efef10e4cdec97be07608ca9`.
The source-age unit used library-test binary SHA256
`1d130100c44c9fbf0b71ae5a23e241b31a5f04e2db5ed8a50a8e06b019dfef5d`.
Each peer group was run as `<peer-binary> <case-name> --test-threads=1`; the factory case and source-age
unit were run with their exact names and `--exact`.

The installed-wheel Python command was:

```powershell
.\.venv\Scripts\python.exe -m pytest python/tests/unit/test_entropy_account_config.py -q -o 'pythonpath=' --import-mode=importlib
```

It covers frozen/credential-opaque readback, legacy positional construction, an unbound factory,
a supported configuration through the ordinary LiveNode builder, and four invalid configurations
through that same builder with specific error-message assertions. The valid builder control prevents
an unrelated builder failure from making all rejection cases appear successful. These are build-time
checks; the Python tests do not claim to run a live private stream. The private stream and nonempty
factory JSON path were separately exercised by the actual native loopback test.

The first Python invocation used pytest's default source-path import behavior and failed during
conftest import because the source package shadowed the installed native package. The corrected
installed-package invocation above passed. Both logs are retained; the failed invocation is not
counted as a candidate test pass.

Reviewer evidence is in `E:/persarb/_tmp/entropy100-validation/`:

| File | SHA256 |
| --- | --- |
| `reviewer-factory-final.log` | `83dadcd468425969c6d43d353f6333da85db5d2f81e5cd2a6a7fc07f786b1bc1` |
| `reviewer-artifact-identity.log` | `31452e688b99234c741537991ee4414bddbbbbae436857e5d0eadc51af3d5973` |
| `reviewer-python-installed.log` | `f77bbd43c3a9ea095a8905f9c933fac7bb34f150e1acb44e473f909afec8b60c` |
| `reviewer-python-candidate.log` (initial import failure) | `4510b25fef74ff27cf71956a3b270e2960886b34fd1159a391a015ca6273ac3e` |

The root agent's broader results were independently checked in their logs, not rerun as an entire
suite by this reviewer: adapter regression **1076 passed / 12 ignored**, final factory peer suite
**46 passed**, and installed candidate Python configuration/readback/stub suites **170 passed in
21.19s**. The corresponding logs are `adapter-all-verified.log`, `peer-factory-final.log`, and
`python-candidate.log`. The ignored cases include live tests and are not live-account acceptance.

## Verification limits and remaining risks

The [frozen official contract](entropy-account-contract-20261008.md) distinguishes supported policy
from venue guarantees. This reviewer independently recalculated the byte counts and SHA256 of all
14 official captures against [public provenance](entropy-account-public/provenance.json), using
`E:/persarb/_tmp/issue100-official-contract-20261008/`. Those public sources do not establish an
observed private account. `disabled` to standard accounting remains an explicit inference. Mode,
role, collateral, and state queries are not an atomic venue snapshot. Changes between queries are
bounded observations, not instant transition detection. WS receive freshness plus fresh REST does
not prove gap-free replay or manufacture a missing WS source timestamp.

`flat` means observed positions are zero, not absence of pending orders, sufficient funding, or
permission to trade. A cached AccountState or a saved detached JSON copy can outlive trust; consumers
must retrieve current diagnostics and honor `trusted`/`flat=null`. No real account transitions,
private venue timing, agents/vaults/subaccounts, unified/portfolio accounting, or economic settlement
were accepted. All adapter io execution remains read-only; raw standalone HTTP/WS signing helpers
are not an account-scope authorization boundary for arbitrary callers.

The standard `make` executable is absent on this Windows host, so `make format`, `make pre-commit`,
and `make pre-flight` cannot be claimed as completed. Scoped Rust formatting/Ruff/diff checks and the
actual regression/build tests were reported by the root agent. Library Clippy finished successfully;
the broader test-target Clippy run is blocked by existing Windows documentation lint errors in
`crates/persistence/src/parquet.rs:656`, unchanged between the base and candidate. This is a recorded
baseline limitation, not a full Clippy pass. The release stub-generator build was deliberately
interrupted during prolonged fat-LTO optimization. Stub generation subsequently succeeded with the
existing nextest profile after adding the selected CPython DLL directory to PATH; the first missing-DLL
attempt is retained in evidence. Generated types were not edited manually, and the nextest wheel is
the tested candidate, not a claimed release-profile artifact.

## Source fingerprints at final review

| File | SHA256 |
| --- | --- |
| `crates/adapters/hyperliquid/src/account_scope.rs` | `29a9af8f250253c567b012c4600d5d69516549eb5e9cc611bc82e7995b6deebc` |
| `crates/adapters/hyperliquid/src/execution.rs` | `8f526d2fd96d05a22317863634c3af0d63aafaf15a3fb9202c8cc9eb682a5f06` |
| `crates/adapters/hyperliquid/src/websocket/client.rs` | `6eec3b079c48a5ea91478b56431a3c040cd7f8a17d8a7bca45ea066246320d8c` |
| `crates/adapters/hyperliquid/src/websocket/handler.rs` | `9fb9ab036f810ca15edcb908b7477a7ddca43a39d46a7ec6d8ab70daf0b11f26` |
| `crates/adapters/hyperliquid/tests/entropy_account.rs` | `43170669ce5b413ebccb7e79eea4082e91beb6519a282f7e53a20616f7eff327` |
| `python/tests/unit/test_entropy_account_config.py` | `0c131e943a03d3f0917501b1975b30b87e2affd50f0b16a91489d502c20b401e` |
