# Entropy bounded native execution acceptance

Issue [101](https://github.com/mmyyrroonn/nautilus_trader/issues/101) has a bounded offline
implementation. All account values, orders, fills and failures below use owned synthetic
loopback HTTP/TCP/WebSocket peers. No real private account, venue order or mainnet action occurred.

## Fixed implementation and artifacts

The issue began from main `95fab86a26781c1f976f93ab84c43fac74eb9d2b` after issue 100 merged.
Before delivery, main advanced with fast book support to `9644fd657e98cdde0067044c9e69a1d8fbd54c54`.
The execution patch was rebased onto that main; range-diff reported an equivalent patch and
the complete integrated regression was rerun. Fast subscription, recovery and ordinary stream
paths were independently reviewed without altering their existing tests.

- Source commit: `64d33d31daf0ef5a7be9de185a8578f1be86bc01`.
- Source tree: `a61662ae0a0fe013ca823cbb0a202697f4aaf2be`.
- Clean source fingerprint: `4f1dd4f491e8d649d19d30b7e63a568d20877d5fb8505f9a5f0b5641f72b0e36`.
- CPython 3.12.9 Windows x64 wheel SHA256: `99d6351fc0e8ac3322d97808c6dc5da5a01eb1981ad8184a7fc73927cade94a1`.
- Actual installed native binary SHA256: `e78c5d14ec5309a665ec918e6903f8ef7459ff107145fb96a03d8750ebefdbfc`.
- Generated Hyperliquid stub SHA256: `a7f3cf9f44ea9dc4f800c38fa8cce20740a2c4ced494a250b492d0b1aa4cec3e`.

The official generator produced stubs; official Ruff normalized all 44 generated files to LF.
The controlled nextest build has matching clean pre/post source identities. Strict install
verified the wheel/native binary/all adapter stubs/current source into only the isolated native
worktree interpreter. Application formal wheel/environment/lock and original dirty checkouts
were preserved. The build is a local validation candidate, not a release or live-venue approval.

Default-native regression uses the integrated execution commit
`acdd46caf83064f47fe57389e89a3e957452ced8`. A subsequent Python-feature build exposed an
unqualified runtime-error mapper. The final commit qualifies the existing mapper, regenerates
factory docstrings and adds the generated signature. The 31 other native inputs match byte-for-byte;
[binding identity](entropy-execution-local/binding-inputs.json) records this separate projection
delta. Actual installed-wheel tests validate that Python-feature result. The report does not
claim the default-feature Rust test command compiled Python projections.

## Verification results

| Verification | Actual result |
| --- | --- |
| Default network all targets | 648 passed; focused subsets not added again |
| Integrated Hyperliquid lib/data/dispatch/peer/exec/HTTP/WS | 1,167 passed; 12 existing ignored cases not executed |
| Normal account/execution factory peers | 70 passed, included in the integrated total; 24 execution instances |
| Production Hyperliquid and network Clippy with `-D warnings` | Passed on integrated main |
| Changed crate nightly format and Python/stub Ruff | Passed |
| Installed config/readback/generator Python tests | 187 passed in 20.63s |
| Independent pre-lint native subset | 55 passed; exact old binary/input identities retained |
| Independent final installed-wheel account/policy subset | 27 passed; wheel/binary/stub/source hashes independently checked |
| Independent final native funds/cancel/ordinary-stream subset | 6 passed; exact binary hashes and nonempty filters verified |

The integrated group is 810 lib, 46 data, 40 dispatch, 70 owned peer, 102 exec, 46 HTTP,
and 53 WS tests. Network's 648 comprises 603 lib, 5 HTTP prepared, 6 actual TCP prepared,
7 backoff property, 13 rate-limit property and 14 proxy tests. Feature-disabled targets with
zero executed tests are not counted. The 12 ignored cases comprise 11 existing live smoke/soak
cases and one existing approximately 30-second account-registration timeout case.
Neither ignored cases nor independent subsets are
added to the complete group as extra coverage.

`make format` and `make pre-commit` were both actually attempted and returned command-not-found
because this Windows host has no make executable. Scoped nightly formatting, Ruff and actual
tests are the available alternatives. Clippy including tests reaches the unchanged Windows
`crates/persistence/src/parquet.rs:656` documentation lints and fails there; no blanket lint
waiver or unrelated persistence edit was made. These limitations remain explicit.

## Verified behavior

The default io execution path remains read-only. An explicit finite policy fixes one strategy,
the exact io instrument set, quantity/price/order/gross estimates, counts, buffers, isolated
leverage, metadata age, absolute journal and deadlines. The normal native factory supports
single GTC/IOC limits, owned single CLOID cancellation and owned reduce-only IOC close.
Wire price/size precision, actual user/coin/asset index and canonical collateral are checked.
Lists, modification, naked market orders, foreign adoption and resend remain outside the scope.

Preparation durably reserves an owned intent. Immutable nonce/expiration/action/payload/frame
binding is persisted before enqueueing. Final admission runs after actual backend readiness
and holds the lifecycle/private-receipt/proof/cancellation boundary through actual `start_send`.
Pending private frames block admission. NotWritten and MayHaveWritten remain distinct; expiry
and lost ACK cannot establish rejection. Prepared sends wait in bounded independent tasks while
already-received private frames continue through the handler. Ordinary clients do not receive
the internal control messages, and readonly account handling is preserved.

Available entry funds use the conservative minimum of exact REST/private free and withdrawable,
then subtract unresolved reservations and the margin buffer. Source values and timestamps are
kept separate; non-atomic PnL values need not equal. Both actual private-fund reduction cases
start from trusted/complete HTTP free=100, observe the lower private value, then assert the
specific insufficient-funds native denial and zero peer actions. Two concurrent orders cannot
spend one available balance twice. Metadata leverage drift and unknown financial activity close
admission without changing venue leverage or transferring funds.

Actual individual trade facts produce native events and update the real ExecutionEngine/cache/
Portfolio. Partial fills and duplicate handling are observed at those objects, not simulated
event counts. The positive lifecycle includes a rebate, entry fee total 0.001 USDC, an owned
IOC close with fee 0.0015 USDC, a flat Portfolio and a separately trusted flat account proof.
Filled aggregate/status markers do not fabricate fills. The fixture's constant balance is not
proof of full native realized-PnL/free-funds accounting. Partial fill then cancel retains the
actual position and denies oversized close. Altered raw trade IDs, wrong CLOID, overfill,
limit-price contradiction and unrepresentable fee close recovery without inventing positions.

Missing ACK plus unknownOid retains the original ownership/reservation and queries the actual
user/CLOID and explicit io, including fresh factory restart, without a second write. Historical
empty arrays cannot establish unknown-order rejection. Durable fills do not mean a cold native
cache has applied them: its projection barrier remains incomplete and refuses automatic close.
Source journal exclusivity, finite bounds and torn/conflicting record checks prevent fabricated
ownership. QueryAccount's total finite timeout covers its full proof/metadata/recovery query;
startup retains separately bounded preceding account/registration/private-ready stages.

## Retained failures and practical limits

The 104 hashed artifacts in the
[manifest](entropy-execution-local/manifest.json) preserve original bytes, successful commands,
intermediate failures, identities and reviewer outputs. Initial peer failures included missing
native instrument cache initialization, sender replacement, exact fee test expectations,
caller REST quotas and recovery/WS sequencing. They were corrected using normal APIs and
explicit caller pacing; production quotas, TTLs and deadlines were not enlarged.

Two Windows OS TCP backpressure fixtures did not establish backend Pending. Their failed logs
and original timestamps/hashes remain under `observations/`; no OS backpressure claim follows.
The observation provenance keeps the original `.log` filenames. The identical archived bodies
use `.txt` to avoid the repository log ignore rule; the manifest maps each original path to its archive.
Backend units and an actual Tungstenite codec over bounded duplex separately prove readiness,
cancellation and no ghost prepared write. Actual TCP factory/replacement cases cover the two
enabled transport backends. The handler test observes real TCP receipt queued before a pending
prepared admission, not arbitrary bytes still in a kernel socket or an atomic remote snapshot.

The journal covers process restart with synchronized file data and an exclusive writer. It does
not promise power-loss persistence of a newly created directory entry. The finite release has
no verifiable native position/cache restoration receipt, no guaranteed historical cursor, no
complete isolated maintenance/liquidation cost and no proven sub-USD10 full-close exemption.
A favorable sell execution can exceed a local notional estimate after a gap. Actual funding and
other ledger consumption is the next issue. Live two-leg acceptance belongs to the application
issue and still requires separately authorized finite venue verification.

See [protocol research](entropy-execution-contract-20261008.md),
[integration instructions](../integrations/hyperliquid.md#bounded-io-execution),
[commands](entropy-execution-local/commands.json) and the
[independent final review](entropy-execution-local/reviewer-final-review.md).
