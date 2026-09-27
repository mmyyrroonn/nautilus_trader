# Ondo DMS shutdown evidence

## Scope and identity

Native issue 10, application companion Nautilus-Perps issue 6. Offline loopback execution only; no real account observation, private venue connection, DMS operation or mainnet order.

- Native starting base for this change: 16a415942814a10a6403d1b83b704b168ce445e5. The PR head and CI artifacts pin the candidate source.
- Application source inspected: aff9530799e70423207bb9e4f5b14e8dd274fba1.
- Historical acceptance: E:/persarb/Nautilus-Perps/reports/ondo-acceptance/btc-operator/20260923T031758387417Z-240158e4b603/acceptance.md.
- Historical candidate wheel SHA256: 8f20735b299ef204464d8a104ca9fdbcec92a5b20d1a5795f474818d56e12985.
- Historical installed binary SHA256: 708450d8dabe55ec098c5fed510619a1cc6d25126c02bfbdc8558b0a12f4dd8f.

These historical hashes identify the earlier acceptance artifact, not this native candidate. The formal wheel and installed application environment were not replaced.

## Historical facts and budgets

The recorded entry, reduce-only close and flat verification remain valid. Release attempted and frame_sent were true, acknowledged was false, outcome was shutdown_timeout; exit code was 1. Economics remained unknown. Zero classified DMS updates does not prove that no other frame arrived.

Trade start was 2026-09-23T03:18:02.285Z and finish was 03:19:10.626Z. The 68.341 seconds cover the entire trade, not just shutdown. That artifact lacks timestamps for individual shutdown phases; no exact root cause can be assigned retrospectively.

The actual application builder passes PRODUCTION_TRADE_DISCONNECTION_TIMEOUT_SECS=30, defined at src/ondo_trade_probe.py:140 and used at line 3195. The native production allowance remains min(15 seconds, approved absolute cleanup time remaining). Request drain has one-second graceful and one-second forced slices within that allowance. WS close independently allows two seconds graceful and one second forced. Synchronous journal writes, including Drop writes, cannot be interrupted by an async caller timeout. Thus 30 seconds exceeds the nominal async sequence, but does not guarantee a hard wall-clock bound under slow filesystem work. The evidence cannot blame a shorter default node timeout.

## Changes

- Record fixed-label text frame classes before and after release, including old DMS subscription ACKs, matching unsubscription shapes, other unsubscriptions and unclassified text. Preserve sent, observed and acknowledged as separate facts. No raw frame, channel from an unknown payload, account id, order id, amount, credential or signature enters this diagnostic.
- Record shutdown budget/start, request-drain completion, release start/send/ACK observation, WS stop start/end, checkpoint completion and shutdown completion timestamps.
- Add production_shutdown_diagnostics to the native client, account runtime and factory, with a Python factory binding. Diagnostics are readable even when account proof is absent; they do not establish readiness or create an account snapshot.
- Consume the final Drop checkpoint before the production clean decision. An exhausted monotonic shutdown budget makes an otherwise completed report timed out. A diagnostic warning states that checkpoint or transport close exhausted the budget.
- Production release no longer borrows a new 500ms allowance after its approved shutdown budget has expired.
- A production disconnect also requires native final account proof and a healthy journal. A complete stop report without final proof is dirty and cannot become an idempotent clean record.

No timeout or cleanup deadline was increased. Matching ACK parsing and protection rules were retained. DMS is account-wide order protection, not a promise to close a position.

## Verification

- Full Ondo regression after issues 29 and 31: 1107 tests passed, including the doctest.
- Four caller-timeout cases: absent ACK, old arm ACK, malformed channel and wrong channel. All preserve unacknowledged release, reject a late matching ACK as completion and remain incomplete through repeated disconnect/stop. Zero HTTP writes and one connection.
- Short/equal/long outer budgets against a six-second absolute cleanup deadline: all three passed. The long caller receives native failure; equal expiry may be reported by either bound, never as clean. Approved cleanup time is not extended.
- Matching delayed ACK: ordered request/release/WS/checkpoint timeline passed under the real application 30-second outer allowance.
- Safe diagnostics remain readable without account proof; arbitrary private marker values never appear.
- Synchronous work crossing the monotonic budget prevents clean while preserving the observed release fact. Existing slow-checkpoint protection, connection loss, renewal/release interlock, unsettled-order and caller-cancel regressions also passed in the full suite.
- cargo check -p nautilus-ondo --features python --offline --locked passed. This checks the binding; it is not installation or LiveNode acceptance.
- Nightly Cargo format and git diff checks passed. Windows Cargo permits only MSVC informational linker messages via CARGO_BUILD_WARNINGS=warn; Rust warning policy is unchanged.
- Scoped pre-commit is locally limited by missing Make and WSL/pinned-tool incompatibilities. Linux GitHub adapter checks must pass before merging; gate configuration was not changed.

## Remaining acceptance

Issue 10 stays open. The release ACK/renewal contract still lacks authoritative authenticated venue observations. Synthetic frames prove parsing and state handling, not venue behavior. The original protocol is not silently reinterpreted to convert updates or sends into acknowledgments.

Application issue 6 owns installation and LiveNode testing of this exact candidate, launcher/watchdog budgets, process interruption/restart and read-only post-run observation. That joint run must record both source SHAs and wheel/binary hashes. Historical successful entry/close/flat observations must remain separate from new candidate acceptance. No same-candidate installed-wheel or live venue acceptance is claimed here.

Real private observations, DMS operations and trading require separate user authorization. This change supplies native offline evidence and safe instrumentation for that later run.
