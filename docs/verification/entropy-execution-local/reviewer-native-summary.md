# Independent frozen native candidate verification

Date: 2026-10-08. Reviewer: entropy_review. Direct execution only; no Cargo or build.

## Identity

Base: `95fab86a26781c1f976f93ab84c43fac74eb9d2b`.
All bytes and SHA256 values in root `tested-inputs.json` matched the current worktree before
execution. The complete verification is in `reviewer-native-identity.json` and
`reviewer-native-results.json`; credentials were excluded and not read.

Peer executable: `E:/persarb/nautilus_trader/target/nextest/deps/entropy_account-69bad49fe9dc76e8.exe`.
SHA256: `20e358c84c80482c9e0e1a6be2a67e86daef47cfcf9a8d4d318d8f616d929178`.

Library executable: `E:/persarb/nautilus_trader/target/nextest/deps/nautilus_hyperliquid-1fcbc36810deefe5.exe`.
SHA256: `63a8aba2f9de81cf6a7b60236fef71c192a7a03930666b57175debdabbe3fe05`.

## Independent results

`reviewer-run-native.py` directly invoked eleven peer filters using `--test-threads=1 --nocapture`.
They executed **19 tests: 19 passed, 0 failed, 0 ignored**. They include the actual normal factory
entry, duplicate/partial real fill facts, rebate/fee totals, owned IOC close, Portfolio flatness,
partial-fill cancel and oversized-close rejection, exact owned cancel asset/CLOID, unknown ACK
and no resend on restart, raw journal/native projection restart barrier, both independent private
fund reductions, account verification deadline, three unsupported private-economic frames,
three terminal trade-identity conflicts, four contradictory/unprojectable fills, and concurrent
budget spending. The positive complete trade lifecycle passed in 40.360 seconds and the
partial-fill/cancel case passed in 5.250 seconds; caller pacing and real quota were retained.

`reviewer-run-lib.py` directly invoked five focused library filters. They executed **36 tests:
36 passed, 0 failed, 0 ignored**: 28 execution-scope arithmetic/policy/journal/native trade tests,
five shared ingress tests, one actual signed-envelope/prequeue test, one actual TCP handler
pending-send/raw-frame test, and one detached private-funds provenance test.

Combined independent subset: **55 passed, 0 failed, 0 ignored**. Exact argv, timings, output logs,
log SHA256 values, and exit codes are retained in `reviewer-native-results.json` and
`reviewer-native-lib-results.json`. Each filter was checked to execute at least one test.

Root `native-all-06.log` reports 1,137 passed/12 ignored across library, data, dispatch, peer,
execution, HTTP, and WebSocket targets with exit zero. This is root's complete regression result,
not an additional independent full-suite run. Earlier failures remain distinct retained evidence.

## Candidate verdict and boundaries

The sampled frozen Rust candidate passes this independent subset and the reviewed bounded,
opt-in execution semantics. I found no remaining material source blocker in this scope after
the latest-private-funds fix and test assertions. This is a limited native-candidate PASS;
installed source-bound wheel, generated stub, and Python normal-builder verification are pending.

After this run, root made production-Clippy fixes and informed the reviewer that these binaries
are the pre-lint candidate. The sampled changes are signature/capture cleanup: cancel receives a
borrowed command and copies the four exact Copy identifiers into its owned task; runtime journal
construction receives `&str` but stores the same owned address; JSON parsing drops an extra
reference; admission context derives Copy; the private-ledger and leverage error branches use
equivalent short-circuit let chains. No admission amount, identity comparison, ingress lock,
start_send continuation, or rejection behavior changed in those inspected branches. Source review
found no material issue, but final rebuilt binary/wheel results must bind to the post-lint source.

These are synthetic local HTTP/TCP/WebSocket exchange peers through actual normal factory,
ExecutionEngine, cache, and Portfolio. No real private account or external exchange action was
used. Shared ingress linearization and real backend pending/cancellation are established by
layered focused tests; the handler waiting test uses a synthetic admission gate/token. Do not
claim a deterministic normal-factory OS-TCP-backpressure race from those tests or atomic remote
account/asset observations. Real Tungstenite codec/duplex Pending is distinct from OS TCP pressure.

Restarted raw fill journals do not establish restored native cache/Portfolio; the persistent
projection barrier correctly keeps new risk and close unsupported there. Unsupported private
funding/ledger/unknown financial activity closes proof rather than calculating that economy.
Exact native precision mismatch closes without rounding into an invented fill. Available funds
remain an explicitly conservative non-atomic estimate using HTTP/private minima and full notional
plus fee/margin buffers, rather than proof of complete isolated maintenance or liquidation cost.
Startup does not have one policy timeout encompassing its preceding separately bounded account
refresh/registration/private-ready stages. No live/mainnet authorization follows from this PASS.
