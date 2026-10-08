# Independent review of native factory acceptance tests

Source-only review, 2026-10-08. Reviewed
`crates/adapters/hyperliquid/tests/entropy_account/execution.rs` in the current issue 101 worktree.
Ten test functions expand to fourteen cases through two three-case rstest matrices. No Cargo command,
test execution, tracked mutation, commit, private account access, or exchange write was performed.
These conclusions assess assertions and fixture semantics, not execution results.

## Meaningful native evidence already present in the test design

The fixture constructs the normal Rust execution factory, creates a real execution client with a
dedicated io account, establishes loopback HTTP/WebSocket peers, inserts caller orders into Cache,
and feeds emitted events into ExecutionEngine and Portfolio. Actual peer action counts, native
order status, fill quantity, trade IDs, commissions, and portfolio net position are observed.
ACK-only tests do not claim a native fill. Restart tests use a genuinely new factory and Cache while
preserving the journal and peer venue state. The artificial signing key is a fixed fixture value.

`fill_events` counts both received FillReports and OrderFilled events; it is an ingress counter,
not by itself proof that the engine applied a fill. The tests that additionally assert native filled
quantity, trade IDs, commissions, or portfolio net position provide the relevant application proof.

## Case-by-case assessment

| Case | What its assertions establish | Strengthening or scope limit |
| --- | --- | --- |
| Partial fills, rebate, replay, marker, owned IOC close | Native partial/full fill processing, net position changes, first negative commission, no extra fill from duplicate/early Filled marker, caller close floor and reduce-only IOC wire fields | Assert entry total commission after second fill and close commission. Account funds remain synthetic constant 100, so this does not verify account PnL, free funds, margin absorption arithmetic, or complete Portfolio accounting. |
| Invalid exact price | Native denied order and zero peer actions for a price beyond precision | Good narrow rejection test; verify production error reason if another unrelated denial could satisfy it. |
| Overlapping order reservations | Two orders are submitted before awaiting either result; one balance cannot fund both, one peer action and one denied/rejected order | Meaningful overlapping pending-intent test, although submissions run sequentially on one actor rather than testing lock contention between threads. |
| Owned cancel | Wrong command instrument and OID cause no extra peer action; correct cancellation uses dedicated io asset and updates native status | Binds command to original ownership. Does not separately corrupt the native cached order or exercise stale cancel tokens, race fills, partial cancel, or concurrent close reservations. |
| Missing ACK, unknown status, restart | Possible-write reservation retained; additional risk denied; original CLOID restored; fresh factory does not resend | Wait for actual ACK timeout and exhausted bounded recovery, not merely phase Unknown set at writer admission. Compare requests recorded after restart; current `any` assertions can be satisfied by earlier requests. |
| Same tid, changed price | Terminal financial conflict, no extra native trade, new risk denied | Ready positive control is present. Prefer a conflict-specific diagnostic/consumption witness to exclude unrelated invalidation. |
| Same tid, changed quantity | Same as preceding case for size conflict | Same limit. |
| Same tid, changed fee | Same as preceding case for fee conflict | Also assert the already-applied native commission stays unchanged. |
| User leverage drift | Query synchronously closes new-risk gate and no write occurs | Current test can pass before receiving the changed asset response: `query_account` immediately invalidates recovery. Require new activeAssetData request/response processing and an out-of-policy leverage diagnostic, or prove the query completed unsuccessfully for that specific cause. |
| Foreign io open order | Real private frame revokes ownership, creates no owned intent/fill, and denies a later order | Starts from Ready. Add an explicit external-ownership diagnostic and drain to a receipt/applied marker when available; do not let arbitrary parse failure substitute for this semantic rejection. |
| Wrong present CLOID fill | No native fill/position is applied | After placement Accepted, post-action recovery may already be pending. Start from Ready and require this frame's conflict witness before evaluating the false state. |
| Overfill | Same native zero-projection rejection for size beyond owned quantity | Same positive-control/consumption requirement. Assert original raw ledger cumulative quantity remains zero. |
| Fee beyond native precision | No quantized native financial event or position is accepted | Same positive-control/consumption requirement. Verify exact unsupported raw evidence retention or explicitly document that this precision is unsupported. |
| Durable fill with new native Cache | Real first-session fill is journaled; new factory cannot claim native projection restored; close does not reach peer | Good conservative restart barrier. Wait for/assert the actual synchronous denial or Denied event rather than only sleeping 40 ms. This simulates absent restored cache, not a process kill precisely between fsync and report emission. |

The strongest immediate test corrections are the leverage-query false positive, the contradictory
fill false-state wait, and restart request scoping. These were sent directly to root during review.

## Remaining cases to map to source units or final peer acceptance

- Same raw coin/tid with changed OID; present mismatched CLOID with a previously known OID.
- New actual tid after NotWritten, Rejected, and terminal cancellation; conflict replay after terminal.
- Partial IOC terminal status with missing real fill/fee evidence; status alone cannot release reserve.
- Terminal reserve release requires a complete later scoped account source and actual owned fill size;
  show a too-old source preserves reservation, then a consistent fresh source releases it.
- Overlapping reduce-only close quantities, direction flip rejection, and stale cancel action tokens.
- Old HTTP status response after reconnect or a newer recovery: no phase/OID mutation and no old report.
- Journal second-writer lease, semantic corruption, missing final newline, runtime file/fill limits,
  and prequeue durability failure, mapped to independent units if not normal-factory tests.
- Exact signed envelope expiry/nonce/digests, old-epoch ACK, timeout/drop before handoff, and possible
  write after handoff; count actual handoffs/actions and forbid reconnect replay.
- A newly received but unapplied private frame must prevent final admission, including parser error
  and abandoned marker cases. Closed gates after errors must not regain trust from a later marker alone.

## Can a normal factory deterministically force quota/backend waiting?

The production Hyperliquid post limiter starts with 1,200 tokens. Bounded single order/cancel actions
cost one token, while policy lifetime actions are capped at 1,024; refill only increases availability.
Therefore normal legal actions from one factory cannot exhaust this limiter. The policy's maximum
64 unresolved orders is also below the PostRouter's 100 inflight permits. A finite burst of small
signed frames cannot reliably exhaust Windows OS TCP send buffers. A peer merely stopping reads is
not evidence that backend `poll_ready` returned Pending.

I do not recommend a test-only production backdoor or changing these policy bounds to induce a test
condition. If a supported configurable quota or transport-injection factory API is introduced for
actual product use, that API can make a single end-to-end readiness test deterministic. None was
identified in this factory review.

With the current normal API, the defensible evidence is layered:

1. Normal factory/cache/engine test: begin Ready, send a real loopback private frame, wait for its
   socket receipt/processing witness, and submit through the ordinary execution client. Prove zero
   actions and native denial. This proves actual private source propagation, not blocked backend.
2. The Hyperliquid handler TCP test: block its prepared-send proof callback using the stated
   synthetic gate, prove a real clearinghouse frame and applied marker still emerge, reject proof,
   and count zero actions. Identify the gate as synthetic; do not call it OS backpressure.
3. The real network backend test: use an actual Tungstenite codec with a bounded duplex transport,
   observe true backend Pending, receive/invalidate/release, and count zero prepared handoffs. This
   establishes the backend boundary under an explicitly bounded fixture, not Windows TCP behavior.
4. Receive/applied fence units: real mutex ordering and contiguous epoch/sequence application across
   receipt and continuation establish composition. Do not describe REST or remote exchange state
   as an atomic snapshot merely because these local gates are linearized.

An ordinary factory test may also send a fragmented private Text frame and submit before its final
fragment arrives, but that would test message receipt framing rather than writer readiness and is
not a substitute for the tests above. A deterministic normal-factory quota/backend-pending result
has not been established.

Root has reported successful network runs and a minimal socket2 getter portability test fix.
Those results and that separate diff have not been independently executed/reviewed in this test-only
pass. Final candidate verification will bind exact stable binary/wheel identities and evidence.
