# Independent execution review notes — preparation only

Reviewed on 2026-10-08 at `E:/persarb/worktrees/entropy-account`, branch based on main
`95fab86a26781c1f976f93ab84c43fac74eb9d2b`. No Cargo command, tracked mutation, commit, secret access,
real account query, or external exchange write was performed. These are design requirements and
existing-code observations, not an implementation verdict.

Sources read: `entropy101-design/README.md`, the official issue 101 contract memorandum, the official
issue 102 economic memorandum, and the current native network writer/post/fill paths. Earlier memo
line numbers and HEAD values are historical and do not identify the new implementation.

## Final writer and cancellation

- Current `SendOnConnection` checks epoch and then awaits `SinkExt::send`, so the planned callback
  must run after `poll_ready`, under a real lifecycle/scope linearization rule, immediately around
  one `start_send` continuation. A snapshot returned before that point is insufficient.
- Timeout or dropping the caller future must atomically cancel a still-prepared intent using the
  same lock/state transition as writer admission. Removing a PostRouter waiter alone does not
  cancel a queued action. Tests must release each blocked queue/readiness point after timeout and
  count actual received exchange actions, not merely assert an error result.
- Once `start_send` is invoked, including an error return, conservatively retain possible-write
  ownership and reservations. Flush timeout is Unknown, not NotWritten. No retry/reconnect-buffer
  path may replay an ownership-bound exchange action.
- Callback must have no await, HTTP, fsync, or event callback. Establish durable ownership and
  possible-write intent before enqueueing; crash recovery of a durable Prepared record must also
  allow for a write occurring before a later journal-state update. Avoid a persistence gap after
  sending, or falsely declaring such a record definitely unwritten.
- Network lifecycle -> adapter scope/intent is the proposed lock order. Sink callbacks must not
  create the opposite order. Caller cancellation, transport loss, and start_send require an explicit
  ordering, including when the response channel has already been dropped.

## Margin, reservations, and ownership

- First-time empty assetPositions cannot establish current user leverage. Metadata.maxLeverage is
  a maximum, not a user setting. Either obtain complete user/asset leverage evidence, or explicitly
  justify a different full-notional conservative budget policy; do not claim the original design's
  leverage proof was implemented by reading the metadata maximum.
- Sell floor bounds the minimum execution price, not the maximum short notional. Budget and price
  deviation bounds need independent evidence and limits.
- Final admission must atomically reserve funds and close size. Two intents that jointly exceed
  available funds must not both reach start_send. Unknown writes retain reservations; an ACK,
  NotFound, missing OID, or terminal status alone cannot prove exposure and fees were absorbed.
- Cancellation must bind the actual cached order as well as command instrument, OID/CLOID, account,
  raw coin, and ownership revision. Close must use confirmed strategy-owned size and subtract other
  unresolved close reservations; reduce_only alone proves neither ownership nor size.

## Actual fills and financial conflicts

Current `make_fill_trade_id` hashes `(hash, oid, px, sz, time, start_position)`, not raw venue tid.
Current dispatch deduplicates that projected TradeId, and skips already-filled orders before checking
later fill payloads. It treats cumulative quantity >= order quantity as terminal rather than rejecting
an evidenced overfill. These legacy behaviors are not sufficient for io ownership accounting.

An io raw ledger must first validate account/socket epoch, exact io coin, owned OID/CLOID, raw trade
identity, quantity bounds, source time, fee, and feeToken. A stable raw identity needs a canonical
payload digest: changing px/qty must not create a second physical fill, and changing fee must not be
silently ignored as a replay. Include conflict-after-terminal cases. Preserve negative fee rebates;
builderFee is included in fee, so adding it again would double count. Filled status and aggregate
filled ACKs do not supply individual trade IDs/fees/times and cannot manufacture OrderFilled events.

## Finite recovery completeness

An empty userFillsByTime response has no general coverage cursor or unlimited-retention guarantee.
RecoveryComplete must explain the owned-intent window, pagination bounds, retained history limits,
source scope, status/open-order/position consistency, and unknowns. Inclusive equal-time boundaries
must retain overlap; lastTime+1 can lose different events. Full boundary pages, no progress,
unknownOid, exhausted deadlines/pages, or missing actual fees must preserve incomplete recovery.
An external position/order is observable but is not owned by a strategy merely because its ticker,
side, or OID resembles local state.

## Requested peer cases

1. Block adapter quota, inflight permit, handler, writer queue, and sink readiness independently;
   invalidate or time out before release and prove zero exchange actions when start_send was absent.
2. Start_send entered, flush blocked, then timeout/disconnect: Unknown, reservation retained,
   reconnect action count <= 1. Abort caller futures on both sides of that boundary.
3. Concurrent funds and close-size admission, stale metadata/epoch, caller cap/floor preservation,
   integer price exception, and missing user leverage.
4. Same raw trade identity with altered price/quantity/fee, replay after terminal status, overfill,
   negative fee, unknown feeToken, and Filled marker before actual fee-bearing fills.
5. Wrong command/cache instrument and foreign orders/positions; late old-epoch ACK/fill frames.
6. Missing/ambiguous ACK after venue acceptance, same CLOID reconciliation, empty/truncated/equal-time
   histories, bounded exhaustion, and restart ownership recovery without adopting external activity.

Awaiting the implementation agents' actual APIs and stable native binaries before reviewing the
candidate or executing final acceptance cases.
