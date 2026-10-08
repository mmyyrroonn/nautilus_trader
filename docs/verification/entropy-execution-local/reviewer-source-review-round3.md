# Independent execution review: wired implementation pass

Source-only review on 2026-10-08, branch based on main
`95fab86a26781c1f976f93ab84c43fac74eb9d2b`. The implementation continued changing during the review.
No Cargo invocation, tracked mutation, commit, private account query, or exchange write occurred.
No implementation PASS or merge recommendation is issued here.

## Findings delivered directly to the implementing agents and root

1. **Private receive blocked by prepared send.** The Hyperliquid handler awaits the prepared
   network send in its command branch. While quota or backend readiness is pending, it cannot
   consume private frames from its raw receive queue. A received same-epoch account/position change
   can therefore remain unapplied while final writer admission sees an older fresh scope. The
   implementation agent confirmed this gap and is adding bounded independent sending plus a
   receive/applied fence. Requested a real private socket frame followed by readiness release,
   with zero backend actions; manually invalidating the state is insufficient evidence.
2. **Foreign io activity ignored.** Unknown io CLOIDs and OIDs return successfully without
   invalidating recovery. A foreign open order or fill observed after the last recovery must revoke
   complete ownership proof rather than be adopted or silently ignored. Other DEX activity remains
   outside this scope. Requested an external io open order on an otherwise flat, fresh account.
3. **Raw fill identity conflicts.** OID-or-CLOID ownership matching can accept a present conflicting
   CLOID, which is subsequently replaced in the native projection. Any present raw CLOID must agree.
   A `(coin, oid, tid)` key also permits a reused same-coin venue tid with a changed owned OID to
   become a second fill; detect stable raw-tid identity conflicts before legacy dispatch.
4. **Impossible or late fills.** New fills after NotWritten or Rejected contradict established
   evidence. New tids after terminal cancellation also require explicit terminal-size/time checks,
   not accumulation into a terminal intent with zero reservation. Identical repeats may be
   idempotent, but conflicts after terminal status must still revoke trust. The agent is fixing this.
5. **Old recovery side effects.** The recovery epoch/revision check originally occurred at final
   commit, after status binding, journal writes, and event emission. Old HTTP responses must not
   mutate phase/OID or emit reports after reconnect or a superseding recovery. Requested a blocked
   orderStatus response, epoch change/new query, then release: no old-generation side effects.
6. **Signed expiration and payload identity.** Prepared posting currently signs with
   `expires_after=None`. Local pre-handoff deadlines do not prevent delayed delivery after a possible
   write. Root was asked to implement the design's immutable signed expiry/nonce/payload binding or
   accurately narrow the claimed time bound. A possible write remains unknown until reconciled.
7. **Crash between durable raw fill and native projection.** The code persists a fill before
   emitting its native report. A process crash between those steps leaves a durable duplicate which
   restart recovery skips; terminal intents are also excluded from active status recovery. A new
   factory/cache must prove native order/fill/position recovery independently, or remain incomplete
   for native projection. Raw ledger completeness alone does not prove the engine received events.
8. **Runtime journal bounds.** The 16 MiB and 10,000-fill checks were load-time only. Full-state
   snapshots appended by repeated status/query handling can grow without new actions. Enforce finite
   runtime record/file bounds or prove atomic checkpointing; otherwise a valid running state may
   become unrecoverable on restart. Also treat a complete JSON record lacking its final newline as
   partial, since a subsequent append would concatenate two records.
9. **Actual asset capacity claims.** `maxTradeSzs` and `availableToTrade` are validated as exact
   nonnegative pairs but not retained or used for admission. Their official per-side semantics must
   determine whether a zero observed capacity blocks entry. A full-notional/free-funds estimate is
   not evidence of current venue asset trading capacity. Do not describe unconsumed fields as a
   completed capacity check.

## Source improvements observed, not yet independently executed

The journal now takes an exclusive file lease before loading and validates internal identity,
immutable history, nonnegative reservations, and fill sums. Arithmetic uses checked Decimal folds.
Metadata commit binds account and execution revisions. ACK variants are mutually exclusive, late
placement resting ACKs do not regress terminal phases, and cancellation ACKs bind a token.
Fill/status handling was changed to validate native projections on cloned candidate intents before
committing them, avoiding partial in-memory changes when parsing or precision checks fail.

Native fill projection explicitly compares commission, quantity, and price against the exact raw
Decimals. A fee that cannot be represented by native Money is rejected rather than silently rounded;
negative representable rebates retain their sign. However rejection occurs before the exact raw fill
enters the journal, so unprojectable financial evidence needs a separate durable evidence record or
an explicit unsupported limitation. No claim of complete raw-fee retention is made here.

The network wrapper now records whether a previous ordinary split-sink slot is occupied, so equal
message bytes cannot relabel that slot as prepared. Reusing a control preserves its prior outcome.
The network agent/root reported successful unit and normal factory tests; this reviewer has not run
their binaries or reproduced those results. OS TCP backpressure has not been established by those
reports. A real Tungstenite backend with bounded duplex readiness is a separate, accurately named
test, not evidence of a Windows OS TCP backpressure condition.

## Receive/applied fence requirements sent to the agent

Register receive epoch/sequence in the real socket callback. Final writer admission must hold the
shared receive gate through the `received == applied` check and actual continuation consumption.
The callback should only register/enqueue under a short lock, without acquiring account/execution
locks. Proposed ordering is network lifecycle, receive gate, account, execution, write control.
Mark applied only after relevant facts are consumed; old-epoch completion cannot advance the new
epoch and a later sequence cannot skip an earlier unapplied frame. Parse errors, enqueue failures,
and abandoned markers must preserve a closed gate or explicit invalidation. No part of this fence
makes independent REST observations an atomic exchange snapshot or covers bytes not yet delivered
to the callback.

Awaiting these fixes and a stable binary for independent acceptance.
