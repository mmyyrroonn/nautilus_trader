# Native execution contract and recovery coverage

Status: proposed E1 contract. This document describes the boundary shared by native adapters.
It does not claim that every adapter implements it. The application owns policy and intent;
native adapters own transport, venue facts, recovery, and event delivery.

## Send boundary

An adapter classifies each command before requesting a transport quota:

| Class              | Required authority                                                    | Failure policy                            |
| ------------------ | --------------------------------------------------------------------- | ----------------------------------------- |
| New risk           | Verified account, current session, risk budget, and venue readiness   | Refuse locally                            |
| Provable reduction | Verified position and a venue mechanism that cannot increase exposure | Refuse if reduction cannot be proved      |
| Owned cancel       | Evidence that this client owns the target order                       | Never cancel an external order implicitly |
| Query              | Authenticated read scope                                              | May continue while writes are blocked     |

The quota permit is specific to one logical request and its weight. The caller carries one
monotonic deadline across queueing, preparation, transport, and any permitted read retry. After
quota acquisition, the adapter checks the deadline, session generation, and the authority for
that command again. Request preparation and signing happen after those checks and before the
transport call, without another unbounded queue. A change of session generation invalidates a
queued new-risk command even if a later session becomes ready. Cancellation and queries use
their own authority; they do not inherit a new-risk permit.

The final local check is the authorization handoff. A response or transport fault after the
send boundary cannot be relabeled as a local refusal merely because the session later changed.
The network API must preserve this phase distinction as a type, not infer it from error text.

## Submission result and recovery

The shared vocabulary has three outcomes:

| Outcome                | Evidence                                                                               | Recovery rule                                                                   |
| ---------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `DefinitelyNotSent`    | Local validation, deadline, quota, or admission failure before transport entry         | A new command requires fresh authorization                                      |
| `DefinitivelyRejected` | Venue response proves the command was refused                                          | Publish the rejection; do not create a pending effect                           |
| `ExecutionUnknown`     | Transport entered without a conclusive venue answer, or venue response is inconclusive | Persist the original identifier and reconcile; never resend creation on a guess |

An adapter records pending or unknown work before it can lose the only copy of an accepted
command. Recovery distinguishes queued, sent, acknowledged, economically applied, and settled
states. A queued event is not proof that the execution engine applied a fill or fee. The adapter
keeps replayable fill identity and coverage until delivery is confirmed or a reliable replay
path can prove application. A checkpoint failure makes the recovery claim incomplete; it cannot
turn unknown work into a clean restart.

Venue-specific position, balance, cancel, and dead man's switch semantics remain in the venue
adapter. A common primitive may be extracted only after at least two adapters use the same
invariant. The public adapter surface supplies facts and coverage, not strategy orders or a raw
transport path that bypasses final admission.

## Diagnostic contract

A future shared snapshot uses a versioned, typed schema. Version 1 separates these fields:

- `entry`, `close`, and `flat` readiness, each with a fixed reason code.
- Current session and recovery generations, and the last successful reconciliation time.
- Market-data age, quota queue delay, unknown creates and cancels, missing fills, residual
  positions, and journal health.
- Owned shutdown and venue protection status separately from economic completeness.

Unavailable venue capabilities are explicit `unsupported` values, never positive defaults.
Reasons are fixed codes with bounded explanatory text. Snapshots and metrics omit credentials,
signatures, private frames, raw venue errors, and account or order identifiers as labels.
Adding this schema must not reinterpret an existing overall acceptance result as complete.

The existing Ondo read-only nine-key Python snapshot remains its own frozen contract. Its
historical source is
[`native-readonly/interface.md`](https://github.com/mmyyrroonn/Nautilus-Perps/blob/main/reports/ondo-acceptance/20260919-production-native/native-readonly/interface.md).
A migration to a current reference page must preserve that historical file and mark the old
location superseded. Native and application tests must validate the same schema version and
field types before an installed wheel is accepted.

## Coverage inventory

This is the native-side inventory. The application test matrix belongs to
[`Nautilus-Perps`](https://github.com/mmyyrroonn/Nautilus-Perps/issues/3) and should link
back to this contract rather than copy this list.

| Boundary                       | Existing native evidence                                                                                                      | Required next test                                               |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| Quota then signing             | Aster prepared request tests in `crates/network/src/http/client.rs` and Aster HTTP tests                                      | Cross-adapter permit and deadline behavior                       |
| Queued command loses readiness | Aster `review_round4_queued_submit_*`; Ondo `test_a_submission_queued_for_the_budget_is_refused_when_the_account_invalidates` | Recovery to Ready must not revive an old permit in both adapters |
| Unknown POST response          | Ondo `test_production_unknown_post_retains_original_id_and_blocks_second_order`; Aster unknown-status tests                   | Lost success response and restart with the original ID           |
| Cancel and fill race           | Ondo `test_a_cancel_whose_last_fill_arrives_late_converges`                                                                   | Owned cancel proof across restart                                |
| Replay and checkpoint          | Ondo `test_an_unknown_submission_survives_shutdown_and_restart` and fill replay tests                                         | Slow or failed checkpoint at the send and delivery boundaries    |
| Recovery barrier               | Ondo `test_production_reconciliation_tail_handoff_barrier`; Aster recovered-fill tests                                        | Partial history failure and old-session late response            |
| Readiness and diagnostics      | Aster `is_ready` and `readiness_phase`; Ondo read-only and production snapshots                                               | Versioned typed cross-adapter schema and application wheel check |

Use controlled clocks, scripted HTTP and WebSocket responses, disk failures, and barriers for
the next native tests. The application acceptance then installs one candidate wheel and records
the native SHA, application SHA, and wheel hash while observing LiveNode order, position, fee,
diagnostic, and report results against a local venue. Mock-only application tests remain useful
but do not prove the native race boundaries.
