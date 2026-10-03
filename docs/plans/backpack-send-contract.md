# Backpack Send Contract and Evidence Index

Status: accepted numeric-loopback tranche, checked on 2026-10-02 against native commit
`caedfc2e21907cd7c61e3ef1ed651578a061c57f`. This indexes the existing implementation and
its tests for [issue 81](https://github.com/mmyyrroonn/nautilus_trader/issues/81).
Current issue 85 work adds an opt-in native synthetic durable consumer and typed platform cache
recovery; see [durable economics](backpack-durable-economics.md) and
[credential-free protocol evidence](backpack-protocol-evidence.md). This addition does not rewrite
the accepted commit or application PR 25 evidence below.
The broader [native execution contract](native-execution-contract.md) and
[parent issue 13](https://github.com/mmyyrroonn/nautilus_trader/issues/13) remain open.

Production mutation authority, authenticated venue account identity, positive private subscription
acknowledgement and complete economic coverage are outside this acceptance. The
[Backpack README](../../crates/adapters/backpack/README.md) defines the supported product and API surface.

Accepted changes are linked through [durable identity PR 55](https://github.com/mmyyrroonn/nautilus_trader/pull/55),
[shared preparation PR 54](https://github.com/mmyyrroonn/nautilus_trader/pull/54),
[read transport PR 57](https://github.com/mmyyrroonn/nautilus_trader/pull/57),
[guarded owner PR 64](https://github.com/mmyyrroonn/nautilus_trader/pull/64),
[native engine PR 74](https://github.com/mmyyrroonn/nautilus_trader/pull/74) and
[restricted Python PR 75](https://github.com/mmyyrroonn/nautilus_trader/pull/75).

## Send contract

[`BackpackOrderOwner::submit` and `dispatch`][owner] use this order for one supported creation:

1. Start one monotonic whole-operation deadline; validate the original order and current authority.
2. Retain its permanent local clientId and immutable intent before waiting for shared HTTP quota.
3. After quota admission, recheck cancellation, generation, readiness, authority and reservation.
4. Persist the attempt as Unknown before transport can emit its first byte.
5. After disk synchronization, refresh the clock and recheck cancellation and admission.
6. Construct the fresh canonical signature and wire body from the same typed parameters.
7. Let the shared transport recheck the whole deadline after preparation; dispatch at most once.

The whole deadline covers identity/checkpoint work, quota, preparation and transport. Synchronous
checkpoint work is not an interruptible disk-operation guarantee. Its elapsed time cannot authorize
a late send. Controlled slow-checkpoint timing coverage remains open below.

The shared [`HttpClient::request_with_url_redacted_prepared_request`][network] performs quota and
final deadline checks. Its compatible `request_with_url_redacted_prepared` entry is also used by
[Aster's HTTP client][aster]. Backpack public reads, authenticated reads and guarded writes use an
explicitly shared `BackpackQuota`; this controls that in-process scope, not other processes or users.
Venue guards, ownership and economic rules remain inside Backpack.

## Evidence and permissions

[`BackpackRequestOutcome`][http-error] distinguishes `NotSent`, `VenueRejected` and `Unknown`.
These correspond to the proposed contract's DefinitelyNotSent, DefinitivelyRejected and
ExecutionUnknown vocabulary. Local refusal proves no transport entry. Rejection needs conclusive
venue evidence. Timeout, cancellation after entry, lost acknowledgement or an inconclusive response
retain uncertainty. A later read rejection cannot erase a previous uncertain attempt.
NotSent does not remove permanent identity or automatically clean conservative durable attempt state.

A guarded creation is never automatically retried, assigned a replacement ID or adopted solely
from a matching numeric clientId. A complete matched POST acknowledgement establishes the original
binding; cumulative queried quantity alone creates neither a true fill nor an economic receipt.
A DELETE 202 records pending cancellation, not a Canceled event or proof of flat exposure.

New risk requires current public/private generations, exact metadata and account facts, finite
reservation limits and unexpired explicit authority. Reduction requires its own provable position
and reduce-only checks. Owned cancel uses its independently authorized original binding and can
remain available when new-risk public freshness fails. Authenticated query is a separate read
permission; the normal read-only factory exposes no mutation path.

The loopback account facts are explicit synthetic caller assertions. They never verify production
identity or private subscription ACK. Python control exposes no raw dispatcher, signing bypass,
production proof or economic acknowledgement callback. Its opt-in `persist_economics()` delegates
to native verified consumption and storage; `reconcile_terminal_evidence(session)` reads native
retained evidence. Neither accepts caller-created economic state or receipts.

## Economic delivery and shutdown

True venue fills carry independent trade identity and exact quantity/price/fees. Duplicates do not
apply economics twice. Native channel delivery, cache mutation and Portfolio application are
observations, not durable consumer acknowledgements. The native acknowledgement boundary requires
an independently durable consumer state/receipt. The current synthetic native consumer commits
actual scoped cache state and exact receipts before ACK, and offers typed recovery through
`ExecutionCacheRecovery` to LiveNodeBuilder. Without opt-in storage, fills remain pending. The
archived application PR 25 runner below supplies no such consumer; its fills remain pending.

Owned cancellation, terminal order evidence, economic completeness, flat verification and clean
shutdown are distinct. A late true fill can reopen risk after terminal evidence. A sticky dirty
shutdown report is not cleaned by repeated stop, an old flat snapshot or disposing local sockets.
A scenario may complete its finite steps while a partial position, pending fill and cancellation
remain unresolved. Missing shutdown or economic evidence never defaults to clean or zero.

The current consumer binds the complete symbol allowlist, namespace, identity directory, account,
trader and execution client. It rejects corrupt, missing initialized, conflicting or partially
restored state. It also checkpoints terminal lifecycle changes without requiring a new fill.
Historical receipts and cached state are not current readiness, subscription ACK or flat evidence.
Default recovery reads only the preceding hour; a longer outage or unknown venue replication lag
cannot establish completeness and must retain uncertainty. Windows process-kill evidence is not a
power-loss durability guarantee. See [durable economics](backpack-durable-economics.md).

## Verified boundary index

Each linked name identifies an existing test in the linked file; use the exact name when filtering.
This is a focused inventory, not a claim that every parent criterion or venue scenario is covered.

- **Intent before bytes, fresh signing after quota and zero-send refusal:**
  [`test_true_post_ack_is_bound_signed_and_intent_precedes_first_byte`][exec],
  [`test_shared_quota_longer_than_receive_window_signs_fresh_after_wait`][exec],
  [`test_queued_request_rechecks_session_stop_freshness_and_authority`][exec],
  [`test_deadline_expiring_during_preparation_is_not_sent`][http-tests].
- **Credential audience and unsupported production mutation:**
  [`test_production_write_and_wrong_credential_audience_are_explicitly_refused`][exec],
  [`test_production_credentials_refuse_local_peer`][http-tests].
- **Single shared public/private read scope and transmission classification:**
  [`test_public_and_private_reads_share_quota_and_queued_cancellation_is_not_sent`][http-tests],
  [`test_read_retry_preserves_unknown_on_later_not_found`][http-tests],
  [`test_cancellation_before_and_after_dispatch`][http-tests],
  [`test_transport_timeout_is_unknown_and_budget_is_bounded`][http-tests].
- **Unknown creation, lost success response and immutable identity:**
  [`test_unknown_response_never_retries_or_changes_original_id`][exec],
  [`test_accept_then_response_loss_keeps_one_post_and_unknown_capacity`][exec],
  [`test_guarded_lost_post_ack_rest_collision_remains_unbound_and_never_retried`][client-tests].
- **Separate reduction/cancel authority and pending 202:**
  [`test_reduce_only_market_requires_direction_and_unreserved_position`][exec],
  [`test_cancel_permission_is_distinct_from_new_risk_and_data_freshness`][exec],
  [`test_cancel_202_pending_owned_only_and_true_fills_required_to_release`][exec],
  [`test_guarded_public_generation_fault_refuses_new_post_but_owned_cancel_uses_exit_authority`][client-tests].
- **Owned queries retain expected evidence gaps without weakening strict parsing/correlation:**
  [`test_attached_owned_query_static_gaps_preserve_session_and_cancel_202`][client-tests],
  [`test_attached_owned_query_cumulative_never_infers_fill_or_acknowledges`][client-tests],
  [`test_attached_owned_query_unknown_field_invalidates_session`][client-tests],
  [`test_attached_owned_query_correlation_conflict_invalidates_session`][client-tests].
  Only AccountIdentityUnverified and OrderTimeUnknown remain Degraded observations;
  [PR 77](https://github.com/mmyyrroonn/nautilus_trader/pull/77) records this narrow fix.
- **Old run/token isolation, recovery and weak owner lifetime:**
  [`test_reconnect_resigns_replays_pending_and_new_run_does_not_inherit_observed_coverage`][client-tests],
  [`test_parse_fault_fences_late_rest_bootstrap_and_query_is_owned`][client-tests],
  [`test_attached_control_recovers_after_private_first_public_admission_failure`][client-tests],
  [`test_guarded_old_config_and_telemetry_cannot_hold_owner_after_client_drop`][client-tests].
- **Durable failure, restart and lock ownership:**
  [`test_intent_storage_failure_sends_zero_and_poison_is_sticky`][exec],
  [`test_checkpoint_failure_after_identity_commit_sends_zero_and_restores_unknown`][exec],
  [`committed_intents_survive_restart_and_never_release_their_ids`][identity],
  [`malformed_checkpoints_are_never_reinitialized`][identity],
  [`killing_an_owner_releases_the_os_lock_without_deleting_the_lock_file`][identity-process].
- **Economic ACK, terminal/late-fill race and recovery:**
  [`test_ack_lease_reentry_error_panic_and_duplicate_do_not_commit_twice`][client-tests],
  [`test_ack_lease_concurrency_counts_new_pending_evidence`][client-tests],
  [`test_guarded_cancel_202_terminal_waits_for_durable_true_fill_ack_and_late_fill_reopens_risk`][client-tests],
  [`test_guarded_restart_rest_true_fill_dedup_couples_native_state_and_consumer_receipt`][client-tests],
  [`test_old_flat_snapshot_does_not_make_shutdown_clean`][exec].
- **Opt-in native durable consumer and platform recovery (current issue 85 test inventory):**
  [`test_durable_consumer_seed_isolated_from_other_native_client_and_denied_order`][client-tests],
  [`test_durable_seed_storage_failure_prevents_native_dispatch`][client-tests],
  [`test_durable_consumption_storage_failure_retains_pending_and_poison`][client-tests],
  [`test_durable_consumer_persists_terminal_lifecycle_without_new_fill`][client-tests],
  [`test_durable_consumer_restart_at_every_economic_boundary`][client-tests],
  [`test_durable_consumer_preserves_zero_fee_rebate_and_fee_currency`][client-tests],
  [`economic_checkpoint_binds_complete_allowlist_and_client`][economics],
  [`initialized_economic_checkpoints_fail_closed`][economics],
  [`invalid_recovery_is_refused_before_any_cache_write`][recovery],
  [`recovery_never_overwrites_existing_account_state`][recovery].
  This inventory describes source coverage, not an installed-wheel result. The restart helper uses
  disconnect/drop; abrupt process-kill acceptance is distinct from those in-process tests.
- **Cached terminal watchdog:**
  [`test_inflight_check_retires_direct_cached_terminal_without_observing_event`][manager],
  [`test_inflight_check_preserves_real_pending_retries_and_queries`][manager],
  [`test_inflight_check_missing_cached_order_keeps_existing_bounded_retry`][manager].
  [PR 79](https://github.com/mmyyrroonn/nautilus_trader/pull/79) retires cached terminal tracking;
  it does not make a real PendingCancel terminal or settle Backpack economics.

## Diagnostics and installed application evidence

[`BackpackPublicHealth`][runtime] and [`BackpackAccountHealth`][account-health] have `schema_version=1`
and exclusive run/generation observations. Public telemetry separates quote freshness from book
continuity, freshness and bounded coverage. Account telemetry separates transport, REST snapshot,
private subscription confirmation, evidence gaps and pending fills. Factory `capabilities_json()`
is static; it does not establish per-run readiness. The Python surface keeps public execution-ready,
production writes and private subscription verification false. Synthetic durable ACK is supported
only by the explicitly configured native consumer after verified consumption and persistence.
Current-run freshness and isolation are covered by
[`loopback_quote_freshness_null_side_old_event_and_malformed_duplicate`][data-tests],
[`loopback_duplicate_depth_does_not_extend_book_freshness_but_empty_progress_does`][data-tests] and
[`loopback_drop_inflight_old_run_cannot_pollute_reclaimed_telemetry`][data-tests].
Constructor/factory contracts are covered by
[`test_python_registry_constructs_native_factory_with_shared_run_telemetry`][python-public],
[`test_opaque_audience_bound_constructors_and_exact_policy_no_io`][python-account] and
[`static_loopback_construction_is_no_io_and_refuses_production_and_non_synthetic_plans`][python-loopback].

[Application PR 25](https://github.com/mmyyrroonn/Nautilus-Perps/pull/25) accepts installed-wheel
Strategy acceptance. Its frozen source is `587c67cd27d9435545ec4147e0f952df07976993`.
Its exact [installed native peer tests][app-tests] are:

- `test_actual_strategy_ack_quote_stale_denial_fill_duplicate_cancel202_and_dirty_stop`.
- `test_unknown_post_is_not_rejected_retried_or_adopted_from_numeric_client_id`.
- `test_report_budget_exhaustion_on_submit_record_stops_before_any_post`.
- `test_external_cancellation_publishes_dirty_native_stop_and_releases_identity`.
- `test_minimum_real_report_budget_keeps_authoritative_native_stop_counts`.

The finite runner sets `reconciliation=false`, `inflight_check_interval_ms=0`,
`open_check_interval_secs=None` and `position_check_interval_secs=None`, and reports
`continuous_reconciliation=false`. It does not accept continuous reconciliation. Final native
CancelPending health proves matching 202 receipt separately from the shutdown count, which combines
Unknown, Pending and ResponseObserved cancellations. Report exhaustion blocks new POST; a once-only
owned cancel retains independent exit authority. Compact evidence preserves actual native dirty
shutdown/pending counts and explicit unknown exposure, never implicit flatness or settlement.

The [archived report][app-report] at application commit
`06e6b41629f4ee71f6bab61619afc0b66f53d249` preserves the summary and event bytes with
an [evidence index][app-index]. The original source-bound CLI evidence remains at
`E:/persarb/.backpack-artifacts/app23-cli-caedfc2e21/cli-evidence.json`:

| Artifact                   | SHA256                                                             |
| -------------------------- | ------------------------------------------------------------------ |
| CLI evidence               | `b475c4d81b6e34c1a10d613a422744c91a354e6942613b84acc9ce60d7b96364` |
| CPython 3.12 Windows wheel | `e978feb6c6d0962a329ae94710ed42fe4c85c2fa3afd53019656b9149e73ea20` |
| Embedded native binary     | `aee2d5cc8de68c8737cd85eb6b09d9ae92b619b697e08bb4fba2fcb517e2e8b2` |
| Generated Backpack stub    | `d67a3b654a1980471a6a0e9e56e9b176e9d10a0619342e046219b243d1dc26c2` |
| Native provenance          | `4694b2e5e90a9b9cdb58e0b72ae1c11a2b762ec0eecf2a4c17300cd61d407173` |
| CLI summary                | `659783ce3316080002ae47f288a678e5f67521b859ada9450ca17ae1a4e97e45` |
| CLI event records          | `e03bdd13b01ad49ff42690d6316bf250511d669792d27b48a37bc4f59d7531b9` |

The CLI peer verifies one POST and one owned DELETE, delivers trade 701 twice, and closes with zero
active connections. Native economics apply it once: `0.00001` LONG and `-0.00000100 USDC` commission.
One fill and one cancellation remain pending; shutdown is dirty, flat verification and durable ACK
are false. The CLI peer releases fills on a fixed three-second delay, not a realtime Denied signal;
only the event-linked test above proves that causal fixture ordering. The archive retains exact
summary/event hashes; the local original artifact path is not a public download link.

## Remaining coverage and venue facts

- **Open timing coverage:** a controlled slow-checkpoint barrier demonstrating the whole deadline
  and stop/admission checks across synchronization. The implementation refreshes admission after
  synchronization; existing failed-checkpoint tests do not prove this timing scenario.
- **Open diagnostic coverage:** measured live quota queue delay and the proposed cross-adapter
  entry/close/flat diagnostic schema. Existing versioned snapshots do not fulfill that entire schema.
- **Open external facts:** venue clientId uniqueness/reuse/retention scope, actual authenticated
  account/subaccount binding, positive public/private subscription ACK semantics, funding estimate
  units and complete account-specific economic/history coverage. The
  [protocol evidence](backpack-protocol-evidence.md) records what official documentation already
  proves, including settled funding sign, fee units, subaccount isolation and paging limits; those
  facts do not verify the intended live account or unknown replication bounds. No DMS guarantee is
  accepted here.
- **Open broader acceptance:** production mutation permission, production economic consumer
  durability, continuous reconciliation, long soak/journal growth and cross-venue acceptance.
  No RFQ, borrowing, transfers, complex/batch orders or modify capability is added by this index.

[owner]: ../../crates/adapters/backpack/src/execution/owner.rs
[network]: ../../crates/network/src/http/client.rs
[aster]: ../../crates/adapters/aster/src/http/client.rs
[http-error]: ../../crates/adapters/backpack/src/http/error.rs
[exec]: ../../crates/adapters/backpack/tests/execution.rs
[http-tests]: ../../crates/adapters/backpack/tests/http_client.rs
[client-tests]: ../../crates/adapters/backpack/tests/execution_client.rs
[identity]: ../../crates/adapters/backpack/src/identity.rs
[identity-process]: ../../crates/adapters/backpack/tests/identity_process.rs
[economics]: ../../crates/adapters/backpack/src/execution_client/economics.rs
[recovery]: ../../crates/live/src/node/recovery.rs
[manager]: ../../crates/live/src/execution/manager.rs
[runtime]: ../../crates/adapters/backpack/src/runtime.rs
[account-health]: ../../crates/adapters/backpack/src/execution_client/telemetry.rs
[python-public]: ../../crates/adapters/backpack/tests/python.rs
[python-account]: ../../crates/adapters/backpack/tests/python_account.rs
[python-loopback]: ../../crates/adapters/backpack/tests/python_loopback.rs
[data-tests]: ../../crates/adapters/backpack/tests/data_client.rs
[app-tests]: https://github.com/mmyyrroonn/Nautilus-Perps/blob/587c67cd27d9435545ec4147e0f952df07976993/tests/test_backpack_loopback_native.py
[app-report]: https://github.com/mmyyrroonn/Nautilus-Perps/tree/06e6b41629f4ee71f6bab61619afc0b66f53d249/reports/backpack/2026-10-02-loopback-execution
[app-index]: https://github.com/mmyyrroonn/Nautilus-Perps/blob/06e6b41629f4ee71f6bab61619afc0b66f53d249/reports/backpack/2026-10-02-loopback-execution/evidence-index.json
