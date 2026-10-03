# Backpack durable synthetic economics

This document describes the opt-in native consumer added for
[issue 85](https://github.com/mmyyrroonn/nautilus_trader/issues/85), following the numeric-loopback
[send contract](backpack-send-contract.md). Its scope is an explicit synthetic loopback peer. It
neither enables production mutations nor verifies the intended production account. Official venue
contracts, public observations and remaining unknown facts are recorded in
[protocol evidence](backpack-protocol-evidence.md).

## Configuration and call boundary

Native `BackpackLoopbackExecutionClientConfig::with_economic_state_directory(path)` and the Python
`BackpackLoopbackExecutionClientConfig(..., economic_state_directory=path)` opt into the consumer.
Omitting the directory retains the existing staged, unacknowledged fill behavior. Configuration
construction performs no filesystem or network I/O. Actual factory creation opens the identity
store and the independently exclusively locked economic directory before node startup.

The weak owner-thread control provides two operations:

- `persist_economics()` reads actual native cache consumption, commits native state and receipts,
  then acknowledges eligible pending fills. Python receives a schema-versioned JSON summary with
  checkpoint revision, receipt count, newly acknowledged fills and remaining pending count.
- `reconcile_terminal_evidence(session)` requires the current native session, persists economics,
  and compares retained terminal observations against current native lifecycle/quantity and durable
  true-fill coverage. Python receives a JSON observation, not a permission to manufacture evidence.

These methods accept no state JSON, receipt payload, trade selector or acknowledgement callback.
The control has no signer, raw transport or direct order dispatcher. The application calls persistence
after actual engine consumption, including before its intended clean shutdown. A successful call
with zero acknowledgements is not proof that all delivered events were consumed. Before the first
native account exists, persistence reports pending observations without acknowledging a fill.

## Consumption and checkpoint ordering

The native consumer requires each acknowledged `FillReport` to match a true `OrderFilled` in both
its native order history and native position history. Matching includes account, instrument,
client/venue order and trade identity, side, exact quantity/price, commission amount and currency,
liquidity and event timestamp. Negative fee rebates, zero fees and distinct fee currencies remain
amounts rather than inferred basis-point rates. Receipt fingerprints are rebuilt and checked.
The dedup key remains instrument plus real trade identity; cumulative quantities create no fill.

The snapshot contains native account, used instruments, orders, positions, cache-owned position
archive frames and immutable applied-fill receipts. It binds the complete sorted symbol allowlist, identity namespace and canonical identity
directory, engine account, trader and execution client. Changing or shrinking the allowlist, or
changing the execution client, cannot silently hide residual positions during recovery. Order
selection follows actual cache client routing. The sole unrouted Initialized-order exception is the
exact order submitted by the current command; unrelated clients and newly encountered locally
denied orders are not captured as its seed. A later local refusal cannot erase an already durable
Initialized replay seed; it is retained conservatively without inventing accepted lifecycle.

Before a mutation task can emit bytes, the consumer checkpoints that original native order seed.
This supplies replay state if the process loses an event before consumption. It does not establish
POST success, retry an unknown creation or adopt a queried numeric clientId. A seed with an
independently durable original POST binding can recover Submitted/Accepted lifecycle through native
events; a seed without such binding remains unresolved.

Consumed native state and immutable receipts are written together before native economic ACK.
A crash after commit but before ACK reloads exactly those receipts; history/private duplicates do
not apply quantity or fees twice. A crash before commit restores the previous checkpoint and needs
true-fill replay within available history. Terminal lifecycle changes are checkpointed even when no
new fill arrives. Failed storage poisons the current consumer, retains pending fills and refuses
subsequent checkpoint/ACK attempts. Storage failure while retaining a submission seed prevents the
mutation task from being dispatched.

## Native platform recovery

[`ExecutionCacheRecovery`](../../crates/common/src/clients/execution.rs) is a shared typed native
boundary containing `AccountAny`, `InstrumentAny`, `OrderAny`, `Position` and cache-owned
`position_snapshot_blobs` references with their bytes. Other clients default to no recovery. It adds no Python arbitrary-payload restore or adapter-specific cache injection.
Backpack verifies its scoped checkpoint, native consumption and receipt fingerprints before
returning this recovery value.

[`LiveNodeBuilder`](../../crates/live/src/node/builder.rs) obtains recovery during client construction
before registration/connection. The [platform validator](../../crates/live/src/node/recovery.rs)
requires an empty account scope, exact account/trader/venue ownership, compatible instruments,
unique order/position and venue aliases, and complete order/position references. It validates the
entire scope before inserting objects. Restored orders receive the actual execution client routing;
cache indices are rebuilt and checked, and Portfolio initializes orders and positions from that
cache. Construction fails if restoration conflicts or a backing-store operation fails.

Recovery validation has two phases. The platform first validates and installs the typed native
objects. Before Portfolio initialization, `cache_recovery_restored()` independently compares the
installed cache with the adapter's exact durable snapshot, including account balances, routing,
orders, positions and archive bytes. Only a successful strict comparison marks that consumer
instance as initially restored. Portfolio can then legitimately recalculate account state from the
restored positions. First startup without the builder hook still performs the same strict check;
subsequent startup/reconnect does not compare a legally evolved account against the old snapshot.
This marker attests initial restoration only; new fill receipts retain their own consumption and
persistence checks. It adds no Python setter or payload interface.

NETTING can reuse a position ID after a closed cycle. Durable recovery therefore preserves the
native cache's archived cycle frames, not only the current position. True fill consumption can be
proved by current/replay history or a preserved archived cycle together with its native order and
exact receipt. The platform validates archive references, encoded parent identity and contiguous
frame indices using a temporary Cache before writing any recovered object into the real cache.
Archive account/trader/instrument/order and fill ownership are also checked. Installation uses
`Cache::restore_snapshot_blob`, which retains the original native frame bytes rather than
re-serializing the decoded position. The adapter's strict initial verification checks those exact
bytes. Closed-cycle quantity, fee/rebate and realized economics therefore remain available when a
later cycle or late fill is recovered under the reused native position ID.

Recovery is historical evidence. It does not restore current public/private transport generations,
new-risk authority, a fresh account snapshot, positive private subscription ACK or verified flatness.
An old retained control cannot target a replacement client. Dropping the actual client releases its
OS ownership; copied telemetry and weak controls do not keep the identity/economic locks alive.

## Terminal evidence and shutdown

Terminal reconciliation requires an observed Filled, Canceled or Expired report, the original POST
binding, matching actual cached terminal status and filled quantity, and exact durable true-fill
quantity for the same client/venue order and instrument. A cached terminal alone cannot release
capacity. A pending DELETE 202 remains cancellation evidence rather than a Canceled event.

Terminal economics, flatness and clean shutdown are separate. The control checks current-generation,
fresh complete synthetic position facts for its flat observation; those facts are explicitly caller
assertions for the local peer, not production account verification. Fresh flat evidence is needed
after capacity release. A later larger true cumulative fill reopens risk, and missing corresponding
receipt/native consumption prevents completeness. Restart does not turn an old terminal or flat
snapshot into fresh clean evidence. Sticky dirty shutdown remains dirty after repeated stop.

## Failure model and history limit

The economic store uses an exclusive OS lock, a versioned checksum envelope, a retained initialized
marker, a 64 MiB file bound and configured fill capacity. Corrupt/unknown-schema state, invalid
receipts, conflicting scope, incompatible native state or a missing checkpoint after initialization
fail closed. Checkpoints are written to a same-directory temporary file, synchronized, atomically
replaced and synchronized again. On non-Windows platforms the directory is also synchronized.
Windows does not have that directory synchronization guarantee in this implementation.

Abrupt Windows process termination tests can establish reopening a synchronized checkpoint after
the OS releases process locks. They do not establish behavior under power loss, disk-controller
cache loss, filesystem failure or replacement atomicity across those failures. The existing native
restart helper uses disconnect/drop and is distinct from process-kill acceptance. No power-loss
claim follows from either kind of test.

Automatic history defaults to `now() - recovery_lookback`, with a **one-hour** default lookback. The
checkpoint does not yet retain an earliest unconsumed historical cutoff. A long outage can put an
unconsumed true fill outside that default window. Venue history retention, indexing conventions,
snapshot atomicity and maximum replication lag remain unknown. An empty page or complete local
checkpoint cannot prove that no older, delayed or not-yet-visible fill exists. Missing coverage and
unresolved creations/cancellations therefore remain dirty or Degraded; they cannot be reported as
zero exposure or fully settled merely because a finite replay completed.

## Evidence and remaining acceptance

The current native source includes exact boundary regressions in
[`tests/execution_client.rs`](../../crates/adapters/backpack/tests/execution_client.rs): before event
delivery, before native consumption, after durable commit but before native ACK, and after ACK;
fee/rebate/currency preservation; seed isolation; seed/consumption storage failure; and terminal
lifecycle persistence without a new fill. Scoped/corrupt storage checks live in
[`economics.rs`](../../crates/adapters/backpack/src/execution_client/economics.rs). Typed cache
validation/conflict tests live in the platform validator. Python embedded binding tests check
signatures and rejection of caller-created payloads. This is a source/test inventory, not a claim
that a newly built installed wheel or application acceptance suite has already passed. The
[send contract](backpack-send-contract.md) preserves the original PR 25 consumer-free dirty evidence
and its frozen hashes.

Future production acceptance still needs actual account/subaccount identity and economics,
authenticated private subscription success/empty-state semantics, fee/collateral/margin and system
history coverage, and written venue clientId uniqueness/reuse/retention and replication contracts.
Official material already establishes several schema and product facts; the protocol review records
those facts instead of treating all questions as credential-dependent. A signed account capture can
observe its own state but cannot prove a global uniqueness or retention guarantee. Funding estimate
units remain unknown; signed settled funding amounts are distinct. Production authority, continuous
reconciliation, long-outage history recovery, power-loss durability and unbounded journal growth
acceptance remain outside this synthetic tranche.
