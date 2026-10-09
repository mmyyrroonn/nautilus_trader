# io startup source contract — native #115

This change lets the normal Hyperliquid `generate_mass_status` startup path reuse an already completed, strictly sealed native `io` account observation. It removes one redundant full account group only while that original source and every startup qualification remain current. It does not repair old journals or promise completion of the bounded two-venue application run.

This contract describes implementation `617ffd42eeed6e8e5fff2ec36c10e25f0de1fcbe`, tree `834cd4329cbbd8c1ba7f4fe317b0d18a887f8e91`, based on freshly fetched main `aadad78fab77467f4a1467a252947ebb84f5361d`. The production changes are confined to `crates/adapters/hyperliquid/src/account_scope.rs` and `execution_scope.rs`. [Acceptance and limitations](io-startup-source-acceptance-20261009.md) are separate from this source contract.

## Original transaction seal

The private `FullAccountSourceSeal` is created inside a successful actual full account transaction. That transaction reads `userRole`, both initial account-mode facts, `meta` for `io`, `spotMeta`, the complete `io` clearinghouse state, then both account-mode facts again. Repetition bounds observed mode transitions; it does not make these independent requests atomic.

The seal retains the actual request JSON, bounded raw response bodies and native transport-receipt milliseconds. It keeps the original verification start, actual final verification end, clearinghouse source time and clearinghouse receipt time. It also captures immutable execution policy, reader generation, epoch, received sequence, account and hard revisions, full private financial/position facts, HTTP native instrument objects and the original metadata context. This context comes from successful normal metadata verification and its once-only normal Engine instrument binding; account trust or a diagnostic cannot retrospectively manufacture it.

The optional sealing qualification is stronger than ordinary legacy account publication. Initial and final ingress must be fully applied in the same reader/epoch with unchanged received sequence. Complete private balances, equity, withdrawable, used/free margin, cross-maintenance and positions must agree with the actual HTTP facts. Selected metadata rows, native asset mapping and full original native objects must remain compatible. Exact account amounts must round-trip through native USDC precision. The Engine half of the original binding is checked again by the owner-side startup boundary.

All candidate raw/context/account copies and serialization happen before the final seal time sample. Under ingress → account guards, original start/source/receipt freshness and current source trust are checked again. The actual last verification end is recorded and the candidate is moved into state. There is no renewal of original source or receipt time. A crossing identical private frame can leave the ordinary account trusted while preventing this stricter seal.

## Startup eligibility and fallback

Reuse requires an explicit `IoExecutionPolicy` and genuine fresh journal origin: the descriptor originally locked by `new()` contained zero bytes. A subsequent empty record persisted during normal connect does not erase that origin; restarting from any nonempty journal does not create it.

Startup requires no native actions, intents, actual fills, reservations, taint, recovery-source debt or native projection-recovery requirement, plus current recovery and epoch. The current complete `io` account must be trusted and flat with valid mode, role, collateral and exact selected metadata/native instrument bindings. Normal Cache history is examined across all strategies, including closed and external records. Same-account orders or positions disqualify startup. Account-less routed Hyperliquid `io` order history is also rejected, including unselected `io` instruments; unrelated venues/accounts remain outside that exclusion.

A seal is usable only if its original policy, reader/epoch/received/applied identity, revisions, complete private facts, metadata context and native objects still match and its original times remain fresh. An identical applied WS frame invalidates reuse through sequence comparison even when quantities and ordinary account trust do not change.

When no usable seal exists but the independent startup eligibility still holds, startup can perform the ordinary complete full account read. **That fallback must itself produce a new qualified seal.** An unsealed legacy-trusted result cannot generate startup flat reports. Thus a newly observed selected precision, isolated-mode or metadata contradiction cannot pass through a weaker fallback account publication. A crossing frame, stale source, missing qualified origin, partial response or conflicting source yields finite failure; it does not authorize a retrospective seal or clear debt.

## Actual history and final report boundary

Every successful call, including repeated calls that reuse the same seal, still fetches all three real sources:

1. `frontendOpenOrders` explicitly scoped to `io`;
2. unfiltered `userFills` for the account;
3. unfiltered `historicalOrders` for the account.

Each must be a successful bounded explicit empty array. `null`, malformed, nonempty, foreign or incomplete evidence is refused. No filtering hides history to infer empty startup. Receipts must belong to the current observation interval.

The final owner boundary holds ingress → account → execution guards without an await. It repeats source identity, policy, reader/applied sequence, revisions, full private facts, metadata and exact HTTP/Engine instrument checks, journal/debt eligibility and ordinary Cache-history exclusions. The finish closure performs large comparisons and constructs the report under those unchanged guards. A final no-allocation scalar time qualification then rechecks live epoch/applied state, source/private freshness, selected metadata TTL, original seal times and the same original total deadline immediately before returning the result. Large serialization is not postponed until after this last sample.

The returned `ExecutionMassStatus` contains one native `Flat` `PositionStatusReport` per selected instrument, using its exact quantity precision and actual account/client/venue identities. Its source timestamp and report-window start remain the original clearinghouse source and verification start. `set_report_window(Some(original_start), false)` explicitly leaves historical/report completeness unproved. Empty orders/fills are supported by the three actual successful requests. Reuse emits no additional `AccountState` merely to renew freshness, introduces no orders or fills, adopts no Cache financial facts, releases no reservations and extends no economics coverage.

## Bounds and diagnostics

The change does not increase configured TTLs, deadlines, action limits, journal limits or weighted quota. The application's existing account age is 2 seconds, metadata age 30 seconds and recovery deadline 3 seconds; the limiter remains 1,200 tokens/minute with 20 tokens/second refill. Different explicit policies, such as the independent readonly procedure's 30-second account TTL, remain distinct evidence.

Opt-in account bodies use the existing 1 MiB per-body and 4 MiB retained-source-group bounds. Startup history uses its existing bounded transport. These finite scopes do not claim that every unrelated legacy HTTP path has a global memory limit. Reusing the complete account group avoids its source-derived minimum weight of 182; this is neither a measured limiter balance nor proof that a later cleanup fits the remaining quota/deadline.

`startup_account_source` and `startup_private_ingress` expose provenance and actual generation/epoch/received/applied diagnostics. Retained or non-null diagnostics are not authorization, nor an independently asserted flag that a particular installed call took the reuse branch. Existing lifecycle invalidation, financial/hard revocation, debt conservation and warm-recovery raw retention remain applicable. No old failed-source debt is forgiven by this optimization.

## Finite scope

The contract proves a selected current fresh-flat startup projection at its final local boundary. HTTP role/mode/metadata/history reads remain non-atomic, and silent unrequested remote changes remain unobservable. Exact private mutex-wait startup integration and isolated unapplied-ingress composition are not separately certified by the final installed readonly run.

This work does not certify generic cold native Cache restoration, arbitrary post-prepare cross-thread Engine Cache mutation, native Portfolio authority, continuous or kernel isolation, whole-account/history completeness, funding or run economics/PnL, real venues or mainnet capital. App Cache/Portfolio and durable-intent gates remain separate. The corrected paired run executed four native fills and returned local positions to zero, but failed to obtain a fresh complete cleanup proof within the original warm deadline. Native #115 therefore does not close application #43 E6, real-capital acceptance or other outstanding issues.

Evidence is indexed by the [local archive manifest](io-startup-source-local/manifest.json). The acceptance document retains failed attempts and distinguish source review, compiled native tests, installed readonly behavior and the failed application run.
