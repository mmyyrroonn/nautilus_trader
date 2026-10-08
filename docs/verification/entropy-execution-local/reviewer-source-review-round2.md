# Independent execution review: second implementation pass

Reviewed on 2026-10-08 in `E:/persarb/worktrees/entropy-account`, based on latest main
`95fab86a26781c1f976f93ab84c43fac74eb9d2b`. Implementation was changing during this review.
No Cargo command, tracked edit, commit, private account request, or exchange write was performed.
This is a source review and peer-test request, not a candidate acceptance verdict.

## Adapter observations sent directly to the implementation agent

The initial `execution_scope.rs` draft used unchecked Decimal addition, subtraction, multiplication,
and absolute values in ownership, pending closes, reserved funds, gross exposure, and free funds.
Missing gross metadata used `Decimal::MAX` as a multiplier. Policy inputs can contain large exact
Decimals, so arithmetic overflow must produce an explicit admission error rather than unwind a
handler or writer callback. Validate metadata precision before subtracting it from six. Validate
positive actual leverage and positive reference prices on every proof path.

Order reservation uses quantity times `max(symbol.max_price, mark_price)`, plus the fee buffer,
without leverage division. This conservatively addresses the short sell-floor issue at admission;
it is an estimate, not a guarantee about future price movement. Gross exposure needs a fresh,
complete price proof for every nonzero position, not merely the new order's instrument. Missing or
stale price evidence must close admission without a sentinel multiplication.

The first journal loader validated outer account/strategy identity and finite counts but omitted
internal ownership and financial invariants. Requested checks include map key/CID/CLOID/native coin
consistency, positive quantity/price, nonnegative reservation, cumulative fill bounds, owned fill
OID/coin/side, stable fill identity and immutable history. A journal also needs an exclusive writer
lease before loading: two clients must not independently load the same empty history and admit
orders against separate reservations. During this review the agent added `File::try_lock` and
`validate_journal`; these additions have not yet been compiled or independently exercised.

Metadata refresh must bind its initial account epoch/revision and its own execution generation to
the final commit. A slow old refresh must not overwrite newer metadata after reconnect, account
invalidation, or another refresh. The agent acknowledged and is implementing this check.

REST `activeAssetData` validates exact decimal-string capacities, integer actual user leverage,
isolated mode, native coin, and queried user. The total verification interval and receipt age are
bounded; absent source time remains unknown. I initially questioned `markPx` using the older
WebSocket DTO; that concern is withdrawn. I independently read the captured official REST examples
in `issue100-official-contract-20261008/perpetuals.md`, which include string `markPx` for ordinary and
HIP-3 perps. The [official REST documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)
and [official SDK types](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/types.py)
also support this field. A stale legacy WebSocket DTO is not evidence against the REST contract.

ACK decoding should require one unambiguous status shape. `resting` plus `filled` or `error` must
remain unknown. Late placement ACKs must not regress a state already established by actual fills,
and cancel success must bind the current pending cancel action rather than a boolean alone.
ACKs must never restore recovery completeness. The final callback must verify signed action fields
against durable intent and reservation: asset, coin, CLOID, exact price/quantity, reduce-only, TIF,
nonce, expiry, and payload identity.

The raw fill application and complete recovery path were not yet present at this sampling point.
The first review's stable venue tid plus payload-conflict checks remain required before legacy
dispatch, including after terminal order state. No synthetic fills from aggregate ACKs are allowed.

## Network observations sent directly to the implementation agent

The new `PreparedTransport` wraps the actual boxed backend before `SplitSink`. Its admission
callback is invoked when the wrapper reaches backend `start_send`, after backend readiness, rather
than when the split sink accepts into its local slot. This addresses the earlier false readiness
boundary in the proposed architecture; successful tests are still required.

Control cancellation and the continuation use the same lock. `MayHaveWritten` is recorded before
calling the underlying sink, including an immediate sink error. Pending timeout paths retire a
transport that may retain a local slot; prepared messages have no reconnect replay path. Requested
additional tests cover an earlier ordinary slot, payload mismatch, cancellation after taking the
prepared slot but before consuming the continuation, admission panic before/after consumption,
and old-writer close during replacement after timeout. Count backend handoffs and received actions.

Reusing an already claimed control currently returns `NotWritten` independently of its previous
outcome. Requested an explicit already-used error or preservation of the original outcome, so
adapter bookkeeping cannot interpret an already possible write as definitely unwritten.

## Independent execution still outstanding

No native binary or wheel from this implementation has been reviewed or run by this reviewer.
After implementation stabilizes, review callback lock order, immutable payload binding, timeout/drop
classification, actual fills/fees, journal restart and conflict handling, bounded recovery, actor
entry points, and normal factory/wheel behavior; execute the supplied stable binary cases without
sharing a Cargo build. No PASS or merge recommendation is issued for this draft.
