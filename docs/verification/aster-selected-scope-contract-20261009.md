# Bounded Aster selected-scope admission

The ordinary Aster execution client can opt into a current selected-instrument proof through
the trailing `selected_scope_policy_json` configuration field. Its default is `None`, preserving
the existing constructor prefix and execution contract. The proof is additional native admission
for new risk. Applications must still verify their own order ownership, current Cache and Portfolio.

```python
policy = {
    "instrument_ids": ["SNDKUSD1-PERP.ASTER"],
    "balance_asset": "USD1",
    "max_age_ms": 2000,
    "max_refresh_ms": 15000,
}
```

Serialize this dictionary as JSON and pass it to `AsterExecutionClientConfig` with explicit user,
signer and signing credentials and `BinanceInstrumentProviderConfig(load_all=False, load_ids=[...])`.
The policy accepts one to eight unique complete instrument IDs, exactly equal to the configured
finite load IDs. Unknown or duplicate fields, floats, booleans and out-of-range integer bounds are
refused before connection. Both time bounds accept 1 through 60000 milliseconds. The balance asset
must be a bounded uppercase ASCII asset code and match every selected native instrument's settlement
metadata. Account identities must be canonical hexadecimal addresses. Position-mode exemptions and
credential-bearing, query-bearing or fragment-bearing source URLs are refused for this opt-in.

## Current proof and diagnostics

Normal connection and `QueryAccount` perform existing native order/fill recovery and read actual
one-way position mode, balances, explicit selected `positionRisk` rows and open orders. Every selected
position needs an explicit valid row; a missing row does not mean zero. Exact raw decimals must fit
their native quantity, price and currency precisions before projection. Money round trips exactly;
unsupported precision, missing original commission or incomplete cumulative fill coverage closes
the proof rather than rounding amounts or manufacturing fills.

Register the ordinary `AsterExecutionClientFactory` with the node and retain that same factory.
`factory.selected_scope_snapshot_json()` returns `None` without a live opted-in client, otherwise
a detached JSON diagnostic. Its client lifetime is weak: background tasks or retained diagnostics
do not keep the created client alive. Replacing a live optional binding is refused.

The diagnostic identifies account, venue, explicit user/signer, source endpoint, selected instrument
IDs, settlement asset, exact positions, original balance strings and open order IDs. It exposes
generation, stream state, mode verification, recovery completion, `fill_chronology_debt`, local
receive time, TTL and Ready/trusted state. `whole_account_verified`, `run_ownership_verified` and
`funding_verified` remain false.

`source_time_ns` is null and `source_time_origin` is
`venue_does_not_supply_snapshot_time`. Venue `updateTime` values remain individual row mutation
metadata. Local received age uses the earliest completed authorizing HTTP read; it is not a claim
about the venue's remote snapshot clock. A snapshot is useful only while the same generation and
instrument metadata remain current, the stream is alive, and native recovery has no debt.

## Revocation and actual send

Every private raw text/binary ingress revokes the old witness before typed decoding and queueing,
including malformed data and reconnect markers. Financial/order changes, explicit queries, stop,
stream loss, Unknown submissions and recovery debt also revoke it. Ping/pong handling is unchanged.
The shared Binance stream observer defaults to absent, preserving ordinary adapter behavior.

New-risk submission checks the witness before admission and again after cooldown, quotas and
signing. The final pre-write check atomically consumes it once. A successful or ambiguous network
write cannot reuse the witness for another entry. Existing cancellation and reduce-only restrictions
remain active; this interface does not grant strategy ownership or a separate executor.

## Chronological recovery

Ordinary Aster recovery now sends distinct order bundles by actual first trade time, rather than
hash-map order or venue order ID. Within each bundle, actual trade time and trade ID determine order.
This fixes the reproduced close-before-open recovery while retaining original commissions and IDs.

The optional selected proof refuses overlapping cross-order intervals and equal-millisecond order
boundaries before publishing any affected bundle. A later previously unapplied trade at or before
the symbol's applied time frontier also refuses evidence. Both cases retain pending order/trade IDs
and set sticky `fill_chronology_debt` for this client lifecycle. Normal account queries, targeted
recovery, reconnects or a flat position row do not clear it. History checkpoints cannot advance past
that debt. Recreating the client does not establish durable cold Cache recovery or historical PnL.

This conservative contract covers current selected inventory and admission. It does not certify
whole-account coverage, funding completeness, run-owned history, arbitrary interleaved default
history PnL, cold native Cache restoration, Windows kernel network isolation or real venue behavior.
The Entropy application's normal io startup reconciliation is a separate Hyperliquid dependency;
turning off node reconciliation is not an acceptance path.
