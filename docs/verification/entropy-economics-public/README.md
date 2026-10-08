# Native Entropy economic protocol preparation

Research for [native issue 102][issue] on 2026-10-08. Only public issue, official Hyperliquid
documentation, and the official Python SDK were read. No account/address queries, secrets, exchange
actions, or tracked worktree edits. Source bytes are independently retained under `sources/`;
`provenance.json` preserves original UTC/hash for twelve locally copied earlier captures and records
three new captures. Existing material was rehashed rather than downloaded again. SDK revision:
`2fdb18f9517675ea03695a0962bd19eece9c83f0`.

## Requests, ownership, and actual amounts

All REST requests below use POST `https://api.hyperliquid.xyz/info`. `user` means the verified actual
account address, not an agent signer. The reviewed user economic endpoints do not document a `dex`
selector; responses must be filtered by exact coin identity where present, not assumed to be io.
See [perpetual info][perps], [general info][info], and [SDK request builders][sdk-info].

| Purpose | Request fields | Economic interpretation |
| ------- | -------------- | ----------------------- |
| Public historical rate | `type=fundingHistory`, `coin=io:SNDK`, inclusive `startTime/endTime`. | Rate history; no account receipt. |
| Actual funding history | `type=userFunding`, `user`, inclusive `startTime/endTime`. | Signed account funding facts. |
| Other account ledger | `type=userNonFundingLedgerUpdates`, `user`, inclusive `startTime/endTime`. | Deposits, transfers, withdrawals, liquidations and other variants. |
| Actual fill/fee history | `type=userFillsByTime`, `user`, inclusive `startTime/endTime`, `aggregateByTime=false`. | Individual trade and fee facts, subject to retention. |

Actual REST funding examples contain `time`, `hash`, and
`delta: {type, coin, usdc, szi, fundingRate, nSamples}`. Preserve signed `usdc` exactly, including
positive, negative, and explicit zero. Missing amount is not zero. Exact `io:<asset>` plus verified
instrument metadata establishes the instrument/dex; an unknown or different coin does not.
Parse string or JSON-number amounts losslessly from their decimal representation; do not introduce
a binary floating-point intermediate. Raw numeric fields are not permission to round cash amounts.

Hourly funding mechanics use the settlement-time position and oracle, not the mark price. Public
current rates, historical rates, and predictions are distinct from actual payments. `predictedFundings`
is documented only for the primary perp DEX. Never manufacture actual income from a rate multiplied
by the present position, apply a current multiplier to an already reported payment, or infer no
payment from missing market predictions. See [funding mechanics][funding] and [perpetual info][perps].

For supported io instruments, currency interpretation must use validated canonical USDC collateral
metadata and supported account mode, as established by issue 100. The legacy field name `usdc`
alone is not a proven universal rule for arbitrary non-USDC HIP-3 collateral. Such an asset or mode
is unsupported/Unknown unless separately established. Shared balance displays are not additional
funding receipts; do not aggregate the same account economics once per DEX state.

Actual funding records do not supply settlement oracle/notional or an exchange position identifier.
They cannot satisfy a domain event's missing mandatory fields by filling zero, borrowing a current
book price, or fabricating a position ID. Preserve the ledger fact with optional context/Unknown
and emit a richer settlement event only when all of its required facts are evidenced.

## Trade fees and other ledger facts

Fill fields include `coin`, `px`, `sz`, `time`, `oid`, `tid`, `hash`, `crossed`, `fee`, `feeToken`,
and optional `builderFee`. Positive fee is a charge; negative fee is a rebate. `builderFee` is already
included in `fee`, so adding it again double counts. Account fee schedules, growth mode, deployer
scale, referral and staking discounts are estimates; the actual fill fee is the evidence of the
trade's charge/rebate. Normalize exact decimals and validate the fee token rather than assuming
that it matches the instrument's quote currency. See [fill contract][info], [WS fills][subs],
and [fee mechanics][fees]. Do not create an additional ledger charge for a commission already
accounted for by the same fill.

Documented non-funding ledger variants include deposit, withdraw, internal transfer, subaccount
transfer, liquidation, vault actions, spot transfer, account-class transfer, spot genesis, and reward
claim. Preserve variant-specific fields and raw facts; first revision need not interpret every variant.
See [ledger schemas][subs].

- Account-wide records such as deposit/withdraw lack an io coin or DEX in the schema. Keep
  `dex`/instrument unset; assigning them to io because the adapter selected io fabricates attribution.
- Transfers contain participants/destination, amounts, and sometimes fees or a spot token. Source
  ownership is the validated request/subscription account; a participant field is not a replacement
  for that ownership. A shared/internal transfer is not automatically net income.
- For withdrawal/internal-transfer `usdc` versus `fee`, the reviewed schema does not define whether
  principal is gross or net of fee. Do not subtract a second fee or invent its sign relationship.
  Keep components explicit and mark economic normalization Unknown until established.
- `liquidation.accountValue` is account value, not documented cash income; its positions may identify
  multiple instruments. Unknown variants, tokens, ambiguous directions, or unrecognized identifiers
  require diagnostics and raw fact preservation, rather than silent drop or coerced zero.

## WS envelopes, snapshots, and source timing

Subscribe using `method=subscribe` and subscription `{type:userFundings,user:<account>}` or
`{type:userNonFundingLedgerUpdates,user:<account>}`. There is no documented subscription DEX selector.
The [official SDK][sdk-ws] routes both channel names using `data.user`, so validate its exact account
before consumption. The [official WS example][sdk-example] subscribes to both without a signer.

The funding item schema is `time`, `coin`, `usdc`, `szi`, `fundingRate`; no per-item hash or unique
event ID is specified. The non-funding ledger item has `time`, `hash`, `delta`. The reviewed Gitbook
names the outer `WsUserFundings` and `WsUserNonFundingLedgerUpdates` types but does not fully declare
their array wrapper schemas. Existing/native wrapper fields must be labeled model evidence until
confirmed by actual venue observations; synthetic fixtures do not close that observation gap.
See [subscriptions and items][subs].

Funding is a snapshot followed by hourly updates; the general streaming rule distinguishes first
`isSnapshot=true` from later `false`. Missing snapshot flags should retain Unknown delivery kind.
A generic subscriptionResponse confirms a subscription, not persistence or complete history.
The `userEvents` channel can also carry funding, without a user echo; bind it to one verified account
socket or prefer the echoing funding feed, and avoid independently applying both copies.

The event's venue `time` is the occurrence/settlement time. Record receive time and connection
generation separately. A late historical funding receipt is still a historical fact; book freshness
rules must not erase it. An hourly feed's silence is not proof of zero income or immediate failure.
Missing source time remains Unknown. Empty snapshots have no individual source timestamp and do
not establish an atomic current balance or indefinite recovery completeness.

Disconnect may happen without announcement. Reconnect snapshots and REST history support recovery,
but the reviewed contract provides no gap-free private event cursor or per-event consumption ACK.
Snapshot replay must pass through the same identity/receipt logic as live and historical facts.
See [connection contract][ws] and [heartbeat contract][heartbeats].

## History, identity, and coverage limits

The general API contract returns up to 500 elements or distinct blocks for time-range queries and
instructs pagination from the last returned timestamp. Start and end are inclusive. Preserve the
boundary overlap, deduplicate evidenced replay, and do not advance blindly to `lastTime+1`, which
can lose distinct events at the same millisecond. Validate ordered/in-window responses; bound page
count, record count, bytes, and elapsed time. Saturated equal-time boundaries or no progress must
report incomplete/Unknown coverage. A single successful HTTP response does not prove a full window.
See [pagination][info].

Fill-specific limits are at most 2000 per response and only the most recent 10000 fills for time
queries. Funding/non-funding ledger retention duration and equal-timestamp overflow behavior are not
specified in the reviewed contract. An old requested window returning empty is not proof of
zero lifetime activity or unlimited retention. Record requested/observed intervals, pagination
termination, truncation suspicion, errors, and recovery gaps without inventing coverage.

REST funding/ledger have hashes, but uniqueness per user event is not promised: one transaction can
have several economic effects. WS funding lacks the hash entirely. Suggested local policy:

1. Preserve canonical decimal facts, raw payload hash, account/network, category, venue time,
   evidenced coin/currency/participants/nonce, transport source, and original event hash when present.
2. Use an evidenced composite identity rather than hash alone or `(time, amount)`. The same identity
   with a changed payload is a conflict requiring diagnostics, not last-write-wins or a second charge.
3. Different hashes/types/coins/participants at the same time and amount are distinct facts. Identical
   no-ID facts can be either replay or separate events; the reviewed protocol cannot resolve that
   ambiguity by fingerprint alone. Mark weak identity/conflict and avoid silently claiming exact-once.
4. Correlate WS/history overlap only when facts support the relationship. A local delivery sequence
   identifies an observation, not a stable venue event. Do not use connection generation in the stable
   receipt key, or every reconnect replay becomes a new payment.

## Journal/receipt acceptance recommendations

The venue provides no durable-consumer receipt contract. Application exact-once behavior therefore
requires an explicit local journal and receipt policy, with protocol ambiguity retained:

- Separate observed/raw, normalized, queued/emitted, consumed, and durably persisted states.
  Queueing/emission must not advance the consumed checkpoint.
- Journal append and durable receipt/checkpoint ordering must recover both crash-before-consumption
  and crash-after-consumption-before-transport-ACK. Persist identities needed for the bounded recovery
  window; an in-memory FIFO alone cannot prove restart recovery.
- Bound journal/event retention and report when recovering beyond that bound; do not turn eviction,
  page exhaustion, receiver failure, or conflicting identity into a complete coverage claim.
- Exercise positive/negative/zero funding, exact decimals, missing required fields, wrong owner/coin,
  unknown currency/variant, same-time distinct facts, replay/conflict, snapshot/history/live overlap,
  full boundary pages, truncated history, disconnect, and restart. Track receipts independently from
  emitted counts. Label all peer data synthetic; no real private settlement observation is claimed.

This is preparation only. Native implementation starts after issue 101 is merged into a fresh branch.
Unknowns remain: arbitrary-collateral `usdc` units, outer wrapper observation, event uniqueness and
hash semantics, funding/ledger retention, transfer gross/net fee semantics, publication lag, and
end-to-end private consumption/recovery in an actual account.

[issue]: https://github.com/mmyyrroonn/nautilus_trader/issues/102
[perps]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
[info]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
[funding]: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding
[fees]: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees
[subs]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
[ws]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
[heartbeats]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats
[sdk-info]: https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/2fdb18f9517675ea03695a0962bd19eece9c83f0/hyperliquid/info.py
[sdk-ws]: https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/2fdb18f9517675ea03695a0962bd19eece9c83f0/hyperliquid/websocket_manager.py
[sdk-example]: https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/2fdb18f9517675ea03695a0962bd19eece9c83f0/examples/basic_ws.py
