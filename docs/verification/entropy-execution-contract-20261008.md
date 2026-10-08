# Entropy bounded execution contract research

Issue: [native 101](https://github.com/mmyyrroonn/nautilus_trader/issues/101).
Implementation starts from native main `95fab86a26781c1f976f93ab84c43fac74eb9d2b`.
Research captures are public documentation and pinned SDK source, never private venue observations.
The separate acceptance record identifies the eventual implementation and installed candidate.

## Existing code and the required boundary

The adapter already has HIP-3 asset numbering, signing, ordinary submit/cancel, status/fill parsing,
and dex-aware activity recovery. Issue 100 added strict explicit io account identity and exact
financial facts while keeping io execution read-only. Reuse these implementations. An explicit finite
policy is required to add single GTC limit, caller-priced IOC limit, owned single cancel, and owned
reduce-only IOC close. Complex orders and external ownership adoption remain outside this scope.

The old network epoch-bound send checks a generation before `SinkExt::send`. `SplitSink` can then
await readiness and buffer a message before handing it to the actual backend. A useful final
admission must run after **actual backend** readiness and synchronously around one backend
`start_send`, with account/lifecycle/cancellation linearization. Flush and remote acceptance remain
separate evidence. This is a local implementation requirement, not a venue finality guarantee.

## Official asset and action facts

- Preserve null `perpDexs` slots and original io universe indexes. A HIP-3 action asset is
  `100000 + dex_index * 10000 + universe_index`; ticker arithmetic cannot replace metadata.
  [Asset IDs](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids).
- `marginMode: strictIsolated` differs from `noCross`. Deprecated `onlyIsolated` does not distinguish
  them. Closing releases strict-isolated margin proportionally; it does not establish current user
  leverage or trading permission.
  [Perpetual metadata and active asset data](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals),
  [margining](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margining).
- `activeAssetData` supplies user/coin, isolated leverage, trade-size/available arrays, and mark price.
  The documented REST response has no venue source timestamp. Bind its request to the actual user
  and exact coin, record request/receive bounds, and preserve source time as unknown. Two matching
  leverage observations are a bounded, non-atomic verification, not a private live observation.
  Metadata `maxLeverage` is not actual user leverage. A full-notional local reserve plus fee/margin
  buffers remains an estimate; isolated total maintenance is unknown.
- Perpetual quantity precision is `szDecimals`; price has at most `6 - szDecimals` decimal places
  and five significant figures, with an integer exception. Reject invalid exact wire values rather
  than widening a caller cap/floor through normalization.
  [Tick and lot size](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size).
- Single limit/IOC fields are `a,b,p,s,r,t.limit.tif,c`. `Ioc` cancels unfilled remainder and does not
  prove full execution. Ordinary minimum perpetual order value is USD 10; a smaller full-close
  exemption has not been established by the reviewed sources.
  [Exchange actions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint),
  [errors](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses).
- A sell limit provides a minimum execution price, not a maximum short notional. A policy's local
  reserve and fresh reference prices must not be described as a hard upper bound on every possible
  favorable sell execution after a market gap.

## Confirmation, actual fills, and recovery

WS post responses correlate by request ID and connection generation. Outer success requires a
recognized single-action response with the correct cardinality. Resting OID, filled aggregate,
explicit error, missing/unknown status, and transport failure have different meanings.
[WS post contract](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests).

`Filled` status and aggregate filled ACK lack individual trade identities, timestamps, and fees.
They cannot produce actual fills. Raw fill identity and canonical economic payload must be checked
before legacy dispatch deduplication, including after terminal status. Preserve rebates and exact
fees; builder fee is already included in reported fee.
[Fill endpoints](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint).

Official `userEvents` publishes on channel `user`; its payload and `orderUpdates` do not echo the
user/dex. Bind them to one verified private user and the originating epoch, exact io coin, and
durable owned intent/OID/CLOID. `userFills` echoes the user and carries snapshot information. Other
coins and unrelated orders cannot become this strategy's ownership.
[Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions),
[pinned SDK routing](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/2fdb18f9517675ea03695a0962bd19eece9c83f0/hyperliquid/websocket_manager.py).

An absent ACK, missing-order cancellation error, or `unknownOid` does not prove rejection or
flatness. Preserve the original intent and reserves, query actual-user CLOID/OID and explicit io
open orders/positions plus actual fills, and exhaust finite retry/time/history limits as Unknown.
An empty fill array has no general gap-free coverage cursor. Recent history is bounded; capped
pages, unverified windows, source mismatch, conflicts, or external exposure cannot become Complete.

Durable ownership must precede enqueueing. Crash recovery of an old Prepared record is Unknown,
because a write could have occurred before a later state record. A local append/queue event is not
durable consumer application; economic delivery/acknowledgement is handled in the following issue.

The finite implementation binds a signed `expiresAfter` to the absolute action deadline and
persists nonce, post ID, epoch, action token and independent action/signed-payload/frame digests
before queueing. The existing signer and default network transport are reused. Local timeout
after `start_send` does not prove rejection, even with a signed expiration.

A private receive callback registers an epoch-scoped contiguous sequence before enqueueing.
Its actor marker advances the applied sequence only after the frame's outputs have been handled.
Final admission holds this receive gate through actual `start_send`. Pending or abandoned
frames therefore close admission. This covers callbacks already observed locally, not bytes
still in the network or an atomic exchange account snapshot.

The file journal is finite, synchronized and exclusively leased by one runtime. It validates
identity, immutable ownership and economic history; incomplete delimiters and overflow remain
blocked. Process restart is covered, but no power-loss directory-entry persistence guarantee is
claimed. A fresh native cache with durable actual fills or still-active intents is not presumed
restored: the explicit native projection barrier remains incomplete until an independently
verifiable restoration mechanism exists. The initial finite scope does not implement such a
mechanism. This is a limitation, not a successful recovery or permission to close.

## Captures and observation level

Source hashes and UTC acquisition times are in the adjacent execution-public provenance files.
Earlier captures keep their original acquisition times. Local source inspection before issue 100
was merged is explicitly historical, and does not identify this candidate. No private account,
mainnet trade, real funding receipt, or venue entry/close is established by this research.
