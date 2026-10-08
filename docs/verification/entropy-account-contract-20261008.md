# Entropy account contract evidence, 2026-10-08

This is public documentation research for native issue 100. No user address was queried, no account
or key was read, and no exchange action was submitted. Proposed adapter policy is distinguished
below from venue guarantees. Source acquisition timestamps and hashes are in
[public provenance](entropy-account-public/provenance.json); the official SDK revision is
`2fdb18f9517675ea03695a0962bd19eece9c83f0`.

## Account mode and balance ownership

POST `https://api.hyperliquid.xyz/info` with `{"type":"userAbstraction","user":"<account>"}`.
The documented response is one of `disabled`, `default`, `dexAbstraction`, `unifiedAccount`, or
`portfolioMargin`. The legacy query is `{"type":"userDexAbstraction","user":"<account>"}`;
its documented example is a JSON boolean. See the [info contract][info] and
[official SDK queries][sdk-info]. Neither request needs a signer.

| Mode | Official accounting rule | Proposed first revision |
| ---- | ------------------------ | ----------------------- |
| Standard/manual | Separate spot, primary perp, and individual DEX balances. | Support explicit `disabled` only. |
| Unified | One balance per collateral asset, shared with spot. | Unsupported for readiness. |
| Portfolio | Eligible assets and cross positions share portfolio margin. | Unsupported for readiness. |
| Legacy DEX abstraction | Automatically moves HIP-3 collateral from primary USDC perps or spot. | Unsupported. |
| `default` or unrecognized/missing result | Effective mode is not defined in the reviewed contract. | Unknown; never infer standard. |

The `disabled`/standard correspondence is an inference from the three current accounting modes
and the three SDK/action settable values (`disabled`, `unifiedAccount`, `portfolioMargin`), not a
documented normalization rule for `default`. Requiring the additional legacy result to be exactly
`false` is a conservative consistency check; it is not an official requirement. False by itself
does not establish standard mode. Query errors, nulls, and contradictory results remain Unknown.
See [account modes][modes], [abstraction actions][exchange], and [SDK wire mapping][sdk-exchange].

For unified/portfolio mode, [spot clearinghouse state][spot] is the balance source of truth across
spot and perps. Do not add primary, io, and spot balances. Portfolio borrowing, interest, collateral
valuation, and cap fallback require additional accounting; a balance sum cannot implement them.
See [portfolio margin][portfolio]. For standard mode, retain separate scoped balances even when
their token is USDC; a primary balance does not fund io readiness.

## Identity and collateral

`{"type":"userRole","user":"<account>"}` returns `user`, `missing`, `vault`, `agent` with
`data.user`, or `subAccount` with `data.master`. Read state using the configured actual account
address. An agent is a signer, and querying its address can return empty state. Its owner does not
identify which subaccount the operator intended. Do not silently replace the configured target.
See [user roles][info] and [API wallets][wallets].

Proposed first scope: a verified actual `user` address. A subaccount requires explicit target/master
validation and should remain unsupported until that path is tested; vault, agent, missing, and
identity conflicts fail closed. Vault/subaccount action routing uses `vaultAddress`, separately
from signing identity; this research does not validate that action path. See [exchange routing][exchange].

Request `{"type":"meta","dex":"io"}` and `{"type":"spotMeta"}`. Resolve
`meta.collateralToken` by the token's explicit `index`, not array position. For current mainnet io,
the 2026-10-08 public capture used by issue 42 reports index 0. Verify the canonical USDC identity
`0x6d1e7cde53ba9467b783cb7c530ce054` against returned token metadata; the same ID appears in the
[official spot metadata example][spot]. A matching ticker alone is insufficient. Fresh metadata
must be validated again; this document does not claim a new io observation.

## Scoped REST and WebSocket state

Use the explicit REST request `{"type":"clearinghouseState","user":"<account>","dex":"io"}`.
Omitting dex selects primary `""`. The REST example contains `assetPositions`, margin summaries,
`withdrawable`, and source `time` in milliseconds. Bind a response to the requested user, dex,
connection generation, and request identifier; the REST body itself does not echo user/dex.
See [perpetual account summary][perps] and [SDK user state][sdk-info].

The current [WebSocket subscription contract][subscriptions] specifies:

```json
{"method":"subscribe","subscription":{"type":"clearinghouseState","user":"<account>","dex":"io"}}
```

The resulting channel is `clearinghouseState`; `data` wraps `user`, `dex`, and an inner
`clearinghouseState`. Validate both routing fields before updating io. `allDexsClearinghouseState`
instead wraps `user` and `clearinghouseStates` pairs keyed by dex string; absent io is Unknown.
The documented inner WS schema does not include source `time`, block height, or sequence.
Receiving a payload or pong does not prove source freshness. Missing source time must not be
replaced with receive time; a fixture that adds `time` does not establish actual venue support.

The SDK at the pinned revision lacks `clearinghouseState` subscription/response identifier routing,
although the venue documents the channel. Implement against the explicit venue contract, rather
than interpreting SDK omission as a venue prohibition. See [SDK WebSocket manager][sdk-ws].

The [connection contract][ws] says servers may disconnect without notice and reconnect snapshots
can recover missed data. The [heartbeat contract][heartbeats] specifies ping/pong and a 60-second
idle timeout. A subscription acknowledgement confirms subscription, not a current io snapshot.
No reviewed channel contract provides a monotonic cursor for gap-free private-state replay.

Proposed trust rule: disconnect, missing data, source staleness, or failed reconciliation invalidates
io readiness immediately. Start a new connection generation; require resubscription, correctly scoped
data, and a fresh explicit io REST reconciliation before restoring trust. Unscoped/default updates
cannot satisfy this gate. Mode and identity queries have no documented source timestamp either;
record their retrieval provenance without inventing source age or an atomic snapshot across calls.

An empty but complete, correctly scoped, fresh position array can establish observed flat positions
after identity/mode checks. It cannot establish funding or trading permission. A missing/malformed
array, wrong dex, wrong identity, or stale state is Unknown. If a strategy requires no pending
orders, reconcile `{"type":"openOrders","user":"<account>","dex":"io"}` separately; position
flatness does not imply no open orders. Do not use primary zero balance/positions as ioReady or Flat.

Actual private WS payload timing, account-mode transitions, and live vault/subaccount permissions
remain unobserved. There is no live account acceptance claim in this document.

[info]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
[modes]: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes
[portfolio]: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/portfolio-margin
[perps]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
[spot]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot
[exchange]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint
[wallets]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets
[subscriptions]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
[ws]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
[heartbeats]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats
[sdk-info]: https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/2fdb18f9517675ea03695a0962bd19eece9c83f0/hyperliquid/info.py
[sdk-exchange]: https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/2fdb18f9517675ea03695a0962bd19eece9c83f0/hyperliquid/exchange.py
[sdk-ws]: https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/2fdb18f9517675ea03695a0962bd19eece9c83f0/hyperliquid/websocket_manager.py
