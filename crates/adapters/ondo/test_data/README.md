# Ondo Perps adapter test data (P0)

This directory is the P0 (protocol + environment freeze) artifact for the Ondo Perps adapter.
It contains **fixtures and evidence only**. There is deliberately no `Cargo.toml`, no `src/`, and
no `tests/` here yet — Task 1 owns those. Nothing here has been validated against a live
authenticated endpoint.

- `manifest.json` — the machine-readable index: one entry per fixture with its kind, source,
  capture time and SHA256, plus the two repo HEADs, tool versions and the unresolved list.
  **Always read the fixture's `kind` from the manifest, not from its file name.**
- `conflicts.md` — the official-document conflict table (REST headers, WS HMAC order, FOK,
  `fill.direction`, market status, plus two schema-vs-observation gaps).
- `ws/` — WebSocket fixtures. `rest/` — REST fixtures (one synthetic, one observed).

## Fixture legend: `observed` vs `official-example` vs `synthetic`

| Kind | Meaning | Which files |
|---|---|---|
| `observed` | Captured from the real, unauthenticated **production** endpoint in an archived probe; raw bytes preserved. | `ws/topofbook_observed.json`, `ws/depth_observed.json`, `ws/trades_observed.json`, `ws/funding_observed.json`, `ws/subscribe_observed.json`, `rest/markets_observed_20260914.json` |
| `official-example` | Copied verbatim from an official Ondo document or spec example. **Not** real traffic; may be stale. | `ws/markprices_observed.json` **only** |
| `synthetic` | Authored for this phase from the official schema because no legitimate response could be obtained when it was written. Values are invented. | `rest/markets_synthetic.json` |

### The observed REST payload (`rest/markets_observed_20260914.json`)

`GET https://api.ondoperps.xyz/v1/markets` was refused with HTTP 403 during P0 (11:50:18Z), so the
only REST fixture then was synthetic. It answered later the same day: **2026-09-14T15:15:02.434934Z**
(see `fetched_at.txt` in the capture directory). This fixture is a **trimmed but faithful excerpt of
that response** — 4 of the 81 trading pairs it served, each pair copied verbatim:

- `NVDA-USD.P` and `TSLA-USD.P` — the two P1 targets, `disabled` **absent**, `tags: ["Stock"]`;
- `ENA-USD.P` — `disabled: true`, `tags: ["Crypto"]`;
- `BTC-USD.P` — a non-stock pair with no `schedule` object at all.

The excerpt keeps the venue's own envelope (`success` + `result.perps.tradingPairs`) and carries **no
`_fixture` block**, exactly as the observed WebSocket fixtures are bare payloads; the manifest entry
is the only statement of its provenance, and it is the authority on this file's kind. Only line
terminators changed (the capture is CRLF; this tree is LF). Its 81-market facts — 0 `status` strings,
20 `disabled: true`, 61 with `disabled` absent — are what resolved `conflicts.md` conflict 5.

**No credentials were used or captured.** The payload was fetched with the adapter's own public,
unauthenticated `OndoHttpClient` (`crates/adapters/ondo/src/http/client.rs`): one read-only GET, no
API key, no signature, no session, no cookie. As everywhere in this directory, no header, signature,
JWT, cookie, key or secret value appears.

Also note the one field this directory records but the adapter deliberately does not interpret:
`schedule` (the venue's market hours: `timezone`, seven `openHours` entries and a `holidays` list,
present on 70 of the 81 pairs). Closure is expressed through `disabled`; hours through `schedule`.
Interpreting the hours is a known, reported limitation — not a behaviour this phase invented.

Two naming caveats you must not misread:

1. `ws/markprices_observed.json` is **not observed**. The WS probe never subscribed to
   `markPricesPerps`, so no real frame exists; the file holds the WS-spec example. The name is
   fixed by the Task 0 layout, so the manifest kind is authoritative.
2. Observed fixture files are **bare protocol payloads** — no wrapper object, no added keys beyond
   the venue's own envelope — so they can be fed straight into a parser and their decimal lexemes
   stay byte-identical to the capture. Only `rest/markets_synthetic.json` carries an in-file
   `_fixture` block, because a synthetic file must be impossible to mistake for a response.
   `ws/subscribe_observed.json` is a JSON **array** of the four subscribe frames.

## Protocol table

`envelope` columns are derived from the observation; columns marked *(spec)* are spec-only and
have no observed sample. Every price/quantity/fee is a **decimal string** on the wire — never
parse it through `f64`.

### WebSocket — public channels (`wss://api.ondoperps.xyz/ws`, no auth)

| Channel | Direction | Schema summary | Exact field paths | Units / notes |
|---|---|---|---|---|
| `topOfBooksPerps` | client→server `{"op":"subscribe","channel":"topOfBooksPerps","markets":["NVDA-USD.P","TSLA-USD.P"]}`; server→client `type=subscribed`, then `type=update` *(spec)* | `BookSnapshot`, best level only in practice | `type`, `channel`, `timestamp` (observed, undeclared in spec), `data[]` → `data[].market`, `data[].time`, `data[].bids[][]`, `data[].asks[][]`, each level `[price, qty]`; optional `data[].depthLevels` *(spec)* | `price` = quote USD per base unit; `qty` = base units. `data[].time` = price event time (ns, RFC3339). Envelope `timestamp` = server send/batch time. One observed item per frame. |
| `depthBooksPerps` | `{"op":"subscribe","channel":"depthBooksPerps","markets":[...],"limit":10}` (optional `depthLevels` *(spec)*) | `BookSnapshot` with full bid/ask arrays | identical to `topOfBooksPerps`, arrays are longer | **`depthLevels` is a price-grouping value, NOT a number of levels** (spec example `"0.01"`). `limit` = max levels, `0` means unlimited *(spec)*. No exchange sequence number and no checksum anywhere in the frame. Each observed frame is a complete replacement of the covered range; `limit=10` proves at most 10 levels, never the whole book. |
| `tradesPerps` | `{"op":"subscribe","channel":"tradesPerps","markets":[...],"numPastTrades":0}` | `Trade` | `data[].market`, `data[].price`, `data[].size`, `data[].cost`, `data[].aggressor_side`, `data[].time`, `data[].id` | `price` USD/base, `size` base, `cost` quote (= price × size). `aggressor_side` ∈ {`buy`,`sell`} (snake_case on the wire). `id` = trade id, usable for dedupe. |
| `fundingRatesPerps` | `{"op":"subscribe","channel":"fundingRatesPerps","markets":[...]}` | `FundingRate` | `data[].market`, `data[].rate`, `data[].intervalEnds`, `data[].premiums[]` → `premiums[].market`, `.time`, `.mark`, `.bid`, `.ask`, `.premiumIndex` | **`rate` is an hourly decimal fraction.** Observed `0.0000063` = 0.063 bp/h (`×1e4` → bp/h; `0.0001` = 1 bp/h). Never divide by 100 and never multiply by 8. **`intervalEnds` is a settlement time** (observed `2026-09-14T12:00:00Z`), not the event time and not a `ts_event`. Premium samples are per-minute; `premiumIndex` is a dimensionless ratio. |
| `markPricesPerps` | `{"op":"subscribe","channel":"markPricesPerps","markets":[...]}` | `MarkPrice` | `data[].market`, `data[].markPrice` | **No observed sample in this phase.** `markPrice` is a quote-currency decimal string. Provisional only. |
| `kLinePerps` | subscribe *(spec)* | `Kline` | not analysed in P0 | out of the P1 scope |
| private channels (`ordersPerps`, `fillsPerps`, `positionsPerps`, `balancePerps`, `fundingPaymentsPerps`, `liquidationPerps`, `liquidationAnnouncementsPerps`, `marginTransfersPerps`, `ordersSummariesPerps`, `cancelAllOrdersAfterPerps`, `deposits`, `withdrawals`) | require `{"op":"login","args":{...}}` first | `Order`, `Fill`, `Position`, `Balance`, … | see `conflicts.md` conflicts 1–4 | **No fixture in P0.** Synthetic only, and only once a sandbox response exists; secrets must never be captured. |

Connection facts *(spec)*: 32 KB max message, 25 requests/second (burst 50), idle disconnect at
180 s, application-level heartbeat `{"op":"ping"}` → `{"type":"pong"}`.
Server message types *(spec)*: `pong`, `loggedIn`, `subscribed`, `unsubscribed`, `update`, `error`.

### REST (`https://api.ondoperps.xyz`)

**Network status.** `GET /v1/markets` and `GET /status` returned `HTTP 403` with body
`error code: 1010` and `Server: cloudflare` at 2026-09-14T11:50:18Z / 11:50:22Z, which is why the
first REST fixture was synthetic. The same day at **15:15:02Z** a second unauthenticated production
capture reached all three of `GET /status`, `GET /v1/markets` and `GET /v1/perps/contracts`, each
answering **200**. Its bodies are archived outside the fork under
`reports/ondo-acceptance/20260914T142730Z-p1-build/rest-capture/` (`status.json`, `markets.json`,
`contracts.json`); its own log records `wrote status.json: 28 bytes`, `wrote markets.json: 392020
bytes`, `wrote contracts.json: 66530 bytes`, and each archived file is exactly that many bytes plus
its CRLF pairs (30, 405679, 69042), so the archive is provably that run's output. Only the
`/v1/markets` body was then promoted into this tree, excerpted in
`rest/markets_observed_20260914.json` (see the section above); **no `/status` and no `/v1/perps/contracts`
fixture exists here**, so those rows stay unverified *in this tree*. No **other** REST endpoint was
re-attempted, and no authenticated call was ever made.

Both the `/status` and `/v1/markets` bodies above were obtained through the adapter's own client,
which strips a `GenericResponse` `result` member when one is present; a file written that way cannot
show whether the wire carried the envelope, so neither is evidence about the envelope in either
direction.

| Endpoint | Direction | Schema summary | Exact field paths | Units / notes |
|---|---|---|---|---|
| `GET /v1/markets` | response 200 | `GenericResponse` + `MarketsResult` | `success`; `result.perps.tradingPairs[]` → `.market`, `.baseIncrement`, `.quoteIncrement` **and, observed 2026-09-14:** `.disabled` (present only when true), `.tags`, `.makerFee`, `.takerFee`, `.defaultLeverage`, `.maxPositionBaseSize`, `.marginInfo[]`, `.schedule`, `.listedAt`, `.logoUrl`, `.backgroundColour`; `result.tokenConfig[]` → `.id`, `.name`, `.decimals`, `.tokenDecimals`, `.networks`; `result.spot` | **`baseIncrement` = quantity step in base units; `quoteIncrement` = price step in quote units. They must never be swapped** (plan §4.1). Both are decimal strings. `tokenConfig` is spot-token config — never read it as perps metadata. The spec declares **no status field** on a trading pair and the observation confirms it (**0 of 81**); status arrives as `disabled: true` on 20 markets and by its **absence** on the other 61 — `conflicts.md` conflict 5, RESOLVED BY OBSERVATION. `schedule` is the venue's market-hours object and is **not interpreted** by the adapter. |
| `GET /v1/perps/contracts` | response 200 | `GenericResponse` + `Contract[]` | `result[].market`, `.productType`, `.contractType`, `.baseCurrency`, `.quoteCurrency`, `.disabled`, `.isClosed`, `.makerFee`, `.takerFee`, `.fundingRate`, `.nextFundingRate`, `.nextFundingRateTimestamp`, `.lastPrice`, `.bid`, `.ask`, `.openInterest`, `.quoteVolume`, `.usdVolume`, `.indexPrice`, `.tags` | `makerFee`/`takerFee` are decimal fractions of filled notional (`0.00025` = 2.5 bp, charged in USDC). `disabled` = perp unavailable; `isClosed` = *underlying* market closed (equities) — different concepts, never merged. |
| `GET /status` | response 200 | `GenericResponse` + `StatusResult` | `result.marketStatus` (the only declared member) | Diagnostics only — the adapter reads nothing here as a price, quantity or fee. Obtained live 2026-09-14T15:15:02Z, where it returned `marketStatus: open`; read it through the caveat above before treating that as the wire shape. Unlike every other endpoint in this table, `/status` carries **no `/v1` prefix**. |
| `POST /v1/perps/orders` (create) | request | `AddOrderReq` | `side`, `market`, `size`, `price`, `type`, `timeInForce`, `postOnly`, `reduceOnly`, `clientOrderId`, `quoteSize` *(unsupported this phase)*, `takeProfit`/`stopLoss` *(unsupported)*, `builderCode` *(unsupported)* | `size`/`price`/`quoteSize` are decimal strings aligned to the increments above. `type` ∈ {`limit`,`market`}; `timeInForce` ∈ {`GTC`,`IOC`} on create — **no `FOK`** (conflict 3). `clientOrderId` is alphanumeric plus `_`/`-`, max 64. |
| `GET /v1/perps/fills` | response 200 | `GenericResponse` + `ApiFill[]` | `result[].id`, `.orderId`, `.clientOrderId`, `.parentOrderID`, `.market`, `.price`, `.size`, `.side`, `.direction`, `.filledCost`, `.fee`, `.feeRebate`, `.pnl`, `.time`, `.isMaker`, `.isADL` | `direction` spelling is contradictory in the spec (conflict 4). `fee`/`feeRebate` are quote-currency decimal strings per fill. |
| `GET /v1/perps/depth`, `/v1/perps/trades`, `/v1/perps/funding_rates`, `/v1/perps/mark_prices`, `/v1/perps/open_interest`, `/v1/perps/volume` | response | never sampled live, in P0 or since | never sampled live, in P0 or since | REST is for first snapshot, diagnostics and recovery only; WS is the primary data source. Future snapshots must never overwrite newer WS state (plan §4.2). |

Auth header names and the WS login digest are **conflicts, not facts** — see `conflicts.md`.

## Support matrix (per plan §1, frozen at P0)

| Capability | P0 stance |
|---|---|
| Instrument (from `baseIncrement`/`quoteIncrement`), `QuoteTick`, L2 snapshot → `CLEAR + ADD` with `F_SNAPSHOT`/`F_LAST`, `TradeTick`, `FundingRateUpdate`, mark price | **Supported (planned P1)** |
| Limit `GTC`/`IOC`, Market (by base `size`), `postOnly`, `reduceOnly` | **Supported (planned P3)** |
| Single submit, list submit (≤20), single cancel, per-market cancel, order/fill/position/balance queries, funding-fee reconciliation, reconnect + reconciliation, dead-man's switch | **Supported (planned P3)** |
| Atomic amend/replace | **Explicitly unsupported** — return a named unsupported error; never emulate as cancel+new |
| `FOK`, `GTD` | **Explicitly unsupported on create** (conflict 3); incoming WS order reports carrying them are still parsed and preserved |
| TWAP | **Explicitly unsupported** |
| Conditional orders / TP-SL **creation** | **Explicitly unsupported**; externally created conditional orders, liquidations and ADL are still recognised, never silently dropped |
| `quoteSize` (quote-denominated market buy) | **Explicitly unsupported** |
| Builder attribution | **Explicitly unsupported** |
| Multi-asset collateral (MAC) trading | **Explicitly unsupported** |
| Deposits, withdrawals, bridging, API-key create/delete, SIWE/JWT session management, points claiming, auto-deploy, adding ONDO to `maker_live.py` | **Out of scope for the whole plan** |
| Production order writes | **Hard-off**; `allow_production_orders=True` must still return unsupported (plan §1, §4.1) |

## Sources (URLs and fetch times)

| Source | URL | Fetched (UTC) | SHA256 |
|---|---|---|---|
| llms index | https://docs.ondoperps.xyz/llms.txt | 2026-09-14T11:08:43.220046+00:00 (refresh 11:52:49, identical) | `c9ddebb5bbecaba65a5c54b88de4960525d9071643cfba5b356216deda583fa8` |
| REST spec | https://docs.ondoperps.xyz/api-reference/rest-spec.json | 2026-09-14T11:08:44.875791+00:00 (refresh 11:52:50, identical) | `860a96ca3fa1e0800841446cddc738bc8ab2749feb67c2f10fcdd1e2aae003f1` |
| WS spec | https://docs.ondoperps.xyz/api-reference/ws-spec.json | 2026-09-14T11:08:45.020593+00:00 (refresh 11:52:54, identical) | `9321654d102d996b6e7aaf3c67675ca651f2b64d7910b99b37f2dae16988d67c` |
| API-key auth page | https://docs.ondoperps.xyz/api-reference/api_key_authentication.md | 2026-09-14T11:08:43.220046+00:00 (refresh 11:52:55, identical) | `4d1a6ce38bf82b2fd486c1d83e77c377891d6bb6d37edabd35079cc87e0c71d2` |
| WS connect | https://docs.ondoperps.xyz/api-reference/connection/connect.md | 2026-09-14T11:08:43.220046+00:00 (refresh 11:52:56, identical) | `5ffc02bc0c0f0df2e8409e6a47aa9e00c4badc3a392362f6a53356b6a6a02db4` |
| WS login | https://docs.ondoperps.xyz/api-reference/connection/login.md | 2026-09-14T11:08:43.220046+00:00 (refresh 11:52:56, identical) | `284c288cdc20e27ccd4186f4a0d96903957c6622a5cdd32638fd8ca98f119b94` |
| Fees | https://docs.ondoperps.xyz/fees.md | 2026-09-14T11:08:45.179791+00:00 (refresh 11:53:16 after one TLS EOF, identical) | `5ec248258c910632fd2bfcc4a2db5b6a32b8c9c7b72ba6c5778030f3b8e50a8f` |
| Funding rates | https://docs.ondoperps.xyz/funding-rates.md | 2026-09-14T11:08:45.198793+00:00 (refresh 11:52:57, identical) | `8ab487da57b31875989fe706002cbe66679d8228cafb0e5b2cacfcb600ba5591` |
| Depth channel | https://docs.ondoperps.xyz/api-reference/public-channels/subscribe:-perps-depth-book.md | 2026-09-14T11:08:44.015247+00:00 | `a7cc216fb294a80eebda2f6b6c5e5fffa93aefd9accd4556bf116f0db3791b38` |
| Top-of-book channel | https://docs.ondoperps.xyz/api-reference/public-channels/subscribe:-perps-top-of-book.md | 2026-09-14T11:08:44.373496+00:00 | `7f69a884a04d4c31d45179e740662ed998d25b12a1d9083e4915304619ac22d9` |
| Funding channel | https://docs.ondoperps.xyz/api-reference/public-channels/subscribe:-perps-funding-rates.md | 2026-09-14T11:08:44.413900+00:00 | `770412a078260c0a17870b8021300b38db063ac4de132c6e84a33e06caf601eb` |
| DMS channel | https://docs.ondoperps.xyz/api-reference/private-channels/subscribe:-perps-dead-mans-switch.md | 2026-09-14T11:08:44.480312+00:00 | `52d9d3c50dabdc012ab9cd0005e69d71bb56f33d5e7e0e0ad55b8473b07f5f04` |
| Production WS capture | `wss://api.ondoperps.xyz/ws` (unauthenticated, ~8 s) | 2026-09-14T11:10:00.861748Z – 11:10:08.776553Z | capture `660a3f57db0d291d713d8801f92a432f275eee5ef87cbfe79729614f76209773` |
| Live REST host | https://api.ondoperps.xyz/v1/markets | 2026-09-14T11:50:18.569618+00:00 → **HTTP 403 `error code: 1010`, cloudflare** (网络未验证) | n/a |
| 2026-09-11 market observation (historical, **not** today's data) | `reports/perps-candidates-2026-09-11/ondo-research.md` | 2026-09-11 | `836fbc89f6a315b2cb1635e55a607f6baa8c6670221f6476fe9c37809db527c1` |

Public materials were copied from the planning evidence in
`reports/ondo-plan-2026-09-14/` with their original retrieval timestamps preserved. No header,
signature, JWT, cookie, key or secret value appears in this directory; credentials come from
`.env`/environment variables only, and tests use fake keys.
