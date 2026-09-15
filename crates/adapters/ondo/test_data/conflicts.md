# Ondo Perps — P0 protocol conflict table

Scope: this file records every official-document conflict and schema/field gap that later tasks
(down to P3 sandbox work) must handle explicitly. **Nothing in this file is resolved by live
sandbox evidence** — no sandbox key existed in this phase, and P0's unauthenticated attempts at the
public REST host were refused (HTTP 403). One later unauthenticated production capture on
2026-09-14 (`rest/markets_observed_20260914.json`, fetched with the adapter's own public client)
resolved conflict 5 and gap 6 **by observation**; every other row says whether it is unresolved and
names the side that P0/Sandbox must prove.

Rules that apply to every row below (plan §1, §6.1):

- Known wire forms are matched explicitly; an unrecognised value is surfaced as a named error or
  an `unknown` state — it is never silently swallowed, coerced, or defaulted.
- No automatic protocol probing: on a signature failure the adapter must not retry with a
  different header set, ordering, or environment.
- Seeds of truth used here are the archived official materials with their SHA256, listed at the
  bottom.

## Required conflicts

| # | Topic | Reading A | Reading B | P0 decision — what later tasks must do |
|---|---|---|---|---|
| 1 | REST API-key headers | **`ONDO-KEY-ID` + `ONDO-TIMESTAMP` + `ONDO-SIGN`** (three headers). `docs/api-reference/api_key_authentication.md` L36–L44 and both worked examples (Go L86–L88, Python L123–L127) | **`X-API-KEY-ID`** (one header). `docs/api-reference/rest-spec.json` → `components.securitySchemes.ApiKeyAuth` | Reading A is the only read-able implementation: B cannot carry a timestamp or a signature, so it cannot authenticate anything. Implement A. **Treat the header contract as UNVERIFIED until a sandbox request returns success** — a sandbox `signature_mismatch`/`api_key_not_found` is a different failure and must be reported as such. Do not auto-switch to B. |
| 2 | WS login HMAC concatenation order | **`time + "ondo_perps_ws_login"`** (timestamp first). `docs/api-reference/connection/login.md` L11 prose | **`"ondo_perps_ws_login" + time`** (literal first). `connection/connect.md` L56 and `connection/login.md` L60 — both quote the shared OpenAPI description in `ws-spec.json` | **UNRESOLVED and unverifiable offline.** Reading A is the login-page prose and matches the community implementations noted in the 2026-09-11 report; Reading B is what the machine-readable spec states. P0 picks neither. Implement A behind one named constant so the flip is one line, cover both candidate digests in a fixture-based unit test (fake key only), and **never try A then B against production.** Sandbox confirmation is a P3 gate. |
| 3 | `FOK` time-in-force | **`FOK` exists.** `ws-spec.json` → `components.schemas.Order.timeInForce` enum = `["GTC","IOC","FOK"]` (also reachable from `ordersPerps`) | **`FOK` does not exist.** `rest-spec.json` → `components.schemas.AddOrderReq.timeInForce` enum = `["GTC","IOC"]`; `ApiOrder.timeInForce` is also `["GTC","IOC"]` | Creation follows the create schema: **FOK is locally rejected as unsupported** (plan §1, §6.2) and never mapped to GTC/IOC. But an *incoming* WS order report carrying `FOK` must be parsed and preserved, not dropped. `GTD` is absent from both enums and is equally unsupported. |
| 4 | `fill.direction` spelling | **camelCase.** `rest-spec.json` → `ApiFill.direction` enum = `["openLong","openShort","closeLong","closeShort","flipLongToShort","flipShortToLong"]`, example `"openLong"` | **Spaced.** The same schema's own `description` lists `"open long", "open short", "close long", "close short", "flip long to short", "flip short to long"`; `ws-spec.json` → `Fill.direction` enum uses exactly those spaced values | The REST schema contradicts **itself** (enum vs description) and disagrees with the WS schema. Parse both spellings into one internal direction and normalise on a single whitespace/underscore boundary; an unrecognised spelling must raise a named parse error with the raw value attached, never be mapped to a default direction. **Which spelling the REST host actually returns is UNVERIFIED.** |
| 5 | Market status field | **`/v1/markets` has no status at all.** `PerpsTradingPair` declares only `market`, `baseIncrement`, `quoteIncrement` — no status/disabled/enabled/trading field exists in the schema | **`/v1/perps/contracts` carries the status.** `Contract.disabled` (required boolean) plus `Contract.isClosed` (optional boolean, documented as "the underlying market is currently closed (e.g. outside trading hours for equities)", i.e. NOT the perp's own tradability). Separately, the 2026-09-11 `/v1/markets` observation reported 61 `active` / 20 `disabled` markets | **RESOLVED BY OBSERVATION** (2026-09-14 production capture of `GET /v1/markets`, `rest/markets_observed_20260914.json`, 81 trading pairs): **0 of 81 markets carry a `status` string**; **20 of 81 carry `disabled: true` and none carries `disabled: false`** (ENA-USD.P, IBM-USD.P, DKNG-USD.P, NEAR-USD.P, …); **61 of 81 omit `disabled` entirely, including both P1 targets `NVDA-USD.P` and `TSLA-USD.P`** (`tags: ["Stock"]`). 81 − 20 = 61 matches the 2026-09-11 note, so the venue's convention is: **`disabled` is emitted only when a market is disabled; absence means enabled.** The adapter therefore resolves (single precedence in `src/common/enums.rs`, `MarketStatusInfo::resolve`): a `status` string wins and an unrecognised value stays `Unknown`; else `disabled: true` ⇒ `Disabled`, `false` ⇒ `Active`; else **active, with the absent flag recorded as `MarketStatusSource::Absent`** — distinct from an explicit `false`, which keeps its own source. Any *other* status marker (a `status` member that is not a string, a `disabled` member that is not a boolean, `null`) cannot be classified and fails the load closed. Reading B's `disabled` is confirmed as the venue's closure signal; `isClosed` remains a separate field and is never merged into status. **Closure is expressed through `disabled`; market hours are expressed through `schedule`** (present on 70 of 81 pairs, with `timezone`, seven `openHours` entries — the 7th being `"closed"` — and a `holidays` list). **The adapter does not yet interpret `schedule`**: a listed equity market stays `Active` outside its hours. That is a known limitation recorded here and reported by Task 2, not a behaviour to invent. |

## Additional schema-vs-observation gaps (not listed in the brief, recorded because later tasks trip on them)

| # | Topic | Reading A | Reading B | P0 decision |
|---|---|---|---|---|
| 6 | `/v1/markets` payload nesting | The plan (§4.1) writes the path as `perps.tradingPairs` | The spec example puts it at **`result.perps.tradingPairs`**, next to a top-level `success` boolean (`GenericResponse`) | `test_data/rest/markets_synthetic.json` follows the spec (`success` + `result.perps.tradingPairs`). Parse defensively: read `result.perps.tradingPairs`, and fail closed with a named error if `result` is absent. **RESOLVED BY OBSERVATION 2026-09-14** (`rest/markets_observed_20260914.json`): the live host uses the spec's nesting — `{"success":true,"result":{"spot":…,"perps":{"tradingPairs":[…]},"tokenConfig":…}}`. P0's 403 attempt is what left this in doubt then. |
| 7 | WS update envelope `timestamp` | The observed production frames carry a top-level **`timestamp`** on `update` and `subscribed` messages (e.g. `"2026-09-14T11:09:59.670112401Z"` on the topOfBooksPerps/depthBooksPerps envelopes, while the item-level `time` is `2026-09-14T11:09:58.8461101Z`/`...59.570112122Z`) | `ws-spec.json` response schemas for every public channel declare only `type`, `channel`, `data`; `WebSocketResponse` has no `timestamp` field either | Trust the observation: keep the envelope `timestamp` (server send/batch time) separate from the item `time` (price event time) and from the local receive time (plan §4.2). Do not treat the undeclared field as an error, and do not use it as `ts_event`. |
| 8 | `subscribed` acknowledgement payload | The observed `subscribed` frame echoes the request inside `data` and adds `timestamp` | The spec's `WebSocketResponse` allows `data` for "update messages" only | Accept a `data` payload on `subscribed`; it is not an error and must not be parsed as a book/market update. |

## Sources (every SHA256 below is of the exact archived bytes used for these readings)

| Source | SHA256 | Retrieved (UTC) |
|---|---|---|
| `docs/api-reference/api_key_authentication.md` | `4d1a6ce38bf82b2fd486c1d83e77c377891d6bb6d37edabd35079cc87e0c71d2` | 2026-09-14T11:08:43.220046+00:00 (re-fetched 11:52:55, same hash) |
| `docs/api-reference/rest-spec.json` | `860a96ca3fa1e0800841446cddc738bc8ab2749feb67c2f10fcdd1e2aae003f1` | 2026-09-14T11:08:44.875791+00:00 (re-fetched 11:52:50, same hash) |
| `docs/api-reference/ws-spec.json` | `9321654d102d996b6e7aaf3c67675ca651f2b64d7910b99b37f2dae16988d67c` | 2026-09-14T11:08:45.020593+00:00 (re-fetched 11:52:54, same hash) |
| `docs/api-reference/connection/connect.md` | `5ffc02bc0c0f0df2e8409e6a47aa9e00c4badc3a392362f6a53356b6a6a02db4` | 2026-09-14T11:08:43.220046+00:00 (re-fetched 11:52:56, same hash) |
| `docs/api-reference/connection/login.md` | `284c288cdc20e27ccd4186f4a0d96903957c6622a5cdd32638fd8ca98f119b94` | 2026-09-14T11:08:43.220046+00:00 (re-fetched 11:52:56, same hash) |
| `docs/fees.md` | `5ec248258c910632fd2bfcc4a2db5b6a32b8c9c7b72ba6c5778030f3b8e50a8f` | 2026-09-14T11:08:45.179791+00:00 (re-fetched 11:53:16 after one TLS EOF, same hash) |
| `docs/funding-rates.md` | `8ab487da57b31875989fe706002cbe66679d8228cafb0e5b2cacfcb600ba5591` | 2026-09-14T11:08:45.198793+00:00 (re-fetched 11:52:57, same hash) |
| 2026-09-14 connectivity probe (real WS capture) | `660a3f57db0d291d713d8801f92a432f275eee5ef87cbfe79729614f76209773` | 2026-09-14T11:10:08.864127+00:00 |
| `reports/perps-candidates-2026-09-11/ondo-research.md` (2026-09-11 observation; **not** today's data) | `836fbc89f6a315b2cb1635e55a607f6baa8c6670221f6476fe9c37809db527c1` | 2026-09-11 |
| Live `api.ondoperps.xyz` REST host | — unavailable: `HTTP 403`, body `error code: 1010`, `Server: cloudflare` at 2026-09-14T11:50:18Z | 网络未验证 |

## Deliberately not attempted in P0

- No private authentication, no sandbox login, no order, no wallet, no deposit. Conflicts 1, 2 and
  the REST side of 4 stay **unverified**; only a P3 sandbox contract test (fake-free, dedicated
  sandbox account, read-only first) can close them.
- No header, signature, JWT, cookie, or secret value is stored anywhere in this directory.
  Conflict 1 and 2 quote header **names** and concatenation **order** only.
