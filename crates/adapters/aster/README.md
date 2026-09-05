# nautilus-aster

[NautilusTrader](https://nautilustrader.io) adapter for the
[Aster DEX](https://www.asterdex.com/) perpetual futures exchange.

Aster exposes a Binance-USD-M-compatible Futures API, so market data is a thin configuration
and factory layer over `nautilus-binance`: it routes the USD-M data path at Aster's REST and
WebSocket endpoints and pins the Nautilus venue to `ASTER`. No Binance protocol code is
duplicated for market data.

Execution cannot be layered the same way. Aster's Futures **V3** API replaced Binance's HMAC
signing with EIP-712 typed-data signatures, so this crate carries its own signed HTTP client
(`src/http/`), its own request and response models (`src/http/models.rs`), and its own
execution client (`src/execution.rs`). The private user data stream is still Binance-shaped,
so its frame decoding is delegated to `nautilus-binance`.

## Capabilities

| Area | Supported | Notes |
|---|---|---|
| USD-M perpetual market data | Yes | Quotes, trades, book deltas, mark price, funding rates |
| Crypto perps | Yes | e.g. `BTCUSDT-PERP.ASTER` |
| US-stock perps | Yes | e.g. `NVDAUSDT-PERP.ASTER` |
| `LIMIT` orders (`GTC` / `IOC` / `FOK`) | Yes | `POST /fapi/v3/order` |
| Post-only | Yes | Sent as the `GTX` time in force |
| `MARKET` orders | Yes | |
| `reduce_only` | Yes | |
| Cancel order | Yes | By client order ID, falling back to the venue order ID |
| Cancel all orders for an instrument | Yes | Side-less: `DELETE /fapi/v3/allOpenOrders`. Side-filtered: only the matching open orders are cancelled, one request each |
| Order status / fill / position reports | Yes | `order`, `openOrders`, `allOrders`, `userTrades`, `positionRisk` |
| Account state | Yes | `GET /fapi/v3/balance`, plus `ACCOUNT_UPDATE` on the user stream; unknown assets are registered on the fly, and explicit zero rows are kept so a drained asset clears |
| User data stream | Yes | Awaited at connect; listen key renewed every 30 minutes; reconnects with a new key on expiry and compensates for the gap |
| Commission rates | Yes | `GET /fapi/v3/commissionRate` per instrument at connect; the instruments are re-published with the account's real fees |
| Order modification | No | Cancel and resubmit; `modify_order` emits a modify-rejected event |
| Conditional / algo orders (`STOP`, `TAKE_PROFIT`, trailing) | No | Rejected at submission |
| Batch orders | No | |
| Quote-denominated quantities | No | Denied before any request; Aster's `quantity` is base-asset only |
| Hedge (dual-side) mode | No | Detected at connect and rejected |
| Venue position IDs | No | One-way (net) mode carries none |

## Authentication

Every `TRADE`, `USER_DATA`, and `USER_STREAM` endpoint is EIP-712 signed. There is no API
key/secret pair; an *API wallet* (agent wallet) signs each request instead:

| Value | Source | Default |
|---|---|---|
| `signer_private_key` | config or `ASTER_SIGNER_PRIVATE_KEY` | required |
| `signer_address` | config or `ASTER_SIGNER_ADDRESS` | derived from the key |
| `user_address` | config or `ASTER_USER_ADDRESS` | the signer address |

The signed payload is an ordered parameter string prefixed with `nonce`, `user`, `signer`,
wrapped in the `Message { string msg }` struct and signed against the domain
`{ name: "AsterSignTransaction", version: "1", chainId: 1666 (mainnet) | 714 (testnet),
verifyingContract: 0x0 }`. The 65-byte `r || s || v` signature (`v` in {27, 28}) is appended as
`&signature=0x…` and sent as the query string for `GET`, or as an
`application/x-www-form-urlencoded` body otherwise.

The private key is never logged: `Debug` renders it redacted on the config, the credential and
the signer, and the Python config's `repr()` omits it entirely.

Aster gates every signed V3 endpoint behind a completed deposit from the master wallet; an
account that has never deposited answers with
`-5050 This function can only be used after deposit`.

## Rate limits

| Budget | Value |
|---|---|
| Request weight | 2400 / minute |
| Orders | 1200 / minute |

Both are enforced client-side by keyed quotas on the shared HTTP client. Aster additionally
rate-limits `exchangeInfo` aggressively, so prefer `BinanceInstrumentProviderConfig::load_ids`
over `load_all` and leave the refresh intervals at their defaults.

The `nonce` is a microsecond timestamp that must fall within ±60 s of server time, and Aster
tracks it per signer address, so it must be strictly increasing. One `AsterHttpClient` and all
of its clones therefore draw from a single monotonic counter.

## Retries

Idempotent `GET` requests (`balance`, `positionRisk`, `openOrders`, `order`, `allOrders`,
`userTrades`, `commissionRate`, `positionSide/dual`) are retried up to three times on
**transport-level** failures only — a TLS handshake EOF, a TCP connect failure or timeout, a
connection reset — with a 500 ms / 1 s / 2 s backoff. Each attempt re-signs the request, so it
draws a fresh nonce.

Nothing else is retried. An answer from the venue is definitive: an Aster error body such as
`-1121 Invalid symbol` is returned to the caller unchanged, as is any unexpected HTTP status.
`POST` and `DELETE` are never repeated, because order submission and cancellation are not
idempotent; a transport failure there is surfaced as ambiguous and resolved by querying the
order (see **Error classification**), never by resubmitting it.

Instrument loading (`exchangeInfo`) runs through `nautilus-binance`, which has no retry of its
own, so the execution client wraps it in the same policy at connect: transport faults only,
never an Aster error body, which matters because Aster rate-limits this endpoint aggressively.

## Error classification

Aster's `{code, msg}` bodies are Binance-shaped, but "the venue answered" is not the same as
"the venue decided". Order submission classifies every failure into one of three groups:

| Class | Examples | Handling |
|---|---|---|
| Definitive rejection | `-1121` invalid symbol, `-2010` new order rejected, `-2019` margin insufficient, `-4164` min notional, `-1111` bad precision, `-1013`, `-1102`, `-4003`/`-4004`/`-4005`, `-1003` rate limited, `-1021`/`-1022` nonce or signature, and any `4xx` status without an Aster body | `OrderRejected` immediately |
| **Execution status unknown** | `-1006 UNEXPECTED_RESP`, `-1007 TIMEOUT`, any transport fault (TLS, TCP, client timeout), an undecodable response body, and **any** `5xx` / `408` response - including one carrying a parseable Aster error body | **Never terminalised.** Logged at error; the order stays in flight and `GET /fapi/v3/order?origClientOrderId=` is queried after 2 s / 5 s / 15 s until the venue answers. A definitive `-2013 NO_SUCH_ORDER` then rejects it locally; any other answer is emitted as the venue reports it. The order is **never** resubmitted |
| Local fault | missing credentials, signing, request validation | `OrderRejected`; the request never left the process |

Aster's own error-code documentation states that `-1006` and `-1007` mean "execution status
unknown", so an order submitted under either may already be resting or filled. Treating them as
rejections desynchronises the engine from the book, which is why `is_venue_rejection` excludes
them explicitly.

The HTTP status is kept alongside the code for the same reason. Aster documents every `5xx` as
"execution status unknown", and it answers some of them with a perfectly parseable body -
`503` with `{"code": -1000, ...}`. Classifying on the code alone reads that as a decision and
terminalises an order that is live on the book, so `AsterHttpError::AsterError` carries the
status and any `5xx` / `408` is ambiguous whatever the code says.

## User data stream lifecycle

`connect` opens the first stream session inline and does not report the client as connected
until the listen key and the socket are both up: an order submitted in that window would
otherwise produce no events at all. Each attempt is bounded by `ws_connect_timeout_secs`
(default 20 s; the shared Binance stream pool's own default is 5 s, which is short for a slow or
proxied egress path) and repeated on a 1 s / 2 s / 4 s backoff for **transport faults only**. A
venue answer - a rejected listen key, an unfunded wallet - fails `connect` on the first attempt,
because repeating it would answer the same way.

Every later session runs a **compensation pass** before resuming, and so does the `Reconnected`
frame the shared streams client raises when it re-establishes the socket underneath a live
session. The gap between the drop and the reconnect carries no events at all, so the pass
restores the four things that gap can invalidate:

1. **Fills first.** Each recovered trade is delivered *with* its order status as a single
   `OrderWithFills` report. Order and fills are not two messages whose arrival order happens to
   work out: a status carrying the cumulative quantity on its own makes the engine infer a fill
   to explain it, and the real trade then arrives against an already-complete order, trips the
   overfill guard and is dropped. The order keeps a synthetic trade ID and no commission - the
   log looks clean and the economics are wrong.
2. **Open orders** - `openOrders` is reported, then every order this client still believes is
   working but the venue no longer lists is queried individually, which surfaces the fills and
   cancels that happened during the outage. Orders the fill pass already delivered are skipped,
   so no second, fill-inferring status follows them.
   If the trade history cannot be read, the pass retries it (1 s, 3 s) and then **holds back
   every order status that carries a filled quantity** rather than publishing it: to the engine
   such a status *is* a fill, and the trade it invents to explain it permanently displaces the
   real one — which then arrives against a complete order and is rejected as an overfill. Those
   orders stay in the working set and the next pass retries them; statuses with nothing filled,
   and the balance and position refreshes, proceed as usual. "No fills were missed" and "the
   fill query failed" are different answers and are no longer both an empty set.

   The fill window starts at the newest trade time seen for each symbol, falling back to a
   one-hour lookback, and never reaches earlier than the connect. Anything older belongs to the
   execution engine's startup reconciliation; replaying it as a live session fill is what makes
   the engine reject an `OrderFilled` for an order it already holds as filled. Trades already
   delivered - by the stream, or by a report request - are skipped by trade ID, so a repeated
   compensation is idempotent.
3. **Balances** - a fresh `balance` snapshot.
4. **Positions** - `positionRisk` for every loaded instrument, including flat rows, so a
   position closed during the outage is cleared instead of being left at its stale quantity.

## History pagination and report completeness

`allOrders` and `userTrades` default to 500 rows and cap at 1000, and Aster refuses their
cursors (`orderId` / `fromId`) together with `startTime` / `endTime`. Both are therefore paged
the same way: the end of the window is pinned before the first request, each 7-day slice is
opened with a time-bounded page at `limit=1000` and continued with the cursor alone, rows are
deduplicated by ID, and the cursor must strictly advance or pagination fails rather than looping.

A failed `userTrades` / `allOrders` / `openOrders` / `positionRisk` request, or a row whose
required fields cannot be parsed, fails `generate_*_reports` instead of yielding a short list.
A failed request also leaves no trace: the trades it covered are recorded as delivered only once
the whole request has succeeded. Recording them while building loses them outright - the call
returns `Err`, the engine never receives the earlier rows, and the next compensation pass skips
them as already applied. `generate_mass_status` holds the same records back until its position
query has succeeded too.
A partial history that looks complete is worse than an error, because the engine infers fills
from it. Rows for instruments this client never loaded are the one thing skipped, and only with
a log line: they are out of scope rather than missing.

## Startup reconciliation

The execution engine builds each historical order it does not know about from an
`OrderStatusReport` plus the `FillReport`s carrying the same `venue_order_id`: it drops the fill
it would otherwise infer from `filled_qty` and applies the real ones instead. Before applying
anything it sorts **every** reconciliation event by `ts_event`, and it takes those timestamps
straight from the reports - `OrderAccepted` from the order's `ts_accepted`, each `OrderFilled`
from its fill's `ts_event`.

That makes one ordering invariant load-bearing: **an order's `ts_accepted` must not follow any
of its own fills.** Aster breaks it. Some `allOrders` rows come back with no `time` field, so
the report has no venue creation time and falls back to the receive time - now - which is later
than every historical fill. The fill then sorts ahead of the acceptance, is applied to an order
still in `Initialized`, and the order state machine rejects it:
`InvalidStateTrigger: ... did not apply OrderFilled(...)`, once per historical filled order on
the account, on every fresh start.

`generate_mass_status` is therefore composed by this adapter rather than inherited, and does
three things the default composition cannot:

- **Aligns each order with its own fills.** A report whose `ts_accepted` follows its first fill
  has the acceptance pulled back to that fill; `ts_last` is never pulled backwards. The order
  report is corrected rather than the fills dropped, so nothing is lost - only the one timestamp
  the venue did not give us. `AsterOrder::to_order_status_report` additionally clamps
  `ts_accepted` to `ts_last`, so a single row can never contradict itself either.
- **Declares the report window.** Aster's history endpoints only answer for a time range, so the
  snapshot is complete *from* `start`, not from the account's first trade. It is published with
  `ExecutionMassStatus::set_report_window`; without it the engine treats the history as complete
  and may synthesise position-opening fills to explain a position whose opening trade simply
  predates the lookback.
- **Keeps every reported fill.** All three sources are requested over one identical window, but
  a window is not a partition of the account's history: `allOrders` filters on the order's
  *creation* time, so a GTC opened two minutes ago and filled one second ago contributes a trade
  with no order on the page. That fill is not an orphan and must not be discarded - it is real
  execution with a real commission. The order is fetched by ID
  (`GET /fapi/v3/order?orderId=`, which is not time-filtered) and linked. A fill that still
  cannot be linked is kept and the snapshot is published with `reports_complete = false`; the
  engine is told the history is partial rather than handed a trimmed one that looks whole.

  Such a fill is also **not** marked as delivered. `ExecutionManager` creates no fill event for
  a one-way fill with neither a cached order nor a venue position ID, so a report call returning
  `Ok` is not the same as the fill having been applied; recording it would make the next
  compensation pass skip a trade nothing ever consumed. It is held as pending instead, and its
  timestamp pins the compensation window open so a later trade for the same symbol cannot
  advance the watermark past it.
- **Reports flat positions.** Declaring the window also turns on the engine's bounded check
  (`ExecutionManager::order_only_venue_order_ids`), which confirms per instrument that the
  reported fills net to the reported position. It looks the expected quantity up in the position
  reports, and an instrument with fills but *no* position row has nothing to confirm against:
  every historical order for it is demoted to order-only projection with
  `Bounded reconciliation does not explain the reported position for ...`. Aster omits a symbol
  from `positionRisk` when the account is flat in it, so `generate_position_status_reports` now
  reports flat rows rather than dropping them, and `generate_mass_status` adds an explicit flat
  row for any instrument that traded in the window but has none. A flat report and an absent
  report make the same claim; only one of them is legible to the engine.

A flat row is also what closes a position the cache still believes in, so reporting it is right
independently of the bounded check - `reconcile_position_report_netting` no-ops when the venue
and the cache already agree.

## Fees

`exchangeInfo` carries no commission data, so the shared Binance instrument parser fills
`maker_fee` / `taker_fee` with its own VIP-0 defaults (`0.0002` / `0.0005`). Those are neither
Aster's rates nor this account's. At connect, once the instruments have loaded, the execution
client queries `GET /fapi/v3/commissionRate` once per loaded instrument with the **signed**
client and re-publishes each instrument carrying the real rates on the data event channel, so
the data engine and cache hold them.

A symbol whose query fails keeps the venue default and is named in a warning as **UNVERIFIED**.
That default is a placeholder, not a measurement, and must not be quoted as the account's cost.

Publishing once is not enough. The market-data path is the Binance USD-M client, which rebuilds
instruments from `exchangeInfo` on every explicit `request_instruments` and on its hourly
refresh - with the Binance VIP-0 defaults, over the same cache. The verified rates are therefore
also *registered* with `nautilus_binance::common::fees::register_instrument_fees`, and the shared
instrument parser applies them wherever it would otherwise use a fallback. The registry is empty
unless something registers into it, so Binance itself is unchanged. This is the one place the
Aster work reaches into `nautilus-binance`: the fees can only be obtained through Aster's
EIP-712-signed endpoint, which that client cannot call, so nothing inside the data path could
otherwise know them.

Registrations are owned, not global. The key is the **HTTP endpoint** the rates were verified
against, and the owning **account** is stored with them:

- Mainnet and testnet, or two deployments, share the `ASTER` venue but not their base URL, so
  two clients for different accounts no longer overwrite each other. The parser matches on the
  URL it is itself talking to.
- Two accounts on the *same* endpoint cannot both be served, because nothing downstream could
  tell their instruments apart. The second registration is **refused** and the symbol is
  reported as unverified, rather than silently replacing the first account's measured rates.
- Only the owning account can drop an entry, so one client's failed query cannot clear another's.
- `stop()` releases the client's registrations, so a later client for another account can take
  the endpoint over and a stale rate cannot outlive the session that verified it.

A symbol whose query *fails* on a later connect also has its entry removed: a rate that can no
longer be confirmed must not keep being applied as though it still were.

## Venue quirks

- `NVDAUSDT` reports `pricePrecision 6` but a `0.01` tick. Prices are formatted from the
  instrument's price increment (the `PRICE_FILTER` tick size), not from `pricePrecision`.
- Balances name assets the Nautilus currency map has never seen (testnet answers with `USDT`,
  `BTC`, `ASTER` and `AFEE`). Every currency built from a venue string goes through
  `common::currency::resolve_currency`, which registers an unknown code as an 8-decimal crypto
  currency and logs it once at debug level. `Currency::from` is never used on venue data: it
  panics on an unknown code, which would take the whole trading node down over an airdropped
  or fee-credit asset the account never trades.
- `availableBalance` can exceed `walletBalance` on a cross-margin account, because availability
  includes headroom from other assets. Nautilus requires `total == locked + free`, so `free` is
  clamped to the wallet balance (`locked = 0`) rather than inflating the reported total; the
  first such row per process is logged at warning level.
- Every balance row the venue reports is passed on, **including explicit zeros**. Account
  state is applied per currency, so an asset the venue omits keeps whatever the cache already
  holds; the zero row is the only thing that can clear an asset after a withdrawal. The shared
  Binance `ACCOUNT_UPDATE` parser drops `wb == 0` rows, so the `B` array is parsed in this crate
  instead of changing the Binance parser other venues depend on.
- The testnet symbol set is smaller than mainnet's (it has no `NVDAUSDT`, which answers
  `-1121 Invalid symbol`). `load_ids` entries the venue does not list are logged as a warning
  at connect and skipped; the client still connects as long as one instrument loaded.
- Several numeric fields arrive JSON-encoded as strings (`"orderId": "417663664"`,
  `"updateTime": "1776802344230"`, `"code": "200"`) where Binance returns numbers. The models
  accept both forms.
- `IOC` / `FOK` remainders are reported as `EXPIRED`; `treat_expired_as_canceled` (default
  `true`) maps that onto `CANCELED`.
- The user data stream appends the listen key as a path segment
  (`wss://fstream.asterdex.com/ws/<key>`), unlike Binance USD-M's `?listenKey=<key>`.
- Aster multiplexes venue-specific announcement events onto the user stream; unknown event
  types are logged at debug level and ignored.

## Testing

`test_data/signing_vectors.json` carries the EIP-712 signature vectors. Their parameter strings
come from CCXT's static request fixtures; the expected signatures were regenerated with
`eth-account` (the library used by Aster's own signing example) because CCXT lists `signature`
in its `skipKeys`, so the signatures recorded there are never verified by CCXT's tests and no
longer match the recorded parameter strings. The `test_data/http_*.json` files are verbatim
Aster response bodies used as serde fixtures, each with a `_source` header naming its origin.
`http_balance_testnet_unknown_assets.json` is a real testnet `GET /fapi/v3/balance` body rather
than a CCXT copy: it carries the two currency codes Nautilus does not know and the
`availableBalance > walletBalance` case.

## References

- General info and signing example: <https://asterdex.github.io/aster-api-website/futures-v3/general-info/>
- User data streams: <https://asterdex.github.io/aster-api-website/futures-v3/user-data-streams/>
- Testnet: <https://asterdex.github.io/aster-api-website/futures-testnet/general-info/>
