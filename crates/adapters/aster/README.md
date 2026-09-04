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
| Cancel all orders for an instrument | Yes | `DELETE /fapi/v3/allOpenOrders`; the side filter is ignored |
| Order status / fill / position reports | Yes | `order`, `openOrders`, `allOrders`, `userTrades`, `positionRisk` |
| Account state | Yes | `GET /fapi/v3/balance`, plus `ACCOUNT_UPDATE` on the user stream |
| User data stream | Yes | Listen key renewed every 30 minutes; reconnects with a new key on expiry |
| Commission rates | Yes | `GET /fapi/v3/commissionRate` |
| Order modification | No | Cancel and resubmit; `modify_order` emits a modify-rejected event |
| Conditional / algo orders (`STOP`, `TAKE_PROFIT`, trailing) | No | Rejected at submission |
| Batch orders | No | |
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

## Venue quirks

- `NVDAUSDT` reports `pricePrecision 6` but a `0.01` tick. Prices are formatted from the
  instrument's price increment (the `PRICE_FILTER` tick size), not from `pricePrecision`.
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

## References

- General info and signing example: <https://asterdex.github.io/aster-api-website/futures-v3/general-info/>
- User data streams: <https://asterdex.github.io/aster-api-website/futures-v3/user-data-streams/>
- Testnet: <https://asterdex.github.io/aster-api-website/futures-testnet/general-info/>
