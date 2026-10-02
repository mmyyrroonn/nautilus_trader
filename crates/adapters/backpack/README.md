# Backpack Exchange adapter foundations

This crate contains configuration, public market metadata, public stream decoding, and bounded depth synchronization.
It validates configuration, product eligibility, exact instrument fields, complete provider refreshes,
and bounded public observations. It has no HTTP or WebSocket client, live data or execution
client, authentication, account access, factory, or Python bindings. It is not a production adapter.
Constructing a configuration performs no I/O and reads no credentials or environment files.

## Current capability boundary

| Surface                                   | B0.1 behavior                                  | Later scope                                        |
| ----------------------------------------- | ---------------------------------------------- | -------------------------------------------------- |
| Symbol allowlist and market eligibility   | Implemented and tested offline                 | Used by discovery and both clients                 |
| Public metadata and instrument conversion | Implemented, with explicit economic inputs     | Runtime discovery and account verification         |
| Metadata provider                         | Complete refresh validation, without transport | A public data client supplies fresh responses      |
| Endpoint validation                       | Implemented and tested offline                 | Public production or explicit local protocol peers |
| Public stream parsing and depth replay    | Implemented without transport                  | Native data client and WebSocket lifecycle         |
| Public market data runtime                | Explicit unsupported error                     | Instrument discovery and market data               |
| Private account reads                     | Explicit unsupported error                     | Account state and reconciliation                   |
| Restricted execution                      | Explicit unsupported error                     | A bounded order surface after deterministic tests  |

`BackpackCapability::require_implemented` returns `BackpackUnsupportedCapabilityError` for every
runtime capability above. No order command is exposed. Submission, cancellation, modification,
batches, transfers, withdrawals, and a dead man's switch are unavailable. Later phases must reject
commands outside their implemented surface explicitly before transport; venue support alone does
not establish adapter support.

## Product boundary

Configuration requires a non-empty, duplicate-free allowlist of native symbols in the
`<BASE>_USDC_PERP` namespace, for example `BTC_USDC_PERP`. Base names are uppercase ASCII letters
or digits. Wildcards, spot symbols, inverse products, and non-USDC quotes are refused.

The namespace check is only an input constraint. `BackpackConfig::validate_market` also requires
venue metadata with `marketType=PERP` and `quoteSymbol=USDC`, and checks membership in the explicit
allowlist. A matching suffix cannot substitute for this metadata check. `parsing::parse_market` additionally checks native-symbol/base/quote agreement, `orderBookState=Open`,
and `visible=true`. This is a conservative scope filter; venue visibility is not itself proof of
trading state. Unknown states, unavailable currencies, malformed or missing required filters,
non-positive increments, off-grid bounds, and domain precision loss are errors. Unknown currencies
require explicit registration of known currency facts; this parser does not invent currency precision.

All filter and funding decimals arrive as strings and are parsed exactly. A null or absent maximum
quantity remains `None`. Funding interval is retained in milliseconds; lower/upper bounds remain in
basis points. `basis_points_to_ratio` converts by 10,000 with an exact round-trip check. Unknown
funding values remain optional, and inconsistent or zero-interval values are rejected. The optional
`high-precision` feature propagates to `nautilus-model`; default features remain empty.

`models::BackpackMarket` preserves unknown fields, dynamic price bands, and the venue's nonlinear
margin functions. Unknown JSON numeric metadata retains its decimal text through the
`arbitrary_precision` serde mode. The parser uses exact Decimal/Price/Quantity types and does not
route monetary values through floating point.

## Instrument construction and provider

`parsing::parse_market(&market, &config, received_at)` returns `BackpackInstrumentMetadata`.
Native identity is unchanged: `BTC_USDC_PERP.BACKPACK` and `SOL_USDC_PERP.BACKPACK` are distinct.
`metadata.to_instrument(Some(&economics))` constructs a linear USDC-settled `CryptoPerpetual`.
Its increments, quantity/price bounds, currency identity, and raw venue metadata come from the
validated response. Dynamic price bands are retained, not claimed to be enforced.

The public response does not establish account fee rates or constant margin requirements.
`CryptoPerpetual` has mandatory Decimal economic fields and silently defaults omitted inputs to
zero, so this adapter requires `BackpackInstrumentEconomics::new_checked` with four explicit
fractional rates and a non-empty provenance reference. The typed provenance distinguishes
`Synthetic`, `Configured`, and `VenueObserved`. Missing inputs return `MissingEconomics`.
Even a `VenueObserved` source is a caller statement, not an account verification performed here.
Every constructed instrument carries `info.backpack_execution_ready=false`. Margin functions
remain raw metadata; there is no universal margin derivation or private account readiness claim.

`createdAt` is a naive datetime in the venue schema. It is preserved without an inferred timezone
and is not used as the data event timestamp. Instrument `ts_event=0` records unknown event time;
`ts_init` is the caller-supplied receipt time. Documented stream timestamps can use the separate
checked `unix_microseconds_to_nanos` helper; funding intervals are not stream timestamps.

`BackpackInstrumentProvider::replace_markets` accepts a fresh decoded public market response.
It ignores unlisted markets, requires every allowlisted market exactly once, and validates all
selected entries before publishing the replacement snapshot. A failed refresh invalidates the
previous snapshot. `get(&InstrumentId)` and `all()` expose only the latest successful complete
refresh. The future transport owner must call `invalidate()` on request or response-decoding failure.
This provider owns no transport and does not fetch, cache indefinitely, or infer readiness.

## Public stream parsing and depth synchronization

`public::BackpackPublicStreamParser::new(metadata, generation)` scopes decoding to a validated
instrument and a caller-assigned bootstrap token. `decode(generation, frame, received_at)` requires
an exact `stream`/`data.e`/`data.s` match. It accepts `bookTicker`, `trade`, `markPrice`, realtime
`depth`, and documented `depth.200ms`, `depth.600ms`, `depth.1000ms` topics. Subscription/control
acknowledgements and errors belong to the transport owner and are not data decoder input.
Malformed/oversized frames, unsupported topics, wrong symbols, old tokens, non-executable grids,
and checked timestamp overflow return typed errors. Duplicate JSON fields are rejected for known
wire fields. The caller must invalidate cached freshness and associated book state on decoder errors.

Complete BBO and trades become native `QuoteTick` and `TradeTick`. Explicit paired null sides
produce `QuoteUnavailable`, never a zero-filled quote; omitted or unpaired side fields fail.
Quote/trade IDs deduplicate within one parser generation. Official book ticker documentation shows
a string update ID; the captured native perpetual stream uses an integer. Both exact unsigned
forms are accepted; fractional, signed, and overflowing values are rejected. Buyer-maker `m=true`
maps to sell aggressor. `T` is used for event time and both `E`/`T` microsecond values are checked.

Mark price becomes `MarkPriceUpdate`, retaining exact theoretical precision without rounding to
an order-entry tick. Funding `f` is retained as `BackpackFundingEstimate.raw_rate: Decimal`.
The official description does not establish a fraction/percent/bps denominator, so no native
`FundingRateUpdate.rate` is manufactured. `n` is checked milliseconds converted to nanoseconds;
metadata funding interval stays milliseconds. Funding estimates are distinct from settled funding.

`depth::BackpackDepthSynchronizer::new_checked(metadata, generation, snapshot_limit,
max_buffer_frames, max_levels_per_side)` begins buffering validated opaque depth updates from the
parser. Allowed REST limits are 5, 10, 20, 50, 100, 500, 1000 per side. The owner routes the
matching symbol/limit HTTP response to `install_snapshot(generation, body, received_at)`; the REST
body itself contains no symbol. Buffered updates with `u <= snapshot_id` are discarded. As an
explicit bootstrap inference, the first surviving `U..u` range must contain `snapshot_id+1`.
Every later range, including live updates, requires `U=previous_u+1`; live partial overlap fails.
Older duplicates are ignored. Quantities replace the entire level, and zero quantity deletes it.
Snapshot plus replay is validated atomically and published as one native `OrderBookDeltas`
CLEAR/rebuild batch, sorted by price. Only the last delta carries `F_LAST`; snapshot deltas carry
`F_SNAPSHOT`. Subsequent nonempty frames each form one atomic batch. Empty frames still advance
sequence continuity. All protocol buffers, frames, and stored sides have explicit bounds.

Every synchronizer error clears levels, buffers, and coverage before returning `Stale`.
Reconnect, malformed input, and transport faults require owner invalidation before recovery.
`restart(new_generation)` requires a strictly greater token and a new parser, even when reusing
the same socket. Old snapshot tasks must be cancelled or rejected by token. This module owns no
connection, retry timing, freshness timer, snapshot request, or publication channel.

`is_continuous()` establishes only a continuous bounded sequence view. Snapshot truncation does
not establish complete depth or complete top N. `coverage()` retains the initial bid floor and ask
ceiling after edge deletion. `best_covered_bid/ask()` refuse an apparent best level outside that
original band; a newly observed outer level cannot fill an unknown gap. A missing initial side has
unknown coverage. Consumers must separately establish covered sides and wall-clock freshness;
continuity alone does not authorize execution or make emitted outer levels a trusted BBO.

## Endpoints and credentials

The only venue environment recorded by this crate is Production:

- REST: `https://api.backpack.exchange`
- WebSocket: `wss://ws.backpack.exchange`

No official testnet or sandbox endpoint has been verified for this phase. There is no environment
fallback to production and no arbitrary remote URL override.

`BackpackEndpoints::loopback_override` is an explicit selection for local public protocol peers,
not another venue environment. It accepts only numeric loopback IP origins, the transport's HTTP
or WebSocket scheme, and no URL credentials, path, query, fragment, or port zero. DNS names,
including `localhost`, are rejected. This does not authenticate a peer or implement a transport.
Production credentials must never be sent to loopback overrides. A future authenticated transport
must independently enforce its endpoint and redirect policy before signing or transmitting requests.

```rust
use nautilus_backpack::config::BackpackConfig;

let config = BackpackConfig::new_checked(vec!["BTC_USDC_PERP".to_string()])?;
config.validate_market("BTC_USDC_PERP", "PERP", "USDC")?;
# Ok::<(), nautilus_backpack::config::BackpackConfigError>(())
```

## Protocol references

The following official sources establish the venue contract, not implemented runtime features:

- [Introduction and production origins](https://docs.backpack.exchange/#section/Introduction).
- [Market metadata](https://docs.backpack.exchange/#tag/Markets/operation/get_market): native symbols,
  `marketType`, `baseSymbol`, `quoteSymbol`, and decimal-valued filters.
- [Public streams](https://docs.backpack.exchange/#tag/Streams) and
  [depth snapshot](https://docs.backpack.exchange/#tag/Markets/operation/get_depth): envelope,
  timestamp units, sequence ranges, and explicit snapshot limits.
- [Order execution contract](https://docs.backpack.exchange/#tag/Order/operation/execute_order): venue
  order types and flags. No order payload or execution semantics are implemented here.

Authentication, signing, request canonicalization, client order identity, transport, and recovery
are separate subsequent changes. Public fixture provenance lives in `test_data/manifest.json`. Unknown protocol fields or behavior must
be checked against official evidence before those changes expose capabilities.

## Validation

Run `cargo test -p nautilus-backpack` and `cargo clippy -p nautilus-backpack --all-targets -- -D warnings`.
Tests cover official BTC/SOL market observations, captured public BTC perpetual frames, native
order book replay, truncated coverage and synchronization faults and explicitly synthetic adverse metadata, exact
decimal and unit boundaries, complete refreshes, economic provenance, allowlist errors, production
defaults, loopback origin validation, and explicit refusals of planned runtime capabilities. They perform no network I/O or account operations.
See the repository [adapter guide](../../../docs/developer_guide/adapters.md) for later transport,
client, and acceptance tests.
