# Backpack Exchange adapter foundations

This crate contains configuration, authentication and transport foundations, exact public market
metadata, public stream decoding, bounded depth synchronization, and durable local order identity for a phased Backpack Exchange integration. It validates
product eligibility and complete provider refreshes, constructs Ed25519 authentication, and provides
a restricted GET transport. Domain data/account runtime clients, execution, factories, and Python
bindings remain later work. Constructing configuration and credentials performs no I/O or
environment lookup.

## Current capability boundary

| Surface                                             | Behavior                                       | Later scope                                     |
| --------------------------------------------------- | ---------------------------------------------- | ----------------------------------------------- |
| Symbol allowlist and market eligibility             | Implemented offline                            | Discovery and domain clients                    |
| Public metadata and instrument conversion           | Implemented with explicit economic inputs      | Runtime discovery and account verification      |
| Metadata provider                                   | Complete refresh validation without transport  | Data client supplies fresh responses            |
| Durable clientId and unsigned intent                | Local filesystem ownership and recovery tested | Execution admission and reconciliation          |
| Endpoint validation and credential audience         | Implemented offline                            | Production or explicit local protocol peers     |
| REST signing and authenticated/public GET transport | Implemented with local transport tests         | Typed domain parsing and reconciliation         |
| Private WS subscription authentication              | Payload construction only                      | Connection lifecycle and account processing     |
| Public stream parsing and depth replay              | Implemented without transport                  | Native data client lifecycle                    |
| Public market data and account runtime clients      | Explicit unsupported error                     | Instrument discovery, data, account state       |
| Restricted execution                                | Explicit unsupported error                     | Guarded order surface after deterministic tests |

`BackpackCapability::require_implemented` still returns `BackpackUnsupportedCapabilityError` for
all engine runtime capabilities. Low-level GET transport does not establish a working account or
data client. No mutation transport is exposed. Submission, cancellation, modification, batches,
borrowing, transfers, withdrawals, and a dead man's switch are unavailable. Write retry and order
recovery semantics require acceptance when a real guarded execution owner is introduced.

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

`BackpackEndpoints::loopback_override` explicitly selects local protocol peers. It accepts only
numeric loopback IP origins, the transport's HTTP or WebSocket scheme, and no URL credentials,
path, query, fragment, or port zero. DNS names including `localhost` are rejected.
`BackpackCredential::production` and `loopback_peer` decode caller-provided base64 Ed25519 seeds;
they never read environment variables. Local credentials bind to the exact endpoint pair and
cannot be forwarded to production or a different local peer. Production credentials cannot be
used with a local override. The transport rejects redirects and disables system proxy discovery.

## Authentication and transport

`BackpackParameters` provides validated scalar values to both sorted canonical bytes and wire
parameters. Booleans use lowercase text, optional absent values are omitted, and monetary scalars
use exact `Decimal`. Inputs requiring escaping, repeated parameter names, reserved authentication
fields, arrays, and unsupported GET parameters are rejected. The array `marketType` filters on markets and order/fill history are
unavailable in this slice; discover/filter supported products using metadata instead. The supported
receive window is 1 through 60,000 milliseconds, default 5,000. Credentials zeroize their shared seed when the final owner drops; Debug and errors redact
credentials, URLs, payloads, and authentication. Explicit response/wire accessors must not be logged.

Every client consumes a caller-owned, cloneable `BackpackQuota`: public and private traffic must
share it for one subaccount scope. Defaults conservatively pace standard traffic at 32 ms and
historical market traffic at 2.1 s. Separate processes require additional shared coordination.
The shared HTTP client acquires quota once, then prepares the timestamp, signature, and request.
Admission is checked before queuing and after quota acquisition.

`BackpackHttpPolicy` bounds the whole operation, including quota queues, all read attempts, and
429/backoff delays, to at most 60 seconds. Transport failures, 429, and 5xx can retry within the
configured budget; other responses and malformed JSON do not retry. Pagination response headers
are restricted to `X-PAGE-COUNT`, `X-CURRENT-PAGE`, `X-PAGE-SIZE`, `X-TOTAL`, and `Retry-After`.
The shared transport enforces its existing 100 MiB response body cap.

`NotSent`, `VenueRejected`, and `Unknown` describe transmission evidence only. Cancellation or
uncertainty after possible dispatch remains `Unknown`, including across retries. GET order 404
cannot prove an order was never submitted or filled, and cannot resolve mutation state.

```rust
use nautilus_backpack::config::BackpackConfig;

let config = BackpackConfig::new_checked(vec!["BTC_USDC_PERP".to_string()])?;
config.validate_market("BTC_USDC_PERP", "PERP", "USDC")?;
# Ok::<(), nautilus_backpack::config::BackpackConfigError>(())
```

## Durable local order identity

`identity::BackpackClientIdStore` owns a configured local state directory for one exact environment,
venue account and optional subaccount. It uses an OS `File::try_lock` held on a stable lock file for
the owner's entire lifetime. A second handle or process is refused; dropping or killing the owner
releases the OS lock without deleting its file. Namespace strings are explicit, never normalized,
and checked against the stored namespace before any mapping is restored.

`reserve_intent` commits the Nautilus `ClientOrderId`, monotonic Backpack uint32 `clientId`, and
original unsigned submission encoding together before returning an ID. IDs start at one, with zero
reserved conservatively, and are never reclaimed after cancellation, settlement, unknown outcome,
or an intent that was never sent. The next counter is u64 and refuses allocation at u32::MAX + 1.
Both lookup directions and `reserved_ids` survive restart. A duplicate creation intent is refused;
lookup and enumeration are recovery facts and never authority to resend. The external-order
sentinel and unknown venue IDs cannot be adopted as local ownership.

The journal has a strict versioned schema, validates bijection/high-water invariants, and carries
a BLAKE3 checksum over its complete state. The checksum detects accidental corruption, not
malicious edits or a valid historic rollback. Checkpoints use exclusive sibling temporary files,
file synchronization and atomic replacement, followed by synchronization of the replaced file.
Unix also synchronizes the containing directory. Initialization writes a permanent marker first;
a missing initialized journal, partial marker or interrupted initialization fails closed. Orphan
temporary files are never guessed to be newer authoritative state. A commit error returns no ID
and poisons the handle even when the disk outcome may be ambiguous; reopen and reconcile.

Use one stable, private directory per authenticated namespace throughout its lifetime. Moving to
a fresh directory, deleting all namespace state, externally replacing the lock file, whole-state
rollback, and faulty hardware/filesystems are outside this local guarantee. First use and recovery
must independently check venue-side external clientId collisions before execution is enabled.
The Windows guarantee covers process termination on a functioning local filesystem with OS locks
and atomic replacement; it does not claim power-loss durability without a directory metadata
barrier. Namespace and intent Debug representations omit private identity and payload fields.
Unsigned payloads must contain no credentials, authentication headers or signatures.

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
- [Authentication](https://docs.backpack.exchange/#section/Authentication): sorted REST signing,
  Ed25519/base64 headers, timestamp/window, and WS subscribe authentication.
- [Order query](https://docs.backpack.exchange/#tag/Order/operation/get_order): exclusive
  `orderId`/`clientId` identity. This does not establish clientId idempotency or historical finality.

Protocol evidence was checked on 2026-10-02. Metadata fixture provenance lives in
`test_data/manifest.json`. Unknown fields and mutation behavior must be verified before further
capabilities are exposed. No testnet, DMS, or clientId uniqueness guarantee is asserted.

## Validation

Run `cargo test -p nautilus-backpack` and `cargo clippy -p nautilus-backpack --all-targets -- -D warnings`.
Tests cover official BTC/SOL market observations and explicitly synthetic adverse metadata, exact
decimal and unit boundaries, public stream decoding, native depth replay and coverage faults, complete refreshes, economic provenance, configuration, and capability
refusals. Identity tests cover durable restart/mapping, exhaustion, checksum/schema corruption,
interrupted checkpoints, Windows replacement failure, and real multi-process ownership.

Authentication tests cover scalar/wire canonicalization, an independent public RFC 8032 section
[7.1 signature vector](https://www.rfc-editor.org/rfc/rfc8032#section-7.1), audience isolation, and
synthetic loopback transport. Transport probes verify fresh signatures after a queue longer than
the default window, stale admission/deadlines, shared public/private quota, redaction, redirects,
pagination, bounded retries, and cancellation before/after dispatch. Authentication test seeds and
transport responses are public synthetic material, never captured account fixtures. Tests make no
live venue requests or account mutations. Actual POST/DELETE paths are not implemented or tested.
See the repository [adapter guide](../../../docs/developer_guide/adapters.md) for later transport,
client, and acceptance tests.
