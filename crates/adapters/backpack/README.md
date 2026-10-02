# Backpack Exchange adapter foundations

This crate contains configuration, authentication and transport foundations, exact public market
metadata, bounded public stream/depth parsing, and durable local order identity for a phased Backpack Exchange integration. It validates
product eligibility and complete provider refreshes, constructs Ed25519 authentication, and provides
a restricted GET transport with typed account observations and bounded reconciliation.
The guarded order owner supports explicit loopback protocol peers. Engine data/account runtime
clients, execution integration, factories, and Python bindings remain later work. Constructing configuration and credentials performs no I/O or
environment lookup.

## Current capability boundary

| Surface                                             | Behavior                                        | Later scope                                     |
| --------------------------------------------------- | ----------------------------------------------- | ----------------------------------------------- |
| Symbol allowlist and market eligibility             | Implemented offline                             | Discovery and domain clients                    |
| Public metadata and instrument conversion           | Implemented with explicit economic inputs       | Runtime discovery and account verification      |
| Metadata provider                                   | Complete refresh validation without transport   | Data client supplies fresh responses            |
| Durable clientId and unsigned intent                | Local filesystem ownership and recovery tested  | Execution admission and reconciliation          |
| Endpoint validation and credential audience         | Implemented offline                             | Production or explicit local protocol peers     |
| REST signing and authenticated/public GET transport | Implemented with local transport tests          | Typed domain parsing and reconciliation         |
| Account snapshot/history and fill reconciliation    | Read-only protocol and delivery contracts       | Runtime coverage and durable consumer ACK       |
| Private WS subscription authentication              | Payload construction only                       | Connection lifecycle and account processing     |
| Public stream parsing and depth synchronization     | Bounded continuous views with explicit coverage | Native data client and WebSocket lifecycle      |
| Public market data and account runtime clients      | Explicit unsupported error                      | Instrument discovery, data, account state       |
| Guarded loopback mutations                          | Single-attempt protocol owner                   | Native execution/cache integration              |
| Engine execution and production writes              | Explicit unsupported error                      | Separately accepted private execution readiness |

`BackpackCapability::require_implemented` still returns `BackpackUnsupportedCapabilityError` for
all engine runtime capabilities. Low-level GET transport does not establish a working account or
data client. The guarded mutation owner below is limited to explicit local protocol peers.
Production submission/cancellation, modification, batches, borrowing, transfers, withdrawals,
and a dead man's switch remain unsupported. No engine execution client or Python write API is exposed.

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

## Guarded loopback order owner

`execution::owner::BackpackOrderOwner` is an independently testable protocol owner, without an
`ExecutionClient`, engine events, cache/portfolio delivery, factory, or Python interface. It rejects
production endpoints as an explicit runtime capability boundary. It requires exact agreement
between configuration/transport endpoints, endpoint-bound local credentials, and
`BackpackClientIdNamespace::loopback_peer(endpoints, account, subaccount)`. That namespace binds
both normalized loopback origins, not a generic environment label. Local peer account facts are
explicitly synthetic observations; account GETs with unknown identity/non-atomic coverage cannot
be promoted into production readiness by this API.

Supported commands use base quantity with exact metadata grids and bounds. Unsupported native
order types and TIFs fail locally. New risk supports **Buy Limit only**, with GTC/IOC/FOK;
post-only requires Limit/GTC. New-risk Market and Sell Limit are refused because their fill price
has no established finite notional upper bound. Reductions support Buy/Sell Limit with GTC/IOC/FOK
and Market with IOC/FOK, require `reduceOnly=true`, correct direction against a current signed net
position, and reserve quantity so concurrent reductions cannot exceed it. Future position
valuation, price movement and loss are not bounded by an entry-notional cap. Quote-sized,
conditional, broker/strategy, borrow/lend flags, modifications, batches and cancel-all have no
wire path. Advanced fields on a POST result cannot establish an independent owned binding.

`BackpackOrderOwnerConfig` requires finite expiry, evidence ages, order/aggregate notional,
margin, order count, and separate new-risk/reduction/owned-cancellation permissions. Accepted
`BackpackLoopbackAccountFacts` require complete allowlist net positions, an explicit available
USDC margin amount and explicit local margin/fee model provenance; wallet totals or public margin
functions do not supply these values. Entry notional is quantity times Buy limit; margin and fee
reserves use the explicitly supplied local peer model. Decimal arithmetic rejects overflow or
unrepresentable precision rather than rounding. Existing held reserves subtract from both account
capacity and authority limits, conservatively including already venue-locked amounts.

`owner.guard()` accepts a strictly increasing session generation and explicit peer account/market
facts. Both quote engine and receipt timestamps, metadata receipt, and account receipt must be
current, and future timestamps are rejected with no positive skew allowance (millisecond clock
precision). Authority expires at its exact millisecond boundary. Metadata is reparsed from retained
venue facts before submission rather than trusting a mutated public metadata container. Final
admission repeats after the shared quota and after durable attempt synchronization, including
changed permissions/limits and unchanged original reservation costs. Credentials are freshly
signed at that final boundary. `submit` and `cancel_owned` expose no raw URL, request body, or
admission callback that can bypass these checks. The existing GET client remains read-only.

Creation durably commits an immutable unsigned command/reservation and original uint32 clientId
before any first byte. `execution.json`, protected by the retained identity-store lock, records
attempts, independent venue bindings, held capacity and sticky shutdown evidence. Atomic
replacement and file synchronization follow the identity store's filesystem contract; Windows
process-termination guarantees do not establish power-loss or whole-directory rollback protection.
Checksum validation detects accidental corruption, not malicious edits. Any commit failure poisons
the owner. Missing execution state with existing identities restores original intents as Unknown,
never a new create. The exact transport budget starts before intent persistence and includes quota;
synchronous filesystem work cannot be preempted, but budget expiry refuses subsequent transport.

Exactly one POST/DELETE attempt is possible for each recorded command. Timeout, cancellation after
possible dispatch, 429, 5xx, malformed/mismatched response or resting-only 404 remain Unknown.
Only the narrow documented failure-code classification provides definitive rejection. Unknown and
unsent reservations retain capacity; unknown creates block additional new risk until independent
reconciliation. A later call or process restart cannot allocate another ID for the same intent.
Only a true POST response matching immutable standard-order fields independently binds a venue
order ID. Numeric clientId lookup alone never adopts an external order. `cancel_owned` uses only
that original bound venue ID and symbol; it requires current session/identity/cancel authority,
but does not require fresh new-risk market/account facts. DELETE 202 remains `CancelPending`.
HTTP 200 is response evidence requiring economic/lifecycle reconciliation, not a native terminal
event or fabricated fill.

`acknowledge_reconciled_terminal` is an explicit local peer integration boundary requiring the
original binding/generation, exact cumulative quantity, already-applied true fill quantity, and
an identified durable economic ACK. It cannot manufacture fees or fills; #53/native client delivery
must establish that acknowledgement. Cumulative quantities cannot regress below an observed ACK.
Terminal reconciliation can proceed after stop, and a late cancel 202 cannot undo it. Restored
unreconciled observations retain capacity and require facts, not retries. `stop()` freezes writes
and records unsent, unknown, observed/unreconciled, pending cancellation, and unknown/nonzero
positions. An expired flat snapshot is unknown. Once dirty, repeated stop/recovery cannot erase
the recorded dirty shutdown. Production private readiness, unknown-create lookup/binding recovery,
full cumulative/fill/fee handling and engine cache/portfolio acceptance remain subsequent #19 work.

All mutation fixtures are explicitly synthetic local peer payloads. Tests use real loopback HTTP
for accepted-but-lost responses, pending cancellation/fill races, shared-quota delays longer than
the signing window, stale/expired queued commands, durable checkpoint failure and restart.
No private venue call or production mutation is performed.

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

The following official sources establish the venue contract; the capability table above describes
the implemented scope:

- [Introduction and production origins](https://docs.backpack.exchange/#section/Introduction).
- [Market metadata](https://docs.backpack.exchange/#tag/Markets/operation/get_market): native symbols,
  `marketType`, `baseSymbol`, `quoteSymbol`, and decimal-valued filters.
- [Public streams](https://docs.backpack.exchange/#tag/Streams) and
  [depth snapshot](https://docs.backpack.exchange/#tag/Markets/operation/get_depth): envelope,
  timestamp units, sequence ranges, and explicit snapshot limits.
- [Order execution contract](https://docs.backpack.exchange/#tag/Order/operation/execute_order): venue
  order types and flags. The guarded loopback owner implements only the standard command subset
  described above; production writes remain unsupported.
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
decimal and unit boundaries, complete refreshes, economic provenance, configuration, and capability
refusals. Identity tests cover durable restart/mapping, exhaustion, checksum/schema corruption,
interrupted checkpoints, Windows replacement failure, and real multi-process ownership.

Public stream tests replay captured BTC perpetual quotes, trades, marks and depth frames. They
verify exact units/grids, nullable sides, atomic native book batches, initial overlap, gaps, bounded
buffers, stale generations and truncated price-band coverage.

Authentication tests cover scalar/wire canonicalization, an independent public RFC 8032 section
[7.1 signature vector](https://www.rfc-editor.org/rfc/rfc8032#section-7.1), audience isolation, and
synthetic loopback transport. Transport probes verify fresh signatures after a queue longer than
the default window, stale admission/deadlines, shared public/private quota, redaction, redirects,
pagination, bounded retries, and cancellation before/after dispatch. Authentication test seeds and
transport responses are public synthetic material, never captured account fixtures. Tests make no
live venue requests or real account mutations. Authentication/read-transport tests do not exercise
mutations; guarded loopback mutation validation is described above.
See the repository [adapter guide](../../../docs/developer_guide/adapters.md) for later transport,
client, and acceptance tests.

## Account observations and reconciliation

`account::client::BackpackAccountReader` wraps the shared authenticated GET client and quota.
`BackpackReadBudget::new(page_size, max_pages, max_items, timeout)` supplies a finite total deadline
covering the whole snapshot or traversal, including all transport retries and quota waits. Snapshot
reads use the documented account, balances, collateral, positions and resting orders endpoints.
Missing/null responses and failed requests are errors; valid empty maps/arrays remain observed empty.
An omitted position is unobserved and never manufactured as a flat position.

`snapshot` preserves wallet available/locked/staked independently from collateral assets, equity,
liabilities, exposure, margin fractions and PnL. `wallet_report` emits an exact native `AccountBalance`
for available+locked trading funds and separate staked/wallet totals; it never labels staked as free
or wallet balance as equity. Borrow/lend, liquidation, liabilities, collateral haircut/lending and
unsettled-equity policies degrade evidence. Unknown policy/enum/system fields remain observable.
No settings update or unsupported capability is emulated.

The account settings endpoint does not attest account/user/subaccount identity. Position/funding
rows may contain user/subaccount IDs, but no correlation to caller credentials or durable namespace
is inferred, especially from an empty response. Every snapshot retains `AccountIdentityUnverified`
and `NonAtomicSnapshot`. No generation, watermark, verified identity, freshness or execution-ready
proof is minted; the future execution owner must establish its own admission evidence.

History uses one fixed `[from_ms, to_ms)` window. Fills send both documented inclusive/exclusive UTC
filters on every page, without filtering away system fills. Orders and funding have no documented
server time filters; all observed rows are returned with `CutoffNotServerEnforced`, not falsely
restricted to the requested interval. Order event time and funding currency/timezone remain unknown.
All four required pagination headers are retained. Their total/count/size must stay consistent,
rows must cover the declared total exactly, page indices must progress from one observed zero/one
origin, and unique row identities must progress within page/item bounds. The reported page size must
match the explicit limit. Ambiguous header conventions, repeated rows/pages, failed pages and budget
exhaustion return errors. Header exhaustion still retains non-atomic and unknown retention/replication
coverage; it never proves an unknown submission was rejected or never executed. Resting-order HTTP404
is `UnknownNotResting`. Successful resting responses must match the exact requested symbol/selector.

`BackpackReportContext` requires the already validated instrument provider and durable client-ID
store. Only original persisted IDs supply candidate `Reserved` correlation with `OwnershipUnverified`;
native reports keep `client_order_id=None` unless a separate `BackpackOrderBindings` map from the
command owner establishes an acknowledged venueOrderId/instrument/native-ID link. Its explicit
`confirm_acknowledged` validates consistency against durable reservations but never attests arbitrary
caller claims. The owner must independently establish and persist that command evidence; observations
never populate the bindings. Unknown and system orders
remain external. This correlation is not venue idempotency or globally verified ownership. The
execution owner must detect pre-existing venue clientId collisions before write admission.
`fill_report`, `order_report` and `position_report` build actual native domain reports with exact
Decimal/Price/Quantity/Money round-trip checks. Unknown engine timestamps are zero plus explicit
coverage gaps, not guessed from integer magnitude or naive datetime. A missing true tradeId,
unsupported lifecycle/conditional order or unknown order economics/flags does not receive an invented
native report. True UTC fill timestamps, fee currency, exact rebates and quantities are preserved.
Funding remains signed exact raw cashflow: positive received, negative paid. No payment Money or
Unix event time is emitted without separate denomination/timezone proof.

`BackpackFillReconciler::stage` returns pending economic delivery until `acknowledge_with` successfully
commits the consumer's actual delivery acknowledgement and applied-fill record. A failed callback
leaves pending work retryable. No irreversible dedup record precedes economic delivery ACK.
The consumer must atomically persist application and its acknowledgement in its own journal and be
idempotent by native symbol/true tradeId. Restore only such durable records with `from_applied`;
unacknowledged crash windows redeliver at least once. Pending fills sort by event time and include
first fills before order acknowledgement and terminal late fills. This library does not claim an
atomic external-engine transaction or standalone durable economic journal.

Account fixtures under `test_data/account` have explicit official-schema versus synthetic provenance.
Tests issue signed, bodyless GETs only to audience-bound loopback listeners. They cover empty/missing,
unknown identity/policy, exact economics, system fills, bounded traversal faults and delivery ACK
crash windows; they do not demonstrate a live account or native ExecutionClient lifecycle.
