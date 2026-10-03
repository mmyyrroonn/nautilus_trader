# Backpack protocol evidence

This evidence review covers the credential-free portion of
[issue 85](https://github.com/mmyyrroonn/nautilus_trader/issues/85). It applies to the
Backpack BTC/SOL USDC perpetual allowlist and the adapter at base
`b702553eb7`, with the issue 85 changes reviewed separately. It does not authorize
production mutations or establish production readiness.

## Sources and reproducibility

The official API page was retrieved on **2026-10-03 at 15:58:25 UTC**. Its embedded
OpenAPI version is `3.0.0`; `info.version` is `1.0`, not a pinned deployment build.
The response HTTP Date was `Sat, 03 Oct 2026 15:58:23 GMT`. Selected unchanged operations
and transitively referenced schemas are in
[`official_openapi.json`](../../crates/adapters/backpack/test_data/protocol/official_openapi.json).
The original HTML SHA-256 and collection metadata are in
[`collection.json`](../../crates/adapters/backpack/test_data/protocol/collection.json).

[`manifest.json`](../../crates/adapters/backpack/test_data/protocol/manifest.json) records
file SHA-256, source URL, date, version applicability and transformations. It distinguishes
`official-example` (published sample, never an account observation), `official-schema`,
`official-summary`, `synthetic` and `public-observed`. A hash identifies these stored bytes;
it does not assert that an unversioned remote page or deployment never changes.

The public WebSocket transcript records the exact unsigned subscribe text, UTC start/end,
8-frame bound, 8-second deadline and original text payloads. Only public BTC mark-price and
book-ticker streams were requested. Every captured frame had `stream`/`data`; none was a
standalone success ACK. This finite observation cannot establish that all subscriptions or
private connections have the same behavior. No credential, account response, `.env` or
venue mutation was used. Tests consume local files without network access.

## Order identity and history

The official [order operation](https://docs.backpack.exchange/#tag/Order/operation/get_order)
describes `clientId` as a custom `uint32`. GET accepts exactly one of `orderId` or `clientId`;
providing both makes signed requests fail signature verification. This endpoint returns only
resting orders, so a not-found response cannot disprove an earlier fill, expiry or cancellation.
History offers an `orderId` filter; it does not document a clientId history filter. Order rows
have integer clientId; fill history uses an optional decimal string clientId.

Neither the inspected order operations nor schemas specify account/symbol/API-key uniqueness,
when reuse is allowed, collision response semantics, an idempotency promise, history retention
period or maximum replication lag. These remain **unknown**. Local permanent allocation and an
independent original POST binding are therefore still required. A numeric query match alone is
only a candidate; system fills remain external even when the numeric clientId and an existing
local binding coincide. The new regressions exercise production DTO range checking and native
fill ownership. Existing lost-ACK tests in `tests/execution.rs` and `tests/execution_client.rs`
cover no retry/replacement/adoption after an uncertain POST.

A controlled reuse experiment would be a mutation and needs separate authorization. A read-only
account capture can observe records; it cannot prove a global collision or retention guarantee.
A written venue contract can resolve those policy facts without credentials.

## Account and subaccount identity

Official [subaccount documentation](https://support.backpack.exchange/exchange/exchange-account/account-functions/sub-accounts)
says each subaccount can manage its own API keys and transaction history. The
[technical futures specification](https://support.backpack.exchange/technical-docs/trading/futures-specs)
describes cross-margin within each subaccount and isolation between subaccounts.

The API AccountSummary has settings/limits and fee fields, with no `userId` or `subaccountId`.
Positions and funding-payment rows have a userId and optional subaccountId. Those rows provide
scope observations when present; they do not attest that a caller-configured account label or
persistent namespace belongs to the credential, and empty results supply no identity row.
`AccountIdentityUnverified` remains explicit. Production authority is not promoted by configuration,
REST completion, connection success or synthetic facts. Credential-backed read-only evidence is
still needed for the actual intended account/subaccount binding, including empty-state behavior.

## Subscription acknowledgement

The [Streams usage documentation](https://docs.backpack.exchange/#tag/Streams)
defines subscribe/unsubscribe requests, signed private subscribe parameters and data envelopes.
It does **not** define a positive-success ACK response schema, request correlation guarantee or
an authoritative empty-private-snapshot completion marker in the inspected material.
Position subscription may emit existing open positions without `e`, only if positions exist.
That is initial data, not proof that an empty account snapshot is complete.

The native private decoder treats generic `result`, `success` or request-shaped controls as
unconfirmed observations with `UnknownVenueState` and `AccountIdentityUnverified`. New tests
exercise that decoder and confirm telemetry still reports private subscription unconfirmed.
The existing native runtime test
`test_native_connect_rest_reports_signed_private_stream_and_bounded_repeated_stop` asserts
`BackpackAccountState::Degraded` after connection and REST observations;
`test_private_generic_ack_and_valid_empty_control_never_establish_success_or_flat` covers
unknown controls without inventing success or flatness. Public parser subscription/data delivery
does not establish a private ACK. Actual signed private
traffic plus an official response contract remains needed before a private-success capability
can be enabled. An official contract may be obtained without account access.

## Funding estimate and settled funding

`markPrice.f` is an estimated rate, `n` is the next funding timestamp in milliseconds, and event/
engine times are microseconds. The published sample and actual public frame do not identify the
`f` rate denominator (fraction/percent/bps). Market bounds are separately described in basis
points; that is not evidence that stream `f` shares their denominator. The public parser preserves
`raw_rate` exactly and performs only the documented timestamp conversion.

The [futures fee documentation](https://support.backpack.exchange/exchange/trading/futures/fees)
establishes the payer direction: positive rates make longs pay shorts; negative rates reverse it.
Intervals should be checked per pair. The technical specification gives payment = rate times
position quantity times mark price, and currently describes markets as denominated and settled in
USDC. That is documented product settlement evidence, not a captured account cashflow.

FundingPayment.quantity is a signed payment amount, positive received and negative paid. Its row
has no currency field; its interval timestamp is `naive-date-time` without an explicit timezone.
FundingIntervalRate has the same timezone/denominator ambiguity. These missing wire facts do not
erase the documented product-level USDC settlement statement. The current generic account report
retains `FundingCurrencyUnknown` and `FundingTimezoneUnknown` rather than inventing a self-described
Money or settlement instant. The new test separately exercises a real public estimate and an
explicitly synthetic settled-payment row; neither is normalized into the other.

An explicit venue denominator/timezone statement can resolve protocol ambiguity without credentials.
Matching real settled funding, intended account scope and cash ledger requires read-only credentials;
a public estimate alone never proves a settled amount or an economic receipt.

## Wallet, collateral, margin, fees and system events

Balance.available, locked and staked are distinct fields. The official private balance example
contains absolute balances after a change, not deltas. Native decoding preserves that observation;
wallet conversion excludes staked from trading balance and retains the total separately. A wallet
balance is not collateral equity or available margin.

MarginAccountSummary separately supplies collateral assets/weights, liabilities, unsettled equity,
net equity and locked/available equity; IMF/MMF are fractions. The technical specification explains
haircut collateral and available equity after locked equity. It also documents lending collateral
and default continuous PnL realization. The existing reader refuses unsupported account-policy or
collateral coverage for readiness, and marks snapshots non-atomic; a simple wallet total cannot
replace those facts. Borrowing, lending, transfers and continuous realization ledgers are not
implemented as a complete economic consumer by this review.

AccountSummary maker/taker fee fields are **basis points**; maker fees can be negative rebates.
An OrderFill fee is already a signed asset-denominated amount, paired with feeSymbol. A negative
fee is preserved as negative commission, never treated as a price-rate multiplier. The new synthetic
regression calls production `fill_report` and checks exact negative commission with unverified
local ownership. Actual fee tier/rebate eligibility still requires intended-account settings.

SystemOrderType includes collateral conversion, expiry, ADL, order-book/backstop liquidation and
book closure. Private order origin `O` has a separate documented spelling family. Since the
2025-04-22 API change, default fill history includes system fills; setting fillType=User would
exclude them. The reader does not apply that narrowing. Known/unknown system fills cannot become
local order-owned fills; unknown types retain their economics and explicit unknown-state gaps.
This does not claim complete transfer/borrow/lend/liquidation account economics.

## Pagination boundaries and uncertainty

History limit defaults to 100 and is capped at 1000, offset defaults to zero. OpenAPI declares
X-PAGE-COUNT, X-CURRENT-PAGE, X-PAGE-SIZE and X-TOTAL response headers. Their indexing origin,
empty-page convention, stable snapshot and replication guarantees are unspecified. Fill-history
`from` is inclusive and `to` exclusive milliseconds. Order/funding history provide no such time
filters, so their reader records `CutoffNotServerEnforced` rather than inventing wire parameters.

The strict tracker requires all four headers, validates progression, total/size and unique record
keys, and bounds rows/pages/deadline. It preserves a zero- or one-origin first page rather than
claiming a documented origin. Header exhaustion still records `NonAtomicSnapshot` and
`RetentionOrReplicationUnknown`. New tests exercise required-header parsing and native page-size/
window bounds. Existing account loopback tests cover full multi-page traversal, fixed fill cutoff,
changing totals, duplicate records, missing headers and bounded failures. Those are synthetic
protocol tests, not authenticated pagination observations or proof of venue replication latency.

## Validation and remaining acceptance

`tests/protocol_evidence.rs` exercises native account/public parsing, exact rebate economics,
external system attribution, unknown controls, clientId bounds and pagination bounds. The focused
command uses the assigned cache target:

```powershell
$env:PYO3_PYTHON='E:/persarb/nautilus_trader/.venv/Scripts/python.exe'
$env:CARGO_BUILD_WARNINGS='allow'
cargo test -p nautilus-backpack --test protocol_evidence --offline `
  --target-dir E:/persarb/.backpack-target/instruments
```

The focused command passed on 2026-10-03 UTC: **8 passed, 0 failed**.
`rustfmt +nightly --check crates/adapters/backpack/tests/protocol_evidence.rs` passed.
All nine evidence JSON files parsed successfully and their stored SHA-256 values matched
`manifest.json`. These are offline parser/report tests, not venue account acceptance.

No private fixture is public-observed. Remaining account observations are actual credential-to-
account/subaccount binding, signed private subscription behavior, account policy and fee tier,
collateral/margin completeness, real funding/fee/system cashflows and pagination/empty-history
behavior. Remaining venue policy facts are clientId collision/reuse/retention guarantees, positive
ACK contract, funding wire units/timezone and replication guarantees. Credentials alone do not
settle these policy guarantees. Production mutation permission, durable economic application,
continuous reconciliation and cross-venue acceptance remain separate criteria in the
[send contract](backpack-send-contract.md).
