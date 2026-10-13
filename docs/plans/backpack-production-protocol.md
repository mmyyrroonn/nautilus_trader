# Backpack production protocol evidence

Reviewed on 2026-10-13. This review made no account requests, used no credentials,
and submitted no venue mutations. Network observation of the user's account is
maintained separately by the workspace owner.

## Primary evidence

| Evidence                                                                                                                      | Meaning for this adapter                                                                                                                                                                                                        |
| ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [API key creation](https://support.backpack.exchange/exchange/api-and-developer-docs/generate-api-keys-for-backpack-exchange) | Key creation selects a subaccount; read-only is an explicit key permission.                                                                                                                                                     |
| [Subaccounts](https://support.backpack.exchange/exchange/exchange-account/account-functions/sub-accounts)                     | Subaccounts own separate API keys and transaction histories.                                                                                                                                                                    |
| [Futures specifications](https://support.backpack.exchange/technical-docs/trading/futures-specs)                              | Margin is cross-account within each subaccount; current futures settlement is USDC; available equity subtracts locked equity; IMF/MMF depend on exposure size.                                                                  |
| [Current API reference](https://docs.backpack.exchange/)                                                                      | Ed25519 signatures use alphabetically sorted parameters plus instruction, millisecond timestamp and window. Order creation is POST and cancellation DELETE on `/api/v1/order`; private streams use the `subscribe` instruction. |
| Existing captured OpenAPI: `crates/adapters/backpack/test_data/account/official_schema.json`                                  | Exact account decimal schema, fee basis-point units, fill fee currency, and history response headers. The captured schema is historical evidence; current endpoint review does not rewrite its provenance.                      |

## Implemented credential authority

`BackpackClientIdNamespace::production_credential` binds official REST/WebSocket
origins and a BLAKE3 fingerprint of the canonical Ed25519 verifying key. The key is
not stored in the journal. A successful authenticated response can establish this
credential authority even if the account has no positions. It does not verify a
configured display name or numeric venue user ID. Changing keys changes the
namespace; reusing the old journal fails rather than adopting client IDs.

`credential_scope` also supports an exact, audience-bound loopback peer for
protocol simulation. Production credentials cannot authenticate a loopback peer,
and peer credentials cannot authenticate production. This simulation receives
facts from the same signed HTTP reader instead of externally injecting account
readiness. An optional numeric subaccount assertion must match observed position
rows; an empty position response cannot verify that additional assertion.

## Account admission contract

The reader captures five successful, separately decoded GET responses under one
deadline: settings, wallet balances, collateral, positions, and open orders. It
stores the credential scope and traversal start time internally. The private
`production_facts` conversion checks the exact scope and derives available margin
from `netEquityAvailable`, never wallet free balance. Freshness starts at the first
read, so a slow traversal cannot manufacture a newer observation.

Only unborrowed, unlent, unstaked, par-valued USDC collateral is supported. Automatic
lending, repayment, liquidating state, unsettled claims, unknown response
fields, contradictory position identities, or positions outside the allowlist
refuse admission with a concrete error. Current authenticated open positions use
signed net quantities; missing allowlisted rows yield zero only for this endpoint
observation. The generic read-only `position()` API retains its existing unknown
absence semantics.

The account's `autoBorrowSettlements` and continuous PnL realization settings can
remain enabled. They are observations, not native borrowing authority. Native
orders explicitly disable all four spot-margin auto flags; these flags do not
disable separate futures settlement policy. `borrowLiability` and `unsettledEquity`
must remain zero. `liabilitiesValue` is accepted only when it exactly equals the
absolute value of negative `pnlUnrealized`; other liabilities refuse admission.
Assets, collateral, signed PnL, equity, and available plus locked margin must agree
at representable Decimal precision. Full-notional plus fee reserves constrain entry
without claiming future price moves or funding can never create settlement debt.
The snapshot retains the true account settings; `facts.auto_borrow=false` describes
native order capability.

The guard reserves the entire new notional plus a fee buffer. This is a deliberate
conservative capital limit, not a claim that the venue uses a constant margin tier.
The buffer uses the larger absolute account futures maker/taker basis-point fee,
converted with exact Decimal arithmetic. Actual fill commissions retain their
reported `feeSymbol`; maker rebates remain signed values. Public instrument
economics do not attest private fee tiers or account capacity.

REST responses have no cross-endpoint transaction watermark. Non-atomicity and
unknown position/order event times remain in the snapshot evidence. Runtime must
fence private observations across reconciliation and invalidate queued admission
when private or public evidence changes. Settings are never patched automatically.

## Orders, history and transport boundaries

The supported wire subset is single linear USDC perpetual orders with uint32
client IDs. Limit/Market, GTC/IOC/FOK, post-only and reduce-only are venue concepts;
unsupported combinations are rejected by native order validation. A resting-order
404 means only that the order is not resting; it cannot justify retransmission.
Response order IDs are opaque strings, and their values cannot establish time.

History reads are bounded offset/limit traversals. Fill history has a server
millisecond `[from, to)` filter; order and funding history do not establish that
cutoff. Fill identity uses genuine `tradeId` plus symbol; system fills must remain
observable. The captured OpenAPI establishes UTC for fill timestamps. Funding
interval timezone and denomination remain explicit gaps in the history model.
The current funding-payment stream is a separate newer capability, not evidence
that the old history payload gained currency/timezone fields.

Market admission must retain exact tick/step and min/max grids plus current venue
book state. Metadata availability alone does not attest an open, tradable book.
Dynamic price bands remain exchange-enforced; a rejected or expired order must
follow venue lifecycle evidence. Mutation retry is not delegated to the GET reader.

## Unresolved protocol facts

- Public API documentation does not establish client-ID uniqueness lifetime,
  reusability, exhaustive retention, or historic collision absence. Local durable
  allocation burns IDs and never automatically retries unknown submissions.
- Subscription success is not a complete account snapshot or an account-wide
  sequence watermark. Startup silence cannot attest flatness.
- History pagination completion does not prove replica catch-up, immutable
  retention, or cross-endpoint atomicity. Reconciliation preserves those gaps.
- The account settings endpoint does not report a numeric account identity or a
  separately selectable hedge-mode field. Credential binding and the documented
  signed net-position representation supply the supported identity/netting scope.
- Account policy outside the restricted supported slice remains a configuration
  gate. A supplied credential or a read-only success does not authorize modifying
  settings or placing mainnet orders.

## Verification scope

Unit coverage checks credential rotation/journal refusal, redacted namespace
storage, full-notional and exact basis-point reserves, traversal-start freshness,
and refusal of unbound responses, borrowing, haircuts, unknown fields, and unverifiable
numeric subaccount assertions. Signed-loopback traversal tests exercise the actual
reader separately. These tests are synthetic protocol evidence; current production
admission still depends on native runtime observations and supported account policy.

On 2026-10-13, operator-run live acceptance verified two BTC round trips through the
native ExecutionEngine writer: post-only maker BUY followed by market reduce-only
SELL, then market BUY followed by market reduce-only SELL. Each of the four legs
was 0.00005 BTC, approximately USDC 4.16. All four orders filled and reached
reconciled terminal evidence. Actual fill observations verified the maker entry
and taker market legs. Final REST observations showed a flat position and no open
orders.

The combined wrapper's final account reconciliation was false: a preceding
external cleanup fill was included by the new session's five-second history
backpad. The harness has not been changed, so this run is not a complete wrapper
pass. The four native legs and final REST observations establish only the stated
BTC scenarios. Continuous multi-symbol execution and the complete acceptance
scope of issue 123 remain unverified.
