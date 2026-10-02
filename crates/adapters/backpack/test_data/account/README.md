# Account protocol fixtures

`official_schema.json` contains the unchanged GET operations and transitively referenced schemas extracted from the official Backpack embedded OpenAPI at https://docs.backpack.exchange/ on 2026-10-02. It preserves mandatory pagination headers and field descriptions. It is protocol documentation, not a captured private account response.

Every value in `synthetic.json` is explicitly synthetic offline test material constructed against those schemas. No account, credential or production response was accessed. Correspondence: `policy` -> AccountSummary; `balances` entries -> Balance; `collateral` -> MarginAccountSummary/Collateral; `position` -> FuturePositionWithMargin; `resting_order` -> OrderType_LimitOrder/LimitOrder; `history_order` -> Order; `fill` -> OrderFill; `funding` -> FundingPayment. Values identify a fictional user 101/subaccount 2 and fictional venue IDs.

Official protocol differences are deliberate: order clientId is uint32, fill clientId is a string; history order createdAt is naive datetime, resting createdAt is an integer with undocumented units. Only fill timestamp explicitly establishes UTC. Funding quantity sign is specified, but its currency and interval timezone are not. Synthetic values never supply evidence for those unknowns. Unknown/enlarged/malformed variants are labeled synthetic adverse mutations in tests.

The loopback signing seed is public test material `[7; 32]` and is audience-bound to the listener. No live API request or mutation is used by account tests.
