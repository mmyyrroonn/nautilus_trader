# Backpack public market fixtures

`manifest.json` records each file's source URL, kind, observed HTTP Date, and SHA256. The two
market JSON files are complete unmodified public `GET /api/v1/market` bodies, with only a final
newline added for repository formatting. They contain no account identifiers or credentials.

`official_market_schema.json` contains unmodified named schemas extracted from the official
API page's embedded `__redoc_state.spec.data.components.schemas`. The schema defines decimal
strings, required filter fields, optional funding values, interval milliseconds, funding basis
points, and supported market/book-state enums. The original `createdAt` format is a naive datetime;
no timezone has been invented.

Tests name synthetic inputs explicitly when mutating valid observations. Mutated finite maxima,
precision limits, disabled markets, source economics, refresh timestamps, malformed values, and
future numeric fields are adverse or framework test evidence, not captured venue responses.
No tests use private account data or perform live network access.

The four `official_*.json` stream examples are unmodified JSON payload blocks extracted from the
official Streams documentation, with JSON comments removed. Their original `SOL_USDC` spot
identity is preserved; tests reject them for the perpetual allowlist rather than relabeling them.
`public_*.json` are actual unauthenticated production WebSocket envelopes from the four BTC USDC
perpetual public subscriptions. `btc_depth_limit5.json` is a complete public REST depth body for
an explicit five-level request. WebSocket and snapshot captures were taken independently; tests
do not claim that they form a continuous bootstrap. Synthetic continuity replays are named explicitly.

The observed native book ticker `u` is an integer despite the documentation's string example.
Neither funding `f` shape nor metadata's separate bps bounds establish the stream funding rate
unit. Capture timestamps and source channels are recorded in the manifest. Test runs read local
fixtures only; collecting this evidence made no authenticated request or account mutation.
