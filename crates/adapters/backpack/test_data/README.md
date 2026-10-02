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
