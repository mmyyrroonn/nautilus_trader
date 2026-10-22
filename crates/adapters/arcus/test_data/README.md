# Protocol fixtures

`market.json` uses the official `GET /v1/markets` field names and BTC-USD
increments observed on 2026-10-22. Prices, fee tier, limits, and the abbreviated
tick ladder are deterministic test inputs, not a current trading configuration.

`signature.json` was generated independently with Python `cryptography` 50.0.1,
using the published RFC 8032 section 7.1 test seed and the Arcus typed-payload
rules. The seed is public test material and has no associated Arcus account.
The fixture covers lexical key order, address case folding, preserved client ID
case, integer ticks and quantums, nanosecond timestamps, and microsecond expiry
conversion. No SDK implementation was copied.

Official protocol references:

- [Authentication](https://docs.arcus.xyz/api-reference/authentication)
- [REST trading guide](https://docs.arcus.xyz/guides/rest-trading)
- [Markets](https://docs.arcus.xyz/api-reference/public/get-markets)
- [Order history](https://docs.arcus.xyz/api-reference/public/get-order-history)
- [Fills](https://docs.arcus.xyz/api-reference/public/get-fills)
- [WebSocket lifecycle and sequence rules](https://docs.arcus.xyz/api-reference/websocket)
- [Rate limits](https://docs.arcus.xyz/api-reference/rate-limits)

The HTTP loopback tests verify the on-wire signing headers, account query, exact
decimal string encoding, asynchronous acknowledgments, uncertain write outcomes,
rate-limit hints, and overlapping microsecond history pagination.
