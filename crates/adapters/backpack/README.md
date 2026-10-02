# Backpack Exchange adapter foundations

This crate contains the B0 foundations of a phased Backpack Exchange integration. It validates
configuration and product eligibility, constructs Ed25519 authentication, and provides a restricted
GET transport. Domain data/account clients, execution, factories, and Python bindings remain later
work. Constructing configuration and credentials performs no I/O or environment lookup.

## Current capability boundary

| Surface                                             | Behavior                               | Later scope                                     |
| --------------------------------------------------- | -------------------------------------- | ----------------------------------------------- |
| Symbol allowlist and market eligibility             | Implemented offline                    | Discovery and domain clients                    |
| Endpoint validation and credential audience         | Implemented offline                    | Production or explicit local protocol peers     |
| REST signing and authenticated/public GET transport | Implemented with local transport tests | Typed domain parsing and reconciliation         |
| Private WS subscription authentication              | Payload construction only              | Connection lifecycle and account processing     |
| Public market data and account runtime clients      | Explicit unsupported error             | Instrument discovery, data, account state       |
| Restricted execution                                | Explicit unsupported error             | Guarded order surface after deterministic tests |

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
allowlist. A matching suffix cannot substitute for this metadata check. Market discovery and the
conversion to a USDC-settled Nautilus perpetual remain later work. All monetary values must use
exact decimals or Nautilus domain types when those modules are introduced; this phase has no
price, quantity, fee, or funding arithmetic.

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

## Protocol references

The following official sources establish the venue contract, not implemented runtime features:

- [Introduction and production origins](https://docs.backpack.exchange/#section/Introduction).
- [Market metadata](https://docs.backpack.exchange/#tag/Markets/operation/get_market): native symbols,
  `marketType`, `baseSymbol`, `quoteSymbol`, and decimal-valued filters.
- [Order execution contract](https://docs.backpack.exchange/#tag/Order/operation/execute_order): venue
  order types and flags. No order payload or execution semantics are implemented here.
- [Authentication](https://docs.backpack.exchange/#section/Authentication): sorted REST signing,
  Ed25519/base64 headers, timestamp/window, and WS subscribe authentication.
- [Order query](https://docs.backpack.exchange/#tag/Order/operation/get_order): exclusive
  `orderId`/`clientId` identity. This does not establish clientId idempotency or historical finality.

Protocol evidence was checked on 2026-10-02. Unknown fields and mutation behavior must be verified
before further capabilities are exposed. No testnet, DMS, or clientId uniqueness guarantee is asserted.

## Validation

Run `cargo test -p nautilus-backpack` and `cargo clippy -p nautilus-backpack --all-targets -- -D warnings`.
Tests cover configuration, scalar/wire canonicalization, an independent public RFC 8032 section
[7.1 signature vector](https://www.rfc-editor.org/rfc/rfc8032#section-7.1), audience isolation, and synthetic loopback transport. Transport probes verify
fresh signatures after a queue longer than the default window, stale admission/deadlines,
redaction, redirects, pagination, bounded retries, and cancellation before/after dispatch. Seeds
and responses are public synthetic material, never captured account fixtures. These tests make no
live venue requests or account mutations. Actual POST/DELETE paths are not implemented or tested.
See the repository [adapter guide](../../../docs/developer_guide/adapters.md) for later transport,
client, and acceptance tests.
