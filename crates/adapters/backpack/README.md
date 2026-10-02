# Backpack Exchange adapter foundations

This crate is the B0.1 foundation of a phased Backpack Exchange integration. It validates
configuration and product eligibility. It has no HTTP or WebSocket client, live data or execution
client, authentication, account access, factory, or Python bindings. It is not a production adapter.
Constructing a configuration performs no I/O and reads no credentials or environment files.

## Current capability boundary

| Surface | B0.1 behavior | Later scope |
| --- | --- | --- |
| Symbol allowlist and market eligibility | Implemented and tested offline | Used by discovery and both clients |
| Endpoint validation | Implemented and tested offline | Public production or explicit local protocol peers |
| Public market data | Explicit unsupported error | Instrument discovery and market data |
| Private account reads | Explicit unsupported error | Account state and reconciliation |
| Restricted execution | Explicit unsupported error | A bounded order surface after deterministic tests |

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
- [Order execution contract](https://docs.backpack.exchange/#tag/Order/operation/execute_order): venue
  order types and flags. No order payload or execution semantics are implemented here.

Authentication, signing, request canonicalization, client order identity, transport, fixture
provenance, and recovery are separate subsequent changes. Unknown protocol fields or behavior must
be checked against official evidence before those changes expose capabilities.

## Validation

Run `cargo test -p nautilus-backpack` and `cargo clippy -p nautilus-backpack --all-targets -- -D warnings`.
Tests cover allowlist errors, metadata filtering, production defaults, loopback origin validation,
and explicit refusals of planned capabilities. They perform no network I/O or account operations.
See the repository [adapter guide](../../../docs/developer_guide/adapters.md) for later transport,
client, and acceptance tests.
