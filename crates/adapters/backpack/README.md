# Backpack Exchange adapter foundations

This crate supplies configuration and durable local order identity for a phased Backpack Exchange integration. It validates
configuration and product eligibility. It has no HTTP or WebSocket client, live data or execution
client, authentication, account access, factory, or Python bindings. It is not a production adapter.
Constructing a configuration performs no I/O and reads no credentials or environment files.

## Current capability boundary

| Surface | Current behavior | Later scope |
| --- | --- | --- |
| Symbol allowlist and market eligibility | Implemented and tested offline | Used by discovery and both clients |
| Endpoint validation | Implemented and tested offline | Public production or explicit local protocol peers |
| Durable clientId and submission intent | Implemented with local filesystem tests | Native execution admission and reconciliation |
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
- [Order execution contract](https://docs.backpack.exchange/#tag/Order/operation/execute_order): venue
  order types and flags. No order payload or execution semantics are implemented here.

Authentication, signing, request canonicalization, transport, fixture
provenance, and recovery are separate subsequent changes. Unknown protocol fields or behavior must
be checked against official evidence before those changes expose capabilities.

## Validation

Run `cargo test -p nautilus-backpack` and `cargo clippy -p nautilus-backpack --all-targets -- -D warnings`.
Tests cover allowlist errors, metadata filtering, production defaults, loopback origin validation,
explicit refusals of planned capabilities, durable restart/mapping, exhaustion, checksum/schema corruption, interrupted checkpoints, and real multi-process ownership. They perform no network I/O or account operations.
See the repository [adapter guide](../../../docs/developer_guide/adapters.md) for later transport,
client, and acceptance tests.
