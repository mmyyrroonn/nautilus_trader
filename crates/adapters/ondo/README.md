# nautilus-ondo

[![build](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml)
![license](https://img.shields.io/github/license/nautechsystems/nautilus_trader?color=blue)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/NautilusTrader)

[NautilusTrader](https://nautilustrader.io) adapter for [Ondo Perps](https://docs.ondoperps.xyz),
a venue for tokenized-equity perpetual futures quoted in USD and settled in USDC.

The `nautilus-ondo` crate owns the protocol surface of the venue: instrument identity, exact
decimal precision, nanosecond timestamps, and the public market metadata that drives both.
Every price, quantity and fee the venue sends is a decimal string, and this crate keeps it as
`rust_decimal::Decimal` or a Nautilus domain type. Nothing on the tick/lot/price path routes
through `f64`.

This crate is built in phases inside the NautilusTrader fork and is not published to crates.io.
The code in this tree is the **P1 protocol core**: identity, precision, timestamps, and the
market-metadata parsing boundary. Transport clients, the `DataClient`, factories, recording,
signing, execution and the Python projection are owned by later tasks (see the module map).

## Module map

Only the modules marked *implemented* exist today. The others are named by the integration plan
so the ownership of the remaining work is unambiguous; do not add precision or identity logic
outside `common` and `http::models`.

| Module | Responsibility | Owner |
|---|---|---|
| `src/lib.rs` | Crate docs, module tree, lint policy. | Task 1 (implemented) |
| `src/config.rs` | `OndoDataClientConfig`: environment, `load_ids`, endpoint overrides, timeouts, `book_limit`, `raw_md_path`. | Task 1 (implemented) |
| `src/common/consts.rs` | Venue identity (`ONDO`, `ONDO_VENUE`, `ONDO_CLIENT_ID`), per-environment REST/WS endpoints, product/settlement/multiplier constants, tuning constants. | Task 1 (implemented) |
| `src/common/enums.rs` | `OndoEnvironment`, classified `MarketStatus` with its raw evidence, and `FeeRate`/`FeeSource`/`MarketFees` fee provenance. | Task 1 (implemented) |
| `src/common/parse.rs` | Exact RFC 3339 nanosecond parsing, market string → `InstrumentId`, and exact decimal/price/quantity increment parsing. | Task 1 (implemented) |
| `src/common/credential.rs` | One home for secrets (API key id and secret). Deliberately empty: the public data client holds no credentials. | Task 6 (signing/execution) |
| `src/http/models.rs` | `GET /v1/markets` DTOs, `MarketInfo`, and the **single** parsing boundary (`parse_markets`, `parse_instruments`). Shared by REST and WS; precision is never derived twice. | Task 1 (implemented) |
| `src/http/query.rs` | REST endpoint path constants. | Task 1 (implemented) |
| `src/http/error.rs` | HTTP error taxonomy and the retryability predicate. | Task 1 (implemented) |
| `src/http/client.rs` | REST transport: request building, signing, rate limiting, retries, pagination. | Task 2 / Task 6 |
| `src/websocket/*` | WS lifecycle, channel routing, snapshot/CLEAR semantics, subscription state. | Task 2 |
| `src/data.rs` | `InstrumentProvider` and the `DataClient` (subscriptions, Nautilus event publication). | Task 2 |
| `src/recording.rs` | Bounded public raw-frame recording with rotation and gap markers. | Task 4 |
| `src/signing.rs` | REST/WS API-key HMAC. No wallet signing. | Task 6 |
| `src/execution.rs`, `src/reconciliation.rs` | Order commands, fill dedupe, account init, reconnect recovery and DMS. | Task 7 / Task 8 — the DMS is **not accepted** (see below) |
| `src/factories.rs` | `DataClientFactory` / `ExecutionClientFactory` wiring. | Task 2 / Task 7 |
| `src/python/*` | PyO3 export of config, enums and factories. | Task 3 |
| `tests/http_contract.rs` | Offline contract tests for identity, precision, timestamps, metadata and fail-closed boundaries. | Task 1 (implemented) |
| `test_data/` | P0 protocol fixtures, manifest, conflict table and protocol table. | Task 0 (frozen) |

### The dead man's switch is not accepted

Task 8's last checklist item is a **sandbox** test of the switch: its real renewal message, its
30-second trigger, and what a disconnect does to resting orders and to a position. That test has
not been run, and it cannot be run from here - the switch is an account-level WebSocket channel and
this process holds no credential. Plan §6.4 is explicit about the consequence: *"该真实测试缺失时
DMS 标未验收"* - when the real test is missing, the DMS is marked not accepted. It is marked here.

What that does and does not cover. The local state machine *is* tested offline
(`tests/reconciliation.rs`): the frames this adapter would send, the deadline arithmetic, and what
it refuses to conclude when the switch fires. So this is a verified claim about the adapter's own
logic and an **unverified** claim about the venue: nothing here shows that the venue accepts the
renewal message this adapter builds or that it cancels resting orders when the switch fires. Treat
the switch as untested against Ondo Perps until the sandbox run happens.

## Instrument domain type

All markets in this phase are linear perpetuals, so an instrument is built as a Nautilus
`CryptoPerpetual` (`InstrumentAny::CryptoPerpetual`). This matches how the `lighter`,
`hyperliquid` and `aster` adapters build their perpetual instruments, and it is what the venue's
own product model describes: one contract is one base unit, so the multiplier is `1` and the
products are linear (`is_inverse = false`), quoted in USD and settled in USDC, with quantity
denominated in the base token.

- `InstrumentId` uses the Nautilus product marker, e.g. `NVDA-USD-PERP.ONDO`. The marker keeps the
  instrument distinct from any future Ondo product family that shares a base token.
- `raw_symbol` keeps the venue's own string verbatim, e.g. `NVDA-USD.P`, so a round trip back to a
  subscription does not go through the Nautilus marker.
- Precision comes from the venue's declared increments: `baseIncrement` is the quantity step
  (`size_increment`), `quoteIncrement` is the price step (`price_increment`). They are never
  swapped and never inferred from observed price decimals.
- The venue's market metadata supplies no venue timestamp, so an instrument's `ts_event` falls
  back to `ts_init`, the local time the payload entered the adapter.

## Support matrix

The capability stance for this adapter (supported / explicitly unsupported / out of scope) is
frozen at P0 in **[`test_data/README.md`](test_data/README.md#support-matrix-per-plan-1-frozen-at-p0)**;
that table is authoritative and is not duplicated here.

Two rules from it apply directly to this crate:

1. A market with an unclassifiable status fails the load; a *known* `disabled` market is still
   returned as a complete instrument, with its status and fee provenance attached, so callers can
   gate new orders on `is_tradable()`.
2. Absent metadata fails closed. A missing, zero or negative increment, a missing requested
   `load_id`, or an empty market list is a start-up error, never an empty success and never a
   guessed default.

## Test data provenance

Protocol fixtures live in [`test_data/`](test_data). Always read a fixture's `kind` from
[`test_data/manifest.json`](test_data/manifest.json), **never** from its file name: for example
`test_data/ws/markprices_observed.json` is an `official-example` despite the `_observed` suffix.
Inline test bodies carry an explicit `_fixture.kind` marker so a synthetic body can never be
mistaken for a captured response. The live REST host answered HTTP 403 during the protocol freeze,
so every REST fixture is synthetic and marked as such.

## Feature flags

- `high-precision` (default) - enables NautilusTrader's 128-bit value types via
  `nautilus-model/high-precision`, matching
  [high-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation#precision-mode).
- `python` - **not yet defined.** Task 3 adds it together with the PyO3 export and extension-module
  wiring; the Python package is exported as `nautilus_trader.adapters.ondo`.

## Build and test

All commands run from the fork root (`E:\nautilus_trader`), which pins the toolchain with
`rust-toolchain.toml`.

```bash
# Full crate test suite (unit tests + integration tests + doc tests).
cargo test -p nautilus-ondo

# The Task 1 contract tests only.
cargo test -p nautilus-ondo --test http_contract

# Formatting check (no write).
cargo fmt -p nautilus-ondo -- --check
```

The fork's `CONTRIBUTING.md` and `AGENTS.md` name these repository-wide checks; run them before a
change is considered review-ready:

```bash
make format      # cargo +nightly fmt  (requires the nightly toolchain)
make pre-commit  # prek run --all-files
make pre-flight  # optional: broad local validation suite
make cargo-test  # workspace Rust tests (cargo nextest)
```

After Task 3 wires the PyO3 export, it will additionally run the python-feature checks:

```bash
cargo check -p nautilus-ondo --features python
cargo check -p nautilus-pyo3
```

## License

Licensed under the GNU Lesser General Public License Version 3.0 (the "License").
