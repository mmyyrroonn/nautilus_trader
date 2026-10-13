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
Every layer in the module map below exists today: the public data client and its transport, the
private login-required session the account lives on, the execution client with its order write
surface, the reconciliation machine with its durable ledger journal and its dead man's switch,
recording, signing, the factories and the Python projection.

The fixtures in `test_data/` record the original protocol baseline. Acceptance evidence must name
its exact source and wheel: a historical bounded-probe run does not establish continuous execution
acceptance. The continuous implementation below is exercised against local synthetic services;
this change does not claim a live venue run.

## Module map

Every module below exists today; the status column marks the rows that carry a caveat. Precision and
identity logic stays in `common` and `http::models`, and is not duplicated elsewhere.

| Module | Responsibility | Status |
|---|---|---|
| `src/lib.rs` | Crate docs, module tree, lint policy. | Implemented |
| `src/config.rs` | `OndoDataClientConfig` and `OndoExecutionClientConfig`: environment, `load_ids`, endpoint overrides, timeouts, `book_limit`, `raw_md_path`, and the execution side's credential, account mode, journal and switch settings. | Implemented |
| `src/common/consts.rs` | Venue identity (`ONDO`, `ONDO_VENUE`, `ONDO_CLIENT_ID`), per-environment REST/WS endpoints, product/settlement/multiplier constants, tuning constants. | Implemented |
| `src/common/enums.rs` | `OndoEnvironment`, classified `MarketStatus` with its raw evidence, and `FeeRate`/`FeeSource`/`MarketFees` fee provenance. | Implemented |
| `src/common/parse.rs` | Exact RFC 3339 nanosecond parsing, market string → `InstrumentId`, and exact decimal/price/quantity increment parsing. | Implemented |
| `src/common/endpoint.rs` | Authenticated endpoint allowlist for the configured environment and explicit loopback test services. Production writes additionally require native execution authority. | Implemented |
| `src/common/credential.rs` | Environment-bound API key credentials with redacted secrets. The public data client holds no credentials. | Implemented |
| `src/http/models.rs` | `GET /v1/markets` DTOs, `MarketInfo`, and the **single** parsing boundary (`parse_markets`, `parse_instruments`). Shared by REST and WS; precision is never derived twice. | Implemented |
| `src/http/query.rs` | REST endpoint path constants, the request target the signature covers, and cursor pagination. | Implemented |
| `src/http/error.rs` | HTTP error taxonomy and the retryability predicate. | Implemented |
| `src/http/client.rs`, `src/http/rate_limit.rs` | REST transport: request building, signing, retries, pagination, and the one shared request budget per environment that both clients draw on. | Implemented |
| `src/http/orders.rs` | The order write surface: the locally validated create command, the exact JSON body the signature covers, and the named local refusals (`FOK`, conditional orders, `quoteSize`, `postOnly` on a market order). | Implemented |
| `src/http/private.rs` | The authenticated read surface: request shapes and response schemas for the account, positions, balance, orders and funding fees. | Implemented |
| `src/websocket/*` | Public feed: wire schema, wire-to-domain parsing, the book state machine (a snapshot becomes one `CLEAR` + `ADD` batch carrying `F_SNAPSHOT`/`F_LAST`), and the session/transport lifecycle (one connection, heartbeat, idle bound, reconnect). | Implemented |
| `src/websocket/private/*` | The account's own connection: the login handshake, the private channel schema, the private session state machine, its own diagnostics record, and the transport that owns the socket. | Implemented - offline-verified only |
| `src/data.rs` | `InstrumentProvider` and the `DataClient` (subscriptions, Nautilus event publication). | Implemented |
| `src/recording.rs` | Bounded public raw-frame recording with rotation and gap markers. | Implemented |
| `src/signing.rs` | REST/WS API-key HMAC. No wallet signing. | Implemented |
| `src/execution.rs`, `src/reconciliation.rs` | Order commands, fill dedupe, account state, reconnect recovery, the durable ledger journal and the DMS. | Implemented - the DMS is **not accepted** (see below) |
| `src/production.rs`, `src/continuous.rs` | Separate native authorities for bounded probes and explicitly configured continuous execution, including exact exposure limits and durable original order ownership. | Implemented - continuous venue execution requires separate acceptance |
| `src/factories.rs` | `DataClientFactory` / `ExecutionClientFactory` wiring. | Implemented |
| `src/python/*` | PyO3 export of config, enums, factories and the public HTTP client, behind the `python` feature. | Implemented |
| `tests/http_contract.rs` | Offline contract tests for identity, precision, timestamps, metadata and fail-closed boundaries. | Implemented |
| `tests/continuous_execution.rs` | Native execution against loopback HTTP and private WebSocket services: repeated multi-instrument cycles, cancel/replace, fills, ownership conflicts, restart and shutdown. | Offline acceptance scenarios |
| `test_data/` | P0 protocol fixtures, manifest, conflict table and protocol table. | Frozen at P0 |

### Live dead man's switch acceptance is separate

Venue acceptance requires a separately recorded sandbox or authorized live run of renewal,
expiration and disconnect behavior, including resting orders and residual positions. The local
continuous tests do not provide that evidence. Plan §6.4 requires the real test before marking
the DMS accepted for a particular candidate.

What that does and does not cover. The local state machine *is* tested offline
(`tests/reconciliation.rs`): the frames this adapter would send, the deadline arithmetic, and what
it refuses to conclude when the switch fires. So this is a verified claim about the adapter's own
logic and an **unverified** claim about the venue: nothing here shows that the venue accepts the
renewal message this adapter builds or that it cancels resting orders when the switch fires. Treat
the switch as untested against Ondo Perps until the sandbox run happens.

## Continuous execution

`OndoExecutionClientConfig.continuous_execution` accepts an explicit
`OndoContinuousExecutionConfig`. It is mutually exclusive with `execution_envelope`, which retains
the bounded probe workflow. Production continuous execution also requires
`allow_production_orders = true`, the expected venue account identity, a run token, a durable
journal, one-second reconciliation and a configured dead man's switch. Read-only clients cannot
enable either write authority.

The continuous authority supports repeated order cycles across the configured perpetual instrument
IDs. It permits limit IOC, limit GTC and genuine market orders; post-only orders require limit GTC.
Market bodies omit both price and time in force. Their per-order and gross notional limits use
the current native ask as a conservative admission estimate, and their published minimum uses the
current ask for buys or bid for sells. These estimates are rechecked before submission, including
after durable reservation; market orders have no execution price bound. Quantities remain base
units, prices and limits use exact decimals, and all symbols share the venue's one-way net position.
Reduce-only orders must fit the confirmed residual and outstanding close reservations. Virtual
strategy legs do not create separate venue positions.

The explicit limits include per-order notional, aggregate position and outstanding entry exposure,
available USDC margin, live orders, application request rate and unresolved orders. The per-order
ceiling cannot exceed USD 100 and the aggregate ceiling cannot exceed USD 500. Admission requires
native metadata, fresh native quotes, verified identity, complete account reconciliation, a healthy
journal and genuine DMS acknowledgements. An unresolved submission retains its original ID and
suspends new risk; recovery queries that identity without resubmitting it. External open orders or
unexplained positions prevent new risk and clean shutdown.

`native_amend` is unsupported: `ModifyOrder` is rejected because Ondo has no atomic amend endpoint.
An explicit cancel/replace sequence cancels the owned original order, confirms its terminal status
and actual fills, then submits a replacement under a new client order ID. A cancel acknowledgement
alone does not prove that the original order filled nothing. Shutdown cancels owned orders
individually and retains unresolved outcomes or residual positions as incomplete cleanup.

The continuous acceptance scenarios use local HTTP and WebSocket services through the native
client and transport. They exercise repeated multi-symbol cycles, post-only cancel/replace, partial
and duplicate fills, cancel races, lost responses, journal recovery, external identity collisions
and stopping with residual risk. These offline scenarios establish adapter behavior against a
simulated venue; they do not establish mainnet acceptance or verify the venue's private protocol,
execution or DMS behavior.

## Instrument domain type

All markets this adapter trades are linear perpetuals, so an instrument is built as a Nautilus
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

The protocol capability stance (supported / explicitly unsupported / out of scope) was frozen at P0
in **[`test_data/README.md`](test_data/README.md#support-matrix-per-plan-1-frozen-at-p0)**.
The continuous execution section above describes the additional native execution authority;
the protocol fixture table remains the P0 reference.

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
so the first REST fixture was synthetic; a second unauthenticated capture the same day reached the
host, and its `GET /v1/markets` body was promoted into the tree as
`test_data/rest/markets_observed_20260914.json`, an `observed` excerpt.

## Feature flags

- `high-precision` (default) - enables NautilusTrader's 128-bit value types via
  `nautilus-model/high-precision`, matching
  [high-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation#precision-mode).
- `python` - the PyO3 export and extension-module wiring (`src/python/`): the venue constants, the
  environment, both client configurations and their factories, and the public HTTP client. The
  Python package is exported as `nautilus_trader.adapters.ondo`.

## Build and test

All commands run from the fork root (`E:\persarb\nautilus_trader`), which pins the toolchain with
`rust-toolchain.toml`.

```bash
# Full crate test suite (unit tests + integration tests + doc tests).
cargo test -p nautilus-ondo

# The offline contract tests only.
cargo test -p nautilus-ondo --test http_contract

# The native continuous execution acceptance scenarios against loopback services.
cargo test -p nautilus-ondo --test continuous_execution -- --test-threads=1

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

The PyO3 export is wired, so the python-feature checks run as well:

```bash
cargo check -p nautilus-ondo --features python
cargo check -p nautilus-pyo3
```

`cargo test` is the only gate that has been demonstrated green for this crate. The workspace denies
warnings, so `cargo clippy -p nautilus-ondo` and `cargo doc -p nautilus-ondo` are red at this HEAD
on pre-existing findings; that is not something this crate's phases have fixed.

## License

Licensed under the GNU Lesser General Public License Version 3.0 (the "License").
