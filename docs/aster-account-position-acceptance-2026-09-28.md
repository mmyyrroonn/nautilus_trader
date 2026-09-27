# Aster account and position acceptance

This closes the remaining acceptance work for fork issues #2 and #6. The balance stream
mapping from PR #25 is retained. Installed-wheel testing exposed an additional startup
problem: Portfolio recomputed venue-reported free funds from local position margin even
when `calculate_account_state` was false. Initialization now retains reported currency
balances while keeping local order and position margin estimates.

## Account contract

| Venue value                          | Nautilus meaning                                                                          |
| ------------------------------------ | ----------------------------------------------------------------------------------------- |
| REST `balance` / WS `wb`             | `AccountBalance.total`: wallet balance                                                    |
| REST `availableBalance`              | Verified `AccountBalance.free`, bounded by wallet balance                                 |
| WS `cw`                              | Cross wallet balance; never a source of available funds                                   |
| Wallet minus available               | `AccountBalance.locked`: aggregate unavailable wallet funds                               |
| Unrealized PnL / equity              | Not substituted for wallet balance or available funds                                     |
| Local order/position margin estimate | `MarginBalance`; does not replace reported currency balances when calculation is disabled |

A WS update carries a conservative previously verified free amount, never increases it,
and owes a REST refresh. Unknown availability is withheld. An explicit zero clears that
currency; omission does not. Stream epochs and refresh generations prevent an overtaken
REST response from increasing the bound or clearing newer verification debt. Failed
verification preserves the conservative amount and leaves readiness degraded. Position
and order initialization continues to calculate balances for accounts that enable it.

## Position contract

The unpaginated `positionRisk` request covers the loaded instrument set captured before
the request and still loaded when it completes. A symbol request covers only that symbol.
Parsing is shared by startup/mass status, periodic reports, and reconnect refresh.

Only an entirely valid successful snapshot can prove an omitted covered symbol flat.
Bad quantities, bad open-position entry prices, duplicate symbols, hedge-mode rows,
timeouts, and HTTP errors do not justify omitted-symbol Flat reports. Recovery may publish
explicit valid rows from a partially malformed response, but remains degraded. A newly
loaded or unloaded instrument is excluded from inference.

Real fill and order recovery precedes inferred Flat reports. Pending trade/order evidence
or incomplete fill coverage suppresses closing inference, preventing Flat from replacing
missing real trade IDs and commissions. An incomplete mass status also withholds explicit
Flat reports until its fill bundle is complete.

The venue omission fixture preserves the empty response recorded in the historical
[2026-09-05 testnet acceptance, D2](https://github.com/mmyyrroonn/Nautilus-Perps/blob/aff9530799e70423207bb9e4f5b14e8dd274fba1/reports/aster-review-2026-09-05-response.md).
It is a historical observation, not a fresh capture or a complete venue trade archive.
The [official position API](https://asterdex.github.io/aster-api-website/futures-v3/account%26trades/#position-information-v3-user_data)
describes the request shape; it does not explicitly promise omitted-zero behavior.

## Validation

- Aster: 421 native tests passed; Portfolio: 227 native tests passed.
- Affected-crate formatting, doctests, and Clippy with `-D warnings` passed.
- Python Ruff, Ruff format, docformatter, and trailing-comma hooks passed.
- Nautilus-Perps' existing `test_exec_probe.py`, `test_live_limits.py`, and
  `test_maker_live.py` passed against the isolated wheel: 385 tests and 97 subtests.
- The installed CPython 3.12 wheel passed a LiveNode test against scripted loopback REST/WS.
  The test calls Nautilus-Perps' existing `LighterMaker._report_accounts` with the native
  portfolio/cache and sends its test order through the native risk engine.
- Initial wallet/free/locked were 100/20/80 despite an open position and startup replay.
  A subsequent WS `wb=100,cw=100` preserved free=20. Application display showed that same
  amount. The order requiring margin=25 was denied with free=20 before any HTTP order POST.
- An external close during disconnection followed by `positionRisk=[]` produced a flat
  cache and free=30 after reconnect. Both true trade IDs and each 0.02 USDT commission were
  retained; another reconnect did not duplicate their economic effects.
- Tests include HTTP failure, timeout, bad rows, scope exclusion, and outstanding fill
  evidence. Existing balance tests cover explicit zero, unknown assets, negative wallet,
  missing availability, stale events, and overlapping snapshot generations.

The [machine-readable evidence](aster-account-position-acceptance-2026-09-28.json) records
source hashes, wheel/native binary hashes, and observed application output. The installed
native object was checked against the object embedded in the wheel. The final native
checks observed no checkout identity change while running.

### Reproduction

From the fork root, run the affected-crate checks:

```text
python scripts/adapter-evidence/native_checks.py --crates nautilus-aster nautilus-portfolio --output target/native-acceptance.json
```

Build from `python/` with Maturin so its configured extension features and Python files
are included, install the wheel plus `websockets` in an isolated environment, then run:

```text
python tests/integration_tests/adapters/aster/account_position_acceptance.py --app-root E:/persarb/Nautilus-Perps --output target/application-acceptance.json
```

This run used the `nextest` build profile and `clang`/`clang++`. On Windows,
`CARGO_BUILD_WARNINGS=warn` allowed MSVC import-library linker messages; Rust/Clippy
warning denial remained enabled. The active application's wheel was not replaced.
No `.env` was read and no real venue orders were sent.

### Broader check limitations

Full pre-commit was attempted but did not pass: this Windows environment lacks `make`
and pinned `cargo-machete` 0.9.2, and global convention hooks report existing Aster/Ondo
and workspace documentation violations outside the changed logic. Go hook installation
also initially failed on a module download. A scoped hook run was attempted; affected
Rust Clippy failures were corrected and revalidated, and Python formatting hooks passed
after their automatic edits. This report does not claim a clean full-workspace gate.
