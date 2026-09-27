# Cleanup minimum admission verification

Base: `9f80aa7b2c62b99af4af5fefa51c15356e031a43`. Related: #29, #8.

Entry admission checks the published USD minimum against an approved close.
Buy closes use the approved quantity and price ceilings. Sell closes use one
venue lot and a tick-aligned price at or above the approved floor, subject to
both notional ceilings. Configuration validation remains independent of metadata.
Missing minimum keeps the existing admission policy. This establishes existence
of a legal close, not liquidity or a guarantee of cleaning every partial fill.

## Offline verification

- `cargo test -p nautilus-ondo --lib production::tests --offline --locked`: 23 passed.
- `cargo test -p nautilus-ondo --test private_runtime test_production_cleanup_minimum --offline --locked`: 3 passed.
- `cargo test -p nautilus-ondo --offline --locked`: 1096 passed including one doctest.
- `cargo +nightly fmt --check -p nautilus-ondo` and `git diff --check`: passed after formatting.
- Loopback tests run the actual production authority and account runtime with fake
  credentials. The counterexample validates configuration but emits zero HTTP
  writes. Feasible and unpublished-minimum cases each emit one entry request.

Commands set `CARGO_TARGET_DIR=E:/persarb/nautilus_trader/target` and
`CARGO_BUILD_WARNINGS=warn`: MSVC emits informational linker stdout that Cargo's
workspace warning policy otherwise treats as failure. Rust warnings remain denied
by the existing rustflags. No lint configuration was changed.

## Existing gate limitations

`make` is unavailable. Its Rust formatting command was run directly for Ondo;
the Python formatter check was attempted separately. Full `prek run --all-files`
failed at `make pytest-collect-fast`. Scoped prek also failed on Windows/WSL Git
worktree path translation, missing python3/cargo in its shell and missing pinned
cargo-machete 0.9.2. Standalone Clippy found existing Ondo violations (including
verify_identity and the existing shutdown diagnostic condition); none in the new
admission logic or tests. This is not a claim of clean workspace CI.

No real venue access, account operations, wheel replacement or live trading ran.
