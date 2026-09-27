# Ondo reconciliation tail handoff evidence

## Scope

Native issue 31. Offline loopback HTTP and WebSocket only; no real account or DMS operations.

## Implementation

The operational recovery API exposes a read transaction followed by explicit commit. Normal `reconcile_account` callers retain their convenience path. The transaction retains the existing account claim through conclusion, publication and checkpoint. Dropping it records a failed pass and releases ownership. Recovery generation is captured atomically at buffer drain; a session or recovery change between drain and commit refuses publication and records report loss. Reading before drain retains the existing semantics. This is a usable coordinator API, not a production test hook.

## Acceptance

- A pauses after drain and replay, before commit; B is refused while A retains the claim.
- A tail order, a new fill and a duplicate fill pass through actual production stream ingestion; applied and published state stay unchanged while A is paused.
- Same generation: the real periodic reconciler consumes the tail without another WebSocket message or explicit trigger. The fee is reported once; a later REST copy remains deduplicated.
- Different generation: A cannot publish the old reading. The next pass records LostReports, stays dirty and does not apply the superseded fill as current.
- Dropping a transaction publishes nothing, marks uncertainty and releases the claim.

## Validation

- Full Ondo suite on the initial branch base: 1090 tests passed, including the doctest.
- Both tail barrier cases and the abandoned transaction case passed.
- Nightly cargo format check and git diff check passed.
- Windows cargo uses CARGO_BUILD_WARNINGS=warn for MSVC informational linker output; Rust warning policy stays unchanged.
- Full and scoped pre-commit are blocked locally by missing Make, WSL worktree path incompatibility and missing pinned WSL tools. GitHub adapter checks are the Linux gate. No unrelated files or gate settings were changed.

## Limits

Stale venue REST pages still require the existing agreeing passes and production activity checks. No live venue acceptance is claimed.
