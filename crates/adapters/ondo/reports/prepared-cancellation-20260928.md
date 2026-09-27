# Ondo prepared cancellation barrier evidence

## Scope

Native issue 30, related to issue 7. Offline loopback only. Production envelope, dispatcher, request task group, account merge and HTTP transport are used with synthetic credentials.

## Deterministic scenarios

A single-worker native runtime runs in isolated child processes. A test-owned scheduler barrier parks that worker; the local fixture runtime stays available. No production test hooks were added.

- Before first task poll: cancel the prepared submission while the worker is parked. No Submitted event or HTTP write; terminal denial, no tracked prepared order, clean reconcile and disconnect.
- During exhausted shared-budget wait: observe Submitted, park the native worker and cancel before HTTP authorization. Zero HTTP writes; terminal rejection, no tracked prepared order, clean reconcile and disconnect.
- After HTTP send: observe the actual POST before cancelling with its answer withheld. Exactly one POST; uncertainty and owned order persist. Flat REST reads cannot manufacture production proof. Disconnect is dirty even though request tasks drained. Shutdown DELETE is permitted and cannot turn the unknown POST into known zero.

Worker barrier Drop releases the worker on assertion failures. The existing quota is restored for confirmation reads without discarding limiter debt.

## Validation

- The focused parent test passed all three isolated child scenarios.
- Full Ondo suite after issue 29 merge: 1097 tests passed, including the doctest.
- Nightly cargo formatting and diff whitespace checks passed.
- Windows Cargo uses CARGO_BUILD_WARNINGS=warn for informational MSVC linker output; Rust warning policy stays unchanged.
- Pre-commit tooling has local Make/WSL/pinned-tool limitations; GitHub native adapter checks remain the Linux gate. No unrelated gate configuration was changed.

## Limits

No real venue or live wheel acceptance is claimed. These barriers establish native task cancellation semantics.
