# Backpack shutdown account proof

Issue: <https://github.com/mmyyrroonn/nautilus_trader/issues/137>.
Source baseline: `1c62da9b8c`, isolated worktree
`E:\persarb\worktrees\issue137-backpack-shutdown`.

## Failure boundary

The incident recorded a flat application ledger and an unresolved native position
flag. That flag did not establish a real residual position. The previous native
report discarded the verification error, so it cannot establish which exact
check failed during that historical stop.

Two code paths could leave the proof unavailable: a synchronous `stop()` finalized
the owner before `disconnect()` could refresh it, and a private event during the
single final REST traversal invalidated the traversal without a bounded retry.
Repeated owner stops also re-evaluated proof freshness, potentially replacing an
earlier clean observation with an aged one during disposal.

## Shutdown contract

Production shutdown first fences financial dispatch and session reentry. The
synchronous stop leaves private reads/events alive until asynchronous disconnect
has finished its final observation. It does not grant financial admission.

Disconnect uses the existing authenticated metadata/account/order/fill recovery,
with at most three attempts for private revision races and one shared configured
shutdown read deadline. Other read failures fail closed. The final proof is
checked under the private gate before closing the connection: the generation,
epoch, revision, pending frames, open orders, pending fills, native terminal
economics, supported account facts, native flatness, and snapshot start-time age
must agree. Confirmed nonzero venue positions remain nonzero even if the native
cache is empty; application flatness never supplies native proof.

The report separates `positions_unknown` and `positions_nonzero`, preserves the
legacy combined flag, and records account read timestamps, evaluation time and
private generation/epoch/revision for a verified account observation. The final
report is frozen and persisted in the existing checksummed execution checkpoint.
It describes the final local observation boundary, not future account state or
an exchange-provided atomic watermark. A new process must acquire its own proof.
Disposal without completed disconnect remains dirty.

The checkpoint's optional `shutdown_report` field accepts existing checkpoints.
Older binaries with strict checkpoint decoding will reject checkpoints containing
this new field; do not roll back a writer against one without an explicit migration.

## Operator interpretation

| Reason                                                                                 | Interpretation                                                                                 |
| -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `refresh_timeout`, `refresh_failed`                                                    | Final authenticated reads did not finish; inspect bounded recovery stage/category telemetry.   |
| `private_unavailable`, `private_revision_changed`                                      | Private ownership or consistency was lost; a fresh session observation is required.            |
| `account_stale`, `account_invalid`, `account_proof_missing`                            | Account age, supported facts, or authenticated coverage did not establish a current proof.     |
| `positions_nonzero`                                                                    | A fresh supported venue account observation contains a residual position.                      |
| `native_position_mismatch`                                                             | Venue flatness conflicts with the native position cache; reconcile actual fills and positions. |
| `open_orders`, `unconsumed_fills`, `terminal_evidence_unresolved`, `unresolved_orders` | Resolve the native order/fill evidence before treating the account as clean.                   |
| `prior_dirty_stop`, `disconnect_incomplete`                                            | A prior dirty checkpoint or incomplete lifecycle still blocks a clean result.                  |

The companion application change preserves these bounded diagnostics in
`collect_native_shutdown`, including unknown/nonzero distinction and timestamps.
Legacy reports retain their existing conservative interpretation. See
<https://github.com/mmyyrroonn/Nautilus-Perps/pull/101>.

## Validation and cost

Validation uses synthetic signed HTTP/WebSocket peers only. The regression matrix
covers no-trade shutdown, direct disconnect and stop-before-disconnect, prior
private invalidation, a late event and successful reread, repeated late events,
read timeout, expired proof, and confirmed nonzero venue positions. Owner tests
cover mutation fencing, session reentry refusal, immutable repeated reports,
checkpoint persistence and refusal to reuse old proof after restoration.

The stop-before-disconnect reproduction fails on baseline `1c62da9b8c`: the flat
case remains dirty and the timeout case performs no final read. With the fix,
`cargo nextest run -p nautilus-backpack --lib --tests --cargo-profile dev
--test-threads 1 --no-fail-fast` passed 584 tests with one skipped. The focused
Python production-owner disposal test passed with the extended JSON diagnostics,
and `cargo clippy -p nautilus-backpack --lib -- -D warnings` passed. Application
shutdown/execution/exit tests passed 133 cases; runner stop-proof tests passed nine.

`make py-stubs` completed successfully. The generated Backpack stub is unchanged
(SHA-256 `b68044b50bb7afeace3aad48ccc07822fa578b773737492d080a15b188c10e62`).
The existing `exact_production_config_readback_has_no_io_or_fact_injection` Python
test fails its URL readback assertion on both this branch and the baseline;
the shutdown/disposal test was run in its own process to isolate registry state.

Full-tree format/pre-commit checks were run in an isolated validation copy. The
inherited tree contains a Backpack `runtime.rs` comment-formatting problem and
unrelated historical JSON, whitespace, lint and documentation failures. Scoped
format/style/secret checks pass. Scoped pre-commit still reports inherited
Backpack test-only redundant-clone/constructor-order Clippy failures, missing
Aster/Ondo workspace-test inventory entries and unavailable `cargo-machete`.
Their unrelated rewrites were not copied into this change. An initial parallel
test run hit timing-dependent failures; the serial full run above passed.

Quote handling, signing, submission checks and normal recovery scheduling are
unchanged. The new session-reentry check runs at session establishment. The audit
report allocation occurs once at final stop; the owner retains a boxed report to
avoid embedding its full size in the trading state. No new hot-path I/O, queue or
per-event allocation is introduced. Shutdown may perform up to two additional
read traversals within the existing read deadline. WebSocket/task cleanup retains
its existing separate bounds. Production end-to-end latency, tail latency,
throughput, CPU and memory have not been benchmarked.

Validation logs are kept under `E:\persarb\_tmp\issue137-validation`.
No production process was started, no mainnet orders were sent, and no installed
native wheel or deployment was replaced.
