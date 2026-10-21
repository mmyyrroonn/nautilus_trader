# Hyperliquid timing validation

The diagnostic implementation for native issue 135 was developed in
`E:\persarb\worktrees\issue135-hyperliquid-timing`, based on native
`c8200a2915f35eb7eee579a5a9413cafbd93a59c`. The original application and native
working directories were not used for implementation.

## Offline correctness

Rust 1.98.0 on Windows, locked offline dependencies, default high-precision mode:

- `cargo test -p nautilus-hyperliquid --lib --tests --locked --offline -- --test-threads=1`:
  1,051 passed, 14 ignored. The ignored entries are 11 live tests, two manual
  timing profiles, and one existing manual test.
- After adding coverage of deferred raw rejection dispatch, the library and
  execution-client suites passed again: 759 library tests and 108 execution-client
  tests, with three execution-client tests ignored. Together with the unchanged
  suites above, 1,052 distinct tests passed.
- The new loopback tests cover success, IOC-style per-order rejection, request
  error, timeout, reconnect, and malformed response. An 80 ms mock-server hold
  appears after writer enqueue and before response dequeue. It is not assigned
  to signing or parsing. Native submitted/rejection emitter boundaries remain
  correlated by session and client order ID.
- Real limiter exhaustion, saturated inflight admission, and 60 ms handler
  backlog are tested independently. Saturation and errors retain missing stages
  rather than inventing successful enqueue/write/response observations.
- The collector is tested at queue overflow, receiver closure, the 100,000-record
  cap, disabled capture, and separate monotonic sessions.
- A separate process enabled the JSONL exporter on an existing rejection
  loopback test: 28 records were parsed and checked for the exact seven-field
  allowlist and native/sign/enqueue/response/rejection/finish observations.
- Strict package Clippy passed with `--lib --tests --no-deps -- -D warnings`.
  Including dependency lints fails on the existing Windows-only documentation
  at `crates/persistence/src/parquet.rs:656` (bare file URL and missing backticks).

The repository-wide `make format` and `make pre-commit` commands were also run.
They are not clean baseline gates: formatting reports an existing Backpack
comment-preservation problem; pre-commit reports historical artifact formatting,
invalid archived JSON, existing Python/Markdown/YAML lint findings, and missing
Aster/Ondo workspace-test inventories. Some tools are unavailable in WSL
(`cargo-machete`, `jq`, and `ty`). The Linux Hyperliquid Clippy compilation finished
successfully, although its hook was marked failed because files were modified
during the hook run. The private-key and gitleaks scans passed. All unrelated
automatic formatting edits were reverted.

Several repository convention hooks silently skip checks without `rg`; their
apparent pass in the full run is not counted as validation. Formatting, docs,
Tokio, testing, and logging convention scripts passed when rerun with `rg` on
copies of the changed files. Package rustfmt and strict package Clippy also passed
on the final sources.

The signing and parser intervals are measured around real production operations.
No test-only sleeps, payload branches, or injection knobs were added to production
code. Synthetic clock tests check interval arithmetic; they do not establish a
signing-delay or exchange-latency result.

## Performance

The retained ignored `test_execution_timing_profile` runs real signing, post-router
admission, WebSocket serialization, a local mock exchange, response decoding, and
correlation. It uses 50 warmups and 500 measured requests per run. Three alternating
runs were performed for baseline, candidate disabled, and candidate enabled.
The baseline worktree has unchanged production sources at `c8200a2915` and only
the same measurement test appended. Values below are medians across the three runs.

| Build/capture       | p50      | p95      | p99      | Mean       | Sequential requests/s |
| ------------------- | -------: | -------: | -------: | ---------: | --------------------: |
| Baseline            | 323.7 us | 395.2 us | 470.4 us | 329.984 us | 3,030.4               |
| Candidate, disabled | 310.2 us | 365.2 us | 464.0 us | 316.366 us | 3,160.9               |
| Candidate, enabled  | 311.7 us | 371.7 us | 461.1 us | 318.714 us | 3,137.6               |

This is a Windows debug/test build on a shared host, not a production throughput
or tail-latency guarantee. The disabled candidate shows no regression in this
sample. Enabled collection adds about 2.35 us to the observed mean relative to
disabled; the percentile differences include host noise. The collector is drained
outside the measured operation, so these figures do not measure file-system
contention. Full production load and release-build CPU utilization were not measured.

Disabled capture allocates no diagnostic session/queue/thread and performs no
diagnostic clock reads. Enabled records are 88 bytes on this target; 4,096 slots
hold about 352 KiB of record data plus channel bookkeeping. Existing pending-post
entries carry a small fixed trace handle, under the existing 100-post bound.
Capture adds clock reads, bounded queue operations, and a separate writer thread;
it adds no response cloning, unbounded diagnostic map, or file I/O on the hot path.

Raw logs, the JSON performance result, benchmark executables, and the synthetic
JSONL capture are retained under `E:\persarb\tmp\issue135-*`.

## Acceptance boundary

No wheel was deployed and no live order was sent. The historical four IOC no-match
samples cannot be divided into these new phases retrospectively. The live-sample
acceptance item in issue 135 remains pending a separately authorized run with
explicit execution parameters. The diagnostic implementation alone does not
justify changing transport, signing, trading limits, or recovery behavior.
