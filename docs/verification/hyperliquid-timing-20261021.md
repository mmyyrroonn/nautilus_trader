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

## Transport follow-up

The follow-up adds actual sink send/flush and complete-frame read observations.
An 80 ms mock-server hold is now measured between write completion and frame read;
a separate 60 ms raw-channel backlog is measured between read and handler dequeue.
Blocked-flush tests prove completion is absent until flush finishes, including the
failure path. The existing stalled-write/replay test checks timeout, buffering,
replay, and completion with one retained observer. Both Tungstenite and Sockudo
loopback tests verify read timestamps and an untimed reconnect sentinel. A panicking
write observer does not change the successful send result.

Final default-feature Windows results: 1,053 Hyperliquid tests passed (14 ignored)
and 627 network tests passed. Strict package Clippy passed for both packages.
An environment-enabled rejection loopback produced 32 JSONL records, passed the
seven-field allowlist, and exposed writer queue, write/flush, read-to-handler,
and rejection intervals through the local analyzer. It is an offline sample.

The follow-up full pre-commit run used an identical candidate copy on the local
Linux filesystem after interrupting a slow Windows-mount scan. Linux cargo fmt,
Clippy, cargo doc, private-key scanning, and gitleaks passed. Repository-wide
convention checks additionally exposed existing artifact/source issues; the new
diagnostic-only clock reads were annotated with the supported `dst-ok` exception
because they never drive simulation or transport state, then the DST hook was
rerun. Changed-file convention checks passed. The remaining full-gate failures
are outside this patch; generated formatting changes are excluded.

Five alternating runs compare the pre-transport implementation with the follow-up,
using the same Windows debug fixture (50 warmups, 500 requests). Medians of runs:

| Build/capture       | p50      | p95      | p99      | Mean       | Sequential requests/s |
| ------------------- | -------: | -------: | -------: | ---------: | --------------------: |
| Before, disabled    | 291.9 us | 369.4 us | 459.3 us | 298.935 us | 3,345.2               |
| Before, enabled     | 297.4 us | 383.2 us | 460.6 us | 306.264 us | 3,265.1               |
| Follow-up, disabled | 291.1 us | 366.9 us | 449.6 us | 297.432 us | 3,362.1               |
| Follow-up, enabled  | 298.8 us | 382.0 us | 473.9 us | 305.502 us | 3,273.3               |

No disabled-path latency regression is visible in this sample. Enabled versus
disabled adds 8.07 us to the observed mean (about 2.7%); enabled p99 is 24.3 us
higher. Relative to the previous enabled build, mean is similar and p99 is 13.3 us
higher. These small shared-host samples include scheduling noise and do not prove
production tail latency or CPU utilization. No performance optimization is claimed.
Raw runs are in `issue135-transport-performance.json` under the temporary evidence
directory. Disk-export contention and full-universe market-data throughput remain
unmeasured.

Rust layout inspection on x86-64 Windows reports a 40-byte neutral message and a
56-byte message-plus-optional-observer tuple; the optional read timestamp is 16
bytes. This is fixed metadata on existing queues. Enabled sends allocate one
shared observer capturing the existing trace. Disabled sends allocate no observer
and read no diagnostic clock. The 88-byte record and 4,096-record collector bound
are unchanged; the extra stages consume the session record budget sooner.

## Acceptance boundary

No wheel was deployed and no live order was sent. The historical four IOC no-match
samples cannot be divided into these new phases retrospectively. The user has
authorized observation of the existing five-venue strategy, scanning its configured
universe with $100 per leg, $500 gross limit, and at most two pairs. No new symbol
selection or manual test-order program is required. Deployment and live sampling
are blocked by confirmed server maintenance, not missing trading parameters.

The application validation baseline is the previous production commit
`bb443a0176dbde73e2ed881bdc8fc320521c3796`. The transport follow-up starts from
merged native `7d9228a49879794519166e932da66b5a8c793013`, whose original baseline
is `c8200a2915f35eb7eee579a5a9413cafbd93a59c`. It adds observations to the same
execution path; strategy logic, venue coverage, limits, and reconciliation remain
outside the patch.
