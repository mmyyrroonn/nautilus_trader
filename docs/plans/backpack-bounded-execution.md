# Backpack bounded offline execution

Status: offline implementation and local numeric-loopback evidence, 2026-10-04.
Production mutation transport remains unavailable. Synthetic peer assertions do not
establish authenticated venue identity or a private subscription acknowledgement.

## Deadline and durable admission

One monotonic deadline starts before identity reservation and includes identity,
checkpoint, shared quota, canonical preparation, signing and the sole transport
attempt. File synchronization remains synchronous: expiry stops a late send but
cannot interrupt a blocked filesystem operation or bound the time before return.

The execution owner retains its permanent clientId and immutable intent before
quota. The attempt becomes conservatively Unknown in durable storage before any
transport entry. Errors do not free identities or automatically retry creations.

Control changes publish a fence before waiting for the durable-state mutex. The
fence counts pending changes and maintains a checked, non-wrapping revision.
Consequently stop, invalidation, session replacement, account/market replacement
and authority changes cannot be hidden behind a slow checkpoint holding that
mutex. Exhaustion permanently closes admission. Overlapping updates cannot expose
an intermediate admissible state. A fresh revision is captured for each locked
durable admission interval; valid observation refreshes while queued remain
eligible after full reservation revalidation.

After each admission checkpoint, the owner rechecks cancellation, deadline and
fence. After the attempt checkpoint it also refreshes the Unix clock and rechecks
session, authority expiry, market/account freshness and exact reservation, followed
by a fence/deadline check immediately before signing and a final cancellation/
fence/deadline check after signing and body encoding. The shared HTTP primitive
performs its existing final deadline check after preparation and never retries a
mutation. Current evidence is specific to changes while synchronous checkpoint
work is blocked; it is not an atomic transaction with a remote socket first byte.

The private durable replacement interface retains the default real tempfile
write, file sync, atomic replacement, replaced-file sync and Unix directory sync.
It exposes no public storage callback or restricted Python bypass. Slow-storage
unit tests wrap this same real file implementation and hold its completion behind
a barrier for both intent and attempt checkpoints; no production test-only hook
or conditional behavior is added.

## Finite supported surface

The configured nonempty allowlist admits only metadata-confirmed visible, open
USDC perpetuals. The immutable native order spec is reparsed against retained
venue metadata and exact quantity/price bounds and increments.

- New risk: finite authorized Buy Limit orders with GTC, IOC or FOK; post-only
  requires Limit GTC. Notional, margin, fees and unsettled count retain exact limits.
- Reduction: independently authorized reduce-only orders, exact signed position
  and unreserved reducible quantity; Market requires IOC or FOK and no price.
- Cancellation: one independently authorized original venue binding, separate
  from new-risk market freshness; HTTP 202 remains pending.
- Modify, batch submit/modify, batch cancel and cancel-all have no supported mutation
  dispatch. Advanced trigger, borrowing/lending and unsupported TIF/order forms
  are explicitly refused. No wildcard universe or permissive economic default exists.

## Measured quota diagnostics

`BackpackQuota::diagnostics()` returns a sanitized schema-version 1 snapshot for
one caller-owned, in-process quota scope. Public/private read clients and the
restricted order owner retain clones of this same scope.

`observed_waits`, `admitted_waits`, `refused_waits`, `last_queue_wait_ns`,
`total_queue_wait_ns` and `last_admitted` record actual monotonic elapsed waits.
The measurement begins at the shared request primitive and ends when preparation
starts. If the queued request is cancelled, dropped or expires before preparation,
a drop guard records its actual elapsed wait as refused. Local validation before
entering this primitive contributes no fabricated queue observation. Counters and
nanoseconds saturate instead of wrapping. Concurrent completions determine the
latest observation; this snapshot is not an attempt journal.

Preparation, disk synchronization, signing, transport response time and retry
backoff are excluded. Quota admission means permission to prepare, not proof of
transport entry, venue acceptance, fills or durable economic application. The
existing request outcomes still distinguish NotSent, VenueRejected and Unknown;
a later refused/rejected read cannot erase an earlier uncertain attempt. Sharing
this limiter does not account for other processes or external venue callers.

## Evidence

- `test_checkpoint_block_cannot_hide_deadline_or_admission_revocation`: real
  synchronized files and controlled barriers at both durable admission phases;
  no-change positive POST control, plus deadline/cancellation/session/generation/
  stop/authority/readiness/freshness/expiry zero-byte refusals and permanent identity
  recovery after owner release.
- `test_overlapping_control_changes_never_admit_between_updates` and
  `test_revision_exhaustion_cannot_reauthorize_an_old_snapshot`: overlapping and
  exhausted fences remain closed without revision reuse.
- `test_shared_quota_longer_than_receive_window_signs_fresh_after_wait`: actual
  measured shared wait longer than receive window, accepted refreshed observations,
  fresh timestamp and independently verified canonical signature.
- `test_public_and_private_reads_share_quota_and_queued_cancellation_is_not_sent`:
  shared public/private scope and measured cancelled wait without another request.
- `test_quota_diagnostics_measure_refused_wait_without_dispatch`: actual bounded
  deadline wait, shared counters and zero additional server request.
- `test_quota_diagnostics_exclude_slow_signing_and_transport_unknown`: slow clock
  preparation and transport timeout excluded from measured quota; sent request
  retains Unknown.

The complete existing finite execution and HTTP regressions remain in place.
Engine, native economic delivery, wheel installation and installed application
validation are independent follow-on acceptance evidence.

## Local validation record

With `CARGO_TARGET_DIR=E:/persarb/.backpack-target/native`,
`PYO3_PYTHON=E:/persarb/nautilus_trader/.venv/Scripts/python.exe` and
`CARGO_BUILD_WARNINGS=allow`:

- `cargo test -p nautilus-backpack --lib execution:: --offline -- --test-threads=1`
  passed 22 execution unit cases.
- `cargo test -p nautilus-backpack --test execution --test http_client --offline -- --test-threads=1`
  passed 51 execution and 19 HTTP cases, including the unchanged response-loss test.
- An earlier parallel execution run passed 50 cases and failed the existing
  `test_accept_then_response_loss_keeps_one_post_and_unknown_capacity`: its 100ms
  operation budget produced NotSent while the test expected Unknown. This is
  consistent with expiry before transport under contention; filesystem duration
  was not captured for that failed attempt. The test was retained unchanged and
  the full serial suite passed. A short deadline does not itself prove acceptance.
- `cargo clippy -p nautilus-backpack --lib --tests --offline -- -D warnings`
  initially stopped in the concurrent live-node recovery implementation on
  `needless_pass_by_value`; final unified lint and validation belong to the main
  review and include the final post-signing refusal check.
