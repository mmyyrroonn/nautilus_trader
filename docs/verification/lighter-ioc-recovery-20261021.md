# Lighter IOC terminal recovery

Issue: <https://github.com/mmyyrroonn/nautilus_trader/issues/136>.

## Incident timeline

The 2026-10-20 TIA incident ran native `c8200a2915` and application `bb443a0`.
The source capture is the application's
`reports/opportunity-trading/incidents/entry-investigation-20261020/session-analysis-evidence.json`,
SHA-256 `41efd6ddd9ff21d72bd24be373987e9eaeee0780bc2bcfa72d1dc65bc2aefd39`.
The issue records the original archive SHA-256 separately.

| Observation                          | Duration (ms) | Meaning                                                  |
| ------------------------------------ | ------------- | -------------------------------------------------------- |
| First exit intent to canceled        | 11163.390093  | Zero fills; intent at approximately 12:15:43.585 UTC.    |
| First exit intent to receipt timeout | 10653.756626  | Still unknown; timeout was not cancellation evidence.    |
| Second exit intent to canceled       | 1115.945941   | Zero fills; intent at approximately 12:15:55.839 UTC.    |
| Third exit intent to terminal        | 1602.799061   | Filled 153.7 TIA; intent approximately 12:15:57.949 UTC. |
| First imbalance to flat inventory    | 17170.825041  | The independent 10-second exposure limit was breached.   |
| Flat inventory to completed position | 1540.583665   | Terminal/account completion remained a separate gate.    |

The first `pending_order` sample group spans 10000.766988 ms. The three
`predates_activity` to `account_accepted` intervals are 1394.735763, 1090.952126,
and 994.926253 ms. `order_retry_throttled` samples show approximately 1000 ms
remaining after each dispatch and 0.055497 ms immediately before one retry gate
expired. These gates overlap: their durations must not be summed as independent
network delays. Event arrival order has sub-millisecond inversions; the above
intervals use event timestamps and explicit stage endpoints.

The capture does not establish when the first exit became visible in inactive
history or prove loss of a particular WebSocket event. It cannot quantify the
production improvement from this change.

## Recovery changes

- A locally retained client index plus submission nonce can identify a recent
  inactive order before the first venue ID arrives. The lookup checks the account,
  market, nonce, client index, and agreement between numeric and string venue IDs.
  A derived client index without a retained nonce remains active-order-only.
- Explicit venue IDs remain authoritative. Invalid IDs fail rather than falling
  back to another identity. Missing rows, old nonces, ambiguous rows, repeated
  cursors, and exhausted history budgets never manufacture terminal evidence.
- Recent-create recovery reads at most three inactive pages per attempt. It retains
  the existing two-second delay and three-attempt budget. IOC and ordinary market
  order probes continue after acceptance until terminal reconciliation, rather
  than treating acceptance as completion.
- Real trade IDs, quantities, and fees must cover the reported cumulative quantity
  before publication. Delayed details leave a retry opportunity. Publication binds
  the verified venue ID and gives fills the original local client ID before
  retiring the identity into the bounded replay cache.
- A query retains its original submission generation across HTTP waits. A replaced
  local ID cannot inherit the old query's terminal evidence. Publication is fenced
  against concurrent create registration without holding the registry across I/O.
- Debug observations report `initial_delay`, `order_lookup`, `execution_details`,
  `transaction_lookup`, and `retry_delay` durations per original order/attempt.
  `probe_elapsed_ms` starts after the initial delay; it is not socket latency.

The adapter never submits a replacement order. The application still owns retry
quantity, fresh-account fences, independent unhedged deadlines, reduce-only
protection, fee confirmation, durable recovery, and risk-budget release.

## Validation

The new exact-identity inactive lookup test fails against unmodified native
`af301ba26f`: a matching canceled order returns no report. The fixed loopback
replays cover zero-fill IOC, delayed inactive history, acceptance before terminal,
partial fills, delayed trade details, repeated stream fills, stale identities,
explicit venue-ID precedence, and bounded pagination.

`cargo nextest run -p nautilus-lighter --lib --tests --cargo-profile dev` passed
1452 tests with 5 skipped. `cargo clippy -p nautilus-lighter --lib --tests -- -D warnings`
passed. The test suite includes both acceptance-before-probe and an initial
nonterminal HTTP response, followed by terminal recovery on the next attempt.

Application guards were run from a separate clean checkout at `fe971de`:
192 tests passed across `test_opportunity_live`, `test_opportunity_exit_reliability`,
`test_opportunity_unhedged_risk`, `test_opportunity_native_execution`,
`test_opportunity_account_batching`, `test_opportunity_review`, and
`test_opportunity_account_reconciliation`. These check unknown orders retaining
exposure, retries using owned residual quantities, stale account snapshots,
late-fill ownership, independent deadlines, fees, duplicate handling, and restart
state. They use the existing Python environment; this is not a rebuilt-wheel
end-to-end acceptance or a mainnet run.

## Cost and limits

Changes are confined to asynchronous recovery. Quote processing, signing, order
dispatch, and the live fill handler are unchanged. A missing recent create now
costs one active request plus at most three inactive requests per attempt; memory
is bounded by response pages and at most three cursor keys. Zero-fill terminals still
require no trade request. No blocking I/O or new task/queue is added to a hot path.
Evidence publication takes the existing create registry lock for synchronous
validation and routing; fill-map allocation and HTTP waits stay outside it. Its
contention with concurrent submissions has not been benchmarked.

The first fixed loopback replay completed in 4006-4009 ms, including the unchanged
two-second initial delay and one retry. Both delayed terminal visibility and
delayed fill details are tested. This is local recovery latency, not venue latency
or a production p99/throughput/CPU benchmark; those were not measured.

`make format` and `make pre-commit` were run in an isolated Linux validation copy.
The full-tree checks are not clean on the inherited fork: nightly rustfmt reports
an existing Backpack `runtime.rs` comment-formatting error, and pre-commit reports
unrelated historical JSON/whitespace/table issues and existing lint/manifest
failures. Their unrelated rewrites were discarded. The focused checks and full
Lighter suite are recorded with the pull request. The scoped hook run also reports
the inherited Aster/Ondo workspace-test inventory gap and missing local
`cargo-machete` executable; neither is a Lighter test failure.

Local validation logs and extracted incident intervals are under
`E:\persarb\_tmp\issue136-validation`. No live deployment or native wheel
replacement is part of this change.
