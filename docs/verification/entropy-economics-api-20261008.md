# Bounded native Entropy economic reporting

This fork exposes actual user economic observations through the normal
`HyperliquidExecutionClientFactory`, without inventing a `FundingSettlement` with missing mandatory
settlement price or position identifiers. Protocol research and original public source captures are
in [entropy-economics-public](entropy-economics-public/README.md).

`io_economics_policy_json=None` is the default. Enabling it requires `account_dex="io"` and the
supported direct-account standard mode and canonical USDC semantics established by issue 100.
It works with the read-only io account configuration; the separate bounded
`io_execution_policy_json` is required for order actions. Only validated full native instrument IDs
in the explicit economic policy can acquire io attribution.

An example policy uses an existing absolute directory and a finite historical window:

```python
policy = {
    "schema_version": 1,
    "checkpoint_path": "E:/persarb/run-evidence/economic-report.json",
    "instruments": ["io:SNDK-USD-PERP.HYPERLIQUID"],
    "history_start_ms": 1791475200000,
    "history_max_window_ms": 60000,
    "history_timeout_ms": 3000,
    "history_max_pages": 8,
    "history_max_records": 2000,
    "max_observations": 2000,
    "max_receipts": 1000,
    "max_checkpoint_bytes": 16777216,
    "max_raw_frame_bytes": 65536,
    "max_history_body_bytes": 1048576,
}
```

The timestamp is illustrative. A run must select its actual finite requested interval. The policy
does not automatically slide forward or silently evict facts after its capacity is exhausted.
The snapshot exposes effective limits and diagnostics. Unknown fields, invalid instrument IDs,
relative/credentials paths and out-of-range limits fail native factory construction before I/O.
`history_max_pages` limits pages across both endpoints in each recovery. Coverage rows have a
separate finite lifetime cap of `history_max_pages * 3`, exposed as `coverage_record_limit`;
startup, explicit queries and recorded stream gaps consume it without eviction.

## Native factory methods

| Method                            | Meaning                                                                                     |
| --------------------------------- | ------------------------------------------------------------------------------------------- |
| `economics_scope_snapshot_json()` | Detached current facts, original source envelopes, report, coverage and provenance.         |
| `pending_economics_json()`        | Phase-A durable observations not yet durably consumed into this native report.              |
| `persist_economics()`             | Atomically persist report output, immutable receipts and consumed observation IDs together. |

All return `None` when the factory has no live economic client. They use the same ordinary factory
registered with `LiveNode.builder(...).add_exec_client(...)`. The consumer holds an exclusive
checkpoint lease; diagnostics do not keep a destroyed client's lease alive. A second active client
cannot share the same checkpoint, and an active factory binding cannot be silently replaced.

There is one authoritative checkpoint. Phase A preserves the exact item lexeme and references one
immutable original envelope in `raw_envelopes`; a large page is not copied once per event. Phase B
contains the actual native report output and its receipts in the same atomic replacement. Queueing,
raw history, notifications, or a caller-supplied boolean are not durable consumption. Restarts before
B offer pending observations; restarts after B restore identical receipts and recognized totals.
Fsync/replace failures preserve the previous checkpoint and close the running consumer until reopen
and validation. File flushing and process-restart behavior are verified; generic Windows directory
power-loss durability and adversarial rewriting of all owner/checksum evidence are not claimed.

## Recognized amounts and unresolved observations

`report.funding_usdc` sums supported REST `userFunding` cash amounts once per evidenced local
composite identity. `report.actual_fee_usdc` sums supported individual fill fees, including negative
rebates. Builder fee is already included in a fill's fee and is not added again. Public current or
historical rates and predictions are separate estimates and never enter either actual cash total.

String decimals are normalized without floating-point conversion or rounding. Unsupported precision
and numeric JSON amount literals retain the original bytes and are Unknown in this first revision.
Explicit zero is a supported actual amount; missing amount is never coerced to zero.

Receipt keys include account/network/category and actual identity fields. Funding keys exclude amount
and rate so changed financial payloads conflict. Distinct hashes at the same millisecond remain
distinct. Exact repeated strong facts do not add cash. Conflicts exclude unresolved observations;
an already consumed receipt stays immutable and the later conflict is visible. These are local
identity rules, not a claim that the venue guarantees hash uniqueness.

WS funding lacks a stable event ID. Every weak occurrence, snapshot replay and apparent REST overlap
is retained separately and excluded from uniquely recognized cash. Account-wide deposits, withdrawals,
transfers and liquidation facts keep dex/instrument and unsupported net amount/currency unset.
Transfer gross/net fee interpretation and liquidation account value are not fabricated income.
Foreign instruments, account/mode/currency mismatches and malformed/duplicate fields remain Unknown.

The snapshot's zero totals mean no recognized receipts, not a zero lifetime account result.
`unknown_observations`, `coverage` and source observations must accompany every derived report.

## Recovery and risk boundaries

The normal client subscribes to user funding and non-funding ledger and performs finite history
recovery at startup and explicit `QueryAccount`. Requests bind the actual user and do not add an
undocumented DEX selector. Pagination preserves the inclusive last timestamp; incomplete, unordered,
out-of-window, equal-time saturated, limited or failed recovery is diagnostic and retention Unknown.
Stream replacement/disconnect adds gap coverage; there is no invented gap-free private cursor.

New financial facts, weak identity, unknown normalization and conflicts close current scope/execution
proof before durable observation. A current, exactly equivalent strong history fact already present
in Phase A can avoid closing it again; this check does not read report receipts or totals. Current
account/metadata/owned-order recovery independently restores any permitted execution state. A window
containing unresolved ledger or weak facts remains conservative and may continue to block readiness.

`persist_economics()` never changes account funds, applies an additional commission, clears a cold
native cache barrier, or restores trading readiness. `native_applied="Unknown"`,
`balance_adjustment=false` and `native_cache_recovery=false` remain explicit. An accepted owned raw
fill is ownership evidence, not a durable native cache application acknowledgement. Only normal
native fill events update the real execution engine/cache/portfolio.

Fee attribution uses a separate context established by complete successful HTTP account role,
mode, collateral and universe verification. A validated position change can preserve that context
while funds `trusted/flat` remain closed. Its getter independently checks time, acknowledgements,
active metadata membership and the originating reader generation/epoch; it never turns the funds
snapshot's `trusted` flag true. Failed identity/mode/collateral verification, malformed source,
stale context and stream replacement revoke it. The actor rechecks the originating connection
after durable observation before using the frame for native projection.

HTTP economic limits apply to the returned response before parsing; the existing transport has its
own larger response/chunk bound. WS raw acceptance is checked before economic decoding, after the
transport has received its text. These policy bounds are not socket allocation limits.

Protocol captures are official public evidence. Local peer fixtures and installed builder tests are
synthetic, credential-free acceptance. Actual private settlements and real venue actions must be
recorded separately by application issue 43.
