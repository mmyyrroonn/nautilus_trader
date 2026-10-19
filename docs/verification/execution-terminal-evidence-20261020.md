# Lighter terminal-fill and Backpack shutdown evidence

This change addresses the application reliability plan dated 2026-10-19. It makes no
venue write and does not authorize deployment or restarting production.

## Venue evidence contract

The [official WebSocket reference](https://apidocs.lighter.xyz/docs/websocket-reference)
defines separate `account_orders` and `account_all_trades` channels. Its Order schema
includes `filled_base_amount`, status, venue/client identity and timestamps. Its Trade
schema supplies individual trade IDs, sizes and fees; zero `maker_fee`/`taker_fee` values
may be omitted. The [official SDK Order model](https://github.com/elliottech/lighter-python/blob/main/docs/Order.md)
requires the cumulative amount. The [official agent-kit schema](https://github.com/elliottech/lighter-agent-kit/blob/main/references/schemas-read.md)
identifies history results as filled, canceled or expired orders.

These sources describe an explicit terminal cumulative statement. They do not promise
atomic ordering across the order and trade channels, or provide a separate immutable
finality certificate. This change therefore never derives final volume from a closed
engine cache or from temporary equality of two zero-valued caches. The application
requires an original-order HTTP confirmation, followed by exact agreement with its
processed real-trade ledger, before releasing a Lighter terminal.

`LighterExecutionControl.terminal_fill_evidence_json(client_order_id)` returns no
proof until an exactly bound order supplies a terminal statement. It returns the
original client/instrument/venue identity, exact `final_filled_quantity`, source,
venue event time and conflict state. A handle attaches to one execution client. Its
100,000-order replay horizon survives reconnects but not process restart; a restart
requires querying the original venue order again. Missing/evicted proof fails closed.

HTTP confirmation can resolve stream-only disagreement with a nondecreasing quantity.
A conflicting HTTP cumulative statement or malformed HTTP terminal remains a conflict.
An older stream replay cannot replace HTTP evidence. A newer conflicting stream frame
requires HTTP reconfirmation. Neither retirement nor cache closure manufactures proof.

## Dispatch and reconciliation

Tracked terminal frames wait until their explicit cumulative amount equals the sum
of successfully dispatched, unique real trades. Partial trades, duplicates, reverse
channel ordering and terminal replays preserve this fence. Invalid quantity precision
cannot round down into a false equality. Retired identities retain the same fence.

Runtime original-order queries and acknowledged-create probes fetch the original
venue `order_index` trades through the full pagination chain. Each response is checked
for account, instrument, order, exact quantity and real fee semantics. Conflicting
copies of one trade ID, repeated cursors, excessive pages, malformed fees or incomplete
cumulative coverage fail before a cumulative runtime report is emitted. Successful
runtime reports bundle every real fill with their status, so a dedup marker created
before cache consumption cannot create a gap that the core engine fills by inference.
Per-order locking couples queueing with the actual trade ID and its quantity, price,
fee, side, liquidity and event time. A waiting WebSocket worker cannot count a queried
trade twice. A query snapshot that omits a newer routed trade remains pending; changed
economics for the same trade ID invalidate existing evidence. HTTP evidence is published
only after the complete validated batch is queued, never by the status lookup alone.

The Lighter commission hook rejects inferred venue-order economics in the live
reconciliation manager. That hook is not a guarantee for arbitrary engine configurations:
position-driven synthetic orders have their own path. The application separately sets
`reconciliation=False` and `generate_missing_orders=False`. The runtime bundled-report
boundary protects the core engine path, which does not call the commission hook.

The additional original-order HTTP confirmation and paginated trade reads consume
Lighter's configured REST quota and may increase terminal-confirmation latency. No
fixed sleep substitutes for evidence, and reduction does not require silently relaxing
these terminal or account safeguards.

## Backpack shutdown

The normal LiveNode shutdown disconnects clients before stopping their engines. The
production Backpack disconnect now performs a bounded, authenticated full recovery
before recording shutdown proof and canceling its private task group. Failed or timed
out recovery invalidates the previous private revision, leaving shutdown dirty. Account
age limits, real position/cache equality, unknown submissions, unconsumed economics and
sticky dirty state remain enforced. Direct synchronous `stop()` still requires already
valid proof; it cannot perform an asynchronous refresh.

Synthetic production-protocol loopback tests verify an expired old snapshot followed
by a fresh complete flat read becomes clean, and a read exceeding the shutdown budget
remains dirty. These tests do not establish the historical reason for the 2026-10-19
Backpack dirty exit and do not erase that evidence.

## Local validation and limits

- Lighter Rust library: 1,197 passed and three existing isolated cases ignored.
- Lighter execution integration: 110 passed and two existing isolated cases ignored.
- Lighter data/HTTP/WebSocket integration: 37 / 50 / 34 passed.
- Lighter Python binding integration: three passed, including the readonly control.
- Backpack Rust test targets passed, including both new production-protocol shutdown cases.
- Lighter library Clippy passed with warnings denied. The existing Windows MSVC
  import-library notice uses the repository builder's scoped `CARGO_BUILD_WARNINGS=allow`.
- `make format` ran through WSL. Its eight unrelated formatter changes were restored.
- Python stubs were generated with the official release stub generator and official
  postprocessing. The generated binary needed the Python 3.12 DLL directory on PATH;
  unrelated stub line-ending rewrites were restored.
- `make pre-commit` was attempted through WSL and stopped after its initial historical
  Git diff stalled. Windows scoped pre-commit could not launch its required `make`
  collection hook. Separate convention hooks also report existing full-tree debt and
  Windows shell/path failures; these are not claimed as passing.

The source-bound wheel and joint Windows/Linux application acceptance are recorded by
those builders separately. None of the offline checks establishes venue execution,
mainnet latency improvements, or operating-system network isolation on this host.
