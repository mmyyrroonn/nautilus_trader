# Hyperliquid execution timing

This diagnostic separates native submission, scheduler wait, limiter wait, signing,
post admission, handler wait, serialization, transport enqueue, response parsing,
and event emission. It does not change trading, retry, timeout, or reconciliation
behavior. Enabling capture is not authorization to resume trading.

## Capture

Capture is disabled by default. Before constructing clients, set
`NAUTILUS_HYPERLIQUID_TIMING_DIR` to an existing local directory, for example
`E:\persarb\tmp\hyperliquid-timing`. Each WebSocket client writes a new
`hyperliquid-timing-<session UUID>.jsonl` file; clones share the same session.
Existing files are never overwritten. An unavailable directory or writer disables
capture with a warning and does not prevent execution.

The execution path copies fixed-size records into a nonblocking queue with a
capacity of 4,096 records. Serialization and file writes run on a separate thread.
Each session attempts at most 100,000 records, including dropped records. Full or
closed queues discard diagnostics. `dropped` is the cumulative loss count observed
at capture time; missing stages must remain missing, even if this count is zero
(shutdown or the record limit may truncate the tail). No rotating files or
unbounded diagnostic maps are created. A process crash can lose queued records.

Rust callers can instead attach `ExecutionTiming::bounded(capacity)` through
`set_execution_timing` before connecting and drain its receiver outside execution
tasks. The capacity is clamped to 1..=4,096. `ExecutionTiming::dropped()` also reports
loss after the last delivered record. The environment option works with a native
Python runtime without adding Python calls to the order path.

Records contain session/trace/post IDs, an optional client order ID, a stage, local
elapsed nanoseconds, and a loss count. They contain no credentials, signatures,
nonces, order prices/sizes, raw private requests/responses, or rejection text.

## Correlation and clocks

Group by `(session_id, trace_id)` for one attempt. Post IDs are local to a client;
the non-null `post_id` records link the attempt to its WebSocket request. Native
single-order submission records also carry `client_order_id`. Asynchronous order
and fill dispatch, and rejection emission, use `trace_id=0`: join these by
`(session_id, client_order_id)`, keeping repeated dispatch observations distinct.
Do not interpret a duplicate-suppressed dispatch as a newly generated event.

`elapsed_ns` uses `std::time::Instant` relative to the session origin. Sort records
by this value, not by file order: the response dequeue observation is captured
before parsing but published after its post ID is known. Subtract only timestamps
from the same session. Never subtract these values from exchange time, wall-clock
event timestamps, another host, another session, or a restarted process.

## Boundaries

| Stages                                          | Actual observation                                                                                                                             |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `native_received`                               | Entry to native `submit_order`, before cache lookup and validation. This is not Python command creation.                                       |
| `task_queued` / `task_started`                  | Immediately before spawning / first statement of the submission task.                                                                          |
| `submitted_event_start` / `submitted_event_end` | Bracket the submitted-event emitter call, including event construction and local delivery enqueue.                                             |
| `post_started`                                  | Entry to the typed WebSocket action path.                                                                                                      |
| `limiter_start` / `limiter_end`                 | Bracket acquisition from the existing weighted action limiter.                                                                                 |
| `sign_start` / `sign_end`                       | Bracket the canonical signer call, including nonce acquisition, MessagePack encoding, signature, and envelope construction.                    |
| `inflight_start` / `inflight_end`               | Bracket post-router registration, including the semaphore and router lock.                                                                     |
| `command_lock_start` / `command_enqueue`        | Before acquiring the handler sender lock / immediately before synchronous command-channel send.                                                |
| `handler_received`                              | Handler consumes the post command.                                                                                                             |
| `serialize_start` / `serialize_end`             | JSON encoding of the WebSocket post envelope.                                                                                                  |
| `transport_attempt`                             | Immediately before each `send_text` call; repeated records indicate retries.                                                                   |
| `writer_enqueued`                               | `send_text` returned success. This confirms writer-channel enqueue only.                                                                       |
| `response_dequeued`                             | Handler dequeues the complete text frame, immediately before JSON envelope parsing. Network read and raw-channel queue time are not separated. |
| `envelope_parsed`                               | Post envelope decoded, before router lookup/delivery.                                                                                          |
| `caller_resumed`                                | Waiting post future resumes with its correlated response.                                                                                      |
| `response_parse_start` / `response_parse_end`   | Typed action response conversion and API-status classification.                                                                                |
| `rejection_event_start` / `rejection_event_end` | Bracket actual rejection emission after deduplication, including construction and local delivery enqueue.                                      |
| `order_dispatch_start` / `order_dispatch_end`   | Bracket order report dispatch; this may emit, defer, or suppress an event.                                                                     |
| `fill_dispatch_start` / `fill_dispatch_end`     | Bracket fill report dispatch with the same caveat.                                                                                             |
| `submission_finished`                           | Submission task finishes post-response/reconciliation handling.                                                                                |

`sign_error`, `transport_error`, `response_parse_error`, `post_error`, `timeout`,
`post_rejected`, and `post_success` describe observed local outcomes without private
error strings. A successful post does not prove a fill. A transport error or timeout
does not prove rejection and still does not cause the adapter to reject the order.
`reconnected` marks receipt of the existing reconnection sentinel for pending posts;
it does not prove whether an enqueued request was written or replayed. The same
trace/post ID survives this observation. Late responses after timeout are not
reattached to another attempt.

The native receive/task/submitted boundaries cover single-order `submit_order`.
Other typed actions record the post stages; batches are not decomposed into
per-order native scheduling. HTTP-only execution is outside this capture.

**Actual socket write, kernel/NIC transmission, server receipt, exchange processing,
and socket-read timestamps are unobservable here and remain absent.** In particular,
`response_dequeued - writer_enqueued` includes writer backlog, reconnect/replay,
network/server work, and receive-handler backlog. It is not network RTT.

## Validation and live interpretation

Offline tests cover response delay on the actual loopback WebSocket path, successful
posts, per-order rejection, post errors, malformed responses, timeout, reconnection,
real limiter exhaustion, inflight admission saturation, and handler backlog. Collector
tests cover bounded loss, the session cap, disabled capture, and monotonic timestamps.
Synthetic clock observations validate interval arithmetic separately; they are not
measurements of signing or exchange performance.

The four historical IOC no-match samples in issue 135 establish a roughly
858-1,019 ms submitted-to-rejection interval, but contain none of these intermediate
timestamps. They cannot identify which phase needs optimization. Collect a new
sample only during a separately authorized execution run, preserve its native commit,
session file, order IDs, loss count, and execution parameters, and compare measured
phases before changing transport or signing behavior. This diagnostic does not
deploy a wheel or start a live order on its own.
