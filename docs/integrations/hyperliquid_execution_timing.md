# Hyperliquid execution timing

This diagnostic separates native submission, scheduler wait, limiter wait, signing,
post admission, handler wait, serialization, transport enqueue/write, response read/parsing,
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
by this value, not by file order: response read and dequeue observations are captured
before parsing but published after the post ID is known. Subtract only timestamps
from the same session. Never subtract these values from exchange time, wall-clock
event timestamps, another host, another session, or a restarted process.

On Linux, capture also creates `hyperliquid-clock-<session UUID>.json` once at
client construction. It contains PID, boot ID, and one `CLOCK_MONOTONIC` reading
bracketed by `elapsed_before_ns` and `elapsed_after_ns`. Thus a record's host
monotonic time lies between `elapsed_ns + monotonic_ns - elapsed_after_ns` and
`elapsed_ns + monotonic_ns - elapsed_before_ns`. A consumer must verify the same
host boot/process and clock implementation before joining Python/native clocks.
Carry the calibration window into derived intervals; do not invent an exact
offset. Calibration uses the safe rustix clock interface, adds no order-path
clock calls, and never overwrites an existing anchor. Other platforms retain
session-relative capture without this cross-language calibration.

## Boundaries

| Stages                                            | Actual observation                                                                                                          |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `native_received`                                 | Entry to native `submit_order`, before cache lookup and validation. This is not Python command creation.                    |
| `task_queued` / `task_started`                    | Immediately before spawning / first statement of the submission task.                                                       |
| `submitted_event_start` / `submitted_event_end`   | Bracket the submitted-event emitter call, including event construction and local delivery enqueue.                          |
| `post_started`                                    | Entry to the typed WebSocket action path.                                                                                   |
| `limiter_start` / `limiter_end`                   | Bracket acquisition from the existing weighted action limiter.                                                              |
| `sign_start` / `sign_end`                         | Bracket the canonical signer call, including nonce acquisition, MessagePack encoding, signature, and envelope construction. |
| `inflight_start` / `inflight_end`                 | Bracket post-router registration, including the semaphore and router lock.                                                  |
| `command_lock_start` / `command_enqueue`          | Before acquiring the handler sender lock / immediately before synchronous command-channel send.                             |
| `handler_received`                                | Handler consumes the post command.                                                                                          |
| `serialize_start` / `serialize_end`               | JSON encoding of the WebSocket post envelope.                                                                               |
| `transport_attempt`                               | Immediately before each `send_text` call; repeated records indicate retries.                                                |
| `writer_enqueued`                                 | `send_text` returned success. This confirms writer-channel enqueue only.                                                    |
| `writer_command_enqueue`                          | Immediately before the actual writer-channel send, after transport admission.                                               |
| `socket_write_started` / `socket_write_completed` | Bracket the transport sink send-and-flush operation. Completion is not peer acknowledgement or actual NIC transmission.     |
| `socket_read_completed`                           | Complete WebSocket message returned by the transport reader, after TCP/TLS/WebSocket decoding, before adapter dispatch.     |
| `read_wake_observed`                              | First forwarded wake after the preceding poll, if the final read was pending. Not necessarily packet readiness.             |
| `read_poll_started`                               | Entry to the final stream poll returning this complete frame. Excludes earlier partial reads.                               |
| `response_dequeued`                               | Handler dequeues the complete text frame, immediately before JSON envelope parsing.                                         |
| `envelope_parsed`                                 | Post envelope decoded, before router lookup/delivery.                                                                       |
| `caller_resumed`                                  | Waiting post future resumes with its correlated response.                                                                   |
| `response_parse_start` / `response_parse_end`     | Typed action response conversion and API-status classification.                                                             |
| `rejection_event_start` / `rejection_event_end`   | Bracket actual rejection emission after deduplication, including construction and local delivery enqueue.                   |
| `order_dispatch_start` / `order_dispatch_end`     | Bracket order report dispatch; this may emit, defer, or suppress an event.                                                  |
| `fill_dispatch_start` / `fill_dispatch_end`       | Bracket fill report dispatch with the same caveat.                                                                          |
| `submission_finished`                             | Submission task finishes post-response/reconciliation handling.                                                             |

`sign_error`, `transport_error`, `response_parse_error`, `post_error`, `timeout`,
`post_rejected`, and `post_success` describe observed local outcomes without private
error strings. A successful post does not prove a fill. A transport error or timeout
does not prove rejection and still does not cause the adapter to reject the order.
`reconnected` marks receipt of the existing reconnection sentinel for pending posts;
it carries no socket-read timestamp. `writer_buffered` and `replay_write_started`
observe the existing replay buffer. A replay emits another write-start and
write-completion/failure pair with the same trace/post ID. `socket_write_failed`
and `socket_write_timed_out` do not prove the peer received nothing. Late responses
after timeout are not reattached to another attempt.

The native receive/task/submitted boundaries cover single-order `submit_order`.
Other typed actions record the post stages; batches are not decomposed into
per-order native scheduling. HTTP-only execution is outside this capture.

`socket_write_started - writer_command_enqueue` measures writer queue wait on a
single uninterrupted attempt. `socket_write_completed - socket_write_started`
measures sink readiness/write/flush. `response_dequeued - socket_read_completed`
measures conversion, dispatch, and raw-channel queue wait before envelope parsing.
For replayed attempts, retain each occurrence and do not subtract arbitrary first
or last stages to invent a single write interval.

**Kernel/NIC transmission, server receipt, and exchange processing remain
unobservable.** `socket_read_completed - socket_write_completed` still includes
network/server work and local read scheduling/decoding; it is not pure network RTT
or exchange processing time. A response can race the write-completion observation
on another task: retain such ordering rather than clamping a negative interval.

`read_poll_started - read_wake_observed` exposes local wake-to-poll delay when
observed. The wake can come from readiness or the split-stream lock; this is not
proof of kernel arrival. `socket_read_completed - read_poll_started` covers the
final poll's local work, including read/TLS/WebSocket processing. Previous polls
and partial-frame waits remain outside that interval. Buffered frames, reconnect
sentinels and reads canceled by the existing timeout do not inherit a stale wake.
Missing wake observations remain missing. No timeout, wake forwarding, retry or
transport state decision uses these diagnostic clocks.

Write observers travel with existing writer/replay commands; they do not add a
queue or change transport decisions. Disabled capture adds no observer allocation
or diagnostic clock reads. Enabled capture allocates one shared observer per
timed send and reads the clock for complete incoming frames on that client.
Detailed reads reuse one shared waker observer per reader task (replaced only if
the parent task waker changes), with one atomic timestamp and no per-frame
observer allocation. The existing raw-message channel carries three read
observations rather than one; no additional queue is introduced. Kernel TCP
metadata, when collected by a separate bounded probe, still requires explicit
connection attribution because TLS may combine or split application messages.

## Validation and live interpretation

Offline tests cover response delay on the actual loopback WebSocket path, successful
posts, per-order rejection, post errors, malformed responses, timeout, reconnection,
real limiter exhaustion, inflight admission saturation, and handler backlog. Transport
tests cover pending flush, failure, timeout/replay, callback panic isolation, and
read timestamps on both WebSocket backends. Collector
tests cover bounded loss, the session cap, disabled capture, and monotonic timestamps.
Synthetic clock observations validate interval arithmetic separately; they are not
measurements of signing or exchange performance.

The four historical IOC no-match samples in issue 135 establish a roughly
858-1,019 ms submitted-to-rejection interval, but contain none of these intermediate
timestamps. They cannot identify which phase needs optimization. Collect a new
sample during an authorized execution run, preserve its native commit,
session file, order IDs, loss count, and execution parameters, and compare measured
phases before changing transport or signing behavior. This diagnostic does not
deploy a wheel or start a live order on its own.
