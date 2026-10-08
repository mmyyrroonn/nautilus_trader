# Independent ingress and signed-envelope bridge review

Source-only review, 2026-10-08. Read the frozen Hyperliquid WebSocket client, handler, messages,
execution actor marker consumption, and execution-scope preparation/admission integration.
No source edit, Cargo command, native test execution, commit, private account call, or venue write.
No candidate PASS is issued; implementation changes following these findings still require testing.

## Correct local boundaries observed

- Relevant/unknown/private/error frames register a generation/epoch/sequence under a shared mutex
  before enqueueing. Final admission holds that same mutex through scope/intent validation and
  continuation consumption. Account and execution locks follow ingress; write control is last.
- Contiguous applied sequence is enforced. Old generation/epoch completions do not advance the new
  connection. Gaps fail the gate; enqueue/conversion failure and Close cannot certify a clean stream.
- Handler output queues place the applied marker after the outputs of a private frame. Parse errors
  are emitted before the marker. Marker application itself does not reset an invalidated account or
  recovery proof. This ordering is useful only if every relevant output is actually consumed.
- Prepared sends use a bounded JoinSet rather than awaiting writer readiness inside the raw parser.
  Cancellation guard construction precedes spawn, so abort before first poll still cancels control.
  Disconnect aborts and joins prepared tasks before disconnecting the client.
- Actual signed payload has expiry, nonce, post ID, connection generation/epoch, and three keccak
  digests: typed action, signed request, and full wire frame. The prequeue hook independently
  reconstructs these and persists the binding before a writer command exists. Final admission checks
  the persisted binding and actual typed fields without serde or filesystem work, then calls the
  continuation under the full lock chain. Local and signed deadlines are both checked.

These are source observations, not proof that the complete engine path passed native tests.

## Material remaining consumption gap reported to root and both agents

The handler raw bridge originally inserted `IoExecutionFrame` for only `user`/`orderUpdates`.
The execution-scope `observe_frame("user")` processed only `data.fills`, returning success for other
payloads. Funding, liquidation, and canceled-open-order user events could therefore create a nonempty
output list, be ignored as economic evidence, and still advance the applied marker without revoking
scope or recovery. This must explicitly remain unsupported/unknown; issue 102 accounting does not
need to be implemented to close the gate.

The `userEvents` alias and `userFills` parsed variants can generate nonempty legacy ExecutionReports,
which the io execution actor intentionally ignores. The handler's empty-output-only Error fallback
does not catch this. Other private channels with outputs the io actor does not consume must likewise
be explicitly rejected or routed through the strict raw account/ownership pipeline. A marker cannot
assert complete application merely because the handler generated some messages.

Requested cases: start from trusted/Ready, inject real private funding/liquidation/userEvents/userFills
frames, consume their markers, and verify the account/recovery remains invalidated or the strict
supported raw fill path consumed actual owned facts. Do not accept arbitrary parse failure as proof
of a specific supported-path semantic decision. Findings were sent directly to both implementation
agents and root; fixes were not yet independently reviewed in this note.

## Exact meaning of receive linearization

The callback captures received time and classifies JSON before acquiring the ingress lock. A write
can therefore overlap a callback still classifying its message and linearize before receipt
registration. The implemented guarantee is admission versus registered callback receipts under the
shared mutex. It is not admission versus every byte physically received, nor versus callback entry
time. If callback-entry registration is required, acquire/register before classification with a
bounded-frame rule. This distinction was sent to the WebSocket agent as a scope clarification,
not asserted as a demonstrated post-registration write bug.

The synthetic-token TCP handler test remains evidence that a pending prepared task does not block
real raw parsing/actor outputs. It does not by itself prove end-to-end ingress fence behavior.
Independent final verification must bind exact compiled identities and exercise the actual normal
factory peer, network readiness fixture, and fence unit boundaries with accurately scoped claims.
