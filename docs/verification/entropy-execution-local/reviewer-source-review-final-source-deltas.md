# Independent final source-delta check

Read-only source check on 2026-10-08. No Cargo, native test execution, tracked edit, commit,
private account query, or venue write. The candidate remains unverified until stable binaries.

## QueryAccount and startup deadlines

The explicit io QueryAccount outer timeout now includes account refresh, metadata verification,
and owned recovery. Failure or timeout invalidates both account scope and execution recovery with
a detailed reason. This is a complete timeout for that query operation's verification sequence.

At the sampled startup source, the timeout includes metadata verification and owned recovery,
but the preceding account refresh, account registration wait, and private-ready wait are outside it.
Those steps have their existing lifecycle limits, but the implementation must not describe this
particular policy timeout as covering the entire startup/account verification sequence. Root was
sent this exact remaining boundary, with the option to move those steps into the same outer timeout.
Instrument initialization and network connection retain distinct lifecycle behavior.

## Private channel and classification fixes

The socket callback now acquires the ingress mutex before message conversion and JSON classification,
then registers/enqueues while holding it. It does not take account/execution/network locks.
Conversion failure sets failed and drops the gate before logging.

The finite raw ledger bridge maps `userEvents` to `user` and also sends userFills, userFundings,
nonfunding ledger, active asset data, TWAP, and webData2 payloads to the io actor. Unsupported
channels therefore explicitly fail its strict switch even if legacy parsing generated nonempty
ExecutionReports. Strict `user` input permits only a single complete `fills` field; funding,
liquidation, cancellation, or mixed payloads fail and invalidate execution recovery before the
following applied marker. Marking a frame applied does not restore an invalidated proof.
The earlier specific marker-consumption gap is fixed in the sampled source, awaiting peer execution.

## HTTP actual fills and execution revision

`recover_once` applies retrieved actual fills with the captured socket epoch but without its captured
execution revision. In the current source I do **not** classify this alone as a material stale
snapshot revival defect.

Actual trade facts are immutable economic evidence. Application checks active transport/current
epoch, exact raw coin, durable OID/CLOID and direction, source time versus intent creation/current
time, impossible terminal/NotWritten/Rejected phases, stable tid payload conflicts, cumulative size,
and exact native fee/quantity/price representability. Successful application retains reservations
and sets recovery incomplete. It does not adopt an HTTP balance/open-order snapshot, release funds,
or establish Ready. A same-epoch superseding query does not make an earlier genuine fill untrue.
The final old recovery revision check still prevents its completeness commit, and a subsequent
recovery must verify current order/account consistency. Mutable order-status application remains
generation-checked, which is the distinct requirement previously identified.

Automatic reconnect changes epoch and rejects old actual fill replies. Full disconnect uses task
shutdown to stop the old operation. Replaying a previously known real tid is idempotent; conflicting
financial evidence closes recovery. A native projection restart barrier suppresses unsupported
historical projection and cannot establish Ready from raw journal completeness alone.

If the product contract instead requires absolutely no events from a superseded HTTP operation,
an optional revision guard could implement that stricter contract. It should not be introduced
merely by analogy with mutable snapshots or described as a proven exposure/funds bug here.

These source findings were sent directly to root; the WebSocket agent was copied on the verified
bridge fixes. Final acceptance will use the exact stable candidate identity and retained evidence.
