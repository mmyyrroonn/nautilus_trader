# Independent timing and latest-private-funds review

Date: 2026-10-08. Scope: read-only source and synthetic acceptance fixture review.
No Cargo, real account, external write, production edit, or final candidate PASS in this review.

## Test timing changes

The sampled execution peer uses a five-second periodic clearinghouse interval only when its
synthetic execution configuration is present. The previous account-only peers retain their
60-millisecond interval. Subscription triggers an immediate actual private snapshot, and
`set_position` explicitly sends a private snapshot with a fixture source timestamp. This changes
fixture churn, rather than weakening production age, ingress, epoch, quota, or deadline checks.

Waiting for post-Accepted recovery before the next economic operation is appropriate: Accepted
alone does not establish account/recovery completeness. `refresh_owned` uses the ordinary owned
QueryOrder path to recover, while avoiding a duplicate expensive all-account metadata pass.
The 25/15/5-second caller sleeps retain real HTTP quota behavior. There is no rate-limit production
diff in the inspected candidate. These are valid fixture scheduling changes, subject to the new
full run actually passing; they do not establish general throughput or latency guarantees.

`native-all-03.log` remains pre-fix evidence: library 789 passed, peer 62/68 passed, with six
failed peer cases. Their closed proof/deadline failures cannot be reported as accepted execution.
Root has retained that log and is rerunning after the latest fixes.

The Windows network portability change only conditionally excludes the socket2 getter assertion
on platforms where the pinned library does not expose that getter. Nodelay, keepalive enablement,
and the setter remain exercised. The inspected change is confined to a test, with the original
getter assertion preserved on supported targets.

## Latest private funds defect and sampled correction

Before this correction, a complete same-position private snapshot could show free/withdrawable
falling from HTTP 100 to private 1 while execution continued to budget against HTTP 100. Position,
epoch, source freshness, and ingress completeness alone did not prevent a new 20+fee entry.
This was a material admission defect and was sent to root and the production owner.

The sampled correction exposes a detached `PrivateFundsWitness` containing exact free,
withdrawable, actual receive time, and optional unmodified source time. The HTTP account DTO is
not overwritten with private values. Existing account-only behavior allowing floating REST/WS
financial differences remains intact.

Both prepare and the actual backend admission callback calculate the minimum of HTTP free,
HTTP withdrawable, latest private free, and latest private withdrawable. New-risk admission then
subtracts unresolved reservations and the policy margin buffer with checked Decimal arithmetic.
Missing private evidence rejects admission. A private increase cannot exceed the HTTP bound.
Final admission reads current private evidence under the account lock held through start_send,
after ingress completeness, current epoch, and trusted/fresh account checks. An observed reduction
after prepare therefore changes the final budget. The private source timestamp remains null when
the source did not provide it; it is not replaced with receive time.

Confirmed owned reduce-only IOC admission remains governed by exact signed position and pending
close quantity rather than a new-risk fund requirement. This preserves its existing bounded
close behavior and does not permit an entry to bypass the budget.

The new normal-factory test samples falling free and falling withdrawable independently, waits
for the actual private source timestamp to be consumed, keeps the old HTTP facts at 100, then
requires Denied, zero peer writes, and flat Portfolio. After my request, root added trusted/Ready
controls, exact latest-private-funds values and receive/source timestamps, and the precise
`Insufficient conservative io funds` reason both on direct submit failure and the cached native
Denied event. This closes the unrelated-ingress-rejection false positive in the sampled source.
The opt-in ingress installation is now called by `IoExecutionRuntime::new` after journal checks
and before any connection; its account WebSocket clone shares the same ingress state.

## Remaining evidence boundaries

The independent arithmetic and getter tests are source reviewed, not independently executed yet.
The fixed source, explicit io ingress installation, full suite, source-bound wheel, and installed
normal factory still require final stable-candidate evidence. Synthetic constant account values
and fabricated source timestamps describe the local peer, not remote exchange atomicity or
real fee/PnL/margin fidelity. Startup's sampled outer policy timeout covers metadata/recovery;
its preceding account refresh/registration/private-ready stages are separately bounded and are
not included in that policy timeout. Avoid claiming a single whole-connect policy deadline.
