# Fresh-flat io startup reconciliation

This contract supports the normal Hyperliquid `generate_mass_status` startup path
with reconciliation enabled and an explicit finite io execution policy. It proves
only a new client's current selected flat projection. Generic cold restoration,
owned historical lineage, whole-venue completeness and real economics remain
outside this contract. General io order/fill/position report APIs retain their
existing refusal; the non-io mass-status path is unchanged.

## Eligibility

The original journal descriptor must be empty when this client opens and
exclusively locks it, before replay or connect persistence. Same-client empty
verification records preserve this origin. Any subsequent client opening a
nonempty journal is nonfresh, even if its last record has empty maps. Startup
requires untainted origin, zero actions/intents/fills, no cold projection debt,
current completed native recovery and no outstanding reservation. Errors never
erase journals, release unknown reservations or replay fabricated financial facts.

The normal account proof must be current and trusted, with the actual user role,
mode, io DEX, canonical USDC collateral, full clearinghouse source timestamp and
matching private stream. This minimum contract requires a complete explicit empty
`assetPositions` array; missing/null/invalid arrays and retained position rows,
including zero-size rows, refuse admission. Equity/used/free must roundtrip through
the actual native USDC Money precision without rounding.

Finite selected metadata must match complete native instruments in both the HTTP
provider and engine Cache: exact ID/raw coin/venue, active strict-isolated margin
mode and leverage, freshness, native quantity precision/increment, USD quote,
USDC settlement and non-inverse convention. Serialized full native instrument
facts bind both caches across awaits; ID-only equality cannot authorize changes.

All same-account Cache order and position history is refused, including closed,
external and cross-strategy facts. Any account-less routed Hyperliquid io order,
selected or unselected, is ambiguous and refuses startup. A venue Flat report must
never synthesize a close of existing cached financial history.

## Sources and final boundary

One monotonic `recovery_timeout_ms` deadline covers the entire attempt. After a
normal role/mode/collateral/account refresh, actual full io `frontendOpenOrders`,
unfiltered retained `userFills`, and unfiltered `historicalOrders` must each succeed
with an explicit empty array. Any observed row, including another DEX's history,
is conservatively rejected rather than filtered into apparent freshness. The
observed empty retained window does not certify account-lifetime absence.

History JSON acceptance is bounded to 1 MiB per body before decoding, using the
existing native info transport, quotas and retries. That transport first buffers
under its existing 100 MiB read bound. Account-proof reads retain their existing
bounds. This change does not claim a global 1 MiB network allocation limit.

No final proof lock crosses an HTTP await. Before/after the explicit refresh and
at final publication, the native private-ingress receipt sequence is identical,
with the same reader generation and epoch. Final ingress must be completely
actor-applied; accepted same-reader financial or identical snapshot messages also
invalidate the earlier read group. Account/execution revisions, exact private
funds, complete account snapshot, selected metadata and native Cache facts remain
bound. The final callback constructs reports while holding ingress, account,
execution and read-only Cache guards in that order. TTL and monotonic deadline
are checked after proof-lock waits.

Each verified selected ID receives a genuine native Flat position report with
exact zero quantity precision, actual clearinghouse source time and local report
receipt. The mass report explicitly uses the local account verification start as
its observation window and sets `reports_complete=false`. Empty orders/fills are
backed by actual successful reads and the empty durable origin. Normal manager
reconciliation of an empty Cache must create no financial order/fill/position
adjustment; normal Portfolio initialization is a separate lifecycle observation.

## Evidence limits

REST/private observations are non-atomic venue reads. Local fences prevent mixed
or superseded authority, without claiming an atomic remote transaction. Native
integration races cover changed and identical actually received private messages,
reader loss, metadata/action changes, history deadlines and account TTL. Precise
unapplied-only startup and TTL-expiry-during-final-lock-wait scenarios are not
new end-to-end passes: the existing ingress counter/lock tests and final source
fence are explicitly compositional evidence. Numeric-loopback peers do not prove
Windows kernel isolation, actual venue execution, funding, run ownership, PnL or
durable cold recovery. Those acceptance items remain separate and open.
