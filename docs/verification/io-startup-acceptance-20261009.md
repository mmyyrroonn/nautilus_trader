# Fresh-flat io startup acceptance

Native issue [111](https://github.com/mmyyrroonn/nautilus_trader/issues/111) supports
normal startup reconciliation for a fresh, explicitly selected, currently flat io
client. The [contract](io-startup-contract-20261009.md) describes positive authority
and conservative refusals. Generic cold restoration and account-lifetime history
remain outside this change.

The branch began at freshly fetched `main@9ff73d1205b4199f9b44686fdf785382388e67e2`.
The clean implementation commit is `6f46bad474f850b9279a3a8950f5b300570f179d`,
tree `a42012494565dbe33e949e0d0e7db200305965b0`. Later documentation and evidence
packaging do not replace the actual wheel's source identity.

## Bound artifacts

The wheel is
`E:/persarb/_tmp/entropy111-wheel-nextest-20261009/nautilus_trader-2.0.0rc4-cp312-cp312-win_amd64.whl`.
Its SHA256 is `5aacb4129526edfdabe77739781fa9518a6b21224fdc59d0fa8b5e99006801fb`;
the actual embedded and installed pyd SHA256 is
`f65f0a614f6a57e1a34186ce924bb9e6fc1b887b92b9b2f630cb352319786511`.
Source fingerprint SHA256 is
`cf0c60ef59da6a0a2e1018b3b124013a35fe3db9db6543e588925a669f3829e4`.
The build used CPython 3.12.9, uv 0.12.6, Rust 1.98.0, maturin 1.15.0 and the
`nextest` profile, from 04:37:18 through 04:39:13 UTC on 2026-10-09.
Both pre/post build source identities were clean and unchanged.

Strict installation selected the isolated native-proof and application execution
interpreters. The application's formal candidate lock and original workspace
interpreter remain separate. The reviewer independently compared the actual
loaded pyd and all 23 declared adapter stubs with wheel ZIP bytes and provenance,
and checked 56 source/installed/artifact identities after acceptance without changes.
The source Git commit/tree are root-supplied build provenance; the reviewer checked
file bytes without independently recomputing Git objects.

[Provenance](io-startup-local/native-provenance.json),
[manifest](io-startup-local/manifest.json) and
[commands](io-startup-local/commands.json) retain hashes, actual argv, UTC intervals,
failures, reviews and temporary reproduction procedures. Large binaries are
identified rather than committed.

## Actual checks

- Hyperliquid library/integration regression: 1301 passed, zero failures and
  twelve pre-existing ignored cases, 04:26:53 through 04:28:09 UTC.
- Final startup group: 55 passed after a test-only semicolon correction and
  recompilation. Production source was unchanged from the full regression.
- Independent final native execution: 92 distinct cases passed, including all
  55 startup cases, 30 execution-scope cases, six ingress mechanism cases and one
  prior actual-fill cold-restart rejection. These overlap root regression and
  are not added to its total.
- Installed Python account/execution/economic/config/stub regression: 242 passed.
- Independent normal read-only installed LiveNode: PASS with reconciliation
  enabled, actual RUNNING state, initialized Portfolio, zero selected exposure,
  exact USDC total/free/locked `100`/`100`/`0`, genuine fresh durable origin and
  native first reader epoch zero.
- Production Hyperliquid Clippy `--lib --no-deps -- -D warnings`: PASS.
  Test-inclusive Clippy remains FAIL on ten unchanged baseline Hyperliquid test
  diagnostics; dependency-inclusive Clippy also encounters three inherited
  Windows parquet diagnostics. No warning suppression was added to Clippy.
- Official stub generation: PASS; 44 generated stubs had unchanged AST and
  normalized text against baseline. Scoped nightly Rust formatting passed.
- Required `make format` and `make pre-commit`: launch exit 127 because make is
  unavailable on this Windows host. Scoped checks do not establish a full
  workspace pre-commit pass.

The full suite consists of library 867, data 46, dispatch 40, account/execution
147, ordinary execution 102, HTTP 46 and WebSocket 53 passed cases. Eleven live
or soak cases and one account-registration case remain ignored and unexecuted.
The final account executable is SHA256
`54bc216651f2d99933347023ec7400302d729367b43e6e17f57fae4a2b9509cf`;
the unchanged library executable is SHA256
`52ebd610eabccb706a9bbf5de38a2827529108859b2177357b7f8718fe38ff59`.
The earlier regression account executable differs only because the final
test-only semicolon change was subsequently compiled. Both identities are retained.

The [final source review](io-startup-local/reviewer-final-fence-source-review.md.txt)
and [lint addendum](io-startup-local/reviewer-final-lint-source-addendum.md.txt)
found no blocker for the narrow contract. Baseline lint provenance is an exact
unchanged-source comparison against the branch base, not a new baseline Clippy run.
Precision mismatch and lock races are covered as described below; precise
unapplied-only startup and TTL expiry during final lock wait are compositional
ingress/source evidence, not additional claimed end-to-end startup passes.

The [installed acceptance](io-startup-local/reviewer-installed-normal-startup-report.md.txt)
ran from 04:44:16 through 04:44:18 UTC. It retained 45 real loopback requests and
responses, including actual explicit empty open-order/fill/history arrays and
normal role/mode/collateral/metadata/account sources. Three WS commands were
ordinary read subscriptions, not financial actions. Normal stop/dispose left
zero sessions, zero peer errors and zero orders/fills/actions. No Strategy or
Actor, manual Cache event or synthetic financial adjustment was used.

Exact-head Actions results are recorded in the issue and PR timeline after
packaging. The existing dispatchable adapter workflow checks Aster, Ondo and
Backpack; its result does not replace the local Hyperliquid checks. Workflows,
upstream rules and release files were not changed.

## Preserved failures and scope

Initial compile and five initial startup failures remain intact. Three failures
were Windows journal reads conflicting with the original exclusive lock; the
tests now stop/drop the normal client before reading unchanged journal bytes.
One negative precision value was exactly representable under real USDC precision
eight; its corrected fixture uses precision plus one and asserts actual native
Money inequality. One exposed a real production race: an accepted same-reader
private funds update could leave account revision unchanged. The fix binds the
actual checked ingress receipt sequence, exact private funds and full account
snapshot across reads and final publication. An identical-frame negative case
also verifies that revision equality cannot borrow authority from later ingress.

The reviewer originally expected intersecting libtest filters; Rust actually ran
30 union-matching cases, all passing. The failed count assertion and subsequent
continuation are preserved without rerunning passed cases. Two normal-node
reviewer failures came from an overbroad `commands == []` assertion and wrong
failure cleanup order. The final harness retains financial-zero assertions and
allows only exact known ordinary read subscriptions, stopping clients before
peer teardown. Production/app/fixture bytes did not change between those runs.

A [superseding accuracy note](io-startup-local/reviewer-identity-helper-correction.md.txt)
corrects older issue 109 wording: its normal runner indirectly executed read-only
Git identity queries through application verification. Actual passes, hashes and
traffic are unchanged; no Git mutation occurred. The issue 111 reviewer performed
its own ZIP/provenance verification and did not call that helper. Earlier raw
reports claiming later acceptance was pending remain unchanged and are superseded
by the separate final installed acceptance.

Application issue 43 resumed all seven installed-native scenarios on this wheel:
one foreign-activity refusal passed and six failed in application logic. No
orders/fills occurred in that initial seven-case attempt. Native startup now
reconciles and runs normally; application proof epochs and refresh scheduling
require their own corrections and subsequent evidence. This acceptance does not
claim that paired execution or application issue 43 is complete.

Kernel isolation, actual venues/accounts, funding, run PnL, full account/history
completeness and durable cold restoration remain unverified. Issues 29, 43, 88,
90 and 107 retain their distinct requirements. No real mainnet order, transfer,
mode change or leverage change was performed.
