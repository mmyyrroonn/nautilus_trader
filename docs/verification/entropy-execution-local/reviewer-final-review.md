# Entropy #101 independent final review

Date: 2026-10-08. Verdict: **PASS for the bounded opt-in implementation and local synthetic
acceptance scope; suitable for the user's authorized PR merge.** No remaining material blocker
was found in the reviewed source, final native regressions, Python projection, or installed wheel.
This is not live-account or mainnet approval.

## Final source and artifact

Native core: `acdd46caf83064f47fe57389e89a3e957452ced8`, based on latest main
`9644fd657e98cdde0067044c9e69a1d8fbd54c54`.
Final Python projection: `64d33d31daf0ef5a7be9de185a8578f1be86bc01`.
Tree: `a61662ae0a0fe013ca823cbb0a202697f4aaf2be`.

Independent verification confirmed clean current source and equal declared/pre-build/post-build
identities, zero dirty files, current Cargo.lock/builder/maturin config/Python lock hashes, and
recomputed canonical source fingerprint
`4f1dd4f491e8d649d19d30b7e63a568d20877d5fb8505f9a5f0b5641f72b0e36`.
Provenance bytes matched the strict installation record, and its declared build-input file hash
matched actual bytes. The documented build profile is `nextest`, not release.

Wheel: `E:/persarb/_tmp/entropy101-wheel-nextest-20261008/nautilus_trader-2.0.0rc4-cp312-cp312-win_amd64.whl`.
SHA256: `99d6351fc0e8ac3322d97808c6dc5da5a01eb1981ad8184a7fc73927cade94a1`.

Actually imported installed native module:
`E:/persarb/worktrees/entropy-account/.venv/Lib/site-packages/nautilus_trader/_libnautilus.cp312-win_amd64.pyd`.
Its actual bytes, wheel ZIP entry, provenance, and installation record all match SHA256
`e78c5d14ec5309a665ec918e6903f8ef7459ff107145fb96a03d8750ebefdbfc`.
All **23** embedded adapter stubs matched provenance and installed files. Hyperliquid's installed,
embedded, and generated-source stub matched
`a7f3cf9f44ea9dc4f800c38fa8cce20740a2c4ced494a250b492d0b1aa4cec3e`.

All **31** default-native test inputs matched final source bytes. The separately documented one
Python-feature source delta qualifies the existing RuntimeError mapper in `python/factories.rs`
and corrects its generated API documentation. Its file SHA and generated stub match
`binding-inputs.json`. Default-native tests did not compile this Python-feature projection;
the source-bound final wheel and installed Python tests provide its separate evidence.

## Independent and root verification

The final installed wheel independently passed **27/27** tests: 17 execution-policy/normal-factory
cases and 10 account-config/legacy-ABI cases. Tests used normal `LiveNode.builder` and actual native
factories, frozen settings, precise failure reasons, separate journal ownership/lease, unbound
diagnostics, and no fabricated account/recovery proof. Loopback port-zero endpoints were never
started. The interpreter imported `.venv/Lib/site-packages`, with `pythonpath=` and importlib mode
preventing source-package shadowing. No Cargo, generator, real account, or external order ran.

Final native binaries independently passed **6/6** selected cases: both actual private-funds
reductions, owned cancel, ordinary private subscription, subscription restoration, and physical
reconnect subscription replay. Exact filters and expected counts prevent empty-filter passes.
Binary hashes were checked before and after execution. Detailed argv, identities, output, exit
codes, and log SHA256 values are in `reviewer-final-native.json` and its referenced logs.

Earlier independent pre-lint verification passed 19 actual-factory economic/recovery peers plus
36 scope/ingress/signing/handler units. Those **55** results remain distinctly bound to the earlier
binary, rather than relabeled final-main execution. The subsequent Clippy changes were source
reviewed and did not alter identities, budget, lock order, or rejection semantics.

Root's final-main `native-all-final.log` independently inspected totals are **1,167 passed,
12 ignored**: library 810, data 46, dispatch 40, peer 70, execution 102, HTTP 46, WebSocket 53;
zero failed. Ignored coverage is 11 live smoke/soak cases and one existing approximately
30-second account-registration timeout case, not 12 live cases. Root's final installed
Python log reports **187 passed in 20.63 seconds**. These complete-suite counts are root runs,
not another reviewer full-suite run. Earlier failed build/test logs are retained as earlier attempts.

## Reviewed behavior

The opt-in path binds exact account/user/strategy/instrument/asset/CLOID and signed action facts.
Current source/epoch, isolated leverage, native precision, finite recovery, private stream receipt
and contiguous actor application are required. Explicit io ingress is installed before connect;
ordinary and account-only streams keep their legacy output.

Admission uses checked Decimal arithmetic and conservative full notional plus fee/margin buffers.
Both preparation and actual backend start_send recheck the minimum of HTTP/private free and
withdrawable, unresolved reservations, freshness, metadata, ownership, and policy limits under
the shared ingress/account/execution boundary. Private funds increases cannot loosen the HTTP
bound; actual receive time and unknown source time retain separate provenance. Funding/unknown
economic activity and exact fill conflicts invalidate proof before any applied marker can admit
new risk. Unsupported private activity is not silently ignored.

Durable journal lease/identity, finite size/fill bounds, signed prequeue fsync, backend readiness
and actual start_send linearization, deadline cancellation, and no migration/replay constrain
unknown writes. Only real owned fill facts project native fills; ACK aggregates do not synthesize
trades. Raw rebate/fee facts are exact and builder fees are not added twice. Cancel and confirmed
owned reduce-only IOC retain bounded ownership/quantity/limit restrictions.

## Remaining limits

Validation uses local synthetic peers with actual native factory, engine, cache, Portfolio, HTTP,
and WebSocket transport. It establishes neither real isolated-maintenance/liquidation economics
nor atomic exchange-wide account/asset observations. Private source time can remain unknown.
Full-notional/minimum-funds estimates are conservative policy inputs, not guaranteed venue margin.

The handler pending-send test uses a synthetic proof token/gate. Shared ingress and real backend
Pending/cancellation have layered focused evidence; deterministic normal-factory OS TCP pressure
was not established. Real Tungstenite codec/duplex Pending is not evidence of OS TCP backpressure.

Raw journal recovery cannot restore native cache/Portfolio after a fresh process. The persistent
projection barrier keeps new risk and close unsupported there; this is an intentional limit.
Funding/ledger activity and unrepresentable native fill precision close proof rather than being
calculated or rounded. Brackets, batches, modify, blanket cancel, external exposure adoption,
unbounded markets, and automatic resends remain outside the supported path. Startup's separately
bounded account registration/private-ready stages precede its metadata/recovery policy timeout.

Artifacts and commands: `reviewer-final-artifact.json`, `reviewer-final-python.log`,
`reviewer-final-native.json`, `reviewer-final-native-*.log`, and executable verification scripts in
this validation directory. No production/test/tracked file, commit, push, or external account was
modified by this final review.
