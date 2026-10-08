# Read-only rebase integration review

Reviewed at 2026-10-08 14:07:14 UTC. Worktree: `E:/persarb/worktrees/entropy-account`.

- Base main: `9644fd657e98cdde0067044c9e69a1d8fbd54c54` (PR #105, fast books).
- Original execution commit: `ee57c87fec899cce1168ff58c6e09944bd51e078`.
- Rebased HEAD: `acdd46caf83064f47fe57389e89a3e957452ced8`.
- `git range-diff 95fab86a..ee57c87fec 9644fd657e..acdd46caf8` reports `=`. Git status was clean.

No material integration finding from source inspection:

1. `SubscriptionRequest::L2Book.fast` remains optional and omits only `None`; `Some(false)` stays explicit. Book options retain fast/precision on registration, ordinary subscribe, recovery unsubscribe+subscribe, final unsubscribe, and automatic reconnect (topic reconstruction is followed by options restoration).
2. Fast book registry and timestamp parser, plus existing websocket/data-client tests, have no diff from new main. Fast=true remains limited to five levels and incompatible with Depth10; conflicting shared fast options fail before changing the active stream.
3. The io fence is default-disabled and installed once by successful explicit `IoExecutionRuntime::new`. Only enabled relevant private receipts receive tokens; handler raw bridging/markers require those tokens. Ordinary public/private Custom/report streams remain free of io internal events. Read-only io clearinghouse and subscription ACK branches remain intact.
4. Public l2Book frames bypass the private receipt fence, whether fast is omitted/false/true. Public subscription ACKs are ignored by account identity acknowledgement unless one of the scoped user streams; they cannot create io readiness. Both reconnect options restoration and the private epoch marker ordering remain present.
5. Against new main, `messages.rs` adds only the two internal io variants; `data.rs` adds only explicit ignoring of those variants in its execution-only match. No fast parameter path was reverted.

This is a static integration review. No Cargo, stubs, commit, tracked-file edits, account queries, or new runtime PASS claims. Root is running the new-main regressions and wheel verification.

Captured file SHA-256 (raw worktree bytes):

- `crates/adapters/hyperliquid/src/websocket/client.rs`: `aaa17b216bb35466a91a56b9523ea2c766da9761214c9da3d1aaa1c93b17890a`
- `crates/adapters/hyperliquid/src/websocket/handler.rs`: `0d1dbc53fbf32b1f6a6cb81425f32029fb049b2e3a53b2131f0417eb9827995e`
- `crates/adapters/hyperliquid/src/websocket/messages.rs`: `e098e2f9b201d6adae1850a6705e04bb172d251847e3d4863ec004ec74d32fbe`
- `crates/adapters/hyperliquid/src/websocket/book.rs`: `1bfe5150c644cf8eff64bf0c7b65a36c73934c4799ae2177c5f87a2f8092aaee`
- `crates/adapters/hyperliquid/src/data.rs`: `d8fa13a5d29a9248f3bf9272bf5861ec16b5eb72f99f17e472322e1f7af8c037`
- `crates/adapters/hyperliquid/src/common/parse.rs`: `d4eb8beb578585159a0f13cd0d31c7b184f17302a8c643bdb0b3d7f747d59609`
