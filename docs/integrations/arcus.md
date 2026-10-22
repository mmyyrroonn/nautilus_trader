# Arcus

The Arcus adapter connects NautilusTrader LiveNode to the Arcus perpetuals REST
and WebSocket APIs. Its Rust clients are exposed through Python configuration
and factories, following the same integration pattern as Hyperliquid.

## Supported operations

| Area          | Support                                                                            |
| :------------ | :--------------------------------------------------------------------------------- |
| Instruments   | USD-quoted perpetuals, `BTC-USD-PERP.ARCUS` naming, exact tick and size increments |
| Streaming     | Best bid/ask, trades, L2, mark prices, oracle/index prices, predicted funding      |
| History       | Hourly market funding rates with bounded pagination                                |
| Orders        | Limit GTC, IOC, FOK, post-only GTC, reduce-only                                    |
| Market orders | Slippage-bounded IOC limit using a recent cached quote                             |
| Modification  | Price and total quantity of confirmed resting limit orders                         |
| Cancellation  | Individual orders and instrument/side-scoped cancel-all                            |
| Account       | USDG margin balance, order/fill/position reports and reconciliation                |
| Environments  | Mainnet and testnet, with separate credential variables                            |

Conditional/trailing orders, order lists, bars, recent public trade requests,
and standalone REST book requests are not supported. Unsupported trading
commands fail explicitly before sending an order.

Arcus uses GTT for resting orders. Nautilus GTC maps to GTT with a 33-day expiry;
the adapter does not renew resting orders automatically. Arcus also requires a
long replay-protection expiry on IOC and FOK orders. A market order is represented
on the venue as an IOC limit. `market_slippage_bps` defaults to 50 and is limited
to 1 through 1000. A missing or stale quote rejects the market order locally.

Arcus client IDs are case-sensitive, 1 through 36 ASCII letters, digits,
hyphens, or underscores. Use unique client order IDs across the account's
history; the venue permits reusing a client ID after a terminal order, which
does not provide durable request idempotency.

## Reference prices and funding

`subscribe_mark_prices` publishes the engine's `markPrice` with its
`markEpochNanos` source timestamp. A zero or unavailable mark is not substituted
with the oracle price. An initial snapshot without a mark timestamp waits for
a live update before publishing a mark.

`subscribe_index_prices` maps Arcus's oracle `price` into `IndexPriceUpdate`.
The oracle feed's live batch `epoch` is in nanoseconds. An initial snapshot with
no source timestamp uses receipt time, without advancing the live source
watermark. Mark and index subscribers share one `oraclePrices` subscription
per market; removing one leaves the other active. Reconnection restores the
subscriptions and clears the previous connection's decoding watermarks.

`subscribe_funding_rates` publishes the `predictedFunding` channel's `rate1h`
as a decimal hourly fraction with `interval=60` minutes. For example, `0.0001`
means 0.01% per hour. Negative rates retain their sign. These predictions are
timestamped at receipt because the documented payload has no source event time;
`next_funding_ns` remains unset. Predictions and settled historical rates have
different meanings even though both use `FundingRateUpdate`.

`request_funding_rates` reads public
[`/v1/fundingRates`](https://docs.arcus.xyz/api-reference/public/get-market-funding-rates)
history. The adapter
converts inclusive time bounds to microseconds, deduplicates overlapping page
boundaries, and returns rates in ascending payment-time order. A requested limit
selects the latest available rows. Requests are bounded to 64 pages of at most
1,000 rows and fail if the requested result cannot be proven complete within
that bound. The venue does not document a lifetime retention guarantee. These
requests share the process-wide read budget and do not block streaming updates.
Invalid request bounds fail synchronously. Asynchronous retrieval failures are
logged with the request ID and leave the framework request to time out; they do
not emit a successful empty result.

## Order modification

`modify_order` changes a confirmed open limit order's price, total quantity, or
both. Total quantity includes fills already received. The request preserves the
original side, time in force, reduce-only flag, and expiry. Modification requires
an authoritative server order ID and reconciled order state; unconfirmed orders,
market orders, and trigger-price changes are rejected locally. The original
expiry must still satisfy Arcus's one-month minimum when the modify is signed;
the adapter does not silently extend it.

The adapter signs [`modifyOrder`](https://docs.arcus.xyz/api-reference/exchange/modify-order)
with typed operation `3`. An HTTP acknowledgment
does not produce an order update. Post-only modifications use the cancellation
priority queue. Depending on the venue deployment, a modify may preserve the
order in place or emit an intermediate `MODIFY_CANCELED` followed by a replacement
under the same server ID. That intermediate event is not treated as a terminal
cancellation. Ambiguous outcomes require authoritative reconciliation and are
never retried automatically. A stale or unsequenced report cannot confirm a
pending modification. An unresolved modification prevents new risk until its
outcome is reconciled; cancellation remains available.

## Credentials and accounts

Register an Ed25519 API key through the Arcus application or the official
[authentication flow](https://docs.arcus.xyz/api-reference/authentication).
The adapter takes the 32-byte Ed25519 seed encoded as hexadecimal. An Ethereum
wallet private key is not a substitute. It does not create keys, deposit,
withdraw, transfer funds, or change leverage.

| Environment | Address                 | Secret seed                | Optional expected public key |
| :---------- | :---------------------- | :------------------------- | :--------------------------- |
| Mainnet     | `ARCUS_ADDRESS`         | `ARCUS_API_SECRET`         | `ARCUS_API_KEY`              |
| Testnet     | `ARCUS_TESTNET_ADDRESS` | `ARCUS_TESTNET_API_SECRET` | `ARCUS_TESTNET_API_KEY`      |

Store values only in the application's gitignored `.env` and load them into
the process environment before building the node. The adapter never loads
arbitrary `.env` files. Testnet never falls back to mainnet variables. If an
expected public key is supplied, it must match the seed. Secrets are excluded
from configuration serialization and Python representations.

Set `subaccount_id` explicitly when using a nonzero subaccount (0 through 9).
The address is the master Ethereum address owning the API key. Testnet accounts
must be funded separately; registering a key does not create collateral.

The integration models the perpetual margin account using `USDG.ARCUS`, a
nine-decimal ledger currency backed one-for-one by USDG. Arcus accounts and fees
use nano-dollar precision; the separate code preserves exact values when
serializing and restoring balances and PnL without changing Nautilus's existing
eight-decimal `USDG` currency. Instruments retain USD as their quote currency.
Importing the adapter registers `USDG.ARCUS` for persisted data replay.

Spot collateral holdings, borrowing, and negative free collateral are not
modeled. Inconsistent or unsupported margin snapshots prevent execution
readiness rather than inventing available funds.

## LiveNode configuration

```python
from nautilus_trader.adapters.arcus import ArcusDataClientConfig
from nautilus_trader.adapters.arcus import ArcusDataClientFactory
from nautilus_trader.adapters.arcus import ArcusEnvironment
from nautilus_trader.adapters.arcus import ArcusExecutionClientConfig
from nautilus_trader.adapters.arcus import ArcusExecutionClientFactory
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountId, InstrumentId, TraderId

instrument = InstrumentId.from_str("BTC-USD-PERP.ARCUS")
environment = ArcusEnvironment.TESTNET
node = (
    LiveNode.builder("ARCUS-TRADER", TraderId.from_str("TRADER-001"), Environment.LIVE)
    .with_reconciliation(reconciliation=True)
    .add_data_client(
        None,
        ArcusDataClientFactory(),
        ArcusDataClientConfig(environment=environment, load_ids=[instrument]),
    )
    .add_exec_client(
        None,
        ArcusExecutionClientFactory(),
        ArcusExecutionClientConfig(
            account_id=AccountId.from_str("ARCUS-001"),
            environment=environment,
            load_ids=[instrument],
            market_slippage_bps=50,
        ),
    )
    .build()
)
# Add the strategy and its risk configuration before node.run().
```

The [data example](../../examples/live/arcus/data_tester.py) streams public
mainnet data. The [execution example](../../examples/live/arcus/exec_tester.py)
defaults to testnet and dry-run. Retain the application's existing order,
position, notional, and loss limits when integrating an execution strategy.

## Recovery and performance

An HTTP 202 is a transport acknowledgement, not an order acceptance. The adapter
uses order and fill evidence to resolve the outcome. Transport timeouts,
ambiguous responses, and disconnected streams do not cause automatic write
retries. New submissions require healthy account streams and reconciliation;
an unresolved submission prevents adding risk until its outcome is known.

WebSocket reconnection replays subscriptions and requires fresh snapshots.
L2 sequence checking follows Arcus's documented splice rule: the first delta
may jump beyond the snapshot sequence; later deltas must be contiguous. BBO
sequence jumps are normal. Internal queues have a configurable bound, and
overflow invalidates stream readiness. The shared Nautilus engine event channel
retains the framework's existing queue contract.

Account history requests use bounded pagination and fail if a full result
cannot be proven within 64 pages of up to 1,000 records. Initial recovery reads
the available history; subsequent cycles combine overlapping incremental
history with complete open-order snapshots. Missing open orders require an
authoritative status lookup before their final state is reported.

Account and position notifications are coalesced by an independent reconciliation
worker, with at least ten seconds between full refreshes. Order and fill streams
continue to be processed during these HTTP requests. The session retains at most
100,000 order intents and identities and refuses further risk when that capacity
is reached; plan session rotation for sustained high turnover. Fill deduplication
is bounded to 100,000 identities in its recent overlap window.

The adapter's process-wide read budget is shared by its HTTP
clients; other processes sharing the same source IP need deployment-level
coordination. An HTTP 429 is reported to the caller. REST I/O and WebSocket
handling are asynchronous, and signing keys and HTTP connection pools are reused.

See the official [REST trading guide](https://docs.arcus.xyz/guides/rest-trading),
[WebSocket reference](https://docs.arcus.xyz/api-reference/websocket), and
[latency guide](https://docs.arcus.xyz/guides/latency) for venue execution semantics.
Local protocol benchmarks measure parsing and signing only; they do not measure
network latency, venue matching latency, or trading profitability.
