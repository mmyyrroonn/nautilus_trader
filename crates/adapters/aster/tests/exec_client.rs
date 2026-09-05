// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Integration tests for the Aster execution client against a scripted mock venue.

mod common;

use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};

use common::{MockVenue, PAGE_LIMIT, SubmitOutcome, TEST_PRIVATE_KEY};
use log::{Level, LevelFilter, Log, Metadata, Record};
use nautilus_aster::{
    common::consts::{ASTER_CLIENT_ID, ASTER_VENUE},
    config::AsterExecutionClientConfig,
    execution::AsterExecutionClient,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::TestClock,
    live::runner::{replace_data_event_sender, replace_exec_event_sender},
    messages::{
        DataEvent, ExecutionEvent,
        execution::{
            CancelAllOrders, ExecutionReport, GenerateFillReports, GenerateOrderStatusReports,
            GeneratePositionStatusReports, SubmitOrder,
        },
    },
    testing::wait_until_async,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_live::{
    ExecutionClientCore,
    manager::{ExecutionManager, ExecutionManagerConfig},
};
use nautilus_model::{
    accounts::{Account, AccountAny, MarginAccount},
    enums::{AccountType, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{AccountState, OrderEventAny},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TraderId},
    instruments::Instrument,
    orders::{Order, OrderAny, builder::OrderTestBuilder},
    types::{AccountBalance, Currency, Money, Price, Quantity},
};
use rstest::rstest;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedReceiver;

const ACCOUNT_ID: &str = "ASTER-001";
const BTC: &str = "BTCUSDT-PERP.ASTER";
const ETH: &str = "ETHUSDT-PERP.ASTER";

/// Binance's USD-M VIP-0 defaults, which the shared instrument parser fills in.
const BINANCE_DEFAULT_MAKER: &str = "0.0002";
const BINANCE_DEFAULT_TAKER: &str = "0.0005";

/// The account rates Aster's live testnet reports for BTCUSDT, deliberately distinct from the
/// Binance defaults above so a test cannot pass on the fallback.
const ASTER_MAKER: &str = "0.000050";
const ASTER_TAKER: &str = "0.000400";

struct Harness {
    client: AsterExecutionClient,
    exec_rx: UnboundedReceiver<ExecutionEvent>,
    data_rx: UnboundedReceiver<DataEvent>,
    cache: Rc<RefCell<Cache>>,
}

fn build_harness(venue: &MockVenue, http_timeout_secs: Option<u64>) -> Harness {
    build_harness_with(venue, http_timeout_secs, None)
}

fn build_harness_with(
    venue: &MockVenue,
    http_timeout_secs: Option<u64>,
    ws_connect_timeout_secs: Option<u64>,
) -> Harness {
    let account_id = AccountId::from(ACCOUNT_ID);
    let cache = Rc::new(RefCell::new(Cache::default()));

    let core = ExecutionClientCore::new(
        TraderId::from("TESTER-001"),
        *ASTER_CLIENT_ID,
        *ASTER_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None, // base_currency
        cache.clone(),
    );

    let config = AsterExecutionClientConfig {
        account_id,
        signer_private_key: Some(TEST_PRIVATE_KEY.to_string()),
        base_url_http: Some(venue.http_url()),
        base_url_ws: Some(venue.ws_url()),
        http_timeout_secs,
        ws_connect_timeout_secs: ws_connect_timeout_secs
            .or(AsterExecutionClientConfig::default().ws_connect_timeout_secs),
        ..Default::default()
    };

    let (exec_tx, exec_rx) = tokio::sync::mpsc::unbounded_channel();
    let (data_tx, data_rx) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(exec_tx);
    replace_data_event_sender(data_tx);

    let client = AsterExecutionClient::new(core, config).expect("client");

    Harness {
        client,
        exec_rx,
        data_rx,
        cache,
    }
}

fn seed_account(cache: &Rc<RefCell<Cache>>) {
    let state = AccountState::new(
        AccountId::from(ACCOUNT_ID),
        AccountType::Margin,
        vec![AccountBalance::new(
            Money::from("1000.0 USDT"),
            Money::from("0 USDT"),
            Money::from("1000.0 USDT"),
        )],
        vec![],
        true, // is_reported
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        None, // base_currency
    );
    cache
        .borrow_mut()
        .add_account(AccountAny::Margin(MarginAccount::new(state, true)))
        .expect("account");
}

/// Scripts the venue's happy-path connect responses.
fn script_connect(venue: &MockVenue) {
    venue.script(|script| {
        script.balances = json!([
            {"asset": "USDT", "balance": "1000.0", "availableBalance": "1000.0"},
        ]);
        script.commission_rates.insert(
            "BTCUSDT".to_string(),
            json!({
                "symbol": "BTCUSDT",
                "makerCommissionRate": ASTER_MAKER,
                "takerCommissionRate": ASTER_TAKER,
            }),
        );
        script.commission_rates.insert(
            "ETHUSDT".to_string(),
            json!({
                "symbol": "ETHUSDT",
                "makerCommissionRate": ASTER_MAKER,
                "takerCommissionRate": ASTER_TAKER,
            }),
        );
    });
}

async fn connected_harness(venue: &MockVenue) -> Harness {
    script_connect(venue);
    let mut harness = build_harness(venue, Some(30));
    seed_account(&harness.cache);
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    venue.clear_requests();
    harness
}

fn limit_order(client_order_id: &str, side: OrderSide, quote_quantity: bool) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(BTC))
        .client_order_id(ClientOrderId::from(client_order_id))
        .side(side)
        .quantity(Quantity::from("0.010"))
        .price(Price::from("50000.00"))
        .time_in_force(TimeInForce::Gtc)
        .quote_quantity(quote_quantity)
        .build()
}

fn submit_command(order: &OrderAny) -> SubmitOrder {
    SubmitOrder::new(
        order.trader_id(),
        Some(*ASTER_CLIENT_ID),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        order.init_event().clone(),
        None, // exec_algorithm_id
        None, // position_id
        None, // params
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    )
}

fn drain_exec(rx: &mut UnboundedReceiver<ExecutionEvent>) -> Vec<ExecutionEvent> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

fn order_events(events: &[ExecutionEvent]) -> Vec<&OrderEventAny> {
    events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Order(order) => Some(order),
            _ => None,
        })
        .collect()
}

fn order_reports(events: &[ExecutionEvent]) -> Vec<(OrderStatus, Option<ClientOrderId>)> {
    events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Report(ExecutionReport::Order(report)) => {
                Some((report.order_status, report.client_order_id))
            }
            ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, _)) => {
                Some((report.order_status, report.client_order_id))
            }
            _ => None,
        })
        .collect()
}

fn fill_trade_ids(events: &[ExecutionEvent]) -> Vec<String> {
    events
        .iter()
        .flat_map(|event| match event {
            ExecutionEvent::Report(ExecutionReport::Fill(report)) => {
                vec![report.trade_id.to_string()]
            }
            ExecutionEvent::Report(ExecutionReport::OrderWithFills(_, fills)) => {
                fills.iter().map(|fill| fill.trade_id.to_string()).collect()
            }
            _ => Vec::new(),
        })
        .collect()
}

fn account_states(events: &[ExecutionEvent]) -> Vec<&AccountState> {
    events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Account(state) => Some(state),
            _ => None,
        })
        .collect()
}

fn venue_order(
    order_id: i64,
    client_order_id: &str,
    symbol: &str,
    status: &str,
    side: &str,
) -> Value {
    json!({
        "symbol": symbol,
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "price": "50000.00",
        "avgPrice": "0.00000",
        "origQty": "0.010",
        "executedQty": if status == "FILLED" { "0.010" } else { "0" },
        "cumQuote": "0",
        "status": status,
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": side,
        "positionSide": "BOTH",
        "reduceOnly": false,
        "closePosition": false,
        "time": 1_788_571_663_000i64,
        "updateTime": 1_788_571_663_397i64,
    })
}

/// Milliseconds since the epoch, for fixtures that must fall inside a relative lookback.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as i64
}

fn venue_trade(id: i64, order_id: i64, symbol: &str, time: i64, commission: &str) -> Value {
    json!({
        "symbol": symbol,
        "id": id,
        "orderId": order_id,
        "price": "50000.00",
        "qty": "0.010",
        "quoteQty": "500.0",
        "realizedPnl": "0",
        "side": "BUY",
        "positionSide": "BOTH",
        "maker": true,
        "buyer": true,
        "commission": commission,
        "commissionAsset": "USDT",
        "time": time,
    })
}

// ------------------------------------------------------------------------------------------------
// F01 - quote-denominated quantities
// ------------------------------------------------------------------------------------------------

#[rstest]
#[case(OrderType::Limit)]
#[case(OrderType::Market)]
#[tokio::test]
async fn test_quote_quantity_order_is_denied_without_reaching_the_venue(
    #[case] order_type: OrderType,
) {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    let mut builder = OrderTestBuilder::new(order_type);
    builder
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(BTC))
        .client_order_id(ClientOrderId::from("O-QUOTE-1"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .quote_quantity(true);
    if order_type == OrderType::Limit {
        builder.price(Price::from("50000.00"));
    }
    let order = builder.build();

    harness
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cached");
    drain_exec(&mut harness.exec_rx);

    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit accepted");

    let events = drain_exec(&mut harness.exec_rx);
    let emitted = order_events(&events);
    assert_eq!(emitted.len(), 1, "{events:?}");
    assert!(
        matches!(emitted[0], OrderEventAny::Denied(_)),
        "{:?}",
        emitted[0]
    );
    assert!(
        venue.requests_for("POST", "order").is_empty(),
        "a quote-denominated order must never reach the venue",
    );
}

// ------------------------------------------------------------------------------------------------
// F02 - unknown execution status
// ------------------------------------------------------------------------------------------------

/// Submits `order` and waits until the client stops having work in flight for it.
async fn submit_and_settle(harness: &Harness, order: &OrderAny, venue: &MockVenue) {
    harness
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cached");
    harness
        .client
        .submit_order(submit_command(order))
        .expect("submit accepted");

    wait_until_async(
        || async { !venue.requests_for("POST", "order").is_empty() },
        Duration::from_secs(5),
    )
    .await;
}

#[rstest]
#[case(-1006)]
#[case(-1007)]
#[tokio::test]
async fn test_unknown_execution_status_is_reconciled_rather_than_rejected(#[case] code: i64) {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    venue.script(|script| {
        script.submit = SubmitOutcome::AsterError {
            code,
            msg: "Execution status unknown.".to_string(),
        };
        // The order did reach the book despite the ambiguous answer.
        script.orders.insert(
            "O-AMBIG-1".to_string(),
            venue_order(900_100, "O-AMBIG-1", "BTCUSDT", "NEW", "BUY"),
        );
    });

    let order = limit_order("O-AMBIG-1", OrderSide::Buy, false);
    drain_exec(&mut harness.exec_rx);
    submit_and_settle(&harness, &order, &venue).await;

    wait_until_async(
        || async { !venue.requests_for("GET", "order").is_empty() },
        Duration::from_secs(20),
    )
    .await;

    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        !order_events(&events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Rejected(_))),
        "an unknown execution status must not terminalise the order: {events:?}",
    );
    assert_eq!(
        venue.requests_for("POST", "order").len(),
        1,
        "an ambiguous submission must never be resubmitted",
    );
    assert_eq!(
        order_reports(&events)
            .iter()
            .map(|(status, _)| *status)
            .collect::<Vec<_>>(),
        vec![OrderStatus::Accepted],
        "the reconciliation query's answer must be emitted: {events:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_client_side_timeout_on_submit_is_reconciled_rather_than_rejected() {
    let venue = MockVenue::start().await;
    script_connect(&venue);
    let mut harness = build_harness(&venue, Some(1));
    seed_account(&harness.cache);
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    venue.clear_requests();

    venue.script(|script| {
        script.submit = SubmitOutcome::Stall {
            delay: Duration::from_secs(4),
        };
        script.orders.insert(
            "O-TIMEOUT-1".to_string(),
            venue_order(900_101, "O-TIMEOUT-1", "BTCUSDT", "NEW", "BUY"),
        );
    });

    let order = limit_order("O-TIMEOUT-1", OrderSide::Buy, false);
    drain_exec(&mut harness.exec_rx);
    submit_and_settle(&harness, &order, &venue).await;

    wait_until_async(
        || async { !venue.requests_for("GET", "order").is_empty() },
        Duration::from_secs(20),
    )
    .await;

    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        !order_events(&events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Rejected(_))),
        "a transport timeout must not terminalise the order: {events:?}",
    );
    assert_eq!(venue.requests_for("POST", "order").len(), 1);
    assert!(
        order_reports(&events)
            .iter()
            .any(|(status, _)| *status == OrderStatus::Accepted),
        "{events:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_unknown_execution_status_rejects_only_when_the_venue_never_saw_the_order() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    venue.script(|script| {
        script.submit = SubmitOutcome::AsterError {
            code: -1007,
            msg: "Timeout waiting for response from backend server.".to_string(),
        };
        // `orders` stays empty, so the query answers -2013 NO_SUCH_ORDER.
    });

    let order = limit_order("O-AMBIG-2", OrderSide::Buy, false);
    drain_exec(&mut harness.exec_rx);
    submit_and_settle(&harness, &order, &venue).await;

    wait_until_async(
        || async { !venue.requests_for("GET", "order").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        order_events(&events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Rejected(_))),
        "a definitive `no such order` answer resolves the ambiguity: {events:?}",
    );
    assert_eq!(venue.requests_for("POST", "order").len(), 1);
}

#[rstest]
#[tokio::test]
async fn test_gateway_failure_on_submit_is_reconciled_rather_than_rejected() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    venue.script(|script| {
        // An edge proxy answering 502 says nothing about whether the order was forwarded.
        script.submit = SubmitOutcome::Status {
            status: 502,
            body: "<html>Bad Gateway</html>".to_string(),
        };
        script.orders.insert(
            "O-GATEWAY-1".to_string(),
            venue_order(900_102, "O-GATEWAY-1", "BTCUSDT", "NEW", "BUY"),
        );
    });

    let order = limit_order("O-GATEWAY-1", OrderSide::Buy, false);
    drain_exec(&mut harness.exec_rx);
    submit_and_settle(&harness, &order, &venue).await;

    wait_until_async(
        || async { !venue.requests_for("GET", "order").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        !order_events(&events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Rejected(_))),
        "a gateway failure must not terminalise the order: {events:?}",
    );
    assert_eq!(venue.requests_for("POST", "order").len(), 1);
}

#[rstest]
#[tokio::test]
async fn test_definitive_venue_rejection_emits_order_rejected_immediately() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    venue.script(|script| {
        script.submit = SubmitOutcome::AsterError {
            code: -2019,
            msg: "Margin is insufficient.".to_string(),
        };
    });

    let order = limit_order("O-REJECT-1", OrderSide::Buy, false);
    drain_exec(&mut harness.exec_rx);
    submit_and_settle(&harness, &order, &venue).await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        order_events(&events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Rejected(_))),
        "{events:?}",
    );
    assert!(
        venue.requests_for("GET", "order").is_empty(),
        "a definitive rejection needs no reconciliation query",
    );
}

// ------------------------------------------------------------------------------------------------
// F03 - user data stream lifecycle
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_connect_fails_when_the_first_user_stream_cannot_start() {
    let venue = MockVenue::start().await;
    script_connect(&venue);
    venue.script(|script| {
        script.listen_key_error = Some(json!({"code": -1125, "msg": "Invalid listen key."}));
    });

    let mut harness = build_harness(&venue, Some(5));
    seed_account(&harness.cache);
    harness.client.start().expect("start");

    let error = harness
        .client
        .connect()
        .await
        .expect_err("connect must fail while the private stream is down");

    assert!(error.to_string().contains("user stream"), "{error}");
    assert!(
        !harness.client.is_connected(),
        "the client must not report as connected without its private stream",
    );
}

#[rstest]
#[tokio::test]
async fn test_connect_retries_a_transport_fault_on_the_first_user_stream() {
    // A live testnet run failed here twice through this host's proxy: the handshake timed out
    // and the execution client could then never come up.
    let venue = MockVenue::start().await;
    script_connect(&venue);
    venue.script(|script| {
        script.listen_key_stalls = 2;
        script.listen_key_stall = Duration::from_secs(3);
    });

    // The listen key POST is not retried by the HTTP client, so its own 1 s timeout is what
    // surfaces the transport fault the connect retry has to absorb.
    let mut harness = build_harness_with(&venue, Some(1), Some(2));
    seed_account(&harness.cache);
    harness.client.start().expect("start");

    harness
        .client
        .connect()
        .await
        .expect("a transient transport fault must not fail connect");

    assert!(harness.client.is_connected());
    assert_eq!(
        venue.requests_for("POST", "listenKey").len(),
        3,
        "two stalled attempts, then the one that succeeds",
    );
}

#[rstest]
#[tokio::test]
async fn test_connect_does_not_retry_a_venue_answer_on_the_first_user_stream() {
    let venue = MockVenue::start().await;
    script_connect(&venue);
    venue.script(|script| {
        script.listen_key_error = Some(json!({"code": -1125, "msg": "Invalid listen key."}));
    });

    let mut harness = build_harness(&venue, Some(5));
    seed_account(&harness.cache);
    harness.client.start().expect("start");

    harness
        .client
        .connect()
        .await
        .expect_err("connect must fail");

    assert_eq!(
        venue.requests_for("POST", "listenKey").len(),
        1,
        "a venue answer would fail identically on every repeat",
    );
}

#[rstest]
#[tokio::test]
async fn test_connect_waits_for_the_user_stream_before_reporting_connected() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    assert!(harness.client.is_connected());
    assert_eq!(
        venue.ws_connection_count(),
        1,
        "the socket must be open before connect returns",
    );
}

#[rstest]
#[tokio::test]
async fn test_outage_compensation_applies_a_fill_and_a_cancel_missed_by_the_stream() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    // Two orders the client believes are working.
    venue.script(|script| {
        script.submit = SubmitOutcome::Accepted;
        script.orders.insert(
            "O-OUT-FILL".to_string(),
            venue_order(900_200, "O-OUT-FILL", "BTCUSDT", "FILLED", "BUY"),
        );
        script.orders.insert(
            "900200".to_string(),
            venue_order(900_200, "O-OUT-FILL", "BTCUSDT", "FILLED", "BUY"),
        );
        script.orders.insert(
            "O-OUT-CANCEL".to_string(),
            venue_order(900_201, "O-OUT-CANCEL", "BTCUSDT", "CANCELED", "SELL"),
        );
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(7_001, 900_200, "BTCUSDT", now_ms(), "0.02")],
        );
    });

    for (client_order_id, side) in [
        ("O-OUT-FILL", OrderSide::Buy),
        ("O-OUT-CANCEL", OrderSide::Sell),
    ] {
        let order = limit_order(client_order_id, side, false);
        submit_and_settle(&harness, &order, &venue).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain_exec(&mut harness.exec_rx);

    // The venue no longer lists either order: one filled, one was cancelled during the outage.
    venue.script(|script| script.open_orders = json!([]));
    venue.drop_ws();

    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "userTrades").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let events = drain_exec(&mut harness.exec_rx);
    let statuses: HashMap<String, OrderStatus> = order_reports(&events)
        .into_iter()
        .filter_map(|(status, id)| id.map(|id| (id.to_string(), status)))
        .collect();

    assert_eq!(
        statuses.get("O-OUT-FILL"),
        Some(&OrderStatus::Filled),
        "a fill during the outage must be reported: {events:?}",
    );
    assert_eq!(
        statuses.get("O-OUT-CANCEL"),
        Some(&OrderStatus::Canceled),
        "a cancel during the outage must be reported: {events:?}",
    );
    assert!(
        fill_trade_ids(&events).contains(&"7001".to_string()),
        "the missed fill must be applied: {events:?}",
    );
    assert!(
        !account_states(&events).is_empty(),
        "compensation must refresh the balances: {events:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_outage_compensation_does_not_reapply_a_fill_already_seen_on_the_stream() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    venue.script(|script| {
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(7_100, 900_300, "BTCUSDT", now_ms(), "0.02")],
        );
        script.orders.insert(
            "900300".to_string(),
            venue_order(900_300, "O-DEDUPE", "BTCUSDT", "FILLED", "BUY"),
        );
    });

    // The stream delivers trade 7100 first.
    let trade_ms = now_ms();
    venue.push_ws(&json!({
        "e": "ORDER_TRADE_UPDATE",
        "E": trade_ms,
        "T": trade_ms,
        "o": {
            "s": "BTCUSDT", "c": "O-DEDUPE", "S": "BUY", "o": "LIMIT", "f": "GTC",
            "q": "0.010", "p": "50000.00", "ap": "50000.00", "sp": "0",
            "x": "TRADE", "X": "FILLED", "i": 900_300i64, "l": "0.010", "z": "0.010",
            "L": "50000.00", "N": "USDT", "n": "0.02", "T": trade_ms,
            "t": 7_100i64, "m": true, "R": false, "wt": "CONTRACT_PRICE",
            "ot": "LIMIT", "ps": "BOTH", "cp": false, "rp": "0"
        }
    }));

    tokio::time::sleep(Duration::from_millis(500)).await;

    let streamed = drain_exec(&mut harness.exec_rx);
    assert!(
        fill_trade_ids(&streamed).contains(&"7100".to_string()),
        "the stream fill must arrive first: {streamed:?}",
    );

    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "userTrades").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let compensated = drain_exec(&mut harness.exec_rx);
    assert!(
        !fill_trade_ids(&compensated).contains(&"7100".to_string()),
        "a trade already applied from the stream must not be applied again: {compensated:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_startup_reconciliation_fills_are_not_replayed_by_compensation() {
    // A live run replayed a fill from a previous process as a session event, which the engine
    // rejected with `InvalidStateTrigger` because the order was already filled. Fills handed to
    // the engine's startup reconciliation must not come back as live fills after a reconnect.
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    let trade_ms = now_ms();
    venue.script(|script| {
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(8_100, 900_400, "BTCUSDT", trade_ms, "0.02")],
        );
        script.orders.insert(
            "900400".to_string(),
            venue_order(900_400, "O-STARTUP", "BTCUSDT", "FILLED", "BUY"),
        );
    });

    let reports = harness
        .client
        .generate_fill_reports(recent_fill_reports_command())
        .await
        .expect("startup reconciliation");
    assert_eq!(
        reports
            .iter()
            .map(|report| report.trade_id.to_string())
            .collect::<Vec<_>>(),
        vec!["8100".to_string()],
        "the startup path must report the fill once",
    );
    drain_exec(&mut harness.exec_rx);

    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "userTrades").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let compensated = drain_exec(&mut harness.exec_rx);
    assert!(
        !fill_trade_ids(&compensated).contains(&"8100".to_string()),
        "a trade already reported to the engine must not be re-delivered: {compensated:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_compensation_ignores_fills_from_before_the_session_started() {
    // The fill predates the connect, so it belongs to the engine's startup reconciliation.
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    venue.script(|script| {
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(
                8_200,
                900_500,
                "BTCUSDT",
                now_ms() - 600_000,
                "0.02",
            )],
        );
        script.orders.insert(
            "900500".to_string(),
            venue_order(900_500, "O-PREVIOUS", "BTCUSDT", "FILLED", "BUY"),
        );
    });
    drain_exec(&mut harness.exec_rx);

    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "userTrades").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let compensated = drain_exec(&mut harness.exec_rx);
    assert!(
        !fill_trade_ids(&compensated).contains(&"8200".to_string()),
        "a fill from before the connect must not be replayed: {compensated:?}",
    );

    let start_times: Vec<i64> = venue
        .requests_for("GET", "userTrades")
        .iter()
        .filter_map(|request| request.param("startTime").and_then(|v| v.parse().ok()))
        .collect();
    assert!(
        !start_times.is_empty() && start_times.iter().all(|start| *start > now_ms() - 300_000),
        "compensation must not query behind the session start: {start_times:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_reconciliation_paths_emit_reports_not_order_events() {
    // The adapter never constructs `OrderFilled`; the engine derives events from reports, so a
    // historical fill cannot arrive as a live event for an order already in a terminal state.
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    let trade_ms = now_ms();
    venue.script(|script| {
        script.open_orders = json!([venue_order(900_600, "O-REPORTS", "BTCUSDT", "NEW", "BUY")]);
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(8_300, 900_600, "BTCUSDT", trade_ms, "0.02")],
        );
        script.orders.insert(
            "900600".to_string(),
            venue_order(900_600, "O-REPORTS", "BTCUSDT", "FILLED", "BUY"),
        );
    });
    drain_exec(&mut harness.exec_rx);

    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "userTrades").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        fill_trade_ids(&events).contains(&"8300".to_string()),
        "the missed fill must still be delivered: {events:?}",
    );
    assert!(
        order_events(&events).is_empty(),
        "reconciliation must speak in reports only, never order events: {events:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_listen_key_expiry_rebuilds_the_session_and_compensates() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    drain_exec(&mut harness.exec_rx);

    venue.push_ws(&json!({"e": "listenKeyExpired", "E": 1_788_571_666_000i64}));

    wait_until_async(
        || async { !venue.requests_for("POST", "listenKey").is_empty() },
        Duration::from_secs(30),
    )
    .await;
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(30),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "userTrades").is_empty() },
        Duration::from_secs(30),
    )
    .await;

    assert!(
        !venue.requests_for("GET", "openOrders").is_empty(),
        "a rebuilt session must re-reconcile the open orders",
    );
    assert!(
        !venue.requests_for("GET", "positionRisk").is_empty(),
        "a rebuilt session must refresh the positions",
    );
}

// ------------------------------------------------------------------------------------------------
// F04 - report completeness
// ------------------------------------------------------------------------------------------------

/// A fill-report window covering the current instant, for fixtures timestamped from `now_ms`.
fn recent_fill_reports_command() -> GenerateFillReports {
    let now_ns = (now_ms() as u64) * 1_000_000;
    GenerateFillReports::new(
        UUID4::new(),
        UnixNanos::default(),
        None, // instrument_id
        None, // venue_order_id
        Some(UnixNanos::from(now_ns - 600_000_000_000)),
        Some(UnixNanos::from(now_ns + 60_000_000_000)),
        None, // params
        None, // correlation_id
    )
}

fn fill_reports_command() -> GenerateFillReports {
    GenerateFillReports::new(
        UUID4::new(),
        UnixNanos::default(),
        None, // instrument_id
        None, // venue_order_id
        Some(UnixNanos::from(1_788_571_000_000_000_000u64)),
        Some(UnixNanos::from(1_788_572_000_000_000_000u64)),
        None, // params
        None, // correlation_id
    )
}

#[rstest]
#[tokio::test]
async fn test_generate_fill_reports_propagates_a_failed_symbol_request() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    venue.script(|script| {
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(1, 10, "BTCUSDT", 1_788_571_663_000, "0.02")],
        );
        script.user_trades_error.insert(
            "ETHUSDT".to_string(),
            json!({"code": -1003, "msg": "Too many requests."}),
        );
    });

    let error = harness
        .client
        .generate_fill_reports(fill_reports_command())
        .await
        .expect_err("a failed source must not be reported as an empty history");

    assert!(error.to_string().contains("ETHUSDT"), "{error}");
}

#[rstest]
#[tokio::test]
async fn test_generate_fill_reports_propagates_an_unparsable_commission() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    venue.script(|script| {
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(
                1,
                10,
                "BTCUSDT",
                1_788_571_663_000,
                "not-a-number",
            )],
        );
    });

    let error = harness
        .client
        .generate_fill_reports(fill_reports_command())
        .await
        .expect_err("an unparsable fee is a hole in the history, not a fill to skip");

    let chain = format!("{error:#}");
    assert!(chain.contains("commission"), "{chain}");
    assert!(chain.contains("BTCUSDT"), "{chain}");
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_reports_propagates_a_failed_request() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    venue.script(|script| {
        script.all_orders.insert(
            "BTCUSDT".to_string(),
            vec![venue_order(1, "O-1", "BTCUSDT", "FILLED", "BUY")],
        );
        script.all_orders_error.insert(
            "ETHUSDT".to_string(),
            json!({"code": -1003, "msg": "Too many requests."}),
        );
    });

    let cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        false, // open_only
        None,  // instrument_id
        Some(UnixNanos::from(1_788_571_000_000_000_000u64)),
        Some(UnixNanos::from(1_788_572_000_000_000_000u64)),
        None, // params
        None, // correlation_id
    );

    let error = harness
        .client
        .generate_order_status_reports(&cmd)
        .await
        .expect_err("a failed source must not be reported as an empty history");

    assert!(error.to_string().contains("ETHUSDT"), "{error}");
}

#[rstest]
#[tokio::test]
async fn test_generate_position_status_reports_propagates_an_unparsable_quantity() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    venue.script(|script| {
        script.position_risk = json!([{
            "symbol": "BTCUSDT",
            "positionAmt": "not-a-number",
            "entryPrice": "50000.0",
            "positionSide": "BOTH",
            "updateTime": 1_788_571_663_397i64,
        }]);
    });

    let cmd = GeneratePositionStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        None, // instrument_id
        None, // start
        None, // end
        None, // params
        None, // correlation_id
    );

    let error = harness
        .client
        .generate_position_status_reports(&cmd)
        .await
        .expect_err("a silently dropped position reads as flat downstream");

    assert!(error.to_string().contains("BTCUSDT"), "{error}");
}

// ------------------------------------------------------------------------------------------------
// F05 - side-filtered cancellation
// ------------------------------------------------------------------------------------------------

fn cancel_all(order_side: Option<OrderSide>) -> CancelAllOrders {
    CancelAllOrders::new(
        TraderId::from("TESTER-001"),
        Some(*ASTER_CLIENT_ID),
        StrategyId::from("S-001"),
        InstrumentId::from(BTC),
        order_side,
        UUID4::new(),
        UnixNanos::default(),
        None, // params
        None, // correlation_id
    )
}

fn script_two_sided_book(venue: &MockVenue) {
    venue.script(|script| {
        script.open_orders = json!([
            venue_order(1_001, "O-BUY-1", "BTCUSDT", "NEW", "BUY"),
            venue_order(1_002, "O-SELL-1", "BTCUSDT", "NEW", "SELL"),
            venue_order(1_003, "O-BUY-2", "BTCUSDT", "NEW", "BUY"),
            venue_order(2_001, "O-ETH-BUY", "ETHUSDT", "NEW", "BUY"),
        ]);
    });
}

#[rstest]
#[case(OrderSide::Buy, vec!["1001", "1003"])]
#[case(OrderSide::Sell, vec!["1002"])]
#[tokio::test]
async fn test_cancel_all_orders_with_a_side_cancels_only_that_side(
    #[case] side: OrderSide,
    #[case] expected: Vec<&str>,
) {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;
    script_two_sided_book(&venue);

    harness
        .client
        .cancel_all_orders(cancel_all(Some(side)))
        .expect("accepted");

    wait_until_async(
        || async { venue.requests_for("DELETE", "order").len() == expected.len() },
        Duration::from_secs(10),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut cancelled: Vec<String> = venue
        .requests_for("DELETE", "order")
        .iter()
        .filter_map(|request| request.param("orderId").map(str::to_string))
        .collect();
    cancelled.sort();

    assert_eq!(cancelled, expected);
    assert!(
        venue.requests_for("DELETE", "allOpenOrders").is_empty(),
        "a side-filtered command must not use the symbol-wide cancel",
    );
}

#[rstest]
#[tokio::test]
async fn test_cancel_all_orders_without_a_side_uses_the_symbol_wide_endpoint() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;
    script_two_sided_book(&venue);

    harness
        .client
        .cancel_all_orders(cancel_all(None))
        .expect("accepted");

    wait_until_async(
        || async { !venue.requests_for("DELETE", "allOpenOrders").is_empty() },
        Duration::from_secs(10),
    )
    .await;

    assert!(venue.requests_for("DELETE", "order").is_empty());
}

#[rstest]
#[tokio::test]
async fn test_cancel_all_orders_with_a_side_and_no_match_sends_nothing() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;
    venue.script(|script| {
        script.open_orders = json!([venue_order(1_002, "O-SELL-1", "BTCUSDT", "NEW", "SELL")]);
    });

    harness
        .client
        .cancel_all_orders(cancel_all(Some(OrderSide::Buy)))
        .expect("accepted");

    wait_until_async(
        || async { !venue.requests_for("GET", "openOrders").is_empty() },
        Duration::from_secs(10),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(venue.requests_for("DELETE", "order").is_empty());
    assert!(venue.requests_for("DELETE", "allOpenOrders").is_empty());
}

// ------------------------------------------------------------------------------------------------
// F07 - pagination
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_fill_reports_page_past_the_venue_limit_and_deduplicate() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    // 2500 trades, with the 1000/1001 boundary sharing one millisecond, plus a duplicate row.
    let mut trades: Vec<Value> = (0..2_500i64)
        .map(|index| {
            let time = if (999..=1_001).contains(&index) {
                1_788_571_663_500i64
            } else {
                1_788_571_663_000 + index
            };
            venue_trade(index + 1, 900_000 + index, "BTCUSDT", time, "0.02")
        })
        .collect();
    trades.push(trades[1_500].clone());

    venue.script(|script| {
        script.user_trades.insert("BTCUSDT".to_string(), trades);
    });

    let reports = harness
        .client
        .generate_fill_reports(fill_reports_command())
        .await
        .expect("pagination must complete");

    assert_eq!(
        reports.len(),
        2_500,
        "every page must be collected exactly once"
    );

    let requests = venue.requests_for("GET", "userTrades");
    let btc_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.param("symbol") == Some("BTCUSDT"))
        .collect();
    assert!(
        btc_requests.len() >= 3,
        "2500 rows need at least three pages, saw {}",
        btc_requests.len(),
    );
    assert!(
        btc_requests
            .iter()
            .all(|request| request.param("limit") == Some(&PAGE_LIMIT.to_string())),
        "every page must request the venue maximum",
    );
    assert!(
        btc_requests[1..].iter().all(
            |request| request.param("fromId").is_some() && request.param("startTime").is_none()
        ),
        "continuation pages must use the documented cursor without a time window",
    );

    let mut ids: Vec<String> = reports
        .iter()
        .map(|report| report.trade_id.to_string())
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 2_500, "duplicated rows must be dropped");
}

#[rstest]
#[tokio::test]
async fn test_order_status_reports_page_past_the_venue_limit() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    let orders: Vec<Value> = (0..1_200i64)
        .map(|index| {
            let mut order =
                venue_order(index + 1, &format!("O-{index}"), "BTCUSDT", "FILLED", "BUY");
            order["time"] = json!(1_788_571_663_000i64 + index);
            order["updateTime"] = json!(1_788_571_663_000i64 + index);
            order
        })
        .collect();

    venue.script(|script| {
        script.all_orders.insert("BTCUSDT".to_string(), orders);
    });

    let cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        false, // open_only
        None,  // instrument_id
        Some(UnixNanos::from(1_788_571_000_000_000_000u64)),
        Some(UnixNanos::from(1_788_572_000_000_000_000u64)),
        None, // params
        None, // correlation_id
    );

    let reports = harness
        .client
        .generate_order_status_reports(&cmd)
        .await
        .expect("pagination must complete");

    assert_eq!(reports.len(), 1_200);

    let btc_requests: Vec<_> = venue
        .requests_for("GET", "allOrders")
        .into_iter()
        .filter(|request| request.param("symbol") == Some("BTCUSDT"))
        .collect();
    assert!(btc_requests.len() >= 2, "1200 rows need at least two pages");
    assert!(
        btc_requests[1..]
            .iter()
            .all(|request| request.param("orderId").is_some()
                && request.param("endTime").is_none()),
        "continuation pages must use the `orderId` cursor without a time window",
    );
}

#[rstest]
#[tokio::test]
async fn test_fill_report_pagination_stops_on_an_empty_history() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    let reports = harness
        .client
        .generate_fill_reports(fill_reports_command())
        .await
        .expect("an empty history is complete");

    assert!(reports.is_empty());
    assert_eq!(
        venue
            .requests_for("GET", "userTrades")
            .iter()
            .filter(|request| request.param("symbol") == Some("BTCUSDT"))
            .count(),
        1,
        "an empty first page ends the pagination",
    );
}

// ------------------------------------------------------------------------------------------------
// F08 - zero balances
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_rest_zero_balance_clears_the_cached_amount() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    let connect_states = drain_exec(&mut harness.exec_rx);
    let initial = account_states(&connect_states)
        .last()
        .expect("connect emits an account state")
        .balances
        .clone();
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0].total, Money::from("1000.0 USDT"));

    venue.script(|script| {
        script.balances = json!([
            {"asset": "USDT", "balance": "0.0", "availableBalance": "0.0"},
            {"asset": "BTC", "balance": "0.5", "availableBalance": "0.5"},
        ]);
    });

    let mut account = MarginAccount::new(
        AccountState::new(
            AccountId::from(ACCOUNT_ID),
            AccountType::Margin,
            initial,
            vec![],
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            None,
        ),
        true,
    );

    harness
        .client
        .query_account(nautilus_common::messages::execution::QueryAccount::new(
            TraderId::from("TESTER-001"),
            Some(*ASTER_CLIENT_ID),
            AccountId::from(ACCOUNT_ID),
            UUID4::new(),
            UnixNanos::default(),
            None, // params
            None, // correlation_id
        ))
        .expect("accepted");

    wait_until_async(
        || async { !venue.requests_for("GET", "balance").is_empty() },
        Duration::from_secs(10),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_exec(&mut harness.exec_rx);
    let refreshed = account_states(&events)
        .last()
        .expect("query_account emits an account state")
        .balances
        .clone();

    account.base.update_balances(&refreshed);

    assert_eq!(
        account.balance_total(Some(Currency::USDT())).unwrap(),
        Money::new(0.0, Currency::USDT()),
        "the explicit zero row must clear the cached amount",
    );
    assert_eq!(
        account.balance_total(Some(Currency::BTC())).unwrap(),
        Money::new(0.5, Currency::BTC()),
    );
}

#[rstest]
#[tokio::test]
async fn test_stream_zero_balance_clears_the_cached_amount() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    drain_exec(&mut harness.exec_rx);

    venue.push_ws(&json!({
        "e": "ACCOUNT_UPDATE",
        "E": 1_788_571_667_000i64,
        "T": 1_788_571_667_000i64,
        "a": {
            "m": "WITHDRAW",
            "B": [
                {"a": "USDT", "wb": "0.00000000", "cw": "0.00000000", "bc": "-1000.0"},
                {"a": "BTC", "wb": "0.50000000", "cw": "0.50000000", "bc": "0"}
            ],
            "P": []
        }
    }));

    tokio::time::sleep(Duration::from_millis(500)).await;

    let events = drain_exec(&mut harness.exec_rx);
    let state = account_states(&events)
        .last()
        .copied()
        .expect("the stream account update must reach the engine");

    let usdt = state
        .balances
        .iter()
        .find(|balance| balance.currency == Currency::USDT())
        .expect("the zero row must survive the parser");
    assert!(usdt.total.as_decimal().is_zero());

    let btc = state
        .balances
        .iter()
        .find(|balance| balance.currency == Currency::BTC())
        .expect("other assets must be untouched");
    assert_eq!(btc.total, Money::from("0.5 BTC"));
}

// ------------------------------------------------------------------------------------------------
// Fees
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_connect_publishes_instruments_with_the_account_commission_rates() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    let mut published = HashMap::new();
    while let Ok(event) = harness.data_rx.try_recv() {
        if let DataEvent::Instrument(instrument) = event {
            published.insert(instrument.id().to_string(), instrument);
        }
    }

    for id in [BTC, ETH] {
        let instrument = published
            .get(id)
            .unwrap_or_else(|| panic!("{id} must be re-published with real fees: {published:?}"));
        assert_eq!(
            instrument.maker_fee().to_string(),
            ASTER_MAKER,
            "the maker fee must come from commissionRate, not the Binance default",
        );
        assert_eq!(instrument.taker_fee().to_string(), ASTER_TAKER);
        assert_ne!(instrument.maker_fee().to_string(), BINANCE_DEFAULT_MAKER);
        assert_ne!(instrument.taker_fee().to_string(), BINANCE_DEFAULT_TAKER);
    }

    assert!(
        !venue
            .requests()
            .iter()
            .any(|request| request.path == "commissionRate"),
        "the rates were queried during connect, before the recorded requests were cleared",
    );
}

#[rstest]
#[tokio::test]
async fn test_commission_rate_failure_leaves_the_venue_default_unpublished() {
    let venue = MockVenue::start().await;
    script_connect(&venue);
    venue.script(|script| {
        // Aster's testnet does not list every mainnet symbol; the query answers -1121 there.
        script.commission_rates.remove("ETHUSDT");
    });

    let mut harness = build_harness(&venue, Some(30));
    seed_account(&harness.cache);
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let mut published = HashMap::new();
    while let Ok(event) = harness.data_rx.try_recv() {
        if let DataEvent::Instrument(instrument) = event {
            published.insert(instrument.id().to_string(), instrument);
        }
    }

    assert!(
        published.contains_key(BTC),
        "the symbol with a real rate is still published",
    );
    assert!(
        !published.contains_key(ETH),
        "a failed rate query must not publish a fabricated fee",
    );
    assert_eq!(
        venue
            .requests()
            .iter()
            .filter(|request| request.path == "commissionRate")
            .count(),
        2,
        "every loaded instrument is queried once",
    );
}

// ------------------------------------------------------------------------------------------------
// Startup reconciliation against the real execution manager
// ------------------------------------------------------------------------------------------------

/// Captures `log` records so a test can assert on what the execution engine reported.
struct LogCapture {
    records: parking_lot::Mutex<Vec<(Level, String)>>,
}

impl LogCapture {
    fn records(&self) -> Vec<(Level, String)> {
        self.records.lock().clone()
    }
}

impl Log for LogCapture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Warn
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            self.records
                .lock()
                .push((record.level(), record.args().to_string()));
        }
    }

    fn flush(&self) {}
}

static LOG_CAPTURE: LogCapture = LogCapture {
    records: parking_lot::Mutex::new(Vec::new()),
};
static INSTALL_LOG_CAPTURE: std::sync::Once = std::sync::Once::new();

fn install_log_capture() -> &'static LogCapture {
    INSTALL_LOG_CAPTURE.call_once(|| {
        let _ = log::set_logger(&LOG_CAPTURE);
        log::set_max_level(LevelFilter::Warn);
    });
    &LOG_CAPTURE
}

/// Scripts two historical filled orders with their trades, as a real account carries them.
fn script_historical_fills(venue: &MockVenue) -> i64 {
    let base_ms = now_ms() - 600_000;

    venue.script(|script| {
        script.all_orders.insert(
            "BTCUSDT".to_string(),
            vec![
                filled_order(
                    910_001,
                    "O-HIST-1",
                    "BTCUSDT",
                    "BUY",
                    base_ms + 500,
                    base_ms + 10,
                ),
                filled_order(
                    910_002,
                    "NTT2X7HGbrBkQhMo4FnvXc",
                    "BTCUSDT",
                    "SELL",
                    base_ms + 20,
                    base_ms + 25,
                ),
                ioc_expired_order(
                    910_003,
                    "O-IOC-1",
                    "BTCUSDT",
                    "BUY",
                    base_ms + 30,
                    base_ms + 35,
                ),
                market_order(
                    910_004,
                    "MANUALMARKET1",
                    "BTCUSDT",
                    "SELL",
                    base_ms + 40,
                    base_ms + 45,
                ),
                timeless_order(910_005, "O-20260905-023725-001-000-2", "BTCUSDT", "BUY"),
            ],
        );
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![
                venue_trade(34_264_618, 910_001, "BTCUSDT", base_ms + 10, "0.02"),
                venue_trade(34_264_619, 910_002, "BTCUSDT", base_ms + 25, "0.02"),
                venue_trade(34_264_620, 910_003, "BTCUSDT", base_ms + 35, "0.02"),
                venue_trade(34_264_621, 910_004, "BTCUSDT", base_ms + 45, "0.02"),
                venue_trade(34_264_622, 910_005, "BTCUSDT", base_ms + 55, "0.02"),
                // A fill whose order falls outside the reported window.
                venue_trade(34_264_623, 999_999, "BTCUSDT", base_ms + 60, "0.02"),
            ],
        );
        // A real account carries an open position alongside its history.
        script.position_risk = json!([{
            "symbol": "BTCUSDT",
            "positionAmt": "0.010",
            "entryPrice": "50000.0",
            "positionSide": "BOTH",
            "updateTime": base_ms + 25,
        }]);
    });

    base_ms
}

/// A fully filled historical order, as `allOrders` reports one.
fn filled_order(
    order_id: i64,
    client_order_id: &str,
    symbol: &str,
    side: &str,
    time_ms: i64,
    update_ms: i64,
) -> Value {
    json!({
        "symbol": symbol,
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "price": "50000.00",
        "avgPrice": "50000.00",
        "origQty": "0.010",
        "executedQty": "0.010",
        "cumQuote": "500.0",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": side,
        "positionSide": "BOTH",
        "reduceOnly": false,
        "closePosition": false,
        "time": time_ms,
        "updateTime": update_ms,
    })
}

/// An `IOC` order whose remainder Aster reports as `EXPIRED` after a partial fill.
fn ioc_expired_order(
    order_id: i64,
    client_order_id: &str,
    symbol: &str,
    side: &str,
    time_ms: i64,
    update_ms: i64,
) -> Value {
    json!({
        "symbol": symbol,
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "price": "50000.00",
        "avgPrice": "50000.00",
        "origQty": "0.020",
        "executedQty": "0.010",
        "cumQuote": "500.0",
        "status": "EXPIRED",
        "timeInForce": "IOC",
        "type": "LIMIT",
        "side": side,
        "positionSide": "BOTH",
        "reduceOnly": false,
        "closePosition": false,
        "time": time_ms,
        "updateTime": update_ms,
    })
}

/// A historical row Aster returns without a `time` field, so the report has no venue creation
/// time to anchor its acceptance to.
fn timeless_order(order_id: i64, client_order_id: &str, symbol: &str, side: &str) -> Value {
    json!({
        "symbol": symbol,
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "price": "50000.00",
        "avgPrice": "50000.00",
        "origQty": "0.010",
        "executedQty": "0.010",
        "cumQuote": "500.0",
        "status": "FILLED",
        "timeInForce": "IOC",
        "type": "LIMIT",
        "side": side,
        "positionSide": "BOTH",
        "reduceOnly": false,
        "closePosition": false,
    })
}

/// A reduce-only `MARKET` order placed outside Nautilus, as the venue reports it.
fn market_order(
    order_id: i64,
    client_order_id: &str,
    symbol: &str,
    side: &str,
    time_ms: i64,
    update_ms: i64,
) -> Value {
    json!({
        "symbol": symbol,
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "price": "0",
        "avgPrice": "50000.00",
        "origQty": "0.010",
        "executedQty": "0.010",
        "cumQuote": "500.0",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": side,
        "positionSide": "BOTH",
        "reduceOnly": true,
        "closePosition": false,
        "time": time_ms,
        "updateTime": update_ms,
    })
}

#[rstest]
#[tokio::test]
async fn test_startup_reconciliation_of_historical_fills_is_accepted_by_the_engine() {
    // A fresh node start on an account with historical filled orders logged one
    // `InvalidStateTrigger: ... did not apply OrderFilled` per order, because the adapter
    // reported both the terminal order and its trades and the engine could not order them.
    let logger = install_log_capture();
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    script_historical_fills(&venue);

    let mass_status = harness
        .client
        .generate_mass_status(None)
        .await
        .expect("mass status")
        .expect("mass status present");

    assert_eq!(mass_status.order_reports().len(), 5, "every order reported");
    assert!(
        mass_status
            .fill_reports()
            .keys()
            .all(|venue_order_id| venue_order_id.as_str() != "999999"),
        "a fill whose order is outside the window must not be reported",
    );
    for (venue_order_id, report) in mass_status.order_reports() {
        if let Some(fills) = mass_status.fill_reports().get(&venue_order_id) {
            let earliest = fills.iter().map(|fill| fill.ts_event).min().expect("fills");
            assert!(
                report.ts_accepted <= earliest,
                "{venue_order_id}: acceptance {} must not follow its first fill {earliest}",
                report.ts_accepted,
            );
        }
    }
    assert!(
        mass_status.lookback_start().is_some(),
        "a bounded history must declare its window",
    );

    // The engine's reconciliation needs the instruments; connect published them with their real
    // commission rates, which is also what the inferred-fill commission would be costed against.
    let cache = Rc::new(RefCell::new(Cache::default()));
    seed_account(&cache);
    while let Ok(event) = harness.data_rx.try_recv() {
        if let DataEvent::Instrument(instrument) = event {
            cache
                .borrow_mut()
                .add_instrument(instrument)
                .expect("instrument");
        }
    }
    let clock = Rc::new(RefCell::new(TestClock::new()));
    let mut manager = ExecutionManager::new(
        clock.clone(),
        cache.clone(),
        ExecutionManagerConfig::default(),
    )
    .expect("manager");
    let mut engine = ExecutionEngine::new(clock, cache.clone(), None);
    engine
        .register_client(Box::new(harness.client))
        .expect("register");
    engine.register_oms_type(StrategyId::from("EXTERNAL"), OmsType::Netting);

    let before = logger.records().len();
    manager
        .reconcile_execution_mass_status(mass_status, Rc::new(RefCell::new(engine)))
        .await;

    let rejected: Vec<String> = logger
        .records()
        .into_iter()
        .skip(before)
        .filter(|(_, message)| message.contains("InvalidStateTrigger"))
        .map(|(_, message)| message)
        .collect();

    assert!(
        rejected.is_empty(),
        "reconciling historical fills must not produce rejected transitions: {rejected:#?}",
    );

    // The orders must still be reconstructed and fully filled, not merely silent.
    let cache_ref = cache.borrow();
    for client_order_id in [
        "O-HIST-1",
        "NTT2X7HGbrBkQhMo4FnvXc",
        "MANUALMARKET1",
        "O-20260905-023725-001-000-2",
    ] {
        let order = cache_ref
            .order(&ClientOrderId::from(client_order_id))
            .unwrap_or_else(|| panic!("{client_order_id} must be reconstructed"));
        assert_eq!(order.status(), OrderStatus::Filled, "{client_order_id}");
        assert_eq!(
            order.filled_qty(),
            Quantity::from("0.010"),
            "{client_order_id} must carry its real fill",
        );
    }
}
