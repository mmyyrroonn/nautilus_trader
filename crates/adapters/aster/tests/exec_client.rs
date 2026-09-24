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
            CancelAllOrders, CancelOrder, ExecutionReport, GenerateFillReports,
            GenerateOrderStatusReports, GeneratePositionStatusReports, SubmitOrder,
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
    enums::{AccountType, OmsType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce},
    events::{AccountState, OrderEventAny},
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, StrategyId, Symbol, TraderId, Venue, VenueOrderId,
    },
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

/// Builds a harness pinned to its own Nautilus venue.
///
/// The account fee registry is process-wide and keyed by venue, so a test that asserts on it
/// needs a venue the rest of this binary does not touch.
fn build_harness_with_venue(mock: &MockVenue, venue: Venue) -> Harness {
    build_harness_inner(mock, Some(30), None, Some(venue))
}

fn build_harness_with(
    venue: &MockVenue,
    http_timeout_secs: Option<u64>,
    ws_connect_timeout_secs: Option<u64>,
) -> Harness {
    build_harness_inner(venue, http_timeout_secs, ws_connect_timeout_secs, None)
}

fn build_harness_inner(
    venue: &MockVenue,
    http_timeout_secs: Option<u64>,
    ws_connect_timeout_secs: Option<u64>,
    venue_override: Option<Venue>,
) -> Harness {
    let account_id = AccountId::from(ACCOUNT_ID);
    let cache = Rc::new(RefCell::new(Cache::default()));

    let resolved_venue = venue_override.unwrap_or(*ASTER_VENUE);
    let core = ExecutionClientCore::new(
        TraderId::from("TESTER-001"),
        *ASTER_CLIENT_ID,
        resolved_venue,
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
        venue: venue_override,
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
    // Compensation never reaches behind the instant the client connected, and these tests
    // timestamp trades at `now_ms()` minus a few milliseconds. Let the session age past that
    // margin before handing the harness over, so a script that runs a millisecond later still
    // places every trade inside the session.
    tokio::time::sleep(Duration::from_millis(50)).await;
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

/// Drains events until `done` holds or `timeout` passes, returning everything drained.
///
/// Recovery passes publish when they finish, and under load that can be later than any fixed
/// delay; a test that sleeps and then drains asserts on how fast the machine is, not on what
/// the client did. Waiting for the expected event keeps the assertion about the behaviour.
async fn wait_for_events(
    harness: &mut Harness,
    timeout: Duration,
    mut done: impl FnMut(&[ExecutionEvent]) -> bool,
) -> Vec<ExecutionEvent> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut events = Vec::new();
    loop {
        events.extend(drain_exec(&mut harness.exec_rx));
        if done(&events) || tokio::time::Instant::now() >= deadline {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
    script_connect(&venue);
    // The stream row for BTC is merged against a verified snapshot, so the connect has to
    // leave one that names BTC as well as USDT.
    venue.script(|script| {
        script.balances = json!([
            {"asset": "USDT", "balance": "1000.0", "availableBalance": "1000.0"},
            {"asset": "BTC", "balance": "0.5", "availableBalance": "0.5"},
        ]);
    });
    let mut harness = build_harness(&venue, Some(30));
    seed_account(&harness.cache);
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    venue.clear_requests();
    drain_exec(&mut harness.exec_rx);

    // The stream update carries no available amount, so an owed snapshot follows it; the
    // account it then reads has already seen the withdrawal the frame states.
    venue.script(|script| {
        script.balances = json!([
            {"asset": "USDT", "balance": "0.0", "availableBalance": "0.0"},
            {"asset": "BTC", "balance": "0.5", "availableBalance": "0.5"},
        ]);
    });

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

/// A burst of stream updates inside the success window costs one balance read, and the session
/// timer takes it when the window expires even though no further message arrives.
#[rstest]
#[tokio::test]
async fn test_stream_updates_within_the_window_cost_one_snapshot_read() {
    let venue = MockVenue::start().await;
    script_connect(&venue);
    venue.script(|script| {
        script.balances = json!([
            {"asset": "USDT", "balance": "1000.0", "availableBalance": "1000.0"},
            {"asset": "BTC", "balance": "0.5", "availableBalance": "0.5"},
        ]);
    });
    let mut harness = build_harness(&venue, Some(30));
    seed_account(&harness.cache);
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    venue.clear_requests();
    drain_exec(&mut harness.exec_rx);

    // The snapshot a later read would get states a different available amount than the connect
    // did, so a read that happened can be told from one that did not.
    venue.script(|script| {
        script.balances = json!([
            {"asset": "USDT", "balance": "1000.0", "availableBalance": "900.0"},
            {"asset": "BTC", "balance": "0.5", "availableBalance": "0.5"},
        ]);
    });

    for nonce in 0..3 {
        venue.push_ws(&json!({
            "e": "ACCOUNT_UPDATE",
            "E": 1_788_571_667_000i64 + nonce,
            "T": 1_788_571_667_000i64 + nonce,
            "a": {
                "m": "ORDER",
                "B": [{"a": "USDT", "wb": "1000.00000000", "cw": "1000.00000000", "bc": "0"}],
                "P": []
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        venue.requests_for("GET", "balance").len(),
        0,
        "the success window keeps a burst of updates from turning into a read each",
    );

    // No further message arrives: the session timer is what has to take the owed snapshot once
    // the window expires.
    let mut reads = 0;
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        reads = venue.requests_for("GET", "balance").len();
        if reads > 0 {
            break;
        }
    }
    assert!(
        reads >= 1,
        "the timer compensates for the owed snapshot without another stream message",
    );

    let events = drain_exec(&mut harness.exec_rx);
    let state = account_states(&events)
        .last()
        .copied()
        .expect("the owed snapshot is published");
    let usdt = state
        .balances
        .iter()
        .find(|balance| balance.currency == Currency::USDT())
        .expect("the snapshot carries USDT");
    assert_eq!(
        usdt.free,
        Money::from("900.0 USDT"),
        "the snapshot is what corrected the carried bound",
    );
}

/// A stream row that restates the same wallet balance still leaves the available split
/// unverified, so a balance response read before it cannot clear the debt: the timer has to
/// read again even though no further message arrives.
#[rstest]
#[tokio::test]
async fn test_a_delayed_snapshot_cannot_clear_the_debt_a_newer_stream_row_raised() {
    let venue = MockVenue::start().await;
    script_connect(&venue);
    venue.script(|script| {
        script.balances = json!([
            {"asset": "USDT", "balance": "1000.0", "availableBalance": "1000.0"},
            {"asset": "BTC", "balance": "0.5", "availableBalance": "0.5"},
        ]);
    });
    let mut harness = build_harness(&venue, Some(30));
    seed_account(&harness.cache);
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    venue.clear_requests();
    drain_exec(&mut harness.exec_rx);

    // The next balance read is answered late, with the account as it was when the read began.
    venue.script(|script| {
        script.balance_stalls = 1;
        script.balance_stall = Duration::from_millis(1500);
    });

    // The explicit query is the read that is in flight while the stream row lands.
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
        || async { venue.requests_for("GET", "balance").len() == 1 },
        Duration::from_secs(10),
    )
    .await;

    // A newer row restates the same wallet balance: the bound does not move, but the split is
    // no longer verified, and the read in flight cannot answer for it.
    venue.push_ws(&json!({
        "e": "ACCOUNT_UPDATE",
        "E": 1_788_571_667_000i64,
        "T": 1_788_571_667_000i64,
        "a": {
            "m": "ORDER",
            "B": [{"a": "USDT", "wb": "1000.00000000", "cw": "1000.00000000", "bc": "0"}],
            "P": []
        }
    }));

    // The venue has moved on since the read began; only a read that starts after the row can
    // see it.
    venue.script(|script| {
        script.balances = json!([
            {"asset": "USDT", "balance": "1000.0", "availableBalance": "900.0"},
            {"asset": "BTC", "balance": "0.5", "availableBalance": "0.5"},
        ]);
    });

    // The delayed response lands and must not clear the debt. No further stream message
    // arrives, so the session timer is what has to take the owed snapshot.
    let mut reads = 0;
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        reads = venue.requests_for("GET", "balance").len();
        if reads >= 2 {
            break;
        }
    }
    assert!(
        reads >= 2,
        "the row that arrived during the read keeps the debt the timer has to settle",
    );

    let events = drain_exec(&mut harness.exec_rx);
    let state = account_states(&events)
        .last()
        .copied()
        .expect("the owed snapshot is published");
    let usdt = state
        .balances
        .iter()
        .find(|balance| balance.currency == Currency::USDT())
        .expect("the snapshot carries USDT");
    assert_eq!(
        usdt.free,
        Money::from("900.0 USDT"),
        "the snapshot taken after the row is what corrects the carried bound",
    );
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

/// Returns the reconciliation complaints logged since `since`, for one instrument.
///
/// The `log` facade takes a single process-wide logger, so this capture also sees whatever the
/// other tests in this binary are logging in parallel. Records are therefore narrowed to the
/// instrument under test, which is unique per test; every reconciliation complaint that matters
/// here names its instrument (`Bounded reconciliation does not explain the reported position for
/// {instrument}`, `InvalidStateTrigger: ... instrument_id={instrument}`).
///
/// The `no registered endpoint` records are excluded as a property of the rig, not the adapter:
/// this harness wires an engine and a manager but no portfolio, so published order events find
/// no `Portfolio.update_order` endpoint. The live node registers one.
fn reconciliation_complaints(
    logger: &'static LogCapture,
    since: usize,
    instrument_id: &str,
) -> Vec<String> {
    logger
        .records()
        .into_iter()
        .skip(since)
        .filter(|(level, _)| *level <= Level::Warn)
        .filter(|(_, message)| !message.contains("no registered endpoint"))
        .filter(|(_, message)| message.contains(instrument_id))
        .map(|(level, message)| format!("[{level}] {message}"))
        .collect()
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
                sized_trade(34_264_618, 910_001, "BTCUSDT", base_ms + 10, "0.010", "BUY"),
                sized_trade(
                    34_264_619,
                    910_002,
                    "BTCUSDT",
                    base_ms + 25,
                    "0.010",
                    "SELL",
                ),
                sized_trade(34_264_620, 910_003, "BTCUSDT", base_ms + 35, "0.010", "BUY"),
                sized_trade(
                    34_264_621,
                    910_004,
                    "BTCUSDT",
                    base_ms + 45,
                    "0.010",
                    "SELL",
                ),
                sized_trade(34_264_622, 910_005, "BTCUSDT", base_ms + 55, "0.010", "BUY"),
                // Filled inside the window, but its order was created before it, so
                // `allOrders` never returns the order and only the trade shows up.
                sized_trade(34_264_623, 999_999, "BTCUSDT", base_ms + 60, "0.010", "BUY"),
            ],
        );
        // The venue still answers for that order by ID; `allOrders` filters on creation time.
        let mut old_order = filled_order(
            999_999,
            "O-BEFORE-WINDOW",
            "BTCUSDT",
            "BUY",
            base_ms - 600_000,
            base_ms + 60,
        );
        old_order["origQty"] = json!("0.010");
        old_order["executedQty"] = json!("0.010");
        script.orders.insert("999999".to_string(), old_order);
        // A real account carries an open position alongside its history: the five windowed
        // orders net to +0.010 and the pre-window order adds another +0.010.
        script.position_risk = json!([{
            "symbol": "BTCUSDT",
            "positionAmt": "0.020",
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

    assert_eq!(
        mass_status.order_reports().len(),
        6,
        "five orders from the window plus the one fetched for a fill that predates it",
    );
    assert!(
        mass_status
            .fill_reports()
            .keys()
            .any(|venue_order_id| venue_order_id.as_str() == "999999"),
        "a real trade inside the window must be kept, whatever its order's creation time",
    );
    assert!(
        mass_status
            .order_reports()
            .contains_key(&VenueOrderId::new("999999")),
        "the order behind that trade must be fetched by ID and linked",
    );
    assert!(
        mass_status.reports_complete(),
        "every fill was linked, so the snapshot is complete",
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

    let rejected = reconciliation_complaints(logger, before, BTC);

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
        "O-BEFORE-WINDOW",
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

/// A filled `IOC` buy, as the probe leaves behind on each run.
fn ioc_buy(order_id: i64, client_order_id: &str, symbol: &str, time_ms: i64, qty: &str) -> Value {
    json!({
        "symbol": symbol,
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "price": "50000.00",
        "avgPrice": "50000.00",
        "origQty": qty,
        "executedQty": qty,
        "cumQuote": "150.0",
        "status": "FILLED",
        "timeInForce": "IOC",
        "type": "LIMIT",
        "side": "BUY",
        "positionSide": "BOTH",
        "reduceOnly": false,
        "closePosition": false,
        "time": time_ms,
        "updateTime": time_ms,
    })
}

/// A manual reduce-only `MARKET` sell placed outside Nautilus to flatten the account.
fn manual_reduce_only_sell(
    order_id: i64,
    client_order_id: &str,
    symbol: &str,
    time_ms: i64,
    qty: &str,
) -> Value {
    json!({
        "symbol": symbol,
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "price": "0",
        "avgPrice": "50000.00",
        "origQty": qty,
        "executedQty": qty,
        "cumQuote": "600.0",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": "SELL",
        "positionSide": "BOTH",
        "reduceOnly": true,
        "closePosition": false,
        "time": time_ms,
        "updateTime": time_ms,
    })
}

fn sized_trade(id: i64, order_id: i64, symbol: &str, time: i64, qty: &str, side: &str) -> Value {
    json!({
        "symbol": symbol,
        "id": id,
        "orderId": order_id,
        "price": "50000.00",
        "qty": qty,
        "quoteQty": "150.0",
        "realizedPnl": "0",
        "side": side,
        "positionSide": "BOTH",
        "maker": false,
        "buyer": side == "BUY",
        "commission": "0.02",
        "commissionAsset": "USDT",
        "time": time,
    })
}

#[rstest]
#[tokio::test]
async fn test_flat_account_with_pre_session_history_reconciles_without_complaint() {
    // The live account after a probe run: five IOC buys of 0.003 and two manual reduce-only
    // sells (0.012 then 0.003) that flatten it again. `positionRisk` then lists nothing for the
    // instrument, and the engine's bounded-window check had no expected quantity to confirm the
    // fills against, so it logged
    // "Bounded reconciliation does not explain the reported position ...".
    let logger = install_log_capture();
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    let base_ms = now_ms() - 600_000;
    venue.script(|script| {
        let mut orders = Vec::new();
        let mut trades = Vec::new();

        // Four buys, then the 0.012 flattening sell, then a fifth buy and its 0.003 sell.
        for index in 0..4i64 {
            let time_ms = base_ms + index * 1_000;
            orders.push(ioc_buy(
                920_000 + index,
                &format!("O-PROBE-{index}"),
                "ETHUSDT",
                time_ms,
                "0.003",
            ));
            trades.push(sized_trade(
                35_000 + index,
                920_000 + index,
                "ETHUSDT",
                time_ms,
                "0.003",
                "BUY",
            ));
        }

        orders.push(manual_reduce_only_sell(
            920_100,
            "NTT2X7HGbrBkQhMo4FnvXc",
            "ETHUSDT",
            base_ms + 10_000,
            "0.012",
        ));
        trades.push(sized_trade(
            35_100,
            920_100,
            "ETHUSDT",
            base_ms + 10_000,
            "0.012",
            "SELL",
        ));

        orders.push(ioc_buy(
            920_004,
            "O-PROBE-4",
            "ETHUSDT",
            base_ms + 20_000,
            "0.003",
        ));
        trades.push(sized_trade(
            35_004,
            920_004,
            "ETHUSDT",
            base_ms + 20_000,
            "0.003",
            "BUY",
        ));

        orders.push(manual_reduce_only_sell(
            920_101,
            "NTT3MANUALSELL2",
            "ETHUSDT",
            base_ms + 30_000,
            "0.003",
        ));
        trades.push(sized_trade(
            35_101,
            920_101,
            "ETHUSDT",
            base_ms + 30_000,
            "0.003",
            "SELL",
        ));

        script.all_orders.insert("ETHUSDT".to_string(), orders);
        script.user_trades.insert("ETHUSDT".to_string(), trades);
        // The account is flat, so the venue lists no position for the instrument.
        script.position_risk = json!([]);
    });

    let mass_status = harness
        .client
        .generate_mass_status(None)
        .await
        .expect("mass status")
        .expect("mass status present");

    assert_eq!(
        mass_status.order_reports().len(),
        7,
        "seven historical orders"
    );
    let eth = InstrumentId::from(ETH);
    let position_reports = mass_status.position_reports();
    let reported = position_reports
        .get(&eth)
        .and_then(|reports| reports.first())
        .expect("a traded instrument must carry a position row even when flat");
    assert_eq!(reported.position_side, PositionSide::Flat);
    assert!(reported.signed_decimal_qty.is_zero());

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

    let complaints = reconciliation_complaints(logger, before, ETH);

    assert!(
        complaints.is_empty(),
        "a flat account with pre-session history must reconcile silently: {complaints:#?}",
    );

    // Every order is still reconstructed, and the account is still flat.
    let cache_ref = cache.borrow();
    assert_eq!(
        cache_ref.orders(None, Some(&eth), None, None, None).len(),
        7,
        "every historical order must still be reconstructed",
    );
    assert!(
        cache_ref
            .positions_open(None, Some(&eth), None, None, None)
            .is_empty(),
        "the netted history must leave no open position",
    );
}

#[rstest]
#[tokio::test]
async fn test_unlinkable_fill_is_kept_and_the_snapshot_declared_incomplete() {
    // The venue cannot answer for the order behind a trade it reported. Dropping the trade
    // would lose real execution and its commission, so it is kept and the snapshot says so.
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    let trade_ms = now_ms() - 5_000;
    venue.script(|script| {
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![sized_trade(
                44_000, 940_404, "BTCUSDT", trade_ms, "0.010", "BUY",
            )],
        );
        // `orders` stays empty, so the order query answers -2013 NO_SUCH_ORDER.
    });

    let mass_status = harness
        .client
        .generate_mass_status(None)
        .await
        .expect("mass status")
        .expect("mass status present");

    let trade_ids: Vec<String> = mass_status
        .fill_reports()
        .values()
        .flatten()
        .map(|fill| fill.trade_id.to_string())
        .collect();
    assert_eq!(
        trade_ids,
        vec!["44000".to_string()],
        "a real trade must never be discarded to silence a warning",
    );
    assert!(
        !mass_status.reports_complete(),
        "a fill with no order behind it makes the snapshot partial, and it must say so",
    );
}

#[rstest]
#[tokio::test]
async fn test_repeated_compensation_does_not_duplicate_a_recovered_fill() {
    // R2-01 asks for idempotency: a second outage must not re-apply a trade the first one
    // already delivered, nor follow it with a bare status that infers a replacement.
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    let order = limit_order("O-IDEMPOTENT", OrderSide::Buy, false);
    submit_and_settle(&harness, &order, &venue).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    drain_exec(&mut harness.exec_rx);

    let trade_ms = now_ms();
    venue.script(|script| {
        let mut row = venue_order(900_900, "O-IDEMPOTENT", "BTCUSDT", "FILLED", "BUY");
        row["time"] = json!(trade_ms - 1);
        row["updateTime"] = json!(trade_ms);
        script
            .orders
            .insert("O-IDEMPOTENT".to_string(), row.clone());
        script.orders.insert("900900".to_string(), row);
        script.open_orders = json!([]);
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(7_777, 900_900, "BTCUSDT", trade_ms, "0.02")],
        );
    });

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
    tokio::time::sleep(Duration::from_millis(500)).await;

    let first = drain_exec(&mut harness.exec_rx);
    assert_eq!(
        fill_trade_ids(&first)
            .iter()
            .filter(|id| *id == "7777")
            .count(),
        1,
        "the recovered fill must be delivered exactly once: {first:?}",
    );

    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 3 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async {
            venue
                .requests_for("GET", "userTrades")
                .iter()
                .filter(|request| request.param("symbol") == Some("BTCUSDT"))
                .count()
                >= 2
        },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let second = drain_exec(&mut harness.exec_rx);
    assert!(
        !fill_trade_ids(&second).contains(&"7777".to_string()),
        "a second compensation must not re-apply an already delivered trade: {second:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_compensation_reports_the_order_together_with_its_fills() {
    // The order status and its trades must reach the engine as one report. A bare status
    // carrying the cumulative quantity would make the engine invent the missing fill, and the
    // real trade would then be rejected by the overfill guard.
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    let order = limit_order("O-BUNDLED", OrderSide::Buy, false);
    submit_and_settle(&harness, &order, &venue).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    drain_exec(&mut harness.exec_rx);

    let trade_ms = now_ms();
    venue.script(|script| {
        let mut row = venue_order(901_000, "O-BUNDLED", "BTCUSDT", "FILLED", "BUY");
        row["time"] = json!(trade_ms - 1);
        row["updateTime"] = json!(trade_ms);
        script.orders.insert("O-BUNDLED".to_string(), row.clone());
        script.orders.insert("901000".to_string(), row);
        script.open_orders = json!([]);
        script.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(8_888, 901_000, "BTCUSDT", trade_ms, "0.02")],
        );
    });

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
    tokio::time::sleep(Duration::from_millis(500)).await;

    let events = drain_exec(&mut harness.exec_rx);
    let bundled = events.iter().any(|event| {
        matches!(
            event,
            ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills))
                if report.client_order_id == Some(ClientOrderId::from("O-BUNDLED"))
                    && fills.iter().any(|fill| fill.trade_id.as_str() == "8888")
        )
    });
    assert!(
        bundled,
        "order and fills must arrive as one report: {events:?}"
    );

    let bare_status_for_order = events.iter().any(|event| {
        matches!(
            event,
            ExecutionEvent::Report(ExecutionReport::Order(report))
                if report.client_order_id == Some(ClientOrderId::from("O-BUNDLED"))
        )
    });
    assert!(
        !bare_status_for_order,
        "no fill-inferring status may accompany the bundled report: {events:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_a_failed_rate_query_does_not_leave_an_earlier_rate_applied() {
    // Registered rates outlive the client that published them, so a reconnect whose query fails
    // must not keep silently applying the previous session's rate as if it were verified.
    // Its own venue: the registry is process-wide, and the other tests in this binary connect
    // against `ASTER` concurrently.
    let fee_venue = Venue::from("ASTERFEESCOPE");
    let btc = InstrumentId::new(Symbol::from("BTCUSDT-PERP"), fee_venue);
    let venue = MockVenue::start().await;
    script_connect(&venue);

    let mut first = build_harness_with_venue(&venue, fee_venue);
    seed_account(&first.cache);
    first.client.start().expect("start");
    first.client.connect().await.expect("connect");

    let mut seen_verified = false;
    while let Ok(event) = first.data_rx.try_recv() {
        if let DataEvent::Instrument(instrument) = event
            && instrument.id() == btc
        {
            assert_eq!(instrument.taker_fee().to_string(), ASTER_TAKER);
            seen_verified = true;
        }
    }
    assert!(seen_verified, "the first session must verify the rate");

    // The venue stops answering `commissionRate` for BTCUSDT.
    venue.script(|script| {
        script.commission_rates.remove("BTCUSDT");
    });

    let mut second = build_harness_with_venue(&venue, fee_venue);
    seed_account(&second.cache);
    second.client.start().expect("start");
    second.client.connect().await.expect("connect");

    let mut published = HashMap::new();
    while let Ok(event) = second.data_rx.try_recv() {
        if let DataEvent::Instrument(instrument) = event {
            published.insert(instrument.id().to_string(), instrument);
        }
    }

    assert!(
        !published.contains_key(&btc.to_string()),
        "a symbol whose rate query failed must not be republished as verified",
    );

    // And the shared parser must no longer apply the stale rate either.
    assert_eq!(
        nautilus_binance::common::fees::instrument_fees(&venue.http_url(), "BTCUSDT"),
        None,
        "the previous session's registration must be cleared",
    );
    nautilus_binance::common::fees::clear_endpoint_fees(&venue.http_url());
}

// ------------------------------------------------------------------------------------------------
// Round-2 review regressions
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn review_round2_reconnect_preserves_real_trade_economics() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    let order = limit_order("O-REVIEW-GAP", OrderSide::Buy, false);
    submit_and_settle(&harness, &order, &venue).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    drain_exec(&mut harness.exec_rx);
    let trade_ms = now_ms();
    venue.script(|s| {
        let mut row = venue_order(900200, "O-REVIEW-GAP", "BTCUSDT", "FILLED", "BUY");
        row["time"] = json!(trade_ms - 1);
        row["updateTime"] = json!(trade_ms);
        s.orders.insert("O-REVIEW-GAP".to_string(), row.clone());
        s.orders.insert("900200".to_string(), row);
        s.open_orders = json!([]);
        s.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(7001, 900200, "BTCUSDT", trade_ms, "0.02")],
        );
    });
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
    tokio::time::sleep(Duration::from_millis(600)).await;
    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        fill_trade_ids(&events).contains(&"7001".to_string()),
        "{events:?}"
    );
    let cache = Rc::new(RefCell::new(Cache::default()));
    seed_account(&cache);
    while let Ok(event) = harness.data_rx.try_recv() {
        if let DataEvent::Instrument(i) = event {
            cache.borrow_mut().add_instrument(i).unwrap();
        }
    }
    let mut engine =
        ExecutionEngine::new(Rc::new(RefCell::new(TestClock::new())), cache.clone(), None);
    engine.register_client(Box::new(harness.client)).unwrap();
    engine.register_oms_type(StrategyId::from("EXTERNAL"), OmsType::Netting);
    for event in &events {
        if let ExecutionEvent::Report(r) = event {
            engine.reconcile_execution_report(r);
        }
    }
    let c = cache.borrow();
    let o = c.order(&ClientOrderId::from("O-REVIEW-GAP")).unwrap();
    assert_eq!(o.filled_qty(), Quantity::from("0.010"));
    assert!(
        o.trade_ids().iter().any(|id| id.as_str() == "7001"),
        "real trade lost: ids={:?}, fees={:?}",
        o.trade_ids(),
        o.commissions()
    );
    assert_eq!(
        o.commissions().get(&Currency::USDT()),
        Some(&Money::from("0.02 USDT"))
    );
}

#[rstest]
#[tokio::test]
async fn review_round2_mass_status_keeps_fill_for_order_created_before_window() {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;
    let time = now_ms();
    venue.script(|s| {
        let mut row = venue_order(920001,"O-OLD-GTC","BTCUSDT","FILLED","BUY");
        row["time"] = json!(time - 120_000);
        row["updateTime"] = json!(time - 1_000);
        s.all_orders.insert("BTCUSDT".to_string(),vec![row.clone()]);
        s.orders.insert("920001".to_string(),row);
        s.user_trades.insert("BTCUSDT".to_string(),vec![venue_trade(920101,920001,"BTCUSDT",time-1_000,"0.02")]);
        s.position_risk = json!([{"symbol":"BTCUSDT","positionAmt":"0.010","entryPrice":"50000.00","positionSide":"BOTH","updateTime":time}]);
    });
    let mass = harness
        .client
        .generate_mass_status(Some(1))
        .await
        .unwrap()
        .unwrap();
    let ids: Vec<String> = mass
        .fill_reports()
        .values()
        .flat_map(|rows| rows.iter().map(|r| r.trade_id.to_string()))
        .collect();
    assert!(
        ids.contains(&"920101".to_string()),
        "in-window real trade discarded; complete={}, orders={}, fills={ids:?}",
        mass.reports_complete(),
        mass.order_reports().len()
    );
}

#[rstest]
#[tokio::test]
async fn review_round2_structured_503_must_not_reject_live_order() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    venue.script(|s| {
        s.submit = SubmitOutcome::Status {
            status: 503,
            body: json!({"code": -1000, "msg": "An unknown error occured while processing the request."}).to_string(),
        };
        s.orders.insert("O-503-JSON".to_string(),
            venue_order(930001,"O-503-JSON","BTCUSDT","NEW","BUY"));
    });
    let order = limit_order("O-503-JSON", OrderSide::Buy, false);
    drain_exec(&mut harness.exec_rx);
    submit_and_settle(&harness, &order, &venue).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        !order_events(&events)
            .iter()
            .any(|e| matches!(e, OrderEventAny::Rejected(_))),
        "structured HTTP503 terminalised a potentially live order: {events:?}"
    );
}

#[rstest]
#[tokio::test]
async fn review_round2_failed_fill_report_does_not_consume_recovery_trade() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    drain_exec(&mut harness.exec_rx);
    let trade_ms = now_ms();
    venue.script(|s| {
        s.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![
                venue_trade(930101, 930001, "BTCUSDT", trade_ms, "0.02"),
                venue_trade(930102, 930002, "BTCUSDT", trade_ms, "not-a-number"),
            ],
        );
        let mut row = venue_order(930001, "O-REPORT-RETRY", "BTCUSDT", "FILLED", "BUY");
        row["time"] = json!(trade_ms);
        row["updateTime"] = json!(trade_ms);
        s.orders.insert("930001".to_string(), row);
        s.open_orders = json!([]);
    });
    let error = harness
        .client
        .generate_fill_reports(recent_fill_reports_command())
        .await
        .expect_err("invalid second commission must fail the whole report request");
    assert!(format!("{error:#}").contains("commission"), "{error:#}");
    assert!(fill_trade_ids(&drain_exec(&mut harness.exec_rx)).is_empty());
    venue.script(|s| {
        s.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(930101, 930001, "BTCUSDT", trade_ms, "0.02")],
        );
    });
    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async {
            venue
                .requests_for("GET", "userTrades")
                .iter()
                .filter(|r| r.param("symbol") == Some("BTCUSDT"))
                .count()
                >= 2
        },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        fill_trade_ids(&events).contains(&"930101".to_string()),
        "failed report request must not consume a trade that the engine never received: {events:?}"
    );
}

#[rstest]
#[tokio::test]
async fn review_round2_data_request_preserves_verified_fees() {
    use nautilus_aster::config::AsterDataClientConfig;
    use nautilus_binance::{
        common::enums::BinanceProductType, futures::data::BinanceFuturesDataClient,
    };
    use nautilus_common::{
        clients::DataClient,
        messages::data::{DataResponse, RequestInstruments},
    };
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    while let Ok(event) = harness.data_rx.try_recv() {
        if let DataEvent::Instrument(i) = event {
            harness.cache.borrow_mut().add_instrument(i).unwrap();
        }
    }
    let before = harness
        .cache
        .borrow()
        .instrument(&InstrumentId::from(BTC))
        .unwrap()
        .taker_fee();
    assert_eq!(before, ASTER_TAKER.parse().unwrap());
    let config = AsterDataClientConfig {
        base_url_http: Some(venue.http_url()),
        base_url_ws: Some(venue.ws_url()),
        ..Default::default()
    };
    let mut data = BinanceFuturesDataClient::new(
        *ASTER_CLIENT_ID,
        config.to_binance(),
        BinanceProductType::UsdM,
    )
    .unwrap();
    data.start().unwrap();
    data.request_instruments(RequestInstruments::new(
        None,
        None,
        Some(*ASTER_CLIENT_ID),
        Some(*ASTER_VENUE),
        UUID4::new(),
        UnixNanos::default(),
        None,
    ))
    .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(DataEvent::Response(DataResponse::Instruments(r))) =
                harness.data_rx.recv().await
            {
                break r;
            }
        }
    })
    .await
    .expect("instrument response must arrive");
    for instrument in response.data {
        harness
            .cache
            .borrow_mut()
            .add_instrument(instrument)
            .unwrap();
    }
    let after = harness
        .cache
        .borrow()
        .instrument(&InstrumentId::from(BTC))
        .unwrap()
        .taker_fee();
    data.stop().unwrap();
    assert_eq!(
        after, before,
        "data request overwrote verified account taker fee"
    );
}

// ------------------------------------------------------------------------------------------------
// Round-3 review regressions
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn review_round3_failed_fill_compensation_preserves_eventual_real_economics() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    let order = limit_order("O-R3-TRANSIENT", OrderSide::Buy, false);
    submit_and_settle(&harness, &order, &venue).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    drain_exec(&mut harness.exec_rx);
    let trade_ms = now_ms();
    venue.script(|s| {
        let mut row = venue_order(950001, "O-R3-TRANSIENT", "BTCUSDT", "FILLED", "BUY");
        row["time"] = json!(trade_ms - 1);
        row["updateTime"] = json!(trade_ms);
        s.orders.insert("O-R3-TRANSIENT".to_string(), row.clone());
        s.orders.insert("950001".to_string(), row);
        s.open_orders = json!([]);
        s.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(950101, 950001, "BTCUSDT", trade_ms, "0.02")],
        );
        s.user_trades_error.insert(
            "BTCUSDT".to_string(),
            json!({"code": -1000, "msg": "temporary history failure"}),
        );
    });
    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "openOrders").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut events = drain_exec(&mut harness.exec_rx);
    venue.script(|s| {
        s.user_trades_error.clear();
    });
    venue.clear_requests();
    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 3 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "openOrders").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    events.extend(drain_exec(&mut harness.exec_rx));
    assert!(
        fill_trade_ids(&events).contains(&"950101".to_string()),
        "recovered real fill must reach the engine: {events:?}"
    );
    let cache = Rc::new(RefCell::new(Cache::default()));
    seed_account(&cache);
    while let Ok(event) = harness.data_rx.try_recv() {
        if let DataEvent::Instrument(i) = event {
            cache.borrow_mut().add_instrument(i).unwrap();
        }
    }
    let mut engine =
        ExecutionEngine::new(Rc::new(RefCell::new(TestClock::new())), cache.clone(), None);
    engine.register_client(Box::new(harness.client)).unwrap();
    engine.register_oms_type(StrategyId::from("EXTERNAL"), OmsType::Netting);
    for event in &events {
        if let ExecutionEvent::Report(r) = event {
            engine.reconcile_execution_report(r);
        }
    }
    let c = cache.borrow();
    let o = c.order(&ClientOrderId::from("O-R3-TRANSIENT")).unwrap();
    assert_eq!(o.filled_qty(), Quantity::from("0.010"));
    assert!(
        o.trade_ids().iter().any(|id| id.as_str() == "950101"),
        "temporary fill-history failure permanently replaced real trade: ids={:?}, fees={:?}",
        o.trade_ids(),
        o.commissions()
    );
    assert_eq!(
        o.commissions().get(&Currency::USDT()),
        Some(&Money::from("0.02 USDT"))
    );
}

#[rstest]
#[tokio::test]
async fn review_round3_unlinked_startup_fill_remains_recoverable() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    drain_exec(&mut harness.exec_rx);
    let trade_ms = now_ms();
    venue.script(|s| {
        s.user_trades.insert("BTCUSDT".to_string(), vec![venue_trade(960101, 960001, "BTCUSDT", trade_ms, "0.02")]);
        s.position_risk = json!([{"symbol":"BTCUSDT","positionAmt":"0.010","entryPrice":"50000.00","positionSide":"BOTH","updateTime":trade_ms}]);
    });
    let mass = harness
        .client
        .generate_mass_status(Some(1))
        .await
        .unwrap()
        .unwrap();
    assert!(!mass.reports_complete());
    assert_eq!(mass.fill_reports().values().flatten().count(), 1);
    let cache = Rc::new(RefCell::new(Cache::default()));
    seed_account(&cache);
    while let Ok(event) = harness.data_rx.try_recv() {
        if let DataEvent::Instrument(i) = event {
            cache.borrow_mut().add_instrument(i).unwrap();
        }
    }
    let clock = Rc::new(RefCell::new(TestClock::new()));
    let mut manager = ExecutionManager::new(
        clock.clone(),
        cache.clone(),
        ExecutionManagerConfig::default(),
    )
    .unwrap();
    let engine = Rc::new(RefCell::new(ExecutionEngine::new(
        clock,
        cache.clone(),
        None,
    )));
    engine
        .borrow_mut()
        .register_client(Box::new(harness.client))
        .unwrap();
    engine
        .borrow_mut()
        .register_oms_type(StrategyId::from("EXTERNAL"), OmsType::Netting);
    manager
        .reconcile_execution_mass_status(mass, engine.clone())
        .await;
    assert!(
        cache
            .borrow()
            .order(&ClientOrderId::from("O-R3-UNLINKED"))
            .is_none()
    );
    venue.script(|s| {
        let mut row = venue_order(960001, "O-R3-UNLINKED", "BTCUSDT", "FILLED", "BUY");
        row["time"] = json!(trade_ms - 120_000);
        row["updateTime"] = json!(trade_ms);
        s.orders.insert("960001".to_string(), row);
    });
    venue.clear_requests();
    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "openOrders").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        fill_trade_ids(&events).contains(&"960101".to_string()),
        "unlinked fill was skipped by startup reconciliation and must remain recoverable after its order becomes available: {events:?}"
    );
}

#[rstest]
#[tokio::test]
async fn review_round3_account_fee_registry_leaks_between_clients() {
    use nautilus_binance::{
        common::enums::{BinanceEnvironment, BinanceProductType},
        futures::http::client::BinanceFuturesHttpClient,
    };
    use nautilus_core::time::get_atomic_clock_realtime;
    let fee_venue = Venue::from("ASTERROUND3FEES");
    let build = |mock: &MockVenue, account: &str| {
        let account_id = AccountId::from(account);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            *ASTER_CLIENT_ID,
            fee_venue,
            OmsType::Netting,
            account_id,
            AccountType::Margin,
            None,
            cache.clone(),
        );
        let config = AsterExecutionClientConfig {
            account_id,
            signer_private_key: Some(TEST_PRIVATE_KEY.to_string()),
            base_url_http: Some(mock.http_url()),
            base_url_ws: Some(mock.ws_url()),
            http_timeout_secs: Some(30),
            venue: Some(fee_venue),
            ..Default::default()
        };
        let (exec_tx, exec_rx) = tokio::sync::mpsc::unbounded_channel();
        let (data_tx, data_rx) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(exec_tx);
        replace_data_event_sender(data_tx);
        Harness {
            client: AsterExecutionClient::new(core, config).unwrap(),
            exec_rx,
            data_rx,
            cache,
        }
    };
    let first_mock = MockVenue::start().await;
    script_connect(&first_mock);
    let mut first = build(&first_mock, "ASTER-ROUND3-A");
    first.client.start().unwrap();
    first.client.connect().await.unwrap();
    let data = BinanceFuturesHttpClient::new(
        BinanceProductType::UsdM,
        BinanceEnvironment::Live,
        get_atomic_clock_realtime(),
        None,
        None,
        Some(first_mock.http_url()),
        None,
        Some(30),
        None,
        false,
    )
    .unwrap()
    .with_venue(fee_venue);
    let load = nautilus_binance::config::BinanceInstrumentProviderConfig::default();
    let before = data
        .request_instruments_with_config(&load)
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.raw_symbol().as_str() == "BTCUSDT")
        .unwrap()
        .taker_fee();
    assert_eq!(before, ASTER_TAKER.parse().unwrap());
    let second_mock = MockVenue::start().await;
    script_connect(&second_mock);
    second_mock.script(|s| {
        s.commission_rates.get_mut("BTCUSDT").unwrap()["takerCommissionRate"] = json!("0.001000");
    });
    let mut second = build(&second_mock, "ASTER-ROUND3-B");
    second.client.start().unwrap();
    second.client.connect().await.unwrap();
    let after = data
        .request_instruments_with_config(&load)
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.raw_symbol().as_str() == "BTCUSDT")
        .unwrap()
        .taker_fee();
    first.client.stop().unwrap();
    second.client.stop().unwrap();
    // Cleanup only: the registry is now keyed by endpoint, so each mock is released by URL.
    nautilus_binance::common::fees::clear_endpoint_fees(&first_mock.http_url());
    nautilus_binance::common::fees::clear_endpoint_fees(&second_mock.http_url());
    assert_eq!(
        after, before,
        "first endpoint acquired the second account's fees"
    );
}

// ------------------------------------------------------------------------------------------------
// Round-4 review regressions
// ------------------------------------------------------------------------------------------------

fn cancel_command(client_order_id: &str, venue_order_id: Option<&str>) -> CancelOrder {
    CancelOrder::new(
        TraderId::from("TESTER-001"),
        Some(*ASTER_CLIENT_ID),
        StrategyId::from("S-001"),
        InstrumentId::from(BTC),
        ClientOrderId::from(client_order_id),
        venue_order_id.map(VenueOrderId::from),
        UUID4::new(),
        UnixNanos::default(),
        None, // params
        None, // correlation_id
    )
}

/// Issues `cmd` and waits until the cancel request has reached the venue and settled.
async fn cancel_and_settle(harness: &Harness, cmd: CancelOrder, venue: &MockVenue) {
    harness.client.cancel_order(cmd).expect("cancel accepted");

    wait_until_async(
        || async { !venue.requests_for("DELETE", "order").is_empty() },
        Duration::from_secs(10),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
}

fn has_cancel_rejected(events: &[ExecutionEvent]) -> bool {
    order_events(events)
        .iter()
        .any(|event| matches!(event, OrderEventAny::CancelRejected(_)))
}

/// Builds a started but unconnected harness against a venue already scripted for connect.
fn started_harness(venue: &MockVenue) -> Harness {
    script_connect(venue);
    let mut harness = build_harness(venue, Some(30));
    seed_account(&harness.cache);
    harness.client.start().expect("start");
    harness
}

/// R4-01: a compensation pass that reads a fill but cannot read the order it belongs to must
/// publish neither half. A bare fill bootstraps a synthetic order at that fill's quantity and
/// the remaining fills are then dropped by the overfill guard, so the trades are held back and
/// delivered with their order once the venue answers.
#[rstest]
#[tokio::test]
async fn review_round4_uncovered_fills_are_withheld_until_their_order_answers() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    drain_exec(&mut harness.exec_rx);

    let external = ClientOrderId::from("O-R4-UNCOVERED");
    let trade_ms = now_ms();
    venue.script(|s| {
        s.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![
                venue_trade(970_101, 970_001, "BTCUSDT", trade_ms - 2, "0.02"),
                venue_trade(970_102, 970_001, "BTCUSDT", trade_ms - 1, "0.02"),
                venue_trade(970_103, 970_001, "BTCUSDT", trade_ms, "0.02"),
            ],
        );
        let mut row = venue_order(970_001, "O-R4-UNCOVERED", "BTCUSDT", "FILLED", "BUY");
        row["origQty"] = json!("0.030");
        row["executedQty"] = json!("0.030");
        row["avgPrice"] = json!("50000.00");
        row["time"] = json!(trade_ms - 3);
        row["updateTime"] = json!(trade_ms);
        s.orders.insert("970001".to_string(), row);
        s.open_orders = json!([]);
        // The order query fails once, so the first pass reads the trades but cannot build the
        // order state they belong to.
        s.order_query_faults.insert("970001".to_string(), 1);
    });

    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "order").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let mut events = drain_exec(&mut harness.exec_rx);
    assert!(
        fill_trade_ids(&events).is_empty(),
        "a fill whose order could not be read must not reach the engine alone: {events:?}",
    );
    assert!(
        !order_reports(&events)
            .iter()
            .any(|(_, client_order_id)| *client_order_id == Some(external)),
        "the order status must be withheld with its fills, not published on its own: {events:?}",
    );

    venue.clear_requests();
    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 3 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async {
            venue
                .requests_for("GET", "userTrades")
                .iter()
                .any(|request| request.param("symbol") == Some("BTCUSDT"))
        },
        Duration::from_secs(20),
    )
    .await;

    // The bundle is published when the compensation pass completes, which under load can be
    // later than any fixed delay: wait for the event itself, not for a stopwatch.
    let second = wait_for_events(&mut harness, Duration::from_secs(20), |events| {
        events.iter().any(|event| {
            matches!(
                event,
                ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills))
                    if report.client_order_id == Some(external) && fills.len() == 3
            )
        })
    })
    .await;

    let bundled = second.iter().any(|event| {
        matches!(
            event,
            ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills))
                if report.client_order_id == Some(external) && fills.len() == 3
        )
    });
    assert!(
        bundled,
        "the recovered order must arrive with all three of its trades: {second:?}",
    );
    events.extend(second);

    let cache = Rc::new(RefCell::new(Cache::default()));
    seed_account(&cache);
    while let Ok(event) = harness.data_rx.try_recv() {
        if let DataEvent::Instrument(instrument) = event {
            cache.borrow_mut().add_instrument(instrument).unwrap();
        }
    }
    let mut engine =
        ExecutionEngine::new(Rc::new(RefCell::new(TestClock::new())), cache.clone(), None);
    engine.register_client(Box::new(harness.client)).unwrap();
    engine.register_oms_type(StrategyId::from("EXTERNAL"), OmsType::Netting);
    for event in &events {
        if let ExecutionEvent::Report(report) = event {
            engine.reconcile_execution_report(report);
        }
    }

    let cache = cache.borrow();
    let order = cache.order(&external).expect("the external order");
    assert_eq!(order.filled_qty(), Quantity::from("0.030"));
    assert_eq!(
        order
            .trade_ids()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["970101", "970102", "970103"],
        "every real trade ID must survive: {:?}",
        order.trade_ids(),
    );
    let position = cache
        .position_for_order(&external)
        .expect("the fills must open a position");
    assert_eq!(position.quantity, Quantity::from("0.030"));
}

/// R4-01: the order sweep of the same compensation pass must withhold the status of an order
/// whose fills were held back. Publishing it would report a filled quantity with no trades
/// behind it, which the engine explains by inventing one.
#[rstest]
#[tokio::test]
async fn review_round4_open_order_status_is_withheld_while_its_fills_are() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    drain_exec(&mut harness.exec_rx);

    let external = ClientOrderId::from("O-R4-OPEN");
    let trade_ms = now_ms();
    venue.script(|s| {
        s.user_trades.insert(
            "BTCUSDT".to_string(),
            vec![venue_trade(970_201, 970_002, "BTCUSDT", trade_ms, "0.02")],
        );
        let mut row = venue_order(970_002, "O-R4-OPEN", "BTCUSDT", "PARTIALLY_FILLED", "BUY");
        row["origQty"] = json!("0.030");
        row["executedQty"] = json!("0.010");
        row["avgPrice"] = json!("50000.00");
        row["time"] = json!(trade_ms - 1);
        row["updateTime"] = json!(trade_ms);
        s.orders.insert("970002".to_string(), row.clone());
        // The order is still working, so the order sweep sees it on `openOrders`.
        s.open_orders = json!([row]);
        s.order_query_faults.insert("970002".to_string(), 1);
    });

    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 2 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "openOrders").is_empty() },
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let first = drain_exec(&mut harness.exec_rx);
    assert!(
        fill_trade_ids(&first).is_empty(),
        "the fill must be held back with its order: {first:?}",
    );
    assert!(
        !order_reports(&first)
            .iter()
            .any(|(_, client_order_id)| *client_order_id == Some(external)),
        "a status reporting 0.010 filled with no trade behind it must be withheld: {first:?}",
    );

    venue.clear_requests();
    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= 3 },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "openOrders").is_empty() },
        Duration::from_secs(20),
    )
    .await;

    let second = wait_for_events(&mut harness, Duration::from_secs(20), |events| {
        events.iter().any(|event| {
            matches!(
                event,
                ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills))
                    if report.client_order_id == Some(external)
                        && fills.iter().any(|fill| fill.trade_id.as_str() == "970201")
            )
        })
    })
    .await;
    let bundled = second.iter().any(|event| {
        matches!(
            event,
            ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills))
                if report.client_order_id == Some(external)
                    && fills.iter().any(|fill| fill.trade_id.as_str() == "970201")
        )
    });
    assert!(
        bundled,
        "once the order answers, its status and trade arrive together: {second:?}",
    );
}

/// R4-02: a cancel the venue definitively refuses leaves the order working, and the engine
/// reconciles a stuck `PendingCancel` as canceled, so the refusal must be reported.
#[rstest]
#[case(json!({"code": -1102, "msg": "Mandatory parameter was not sent."}), true)]
#[case(json!({"code": -2011, "msg": "Unknown order sent."}), false)]
#[tokio::test]
async fn review_round4_cancel_reports_only_a_definitive_refusal(
    #[case] error: Value,
    #[case] expect_rejection: bool,
) {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    venue.script(|s| s.cancel_error = Some(error));
    drain_exec(&mut harness.exec_rx);

    cancel_and_settle(
        &harness,
        cancel_command("O-R4-CANCEL", Some("900500")),
        &venue,
    )
    .await;

    let events = drain_exec(&mut harness.exec_rx);
    assert_eq!(has_cancel_rejected(&events), expect_rejection, "{events:?}");
}

/// R4-02: a `5xx` says nothing about whether the cancel landed, so it stays with reconciliation.
#[rstest]
#[tokio::test]
async fn review_round4_ambiguous_cancel_failure_awaits_reconciliation() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    venue.script(|s| s.cancel_status = Some((503, "service unavailable".to_string())));
    drain_exec(&mut harness.exec_rx);

    cancel_and_settle(
        &harness,
        cancel_command("O-R4-AMBIG", Some("900501")),
        &venue,
    )
    .await;

    let events = drain_exec(&mut harness.exec_rx);
    assert!(
        !has_cancel_rejected(&events),
        "an ambiguous cancel failure must not be reported as a refusal: {events:?}",
    );
}

/// R4-02: the side-filtered cancel-all path maps each venue order back to its client order ID,
/// so a per-order refusal can be attributed to the order it belongs to.
#[rstest]
#[tokio::test]
async fn review_round4_cancel_all_for_a_side_reports_a_refusal_per_order() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    venue.script(|s| {
        s.open_orders = json!([venue_order(900_600, "O-R4-SIDE", "BTCUSDT", "NEW", "BUY")]);
        s.cancel_error = Some(json!({"code": -1102, "msg": "Mandatory parameter was not sent."}));
    });
    drain_exec(&mut harness.exec_rx);

    harness
        .client
        .cancel_all_orders(cancel_all(Some(OrderSide::Buy)))
        .expect("cancel-all accepted");
    wait_until_async(
        || async { !venue.requests_for("DELETE", "order").is_empty() },
        Duration::from_secs(10),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let events = drain_exec(&mut harness.exec_rx);
    let attributed = order_events(&events).iter().any(|event| {
        matches!(event, OrderEventAny::CancelRejected(rejected)
            if rejected.client_order_id == ClientOrderId::from("O-R4-SIDE"))
    });
    assert!(
        attributed,
        "each refused cancel must name the order it left working: {events:?}",
    );
}

/// R4-05: the venue order ID is unambiguous where a client order ID may be unknown to the
/// venue, so it is preferred; the client order ID is the fallback.
#[rstest]
#[case(Some("900700"), "orderId", "900700", "origClientOrderId")]
#[case(None, "origClientOrderId", "O-R4-IDENT", "orderId")]
#[tokio::test]
async fn review_round4_cancel_prefers_the_venue_order_id(
    #[case] venue_order_id: Option<&str>,
    #[case] expected_param: &str,
    #[case] expected_value: &str,
    #[case] absent_param: &str,
) {
    let venue = MockVenue::start().await;
    let harness = connected_harness(&venue).await;

    cancel_and_settle(
        &harness,
        cancel_command("O-R4-IDENT", venue_order_id),
        &venue,
    )
    .await;

    let requests = venue.requests_for("DELETE", "order");
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0].param(expected_param), Some(expected_value));
    assert_eq!(requests[0].param(absent_param), None);
}

/// R4-03: a disconnect closes both task generations permanently, so a reconnect must reopen
/// them before it spawns anything, and it must release the listen key it opened.
#[rstest]
#[tokio::test]
async fn review_round4_reconnect_after_disconnect_restores_a_working_session() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;

    harness.client.disconnect().await.expect("disconnect");
    assert!(
        !venue.requests_for("DELETE", "listenKey").is_empty(),
        "a disconnect must release the listen key rather than leave it to expire: {:?}",
        venue.requests(),
    );

    harness.client.connect().await.expect("reconnect");
    venue.clear_requests();

    let order = limit_order("O-R4-RECONNECT", OrderSide::Buy, false);
    submit_and_settle(&harness, &order, &venue).await;

    assert_eq!(
        venue.requests_for("POST", "order").len(),
        1,
        "an order submitted after a reconnect must reach the venue: {:?}",
        venue.requests(),
    );
}

/// R4-04: an unconfirmed position mode is not a one-way mode.
#[rstest]
#[tokio::test]
async fn review_round4_unconfirmed_position_mode_fails_the_connect() {
    let venue = MockVenue::start().await;
    venue.script(|s| s.position_mode_status = Some((503, "service unavailable".to_string())));
    let mut harness = started_harness(&venue);

    let error = harness
        .client
        .connect()
        .await
        .expect_err("a 5xx leaves the account's position mode unknown");

    assert!(format!("{error:#}").contains("position mode"), "{error:#}");
    assert!(
        venue.requests_for("POST", "listenKey").is_empty(),
        "the session must not come up on an unconfirmed mode: {:?}",
        venue.requests(),
    );
}

/// R4-04: a definitive venue answer that the endpoint is unavailable still assumes one-way.
#[rstest]
#[tokio::test]
async fn review_round4_unsupported_position_mode_endpoint_assumes_one_way() {
    let venue = MockVenue::start().await;
    venue.script(|s| {
        s.position_mode_error = Some(json!({"code": -1121, "msg": "Invalid symbol."}));
    });
    let mut harness = started_harness(&venue);

    harness
        .client
        .connect()
        .await
        .expect("a definitive venue answer must not block the session");
}

/// R4-04: hedge mode is still rejected outright.
#[rstest]
#[tokio::test]
async fn review_round4_hedge_mode_account_is_rejected_at_connect() {
    let venue = MockVenue::start().await;
    venue.script(|s| s.position_mode = Some(json!({"dualSidePosition": true})));
    let mut harness = started_harness(&venue);

    let error = harness
        .client
        .connect()
        .await
        .expect_err("hedge mode is not supported by this adapter");

    assert!(format!("{error:#}").contains("hedge"), "{error:#}");
}

// ------------------------------------------------------------------------------------------------
// R4 - bounded dedupe memory against the pending-trade pull-back
// ------------------------------------------------------------------------------------------------

/// Trades the venue holds for the symbol under test, comfortably past the adapter's dedupe cap.
const HISTORY_TRADES: i64 = 6_000;
/// Trades per historical order, so the history is a handful of orders rather than 6,000 of them.
const TRADES_PER_ORDER: i64 = 100;
/// Historical orders behind those trades.
const HISTORY_ORDERS: i64 = HISTORY_TRADES / TRADES_PER_ORDER;
const FIRST_TRADE_ID: i64 = 700_000;
const FIRST_ORDER_ID: i64 = 800_000;
/// The venue order behind the unlinkable trade; the venue never answers for it.
const ORPHAN_ORDER_ID: i64 = 899_999;
const ORPHAN_TRADE_ID: i64 = 699_999;

/// A historical order that a hundred small trades filled.
fn bulk_order(order_id: i64, client_order_id: &str, time_ms: i64, update_ms: i64) -> Value {
    json!({
        "symbol": "BTCUSDT",
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "price": "50000.00",
        "avgPrice": "50000.00",
        "origQty": "0.100",
        "executedQty": "0.100",
        "cumQuote": "5000.0",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "positionSide": "BOTH",
        "reduceOnly": false,
        "closePosition": false,
        "time": time_ms,
        "updateTime": update_ms,
    })
}

/// Scripts a symbol whose recent history is longer than the adapter can remember.
///
/// The oldest trade belongs to an order the venue no longer answers for, which is what keeps a
/// trade pending after startup reconciliation.
fn script_long_history(venue: &MockVenue) -> i64 {
    let base_ms = now_ms() - 900_000;
    let mut orders = Vec::new();
    let mut trades = vec![sized_trade(
        ORPHAN_TRADE_ID,
        ORPHAN_ORDER_ID,
        "BTCUSDT",
        base_ms - 1_000,
        "0.001",
        "BUY",
    )];

    for index in 0..HISTORY_TRADES {
        let order_index = index / TRADES_PER_ORDER;
        let order_id = FIRST_ORDER_ID + order_index;
        let time_ms = base_ms + index * 10;
        trades.push(sized_trade(
            FIRST_TRADE_ID + index,
            order_id,
            "BTCUSDT",
            time_ms,
            "0.001",
            "BUY",
        ));

        if index % TRADES_PER_ORDER == 0 {
            orders.push(bulk_order(
                order_id,
                &format!("O-BULK-{order_index}"),
                time_ms - 5,
                time_ms + (TRADES_PER_ORDER - 1) * 10,
            ));
        }
    }

    venue.script(|script| {
        for order in &orders {
            script.orders.insert(
                order["orderId"].as_i64().expect("order id").to_string(),
                order.clone(),
            );
        }
        script.all_orders.insert("BTCUSDT".to_string(), orders);
        script.user_trades.insert("BTCUSDT".to_string(), trades);
        script.position_risk = json!([{
            "symbol": "BTCUSDT",
            "positionAmt": "6.000",
            "entryPrice": "50000.00",
            "positionSide": "BOTH",
            "updateTime": base_ms,
        }]);
    });

    base_ms
}

/// Drops the socket and waits for the whole compensation pass to finish.
///
/// The pass ends with the position refresh, so a `positionRisk` request after the reconnect is
/// the signal that fills, orders and balances have all been through.
async fn drive_compensation(venue: &MockVenue, expected_connections: usize) {
    venue.clear_requests();
    venue.drop_ws();
    wait_until_async(
        || async { venue.ws_connection_count() >= expected_connections },
        Duration::from_secs(20),
    )
    .await;
    wait_until_async(
        || async { !venue.requests_for("GET", "positionRisk").is_empty() },
        Duration::from_secs(60),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
}

fn feed_reports(engine: &Rc<RefCell<ExecutionEngine>>, events: &[ExecutionEvent]) {
    for event in events {
        if let ExecutionEvent::Report(report) = event {
            engine.borrow_mut().reconcile_execution_report(report);
        }
    }
}

fn btc_position(cache: &Rc<RefCell<Cache>>) -> Option<(Quantity, PositionSide)> {
    let cache = cache.borrow();
    cache
        .positions(None, Some(&InstrumentId::from(BTC)), None, None, None)
        .first()
        .map(|position| (position.quantity, position.side))
}

/// R4-04: a trade held back for recovery must not drag the compensation window back over a
/// history the bounded dedupe set has already forgotten.
///
/// The account below is ordinary: 6,000 trades in the reconciliation window, one of them
/// unlinkable because the venue no longer answers for its order. Startup applies 6,000 trade
/// IDs, `MAX_TRACKED_TRADE_IDS` (4,096) of which fit in the dedupe set, and the remaining 1,904
/// are evicted. If the pending trade pulled the next compensation window back to its own
/// timestamp, every one of those 1,904 evicted trades would read as missed and be re-delivered
/// as a live fill — on *every* reconnect, because re-recording an evicted ID immediately evicts
/// it again. The engine absorbs the replay only while it still holds the orders that own those
/// trade IDs; a node that has purged its closed orders (which live nodes do routinely) instead
/// bootstraps them again and books the quantity a second time.
///
/// So the pending trade is fetched by ID, and the window stays at the watermark.
#[rstest]
#[tokio::test]
async fn review_round4_a_pending_trade_does_not_replay_the_evicted_history() {
    let venue = MockVenue::start().await;
    let mut harness = connected_harness(&venue).await;
    drain_exec(&mut harness.exec_rx);
    let base_ms = script_long_history(&venue);

    let mass = harness
        .client
        .generate_mass_status(Some(10_080))
        .await
        .expect("mass status")
        .expect("mass status present");
    assert_eq!(mass.order_reports().len(), HISTORY_ORDERS as usize);
    assert_eq!(
        mass.fill_reports().values().flatten().count(),
        HISTORY_TRADES as usize + 1,
        "every trade in the window is reported, the unlinkable one included",
    );
    assert!(
        !mass.reports_complete(),
        "the trade whose order the venue will not answer for leaves the snapshot partial",
    );

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
    let engine = Rc::new(RefCell::new(ExecutionEngine::new(
        clock,
        cache.clone(),
        None,
    )));
    engine
        .borrow_mut()
        .register_client(Box::new(harness.client))
        .expect("register");
    engine
        .borrow_mut()
        .register_oms_type(StrategyId::from("EXTERNAL"), OmsType::Netting);
    manager
        .reconcile_execution_mass_status(mass, engine.clone())
        .await;
    drain_exec(&mut harness.exec_rx);

    let linked_qty = Quantity::from("6.000");
    assert_eq!(
        btc_position(&cache),
        Some((linked_qty, PositionSide::Long)),
        "startup applies every linked trade and skips the unlinkable one",
    );

    // Pass one: the compensation that follows the first reconnect, with every order still cached.
    drive_compensation(&venue, 2).await;
    let replayed = drain_exec(&mut harness.exec_rx);
    assert!(
        fill_trade_ids(&replayed).is_empty(),
        "a trade the startup snapshot already applied must not come back as a live fill, \
         however many IDs the dedupe set has since evicted: {:?}",
        fill_trade_ids(&replayed).len(),
    );
    assert!(
        venue.requests_for("GET", "userTrades").len() <= 4,
        "the pass must read the recent window and the held-back trade, not the whole history: {}",
        venue.requests_for("GET", "userTrades").len(),
    );

    feed_reports(&engine, &replayed);
    assert_eq!(btc_position(&cache), Some((linked_qty, PositionSide::Long)));
    assert_eq!(
        cache
            .borrow()
            .order(&ClientOrderId::from("O-BULK-0"))
            .expect("the first historical order")
            .filled_qty(),
        Quantity::from("0.100"),
    );

    // Pass two: the same reconnect against a node that has purged its closed orders, which is
    // where a replay stops being absorbed and starts booking quantity twice.
    cache
        .borrow_mut()
        .purge_closed_orders(UnixNanos::from(u64::MAX / 2), 0);
    assert!(
        cache
            .borrow()
            .orders(None, None, None, None, None)
            .is_empty(),
        "the purge must leave the compensation pass with no order to dedupe against",
    );

    drive_compensation(&venue, 3).await;
    let after_purge = drain_exec(&mut harness.exec_rx);
    assert!(
        fill_trade_ids(&after_purge).is_empty(),
        "the replay must not reappear once the orders that dedupe it are gone: {:?}",
        fill_trade_ids(&after_purge).len(),
    );

    feed_reports(&engine, &after_purge);
    assert_eq!(
        btc_position(&cache),
        Some((linked_qty, PositionSide::Long)),
        "the position must still be the one the venue reports",
    );

    // Pass three: the held-back trade is still recoverable. Its order becomes answerable, and
    // the trade arrives with it — reached by ID, from behind the window this pass reads.
    venue.script(|script| {
        let mut order = bulk_order(
            ORPHAN_ORDER_ID,
            "O-ORPHAN",
            base_ms - 1_001,
            base_ms - 1_000,
        );
        order["origQty"] = json!("0.001");
        order["executedQty"] = json!("0.001");
        order["cumQuote"] = json!("50.0");
        script
            .orders
            .insert(ORPHAN_ORDER_ID.to_string(), order.clone());
    });

    drive_compensation(&venue, 4).await;
    let recovered = drain_exec(&mut harness.exec_rx);
    assert_eq!(
        fill_trade_ids(&recovered),
        vec![ORPHAN_TRADE_ID.to_string()],
        "the trade held back at startup must still be delivered once its order answers",
    );

    feed_reports(&engine, &recovered);
    assert_eq!(
        btc_position(&cache),
        Some((Quantity::from("6.001"), PositionSide::Long)),
        "the recovered trade must reach the position exactly once",
    );
}
