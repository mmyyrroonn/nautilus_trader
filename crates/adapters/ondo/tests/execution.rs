// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License v3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Offline tests for the Ondo Perps execution client.
//!
//! Nothing here reaches the venue. Every request goes to a scripted HTTP/1.1 server bound to
//! `127.0.0.1:0` inside the test process - the harness `tests/http_client.rs` establishes, reused
//! here with one addition: a reply that waits on a gate, so a fill can be applied while a
//! submission is provably in flight. That is how the plan's fill-before-ACK case is driven without
//! a sleep deciding the outcome.
//!
//! The credential is the plan's own fake pair. No test reads the process environment, and none
//! makes a request that is not answered by the mock in the same test.

use std::{
    cell::RefCell,
    collections::VecDeque,
    net::SocketAddr,
    num::NonZeroU32,
    rc::Rc,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::{replace_data_event_sender, replace_exec_event_sender},
    messages::{
        DataEvent, ExecutionEvent,
        execution::{
            CancelAllOrders, CancelOrder, ExecutionReport, GenerateFillReportsBuilder,
            GenerateOrderStatusReport, GenerateOrderStatusReportsBuilder, ModifyOrder, QueryOrder,
            SubmitOrder, SubmitOrderList,
        },
    },
    testing::wait_until_async,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::OrderEventAny,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TradeId,
        TraderId, VenueOrderId,
    },
    orders::{Order, OrderAny, OrderList, builder::OrderTestBuilder},
    reports::{FillReport, OrderStatusReport},
    types::{Currency, Price, Quantity},
};
use nautilus_network::ratelimiter::quota::Quota;
use nautilus_ondo::{
    common::{
        consts::{ONDO_SETTLEMENT_CURRENCY, ONDO_VENUE},
        credential::OndoCredential,
        enums::OndoEnvironment,
        parse::parse_timestamp,
    },
    config::OndoExecutionClientConfig,
    execution::{OndoExecutionClient, OndoFillApplication, OndoOrderApplication},
    http::{
        orders::{OndoApiOrder, OndoOrderStatus},
        private::OndoApiFill,
        rate_limit::OndoRateBudget,
    },
    signing::{ONDO_KEY_ID_HEADER, ONDO_SIGN_HEADER, ONDO_TIMESTAMP_HEADER},
};
use rstest::rstest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc::UnboundedReceiver},
    task::JoinHandle,
};

const ACCOUNT_ID: &str = "ONDO-SANDBOX-001";
const CLIENT_ID: &str = "ONDO-EXEC";
/// The plan's own fake credential. It is never a real key and never leaves this process.
const TEST_KEY_ID: &str = "ondoKeyId_UNIT_TEST_ONLY";
const TEST_API_SECRET: &str = "ondoApiSecret_UNIT_TEST_ONLY";
const NVDA: &str = "NVDA-USD-PERP.ONDO";
const VENUE_ORDER_ID: &str = "197ec08e001658690721be129e7fa595";
const CLIENT_ORDER_ID: &str = "ondo_probe_1";

// ------------------------------------------------------------------------------------------------
// Scripted mock HTTP server (the harness `tests/http_client.rs` establishes)
// ------------------------------------------------------------------------------------------------

/// One request as the mock server saw it.
#[derive(Debug, Clone)]
struct CapturedRequest {
    method: String,
    target: String,
    body: String,
    head: String,
}

/// One scripted reply. The last reply of a script is sticky, so a script of one reply answers every
/// connection the same way.
#[derive(Debug, Clone)]
enum Reply {
    /// Answer with this status and body.
    Answer { status: u16, body: String },
    /// Wait until the gate is released, then answer `200 OK` with this body.
    ///
    /// Drives an exact race: the test can act while the request is provably in flight, instead of
    /// hoping a sleep was long enough.
    Gated { gate: Arc<Notify>, body: String },
}

impl Reply {
    fn ok(body: impl Into<String>) -> Self {
        Self::Answer {
            status: 200,
            body: body.into(),
        }
    }

    fn answer(status: u16, body: impl Into<String>) -> Self {
        Self::Answer {
            status,
            body: body.into(),
        }
    }
}

/// A scripted HTTP/1.1 server bound inside the test process.
#[derive(Debug)]
struct MockServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    handle: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl MockServer {
    async fn start(script: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the mock server");
        let addr = listener.local_addr().expect("read the mock server address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let script = Arc::new(Mutex::new(VecDeque::from(script)));

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let script = Arc::clone(&script);
                let seen = Arc::clone(&seen);

                tokio::spawn(async move {
                    serve_connection(stream, script, seen).await;
                });
            }
        });

        Self {
            addr,
            requests,
            handle,
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn captured(&self) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .expect("mock server request log")
            .clone()
    }

    fn targets(&self) -> Vec<String> {
        self.captured()
            .into_iter()
            .map(|request| request.target)
            .collect()
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    script: Arc<Mutex<VecDeque<Reply>>>,
    seen: Arc<Mutex<Vec<CapturedRequest>>>,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];

    loop {
        let read = match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        buffer.extend_from_slice(&chunk[..read]);

        if let Some(request) = parse_request(&buffer) {
            seen.lock().expect("mock server request log").push(request);
            break;
        }
    }

    let reply = {
        let mut script = script.lock().expect("mock server script");
        if script.len() > 1 {
            script.pop_front()
        } else {
            script.front().cloned()
        }
    };

    match reply {
        Some(Reply::Answer { status, body }) => write_response(&mut stream, status, &body).await,
        Some(Reply::Gated { gate, body }) => {
            gate.notified().await;
            write_response(&mut stream, 200, &body).await;
        }
        None => drop(stream),
    }
}

/// Parses one HTTP/1.1 request once its body is complete.
fn parse_request(buffer: &[u8]) -> Option<CapturedRequest> {
    let text = String::from_utf8_lossy(buffer);
    let (head, rest) = text.split_once("\r\n\r\n")?;

    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _value)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_name, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);

    if rest.len() < content_length {
        return None;
    }

    Some(CapturedRequest {
        method,
        target,
        body: rest[..content_length].to_string(),
        head: head.to_string(),
    })
}

async fn write_response(stream: &mut TcpStream, status: u16, body: &str) {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Status",
    };

    let response = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );

    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

// ------------------------------------------------------------------------------------------------
// Payload fixtures
// ------------------------------------------------------------------------------------------------

/// Wraps a `result` payload in the `GenericResponse` envelope every documented endpoint uses.
fn envelope(result: &str) -> String {
    format!(r#"{{"success":true,"result":{result}}}"#)
}

/// One `ApiOrder` in the documented shape.
fn order_json(
    order_id: &str,
    client_order_id: &str,
    status: &str,
    size: &str,
    filled_size: &str,
    fee: &str,
    last_fill_size: &str,
) -> String {
    format!(
        r#"{{"orderId":"{order_id}","clientOrderId":"{client_order_id}","side":"buy","price":"227.50","size":"{size}","market":"NVDA-USD.P","filledSize":"{filled_size}","lastFillSize":"{last_fill_size}","filledCost":"0.00","fee":"{fee}","status":"{status}","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}}"#,
    )
}

/// An `ApiOrder` carrying a status this adapter does not know.
fn unknown_status_order_json(status: &str) -> String {
    format!(
        r#"{{"orderId":"{VENUE_ORDER_ID}","clientOrderId":"{CLIENT_ORDER_ID}","side":"buy","price":"227.50","size":"1.00","market":"NVDA-USD.P","filledSize":"0.00","lastFillSize":"0.00","filledCost":"0.00","fee":"0.00","status":"{status}","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}}"#,
    )
}

/// One `ApiFill` in the documented shape, parsed exactly as the client parses a live one.
fn fill(id: &str, size: &str, fee: &str) -> OndoApiFill {
    fill_for(id, VENUE_ORDER_ID, CLIENT_ORDER_ID, size, fee, "0.01")
}

fn fill_for(
    id: &str,
    order_id: &str,
    client_order_id: &str,
    size: &str,
    fee: &str,
    price: &str,
) -> OndoApiFill {
    let text = format!(
        r#"{{"id":"{id}","orderId":"{order_id}","clientOrderId":"{client_order_id}","market":"NVDA-USD.P","price":"{price}","size":"{size}","side":"buy","direction":"openLong","fee":"{fee}","time":"2025-03-05T14:30:01.000000000Z","isMaker":false}}"#,
    );

    OndoApiFill::from_raw(&serde_json::value::RawValue::from_string(text).expect("JSON"))
        .expect("the fill fixture is an ApiFill")
}

fn fill_json(fill: &OndoApiFill) -> String {
    // The fill's own raw text is not exposed, so it is rebuilt from the members the fixture set.
    format!(
        r#"{{"id":"{}","orderId":"{}","clientOrderId":"{}","market":"{}","price":"{}","size":"{}","side":"{}","direction":"{}","fee":"{}","time":"{}","isMaker":{}}}"#,
        fill.id(),
        fill.order_id(),
        fill.client_order_id().unwrap_or_default(),
        fill.market(),
        fill.price().unwrap_or_default(),
        fill.size().unwrap_or_default(),
        fill.side().unwrap_or_default(),
        fill.direction().as_str(),
        fill.fee().unwrap_or_default(),
        fill.time().unwrap_or_default(),
        fill.is_maker().unwrap_or(false),
    )
}

// ------------------------------------------------------------------------------------------------
// Harness
// ------------------------------------------------------------------------------------------------

struct Harness {
    client: OndoExecutionClient,
    exec_rx: UnboundedReceiver<ExecutionEvent>,
    cache: Rc<RefCell<Cache>>,
}

fn build_harness(mock: &MockServer, config: OndoExecutionClientConfig) -> Harness {
    let account_id = AccountId::from(ACCOUNT_ID);
    let cache = Rc::new(RefCell::new(Cache::default()));

    let core = ExecutionClientCore::new(
        TraderId::from("TESTER-001"),
        ClientId::from(CLIENT_ID),
        *ONDO_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None, // base_currency
        cache.clone(),
    );

    let config = OndoExecutionClientConfig {
        base_url_http: Some(mock.url()),
        account_id: Some(account_id),
        ..config
    };

    let credential = OndoCredential::new(
        OndoEnvironment::Sandbox,
        TEST_KEY_ID.to_string(),
        TEST_API_SECRET.to_string(),
    )
    .expect("the plan's fake credential is well formed");

    // The venue's production budget is one request per second, which a test that makes four
    // requests does not need to wait for: only the *sharing* is the property under test.
    let budget = OndoRateBudget::with_quota(
        Quota::per_second(NonZeroU32::new(1_000).expect("a nonzero quota"))
            .expect("a burst this size replenishes"),
    );

    let (exec_tx, exec_rx) = tokio::sync::mpsc::unbounded_channel();
    let (data_tx, _data_rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_exec_event_sender(exec_tx);
    replace_data_event_sender(data_tx);

    let client = OndoExecutionClient::with_credential(core, config, Some(credential), Some(budget))
        .expect("the client builds");

    Harness {
        client,
        exec_rx,
        cache,
    }
}

/// A configuration with the sandbox credential pair and no base URL of its own.
fn sandbox_config() -> OndoExecutionClientConfig {
    OndoExecutionClientConfig {
        environment: OndoEnvironment::Sandbox,
        account_id: Some(AccountId::from(ACCOUNT_ID)),
        api_key: Some(TEST_KEY_ID.to_string()),
        api_secret: Some(TEST_API_SECRET.to_string()),
        ..Default::default()
    }
}

/// Seeds `order` into the cache, exactly as the execution engine does before a submit command.
fn seed_order(harness: &Harness, order: &OrderAny) {
    harness
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("the order should enter the cache");
}

fn limit_order(client_order_id: &str, side: OrderSide) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(NVDA))
        .client_order_id(ClientOrderId::from(client_order_id))
        .side(side)
        .quantity(Quantity::from("1.00"))
        .price(nautilus_model::types::Price::from("227.50"))
        .time_in_force(TimeInForce::Gtc)
        .build()
}

fn market_order(client_order_id: &str) -> OrderAny {
    OrderTestBuilder::new(OrderType::Market)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(NVDA))
        .client_order_id(ClientOrderId::from(client_order_id))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.50"))
        .time_in_force(TimeInForce::Ioc)
        .build()
}

fn submit_command(order: &OrderAny) -> SubmitOrder {
    SubmitOrder::new(
        order.trader_id(),
        Some(ClientId::from(CLIENT_ID)),
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

fn cancel_command(order: &OrderAny, venue_order_id: Option<VenueOrderId>) -> CancelOrder {
    CancelOrder::new(
        order.trader_id(),
        Some(ClientId::from(CLIENT_ID)),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        venue_order_id,
        UUID4::new(),
        UnixNanos::default(),
        None, // params
        None, // correlation_id
    )
}

/// A market-wide cancel for the test instrument, optionally side-filtered.
fn cancel_all_command(order_side: Option<OrderSide>) -> CancelAllOrders {
    CancelAllOrders::new(
        TraderId::from("TESTER-001"),
        Some(ClientId::from(CLIENT_ID)),
        StrategyId::from("S-001"),
        InstrumentId::from(NVDA),
        order_side,
        UUID4::new(),
        UnixNanos::default(),
        None, // params
        None, // correlation_id
    )
}

/// Drains whatever the client has emitted so far, without waiting.
fn drain(harness: &mut Harness) -> Vec<ExecutionEvent> {
    let mut events = Vec::new();
    while let Ok(event) = harness.exec_rx.try_recv() {
        events.push(event);
    }
    events
}

/// Appends events to `events` until `done` holds for the accumulated sequence.
///
/// The events are accumulated rather than re-drained, so a condition that has to observe an
/// event which arrived between two polls still sees it.
async fn collect_until(
    harness: &mut Harness,
    events: &mut Vec<ExecutionEvent>,
    mut done: impl FnMut(&[ExecutionEvent]) -> bool,
) {
    let start = Instant::now();

    loop {
        while let Ok(event) = harness.exec_rx.try_recv() {
            events.push(event);
        }

        if done(events) {
            return;
        }

        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "timed out after {:?} with {} events",
            start.elapsed(),
            events.len(),
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The order events among `events`.
fn order_events(events: &[ExecutionEvent]) -> Vec<&OrderEventAny> {
    events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Order(order) => Some(order),
            _ => None,
        })
        .collect()
}

/// Every fill report among `events`, whichever envelope carried it.
fn fill_reports(events: &[ExecutionEvent]) -> Vec<FillReport> {
    events
        .iter()
        .flat_map(|event| match event {
            ExecutionEvent::Report(ExecutionReport::Fill(report)) => vec![(**report).clone()],
            ExecutionEvent::Report(ExecutionReport::OrderWithFills(_report, fills)) => {
                fills.clone()
            }
            _ => Vec::new(),
        })
        .collect()
}

/// Every order status report among `events`, whichever envelope carried it.
fn order_reports(events: &[ExecutionEvent]) -> Vec<OrderStatusReport> {
    events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Report(ExecutionReport::Order(report)) => Some((**report).clone()),
            ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, _fills)) => {
                Some((**report).clone())
            }
            _ => None,
        })
        .collect()
}

fn total_filled(reports: &[FillReport]) -> Quantity {
    reports.iter().fold(Quantity::from("0"), |total, report| {
        Quantity::from_decimal(total.as_decimal() + report.last_qty.as_decimal())
            .expect("the sum of two fill quantities is representable")
    })
}

fn total_commission(reports: &[FillReport]) -> rust_decimal::Decimal {
    reports
        .iter()
        .fold(rust_decimal::Decimal::ZERO, |total, report| {
            total + report.commission.as_decimal()
        })
}

/// Waits until the mock server has received `count` requests.
async fn wait_for_requests(mock: &MockServer, count: usize) {
    wait_until_async(
        || async { mock.captured().len() >= count },
        Duration::from_secs(5),
    )
    .await;
}

// ------------------------------------------------------------------------------------------------
// The plan's own scenario (plan §7, Task 7)
// ------------------------------------------------------------------------------------------------

/// plan §7 Task 7:
///
/// ```text
/// input: fill F1(qty=0.2), ACK O1, duplicate F1 via REST, fill F2(qty=0.3), cancel O1
/// expected: total_filled=0.5; exactly 2 fills; remaining canceled; fee charged twice, once per fill
/// ```
///
/// `F1` is applied while the submission is still in flight, so this is the fill-before-ACK case as
/// well: it must be held, not dropped and not double counted, and it must be reported after the
/// acknowledgement that makes it reportable.
#[rstest]
#[tokio::test]
async fn test_the_plans_scenario_two_fills_one_duplicate_and_a_cancel() {
    let gate = Arc::new(Notify::new());
    let mock = MockServer::start(vec![
        // The create answer, held open until the fill below has been applied.
        Reply::Gated {
            gate: Arc::clone(&gate),
            body: envelope(&order_json(
                VENUE_ORDER_ID,
                CLIENT_ORDER_ID,
                "open",
                "1.00",
                "0.00",
                "0.00",
                "0.00",
            )),
        },
        // The cancel answer: the venue's own post-cancel state, with the cumulative fee the
        // adapter must never charge a second time.
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "canceled",
            "1.00",
            "0.50",
            "0.025",
            "0.30",
        ))),
    ])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);

    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_requests(&mock, 1).await;

    // F1 arrives before the venue has acknowledged the order.
    let f1 = fill("f1", "0.20", "0.010");
    assert_eq!(
        harness.client.apply_fill(&f1).unwrap(),
        OndoFillApplication::Buffered,
        "a fill that precedes the order's ACK is held, not dropped",
    );

    gate.notify_one();

    let mut events = Vec::new();

    collect_until(&mut harness, &mut events, |events| {
        order_events(events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Accepted(_)))
    })
    .await;

    // The buffered fill is reported once the acknowledgement lands: it is not lost, and it is not
    // reported before the acknowledgement that makes it reportable.
    assert_eq!(
        fill_reports(&events).len(),
        1,
        "the fill that preceded the ACK is reported after it",
    );

    // F2 is a second fill of the same order, and the two must accumulate.
    let f2 = fill("f2", "0.30", "0.015");
    assert_eq!(
        harness.client.apply_fill(&f2).unwrap(),
        OndoFillApplication::Applied,
    );

    // ... and F1 delivered a second time, by the REST history this time, is the same fill.
    assert_eq!(
        harness.client.apply_fill(&f1).unwrap(),
        OndoFillApplication::Duplicate,
        "the same fill id is one fill, whichever source delivered it",
    );

    // Cancel O1. The venue order id is deliberately not supplied: the client must have mapped it.
    harness
        .client
        .cancel_order(cancel_command(&order, None))
        .expect("cancel");

    collect_until(&mut harness, &mut events, |events| {
        order_reports(events)
            .iter()
            .any(|report| report.order_status == OrderStatus::Canceled)
    })
    .await;

    let events = events;
    let fills = fill_reports(&events);
    let reports = order_reports(&events);

    assert_eq!(fills.len(), 2, "exactly two fills, one per fill id");
    assert_eq!(total_filled(&fills), Quantity::from("0.50"));
    assert_eq!(
        total_commission(&fills),
        rust_decimal::Decimal::new(25, 3),
        "the order-level fee of 0.025 is the sum of the two fills, never a third charge",
    );
    assert!(
        fills
            .iter()
            .all(|report| report.commission.currency == Currency::from(ONDO_SETTLEMENT_CURRENCY)),
        "every commission is in the settlement currency",
    );
    assert_eq!(
        fills
            .iter()
            .map(|report| report.trade_id.clone())
            .collect::<Vec<_>>(),
        vec![TradeId::from("f1"), TradeId::from("f2")],
    );

    let terminal = reports
        .iter()
        .find(|report| report.order_status == OrderStatus::Canceled)
        .expect("the cancel was reported");

    assert_eq!(terminal.filled_qty, Quantity::from("0.50"));
    assert_eq!(terminal.quantity, Quantity::from("1.00"));
    assert_eq!(terminal.venue_order_id, VenueOrderId::from(VENUE_ORDER_ID));
    assert_eq!(
        terminal.client_order_id,
        Some(ClientOrderId::from(CLIENT_ORDER_ID)),
    );
    assert_eq!(terminal.instrument_id, InstrumentId::from(NVDA));

    // The accepted acknowledgement arrived once, through the order event API.
    let accepted = order_events(&events)
        .iter()
        .filter(|event| matches!(event, OrderEventAny::Accepted(_)))
        .count();
    assert_eq!(accepted, 1, "one submission is acknowledged once");

    let state = harness
        .client
        .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
        .expect("the order is tracked");

    assert_eq!(state.filled, Quantity::from("0.50"));
    assert_eq!(state.venue_filled, Some(Quantity::from("0.50")));
    assert!(!state.is_unresolved());
    assert_eq!(
        state.last_fill_size,
        Some(Quantity::from("0.30")),
        "lastFillSize is recorded as the informational field it is",
    );
    assert_eq!(
        state.venue_fee,
        Some(rust_decimal::Decimal::new(25, 3)),
        "the venue's cumulative fee is kept as a diagnostic",
    );
    assert!(state.resolved, "the fills agree with the venue's own total");
    assert!(harness.client.unresolved_orders().is_empty());
    assert_eq!(harness.client.applied_fill_count(), 2);
}

// ------------------------------------------------------------------------------------------------
// Serialization and local refusals
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_a_limit_gtc_post_only_reduce_only_order_is_serialized_and_signed_as_one_request() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(NVDA))
        .client_order_id(ClientOrderId::from("ondo_probe_1"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.01"))
        .price(Price::from("100.00"))
        .time_in_force(TimeInForce::Gtc)
        .post_only(true)
        .reduce_only(true)
        .build();

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_requests(&mock, 1).await;

    let captured = mock.captured();

    assert_eq!(captured[0].method, "POST");
    assert_eq!(captured[0].target, "/v1/perps/orders");
    assert_eq!(
        captured[0].body,
        r#"{"market":"NVDA-USD.P","side":"buy","type":"limit","size":"0.01","price":"100.00","timeInForce":"GTC","postOnly":true,"reduceOnly":true,"clientOrderId":"ondo_probe_1"}"#,
        "the body is the plan's §6.2 example shape, exactly",
    );
    // The transport lowercases the header names it puts on the wire; the signature does not care.
    let head = captured[0].head.to_lowercase();

    assert!(
        head.contains(&ONDO_KEY_ID_HEADER.to_lowercase())
            && head.contains(&ONDO_TIMESTAMP_HEADER.to_lowercase())
            && head.contains(&ONDO_SIGN_HEADER.to_lowercase()),
        "every write is signed: {}",
        captured[0].head,
    );
    assert!(
        !captured[0].head.contains(TEST_API_SECRET),
        "the secret never travels as a header",
    );
}

#[rstest]
#[tokio::test]
async fn test_a_market_order_sends_the_base_size_and_neither_price_nor_time_in_force() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        "ondo_probe_market",
        "open",
        "0.50",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = market_order("ondo_probe_market");
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_requests(&mock, 1).await;

    assert_eq!(
        mock.captured()[0].body,
        r#"{"market":"NVDA-USD.P","side":"sell","type":"market","size":"0.50","postOnly":false,"reduceOnly":false,"clientOrderId":"ondo_probe_market"}"#,
    );
}

/// A `GTD` order is refused for the expiration it cannot be sent without, and a `FOK` order for
/// the time in force the schema does not carry. Both are refusals by name, before any request.
#[rstest]
#[tokio::test]
async fn test_a_limit_ioc_order_carries_ioc_and_the_size_is_the_base_quantity() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        "ondo_probe_ioc",
        "open",
        "0.25",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(NVDA))
        .client_order_id(ClientOrderId::from("ondo_probe_ioc"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.25"))
        .price(Price::from("228.75"))
        .time_in_force(TimeInForce::Ioc)
        .build();

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_requests(&mock, 1).await;

    assert_eq!(
        mock.captured()[0].body,
        r#"{"market":"NVDA-USD.P","side":"sell","type":"limit","size":"0.25","price":"228.75","timeInForce":"IOC","postOnly":false,"reduceOnly":false,"clientOrderId":"ondo_probe_ioc"}"#,
    );
}

#[rstest]
#[case::fok(TimeInForce::Fok, "unsupported_time_in_force")]
#[case::gtd(TimeInForce::Gtd, "expire_time_unsupported")]
#[tokio::test]
async fn test_a_time_in_force_outside_the_create_schema_is_denied_without_a_request(
    #[case] time_in_force: TimeInForce,
    #[case] expected: &str,
) {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let mut builder = OrderTestBuilder::new(OrderType::Limit);

    builder
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(NVDA))
        .client_order_id(ClientOrderId::from(CLIENT_ORDER_ID))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.00"))
        .price(Price::from("227.50"))
        .time_in_force(time_in_force);

    if time_in_force == TimeInForce::Gtd {
        // A GTD order carries an expiration; the venue's create schema has no member to send it
        // in, which is exactly why the command is refused rather than silently stripped.
        builder.expire_time(UnixNanos::from(1_800_000_000_000_000_000));
    }

    let order = builder.build();

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain(&mut harness);
    let denied: Vec<&OrderEventAny> = order_events(&events)
        .into_iter()
        .filter(|event| matches!(event, OrderEventAny::Denied(_)))
        .collect();

    assert_eq!(denied.len(), 1, "the command is denied, not submitted");
    let reason = match denied[0] {
        OrderEventAny::Denied(event) => event.reason.to_string(),
        other => panic!("expected a denial, was {other:?}"),
    };
    assert!(reason.contains(expected), "was `{reason}`");
    assert!(
        !reason.contains("submit-order-error"),
        "the refusal is local: `{reason}`",
    );
    assert!(
        mock.captured().is_empty(),
        "a refused command never becomes a request: {:?}",
        mock.targets(),
    );
    assert!(!harness.client.tracks(&ClientOrderId::from(CLIENT_ORDER_ID)));
}

/// A combination this adapter cannot express is refused by name, before any request, and never
/// converted into one it can.
///
/// Two refusals the command itself makes are deliberately absent here, because the Nautilus order
/// model cannot produce the order that would carry them: a `postOnly` **market** order (the model's
/// market order has no such member) and a non-positive quantity (the model refuses to construct
/// one). Both are covered where they are reachable, in `http::orders`' own tests.
#[rstest]
// A conditional order is refused for the trigger price it cannot be sent without - the first of
// the two things this phase cannot express about it.
#[case::conditional_order(
    OrderType::StopMarket,
    false,
    "ondo_probe_stop",
    "trigger_price_unsupported"
)]
#[case::quote_size_unsupported(
    OrderType::Limit,
    true,
    "ondo_probe_quote",
    "quote_size_unsupported"
)]
#[case::illegal_client_order_id(
    OrderType::Limit,
    false,
    "ondo probe id",
    "invalid_client_order_id"
)]
#[tokio::test]
async fn test_an_illegal_combination_is_denied_without_a_request(
    #[case] order_type: OrderType,
    #[case] quote_quantity: bool,
    #[case] client_order_id: &str,
    #[case] expected: &str,
) {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let mut builder = OrderTestBuilder::new(order_type);

    builder
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(NVDA))
        .client_order_id(ClientOrderId::from(client_order_id))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.00"))
        .quote_quantity(quote_quantity)
        .time_in_force(TimeInForce::Gtc);

    if order_type == OrderType::StopMarket {
        builder.trigger_price(Price::from("226.00"));
    } else {
        builder.price(Price::from("227.50"));
    }

    let order = builder.build();

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain(&mut harness);
    let reason = order_events(&events)
        .into_iter()
        .find_map(|event| match event {
            OrderEventAny::Denied(event) => Some(event.reason.to_string()),
            _ => None,
        })
        .expect("the command is denied, not submitted");

    assert!(reason.contains(expected), "was `{reason}`");
    assert!(
        mock.captured().is_empty(),
        "a refused command never becomes a request: {:?}",
        mock.targets(),
    );
}

#[rstest]
#[tokio::test]
async fn test_a_post_only_order_the_venue_refuses_keeps_the_venues_own_reason() {
    let mock = MockServer::start(vec![Reply::answer(
        400,
        r#"{"success":false,"errorCode":"post_only_has_match","error":"post only order would match"}"#,
    )])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_requests(&mock, 1).await;

    let mut events = Vec::new();

    collect_until(&mut harness, &mut events, |events| {
        order_events(events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Rejected(_)))
    })
    .await;

    let events = events;
    let rejected = order_events(&events)
        .into_iter()
        .find_map(|event| match event {
            OrderEventAny::Rejected(event) => Some(event.clone()),
            _ => None,
        })
        .expect("the refusal is reported as a rejection");

    assert!(
        rejected.due_post_only,
        "the rejection names the post-only rule"
    );
    assert!(
        rejected.reason.to_string().contains("post_only_has_match"),
        "the venue's own code is kept: {}",
        rejected.reason,
    );
    assert!(
        !harness.client.tracks(&ClientOrderId::from(CLIENT_ORDER_ID)),
        "an order that never rested is not left in flight",
    );
}

#[rstest]
#[tokio::test]
async fn test_a_modification_is_rejected_because_this_venue_has_no_atomic_amend() {
    let mock = MockServer::start(vec![Reply::ok("{}")]).await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);

    harness
        .client
        .modify_order(ModifyOrder::new(
            TraderId::from("TESTER-001"),
            Some(ClientId::from(CLIENT_ID)),
            StrategyId::from("S-001"),
            InstrumentId::from(NVDA),
            ClientOrderId::from(CLIENT_ORDER_ID),
            Some(VenueOrderId::from(VENUE_ORDER_ID)),
            Some(Quantity::from("0.50")),
            Some(nautilus_model::types::Price::from("228.00")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("modify");

    let events = drain(&mut harness);
    let modified = order_events(&events)
        .into_iter()
        .find_map(|event| match event {
            OrderEventAny::ModifyRejected(event) => Some(event.clone()),
            _ => None,
        })
        .expect("a modification this venue cannot express is rejected, not emulated");

    assert!(
        modified.reason.to_string().contains("atomic amend"),
        "the refusal names why: {}",
        modified.reason,
    );
    assert!(
        mock.captured().is_empty(),
        "a cancel-and-resubmit emulation would have sent a request; none was sent",
    );
}

// ------------------------------------------------------------------------------------------------
// Fills: dedup, buffering, accumulation, and what must never become a fill
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_two_fills_sharing_an_order_id_accumulate_until_the_venue_agrees() {
    let mock = MockServer::start(vec![
        // The create answer.
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "1.00",
            "0.00",
            "0.00",
            "0.00",
        ))),
        // The answer to the query below: the venue says the order is done, and its own filled
        // total is what the two applied fills add up to.
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "fullyfilled",
            "1.00",
            "0.60",
            "0.006",
            "0.40",
        ))),
    ])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_until_async(
        || async { !mock.captured().is_empty() },
        Duration::from_secs(5),
    )
    .await;
    wait_until_async(
        || async {
            harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .is_some_and(|state| state.accepted)
        },
        Duration::from_secs(5),
    )
    .await;

    let client_order_id = ClientOrderId::from(CLIENT_ORDER_ID);

    harness
        .client
        .apply_fill(&fill_for(
            "f1",
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "0.20",
            "0.002",
            "227.50",
        ))
        .unwrap();
    harness
        .client
        .apply_fill(&fill_for(
            "f2",
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "0.40",
            "0.004",
            "227.60",
        ))
        .unwrap();

    let state = harness.client.order_state(&client_order_id).unwrap();
    assert_eq!(state.filled, Quantity::from("0.60"));
    assert!(
        !state.resolved,
        "an `open` order is not a resolved one, whatever its fills add up to",
    );
    assert!(
        !state.is_unresolved(),
        "an order that is still working is not an order this adapter cannot account for",
    );
    assert!(
        harness.client.unresolved_orders().is_empty(),
        "a working order does not make an account unclear",
    );

    // The venue now says it is done with 0.60 filled, which is what the two fills add up to.
    let query = QueryOrder::new(
        TraderId::from("TESTER-001"),
        Some(ClientId::from(CLIENT_ID)),
        StrategyId::from("S-001"),
        InstrumentId::from(NVDA),
        client_order_id,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    harness.client.query_order(query).expect("query");

    wait_until_async(
        || async {
            harness
                .client
                .order_state(&client_order_id)
                .is_some_and(|state| state.resolved)
        },
        Duration::from_secs(5),
    )
    .await;

    let events = drain(&mut harness);
    let fills = fill_reports(&events);

    assert_eq!(fills.len(), 2);
    assert_eq!(total_filled(&fills), Quantity::from("0.60"));
    assert_eq!(harness.client.applied_fill_count(), 2);
    assert_eq!(
        mock.targets(),
        vec![
            "/v1/perps/orders".to_string(),
            format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
        ],
        "a fill is never a request of its own",
    );
}

#[rstest]
#[tokio::test]
async fn test_last_fill_size_is_an_informational_field_and_never_a_fill() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.20",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_until_async(
        || async {
            harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .is_some_and(|state| state.accepted)
        },
        Duration::from_secs(5),
    )
    .await;

    let events = drain(&mut harness);

    assert!(
        fill_reports(&events).is_empty(),
        "lastFillSize describes a fill the fills stream reports; it is not one itself",
    );

    let state = harness
        .client
        .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
        .unwrap();

    assert_eq!(state.filled, Quantity::from("0"));
    assert_eq!(state.last_fill_size, Some(Quantity::from("0.20")));
    assert_eq!(harness.client.applied_fill_count(), 0);
}

#[rstest]
#[tokio::test]
async fn test_a_fill_for_an_order_this_client_does_not_track_is_neither_recorded_nor_reported() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let application = harness
        .client
        .apply_fill(&fill_for(
            "f1",
            "someone-elses-order",
            "not_ours",
            "0.20",
            "0.01",
            "227.50",
        ))
        .unwrap();

    assert_eq!(application, OndoFillApplication::Untracked);
    assert_eq!(
        harness.client.applied_fill_count(),
        0,
        "an unattributable fill is not recorded, so it can still apply once the order is known",
    );
    assert!(drain(&mut harness).is_empty());
}

// ------------------------------------------------------------------------------------------------
// Batch submission
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_a_batch_2xx_reports_each_item_from_the_venues_own_answer() {
    let added_first = order_json(
        VENUE_ORDER_ID,
        "ondo_probe_a",
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    );
    let added_second = order_json(
        "second-venue-id",
        "ondo_probe_b",
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    );
    let refused = r#"{"order":{"clientOrderId":"ondo_probe_c","market":"NVDA-USD.P"},"error":"post only order would match","errorCode":"post_only_has_match"}"#;

    let mock = MockServer::start(vec![Reply::ok(envelope(&format!(
        r#"{{"addedOrders":[{added_first},{added_second}],"failedOrders":[{refused}]}}"#,
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let orders: Vec<OrderAny> = ["ondo_probe_a", "ondo_probe_b", "ondo_probe_c"]
        .iter()
        .map(|id| limit_order(id, OrderSide::Buy))
        .collect();

    for order in &orders {
        seed_order(&harness, order);
    }

    let command = SubmitOrderList::new(
        TraderId::from("TESTER-001"),
        Some(ClientId::from(CLIENT_ID)),
        StrategyId::from("S-001"),
        OrderList::new(
            OrderListId::new("OL-1"),
            InstrumentId::from(NVDA),
            StrategyId::from("S-001"),
            orders.iter().map(Order::client_order_id).collect(),
            UnixNanos::default(),
        ),
        orders
            .iter()
            .map(|order| order.init_event().clone())
            .collect(),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    harness
        .client
        .submit_order_list(command)
        .expect("submit the list");
    wait_for_requests(&mock, 1).await;

    assert_eq!(mock.captured()[0].target, "/v1/perps/orders/batch");
    assert!(
        mock.captured()[0]
            .body
            .starts_with(r#"{"orders":[{"market":"NVDA-USD.P""#),
        "the batch keeps the submitted order: {}",
        mock.captured()[0].body,
    );

    let mut events = Vec::new();

    collect_until(&mut harness, &mut events, |events| {
        order_events(events)
            .iter()
            .filter(|event| matches!(event, OrderEventAny::Rejected(_)))
            .count()
            == 1
    })
    .await;

    let events = events;
    let order_events = order_events(&events);

    let submitted = order_events
        .iter()
        .filter(|event| matches!(event, OrderEventAny::Submitted(_)))
        .count();
    let accepted = order_events
        .iter()
        .filter(|event| matches!(event, OrderEventAny::Accepted(_)))
        .count();
    let rejected: Vec<_> = order_events
        .iter()
        .filter_map(|event| match event {
            OrderEventAny::Rejected(event) => Some(event.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        submitted, 3,
        "each item is submitted before the batch is sent"
    );
    assert_eq!(accepted, 2, "the two added items are acknowledged");
    assert_eq!(rejected.len(), 1, "the refused item is rejected on its own");
    assert_eq!(
        rejected[0].client_order_id,
        ClientOrderId::from("ondo_probe_c"),
        "the refusal is attributed to the item the venue named",
    );
    assert!(rejected[0].due_post_only);
    assert!(
        rejected[0]
            .reason
            .to_string()
            .contains("post_only_has_match"),
        "was {}",
        rejected[0].reason,
    );

    assert!(harness.client.tracks(&ClientOrderId::from("ondo_probe_a")));
    assert!(harness.client.tracks(&ClientOrderId::from("ondo_probe_b")));
    assert!(
        !harness.client.tracks(&ClientOrderId::from("ondo_probe_c")),
        "the refused item is not left in flight",
    );
}

// ------------------------------------------------------------------------------------------------
// Cancellation
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_a_cancel_the_venue_refuses_with_a_race_code_triggers_a_confirming_query() {
    let mock = MockServer::start(vec![
        // The create answer.
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "1.00",
            "0.00",
            "0.00",
            "0.00",
        ))),
        // The cancel is refused with a code that means "ask again".
        Reply::answer(
            400,
            r#"{"success":false,"errorCode":"order_already_fully_filled","error":"order is already fully filled"}"#,
        ),
        // ... and the query says what actually happened.
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "fullyfilled",
            "1.00",
            "1.00",
            "0.01",
            "1.00",
        ))),
    ])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_until_async(
        || async {
            harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .is_some_and(|state| state.accepted)
        },
        Duration::from_secs(5),
    )
    .await;

    harness
        .client
        .cancel_order(cancel_command(
            &order,
            Some(VenueOrderId::from(VENUE_ORDER_ID)),
        ))
        .expect("cancel");

    let mut events = Vec::new();

    collect_until(&mut harness, &mut events, |events| {
        order_reports(events)
            .iter()
            .any(|report| report.order_status == OrderStatus::Filled)
    })
    .await;

    assert_eq!(
        mock.targets(),
        vec![
            "/v1/perps/orders".to_string(),
            format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
            format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
        ],
        "the refusal is answered by a query, not by calling the cancel API a second time",
    );
    assert_eq!(
        mock.captured()[1].method,
        "DELETE",
        "the cancel is a DELETE with no body",
    );
    assert!(mock.captured()[1].body.is_empty());
    assert_eq!(mock.captured()[2].method, "GET");

    let state = harness
        .client
        .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
        .unwrap();

    assert_eq!(state.status.as_str(), "fullyfilled");
    assert!(
        state.reconciliation_needed,
        "the venue filled 1.00 and no fill was applied, so the order is not resolved",
    );
    assert_eq!(
        harness.client.unresolved_orders(),
        vec![ClientOrderId::from(CLIENT_ORDER_ID)],
        "an account holding this order is not clean",
    );
}

#[rstest]
#[tokio::test]
async fn test_a_cancel_all_is_by_market_and_a_side_filtered_one_is_not_sent() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "canceled",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_until_async(
        || async {
            harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .is_some_and(|state| state.accepted)
        },
        Duration::from_secs(5),
    )
    .await;

    let side_filtered = CancelAllOrders::new(
        TraderId::from("TESTER-001"),
        Some(ClientId::from(CLIENT_ID)),
        StrategyId::from("S-001"),
        InstrumentId::from(NVDA),
        Some(OrderSide::Buy),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    harness
        .client
        .cancel_all_orders(side_filtered)
        .expect("cancel all");

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        mock.targets(),
        vec!["/v1/perps/orders".to_string()],
        "a side-filtered cancel-all is refused rather than sent as a whole-market cancel",
    );

    let unfiltered = CancelAllOrders::new(
        TraderId::from("TESTER-001"),
        Some(ClientId::from(CLIENT_ID)),
        StrategyId::from("S-001"),
        InstrumentId::from(NVDA),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    harness
        .client
        .cancel_all_orders(unfiltered)
        .expect("cancel all");

    wait_until_async(
        || async { mock.captured().len() >= 2 },
        Duration::from_secs(5),
    )
    .await;

    let captured = mock.captured();

    assert_eq!(captured[1].method, "DELETE");
    assert_eq!(captured[1].target, "/v1/perps/orders?market=NVDA-USD.P");
    assert!(captured[1].body.is_empty());
}

/// The frozen spec's own 200 for a market cancel carries no order - `{"success":true}` and nothing
/// else - so the cancel itself decides nothing, and the orders must not be left as they were.
///
/// This is the case that used to end in a warning and nothing else: the answer was read as a schema
/// failure, the failure was logged, and no confirming read ever ran, so an order the venue had
/// cancelled stayed `open` in this client forever. The confirming read is the venue's own order list
/// for that market, applied through the same payload path every other answer takes (plan §6.3).
#[rstest]
#[tokio::test]
async fn test_a_market_cancel_that_reports_no_order_is_confirmed_by_a_market_query() {
    let mock = MockServer::start(vec![
        // The create answer: the order exists and is working.
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "1.00",
            "0.00",
            "0.00",
            "0.00",
        ))),
        // The market cancel: the documented bare success, with no `result` at all.
        Reply::ok(r#"{"success":true}"#.to_string()),
        // The confirming read: the venue now reports the order cancelled.
        Reply::ok(envelope(&format!(
            "[{}]",
            order_json(
                VENUE_ORDER_ID,
                CLIENT_ORDER_ID,
                "canceled",
                "1.00",
                "0.00",
                "0.00",
                "0.00",
            ),
        ))),
    ])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_until_async(
        || async {
            harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .is_some_and(|state| state.accepted)
        },
        Duration::from_secs(5),
    )
    .await;

    harness
        .client
        .cancel_all_orders(cancel_all_command(None))
        .expect("cancel all");

    let mut events = Vec::new();
    collect_until(&mut harness, &mut events, |events| {
        order_reports(events)
            .iter()
            .any(|report| report.order_status == OrderStatus::Canceled)
    })
    .await;

    assert_eq!(
        mock.captured()
            .iter()
            .map(|request| (request.method.as_str(), request.target.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("POST", "/v1/perps/orders"),
            ("DELETE", "/v1/perps/orders?market=NVDA-USD.P"),
            ("GET", "/v1/perps/orders?market=NVDA-USD.P"),
        ],
        "a cancel that reported no order is confirmed by the venue's list for that market",
    );

    assert!(
        harness
            .client
            .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
            .is_some_and(|state| state.status == OndoOrderStatus::Canceled),
        "the order the venue reported cancelled is cancelled here too",
    );
}

// ------------------------------------------------------------------------------------------------
// Unknown state, and the queries that report it
// ------------------------------------------------------------------------------------------------

#[rstest]
#[case::unknown("settling")]
#[case::untriggered("untriggered")]
#[tokio::test]
async fn test_a_status_this_adapter_cannot_resolve_is_kept_raw_and_leaves_the_order_unresolved(
    #[case] status: &str,
) {
    let mock = MockServer::start(vec![Reply::ok(envelope(&unknown_status_order_json(
        status,
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_requests(&mock, 1).await;

    wait_until_async(
        || async {
            matches!(
                harness
                    .client
                    .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                    .map(|state| state.status.clone()),
                Some(OndoOrderStatus::Unknown(_)) | Some(OndoOrderStatus::Untriggered)
            )
        },
        Duration::from_secs(5),
    )
    .await;

    let events = drain(&mut harness);
    let state = harness
        .client
        .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
        .unwrap();

    assert_eq!(
        state.status.as_str(),
        status,
        "the raw status is kept verbatim"
    );
    assert!(!state.resolved, "nothing about this status is confirmed");
    assert!(state.last_raw.contains(status), "the payload is kept raw");
    assert!(
        order_reports(&events).is_empty(),
        "no status report states a state this adapter has not confirmed",
    );
    assert_eq!(
        harness.client.unresolved_orders(),
        vec![ClientOrderId::from(CLIENT_ORDER_ID)],
    );
}

/// The same acknowledgement delivered twice is one acknowledgement: a duplicate payload emits
/// nothing, and the submission is not accepted a second time (plan §6.3).
#[rstest]
#[tokio::test]
async fn test_a_repeated_acknowledgement_is_not_reported_twice() {
    let body = order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    );

    let mock = MockServer::start(vec![Reply::ok(envelope(&body))]).await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");

    let mut events = Vec::new();

    collect_until(&mut harness, &mut events, |events| {
        order_events(events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Accepted(_)))
    })
    .await;

    // The very same payload, applied again - the shape a repeated REST or stream delivery has.
    let payload = OndoApiOrder::from_text(&body).expect("the fixture is an ApiOrder");

    assert_eq!(
        harness.client.apply_order(&payload),
        OndoOrderApplication::Unchanged,
        "a payload that says nothing new emits nothing",
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    let events = {
        events.extend(drain(&mut harness));
        events
    };

    assert_eq!(
        order_events(&events)
            .iter()
            .filter(|event| matches!(event, OrderEventAny::Accepted(_)))
            .count(),
        1,
        "one submission is acknowledged once",
    );
}

#[rstest]
#[tokio::test]
async fn test_a_status_report_carries_the_instrument_the_order_id_and_the_settlement_currency() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.25",
        "0.001",
        "0.25",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_until_async(
        || async {
            harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .is_some_and(|state| state.accepted)
        },
        Duration::from_secs(5),
    )
    .await;
    drain(&mut harness);

    harness
        .client
        .apply_fill(&fill("f1", "0.25", "0.001"))
        .unwrap();

    let report = harness
        .client
        .generate_order_status_report(&GenerateOrderStatusReport {
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            instrument_id: Some(InstrumentId::from(NVDA)),
            client_order_id: Some(ClientOrderId::from(CLIENT_ORDER_ID)),
            venue_order_id: None,
            params: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .expect("the report is generated")
        .expect("the order is tracked");

    assert_eq!(report.instrument_id, InstrumentId::from(NVDA));
    assert_eq!(report.venue_order_id, VenueOrderId::from(VENUE_ORDER_ID));
    assert_eq!(
        report.client_order_id,
        Some(ClientOrderId::from(CLIENT_ORDER_ID)),
    );
    assert_eq!(report.account_id, AccountId::from(ACCOUNT_ID));
    assert_eq!(report.order_status, OrderStatus::PartiallyFilled);
    assert_eq!(report.filled_qty, Quantity::from("0.25"));
    assert_eq!(report.quantity, Quantity::from("1.00"));
    assert_eq!(report.post_only, order.is_post_only());

    let fill_mock = MockServer::start(vec![
        // The create answer, so the order this harness reads fills for exists.
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "1.00",
            "0.25",
            "0.001",
            "0.25",
        ))),
        // ... and then the fill history page.
        Reply::ok(envelope(&format!(
            "[{}]",
            fill_json(&fill("f1", "0.25", "0.001")),
        ))),
    ])
    .await;

    let mut fill_harness = build_harness(&fill_mock, sandbox_config());
    fill_harness.client.start().expect("start");
    fill_harness.client.connect().await.expect("connect");

    let fill_harness_order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&fill_harness, &fill_harness_order);
    fill_harness
        .client
        .submit_order(submit_command(&fill_harness_order))
        .expect("submit");
    wait_until_async(
        || async {
            fill_harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .is_some_and(|state| state.accepted)
        },
        Duration::from_secs(5),
    )
    .await;

    let reports = fill_harness
        .client
        .generate_fill_reports(
            GenerateFillReportsBuilder::default()
                .ts_init(UnixNanos::default())
                .venue_order_id(Some(VenueOrderId::from(VENUE_ORDER_ID)))
                .build()
                .expect("the command builds"),
        )
        .await
        .expect("the fill reports are generated");

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].instrument_id, InstrumentId::from(NVDA));
    assert_eq!(
        reports[0].venue_order_id,
        VenueOrderId::from(VENUE_ORDER_ID)
    );
    assert_eq!(reports[0].trade_id, TradeId::from("f1"));
    assert_eq!(
        reports[0].client_order_id,
        Some(ClientOrderId::from(CLIENT_ORDER_ID)),
    );
    assert_eq!(
        reports[0].commission.currency,
        Currency::from(ONDO_SETTLEMENT_CURRENCY),
    );
    assert_eq!(
        reports[0].commission.as_decimal(),
        rust_decimal::Decimal::new(1, 3)
    );
    assert_eq!(reports[0].liquidity_side, LiquiditySide::Taker);
    assert_eq!(reports[0].last_qty, Quantity::from("0.25"));
}

/// A fill whose `time` cannot be read is not a fill outside the window.
///
/// The reconciliation read applies the command's window locally, so a fill that cannot be placed in
/// time must not be measured against it. An unreadable `time` used to arrive as a zero `ts_event`,
/// which is before every window's start, so the fill was dropped without an error and the position
/// it belonged to could never square (plan §6.4). What the window *can* measure it still filters.
#[rstest]
#[tokio::test]
async fn test_a_fill_before_the_window_is_filtered_and_one_that_cannot_be_ordered_is_not() {
    // The fixture fill's own `time` is 2025-03-05, so a 2026 start puts it outside the window.
    let outdated = fill_json(&fill("outdated", "0.25", "0.001"));

    // The same fill with no `time` member at all: `time` is required by the spec, so this is a
    // payload the venue should never send - which is exactly why it must not be dropped silently.
    let unplaceable = format!(
        r#"{{"id":"unplaceable","orderId":"{VENUE_ORDER_ID}","clientOrderId":"{CLIENT_ORDER_ID}","market":"NVDA-USD.P","price":"0.01","size":"0.10","side":"buy","direction":"openLong","fee":"0.001","isMaker":false}}"#,
    );

    let mock = MockServer::start(vec![
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "1.00",
            "0.35",
            "0.002",
            "0.10",
        ))),
        Reply::ok(envelope(&format!("[{outdated},{unplaceable}]"))),
    ])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_until_async(
        || async {
            harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .is_some_and(|state| state.accepted)
        },
        Duration::from_secs(5),
    )
    .await;

    let reports = harness
        .client
        .generate_fill_reports(
            GenerateFillReportsBuilder::default()
                .ts_init(UnixNanos::default())
                .venue_order_id(Some(VenueOrderId::from(VENUE_ORDER_ID)))
                .start(Some(parse_timestamp("2026-01-01T00:00:00Z").unwrap()))
                .build()
                .expect("the command builds"),
        )
        .await
        .expect("the fill history is read");

    assert_eq!(
        reports.len(),
        1,
        "the fill outside the window is filtered; the one that cannot be placed in time is not",
    );
    assert_eq!(reports[0].trade_id, TradeId::from("unplaceable"));
    assert_eq!(reports[0].last_qty, Quantity::from("0.10"));
    assert!(
        !reports[0].ts_event.is_zero(),
        "an unreadable fill time never becomes a zero event time",
    );
}

/// `fee` is one of the ten members the frozen spec's `ApiFill` **requires**, so a fill without one
/// is a payload this adapter does not understand - not a free trade.
///
/// Booking it as a zero commission would put a fabricated cost into the PnL, and nothing downstream
/// could tell that zero from a real one.
#[rstest]
#[tokio::test]
async fn test_a_fill_whose_fee_cannot_be_read_is_refused_rather_than_booked_as_free() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = build_harness(&mock, sandbox_config());
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_until_async(
        || async {
            harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .is_some_and(|state| state.accepted)
        },
        Duration::from_secs(5),
    )
    .await;
    drain(&mut harness);

    let text = format!(
        r#"{{"id":"ffee1e55","orderId":"{VENUE_ORDER_ID}","clientOrderId":"{CLIENT_ORDER_ID}","market":"NVDA-USD.P","price":"0.01","size":"0.25","side":"buy","direction":"openLong","time":"2025-03-05T14:30:01.000000000Z","isMaker":false}}"#,
    );
    let feeleless = OndoApiFill::from_raw(
        &serde_json::value::RawValue::from_string(text).expect("the fixture is JSON"),
    )
    .expect("a spec-required member being absent does not stop it being parsed");

    let error = harness
        .client
        .apply_fill(&feeleless)
        .expect_err("a fill with no fee is not one this adapter can book");

    assert!(error.to_string().contains("fee"), "{error}");
    assert_eq!(
        harness.client.applied_fill_count(),
        0,
        "nothing is recorded, so a corrected delivery still applies",
    );
    assert!(
        fill_reports(&drain(&mut harness)).is_empty(),
        "and no fill is reported with an invented zero fee",
    );
}

#[rstest]
#[tokio::test]
async fn test_an_order_this_client_did_not_place_is_reported_rather_than_dropped() {
    let external = r#"{"orderId":"external-1","clientOrderId":"manual_order","side":"sell","price":"227.50","size":"2.00","market":"NVDA-USD.P","filledSize":"1.00","lastFillSize":"1.00","filledCost":"227.50","fee":"0.01","status":"open","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}"#;

    let mock = MockServer::start(vec![Reply::ok(envelope(&format!("[{external}]")))]).await;

    let harness = build_harness(&mock, sandbox_config());

    let command = GenerateOrderStatusReportsBuilder::default()
        .ts_init(UnixNanos::default())
        .open_only(false)
        .build()
        .expect("the command builds");

    let reports = harness
        .client
        .generate_order_status_reports(&command)
        .await
        .expect("the order history is read");

    assert_eq!(
        reports.len(),
        1,
        "an external order is reported, not dropped"
    );
    assert_eq!(reports[0].venue_order_id, VenueOrderId::from("external-1"));
    assert_eq!(
        reports[0].client_order_id,
        Some(ClientOrderId::from("manual_order")),
        "the venue's own echo is kept even for an order this client did not place",
    );
    assert_eq!(reports[0].instrument_id, InstrumentId::from(NVDA));
    assert_eq!(reports[0].order_status, OrderStatus::PartiallyFilled);
    assert_eq!(reports[0].filled_qty, Quantity::from("1.00"));
}

// ------------------------------------------------------------------------------------------------
// The production gate
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_production_order_entry_is_refused_before_any_request_exists() {
    let mock = MockServer::start(vec![Reply::ok(envelope("{}"))]).await;

    for config in [
        OndoExecutionClientConfig {
            environment: OndoEnvironment::Production,
            ..sandbox_config()
        },
        OndoExecutionClientConfig {
            allow_production_orders: true,
            ..sandbox_config()
        },
    ] {
        let account_id = AccountId::from(ACCOUNT_ID);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            ClientId::from(CLIENT_ID),
            *ONDO_VENUE,
            OmsType::Netting,
            account_id,
            AccountType::Margin,
            None,
            cache,
        );

        let config = OndoExecutionClientConfig {
            base_url_http: Some(mock.url()),
            ..config
        };

        let built = OndoExecutionClient::with_credential(core, config, None, None);
        let error = match built {
            Ok(_) => panic!("there is no production write branch, whatever the configuration says"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("production"),
            "the refusal names production: `{error}`",
        );
    }

    assert!(
        mock.captured().is_empty(),
        "the refusal happens before a socket is opened",
    );
}

/// A configuration carrying no credential pair takes the environment's, and a machine that has
/// none is refused rather than falling back to another account.
///
/// The refusal is asserted only when this process genuinely has no sandbox credential to resolve:
/// the acceptance run sets those variables, and a test that failed because a key exists would be
/// testing the machine rather than the adapter. Either way nothing is sent, which is asserted
/// unconditionally.
#[rstest]
#[tokio::test]
async fn test_a_client_without_a_credential_never_falls_back_and_never_sends() {
    let mock = MockServer::start(vec![Reply::ok(envelope("{}"))]).await;

    let account_id = AccountId::from(ACCOUNT_ID);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let core = ExecutionClientCore::new(
        TraderId::from("TESTER-001"),
        ClientId::from(CLIENT_ID),
        *ONDO_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache,
    );

    let built = OndoExecutionClient::with_credential(
        core,
        OndoExecutionClientConfig {
            environment: OndoEnvironment::Sandbox,
            account_id: Some(account_id),
            base_url_http: Some(mock.url()),
            ..Default::default()
        },
        None, // credential: resolved from the process environment
        None,
    );

    let environment_has_a_credential = std::env::var("ONDO_SANDBOX_API_KEY").is_ok()
        || std::env::var("ONDO_SANDBOX_API_SECRET").is_ok();

    match built {
        Ok(client) => {
            assert!(
                environment_has_a_credential,
                "the client resolved a credential this process does not have",
            );
            assert!(
                !client.is_connected() && client.tracked_order_count() == 0,
                "constructing a client performs no I/O and holds no state",
            );
        }
        Err(error) => {
            assert!(
                !environment_has_a_credential,
                "a credential was available and the client refused it anyway: `{error}`",
            );
            assert!(
                error.to_string().contains("ONDO_SANDBOX_API_KEY"),
                "the error names the variable that was missing: `{error}`",
            );
        }
    }

    assert!(
        mock.captured().is_empty(),
        "no credential is ever sent, and no request is made to find one",
    );
}
