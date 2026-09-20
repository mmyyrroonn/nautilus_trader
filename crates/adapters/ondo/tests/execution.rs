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
            GenerateOrderStatusReport, GenerateOrderStatusReportsBuilder,
            GeneratePositionStatusReports, ModifyOrder, QueryOrder, SubmitOrder, SubmitOrderList,
        },
    },
    testing::wait_until_async,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{
        AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide,
        TimeInForce,
    },
    events::AccountState,
    events::OrderEventAny,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TradeId,
        TraderId, VenueOrderId,
    },
    orders::{Order, OrderAny, OrderList, builder::OrderTestBuilder},
    reports::{FillReport, OrderStatusReport},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_network::ratelimiter::quota::Quota;
use nautilus_ondo::{
    common::{
        consts::{ONDO_SETTLEMENT_CURRENCY, ONDO_VENUE},
        credential::{OndoCredential, OndoEnvironmentError},
        enums::{OndoAccountIdentity, OndoAuthenticationScope, OndoEnvironment},
        parse::parse_timestamp,
    },
    config::{OndoExecutionClientConfig, OndoExecutionConfigError},
    execution::{OndoExecutionClient, OndoFillApplication, OndoOrderApplication},
    http::{
        error::OndoHttpError,
        orders::{OndoApiOrder, OndoOrderStatus},
        private::{FUNDING_FEES_PATH, OndoApiFill},
        rate_limit::{ONDO_REST_BUCKET, OndoRateBudget, OndoRequestPriority},
    },
    reconciliation::{
        Finding, LiquidationState, MetadataValidity, NewRiskRefusal, ReconciliationState,
        RecoveryPassRefusal,
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
use ustr::Ustr;

const ACCOUNT_ID: &str = "ONDO-SANDBOX-001";
const CLIENT_ID: &str = "ONDO-EXEC";
/// The plan's own fake credential. It is never a real key and never leaves this process.
const TEST_KEY_ID: &str = "ondoKeyId_UNIT_TEST_ONLY";
const TEST_API_SECRET: &str = "ondoApiSecret_UNIT_TEST_ONLY";
const NVDA: &str = "NVDA-USD-PERP.ONDO";
const NVDA_MARKET: &str = "NVDA-USD.P";
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
    /// A server whose script answers the two reconciliation passes a submission is admitted after,
    /// then the given replies.
    ///
    /// New risk is refused from construction (plan §6.4), so every test that submits starts here:
    /// the eight recovery reads come first, and the request assertions below count from there.
    async fn start_admitted(script: Vec<Reply>) -> Self {
        Self::start(admitted_script(script)).await
    }

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

    /// The requests the venue would have answered with this method.
    fn with_method(&self, method: &str) -> Vec<CapturedRequest> {
        self.captured()
            .into_iter()
            .filter(|request| request.method == method)
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
            // The execution client's private transport is pointed at this same address (see
            // `build_harness`), and a WebSocket upgrade is not one of this harness's scripted REST
            // replies: answering it from the script would shift every reply the test scripted. It
            // is refused here and consumes nothing, which is what "this harness serves REST only"
            // means - and it is why the private session stays unauthenticated in these tests.
            if is_websocket_upgrade(&buffer) {
                write_response(&mut stream, 400, r#"{"success":false}"#).await;

                return;
            }

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

/// Whether one request asks for a WebSocket upgrade rather than being one of this harness's REST
/// calls.
fn is_websocket_upgrade(buffer: &[u8]) -> bool {
    let text = String::from_utf8_lossy(buffer);
    let head = text.split("\r\n\r\n").next().unwrap_or_default();

    head.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("upgrade") && value.trim().eq_ignore_ascii_case("websocket")
        })
    })
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

/// The same `ApiOrder` shape with the echoing `clientOrderId` member absent.
///
/// The frozen spec's single-order read answers an order the venue knows by its own id, and the
/// member is optional there: a payload without it is a payload whose only identity this client can
/// resolve is the one it recorded itself.
fn order_json_without_client_order_id(status: &str, filled_size: &str) -> String {
    format!(
        r#"{{"orderId":"{VENUE_ORDER_ID}","side":"buy","price":"227.50","size":"1.00","market":"NVDA-USD.P","filledSize":"{filled_size}","lastFillSize":"{filled_size}","filledCost":"0.00","fee":"0.00","status":"{status}","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}}"#,
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

/// The budget an ordinary harness shares.
///
/// The venue's production budget is one request per second, which a test that makes four requests
/// does not need to wait for: only the *sharing* is the property under test.
fn test_budget() -> OndoRateBudget {
    OndoRateBudget::with_quota(
        Quota::per_second(NonZeroU32::new(1_000).expect("a nonzero quota"))
            .expect("a burst this size replenishes"),
    )
}

/// How long a spent budget takes to replenish one request: the window a queued request waits in.
const QUEUE_WINDOW: Duration = Duration::from_secs(1);

/// The budget a test drives by hand: enough cells for the harness's own reads, and one more per
/// [`QUEUE_WINDOW`] after that.
///
/// The venue's own budget is one request per second; what a test of the wait needs is not the same
/// number but the same shape - cells it can spend itself, so that the next request has to wait for a
/// replenishment that is out of its hands.
fn paced_budget() -> OndoRateBudget {
    OndoRateBudget::with_quota(
        Quota::with_period(QUEUE_WINDOW)
            .expect("a one-second period is not zero")
            .allow_burst(NonZeroU32::new(16).expect("a nonzero burst")),
    )
}

/// Empties the shared budget, leaving the next acquisition to wait for a replenishment.
///
/// `check_key` is the same decision `acquire` makes, without the wait, so this establishes that the
/// bucket is empty rather than assuming how many cells a replenishment left in it: a quota's burst
/// tolerance is what makes "spend one and the next waits" an assumption and not a fact. The bucket
/// stays empty for [`QUEUE_WINDOW`] afterwards - which is the window a queued request waits in, and
/// the reason a test can act on a submission while it is provably still queued.
fn drain_budget(budget: &OndoRateBudget) {
    while budget
        .limiter()
        .check_key(&Ustr::from(ONDO_REST_BUCKET))
        .is_ok()
    {}
}

fn build_harness(mock: &MockServer, config: OndoExecutionClientConfig) -> Harness {
    build_harness_on_budget(mock, config, test_budget())
}

/// [`build_harness`] on a budget the caller keeps a handle on.
///
/// The client is built with a clone of `budget`, so the test's own acquisitions draw on the same
/// bucket the client's requests do - which is what lets a test empty it first.
fn build_harness_on_budget(
    mock: &MockServer,
    config: OndoExecutionClientConfig,
    budget: OndoRateBudget,
) -> Harness {
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
        // The private transport dials the same loopback authority, which refuses the upgrade: this
        // harness is a REST surface, and the account session stays unauthenticated here. The
        // endpoint is named rather than left to the environment default so that no test can reach
        // the venue's own host by omission.
        base_url_ws: Some(format!("ws://{}/ws", mock.addr)),
        account_id: Some(account_id),
        ..config
    };

    let credential = OndoCredential::new(
        config.environment,
        TEST_KEY_ID.to_string(),
        TEST_API_SECRET.to_string(),
    )
    .expect("the plan's fake credential is well formed");

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

/// The client's REST path draws on the budget it was built with, not on one of its own.
///
/// This is the client's half of the factory's resolution (finding F13): the factory resolves the
/// environment's budget and hands that instance to `with_credential`, and this asserts what the
/// client actually paces against - no request is sent here.
#[tokio::test]
async fn test_the_client_draws_on_the_budget_it_was_built_with() {
    let mock = MockServer::start(Vec::new()).await;
    let budget = paced_budget();
    let harness = build_harness_on_budget(&mock, sandbox_config(), budget.clone());

    assert!(
        Arc::ptr_eq(
            harness.client.http_client().budget().limiter(),
            budget.limiter()
        ),
        "an injected budget is the bucket the client's requests pace against"
    );
    assert!(
        !Arc::ptr_eq(
            harness.client.http_client().budget().limiter(),
            OndoRateBudget::new().limiter()
        ),
        "and the client did not mint one of its own beside it"
    );
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

/// A batch submission carrying `orders` as one native list.
fn order_list_command(orders: &[OrderAny]) -> SubmitOrderList {
    SubmitOrderList::new(
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

/// The reads the two reconciliation passes make before a test's own replies: five per pass.
const RECOVERY_READS: usize = 10;

/// Waits until the mock server has received `count` requests.
async fn wait_for_requests(mock: &MockServer, count: usize) {
    wait_until_async(
        || async { mock.captured().len() >= count },
        Duration::from_secs(5),
    )
    .await;
}

/// Waits until the mock server has received `count` requests of this test's own.
async fn wait_for_writes(mock: &MockServer, count: usize) {
    wait_for_requests(mock, count + RECOVERY_READS).await;
}

/// The requests that could change something at the venue, in the order they arrived.
fn writes(mock: &MockServer) -> Vec<CapturedRequest> {
    mock.captured()
        .into_iter()
        .filter(|request| request.method != "GET")
        .collect()
}

// ------------------------------------------------------------------------------------------------
// The recovered account a submission is admitted under
// ------------------------------------------------------------------------------------------------

/// The balance summary the venue answers a pass's last read with.
const BALANCE_BODY: &str = r#"{"walletBalance":"5000.00","realizedPnl":"0.00","unrealizedPnl":"0.00","marginBalance":"5000.00","usedMargin":"0.00","availableMargin":"5000.00","withdrawableMargin":"5000.00","maintenanceMarginRequirement":"0.00","totalMaintenanceMargin":"0.00","marginRatio":"0.00","leverage":"0.00","underLiquidation":false,"totalFundingPayments":"0.00","totalTradingFees":"0.00","totalPnL":"0.00","netInvested":"5000.00"}"#;

/// One `ApiPosition` on NVDA, as the venue states a long of `net_quantity`.
fn position_json(net_quantity: &str) -> String {
    format!(
        r#"{{"market":"NVDA-USD.P","direction":"long","netQuantity":"{net_quantity}","averageEntryPrice":"227.50","usedMargin":"45.50","unrealizedPnl":"0.00","markPrice":"227.50","liquidationPrice":"180.00","bankruptcyPrice":"170.00","maintenanceMargin":"2.28","notionalValue":"45.50","leverage":"2.0","netFundingSinceNeutral":"0.00","returnOnEquity":"0.00"}}"#,
    )
}

/// The five reads one reconciliation pass makes when the account is empty and healthy, in the
/// order the pass makes them: orders, fills, positions, balance, funding.
fn clean_pass_reads() -> Vec<Reply> {
    vec![
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope(BALANCE_BODY)),
        Reply::ok(envelope("[]")),
    ]
}

/// `replies` behind the two agreeing passes a new order is admitted after.
fn admitted_script(replies: Vec<Reply>) -> Vec<Reply> {
    let mut script = clean_pass_reads();

    script.extend(clean_pass_reads());
    script.extend(replies);

    script
}

/// A harness whose account is recovered: current metadata, two agreeing passes, nothing unknown.
///
/// New risk is refused from construction (plan §6.4), so a test that submits has to establish this
/// first - and the mock has to be started on [`admitted_script`], which answers the ten reads the
/// two passes make before the test's own replies.
async fn recovered_harness(mock: &MockServer) -> Harness {
    recovered_harness_on(mock, test_budget()).await
}

/// [`recovered_harness`] on a budget the caller keeps a handle on.
async fn recovered_harness_on(mock: &MockServer, budget: OndoRateBudget) -> Harness {
    let mut harness = build_harness_on_budget(mock, sandbox_config(), budget);

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.set_metadata(MetadataValidity::Current);
    harness.client.begin_recovery(UnixNanos::default());
    harness
        .client
        .reconcile_account(UnixNanos::default())
        .await
        .expect("the first pass reads an empty account");
    harness
        .client
        .reconcile_account(UnixNanos::default())
        .await
        .expect("the second pass reads an empty account");

    assert!(
        harness.client.can_submit_new_orders(),
        "the harness is admitted: a submission test needs an account new risk is allowed on",
    );

    harness
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
    let mock = MockServer::start_admitted(vec![
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

    let mut harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);

    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_writes(&mock, 1).await;

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
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let harness = recovered_harness(&mock).await;

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
    wait_for_writes(&mock, 1).await;

    let captured = writes(&mock);

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
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        "ondo_probe_market",
        "open",
        "0.50",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let harness = recovered_harness(&mock).await;

    let order = market_order("ondo_probe_market");
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_writes(&mock, 1).await;

    assert_eq!(
        writes(&mock)[0].body,
        r#"{"market":"NVDA-USD.P","side":"sell","type":"market","size":"0.50","postOnly":false,"reduceOnly":false,"clientOrderId":"ondo_probe_market"}"#,
    );
}

/// A `GTD` order is refused for the expiration it cannot be sent without, and a `FOK` order for
/// the time in force the schema does not carry. Both are refusals by name, before any request.
#[rstest]
#[tokio::test]
async fn test_a_limit_ioc_order_carries_ioc_and_the_size_is_the_base_quantity() {
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        "ondo_probe_ioc",
        "open",
        "0.25",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let harness = recovered_harness(&mock).await;

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
    wait_for_writes(&mock, 1).await;

    assert_eq!(
        writes(&mock)[0].body,
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
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = recovered_harness(&mock).await;

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
        mock.with_method("POST").is_empty(),
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
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = recovered_harness(&mock).await;

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
        mock.with_method("POST").is_empty(),
        "a refused command never becomes a request: {:?}",
        mock.targets(),
    );
}

#[rstest]
#[tokio::test]
async fn test_a_post_only_order_the_venue_refuses_keeps_the_venues_own_reason() {
    let mock = MockServer::start_admitted(vec![Reply::answer(
        400,
        r#"{"success":false,"errorCode":"post_only_has_match","error":"post only order would match"}"#,
    )])
    .await;

    let mut harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_writes(&mock, 1).await;

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
    let mock = MockServer::start_admitted(vec![
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

    let mut harness = recovered_harness(&mock).await;

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
        mock.targets()[RECOVERY_READS..],
        vec![
            "/v1/perps/orders".to_string(),
            format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
        ],
        "a fill is never a request of its own",
    );
}

// ------------------------------------------------------------------------------------------------
// A terminal reading and the fills that account for it (F14)
// ------------------------------------------------------------------------------------------------

/// One `ApiOrder` payload, read the way the REST pages and the private stream read one.
fn api_order(status: &str, filled_size: &str) -> OndoApiOrder {
    OndoApiOrder::from_text(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        status,
        "1.00",
        filled_size,
        "0.00",
        filled_size,
    ))
    .expect("the order fixture is an ApiOrder")
}

/// A recovered harness holding one order the venue has acknowledged, with no fill of it applied.
///
/// The order is submitted against the mock's `open` create answer, which is the state the payloads
/// below are applied to: accepted, unfilled, and tracked under its client order id.
async fn acknowledged_order(mock: &MockServer) -> Harness {
    let harness = recovered_harness(mock).await;
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
}

/// The `open` create answer every test below is admitted behind.
fn open_create_script() -> Vec<Reply> {
    vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))]
}

/// F14: the order stream and the fill stream are two streams, and nothing orders them against each
/// other. A terminal reading that arrives before the fills it counts used to set a flag that
/// nothing ever cleared, so the fills arriving afterwards left the order unresolved for good and no
/// later terminal reading could recover it. The disagreement is a state of the numbers: it holds
/// while they disagree and it is gone once they do not.
#[rstest]
#[tokio::test]
async fn test_a_terminal_reading_that_arrives_before_its_fills_is_settled_by_them() {
    let mock = MockServer::start_admitted(open_create_script()).await;
    let harness = acknowledged_order(&mock).await;
    let client_order_id = ClientOrderId::from(CLIENT_ORDER_ID);

    // The venue says the order is done, and the fill that did it has not arrived yet.
    assert_eq!(
        harness
            .client
            .apply_order(&api_order("fullyfilled", "1.00")),
        OndoOrderApplication::Applied,
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert_eq!(
        state.fill_gap(),
        Some(Quantity::from("1.00")),
        "the venue's terminal total is not one the applied fills account for",
    );
    assert!(state.is_unresolved());
    assert!(!state.resolved);
    assert_eq!(harness.client.unresolved_orders(), vec![client_order_id]);
    assert!(
        harness
            .client
            .order_state(&client_order_id)
            .unwrap()
            .resolved
            == false,
        "an order short of its terminal reading is not a resolved one",
    );

    // Then the fill arrives - the ordinary case, the two streams in the other order.
    assert_eq!(
        harness
            .client
            .apply_fill(&fill("f1", "1.00", "0.00"))
            .unwrap(),
        OndoFillApplication::Applied,
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert_eq!(
        state.fill_gap(),
        None,
        "the numbers agree, so nothing is left to reconcile",
    );
    assert!(
        state.resolved,
        "the fill that satisfied the terminal reading is what settles it, not another report",
    );
    assert!(!state.is_unresolved());
    assert!(harness.client.unresolved_orders().is_empty());
    assert_eq!(
        harness
            .client
            .order_state(&client_order_id)
            .expect("tracked")
            .filled,
        Quantity::from("1.00"),
    );
}

/// F14: the same account read the other way round - the fills first, the terminal reading after -
/// settles the same way. The condition is the numbers agreeing, not the order they arrived in.
#[rstest]
#[tokio::test]
async fn test_a_terminal_reading_that_arrives_after_its_fills_resolves_the_order() {
    let mock = MockServer::start_admitted(open_create_script()).await;
    let harness = acknowledged_order(&mock).await;
    let client_order_id = ClientOrderId::from(CLIENT_ORDER_ID);

    assert_eq!(
        harness
            .client
            .apply_fill(&fill("f1", "1.00", "0.00"))
            .unwrap(),
        OndoFillApplication::Applied,
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert!(!state.resolved, "an `open` order is not a resolved one");
    assert!(
        !state.is_unresolved(),
        "and it is not one this adapter cannot account for"
    );

    assert_eq!(
        harness
            .client
            .apply_order(&api_order("fullyfilled", "1.00")),
        OndoOrderApplication::Applied,
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert_eq!(state.fill_gap(), None);
    assert!(state.resolved);
    assert!(harness.client.unresolved_orders().is_empty());
}

/// F14: a terminal reading the venue states **no total** for is not a reading this adapter can
/// check. `fill_gap` is [`None`] here and rightly so - there is no readable total that disagrees -
/// but reading that as agreement would have this adapter call an order settled on the strength of a
/// number it never read, while the account judgment reports the same order as unresolved. The two
/// answers come from one expression, so the unreadable total is unresolved on both sides.
#[rstest]
#[tokio::test]
async fn test_a_terminal_reading_with_no_readable_filled_size_is_not_settled() {
    let mock = MockServer::start_admitted(open_create_script()).await;
    let harness = acknowledged_order(&mock).await;
    let client_order_id = ClientOrderId::from(CLIENT_ORDER_ID);

    assert_eq!(
        harness
            .client
            .apply_order(&api_order("canceled", "not-a-number")),
        OndoOrderApplication::Applied,
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert_eq!(
        state.venue_filled, None,
        "the venue's terminal total could not be read",
    );
    assert_eq!(
        state.fill_gap(),
        None,
        "and there is no readable total for the applied fills to disagree with",
    );
    assert_eq!(state.status, OndoOrderStatus::Canceled);
    assert!(
        state.is_unresolved(),
        "a terminal state the adapter cannot check is not one it can account for",
    );
    assert!(!state.is_accounted_for());
    assert!(
        !state.resolved,
        "nothing was confirmed at the venue, so the order is not a resolved one",
    );
    assert_eq!(harness.client.unresolved_orders(), vec![client_order_id]);
}

/// F14: a partial fill, then the cancel that ends the order, then the fill the cancel did not wait
/// for. The cancel is reported - the venue did end the order - and the order is settled once the
/// fills account for what the venue said it filled.
#[rstest]
#[tokio::test]
async fn test_a_cancel_whose_last_fill_arrives_late_converges() {
    let mock = MockServer::start_admitted(open_create_script()).await;
    let mut harness = acknowledged_order(&mock).await;
    let client_order_id = ClientOrderId::from(CLIENT_ORDER_ID);

    assert_eq!(
        harness.client.apply_order(&api_order("canceled", "0.20")),
        OndoOrderApplication::Applied,
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert_eq!(state.fill_gap(), Some(Quantity::from("0.20")));
    assert!(state.is_unresolved());

    let reports = order_reports(&drain(&mut harness));
    let terminal = reports
        .iter()
        .find(|report| report.order_status == OrderStatus::Canceled)
        .expect("the cancel is reported: the venue ended the order");

    assert_eq!(
        terminal.filled_qty,
        Quantity::from("0.00"),
        "the report states the fills this adapter applied, not a quantity it never reported - the \
         engine infers the difference as a fill",
    );

    // The fill the venue counted arrives after the cancel that ended the order.
    assert_eq!(
        harness
            .client
            .apply_fill(&fill("f1", "0.20", "0.00"))
            .unwrap(),
        OndoFillApplication::Applied,
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert_eq!(state.fill_gap(), None);
    assert!(
        state.resolved,
        "the order is ended and the fills account for it"
    );
    assert!(harness.client.unresolved_orders().is_empty());
}

/// F14: a fill delivered twice is one fill. The second delivery moves nothing, including the
/// terminal reading the first one settled.
#[rstest]
#[tokio::test]
async fn test_a_fill_delivered_twice_is_counted_once_and_settles_once() {
    let mock = MockServer::start_admitted(open_create_script()).await;
    let harness = acknowledged_order(&mock).await;
    let client_order_id = ClientOrderId::from(CLIENT_ORDER_ID);

    assert_eq!(
        harness
            .client
            .apply_order(&api_order("fullyfilled", "1.00")),
        OndoOrderApplication::Applied,
    );
    assert_eq!(
        harness
            .client
            .apply_fill(&fill("f1", "1.00", "0.00"))
            .unwrap(),
        OndoFillApplication::Applied,
    );
    assert_eq!(
        harness
            .client
            .apply_fill(&fill("f1", "1.00", "0.00"))
            .unwrap(),
        OndoFillApplication::Duplicate,
        "the same fill id is one fill, whichever source delivered it",
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert_eq!(state.filled, Quantity::from("1.00"));
    assert_eq!(harness.client.applied_fill_count(), 1);
    assert_eq!(state.fill_gap(), None);
    assert!(state.resolved);
}

/// F14: a terminal reading the applied fills do not account for stays unresolved, however often
/// the venue repeats it - and the report the adapter emits for it never states a completion the
/// ledger cannot support, because the engine turns a `Filled` report whose quantity is above the
/// cache's into an inferred fill this adapter never reported.
#[rstest]
#[tokio::test]
async fn test_a_terminal_total_the_fills_do_not_reach_stays_unresolved_and_unasserted() {
    let mock = MockServer::start_admitted(open_create_script()).await;
    let mut harness = acknowledged_order(&mock).await;
    let client_order_id = ClientOrderId::from(CLIENT_ORDER_ID);

    assert_eq!(
        harness
            .client
            .apply_fill(&fill("f1", "0.40", "0.00"))
            .unwrap(),
        OndoFillApplication::Applied,
    );
    assert_eq!(
        harness
            .client
            .apply_order(&api_order("fullyfilled", "1.00")),
        OndoOrderApplication::Applied,
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert_eq!(
        state.fill_gap(),
        Some(Quantity::from("1.00")),
        "the venue says 1.00 filled and the applied fills total 0.40",
    );
    assert!(!state.resolved);
    assert_eq!(harness.client.unresolved_orders(), vec![client_order_id]);

    let reports = order_reports(&drain(&mut harness));

    assert!(
        reports
            .iter()
            .all(|report| report.order_status != OrderStatus::Filled),
        "no report asserts a completion the applied fills do not support: {reports:?}",
    );

    let reported = reports.last().expect("the payload is reported");

    assert_eq!(reported.order_status, OrderStatus::PartiallyFilled);
    assert_eq!(
        reported.filled_qty,
        Quantity::from("0.40"),
        "the report states the fills this adapter applied",
    );

    // Repeating the reading says nothing new and settles nothing: only the fills can.
    assert_eq!(
        harness
            .client
            .apply_order(&api_order("fullyfilled", "1.00")),
        OndoOrderApplication::Unchanged,
    );
    assert!(
        harness
            .client
            .order_state(&client_order_id)
            .unwrap()
            .is_unresolved()
    );

    // What settles it is the rest of the fills, and nothing else.
    assert_eq!(
        harness
            .client
            .apply_fill(&fill("f2", "0.60", "0.00"))
            .unwrap(),
        OndoFillApplication::Applied,
    );

    let state = harness
        .client
        .order_state(&client_order_id)
        .expect("tracked");

    assert_eq!(state.fill_gap(), None);
    assert!(state.resolved);
    assert!(harness.client.unresolved_orders().is_empty());
}

#[rstest]
#[tokio::test]
async fn test_last_fill_size_is_an_informational_field_and_never_a_fill() {
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.20",
    )))])
    .await;

    let mut harness = recovered_harness(&mock).await;

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

    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&format!(
        r#"{{"addedOrders":[{added_first},{added_second}],"failedOrders":[{refused}]}}"#,
    )))])
    .await;

    let mut harness = recovered_harness(&mock).await;

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
    wait_for_writes(&mock, 1).await;

    assert_eq!(writes(&mock)[0].target, "/v1/perps/orders/batch");
    assert!(
        writes(&mock)[0]
            .body
            .starts_with(r#"{"orders":[{"market":"NVDA-USD.P""#),
        "the batch keeps the submitted order: {}",
        writes(&mock)[0].body,
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

/// F02's batch half: a batch whose answer was lost used to leave only a log line. Every item is
/// now registered as unknown under its own client order id, which is the only reference a probe
/// may use - a new client order id would be a second order - and the batch is never re-sent.
#[rstest]
#[tokio::test]
async fn test_a_batch_whose_answer_was_lost_leaves_every_item_unknown() {
    let mock = MockServer::start(admitted_script(vec![Reply::answer(
        500,
        r#"{"success":false,"error":"gateway"}"#,
    )]))
    .await;
    let harness = recovered_harness(&mock).await;

    let orders: Vec<OrderAny> = ["ondo_probe_a", "ondo_probe_b"]
        .iter()
        .map(|id| limit_order(id, OrderSide::Buy))
        .collect();

    for order in &orders {
        seed_order(&harness, order);
    }

    harness
        .client
        .submit_order_list(order_list_command(&orders))
        .expect("the command is handled");

    // The eight reads the two passes made, and the batch.
    wait_for_requests(&mock, RECOVERY_READS + 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        mock.with_method("POST").len(),
        1,
        "a POST that may have been applied is never sent twice: {:?}",
        mock.targets(),
    );

    let unknown = harness.client.unknown_submissions();

    assert_eq!(unknown.len(), 2, "both items are registered: {unknown:?}");
    assert_eq!(
        unknown
            .iter()
            .map(|submission| submission.client_order_id)
            .collect::<Vec<_>>(),
        vec![
            ClientOrderId::from("ondo_probe_a"),
            ClientOrderId::from("ondo_probe_b")
        ],
    );
    assert!(
        unknown
            .iter()
            .all(|submission| submission.lookup.starts_with("client:ondo_probe_")),
        "each entry keeps the reference a probe uses: {unknown:?}",
    );
    assert!(
        !harness.client.can_submit_new_orders(),
        "an unresolved batch is not a licence to trade",
    );
}

/// A 2xx that names neither an added nor a refused order for an item settles nothing about that
/// item. It stays in flight, and it is registered - the log line it used to leave behind is not
/// something a probe could find.
#[rstest]
#[tokio::test]
async fn test_a_batch_answer_that_omits_an_item_leaves_that_item_unknown() {
    let added = order_json(
        VENUE_ORDER_ID,
        "ondo_probe_a",
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    );
    let refused = r#"{"order":{"clientOrderId":"ondo_probe_b","market":"NVDA-USD.P"},"error":"post only order would match","errorCode":"post_only_has_match"}"#;

    let mock = MockServer::start(admitted_script(vec![Reply::ok(envelope(&format!(
        r#"{{"addedOrders":[{added}],"failedOrders":[{refused}]}}"#,
    )))]))
    .await;
    let harness = recovered_harness(&mock).await;

    let orders: Vec<OrderAny> = ["ondo_probe_a", "ondo_probe_b", "ondo_probe_c"]
        .iter()
        .map(|id| limit_order(id, OrderSide::Buy))
        .collect();

    for order in &orders {
        seed_order(&harness, order);
    }

    harness
        .client
        .submit_order_list(order_list_command(&orders))
        .expect("the command is handled");
    wait_for_requests(&mock, RECOVERY_READS + 1).await;

    let unknown = harness.client.unknown_submissions();

    assert_eq!(
        unknown.len(),
        1,
        "only the item the venue said nothing about is unknown: {unknown:?}",
    );
    assert_eq!(
        unknown[0].client_order_id,
        ClientOrderId::from("ondo_probe_c"),
    );
    assert!(
        harness.client.tracks(&ClientOrderId::from("ondo_probe_c")),
        "an item that may be resting is not forgotten",
    );
    assert!(
        !harness.client.can_submit_new_orders(),
        "one unaccounted item holds the whole account's new risk",
    );
}

/// The plan's own batch sample: three items are sent and the venue answers for one. The two it said
/// nothing about are both unknown, under their own client order ids, and the account admits no new
/// risk while they are.
#[rstest]
#[tokio::test]
async fn test_a_batch_of_three_answered_for_one_leaves_the_other_two_unknown() {
    let added = order_json(
        VENUE_ORDER_ID,
        "ondo_probe_a",
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    );

    let mock = MockServer::start(admitted_script(vec![Reply::ok(envelope(&format!(
        r#"{{"addedOrders":[{added}],"failedOrders":[]}}"#,
    )))]))
    .await;
    let harness = recovered_harness(&mock).await;

    let orders: Vec<OrderAny> = ["ondo_probe_a", "ondo_probe_b", "ondo_probe_c"]
        .iter()
        .map(|id| limit_order(id, OrderSide::Buy))
        .collect();

    for order in &orders {
        seed_order(&harness, order);
    }

    harness
        .client
        .submit_order_list(order_list_command(&orders))
        .expect("the command is handled");
    wait_for_requests(&mock, RECOVERY_READS + 1).await;

    let unknown = harness.client.unknown_submissions();

    assert_eq!(unknown.len(), 2, "the two unanswered items: {unknown:?}");
    assert_eq!(
        unknown
            .iter()
            .map(|submission| submission.client_order_id)
            .collect::<Vec<_>>(),
        vec![
            ClientOrderId::from("ondo_probe_b"),
            ClientOrderId::from("ondo_probe_c")
        ],
    );
    assert!(
        unknown
            .iter()
            .all(|submission| !submission.reason.is_empty()
                && submission.lookup.starts_with("client:ondo_probe_")),
        "each entry keeps why it is unknown and the reference a probe uses: {unknown:?}",
    );
    assert!(
        harness.client.tracks(&ClientOrderId::from("ondo_probe_b"))
            && harness.client.tracks(&ClientOrderId::from("ondo_probe_c")),
        "an item that may be resting is not forgotten",
    );
    assert!(!harness.client.can_submit_new_orders());
}

/// An item attributed to an order this client never submitted accounts for none of its own. The
/// venue's answer is applied - it is a real order - but it does not settle the item that was sent.
#[rstest]
#[tokio::test]
async fn test_a_batch_answer_that_names_a_foreign_order_leaves_the_item_unknown() {
    let foreign = order_json(
        "someone-elses-venue-id",
        "someone-elses-order",
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    );

    let mock = MockServer::start(admitted_script(vec![Reply::ok(envelope(&format!(
        r#"{{"addedOrders":[{foreign}],"failedOrders":[]}}"#,
    )))]))
    .await;
    let harness = recovered_harness(&mock).await;

    let order = limit_order("ondo_probe_a", OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order_list(order_list_command(&[order]))
        .expect("the command is handled");
    wait_for_requests(&mock, RECOVERY_READS + 1).await;

    let unknown = harness.client.unknown_submissions();

    assert_eq!(
        unknown.len(),
        1,
        "the item is unattributed, not answered: {unknown:?}",
    );
    assert_eq!(
        unknown[0].client_order_id,
        ClientOrderId::from("ondo_probe_a"),
    );
    assert!(!harness.client.can_submit_new_orders());
}

/// The other side of the same rule: a command this adapter refuses locally never became a request,
/// so it is denied by name and enters no uncertainty at all. Recording it as unknown would put an
/// order that cannot exist into the list a probe has to chase.
#[rstest]
#[case(TimeInForce::Fok)]
#[case(TimeInForce::Gtd)]
#[tokio::test]
async fn test_a_command_refused_locally_is_denied_and_never_enters_uncertainty(
    #[case] time_in_force: TimeInForce,
) {
    let mock = MockServer::start(admitted_script(Vec::new())).await;
    let mut harness = recovered_harness(&mock).await;

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
        builder.expire_time(UnixNanos::from(1_800_000_000_000_000_000));
    }

    let order = builder.build();

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        mock.with_method("POST").is_empty(),
        "a locally refused command never becomes a request: {:?}",
        mock.targets(),
    );
    assert!(
        harness.client.unknown_submissions().is_empty(),
        "a request that never existed is not an unknown outcome",
    );
    assert!(
        harness.client.can_submit_new_orders(),
        "a local refusal says nothing about the account",
    );
    assert!(
        order_events(&drain(&mut harness))
            .iter()
            .any(|event| matches!(event, OrderEventAny::Denied(_))),
        "the refusal is reported to the strategy",
    );
}

/// R0.1's window, and the gate that closes it: admission is decided when the command is accepted,
/// and the request is built later, because a request waits for the shared budget. The account can
/// stop admitting new risk *inside* that wait, and a request built then is an order placed against
/// a state nothing has verified.
///
/// The wait is not a sleep this test hopes was long enough: the shared budget is emptied before the
/// submission, so the submission has to queue for a whole [`QUEUE_WINDOW`], and the state change is
/// asserted to land inside that window. The one thing the test cannot do is end the wait early -
/// the client's tasks run on the adapter's own runtime, whose clock a test cannot stop - so the
/// window is checked rather than shortened.
#[tokio::test]
async fn test_a_submission_queued_for_the_budget_is_refused_when_the_account_invalidates() {
    let mock = MockServer::start(admitted_script(Vec::new())).await;
    let mut harness = recovered_harness_on(&mock, paced_budget()).await;
    let budget = harness.client.http_client().budget().clone();

    // The bucket is empty, so the submission below has to wait a whole `QUEUE_WINDOW` for the next
    // cell. The wait is a real one - the client's tasks run on the runtime the adapter owns, and a
    // test cannot stop its clock - so what makes this test deterministic is that everything it needs
    // to happen inside the window is checked to be inside it, below.
    drain_budget(&budget);
    let emptied_at = Instant::now();

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    // `Submitted` is emitted at the request boundary, immediately before the acquisition, so seeing
    // it is seeing the command reach the wait - with no request behind it.
    let mut events = Vec::new();

    collect_until(&mut harness, &mut events, |events| {
        order_events(events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Submitted(_)))
    })
    .await;
    assert!(
        writes(&mock).is_empty(),
        "the submission is queued for the budget, not sent: {:?}",
        mock.targets(),
    );

    // The account stops admitting new risk while the command is queued. This is the property the
    // test is about, so the window it has to happen in is asserted rather than assumed: a state
    // change after the budget came back would be a different test entirely.
    assert!(
        emptied_at.elapsed() < QUEUE_WINDOW,
        "the account must stop admitting new risk inside the budget wait, and {:?} of the {:?}        \
         window has already passed",
        emptied_at.elapsed(),
        QUEUE_WINDOW,
    );
    harness.client.set_metadata(MetadataValidity::Stale {
        reason: "the metadata refresh failed".to_string(),
    });

    // Wait for the budget to come back on its own, and then for the outcome - whichever it is: the
    // refusal, or the write request that must not exist.
    collect_until(&mut harness, &mut events, |events| {
        order_events(events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Rejected(_)))
            || !writes(&mock).is_empty()
    })
    .await;

    let rejected: Vec<&OrderEventAny> = order_events(&events)
        .into_iter()
        .filter(|event| matches!(event, OrderEventAny::Rejected(_)))
        .collect();

    assert!(
        writes(&mock).is_empty(),
        "the queued request is refused before it is built: {:?}",
        mock.targets(),
    );
    assert_eq!(
        rejected.len(),
        1,
        "the queued submission is terminalised rather than left in flight: {events:?}",
    );
    assert!(
        rejected.iter().all(|event| match event {
            OrderEventAny::Rejected(event) => event.reason.contains("order-denied: reconciliation"),
            _ => false,
        }),
        "the refusal carries the account's own reason: {rejected:?}",
    );
    assert!(
        harness.client.unknown_submissions().is_empty(),
        "a request that never existed is not an unknown outcome",
    );
}

/// The batch counterpart of the same window: a list waits for the budget exactly as a single order
/// does, and an account that stops admitting new risk while it waits refuses the whole list - every
/// item terminalised, no item left in flight, and nothing sent.
#[tokio::test]
async fn test_a_batch_queued_for_the_budget_is_refused_when_the_account_invalidates() {
    let mock = MockServer::start(admitted_script(Vec::new())).await;
    let mut harness = recovered_harness_on(&mock, paced_budget()).await;
    let budget = harness.client.http_client().budget().clone();

    drain_budget(&budget);
    let emptied_at = Instant::now();

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order_list(order_list_command(&[order]))
        .expect("the command is handled");

    let mut events = Vec::new();

    collect_until(&mut harness, &mut events, |events| {
        order_events(events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Submitted(_)))
    })
    .await;
    assert!(
        writes(&mock).is_empty(),
        "the batch is queued for the budget, not sent: {:?}",
        mock.targets(),
    );
    assert!(
        emptied_at.elapsed() < QUEUE_WINDOW,
        "the account must stop admitting new risk inside the budget wait, and {:?} of the {:?} \
         window has already passed",
        emptied_at.elapsed(),
        QUEUE_WINDOW,
    );

    harness.client.set_metadata(MetadataValidity::Stale {
        reason: "the metadata refresh failed".to_string(),
    });

    collect_until(&mut harness, &mut events, |events| {
        order_events(events)
            .iter()
            .any(|event| matches!(event, OrderEventAny::Rejected(_)))
            || !writes(&mock).is_empty()
    })
    .await;

    let rejected: Vec<&OrderEventAny> = order_events(&events)
        .into_iter()
        .filter(|event| matches!(event, OrderEventAny::Rejected(_)))
        .collect();

    assert!(
        writes(&mock).is_empty(),
        "the queued batch is refused before it is built: {:?}",
        mock.targets(),
    );
    assert_eq!(
        rejected.len(),
        1,
        "every item of the queued batch is terminalised: {events:?}",
    );
    assert!(
        rejected.iter().all(|event| match event {
            OrderEventAny::Rejected(event) => event.reason.contains("order-denied: reconciliation"),
            _ => false,
        }),
        "the refusal carries the account's own reason: {rejected:?}",
    );
    assert!(
        harness.client.unknown_submissions().is_empty(),
        "no item of a batch that never became a request is an unknown outcome",
    );
}

/// A market cancel speaks for a whole market, so an ambiguous answer leaves every order this client
/// tracks there unsettled - and a confirming read that names none of them settles none of them.
/// Absent evidence is not evidence (plan §6.3): the orders keep their outstanding cancels, and the
/// account stops admitting new risk on a state no answer has stated.
#[rstest]
#[tokio::test]
async fn test_a_market_cancel_whose_answer_was_lost_registers_the_markets_orders() {
    let mock = MockServer::start_admitted(vec![
        // The create answer: the order is resting.
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "1.00",
            "0.00",
            "0.00",
            "0.00",
        ))),
        // The market cancel's answer never arrives.
        Reply::answer(500, r#"{"success":false,"error":"gateway"}"#),
        // The confirming read lists no order at all.
        Reply::ok(envelope("[]")),
    ])
    .await;
    let harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_for_writes(&mock, 1).await;

    harness
        .client
        .cancel_all_orders(cancel_all_command(None))
        .expect("cancel all");
    wait_for_writes(&mock, 2).await;

    let cancels = harness.client.unconfirmed_cancels();

    assert_eq!(
        cancels.len(),
        1,
        "the market's order is registered: {cancels:?}"
    );
    assert_eq!(
        cancels[0].client_order_id,
        ClientOrderId::from(CLIENT_ORDER_ID)
    );
    assert_eq!(
        cancels[0].lookup, VENUE_ORDER_ID,
        "a cancel is asked about under the venue order id this session holds",
    );
    assert!(
        !harness.client.can_submit_new_orders(),
        "a cancel no answer has settled is not a state to trade on",
    );
}

// ------------------------------------------------------------------------------------------------
// Cancellation
// ------------------------------------------------------------------------------------------------

#[rstest]
#[tokio::test]
async fn test_a_cancel_the_venue_refuses_with_a_race_code_triggers_a_confirming_query() {
    let mock = MockServer::start_admitted(vec![
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

    let mut harness = recovered_harness(&mock).await;

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

    // The query's answer is the venue saying the order is `fullyfilled`, and this adapter has
    // applied no fill of it: the report states the state the ledger supports - the order is
    // accepted, nothing is filled - and never the completion (F14). A report that carried the
    // venue's `filledSize` would have the engine infer a fill of 1.00 this adapter never reported.
    collect_until(&mut harness, &mut events, |events| {
        order_reports(events).iter().any(|report| {
            report.order_status == OrderStatus::Accepted && report.filled_qty.is_zero()
        })
    })
    .await;

    assert!(
        !order_reports(&events)
            .iter()
            .any(|report| report.order_status == OrderStatus::Filled),
        "no report asserts a completion the applied fills do not support",
    );

    assert_eq!(
        mock.targets()[RECOVERY_READS..],
        vec![
            "/v1/perps/orders".to_string(),
            format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
            format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
        ],
        "the refusal is answered by a query, not by calling the cancel API a second time",
    );
    assert_eq!(
        mock.captured()[RECOVERY_READS + 1].method,
        "DELETE",
        "the cancel is a DELETE with no body",
    );
    assert!(mock.captured()[RECOVERY_READS + 1].body.is_empty());
    assert_eq!(mock.captured()[RECOVERY_READS + 2].method, "GET");

    let state = harness
        .client
        .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
        .unwrap();

    assert_eq!(state.status.as_str(), "fullyfilled");
    assert_eq!(
        state.fill_gap(),
        Some(Quantity::from("1.00")),
        "the venue filled 1.00 and no fill was applied, so the order is short of its terminal \
         reading",
    );
    assert!(
        state.is_unresolved(),
        "an order whose terminal reading the fills do not account for is not resolved",
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
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "canceled",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let harness = recovered_harness(&mock).await;

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
        mock.targets()[RECOVERY_READS..],
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

    // The create, and the whole-market cancel the unfiltered command sends.
    wait_for_writes(&mock, 2).await;

    let captured = writes(&mock);

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
    let mock = MockServer::start_admitted(vec![
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

    let mut harness = recovered_harness(&mock).await;

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
        mock.captured()[RECOVERY_READS..]
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
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&unknown_status_order_json(
        status,
    )))])
    .await;

    let mut harness = recovered_harness(&mock).await;

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    wait_for_writes(&mock, 1).await;

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

    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&body))]).await;

    let mut harness = recovered_harness(&mock).await;

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
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.25",
        "0.001",
        "0.25",
    )))])
    .await;

    let mut harness = recovered_harness(&mock).await;

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

    let fill_mock = MockServer::start_admitted(vec![
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

    let fill_harness = recovered_harness(&fill_mock).await;

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

    let mock = MockServer::start_admitted(vec![
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

    let harness = recovered_harness(&mock).await;

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
    let mock = MockServer::start_admitted(vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))])
    .await;

    let mut harness = recovered_harness(&mock).await;

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

/// F14 on the query path: a report of an order this client tracks is built from the ledger, not from
/// the venue's own view. `apply_order` accepts a `fullyfilled` payload the applied fills do not
/// account for yet - the fills may still be arriving - so reporting the venue's number would state a
/// completion the ledger cannot support and leave the engine to synthesise an inferred fill for the
/// difference. The ingest path builds its report from the ledger for that very payload, so the two
/// paths have to give one order one answer: the ledger's 0.40, not the venue's 1.00.
#[rstest]
#[tokio::test]
async fn test_a_bulk_report_of_a_tracked_order_is_built_from_the_ledger() {
    let mut script = open_create_script();

    script.push(Reply::ok(envelope(&format!(
        "[{}]",
        order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "fullyfilled",
            "1.00",
            "1.00",
            "0.00",
            "1.00",
        )
    ))));

    let mock = MockServer::start_admitted(script).await;
    let mut harness = acknowledged_order(&mock).await;

    // The ledger has 0.40 of the order applied; the venue's own view calls it finished.
    assert_eq!(
        harness
            .client
            .apply_fill(&fill("f1", "0.40", "0.00"))
            .unwrap(),
        OndoFillApplication::Applied,
    );

    // Everything emitted before the read is setup, not the comparison.
    let _ = drain(&mut harness);

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

    // The same payload through ingest: the bulk loop applied it, and applying it is what reported it.
    let ingest = order_reports(&drain(&mut harness));

    assert_eq!(reports.len(), 1);

    let report = &reports[0];

    assert_eq!(
        report.client_order_id,
        Some(ClientOrderId::from(CLIENT_ORDER_ID)),
        "the order is this client's own, so it is reported from this client's ledger",
    );
    assert_ne!(
        report.order_status,
        OrderStatus::Filled,
        "the venue's completion is not one the applied fills account for",
    );
    assert_eq!(
        report.order_status,
        OrderStatus::PartiallyFilled,
        "the ledger is 0.40 of the way through the order, and that is the state reported",
    );
    assert_eq!(
        report.filled_qty,
        Quantity::from("0.40"),
        "the report carries the ledger's total, not the venue's filledSize",
    );

    let ingest = ingest
        .iter()
        .find(|report| report.venue_order_id == VenueOrderId::from(VENUE_ORDER_ID))
        .expect("the ingest path reports the order the venue just called finished");

    assert_eq!(
        (
            report.order_status,
            report.filled_qty,
            report.quantity,
            report.price,
        ),
        (
            ingest.order_status,
            ingest.filled_qty,
            ingest.quantity,
            ingest.price,
        ),
        "one adapter answers one order one way: the two paths agree",
    );
}

/// F16: the bulk read's filters are the venue's own parameters. `open_only` is sent as the venue's
/// `status` enum value for a working order, and the command's window as the whole milliseconds of
/// UTC the endpoint declares, so the bytes a signature covers are the bytes the transport sends.
/// The window is sent **instead of** trimming the answer locally: which of an order's own
/// timestamps the venue filters on cannot be observed from here, and a second filter over a
/// different one would drop orders the venue meant to return.
#[rstest]
#[tokio::test]
async fn test_a_bulk_read_sends_the_open_filter_and_the_time_window() {
    let mock = MockServer::start(vec![Reply::ok(envelope("[]"))]).await;
    let harness = build_harness(&mock, sandbox_config());

    let command = GenerateOrderStatusReportsBuilder::default()
        .ts_init(UnixNanos::default())
        .open_only(true)
        .instrument_id(Some(InstrumentId::from(NVDA)))
        .start(Some(UnixNanos::from_millis(1_757_088_000_000)))
        .end(Some(UnixNanos::from_millis(1_757_174_400_000)))
        .build()
        .expect("the command builds");

    harness
        .client
        .generate_order_status_reports(&command)
        .await
        .expect("the order history is read");

    assert_eq!(
        mock.captured()
            .iter()
            .map(|request| request.target.as_str())
            .collect::<Vec<_>>(),
        vec![
            "/v1/perps/orders?market=NVDA-USD.P&status=open&startTime=1757088000000&endTime=1757174400000"
        ],
        "the filter and the window are the venue's own query parameters",
    );

    // `open_only: false` is the absence of a filter, not a different one: the venue returns every
    // status and each order is judged on its own.
    let unfiltered = GenerateOrderStatusReportsBuilder::default()
        .ts_init(UnixNanos::default())
        .open_only(false)
        .build()
        .expect("the command builds");

    harness
        .client
        .generate_order_status_reports(&unfiltered)
        .await
        .expect("the order history is read");

    assert_eq!(
        mock.captured().last().expect("the second read").target,
        "/v1/perps/orders",
        "a read that asks for no filter is the path alone",
    );
}

/// F16: the filter is what the read asked the venue for, not a licence to drop what the answer
/// contains. An order that comes back in a status the read did not ask for is reported rather than
/// trimmed away - the venue chose to return it, and a local re-application of the filter is exactly
/// how this read would lose an order silently, which is what the filter exists to avoid.
#[rstest]
#[tokio::test]
async fn test_an_order_outside_the_requested_filter_is_reported_rather_than_dropped() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&format!(
        "[{}]",
        order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "canceled",
            "1.00",
            "0.00",
            "0.00",
            "0.00",
        )
    )))])
    .await;

    let harness = build_harness(&mock, sandbox_config());

    let command = GenerateOrderStatusReportsBuilder::default()
        .ts_init(UnixNanos::default())
        .open_only(true)
        .build()
        .expect("the command builds");

    let reports = harness
        .client
        .generate_order_status_reports(&command)
        .await
        .expect("the order history is read");

    assert_eq!(reports.len(), 1, "the order is reported, not trimmed away");
    assert_eq!(reports[0].order_status, OrderStatus::Canceled);
    assert_eq!(
        reports[0].venue_order_id,
        VenueOrderId::from(VENUE_ORDER_ID)
    );
}

/// F16: the venue's single-order read takes one path parameter, and the venue order id is one of
/// the two forms it accepts - its own description of that parameter is *"Internal order ID, or
/// `client:{clientOrderID}` for client order ID lookup"*. Asking by it is therefore not the same as
/// asking by client order id, and the `client:` value is this adapter's convention for the other
/// form: wrapped around a venue id it would name an order the venue has never heard of.
///
/// The answer is still judged against this client's ledger. The venue's payload need not echo a
/// `clientOrderId` at all - the index identifies the order by the venue id it recorded - and a
/// `filledSize` the applied fills do not agree with is not the total the report states.
#[rstest]
#[tokio::test]
async fn test_a_report_query_by_venue_order_id_alone_is_read_from_the_ledger() {
    let mut script = open_create_script();

    script.push(Reply::ok(envelope(&order_json_without_client_order_id(
        "open", "0.25",
    ))));

    let mock = MockServer::start_admitted(script).await;
    let harness = acknowledged_order(&mock).await;

    assert_eq!(
        harness
            .client
            .apply_fill(&fill("f1", "0.10", "0.001"))
            .unwrap(),
        OndoFillApplication::Applied,
    );

    let report = harness
        .client
        .generate_order_status_report(&GenerateOrderStatusReport {
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            instrument_id: None,
            client_order_id: None,
            venue_order_id: Some(VenueOrderId::from(VENUE_ORDER_ID)),
            params: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .expect("the order is read")
        .expect("the order the venue knows is reported");

    assert_eq!(
        mock.captured().last().expect("the read").target,
        format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
        "a venue order id is the path parameter itself, not a `client:` lookup value",
    );
    assert_eq!(report.venue_order_id, VenueOrderId::from(VENUE_ORDER_ID));
    assert_eq!(report.instrument_id, InstrumentId::from(NVDA));
    assert_eq!(
        report.client_order_id,
        Some(ClientOrderId::from(CLIENT_ORDER_ID)),
        "the identity is this client's, from the ledger: the venue's payload carries none",
    );
    assert_eq!(report.order_status, OrderStatus::PartiallyFilled);
    assert_eq!(
        report.filled_qty,
        Quantity::from("0.10"),
        "the ledger's applied total, not the venue's `filledSize`",
    );
}

/// F16: a report of one order is a read this venue addresses by one identifier or the other.
/// Neither is not a request that can be made, and it is refused where it is still decidable -
/// locally, before anything is signed or sent.
#[rstest]
#[tokio::test]
async fn test_a_report_query_with_neither_identifier_is_refused_before_any_request() {
    let mock = MockServer::start(vec![Reply::ok(envelope("{}"))]).await;
    let harness = build_harness(&mock, sandbox_config());

    let error = harness
        .client
        .generate_order_status_report(&GenerateOrderStatusReport {
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            instrument_id: None,
            client_order_id: None,
            venue_order_id: None,
            params: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .expect_err("neither identifier is a read this adapter can make");

    assert!(
        error
            .to_string()
            .contains("neither a client order id nor a venue order id"),
        "the refusal names both identifiers, was `{error}`",
    );
    assert!(
        mock.captured().is_empty(),
        "the refusal happens before any request exists",
    );
}

// ------------------------------------------------------------------------------------------------
// The endpoint gate
// ------------------------------------------------------------------------------------------------

/// Builds the execution client the way [`build_harness`] does and hands back the constructor's own
/// result.
///
/// The endpoint gate is a construction-time decision, so a refusal has to be observed here rather
/// than through a later request. The event channels are installed exactly as the harness installs
/// them, so a construction that is *not* refused cannot fail for an unrelated reason.
fn try_build_client_against(
    base_url: String,
    config: OndoExecutionClientConfig,
) -> anyhow::Result<OndoExecutionClient> {
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
        cache,
    );

    let config = OndoExecutionClientConfig {
        base_url_http: Some(base_url),
        account_id: Some(account_id),
        ..config
    };

    let credential = OndoCredential::new(
        OndoEnvironment::Sandbox,
        TEST_KEY_ID.to_string(),
        TEST_API_SECRET.to_string(),
    )
    .expect("the fake unit-test credential is well formed");

    let (exec_tx, _exec_rx) = tokio::sync::mpsc::unbounded_channel();
    let (data_tx, _data_rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_exec_event_sender(exec_tx);
    replace_data_event_sender(data_tx);

    OndoExecutionClient::with_credential(core, config, Some(credential), None)
}

/// The execution client signs order entry, so the endpoint allowlist applies to it as it does to
/// the transport: an authority that is neither the sandbox host nor a loopback test service never
/// produces a client, and the refusal is the gate's own, not a later request's failure.
#[rstest]
#[case::an_unrelated_remote_host(
    "https://evil.example",
    OndoEnvironmentError::HostNotAllowed { host: "evil.example".to_string() }
)]
#[case::a_host_that_merely_contains_the_sandbox_host(
    "https://api.ondoperps-sandbox.xyz.evil.example",
    OndoEnvironmentError::HostNotAllowed { host: "api.ondoperps-sandbox.xyz.evil.example".to_string() }
)]
#[case::a_subdomain_of_the_sandbox_host(
    "https://eu.api.ondoperps-sandbox.xyz",
    OndoEnvironmentError::HostNotAllowed { host: "eu.api.ondoperps-sandbox.xyz".to_string() }
)]
#[case::userinfo_in_front_of_the_sandbox_host(
    "https://key:secret@api.ondoperps-sandbox.xyz",
    OndoEnvironmentError::UserInfoForbidden
)]
#[case::the_sandbox_host_over_plain_http(
    "http://api.ondoperps-sandbox.xyz",
    OndoEnvironmentError::UnsupportedScheme { scheme: "http".to_string(), expected: "https" }
)]
#[case::the_production_host(
    "https://api.ondoperps.xyz",
    OndoEnvironmentError::ProductionHostForbidden { host: "api.ondoperps.xyz".to_string() }
)]
fn test_the_execution_client_is_not_built_on_an_endpoint_outside_the_allowlist(
    #[case] url: &str,
    #[case] expected: OndoEnvironmentError,
) {
    let built = try_build_client_against(url.to_string(), sandbox_config());
    let error = match built {
        Ok(_) => panic!("`{url}` is not an endpoint this client may sign for"),
        Err(error) => error,
    };

    assert_eq!(
        error.downcast_ref::<OndoEnvironmentError>(),
        Some(&expected),
        "the refusal is the gate's own, was `{error}`",
    );
}

/// The mock server's own listener, addressed under a name the policy refuses (`0.0.0.0` is not a
/// loopback address), so "nothing was sent" is an observation about a reachable listener.
#[rstest]
#[tokio::test]
async fn test_a_refused_endpoint_receives_no_request() {
    let mock = MockServer::start(vec![Reply::ok(envelope("{}"))]).await;
    let refused = mock.url().replace("127.0.0.1", "0.0.0.0");

    let built = try_build_client_against(refused, sandbox_config());

    assert!(
        built.is_err(),
        "a host outside the allowlist never produces a client",
    );
    assert!(
        mock.captured().is_empty(),
        "the refusal happens before any request exists",
    );
}

/// Production is refused both ways at once: the configuration that asks for production order entry
/// and names a production (or otherwise refused) endpoint is refused, and nothing is sent.
#[rstest]
#[tokio::test]
async fn test_a_refused_endpoint_is_refused_with_production_orders_requested() {
    let mock = MockServer::start(vec![Reply::ok(envelope("{}"))]).await;

    for url in [
        "https://api.ondoperps.xyz",
        "https://api.ondoperps.xyz.evil.example",
        "https://key:secret@api.ondoperps-sandbox.xyz",
    ] {
        let built = try_build_client_against(
            url.to_string(),
            OndoExecutionClientConfig {
                allow_production_orders: true,
                ..sandbox_config()
            },
        );

        let error = match built {
            Ok(_) => panic!("`{url}` and production order entry are both refused"),
            Err(error) => error,
        };

        assert_eq!(
            error.downcast_ref::<OndoExecutionConfigError>(),
            Some(&OndoExecutionConfigError::ProductionOrdersUnsupported),
            "the flag is refused by name whatever the endpoint says, was `{error}`",
        );
    }

    assert!(
        mock.captured().is_empty(),
        "the refusal happens before a socket is opened",
    );
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

/// A production read-only configuration builds the real execution client, and every write path
/// through it refuses before a request exists: the submission is denied by the account's read-only
/// admission, and a low-level cancel on the same HTTP object is refused by the scope dispatch.
#[tokio::test]
async fn test_a_read_only_execution_client_refuses_every_write_and_sends_nothing() {
    let mock = MockServer::start(vec![Reply::ok(envelope("{}"))]).await;
    let config = OndoExecutionClientConfig {
        environment: OndoEnvironment::Production,
        account_id: Some(AccountId::from(ACCOUNT_ID)),
        api_key: Some(TEST_KEY_ID.to_string()),
        api_secret: Some(TEST_API_SECRET.to_string()),
        account_read_only: true,
        ..Default::default()
    };

    let mut harness = build_harness(&mock, config);

    harness.client.start().expect("start");

    assert!(harness.client.is_account_read_only());
    assert_eq!(
        harness.client.http_client().authentication_scope(),
        Some(OndoAuthenticationScope::ProductionReadOnly),
    );

    let order = limit_order(CLIENT_ORDER_ID, OrderSide::Buy);
    seed_order(&harness, &order);
    assert_eq!(
        harness.client.new_risk_refusal(),
        Some(NewRiskRefusal::AccountIsReadOnly),
        "a read-only client refuses new risk",
    );
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the client accepts the command and decides locally");

    let events = drain(&mut harness);
    let denied: Vec<&OrderEventAny> = order_events(&events)
        .into_iter()
        .filter(|event| matches!(event, OrderEventAny::Denied(_)))
        .collect();
    assert_eq!(
        denied.len(),
        1,
        "the submission is denied locally (events: {events:?})"
    );
    assert!(
        mock.with_method("POST").is_empty(),
        "no submission became a request",
    );

    let cancelled = harness
        .client
        .http_client()
        .cancel_order("order-1", OndoRequestPriority::High)
        .await;
    assert!(
        matches!(
            cancelled,
            Err(OndoHttpError::WriteNotPermitted {
                scope: OndoAuthenticationScope::ProductionReadOnly,
                method: "DELETE",
            })
        ),
        "the scope dispatch refuses the cancel: {cancelled:?}",
    );
    assert!(
        mock.captured().is_empty(),
        "no write ever became a request: {:?}",
        mock.targets(),
    );
}

/// The configured venue account id is compared with the authenticated account's own `accountID`.
/// A match is the only way the identity reads `matched`, and it is what lets the connection
/// proceed.
#[tokio::test]
async fn test_a_matching_account_identity_still_requires_private_readiness() {
    let mock = MockServer::start(vec![Reply::ok(envelope(
        r#"{"accountID":"10458932786832481"}"#,
    ))])
    .await;
    let config = OndoExecutionClientConfig {
        environment: OndoEnvironment::Production,
        account_id: Some(AccountId::from(ACCOUNT_ID)),
        api_key: Some(TEST_KEY_ID.to_string()),
        api_secret: Some(TEST_API_SECRET.to_string()),
        account_read_only: true,
        expected_venue_account_id: Some("10458932786832481".to_string()),
        ..Default::default()
    };

    let mut harness = build_harness(&mock, config);
    harness.client.start().expect("start");

    let error = harness
        .client
        .connect()
        .await
        .expect_err("identity match alone is not private readiness");
    assert_eq!(
        error.to_string(),
        "ondo_readonly_readiness:private_login_not_acknowledged"
    );
    assert!(!harness.client.is_connected());
    assert_eq!(
        harness.client.account().account_identity(),
        OndoAccountIdentity::Matched,
    );
    assert!(
        mock.targets().iter().any(|target| target == "/v1/account"),
        "the identity read is the documented account read",
    );
}

/// A mismatch refuses the connection before it is marked connected, and the error never echoes
/// either identifier.
#[tokio::test]
async fn test_a_mismatched_account_identity_refuses_the_connection() {
    let mock = MockServer::start(vec![Reply::ok(envelope(
        r#"{"accountID":"99999999999999999"}"#,
    ))])
    .await;
    let config = OndoExecutionClientConfig {
        environment: OndoEnvironment::Production,
        account_id: Some(AccountId::from(ACCOUNT_ID)),
        api_key: Some(TEST_KEY_ID.to_string()),
        api_secret: Some(TEST_API_SECRET.to_string()),
        account_read_only: true,
        expected_venue_account_id: Some("10458932786832481".to_string()),
        ..Default::default()
    };

    let mut harness = build_harness(&mock, config);
    harness.client.start().expect("start");

    let error = harness
        .client
        .connect()
        .await
        .expect_err("a different account is refused");

    assert!(
        error.to_string().contains("does not match"),
        "was `{error}`",
    );
    assert!(
        !error.to_string().contains("10458932786832481")
            && !error.to_string().contains("99999999999999999"),
        "the refusal never echoes an identifier: `{error}`",
    );
    assert!(
        !harness.client.is_connected(),
        "a mismatched session is never marked connected",
    );
    assert_eq!(
        harness.client.account().account_identity(),
        OndoAccountIdentity::Mismatch,
    );
}

/// An answer that carries no comparable identifier is `unknown`, never `matched`: absence of
/// evidence is not evidence of a match.
#[tokio::test]
async fn test_an_account_answer_without_an_identifier_is_unknown() {
    let mock = MockServer::start(vec![Reply::ok(envelope(
        r#"{"identifier":"someone@example.com"}"#,
    ))])
    .await;
    let config = OndoExecutionClientConfig {
        environment: OndoEnvironment::Production,
        account_id: Some(AccountId::from(ACCOUNT_ID)),
        api_key: Some(TEST_KEY_ID.to_string()),
        api_secret: Some(TEST_API_SECRET.to_string()),
        account_read_only: true,
        expected_venue_account_id: Some("10458932786832481".to_string()),
        ..Default::default()
    };

    let mut harness = build_harness(&mock, config);
    harness.client.start().expect("start");

    let error = harness
        .client
        .connect()
        .await
        .expect_err("the HTTP-only mock cannot acknowledge private login");
    assert_eq!(
        error.to_string(),
        "ondo_readonly_readiness:private_login_not_acknowledged"
    );

    assert_eq!(
        harness.client.account().account_identity(),
        OndoAccountIdentity::Unknown,
    );
}

/// No expected id means no comparison and no identity read at all, and the identity stays
/// `unknown` rather than defaulting to a match.
#[tokio::test]
async fn test_no_expected_identity_makes_no_account_read() {
    let mock = MockServer::start(Vec::new()).await;
    let config = OndoExecutionClientConfig {
        environment: OndoEnvironment::Production,
        account_id: Some(AccountId::from(ACCOUNT_ID)),
        api_key: Some(TEST_KEY_ID.to_string()),
        api_secret: Some(TEST_API_SECRET.to_string()),
        account_read_only: true,
        ..Default::default()
    };

    let mut harness = build_harness(&mock, config);
    harness.client.start().expect("start");
    let error = harness
        .client
        .connect()
        .await
        .expect_err("the HTTP-only mock cannot acknowledge private login");
    assert_eq!(
        error.to_string(),
        "ondo_readonly_readiness:private_login_not_acknowledged"
    );

    assert!(
        !mock.targets().iter().any(|target| target == "/v1/account"),
        "no expected id means no identity read",
    );
    assert_eq!(
        harness.client.account().account_identity(),
        OndoAccountIdentity::Unknown,
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

// ------------------------------------------------------------------------------------------------
// The recovery pass's boundary: what it reads, what it drains, and who owns it (F03, plan §6.4)
// ------------------------------------------------------------------------------------------------

/// F03 (D4): a report that arrives while the pass is still reading belongs to that pass.
///
/// The positions read is held open until the stream has delivered a fill. A pass that drained its
/// buffered reports *before* its last two reads would hand this fill to the next pass - and judge an
/// account stitched together out of two instants, comparing the position the venue states now
/// against the fills it knew a moment ago, which is no fill at all.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_fill_that_arrives_during_the_last_reads_is_in_that_passes_reading() {
    let gate = Arc::new(Notify::new());
    let mock = MockServer::start_admitted(vec![
        // The create answer, which acknowledges the order before the pass reads the account.
        Reply::ok(envelope(&order_json(
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "open",
            "1.00",
            "0.00",
            "0.00",
            "0.00",
        ))),
        Reply::ok(envelope(&format!(
            "[{}]",
            order_json(
                VENUE_ORDER_ID,
                CLIENT_ORDER_ID,
                "open",
                "1.00",
                "0.20",
                "0.00",
                "0.00",
            ),
        ))),
        // The venue's fill history does not carry this fill yet: the stream is ahead of it.
        Reply::ok(envelope("[]")),
        Reply::Gated {
            gate: Arc::clone(&gate),
            body: envelope(&format!("[{}]", position_json("0.20"))),
        },
        Reply::ok(envelope(BALANCE_BODY)),
        // The funding read, which follows the balance.
        Reply::ok(envelope("[]")),
    ])
    .await;
    let harness = recovered_harness(&mock).await;

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

    harness.client.begin_recovery(UnixNanos::default());

    let fill = fill("f1", "0.20", "0.002");

    let (pass, ()) = tokio::join!(
        harness.client.reconcile_account(UnixNanos::default()),
        async {
            // The create answer and this pass's first three reads have landed, so the positions read
            // is provably in flight and the pass is provably still reading the account.
            wait_for_requests(&mock, RECOVERY_READS + 4).await;
            harness.client.buffer_stream_fill(fill);
            gate.notify_one();
        },
    );

    assert!(pass.is_ok(), "the pass reads: {pass:?}");

    assert_eq!(
        harness.client.applied_fill_count(),
        1,
        "the fill the stream delivered mid-pass is applied by that pass, not the next one",
    );

    let reading = harness
        .client
        .last_reading()
        .expect("the pass keeps its reading");

    assert_eq!(
        reading.fills,
        vec!["f1".to_string()],
        "the reading this pass judged carries the report it replayed",
    );
    assert_eq!(reading.orders.len(), 1);
    assert_eq!(
        reading.orders[0]
            .applied_filled
            .map(|quantity| quantity.to_string()),
        Some("0.20".to_string()),
        "and it is built from the state the pass left, not from the page it read",
    );

    let judgment = harness.client.last_judgment().expect("the pass is judged");

    assert!(
        judgment.is_clean(),
        "the position the venue states and the fill that explains it agree: {:?}",
        judgment.reasons(),
    );
}

/// F03 (D4): a report arriving at the instant the pass drains lands in exactly one pass.
///
/// The producer below pushes eight distinct fills while the pass reads, releasing the gated read in
/// the middle of them. The reports before that instant are the pass's own; the ones after it land on
/// one side of the drain or the other, and the assertion is about which: the two passes' readings
/// partition the eight ids - none stranded, none applied twice - and each fill is reported once.
#[tokio::test(flavor = "multi_thread")]
async fn test_reports_arriving_at_the_drain_boundary_land_in_exactly_one_pass() {
    let gate = Arc::new(Notify::new());
    let account = || {
        vec![
            Reply::ok(envelope(&format!(
                "[{}]",
                order_json(
                    VENUE_ORDER_ID,
                    CLIENT_ORDER_ID,
                    "open",
                    "1.00",
                    "0.80",
                    "0.00",
                    "0.00",
                ),
            ))),
            Reply::ok(envelope("[]")),
            Reply::ok(envelope(&format!("[{}]", position_json("0.80")))),
            Reply::ok(envelope(BALANCE_BODY)),
            Reply::ok(envelope("[]")),
        ]
    };
    let mut script = vec![Reply::ok(envelope(&order_json(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "open",
        "1.00",
        "0.00",
        "0.00",
        "0.00",
    )))];

    script.extend(account());

    // The first pass's positions read is the gated one; the second pass reads the account plainly.
    script[3] = Reply::Gated {
        gate: Arc::clone(&gate),
        body: envelope(&format!("[{}]", position_json("0.80"))),
    };
    script.extend(account());

    let mock = MockServer::start_admitted(script).await;
    let mut harness = recovered_harness(&mock).await;

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

    harness.client.begin_recovery(UnixNanos::default());

    let fills: Vec<OndoApiFill> = (1..=8)
        .map(|step| fill(&format!("f{step}"), "0.10", "0.001"))
        .collect();

    let (first, ()) = tokio::join!(
        harness.client.reconcile_account(UnixNanos::default()),
        async {
            // The create answer and this pass's first three reads: the positions read is in flight.
            wait_for_requests(&mock, RECOVERY_READS + 4).await;

            for (at, fill) in fills.iter().enumerate() {
                // The gated read is released in the middle of the stream, so half of the reports
                // arrive before the pass can drain and half of them around it.
                if at == fills.len() / 2 {
                    gate.notify_one();
                }

                harness.client.buffer_stream_fill(fill.clone());
                tokio::task::yield_now().await;
            }
        },
    );

    assert!(first.is_ok(), "the pass reads: {first:?}");

    let first_reading = harness
        .client
        .last_reading()
        .expect("the first pass keeps its reading");

    let second = harness.client.reconcile_account(UnixNanos::default()).await;

    assert!(second.is_ok(), "the pass reads: {second:?}");

    let second_reading = harness
        .client
        .last_reading()
        .expect("the second pass keeps its reading");

    let mut applied = first_reading.fills.clone();

    applied.extend(second_reading.fills.clone());
    applied.sort();

    let expected: Vec<String> = fills.iter().map(|fill| fill.id().to_string()).collect();

    assert_eq!(
        applied, expected,
        "every report the stream delivered is in exactly one pass's reading",
    );
    assert_eq!(
        harness.client.applied_fill_count(),
        fills.len(),
        "and each of them is applied once",
    );

    let events = drain(&mut harness);

    assert_eq!(
        fill_reports(&events).len(),
        fills.len(),
        "each fill is reported once: {events:?}",
    );
}

/// F03 (D3): one pass owns the account, and a second is refused by name.
///
/// Two passes over one account would both drain the buffer - the second finding it empty - and both
/// conclude, each judging a reading the other half-wrote. The refused pass reads nothing, judges
/// nothing, and says which condition it hit rather than reporting the account as failed.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_second_recovery_pass_is_refused_while_one_owns_the_account() {
    let gate = Arc::new(Notify::new());
    let mock = MockServer::start_admitted(vec![
        Reply::Gated {
            gate: Arc::clone(&gate),
            body: envelope("[]"),
        },
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope(BALANCE_BODY)),
    ])
    .await;
    let harness = recovered_harness(&mock).await;

    harness.client.begin_recovery(UnixNanos::default());

    let state_before = harness.client.reconciliation_state();
    let judgment_before = harness.client.last_judgment();

    let (pass, ()) = tokio::join!(
        harness.client.reconcile_account(UnixNanos::default()),
        async {
            // The first read of the pass is in flight, so that pass provably owns the account.
            wait_for_requests(&mock, RECOVERY_READS + 1).await;

            let refused = harness.client.reconcile_account(UnixNanos::default()).await;
            let error = refused.expect_err("a second pass is refused while one owns the account");

            assert_eq!(
                error.downcast_ref::<RecoveryPassRefusal>(),
                Some(&RecoveryPassRefusal::AlreadyRunning),
                "the refusal names the condition: `{error}`",
            );

            // A refused pass leaves the claim where it was: the pass that owns the account still
            // owns it, so the next caller meets the same condition rather than being let in.
            let also_refused = harness.client.reconcile_account(UnixNanos::default()).await;

            assert_eq!(
                also_refused
                    .err()
                    .and_then(|error| error.downcast_ref::<RecoveryPassRefusal>().copied()),
                Some(RecoveryPassRefusal::AlreadyRunning),
                "a refusal does not hand the account on",
            );
            assert_eq!(
                harness.client.last_judgment(),
                judgment_before,
                "the refused pass judged nothing",
            );
            assert_eq!(
                harness.client.reconciliation_state(),
                state_before,
                "and left the account to the pass that owns it",
            );

            gate.notify_one();
        },
    );

    assert!(
        pass.is_ok(),
        "the pass that owns the account reads: {pass:?}"
    );
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Recovering,
        "one agreeing pass, concluded by the pass that owned it",
    );
}

// ------------------------------------------------------------------------------------------------
// The account the engine is told about (plan §R3.2)
// ------------------------------------------------------------------------------------------------

/// One USDC amount, at the settlement currency's own precision.
fn usdc(value: &str) -> Money {
    Money::from_decimal(
        rust_decimal::Decimal::from_str_exact(value).expect("decimal"),
        Currency::from(ONDO_SETTLEMENT_CURRENCY),
    )
    .expect("money")
}

/// Every account state the client published, whichever envelope carried it.
fn account_states(events: &[ExecutionEvent]) -> Vec<AccountState> {
    events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Account(state) => Some(state.clone()),
            _ => None,
        })
        .collect()
}

/// The balance summary a venue answers a pass with, with every documented member spelled out.
fn balance_body(
    margin: &str,
    used: &str,
    available: &str,
    maintenance: &str,
    under_liquidation: bool,
    funding: &str,
) -> String {
    format!(
        r#"{{"walletBalance":"5000.00","realizedPnl":"0.00","unrealizedPnl":"0.00","marginBalance":"{margin}","usedMargin":"{used}","availableMargin":"{available}","withdrawableMargin":"{available}","maintenanceMarginRequirement":"{maintenance}","totalMaintenanceMargin":"0.00","marginRatio":"0.00","leverage":"0.00","underLiquidation":{under_liquidation},"totalFundingPayments":"{funding}","totalTradingFees":"0.00","totalPnL":"0.00","netInvested":"5000.00"}}"#,
    )
}

/// One funding payment in the venue's documented `FundingFeeTransfer` shape.
fn funding_fee(market: &str, time: &str, amount: &str) -> String {
    format!(
        r#"{{"market":"{market}","time":"{time}","markPrice":"227.50","positionSize":"1.00","positionDirection":"long","rate":"0.0000125","payer":"long","amount":"{amount}"}}"#,
    )
}

/// One page of a paginated list: the `result` array with the `pageInfo` a cursor walk follows,
/// which is the shape the frozen spec documents for `GET /v1/perps/funding_fees`.
fn funding_page(records: &[String], next: Option<&str>) -> String {
    let items = format!("[{}]", records.join(","));

    match next {
        Some(cursor) => {
            format!(r#"{{"success":true,"result":{items},"pageInfo":{{"nextCursor":"{cursor}"}}}}"#)
        }
        None => envelope(&items),
    }
}

/// A pass's five reads for an account with `balance`, whose funding history is the pages a walk
/// would ask for in order.
fn pass_reads_paged(balance: &str, funding_pages: &[String]) -> Vec<Reply> {
    let mut reads = vec![
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope(balance)),
    ];

    reads.extend(funding_pages.iter().map(|page| Reply::ok(page.clone())));

    reads
}

/// The requests the client made for the funding history, in order.
fn funding_reads(mock: &MockServer) -> Vec<String> {
    mock.targets()
        .into_iter()
        .filter(|target| target.starts_with(FUNDING_FEES_PATH))
        .collect()
}

/// The instant an RFC 3339 timestamp names, so a test can say when the client's clock reads in the
/// same terms the venue states a payment's `time` in.
fn instant(timestamp: &str) -> UnixNanos {
    parse_timestamp(timestamp).expect("a readable timestamp")
}

/// A pass's five reads for an account with `balance` and `funding`.
fn pass_reads(balance: &str, funding: &[String]) -> Vec<Reply> {
    vec![
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope(balance)),
        Reply::ok(envelope(&format!("[{}]", funding.join(",")))),
    ]
}

/// The account the engine's cache holds is the account this adapter read, and it arrives the way
/// every other execution event does: through the emitter and the event channel.
#[tokio::test]
async fn test_a_verified_balance_reaches_the_engine_as_a_nautilus_account_state() {
    let balance = balance_body("4950.00", "1125.00", "3825.00", "112.50", false, "-5.67");
    let mock = MockServer::start(vec![
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope(&balance)),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope(&balance)),
        Reply::ok(envelope("[]")),
    ])
    .await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(UnixNanos::default());
    harness
        .client
        .reconcile_account(UnixNanos::default())
        .await
        .expect("the pass reads the account");

    let states = account_states(&drain(&mut harness));

    assert_eq!(states.len(), 1, "one concluded pass, one account state");

    let state = &states[0];
    let settlement = Currency::from(ONDO_SETTLEMENT_CURRENCY);

    assert_eq!(state.account_id, AccountId::from(ACCOUNT_ID));
    assert!(
        state.is_reported,
        "the venue reported it, this adapter did not calculate it"
    );
    assert_eq!(state.balances.len(), 1);

    let reported = state.balances[0];

    assert_eq!(reported.currency, settlement);
    assert_eq!(reported.total, usdc("4950.00"));
    assert_eq!(reported.locked, usdc("1125.00"));
    assert_eq!(reported.free, usdc("3825.00"));
    assert_eq!(
        reported.total,
        reported.locked.checked_add(reported.free).expect("the sum"),
        "the venue's own arithmetic is carried, not re-derived",
    );

    // The maintenance requirement travels as the margin balance's maintenance side.
    assert_eq!(state.margins.len(), 1);
    assert_eq!(state.margins[0].maintenance, usdc("112.50"));
    assert_eq!(state.margins[0].initial, usdc("1125.00"));
}

/// A pass that verified no balance publishes no account state: unknown or missing numbers are
/// never reported as a normal state.
#[tokio::test]
async fn test_a_balance_that_is_not_the_whole_account_publishes_no_account_state() {
    // The venue reports a second collateral asset, so the USDC view is not the account.
    let partial = format!(
        r#"{{"walletBalance":"5000.00","marginBalance":"4950.00","usedMargin":"1125.00","availableMargin":"3825.00","withdrawableMargin":"3825.00","maintenanceMarginRequirement":"112.50","underLiquidation":false,"totalFundingPayments":"0.00","USDT":"250.00"}}"#,
    );
    let mock = MockServer::start(vec![
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope(&partial)),
        Reply::ok(envelope("[]")),
    ])
    .await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(UnixNanos::default());
    harness
        .client
        .reconcile_account(UnixNanos::default())
        .await
        .expect("the pass reads the account");

    let states = account_states(&drain(&mut harness));

    assert!(
        states.is_empty(),
        "a USDC-only view of a multi-collateral account is not the account: {states:?}",
    );
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Uncertain,
    );
}

/// A balance the venue states is underwater is published as it was read: the signs are the
/// venue's, and clamping them to zero would report a healthy account.
#[tokio::test]
async fn test_a_negative_balance_is_published_negative_and_stops_new_risk() {
    let balance = balance_body("-1250.00", "100.00", "-1350.00", "112.50", false, "0.00");
    let mock = MockServer::start(pass_reads(&balance, &[])).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(UnixNanos::default());
    harness
        .client
        .reconcile_account(UnixNanos::default())
        .await
        .expect("the pass reads the account");

    let states = account_states(&drain(&mut harness));

    assert_eq!(states.len(), 1);
    assert_eq!(
        states[0].balances[0].total,
        usdc("-1250.00"),
        "an underwater account is reported underwater",
    );
    assert_eq!(states[0].balances[0].free, usdc("-1350.00"));
    assert!(
        !harness.client.can_submit_new_orders(),
        "and nothing new is opened against it",
    );
}

/// The native position report is the venue's position - its quantity, its direction and its own
/// average entry price - and it is not synthesised from a local net.
#[tokio::test]
async fn test_the_position_report_is_the_venue_position() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&format!(
        "[{},{}]",
        position_json("0.25"),
        r#"{"market":"TSLA-USD.P","direction":"short","netQuantity":"3.00","averageEntryPrice":"410.25","usedMargin":"1230.75","unrealizedPnl":"0.00","markPrice":"410.25","liquidationPrice":"500.00","bankruptcyPrice":"520.00","maintenanceMargin":"61.50","notionalValue":"1230.75","leverage":"2.0","netFundingSinceNeutral":"0.00","returnOnEquity":"0.00"}"#,
    )))])
    .await;
    let harness = build_harness(&mock, sandbox_config());
    let cmd = GeneratePositionStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        None, // instrument_id
        None,
        None,
        None,
        None,
    );

    let reports = harness
        .client
        .generate_position_status_reports(&cmd)
        .await
        .expect("the positions read");

    assert_eq!(reports.len(), 2);

    let long = reports
        .iter()
        .find(|report| report.instrument_id == InstrumentId::from(NVDA))
        .expect("NVDA is reported");

    assert_eq!(long.position_side, PositionSide::Long);
    assert_eq!(long.quantity, Quantity::from("0.25"));
    assert_eq!(
        long.signed_decimal_qty,
        rust_decimal::Decimal::from_str_exact("0.25").expect("decimal"),
        "the venue's direction carries the sign, once",
    );
    assert_eq!(
        long.avg_px_open,
        Some(rust_decimal::Decimal::from_str_exact("227.50").expect("decimal")),
        "the venue's own average entry price",
    );
    assert_eq!(long.account_id, AccountId::from(ACCOUNT_ID));

    let short = reports
        .iter()
        .find(|report| report.instrument_id == InstrumentId::from("TSLA-USD-PERP.ONDO"))
        .expect("TSLA is reported");

    assert_eq!(short.position_side, PositionSide::Short);
    assert_eq!(short.quantity, Quantity::from("3.00"));
    assert_eq!(
        short.signed_decimal_qty,
        rust_decimal::Decimal::from_str_exact("-3.00").expect("decimal"),
        "a short is a negative signed quantity and a positive size",
    );

    // The command's own filter narrows the answer and never widens it.
    let filtered = GeneratePositionStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from(NVDA)),
        None,
        None,
        None,
        None,
    );
    let reports = harness
        .client
        .generate_position_status_reports(&filtered)
        .await
        .expect("the positions read again");

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].instrument_id, InstrumentId::from(NVDA));

    assert_eq!(
        writes(&mock).len(),
        0,
        "a position report is a read: nothing was sent that could change the account",
    );
}

/// The funding **payment** the venue stated is accounted; the funding **rate** is never multiplied
/// by a position to produce one.
#[tokio::test]
async fn test_a_stated_funding_payment_is_accounted_and_a_rate_is_never_multiplied_into_one() {
    let opening = balance_body("4950.00", "0.00", "4950.00", "112.50", false, "0.00");
    let paid = balance_body("4950.00", "0.00", "4950.00", "112.50", false, "-15.67");
    let fee = funding_fee(NVDA_MARKET, "2025-03-05T14:30:01.000000000Z", "-15.67");
    let mut script = pass_reads(&opening, &[]);

    script.extend(pass_reads(&paid, &[fee]));

    let mock = MockServer::start(script).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(UnixNanos::default());

    // The first pass reads the account before the payment: the venue's cumulative total is the
    // baseline everything after it is measured from.
    harness
        .client
        .reconcile_account(UnixNanos::default())
        .await
        .expect("the first pass reads the account");
    harness
        .client
        .reconcile_account(UnixNanos::default())
        .await
        .expect("the second pass reads the account the payment landed in");

    let funding = harness.client.account();

    assert!(
        funding.funding_reconciliation().is_reconciled(),
        "the venue's own total and the payment it published agree",
    );

    let accounted = funding.funding_payments();

    assert_eq!(accounted.len(), 1);
    assert_eq!(accounted[0].market, NVDA_MARKET);
    assert_eq!(
        accounted[0].amount,
        rust_decimal::Decimal::from_str_exact("-15.67").expect("decimal"),
    );
    assert_eq!(
        accounted[0].rate,
        Some(rust_decimal::Decimal::from_str_exact("0.0000125").expect("decimal")),
        "the rate is kept as evidence about the payment",
    );
    assert!(
        harness.client.last_judgment().expect("judged").is_clean(),
        "nothing about the account is left unexplained",
    );
}

/// A funding history that cannot be read is reported and books nothing; the account's own state is
/// still read, so the pass concludes rather than failing on a cashflow it could not see.
#[tokio::test]
async fn test_a_funding_read_that_failed_books_nothing_and_is_reported() {
    let balance = balance_body("4950.00", "0.00", "4950.00", "112.50", false, "-15.67");
    let mock = MockServer::start(vec![
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope("[]")),
        Reply::ok(envelope(&balance)),
        // The funding path is not answered at all.
        Reply::answer(404, r#"{"success":false}"#),
    ])
    .await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(UnixNanos::default());
    harness
        .client
        .reconcile_account(UnixNanos::default())
        .await
        .expect("the account itself was read");

    let funding = harness.client.account();

    assert_eq!(
        funding.funding_payments().len(),
        0,
        "nothing was read, so nothing is booked"
    );
    assert!(!funding.funding_reconciliation().is_reconciled());

    let judgment = harness.client.last_judgment().expect("judged");

    assert!(
        judgment
            .findings
            .iter()
            .any(|finding| matches!(finding, Finding::FundingUnreadable { .. })),
        "the gap is said out loud: {:?}",
        judgment.reasons(),
    );
    assert!(
        !judgment.is_uncertain(),
        "and it does not make the account's own state unknown",
    );
}

/// A funding history far deeper than the page cap is read without reaching it: the walk ends where
/// the reconciliation starts, not at the ceiling.
#[tokio::test]
async fn test_a_funding_history_deeper_than_the_page_cap_is_read_without_reaching_it() {
    // The venue's history is 105 pages here, five more than `REPORT_MAX_PAGES`, and every page past
    // the second lies entirely before the account's baseline. Before the walk was bounded by that
    // baseline it asked for all of them and failed the read on the cap - on every pass, for ever.
    let baseline_at = "2025-03-05T14:30:00Z";
    // The payment on the newest page sits at exactly the instant the baseline is established, which
    // is inside the window (`time >= since`), so the walk has a reason to ask for one more page.
    // Its amount is zero, so accounting it agrees with a total that has not moved: this test is
    // about how far the walk goes, not about what a payment does to the reconciliation.
    let inside_the_window = funding_fee(NVDA_MARKET, baseline_at, "0.00");
    let before_the_window = funding_fee(NVDA_MARKET, "2025-03-05T13:00:00Z", "-9.99");
    let pages: Vec<String> = (1..=105)
        .map(|page| {
            let next = (page < 105).then(|| format!("page-{}", page + 1));
            let records = match page {
                1 => [inside_the_window.clone()],
                _ => [before_the_window.clone()],
            };

            funding_page(&records, next.as_deref())
        })
        .collect();

    let balance = balance_body("4950.00", "0.00", "4950.00", "112.50", false, "0.00");
    let mock = MockServer::start(pass_reads_paged(&balance, &pages)).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(UnixNanos::default());
    harness
        .client
        .reconcile_account(instant(baseline_at))
        .await
        .expect("the pass reads the account");

    let reads = funding_reads(&mock);

    assert_eq!(
        reads.len(),
        2,
        "the walk stopped at the first page that lay before the window: {reads:?}",
    );

    let funding = harness.client.account();

    assert!(
        funding.funding_reconciliation().is_reconciled(),
        "the history was read rather than failing on the cap: {:?}",
        funding.funding_reconciliation(),
    );
    assert_eq!(
        funding.funding_payments().len(),
        2,
        "the two pages that were read, and neither of the 103 that were not",
    );
    assert!(
        harness.client.last_judgment().expect("judged").is_clean(),
        "and nothing about the account is left unexplained",
    );
}

/// A funding walk stops on the first page that lies entirely before the baseline the reconciliation
/// starts from - and the records on it are evidence, not something counted against the total.
#[tokio::test]
async fn test_a_funding_walk_stops_on_the_first_page_that_lies_before_the_baseline() {
    let baseline_at = "2025-03-05T14:30:00Z";
    let opening = balance_body("4950.00", "0.00", "4950.00", "112.50", false, "0.00");
    let paid = balance_body("4950.00", "0.00", "4950.00", "112.50", false, "-15.67");
    let mut script = pass_reads(&opening, &[]);

    // The second pass reads a page inside the window, a page before it, and - were the walk to go
    // on - a third one. The second is where it stops, and the third is never asked for.
    script.extend(pass_reads_paged(
        &paid,
        &[
            funding_page(
                &[funding_fee(NVDA_MARKET, "2025-03-05T15:00:00Z", "-15.67")],
                Some("page-2"),
            ),
            funding_page(
                &[funding_fee(NVDA_MARKET, "2025-03-05T13:00:00Z", "-4.00")],
                Some("page-3"),
            ),
            funding_page(
                &[funding_fee(NVDA_MARKET, "2025-03-05T12:00:00Z", "-1.00")],
                None,
            ),
        ],
    ));

    let mock = MockServer::start(script).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(UnixNanos::default());
    harness
        .client
        .reconcile_account(instant(baseline_at))
        .await
        .expect("the first pass reads the account, and its total is the baseline");
    harness
        .client
        .reconcile_account(instant("2025-03-05T15:30:00Z"))
        .await
        .expect("the second pass reads the account the payment landed in");

    let reads = funding_reads(&mock);

    assert_eq!(
        reads.len(),
        3,
        "one funding read in the first pass and two in the second: {reads:?}",
    );
    assert!(
        !reads.iter().any(|target| target.contains("page-3")),
        "the page that lay wholly before the baseline ended the walk: {reads:?}",
    );

    let funding = harness.client.account();

    assert!(
        funding.funding_reconciliation().is_reconciled(),
        "the payment the window covers was read, and it matches the venue's own total: {:?}",
        funding.funding_reconciliation(),
    );

    let accounted = funding.funding_payments();

    assert_eq!(accounted.len(), 2, "the two pages that were read");
    assert!(
        !accounted.iter().any(|payment| payment.amount
            == rust_decimal::Decimal::from_str_exact("-1.00").expect("decimal")),
        "and not the page the walk stopped short of: {accounted:?}",
    );
    assert!(
        harness.client.last_judgment().expect("judged").is_clean(),
        "a payment from before the baseline is recorded without being counted against it",
    );
}

/// An empty page does not end the walk: a page with no records says nothing about the order of what
/// lies beyond it, so it is followed to the cursor it carried.
#[tokio::test]
async fn test_an_empty_funding_page_does_not_end_the_walk() {
    let baseline_at = "2025-03-05T14:30:00Z";
    let opening = balance_body("4950.00", "0.00", "4950.00", "112.50", false, "0.00");
    let paid = balance_body("4950.00", "0.00", "4950.00", "112.50", false, "-15.67");
    let mut script = pass_reads(&opening, &[]);

    script.extend(pass_reads_paged(
        &paid,
        &[
            funding_page(&[], Some("page-2")),
            funding_page(
                &[funding_fee(NVDA_MARKET, "2025-03-05T15:00:00Z", "-15.67")],
                Some("page-3"),
            ),
            funding_page(
                &[funding_fee(NVDA_MARKET, "2025-03-05T13:00:00Z", "-4.00")],
                None,
            ),
        ],
    ));

    let mock = MockServer::start(script).await;
    let mut harness = build_harness(&mock, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    harness.client.begin_recovery(UnixNanos::default());
    harness
        .client
        .reconcile_account(instant(baseline_at))
        .await
        .expect("the first pass reads the account, and its total is the baseline");
    harness
        .client
        .reconcile_account(instant("2025-03-05T15:30:00Z"))
        .await
        .expect("the second pass reads the account the payment landed in");

    let reads = funding_reads(&mock);

    assert_eq!(
        reads.len(),
        4,
        "the empty page was followed to the cursor it carried, not read as the end: {reads:?}",
    );

    let funding = harness.client.account();

    assert!(
        funding.funding_reconciliation().is_reconciled(),
        "the payment behind the empty page was read, and it matches the venue's total: {:?}",
        funding.funding_reconciliation(),
    );
    assert_eq!(funding.funding_payments().len(), 2);
    assert!(
        harness.client.last_judgment().expect("judged").is_clean(),
        "nothing about the account is left unexplained",
    );
}

/// An account the venue is liquidating is refused a new order by name, before any request.
#[tokio::test]
async fn test_an_account_under_liquidation_refuses_the_next_order_without_a_request() {
    let liquidating = balance_body("4950.00", "0.00", "4950.00", "112.50", true, "0.00");
    let mut script = clean_pass_reads();

    script.extend(clean_pass_reads());
    script.extend(pass_reads(&liquidating, &[]));

    let mock = MockServer::start(script).await;
    let mut harness = recovered_harness(&mock).await;

    // The harness converged on a clear account; the venue is now liquidating it. The pass that
    // reads that is a steady-state pass: the account is not re-recovered over a condition the
    // venue stated.
    harness
        .client
        .reconcile_account(UnixNanos::default())
        .await
        .expect("the pass reads the liquidating account");

    assert_eq!(
        harness.client.new_risk_refusal(),
        Some(NewRiskRefusal::Liquidation(
            LiquidationState::UnderLiquidation
        )),
    );

    let order = limit_order("ondo_after_liquidation", OrderSide::Buy);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

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

    assert!(reason.contains("liquidating"), "was `{reason}`");
    assert_eq!(
        writes(&mock).len(),
        0,
        "and nothing was sent: a denied order never leaves the process",
    );
}

/// Bulk position coverage is **not** promised, and one unreadable row is why.
///
/// The venue documents its position list as every open position the account holds, and this adapter
/// reports every row of it that it can name - so for an account whose rows all read, the reports are
/// the account's positions. What the flag would promise is something else: that an instrument with
/// **no** report is flat. One row whose direction this adapter cannot read is an instrument it
/// cannot report, and under that promise the engine would read the missing report as a flat
/// position - the false-clean judgment plan §6.3 forbids. The promise is therefore not made, and the
/// reports are still produced.
#[tokio::test]
async fn test_bulk_position_coverage_is_not_promised_however_readable_the_rows_are() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&format!(
        "[{}]",
        position_json("0.25"),
    )))])
    .await;
    let harness = build_harness(&mock, sandbox_config());

    assert!(
        !harness
            .client
            .provides_bulk_position_coverage(InstrumentId::from(NVDA)),
        "an absent position report is never evidence that a position is flat",
    );

    // The row reads, so it is reported: what is withheld is the promise about the rows that would
    // not read, not the rows that do.
    let reports = harness
        .client
        .generate_position_status_reports(&GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
            None,
            None,
            None,
        ))
        .await
        .expect("the positions read");

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].quantity, Quantity::from("0.25"));
}

/// The two rows this adapter cannot report are not reported, and neither is guessed at.
///
/// One is a market it cannot map onto an instrument at all. The other is the row the coverage
/// argument turns on: a market it *can* name whose direction is a spelling it cannot read. The
/// quantity is right there in the payload, and reporting it as a long would be inventing the one
/// fact the venue did not state - a position on the wrong side is worse than a position the engine
/// does not hear about, because the second is what the missing coverage promise already tells it to
/// expect.
#[tokio::test]
async fn test_a_position_row_this_adapter_cannot_read_is_never_guessed_into_a_report() {
    let mock = MockServer::start(vec![Reply::ok(envelope(&format!(
        "[{},{}]",
        r#"{"market":"SOMETHING-USD","direction":"long","netQuantity":"2.00"}"#,
        r#"{"market":"TSLA-USD.P","direction":"sideways","netQuantity":"3.00"}"#,
    )))])
    .await;
    let harness = build_harness(&mock, sandbox_config());

    let reports = harness
        .client
        .generate_position_status_reports(&GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
            None,
            None,
            None,
        ))
        .await
        .expect("the positions read");

    assert!(
        reports.is_empty(),
        "an unmappable market and an unreadable direction are both unreportable: {reports:?}",
    );
}

/// An explicitly injected credential must belong to the configured environment.
#[tokio::test]
async fn test_execution_refuses_cross_environment_credentials_before_transport() {
    let mock = MockServer::start(Vec::new()).await;
    for (environment, credential_environment) in [
        (OndoEnvironment::Production, OndoEnvironment::Sandbox),
        (OndoEnvironment::Sandbox, OndoEnvironment::Production),
    ] {
        let config = OndoExecutionClientConfig {
            environment,
            account_id: Some(AccountId::from(ACCOUNT_ID)),
            account_read_only: true,
            base_url_http: Some(mock.url()),
            ..Default::default()
        };
        let core = ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            ClientId::from(CLIENT_ID),
            *ONDO_VENUE,
            OmsType::Netting,
            AccountId::from(ACCOUNT_ID),
            AccountType::Margin,
            None,
            Rc::new(RefCell::new(Cache::default())),
        );
        let credential = OndoCredential::new(
            credential_environment,
            TEST_KEY_ID.to_string(),
            TEST_API_SECRET.to_string(),
        )
        .unwrap();
        let result = OndoExecutionClient::with_credential(core, config, Some(credential), None);
        let error = match result {
            Ok(_) => panic!("cross-environment credential must fail"),
            Err(e) => e,
        };
        assert!(error.to_string().contains("does not match"));
        assert!(!error.to_string().contains(TEST_KEY_ID));
        assert!(!error.to_string().contains(TEST_API_SECRET));
    }
    assert!(mock.captured().is_empty());
}
