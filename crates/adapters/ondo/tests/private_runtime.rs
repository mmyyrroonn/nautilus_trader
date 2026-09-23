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

//! Offline lifecycle tests for the Ondo private runtime (plan §R3.1).
//!
//! Everything here is offline and reaches no venue. The private socket is a loopback WebSocket
//! endpoint this file owns, the REST reads go to a loopback HTTP endpoint this file scripts, and
//! every credential is the plan's own fake key material.
//!
//! # What this file is evidence for, and what it is not
//!
//! It is evidence that the **lifecycle** works: login, subscribe, recovery, convergence, a session
//! ending and new risk being refused, a reconnect converging again, an explicit stop that leaves
//! nothing running, and the fail-closed answers to a login refusal, a subscription the venue
//! rejects, a frame this adapter cannot decode, a buffer that overflows and a read-only session.
//!
//! It is **not** evidence about the venue. The frames the endpoint sends are authored from the
//! frozen spec rather than observed, and no sandbox key exists in this phase
//! (`test_data/README.md`, plan §R5.2). Three things in particular stay unverified and are marked
//! as such where they live: the login digest's concatenation order
//! ([`nautilus_ondo::signing`]), what renews an armed switch, and what an `update` on the switch
//! channel means ([`nautilus_ondo::websocket::private::session`]).
//!
//! # The clock is real
//!
//! The transport runs on the process's Tokio runtime and reads the real clock, so `tokio::time`
//! virtual time does not reach it. Every wait here is a real one with a bounded timeout, and where
//! a test needs a pass to run it asks for one rather than waiting for the interval.

use std::{
    cell::RefCell,
    net::SocketAddr,
    num::NonZeroU32,
    ops::Deref,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::{replace_data_event_sender, replace_exec_event_sender},
    messages::{
        DataEvent, ExecutionEvent,
        execution::{SubmitOrder, SubmitOrderList},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType, OrderSide, OrderType, PositionSide, TimeInForce},
    events::OrderEventAny,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TraderId,
        VenueOrderId,
    },
    orders::{Order, OrderAny, OrderList, builder::OrderTestBuilder},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_network::ratelimiter::quota::Quota;
use nautilus_ondo::{
    common::{
        consts::{ONDO_SETTLEMENT_CURRENCY, ONDO_VENUE},
        credential::OndoCredential,
        enums::OndoEnvironment,
    },
    config::OndoExecutionClientConfig,
    execution::{OndoExecutionClient, OndoStreamIngestion},
    http::{private::OndoApiFill, rate_limit::OndoRateBudget},
    reconciliation::{
        DeadMansSwitchState, Finding, JournalStatus, MetadataValidity, NewRiskRefusal,
        ProbeDisposition, ReconciliationState, StopOutcome, StopStep, UncertainKind,
    },
    recording::PublicFrame,
    websocket::{
        messages::WsOp,
        private::{
            OndoPrivateSession, PrivateAction, PrivateChannel, PrivateRunState, PrivateStreamMode,
            session::PrivateSessionPhase,
        },
    },
};
use rstest::rstest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;

const ACCOUNT_ID: &str = "ONDO-SANDBOX-001";
const CLIENT_ID: &str = "ONDO-EXEC";
/// The plan's own fake credential. It is never a real key and it never leaves this process.
const TEST_KEY_ID: &str = "ondoKeyId_UNIT_TEST_ONLY";
const TEST_API_SECRET: &str = "ondoApiSecret_UNIT_TEST_ONLY";
const NVDA: &str = "NVDA-USD-PERP.ONDO";
const NVDA_MARKET: &str = "NVDA-USD.P";
const VENUE_ORDER_ID: &str = "197ec08e001658690721be129e7fa595";
const CLIENT_ORDER_ID: &str = "ondo_private_1";

/// The synthetic `GET /v1/markets` body the account's metadata refresh reads.
const MARKETS_BODY: &str = include_str!("../test_data/rest/markets_synthetic.json");

/// How long any single wait in this file is given before it fails.
const WAIT: Duration = Duration::from_secs(10);

fn now() -> UnixNanos {
    nautilus_core::time::get_atomic_clock_realtime().get_time_ns()
}

// ------------------------------------------------------------------------------------------------
// The loopback REST endpoint
// ------------------------------------------------------------------------------------------------

/// One request as the REST mock saw it.
#[derive(Clone, Debug)]
struct Captured {
    method: String,
    target: String,
    body: String,
}

/// What the REST mock answers, and what it holds back.
#[derive(Debug)]
struct RestState {
    /// The order payloads the orders read returns.
    orders: Mutex<Vec<String>>,
    /// The fill payloads the fill history returns.
    fills: Mutex<Vec<String>>,
    /// The position payloads the positions read returns.
    positions: Mutex<Vec<String>>,
    /// The funding payment payloads the funding history returns.
    funding: Mutex<Vec<String>>,
    /// The answer a create request gets, when a test scripts one.
    ///
    /// Unset by default: an unscripted create is answered `404`, which is what every test that
    /// asserts a submission never becomes a request relies on.
    create_answer: Mutex<Option<String>>,
    production_close_fill_limit: Mutex<Option<String>>,
    production_residual_override: Mutex<Option<String>>,
    contracts: Mutex<String>,
    reject_next_close: AtomicBool,
    available_margin_override: Mutex<Option<String>>,
    /// The answer a single-order cancel (`DELETE /v1/perps/orders/{id}`) gets, when a test scripts
    /// one.
    ///
    /// Unset by default, and the default is what the stop tests turn on: an unscripted cancel is
    /// answered `404`, which the adapter reads as a refusal that never reached the venue - so the
    /// order stays resting and the stop has something real to be careful about.
    cancel_answer: Mutex<Option<String>>,
    /// The answer a single-order read (`GET /v1/perps/orders/{id}`) gets, when a test scripts one.
    ///
    /// Unset by default: an unscripted read is answered `404`, so a confirming query after an
    /// unscripted cancel fails and the cancel stays unconfirmed.
    order_answer: Mutex<Option<String>>,
    /// Whether the orders read waits for [`Self::release`] before answering.
    hold_orders: AtomicBool,
    /// Signals that a held orders read has arrived.
    held: Notify,
    /// Releases a held orders read.
    release: Notify,
    /// Whether a create request waits for [`Self::release_create`] before answering.
    hold_create: AtomicBool,
    /// Releases a held create request.
    release_create: Notify,
    /// Whether a single-order DELETE waits for [`Self::release_cancels`] before answering.
    hold_cancels: AtomicBool,
    /// Releases a held DELETE.
    release_cancels: Notify,
}

impl RestState {
    fn new() -> Self {
        Self {
            orders: Mutex::new(Vec::new()),
            fills: Mutex::new(Vec::new()),
            positions: Mutex::new(Vec::new()),
            funding: Mutex::new(Vec::new()),
            create_answer: Mutex::new(None),
            production_close_fill_limit: Mutex::new(None),
            production_residual_override: Mutex::new(None),
            contracts: Mutex::new(
                r#"[{"market":"NVDA-USD.P","isClosed":false,"disabled":false}]"#.into(),
            ),
            reject_next_close: AtomicBool::new(false),
            available_margin_override: Mutex::new(None),
            cancel_answer: Mutex::new(None),
            order_answer: Mutex::new(None),
            hold_orders: AtomicBool::new(false),
            held: Notify::new(),
            release: Notify::new(),
            hold_create: AtomicBool::new(false),
            release_create: Notify::new(),
            hold_cancels: AtomicBool::new(false),
            release_cancels: Notify::new(),
        }
    }

    async fn await_a_held_read(&self) {
        tokio::time::timeout(WAIT, self.held.notified())
            .await
            .expect("an orders read reached the mock");
    }
}

/// A loopback HTTP endpoint that answers the four reads a reconciliation pass makes.
#[derive(Debug)]
struct MockRest {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<Captured>>>,
    state: Arc<RestState>,
    handle: JoinHandle<()>,
}

impl Drop for MockRest {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl MockRest {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(RestState::new());
        let seen = Arc::clone(&requests);
        let shared = Arc::clone(&state);

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };

                let seen = Arc::clone(&seen);
                let state = Arc::clone(&shared);

                tokio::spawn(async move {
                    serve_rest(stream, state, seen).await;
                });
            }
        });

        Self {
            addr,
            requests,
            state,
            handle,
        }
    }

    fn http_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn captured(&self) -> Vec<Captured> {
        self.requests.lock().expect("the request log").clone()
    }

    /// The requests the harness made that were not reads.
    ///
    /// This is the counter every "no new risk was sent" assertion is made against: a submission
    /// that got past admission would be a `POST` here whatever its outcome.
    fn writes(&self) -> usize {
        self.captured()
            .into_iter()
            .filter(|request| request.method != "GET")
            .count()
    }

    fn reads_of(&self, path: &str) -> usize {
        self.captured()
            .into_iter()
            .filter(|request| request.target.starts_with(path))
            .count()
    }

    /// Scripts the answer a create request gets, so a test can have a tracked order.
    fn set_create_answer(&self, answer: &str) {
        *self.state.create_answer.lock().expect("the create answer") = Some(answer.to_string());
    }

    /// Sets the order payloads the orders read returns.
    fn set_orders(&self, orders: &[String]) {
        *self.state.orders.lock().expect("the order script") = orders.to_vec();
    }

    /// Scripts the answer a single-order cancel gets, so a test can have the venue state the order.
    fn set_cancel_answer(&self, answer: &str) {
        *self.state.cancel_answer.lock().expect("the cancel answer") = Some(answer.to_string());
    }

    /// Scripts the answer a single-order read gets, which is what a confirming query asks for.
    fn set_order_answer(&self, answer: &str) {
        *self.state.order_answer.lock().expect("the order answer") = Some(answer.to_string());
    }

    /// The requests the mock saw, in arrival order, as `METHOD target`.
    fn sequence(&self) -> Vec<String> {
        self.captured()
            .into_iter()
            .map(|request| format!("{} {}", request.method, request.target))
            .collect()
    }

    /// The requests whose method is `method`.
    fn with_method(&self, method: &str) -> Vec<Captured> {
        self.captured()
            .into_iter()
            .filter(|request| request.method == method)
            .collect()
    }

    /// Sets the fill history the fills read returns.
    fn set_fills(&self, fills: &[String]) {
        *self.state.fills.lock().expect("the fill script") = fills.to_vec();
    }

    /// Sets the positions the positions read returns.
    fn set_positions(&self, positions: &[String]) {
        *self.state.positions.lock().expect("the position script") = positions.to_vec();
    }

    fn hold_orders(&self) {
        self.state.hold_orders.store(true, Ordering::SeqCst);
    }

    fn release_orders(&self) {
        self.state.hold_orders.store(false, Ordering::SeqCst);
        self.state.release.notify_waiters();
    }

    /// Holds every create request until [`Self::release_create`] is called.
    fn hold_create(&self) {
        self.state.hold_create.store(true, Ordering::SeqCst);
    }

    fn release_create(&self) {
        self.state.hold_create.store(false, Ordering::SeqCst);
        self.state.release_create.notify_waiters();
    }

    /// Holds every single-order DELETE until [`Self::release_cancels`] is called.
    fn hold_cancels(&self) {
        self.state.hold_cancels.store(true, Ordering::SeqCst);
    }

    fn release_cancels(&self) {
        self.state.hold_cancels.store(false, Ordering::SeqCst);
        self.state.release_cancels.notify_waiters();
    }
}

async fn serve_rest(mut stream: TcpStream, state: Arc<RestState>, seen: Arc<Mutex<Vec<Captured>>>) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];

    loop {
        let read = match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        buffer.extend_from_slice(&chunk[..read]);

        if let Some(request) = parse_request(&buffer) {
            if is_websocket_upgrade(&buffer) {
                // This endpoint is the REST surface; the private socket is a server of its own. A
                // misaimed transport is refused here rather than answered from the script.
                write_response(&mut stream, 400, r#"{"success":false}"#).await;

                return;
            }

            seen.lock().expect("the request log").push(request.clone());
            answer(&mut stream, &state, &request).await;

            return;
        }
    }
}

async fn answer(stream: &mut TcpStream, state: &RestState, request: &Captured) {
    let path = request.target.split('?').next().unwrap_or_default();

    match (request.method.as_str(), path) {
        ("GET", "/v1/markets") => write_response(stream, 200, MARKETS_BODY).await,
        ("GET", "/v1/perps/orders") => {
            if state.hold_orders.load(Ordering::SeqCst) {
                state.held.notify_waiters();
                state.release.notified().await;
            }

            let orders = state.orders.lock().expect("the order script").clone();

            write_response(stream, 200, &page(&orders)).await;
        }
        ("GET", "/v1/account") => {
            write_response(stream, 200, &envelope(r#"{"accountID":"unit-account"}"#)).await;
        }
        ("GET", "/v1/perps/contracts") => {
            let contracts = state.contracts.lock().unwrap().clone();
            write_response(stream, 200, &envelope(&contracts)).await;
        }
        ("GET", "/v1/perps/fills") => {
            let fills = state.fills.lock().expect("the fill script").clone();

            write_response(stream, 200, &page(&fills)).await;
        }
        ("GET", "/v1/perps/positions") => {
            let positions = state.positions.lock().expect("the position script").clone();

            write_response(stream, 200, &page(&positions)).await;
        }
        ("GET", "/v1/perps/balance") => {
            let mut balance: serde_json::Value = serde_json::from_str(&balance_json()).unwrap();
            if let Some(amount) = state.available_margin_override.lock().unwrap().as_ref() {
                for key in [
                    "walletBalance",
                    "marginBalance",
                    "availableMargin",
                    "withdrawableMargin",
                ] {
                    balance[key] = serde_json::json!(amount);
                }
                for key in [
                    "realizedPnl",
                    "unrealizedPnl",
                    "usedMargin",
                    "maintenanceMarginRequirement",
                    "totalMaintenanceMargin",
                    "marginRatio",
                    "leverage",
                ] {
                    balance[key] = serde_json::json!("0.00");
                }
            }
            write_response(stream, 200, &envelope(&balance.to_string())).await;
        }
        ("GET", "/v1/perps/funding_fees") => {
            let funding = state.funding.lock().expect("the funding script").clone();

            write_response(stream, 200, &page(&funding)).await;
        }
        ("POST", "/v1/perps/orders") => {
            let request_value: serde_json::Value = serde_json::from_str(&request.body).unwrap();
            if request_value["reduceOnly"] == true
                && state.reject_next_close.swap(false, Ordering::SeqCst)
            {
                write_response(
                    stream,
                    400,
                    r#"{"success":false,"code":"insufficient_margin"}"#,
                )
                .await;
                return;
            }

            if state.hold_create.load(Ordering::SeqCst) {
                state.release_create.notified().await;
            }

            let answer = state
                .create_answer
                .lock()
                .expect("the create answer")
                .clone();

            match answer.as_deref() {
                // The echo mode answers with an order whose client order id is the one the request
                // carried, so a test can submit more than one order against one mock.
                Some("<echo>") => {
                    let client_order_id = serde_json::from_str::<serde_json::Value>(&request.body)
                        .ok()
                        .and_then(|value| {
                            value
                                .get("clientOrderId")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string)
                        })
                        .unwrap_or_default();
                    let venue_order_id = format!("venue-{client_order_id}");

                    write_response(
                        stream,
                        200,
                        &envelope(&api_order(&venue_order_id, &client_order_id, "open")),
                    )
                    .await;
                }
                Some("<production-ioc>") => {
                    let body: serde_json::Value = serde_json::from_str(&request.body).unwrap();
                    let id = body["clientOrderId"].as_str().unwrap();
                    let venue = format!("venue-{id}");
                    let requested = body["size"].as_str().unwrap();
                    let limited = if body["reduceOnly"] == true {
                        state.production_close_fill_limit.lock().unwrap().take()
                    } else {
                        None
                    };
                    let qty = limited.as_deref().unwrap_or(requested);
                    let side = body["side"].as_str().unwrap();
                    let price = body["price"].as_str().unwrap();
                    let mut order: serde_json::Value =
                        serde_json::from_str(&api_order(&venue, id, "fullyfilled")).unwrap();
                    for key in ["side", "price", "size", "timeInForce", "reduceOnly"] {
                        order[key] = body[key].clone();
                    }
                    order["filledSize"] = serde_json::json!(qty);
                    if limited.is_some() {
                        order["status"] = serde_json::json!("canceled");
                    }
                    let mut fill: serde_json::Value =
                        serde_json::from_str(&api_fill(&format!("fill-{id}"), &venue, id, qty))
                            .unwrap();
                    fill["side"] = serde_json::json!(side);
                    fill["price"] = serde_json::json!(price);
                    fill["direction"] = serde_json::json!(if side == "buy" {
                        "openLong"
                    } else {
                        "closeLong"
                    });
                    state.orders.lock().unwrap().push(order.to_string());
                    let fills = {
                        let mut fills = state.fills.lock().unwrap();
                        fills.push(fill.to_string());
                        fills.clone()
                    };
                    let mut net = rust_decimal::Decimal::ZERO;
                    for fill in fills {
                        let f: serde_json::Value = serde_json::from_str(&fill).unwrap();
                        let q = rust_decimal::Decimal::from_str_exact(f["size"].as_str().unwrap())
                            .unwrap();
                        net += if f["side"] == "buy" { q } else { -q };
                    }
                    if body["reduceOnly"] == true {
                        if let Some(residual) =
                            state.production_residual_override.lock().unwrap().as_ref()
                        {
                            net = rust_decimal::Decimal::from_str_exact(residual).unwrap();
                        }
                    }
                    *state.positions.lock().unwrap() = if net == rust_decimal::Decimal::ZERO {
                        Vec::new()
                    } else {
                        vec![api_position(&net.to_string())]
                    };
                    write_response(stream, 200, &envelope(&order.to_string())).await;
                }
                Some(answer) => write_response(stream, 200, &envelope(answer)).await,
                None => write_response(stream, 404, r#"{"success":false}"#).await,
            }
        }
        ("POST", "/v1/perps/orders/batch") => {
            if state.hold_create.load(Ordering::SeqCst) {
                state.release_create.notified().await;
            }

            write_response(stream, 200, &envelope(r#"{"added":[],"failed":[]}"#)).await;
        }
        // One order, by venue order id or by the `client:{id}` form, for a cancel and for the
        // confirming query after one. The market-wide `DELETE /v1/perps/orders` is deliberately not
        // answered here: a stop that sent one would be a defect, and this mock is what proves it
        // never does.
        ("DELETE", path) if path.starts_with("/v1/perps/orders/") => {
            if state.hold_cancels.load(Ordering::SeqCst) {
                state.release_cancels.notified().await;
            }

            let answer = state
                .cancel_answer
                .lock()
                .expect("the cancel answer")
                .clone();

            match answer {
                Some(answer) => write_response(stream, 200, &envelope(&answer)).await,
                None => write_response(stream, 404, r#"{"success":false}"#).await,
            }
        }
        ("GET", path) if path.starts_with("/v1/perps/orders/") => {
            let answer = state.order_answer.lock().expect("the order answer").clone();

            match answer {
                Some(answer) => write_response(stream, 200, &envelope(&answer)).await,
                None => write_response(stream, 404, r#"{"success":false}"#).await,
            }
        }
        _ => write_response(stream, 404, r#"{"success":false}"#).await,
    }
}

/// A page of items in the `GenericResponse` envelope a cursor walk reads.
fn page(items: &[String]) -> String {
    format!(r#"{{"success":true,"result":[{}]}}"#, items.join(","))
}

fn envelope(result: &str) -> String {
    format!(r#"{{"success":true,"result":{result}}}"#)
}

/// The balance summary in the documented shape.
fn balance_json() -> String {
    r#"{"walletBalance":"5000.00","realizedPnl":"250.00","unrealizedPnl":"-50.00","marginBalance":"4950.00","usedMargin":"0.00","availableMargin":"4950.00","withdrawableMargin":"4950.00","maintenanceMarginRequirement":"112.50","totalMaintenanceMargin":"200.00","marginRatio":"0.04","leverage":"0.46","underLiquidation":false,"totalFundingPayments":"-5.67","totalTradingFees":"12.34","totalPnL":"232.00","netInvested":"4750.00"}"#.to_string()
}

/// One `ApiOrder` payload in the venue's documented shape.
fn api_order(order_id: &str, client_order_id: &str, status: &str) -> String {
    format!(
        r#"{{"orderId":"{order_id}","clientOrderId":"{client_order_id}","side":"buy","price":"227.50","size":"1.00","market":"{NVDA_MARKET}","filledSize":"0.00","lastFillSize":"0.00","filledCost":"0.00","fee":"0.00","status":"{status}","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}}"#,
    )
}

/// One `ApiFill` payload in the venue's documented shape.
fn api_fill(id: &str, order_id: &str, client_order_id: &str, size: &str) -> String {
    format!(
        r#"{{"id":"{id}","orderId":"{order_id}","clientOrderId":"{client_order_id}","market":"{NVDA_MARKET}","price":"227.50","size":"{size}","side":"buy","direction":"openLong","fee":"0.00","time":"2025-03-05T14:30:01.000000000Z","isMaker":false}}"#,
    )
}

/// One `ApiOrder` payload for an order the venue reports as complete.
fn api_order_filled(
    order_id: &str,
    client_order_id: &str,
    status: &str,
    filled_size: &str,
) -> String {
    format!(
        r#"{{"orderId":"{order_id}","clientOrderId":"{client_order_id}","side":"buy","price":"227.50","size":"1.00","market":"{NVDA_MARKET}","filledSize":"{filled_size}","lastFillSize":"0.00","filledCost":"0.00","fee":"0.00","status":"{status}","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}}"#,
    )
}

/// One `ApiPosition` payload in the venue's documented shape.
fn api_position(net_quantity: &str) -> String {
    format!(
        r#"{{"market":"{NVDA_MARKET}","direction":"long","netQuantity":"{net_quantity}","averageEntryPrice":"227.50","usedMargin":"45.50","unrealizedPnl":"0.00","markPrice":"227.50","liquidationPrice":"180.00","bankruptcyPrice":"170.00","maintenanceMargin":"2.28","notionalValue":"45.50","leverage":"2.0","netFundingSinceNeutral":"0.00","returnOnEquity":"0.00"}}"#,
    )
}

/// One `ordersPerps` update frame carrying one order item.
fn orders_frame(item: &str) -> String {
    format!(r#"{{"type":"update","channel":"ordersPerps","data":[{item}]}}"#)
}

fn parse_request(buffer: &[u8]) -> Option<Captured> {
    let text = String::from_utf8_lossy(buffer);
    let (head, rest) = text.split_once("\r\n\r\n")?;

    let mut lines = head.lines();
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let target = request_line.next()?.to_string();

    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _value)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_name, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);

    if rest.len() < content_length {
        return None;
    }

    Some(Captured {
        method,
        target,
        body: rest[..content_length].to_string(),
    })
}

fn is_websocket_upgrade(buffer: &[u8]) -> bool {
    let text = String::from_utf8_lossy(buffer);
    let head = text.split("\r\n\r\n").next().unwrap_or_default();

    head.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("upgrade") && value.trim().eq_ignore_ascii_case("websocket")
        })
    })
}

async fn write_response(stream: &mut TcpStream, status: u16, body: &str) {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
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
// The loopback private WebSocket endpoint
// ------------------------------------------------------------------------------------------------

/// How the endpoint answers what the client sends it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Venue {
    /// Acknowledge the login and every subscription: a session that works.
    Ack,
    /// Refuse the login with a permanent error, which the adapter must not retry.
    RefuseLogin,
    /// Acknowledge the login and refuse every subscription.
    RefuseSubscribe,
    NoDmsAck,
    InitialDmsAckOnly,
    NoLoginAck,
    NoReportAck,
    NoReleaseAck,
    WrongReleaseAck,
    DelayedReleaseAck,
    AmbiguousReleaseUpdate,
    MissingReleaseUpdateData,
}

/// The endpoint's record of what the client sent, and its way of pushing back.
///
/// Cloneable on purpose: a test that needs to push a frame while another task waits for a pass to
/// reach its read has to own one of these, and the endpoint task owns the other.
#[derive(Clone, Debug)]
struct PrivateHandle {
    connections: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<String>>>,
    sinks: Arc<Mutex<Vec<mpsc::UnboundedSender<Message>>>>,
}

impl PrivateHandle {
    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn bodies(&self) -> Vec<String> {
        self.bodies.lock().expect("the frame log").clone()
    }

    /// The frames the client sent whose `op` is `op`.
    fn frames_with_op(&self, op: &str) -> Vec<String> {
        let needle = format!(r#""op":"{op}""#);

        self.bodies()
            .into_iter()
            .filter(|body| body.contains(&needle))
            .collect()
    }

    /// Pushes one frame to the client on the most recent connection.
    fn push(&self, frame: &str) {
        let sinks = self.sinks.lock().expect("the sink log");
        let sender = sinks.last().expect("a connection is open");

        sender
            .send(Message::Text(frame.into()))
            .expect("the client is reading");
    }

    /// Closes the most recent connection, which is what a venue-side disconnect looks like.
    fn disconnect(&self) {
        self.sinks.lock().expect("the sink log").pop();
    }
}

/// A loopback WebSocket endpoint standing in for the venue's private channels.
#[derive(Debug)]
struct MockPrivate {
    url: String,
    handle: PrivateHandle,
    task: JoinHandle<()>,
}

impl Drop for MockPrivate {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Deref for MockPrivate {
    type Target = PrivateHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl MockPrivate {
    async fn start(venue: Venue) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}/ws", listener.local_addr().expect("address"));

        let handle = PrivateHandle {
            connections: Arc::new(AtomicUsize::new(0)),
            bodies: Arc::new(Mutex::new(Vec::new())),
            sinks: Arc::new(Mutex::new(Vec::new())),
        };

        let accepted = Arc::clone(&handle.connections);
        let recorded = Arc::clone(&handle.bodies);
        let shared_sinks = Arc::clone(&handle.sinks);

        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                accepted.fetch_add(1, Ordering::SeqCst);

                let recorded = Arc::clone(&recorded);
                let sinks = Arc::clone(&shared_sinks);

                tokio::spawn(async move {
                    let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
                        return;
                    };

                    let (mut sink, mut source) = socket.split();
                    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
                    sinks.lock().expect("the sink log").push(tx);

                    loop {
                        tokio::select! {
                            outbound = rx.recv() => match outbound {
                                Some(message) => {
                                    if sink.send(message).await.is_err() {
                                        return;
                                    }
                                }
                                // Dropping the test's sender is this task's close signal.
                                None => return,
                            },
                            inbound = source.next() => match inbound {
                                Some(Ok(Message::Text(text))) => {
                                    let text = text.to_string();
                                    recorded.lock().expect("the frame log").push(text.clone());

                                    let dms_count=recorded.lock().unwrap().iter().filter(|body|body.contains("cancelAllOrdersAfterPerps") && body.contains("subscribe")).count();
                                    let effective=if venue==Venue::InitialDmsAckOnly && dms_count>1 {Venue::NoDmsAck}else{venue};
                                    if venue==Venue::DelayedReleaseAck && text.contains("unsubscribe") {tokio::time::sleep(Duration::from_millis(80)).await;}
                                    for reply in responses(effective, &text) {
                                        let sinks = sinks.lock().expect("the sink log");
                                        if let Some(sender) = sinks.last() {
                                            let _ = sender.send(Message::Text(reply.into()));
                                        }
                                    }
                                }
                                Some(Ok(_)) => {}
                                Some(Err(_)) | None => return,
                            },
                        }
                    }
                });
            }
        });

        Self { url, handle, task }
    }
}

/// The frames the endpoint answers one client frame with.
fn responses(venue: Venue, text: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return Vec::new();
    };

    let op = value.get("op").and_then(serde_json::Value::as_str);
    let channel = value
        .get("channel")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();

    match (venue, op) {
        (Venue::RefuseLogin, Some("login")) => vec![
            r#"{"type":"error","code":"signature_mismatch","msg":"the signature does not match"}"#
                .to_string(),
        ],
        (Venue::NoLoginAck, Some("login")) => Vec::new(),
        (_, Some("login")) => vec![r#"{"type":"loggedIn","msg":"Login successful"}"#.to_string()],
        (Venue::RefuseSubscribe, Some("subscribe")) => vec![format!(
            r#"{{"type":"error","channel":"{channel}","msg":"subscription refused"}}"#,
        )],
        (Venue::NoDmsAck, Some("subscribe")) if channel == "cancelAllOrdersAfterPerps" => {
            Vec::new()
        }
        (Venue::NoReportAck, Some("subscribe")) if channel != "cancelAllOrdersAfterPerps" => {
            Vec::new()
        }
        (_, Some("subscribe")) => vec![format!(r#"{{"type":"subscribed","channel":"{channel}"}}"#)],
        (Venue::NoReleaseAck, Some("unsubscribe")) => Vec::new(),
        (Venue::WrongReleaseAck, Some("unsubscribe")) => {
            vec![r#"{"type":"unsubscribed","channel":"ordersPerps"}"#.into()]
        }
        (Venue::AmbiguousReleaseUpdate, Some("unsubscribe")) => vec![
            r#"{"type":"update","channel":"cancelAllOrdersAfterPerps","data":{"status":"disabled","enabled":false,"timeout_seconds":0,"op":"unsubscribe","private":"SYNTHETIC_PRIVATE"}}"#.into(),
        ],
        (Venue::MissingReleaseUpdateData, Some("unsubscribe")) => vec![
            r#"{"type":"update","channel":"cancelAllOrdersAfterPerps"}"#.into(),
        ],
        (_, Some("unsubscribe")) => {
            vec![format!(
                r#"{{"type":"unsubscribed","channel":"{channel}"}}"#
            )]
        }
        (_, Some("ping")) => vec![r#"{"type":"pong"}"#.to_string()],
        _ => Vec::new(),
    }
}

// ------------------------------------------------------------------------------------------------
// The harness
// ------------------------------------------------------------------------------------------------

struct Harness {
    client: OndoExecutionClient,
    exec_rx: mpsc::UnboundedReceiver<ExecutionEvent>,
    cache: Rc<RefCell<Cache>>,
    /// Every event this harness has taken off the channel and not yet handed to a test.
    ///
    /// The waits in this file poll the channel, so an event a test wants to assert on would
    /// otherwise be consumed by a wait for something else. Nothing is discarded here: a test that
    /// asks for the events gets all of them, in arrival order.
    events: Vec<ExecutionEvent>,
}

fn sandbox_config() -> OndoExecutionClientConfig {
    OndoExecutionClientConfig {
        environment: OndoEnvironment::Sandbox,
        account_id: Some(AccountId::from(ACCOUNT_ID)),
        api_key: Some(TEST_KEY_ID.to_string()),
        api_secret: Some(TEST_API_SECRET.to_string()),
        ..Default::default()
    }
}

fn build_harness(
    rest: &MockRest,
    private: &MockPrivate,
    config: OndoExecutionClientConfig,
) -> Harness {
    build_harness_on_budget(
        rest,
        private,
        config,
        OndoRateBudget::with_quota(Quota::per_second(NonZeroU32::new(1_000).unwrap()).unwrap()),
    )
}

fn build_harness_on_budget(
    rest: &MockRest,
    private: &MockPrivate,
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
        base_url_http: Some(rest.http_url()),
        base_url_ws: Some(private.url.clone()),
        account_id: Some(account_id),
        ..config
    };

    let credential = OndoCredential::new(
        config.environment,
        TEST_KEY_ID.to_string(),
        TEST_API_SECRET.to_string(),
    )
    .expect("the plan's fake credential is well formed");

    let (exec_tx, exec_rx) = mpsc::unbounded_channel();
    let (data_tx, _data_rx) = mpsc::unbounded_channel::<DataEvent>();
    replace_exec_event_sender(exec_tx);
    replace_data_event_sender(data_tx);

    let client = OndoExecutionClient::with_credential(core, config, Some(credential), Some(budget))
        .expect("the client builds");

    Harness {
        client,
        exec_rx,
        cache,
        events: Vec::new(),
    }
}

fn limit_order(client_order_id: &str) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(NVDA))
        .client_order_id(ClientOrderId::from(client_order_id))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.00"))
        .price(Price::from("227.50"))
        .time_in_force(TimeInForce::Gtc)
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
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

/// Seeds `order` into the cache, exactly as the execution engine does before a submit command.
fn seed_order(harness: &Harness, order: &OrderAny) {
    harness
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("the order should enter the cache");
}

/// Waits until `done` holds, polling the events the client emitted.
async fn wait_until(
    harness: &mut Harness,
    what: &str,
    mut done: impl FnMut(&OndoExecutionClient, &[ExecutionEvent]) -> bool,
) {
    let start = Instant::now();

    loop {
        while let Ok(event) = harness.exec_rx.try_recv() {
            harness.events.push(event);
        }

        if done(&harness.client, &harness.events) {
            return;
        }

        assert!(
            start.elapsed() <= WAIT,
            "timed out waiting for {what} after {:?}; run state is {:?}, the account is {:?}, \
             refusal is {:?}",
            start.elapsed(),
            harness.client.private_run_state(),
            harness.client.reconciliation_state(),
            harness.client.new_risk_refusal(),
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Takes every execution event the client has emitted since the last call.
///
/// It is the waits' own record plus whatever has arrived since, so an event a wait consumed on its
/// way to a different condition still reaches the test that asserts on it.
fn drain_events(harness: &mut Harness) -> Vec<ExecutionEvent> {
    while let Ok(event) = harness.exec_rx.try_recv() {
        harness.events.push(event);
    }

    std::mem::take(&mut harness.events)
}

/// Starts a client and waits for its private session to be established.
async fn connect(harness: &mut Harness) {
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    wait_until(harness, "an established session", |client, _events| {
        matches!(
            client.private_run_state().state,
            PrivateRunState::Recovering | PrivateRunState::ReadOnlySynced
        )
    })
    .await;
}

/// Drives reconciliation passes until the account converges.
///
/// The passes are asked for explicitly rather than waited for: the configured interval is thirty
/// seconds, and what these tests are about is the account's convergence, not the timer. The timer
/// is what [`test_a_periodic_pass_and_a_second_caller_cannot_both_own_the_account`] covers.
async fn converge(harness: &mut Harness) {
    connect(harness).await;

    for _ in 0..8 {
        if harness.client.reconciliation_state() == ReconciliationState::Ready {
            break;
        }

        let _ = harness.client.account().reconcile_account(now()).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Ready,
        "the account converges over a clean REST surface",
    );

    wait_until(harness, "a trading-ready account", |client, _events| {
        client.private_run_state().state == PrivateRunState::TradingReady
    })
    .await;
}

/// Runs one reconciliation pass beside the test, returning its task.
fn spawn_pass(
    harness: &Harness,
) -> JoinHandle<anyhow::Result<nautilus_ondo::reconciliation::AccountJudgment>> {
    let account = harness.client.account();

    tokio::spawn(async move { account.reconcile_account(now()).await })
}

// ------------------------------------------------------------------------------------------------
// The complete lifecycle
// ------------------------------------------------------------------------------------------------

/// The plan's own lifecycle, end to end: create, connect, log in, subscribe, recover, converge,
/// lose the connection, refuse new risk, reconnect and converge again.
#[tokio::test(flavor = "multi_thread")]
async fn test_the_whole_private_lifecycle_from_login_to_a_refused_reconnect() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;

    // The order the venue fixes: the login frame first, and only then the subscriptions.
    let sent = private.bodies();
    let login_at = sent
        .iter()
        .position(|body| body.contains(r#""op":"login""#))
        .expect("the login frame is sent");
    let subscribe_at = sent
        .iter()
        .position(|body| body.contains(r#""op":"subscribe""#))
        .expect("a subscription is sent");

    assert!(login_at < subscribe_at, "{sent:?}");
    assert!(harness.client.can_submit_new_orders());

    // The connection ends.
    private.disconnect();

    wait_until(&mut harness, "the account to be unverified", |client, _| {
        client.reconciliation_state() == ReconciliationState::Disconnected
            && client.refuses_new_risk()
    })
    .await;

    assert!(
        matches!(
            harness.client.new_risk_refusal(),
            Some(NewRiskRefusal::AccountState(_)),
        ),
        "the refusal names the account, not an unknown outcome: {:?}",
        harness.client.new_risk_refusal(),
    );

    // A submission now must not become a request.
    let order = limit_order(CLIENT_ORDER_ID);
    seed_order(&harness, &order);
    let writes_before = rest.writes();

    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is accepted for judging");

    wait_until(&mut harness, "the refusal", |_client, events| {
        events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::Order(OrderEventAny::Denied(_))))
    })
    .await;

    assert_eq!(
        rest.writes(),
        writes_before,
        "a refused submission sends nothing: {:?}",
        rest.captured(),
    );

    // The venue accepts a new connection, and the account converges again.
    let start = Instant::now();

    while private.connection_count() < 2 {
        assert!(
            start.elapsed() <= WAIT,
            "the transport reconnects on its own backoff",
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    for _ in 0..16 {
        if harness.client.private_run_state().state == PrivateRunState::TradingReady {
            break;
        }

        let _ = harness.client.account().reconcile_account(now()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        harness.client.private_run_state().state,
        PrivateRunState::TradingReady,
        "the account converges again after a reconnect",
    );
    assert!(harness.client.can_submit_new_orders());
}

/// A report that arrives while a pass is reading the account is held for that pass and replayed
/// into it, rather than being applied behind the pass's back or lost.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_report_that_arrives_during_a_pass_is_replayed_rather_than_lost() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    connect(&mut harness).await;

    // Hold the orders read, so the pass is provably in flight when the stream reports. The wait
    // runs beside the test, because the test is the only thing that can push a frame.
    rest.hold_orders();

    let endpoint = private.handle.clone();
    let waiter = tokio::spawn({
        let state = Arc::clone(&rest.state);

        async move {
            state.await_a_held_read().await;
            endpoint.push(&orders_frame(&api_order(
                VENUE_ORDER_ID,
                "someone_elses_order",
                "canceled",
            )));
        }
    });

    let pass = spawn_pass(&harness);

    waiter
        .await
        .expect("the held read is reached and the frame is pushed");
    tokio::time::sleep(Duration::from_millis(100)).await;
    rest.release_orders();

    let judged = pass.await.expect("the pass runs to completion");

    assert!(judged.is_ok(), "the pass read the account: {judged:?}");

    let reading = harness.client.last_reading().expect("a reading");
    let reported = reading
        .orders
        .iter()
        .any(|order| order.venue_order_id == VENUE_ORDER_ID);

    assert!(
        reported,
        "the report the stream delivered mid-pass is in the reading that pass concluded: {:?}",
        reading
            .orders
            .iter()
            .map(|order| &order.venue_order_id)
            .collect::<Vec<_>>(),
    );
}

/// A login the venue refuses for good ends the session rather than being retried, and the account
/// never becomes ready.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_refused_login_ends_the_session_and_never_becomes_ready() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::RefuseLogin).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");

    wait_until(&mut harness, "the session to end", |client, _| {
        client.private_run_state().state == PrivateRunState::Stopped
    })
    .await;

    assert!(
        harness
            .client
            .private_run_state()
            .detail
            .contains("signature_mismatch"),
        "the refusal, in the venue's own words, is what the transport ends on: {:?}",
        harness.client.private_run_state(),
    );
    assert!(!harness.client.can_submit_new_orders());

    // The refusal is permanent, so the transport stops rather than reconnecting: the endpoint sees
    // exactly one connection however long the test waits.
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(
        private.connection_count(),
        1,
        "a permanent refusal is not retried",
    );
    assert_eq!(rest.writes(), 0);

    let diagnostics = harness.client.private_diagnostics().expect("diagnostics");
    let rendered = diagnostics.render();

    assert!(
        rendered.contains("signature_mismatch"),
        "the venue's own words are kept: {rendered}",
    );
    assert!(
        !rendered.contains(TEST_API_SECRET),
        "the secret is not in the record: {rendered}",
    );
}

/// A subscription the venue rejects leaves the session unestablished, so the account is never
/// trading-ready however cleanly it reads.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_refused_subscription_never_becomes_ready() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::RefuseSubscribe).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    connect(&mut harness).await;

    // The REST surface is clean, so the account's own passes converge - and the run state still
    // does not, because the report subscriptions were never acknowledged.
    for _ in 0..6 {
        let _ = harness.client.account().reconcile_account(now()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Ready,
        "the account itself reads clean",
    );
    assert_ne!(
        harness.client.private_run_state().state,
        PrivateRunState::TradingReady,
        "a socket with no acknowledged subscriptions is not a trading session",
    );
    assert!(!harness.client.can_submit_new_orders());
    assert_eq!(rest.writes(), 0);
}

/// A frame this adapter cannot decode costs the account its certainty and keeps the connection: the
/// venue may legally send one (`required: []`), and killing the socket over it would take the whole
/// account offline.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_decode_loss_makes_the_account_uncertain_and_keeps_the_socket() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;
    let connections = private.connection_count();

    // An `ordersPerps` item with no `status`: the frozen schema's `required: []` permits it, and
    // the REST decoder cannot do without it.
    private.push(&orders_frame(
        r#"{"orderId":"197ec08e001658690721be129e7fa595","market":"NVDA-USD.P"}"#,
    ));

    wait_until(&mut harness, "the loss to be recorded", |client, _| {
        client.reconciliation_state() == ReconciliationState::Uncertain
    })
    .await;

    assert!(harness.client.refuses_new_risk());
    assert_eq!(
        private.connection_count(),
        connections,
        "the socket survives a payload the adapter could not read",
    );

    let diagnostics = harness.client.private_diagnostics().expect("diagnostics");

    assert_eq!(diagnostics.counters().reports_lost, 1);
    assert!(
        diagnostics.counters().frames_in >= 1,
        "the frame reached the session and was refused there, not dropped by the transport",
    );
}

/// A buffer that overflows stops the account from reading ready: the reports it refused are reports
/// this client saw and does not hold.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_buffer_overflow_makes_the_account_uncertain() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    connect(&mut harness).await;

    rest.hold_orders();

    let mut pass = spawn_pass(&harness);
    rest.state.await_a_held_read().await;

    // One more report than the buffer holds, each with a distinct venue order id.
    let capacity = nautilus_ondo::reconciliation::ONDO_RECOVERY_BUFFER_CAPACITY;

    for index in 0..=capacity {
        private.push(&orders_frame(&api_order(
            &format!("buffered-{index}"),
            "someone_elses_order",
            "canceled",
        )));
    }

    // The overflow is recorded the moment the report the buffer refuses is seen, not at the next
    // pass: waiting for a pass would leave a window in which a converged account is treated as
    // read.
    wait_until(&mut harness, "the overflow to be recorded", |client, _| {
        client.reconciliation_state() == ReconciliationState::Uncertain
    })
    .await;

    assert!(harness.client.refuses_new_risk());

    rest.release_orders();
    let _ = (&mut pass).await;

    // The pass that read the account judges the loss, with the count: the reports this client saw
    // and does not hold are what the account cannot vouch for, and the judgment is where that is
    // stated (plan §6.4).
    let judgment = harness.client.last_judgment().expect("a judgment");
    let lost = judgment.findings.iter().find_map(|finding| match finding {
        Finding::LostReports { count, .. } => Some(*count),
        _ => None,
    });

    assert!(
        lost.is_some_and(|count| count >= 1),
        "the refused reports are counted and judged, not merely logged: {judgment:?}",
    );
}

/// A periodic pass and anything else that asks for one never run beside each other: the claim is
/// the account's, and the second caller is refused rather than queued behind the first.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_periodic_pass_and_a_second_caller_cannot_both_own_the_account() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(
        &rest,
        &private,
        OndoExecutionClientConfig {
            // One second, so the run loop's timer fires while the first pass is held.
            reconcile_interval_secs: 1,
            ..sandbox_config()
        },
    );

    connect(&mut harness).await;

    rest.hold_orders();

    let mut pass = spawn_pass(&harness);
    rest.state.await_a_held_read().await;

    let before = rest.reads_of("/v1/perps/orders");
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    assert_eq!(
        rest.reads_of("/v1/perps/orders"),
        before,
        "a pass already owns the account, so a tick reads nothing: {:?}",
        rest.captured(),
    );

    // And the same refusal is visible to a caller that asks directly.
    let refused = harness.client.account().reconcile_account(now()).await;

    assert!(
        refused.is_err(),
        "a second pass is refused at the claim rather than run beside the first",
    );

    rest.release_orders();
    let _ = (&mut pass).await;
}

/// A read-only account session reads the account and never arms the switch, which is the whole
/// point: arming it is a frame with a cancelling side effect.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_read_only_session_never_arms_the_switch_and_never_trades() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(
        &rest,
        &private,
        OndoExecutionClientConfig {
            account_read_only: true,
            ..sandbox_config()
        },
    );

    connect(&mut harness).await;

    for _ in 0..8 {
        if harness.client.private_run_state().state == PrivateRunState::ReadOnlySynced {
            break;
        }

        let _ = harness.client.account().reconcile_account(now()).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let switch_frames: Vec<String> = private
        .bodies()
        .into_iter()
        .filter(|body| body.contains("cancelAllOrdersAfterPerps"))
        .collect();

    assert!(
        switch_frames.is_empty(),
        "a read-only session never subscribes to the switch: {switch_frames:?}",
    );
    assert_eq!(
        harness.client.new_risk_refusal(),
        Some(NewRiskRefusal::AccountIsReadOnly),
    );
    assert_eq!(
        harness.client.private_run_state().state,
        PrivateRunState::ReadOnlySynced,
        "a read-only session is synced, never trading-ready",
    );

    // The native snapshot is the application's read-only view of exactly this: an accepted login
    // and the acknowledged report channels, with the switch absent, and the run state that says so.
    let snapshot = harness.client.read_only_diagnostics().snapshot();

    assert!(snapshot.logged_in, "the venue accepted the login");
    assert_eq!(snapshot.run_state, "read_only_synced");
    assert!(snapshot.subscriptions_acked.contains(&"ordersPerps"));
    assert!(snapshot.subscriptions_acked.contains(&"fillsPerps"));
    assert_eq!(snapshot.identity_match, "unknown");
    assert_eq!(snapshot.shutdown_status, "running");

    // And a submission is refused by name, with nothing sent.
    let order = limit_order(CLIENT_ORDER_ID);
    seed_order(&harness, &order);

    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is judged");

    wait_until(&mut harness, "the read-only refusal", |_client, events| {
        events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::Order(OrderEventAny::Denied(_))))
    })
    .await;

    assert_eq!(rest.writes(), 0);
}

/// A read-only session triggers no switch side effect and sends no cancel of any shape.
///
/// Read-only is a property the client sets on itself, not a promise its caller keeps
/// ([`nautilus_ondo::reconciliation::ReconciliationMachine::mark_account_read_only`]): the switch's
/// arm has a cancelling side effect at the venue, so a read-only session that armed one would be
/// changing the account it was only asked to read. The same is true of the stop sequence's release
/// step, which is why this test stops the session and looks at the wire afterwards rather than
/// reading the code.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_read_only_session_triggers_no_switch_side_effect_and_no_cancel() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(
        &rest,
        &private,
        OndoExecutionClientConfig {
            account_read_only: true,
            ..sandbox_config()
        },
    );

    connect(&mut harness).await;

    for _ in 0..8 {
        if harness.client.private_run_state().state == PrivateRunState::ReadOnlySynced {
            break;
        }

        let _ = harness.client.account().reconcile_account(now()).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        harness.client.new_risk_refusal(),
        Some(NewRiskRefusal::AccountIsReadOnly),
        "the session reads the account and refuses every order for that reason alone",
    );
    assert!(
        harness.client.account().dead_mans_switch_permits_orders(),
        "and it asked for no switch, so there is none for it to be stopped by",
    );

    harness.client.disconnect().await.expect("the stop");

    assert_eq!(
        harness
            .client
            .read_only_diagnostics()
            .snapshot()
            .shutdown_status,
        "complete",
        "a read-only stop has nothing to cancel and finishes clean",
    );

    let switch_frames: Vec<String> = private
        .bodies()
        .into_iter()
        .filter(|body| body.contains("cancelAllOrdersAfterPerps"))
        .collect();

    assert!(
        switch_frames.is_empty(),
        "a read-only session never arms, renews or releases the switch: {switch_frames:?}",
    );

    let cancels: Vec<Captured> = rest
        .captured()
        .into_iter()
        .filter(|request| request.method == "DELETE")
        .collect();

    assert!(
        cancels.is_empty(),
        "a read-only session sends no cancel, market-wide or otherwise: {cancels:?}",
    );
    assert_eq!(
        rest.writes(),
        0,
        "and no write of any other shape either: {:?}",
        rest.captured(),
    );
}

/// The deadline is enforced at the submission gate: a new order that arrives after it is refused by
/// name, and **nothing is sent**.
///
/// This is the end-to-end half of §R3.3's first three items. The unit tests prove the projection and
/// the predicate; this proves the live client reads them, with the transport running and the account
/// otherwise ready to trade.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_new_order_after_the_deadline_is_refused_with_nothing_sent() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;

    // The account is ready and the switch governs it, so the only thing that stops the order below
    // is the deadline. Sixty seconds is room to arm without reaching `converge`'s own wait for the
    // switch's confirmation.
    let armed_at = now();

    harness
        .client
        .account()
        .dead_mans_switch_confirmed_at(armed_at);

    assert_eq!(
        harness.client.new_risk_refusal(),
        None,
        "inside its deadline the switch still governs the account",
    );

    // Now the deadline is in the past: confirmed one minute and the switch's own timeout ago, so it
    // lapsed the instant it was confirmed and no live timer can move it forward again.
    let lapsed_at = UnixNanos::from(
        armed_at
            .as_u64()
            .saturating_sub(60 * 1_000_000_000)
            .saturating_sub(30 * 1_000_000_000),
    );

    harness
        .client
        .account()
        .dead_mans_switch_confirmed_at(lapsed_at);

    let refusal = harness
        .client
        .new_risk_refusal()
        .expect("a lapsed switch refuses new risk");

    assert!(
        matches!(
            refusal,
            NewRiskRefusal::DeadMansSwitch(DeadMansSwitchState::Lapsed { .. })
        ),
        "the refusal names the lapse and not the venue's firing: {refusal:?}",
    );

    let writes_before = rest.writes();
    let order = limit_order(CLIENT_ORDER_ID);

    seed_order(&harness, &order);

    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is judged");

    wait_until(
        &mut harness,
        "the refusal of the lapsed order",
        |_client, events| {
            events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::Order(OrderEventAny::Denied(_))))
        },
    )
    .await;

    assert_eq!(
        rest.writes(),
        writes_before,
        "a refused order is not a request: {:?}",
        rest.captured(),
    );
}

/// The run state is the account's axis and the switch's joined, and a switch that lapses takes the
/// session out of `TradingReady` on the next tick - however clean the account still reads.
///
/// This is §R3.3's third item at the run loop's own site, and it is the one the deadline could not
/// reach before: the state tick recomputes the run state from
/// [`OndoAccountRuntime::dead_mans_switch_permits_orders`], which read the stored state and never
/// the clock, so a session whose armed switch had already lapsed went on reporting itself ready to
/// trade every 250 ms. Nothing else happens in this test - no order arrives, no frame is sent, the
/// account is not read again - so the deadline is the only thing that can move the state.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_lapsed_switch_takes_the_run_state_out_of_trading_ready() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;

    assert_eq!(
        harness.client.private_run_state().state,
        PrivateRunState::TradingReady,
        "the account is read and the switch is confirmed to begin with",
    );

    // The confirmation is placed a minute and a timeout in the past, which is the only way to reach
    // a lapsed switch without waiting out a real thirty-second timeout: the transport reads the same
    // clock these tests do, so moving that clock would move the thing under test with it.
    let lapsed_at = UnixNanos::from(
        now()
            .as_u64()
            .saturating_sub(60 * 1_000_000_000)
            .saturating_sub(30 * 1_000_000_000),
    );

    harness
        .client
        .account()
        .dead_mans_switch_confirmed_at(lapsed_at);

    // Nothing else is done: the 250 ms state tick is what has to notice.
    wait_until(
        &mut harness,
        "the run state to leave trading ready",
        |client, _events| client.private_run_state().state != PrivateRunState::TradingReady,
    )
    .await;

    let run = harness.client.private_run_state();

    assert_eq!(
        run.state,
        PrivateRunState::Recovering,
        "an account that still reads clean behind a switch that lapsed is recovering: {run:?}",
    );
    assert!(
        run.detail.contains("deadline"),
        "and the run says which way it is not ready rather than blaming an unconfirmed switch: \
         {run:?}",
    );

    // The submission gate refuses the same way, by name, and the refusal never becomes a request.
    let writes_before = rest.writes();
    let order = limit_order(CLIENT_ORDER_ID);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is judged");

    wait_until(
        &mut harness,
        "the refusal of the lapsed order",
        |_client, events| {
            events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::Order(OrderEventAny::Denied(_))))
        },
    )
    .await;

    assert_eq!(
        rest.writes(),
        writes_before,
        "a refused order is not a request: {:?}",
        rest.captured(),
    );
}

// ------------------------------------------------------------------------------------------------
// The stop executor (plan §R3.3)
// ------------------------------------------------------------------------------------------------

/// Builds a harness whose account is ready and which holds one resting order this run placed.
async fn harness_with_one_order(
    rest: &MockRest,
    private: &MockPrivate,
    config: OndoExecutionClientConfig,
) -> Harness {
    rest.set_create_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open"));

    let mut harness = build_harness(rest, private, config);

    converge(&mut harness).await;

    let order = limit_order(CLIENT_ORDER_ID);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    // The order index is written before the create request is sent, so `tracks` alone returns while
    // `venue_order_id` is still `None`; the stop reads that id to name the cancel, and the test
    // below asserts the request names it. Wait for the id itself, not for mere presence.
    wait_until(&mut harness, "the venue order id", |client, _events| {
        client
            .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
            .is_some_and(|state| state.venue_order_id.is_some())
    })
    .await;

    harness
}

/// New risk stops first: an order submitted after the stop is refused and **no request is sent**.
///
/// This is step one of the sequence as evidence rather than as an ordering in a `Vec`. The stop ends
/// the account's session before it does anything else, so the admission the submission path reads
/// is already refusing by the time the stop returns - and a refusal that never became a request is
/// what the mock's request log shows.
#[tokio::test(flavor = "multi_thread")]
async fn test_an_order_after_the_stop_is_refused_with_nothing_sent() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.set_cancel_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "canceled"));

    let mut harness = harness_with_one_order(&rest, &private, sandbox_config()).await;
    let report = harness
        .client
        .stop_and_wait(now(), Duration::from_secs(5))
        .await;

    assert!(report.is_clean(), "{report:?}");

    let writes_after_the_stop = rest.writes();
    let order = limit_order("ondo_after_the_stop");

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is judged");

    wait_until(
        &mut harness,
        "the refusal of a post-stop order",
        |_client, events| {
            events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::Order(OrderEventAny::Denied(_))))
        },
    )
    .await;

    assert!(
        harness.client.refuses_new_risk(),
        "the stopped account refuses new risk: {:?}",
        harness.client.reconciliation_state(),
    );
    assert_eq!(
        rest.writes(),
        writes_after_the_stop,
        "and the refusal never became a request: {:?}",
        rest.sequence(),
    );
}

/// The stop cancels this run's own orders by id and never sends a market-wide delete.
///
/// The venue's market cancel (`DELETE /v1/perps/orders?market=...`) takes no ownership filter: it
/// cancels every order on the market, including ones this client never placed. The stop's ownership
/// is a list of this client's own order ids, and this is what proves the request is that list
/// rather than a market.
#[tokio::test(flavor = "multi_thread")]
async fn test_the_stop_cancels_this_runs_own_orders_by_id_and_never_a_market() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // The venue states the order as cancelled, so the stop has nothing left to be careful about.
    rest.set_cancel_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "canceled"));

    let mut harness = harness_with_one_order(&rest, &private, sandbox_config()).await;

    assert!(
        harness.client.account().dead_mans_switch_permits_orders(),
        "a converged trading session holds a switch, so the stop has one to release",
    );

    let report = harness
        .client
        .stop_and_wait(now(), Duration::from_secs(5))
        .await;

    assert!(report.is_clean(), "{report:?}");
    assert_eq!(report.cancellations_issued, 1);
    assert!(report.released_switch);
    assert!(
        report.stream_ended,
        "the transport task ended with the stop"
    );

    let deletes = rest.with_method("DELETE");

    assert_eq!(deletes.len(), 1, "{:?}", rest.sequence());
    assert_eq!(
        deletes[0].target,
        format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
        "the cancel is this run's own order, named by the venue order id",
    );
    assert!(
        !deletes[0].target.contains("market="),
        "a market-wide cancel is not this client's to send: {}",
        deletes[0].target,
    );

    // And it is the *only* delete: no second, wider request was made behind it.
    assert!(
        rest.sequence()
            .iter()
            .filter(|request| request.starts_with("DELETE"))
            .count()
            == 1,
        "{:?}",
        rest.sequence(),
    );
}

/// The switch is released only after the cancels have been confirmed by a read, and the order on
/// the wire shows it: the delete, then the query, then the release frame.
#[tokio::test(flavor = "multi_thread")]
async fn test_the_stop_confirms_the_cancels_before_it_releases_the_switch() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // The cancel is answered `success` **without** an order payload - the one documented shape
    // that says "accepted" and not "done" - so the adapter has to ask what became of the order.
    rest.set_cancel_answer("{}");
    rest.set_order_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "canceled"));

    let mut harness = harness_with_one_order(&rest, &private, sandbox_config()).await;

    let report = harness
        .client
        .stop_and_wait(now(), Duration::from_secs(5))
        .await;

    assert!(report.is_clean(), "{report:?}");

    let sequence = rest.sequence();
    let cancel_at = sequence
        .iter()
        .position(|request| request.starts_with("DELETE") && request.contains(VENUE_ORDER_ID))
        .expect("the stop cancelled the order");
    let confirm_at = sequence
        .iter()
        .position(|request| request.starts_with("GET") && request.contains(VENUE_ORDER_ID))
        .expect("the stop asked what became of the order it cancelled");

    assert!(
        confirm_at > cancel_at,
        "the query follows the cancel it is confirming: {sequence:?}",
    );
    assert!(
        matches!(
            harness
                .client
                .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
                .map(|state| state.status),
            Some(nautilus_ondo::http::orders::OndoOrderStatus::Canceled),
        ),
        "the query is what settled the order, not the cancel call",
    );
    assert_eq!(
        private.frames_with_op("unsubscribe").len(),
        1,
        "and only then is the switch released: {:?}",
        private.bodies(),
    );
}

/// A cancel this client cannot confirm keeps the switch armed.
///
/// Releasing the switch after an unconfirmed cancel would hand the account back with an order
/// nobody can account for and no protection covering it - the tidier exit bought with the one thing
/// the sequence exists to keep.
#[tokio::test(flavor = "multi_thread")]
async fn test_an_unconfirmed_cancel_keeps_the_switch_armed() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // Neither the cancel nor the query that would confirm it is answered.
    let mut harness = harness_with_one_order(&rest, &private, sandbox_config()).await;
    let armed_at = now();

    harness
        .client
        .account()
        .dead_mans_switch_confirmed_at(armed_at);

    assert!(harness.client.account().dead_mans_switch_permits_orders());

    let report = harness
        .client
        .stop_and_wait(now(), Duration::from_secs(5))
        .await;

    assert_eq!(
        report.unconfirmed_cancels,
        vec![ClientOrderId::from(CLIENT_ORDER_ID)],
        "the cancel is registered as unconfirmed: {report:?}",
    );
    assert!(
        !report.released_switch,
        "the switch stays armed over an order this client cannot account for: {report:?}",
    );
    assert!(!report.is_clean(), "{report:?}");
    assert!(
        !rest.with_method("DELETE").is_empty(),
        "the stop did try: {:?}",
        rest.sequence(),
    );

    // The switch is still the one that was armed and confirmed before the stop: had the stop
    // released it, the account would permit orders again over an order nobody can account for.
    assert!(
        harness.client.account().dead_mans_switch_permits_orders(),
        "the switch was not released, so it is still the armed one: {report:?}",
    );
    assert_eq!(
        harness.client.new_risk_refusal(),
        Some(NewRiskRefusal::UnconfirmedCancels {
            client_order_ids: vec![ClientOrderId::from(CLIENT_ORDER_ID)],
        }),
        "and the account is refused by the cancel rather than reported clean",
    );

    // The transport, at least, is down: the stop does not abandon the socket over a cancel.
    assert!(report.stream_ended);
}

/// A stop whose cancels never settle reports a timeout rather than the completion it did not see.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_stop_that_times_out_reports_a_timeout_and_not_a_completion() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // Nothing answers the cancel, so the order stays working for as long as the stop waits.
    let mut harness = harness_with_one_order(&rest, &private, sandbox_config()).await;

    let started = Instant::now();
    let report = harness
        .client
        .stop_and_wait(now(), Duration::from_millis(300))
        .await;

    assert!(
        started.elapsed() >= Duration::from_millis(300),
        "the stop waits for its bound before giving up: {:?}",
        started.elapsed(),
    );
    assert_eq!(
        report.outcome,
        StopOutcome::TimedOut,
        "a wait that ran out is not a completion: {report:?}",
    );
    assert!(
        !report.outstanding().is_empty(),
        "and the work it was still waiting on is named: {report:?}",
    );
    assert!(!report.is_clean(), "{report:?}");
    assert!(
        !report.released_switch,
        "the switch is not released over an order that may still be resting: {report:?}",
    );
}

/// A restart inherits what the stop left outstanding, and does not reach Ready on a clean read.
///
/// This is plan §R3.3's last item: the journal carries the unconfirmed writes, so the restart's
/// account starts from what the last run could not account for rather than from an empty account
/// that a clean read would make tradable.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_restart_inherits_the_stops_outstanding_cancels() {
    let journal = JournalPath::new("stop-pending");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // The first run places an order and stops over a cancel nothing answers.
    let mut first = harness_with_one_order(&rest, &private, journal.config()).await;
    let report = first
        .client
        .stop_and_wait(now(), Duration::from_secs(5))
        .await;

    assert_eq!(
        report.unconfirmed_cancels,
        vec![ClientOrderId::from(CLIENT_ORDER_ID)],
        "{report:?}",
    );

    let written = journal_at(&journal).expect("the stop checkpointed what it left");

    assert_eq!(
        written
            .unsettled()
            .iter()
            .map(|entry| entry.client_order_id().to_string())
            .collect::<Vec<_>>(),
        vec![CLIENT_ORDER_ID.to_string()],
        "the outstanding cancel is in the journal the stop wrote",
    );
    assert!(
        written
            .unsettled()
            .iter()
            .any(|entry| entry.kind == UncertainKind::Cancel),
        "as a cancel, which is what a restart re-registers it as",
    );

    drop(first);

    // The venue now reads clean: no orders, and the pass would call the account Ready.
    rest.set_orders(&[]);
    rest.set_fills(&[]);

    let mut second = build_harness(&rest, &private, journal.config());

    assert!(
        matches!(
            second.client.account().journal_status(),
            JournalStatus::Restored { .. }
        ),
        "the restart restores the journal: {:?}",
        second.client.account().journal_status(),
    );

    // The restart is driven to the same point of the recovery the first run reached, and the venue
    // now reads clean - so the *only* thing standing between it and a tradable account is the entry
    // the journal restored. This is deliberately **not** `converge`: that helper asserts the account
    // reaches `Ready`, and the whole claim here is that this one must not.
    connect(&mut second).await;

    for _ in 0..8 {
        let _ = second.client.account().reconcile_account(now()).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_ne!(
        second.client.reconciliation_state(),
        ReconciliationState::Ready,
        "a clean read of the venue is not enough: the restored cancel is what the account is judged          on, and it is still there",
    );
    assert_eq!(
        second.client.new_risk_refusal(),
        Some(NewRiskRefusal::UnconfirmedCancels {
            client_order_ids: vec![ClientOrderId::from(CLIENT_ORDER_ID)],
        }),
        "the refusal names the cancel the last run left, which the journal is what restored",
    );
    assert!(
        second.client.refuses_new_risk(),
        "so the restart is not a trading account however clean the venue reads",
    );
}

/// The credential never reaches the private session's own record, and the login frame never reaches
/// the public recorder's whitelist.
#[tokio::test(flavor = "multi_thread")]
async fn test_the_login_frame_is_outside_every_recording_boundary() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;

    // The frame really did carry the credential: the endpoint received it.
    let login = private
        .frames_with_op("login")
        .into_iter()
        .next()
        .expect("a login frame is sent");

    assert!(login.contains(TEST_KEY_ID), "{login}");
    assert_eq!(
        login.matches(r#""sign""#).count(),
        1,
        "the login frame carries exactly one signature member: {login}",
    );

    // The public recorder refuses that same body by name.
    assert!(
        PublicFrame::outbound(&login).is_err(),
        "the public raw market-data whitelist must refuse a login body",
    );

    // And the private session's own record does not hold it.
    let diagnostics = harness.client.private_diagnostics().expect("diagnostics");
    let rendered = diagnostics.render();

    assert!(!rendered.contains(TEST_KEY_ID), "{rendered}");
    assert!(!rendered.contains(TEST_API_SECRET), "{rendered}");
    assert!(
        !rendered.contains(r#""op":"login""#),
        "the record holds the fact that a login was sent, not the frame: {rendered}",
    );
    assert_eq!(diagnostics.counters().login_frames, 1);

    // The signature is a hex digest of the login message, so nothing in the record may look like
    // one: the record holds no frame bytes at all.
    let signature = login
        .split(r#""sign":""#)
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("the login frame carries a signature");

    assert_eq!(signature.len(), 64, "a hex HMAC-SHA256 digest: {signature}");
    assert!(!rendered.contains(signature), "{rendered}");
}

/// Stopping leaves nothing running, and the client says so rather than leaving it to be assumed.
#[tokio::test(flavor = "multi_thread")]
async fn test_stop_leaves_no_task_behind() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;
    assert!(harness.client.private_stream_is_running());

    // Every await point of a stop: the transport is stopped, and the task has ended by the time
    // the call returns.
    harness.client.disconnect().await.expect("disconnect");

    assert!(
        !harness.client.private_stream_is_running(),
        "the transport's task has ended by the time disconnect returns",
    );
    assert_eq!(
        harness.client.private_run_state().state,
        PrivateRunState::Disconnected,
        "a disconnected client has no account session, and says so",
    );

    // Stopping again is a no-op rather than a second teardown.
    harness.client.disconnect().await.expect("disconnect again");
    assert!(!harness.client.private_stream_is_running());

    // A client that is reconnected gets a fresh transport, and a synchronous stop takes it down.
    harness.client.connect().await.expect("reconnect");

    for _ in 0..16 {
        if harness.client.private_run_state().state == PrivateRunState::TradingReady {
            break;
        }

        let _ = harness.client.account().reconcile_account(now()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(harness.client.private_stream_is_running());

    harness.client.stop().expect("stop");
    await_the_transport_to_end(&harness).await;
}

/// The synchronous stop takes the socket down too, and the task really ends rather than being
/// merely abandoned.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_synchronous_stop_also_ends_the_transport_task() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;
    harness.client.stop().expect("stop");

    await_the_transport_to_end(&harness).await;
}

/// Waits for the transport's task to be observably finished.
async fn await_the_transport_to_end(harness: &Harness) {
    let start = Instant::now();

    while harness.client.private_stream_is_running() {
        assert!(
            start.elapsed() <= WAIT,
            "the transport task outlived its stop",
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ------------------------------------------------------------------------------------------------
// The real lifecycle hooks carry out the ordered stop (plan §R3.3)
// ------------------------------------------------------------------------------------------------

/// The frame the stop puts on the switch's channel when it releases it.
fn is_a_switch_release(body: &str) -> bool {
    body.contains("cancelAllOrdersAfterPerps") && body.contains(r#""op":"unsubscribe""#)
}

/// The graceful `disconnect` hook carries out the ordered stop: this run's own order is cancelled
/// by id and confirmed, and only then is the switch released and the transport closed.
///
/// This is the R5.2 gap: before it, the lifecycle's `disconnect` stopped the transport and nothing
/// else, so the real path never cancelled an owned order, never confirmed a cancel and never
/// applied the ordered switch release. The hook used here is the one `LiveNode` drives, not the
/// account runtime directly.
#[tokio::test(flavor = "multi_thread")]
async fn test_disconnect_cancels_this_runs_order_and_releases_the_switch() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // The venue states the order as cancelled, so the stop has nothing left to be careful about.
    rest.set_cancel_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "canceled"));

    let mut harness = harness_with_one_order(&rest, &private, sandbox_config()).await;

    harness.client.disconnect().await.expect("the disconnect");

    let deletes = rest.with_method("DELETE");

    assert_eq!(deletes.len(), 1, "{:?}", rest.sequence());
    assert_eq!(
        deletes[0].target,
        format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
        "the cancel is this run's own order, named by the venue order id",
    );
    assert!(
        !deletes[0].target.contains("market="),
        "a market-wide cancel is not this client's to send: {}",
        deletes[0].target,
    );

    // The release is only on the wire: the switch's own channel is where the step lands.
    let releases: Vec<String> = private
        .bodies()
        .into_iter()
        .filter(|body| is_a_switch_release(body))
        .collect();

    assert_eq!(releases.len(), 1, "{:?}", private.bodies());
    assert!(
        !harness.client.private_stream_is_running(),
        "the transport task has ended by the time the disconnect returns",
    );
    assert!(!harness.client.is_connected());
}

/// A caller that bounds the disconnect and cuts it short does not lose the ledger.
///
/// The cancel is registered **before** its request exists, so when the node's own
/// `timeout_disconnection` drops the stop future mid-wait the stop's checkpoint guard still writes
/// it. The switch stays armed - the release step was never reached - and the transport is still
/// alive for the acknowledgements the stop was waiting for.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_disconnect_the_caller_cuts_short_checkpoints_the_unconfirmed_cancel() {
    let journal = JournalPath::new("disconnect-cut-short");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // Nothing answers the cancel or the confirming query, so the order stays working and the stop
    // is still waiting when the caller's bound fires.
    let mut harness = harness_with_one_order(&rest, &private, journal.config()).await;

    let cut_short = tokio::time::timeout(Duration::from_secs(1), harness.client.disconnect()).await;

    assert!(
        cut_short.is_err(),
        "the caller's bound is what ended the disconnect",
    );

    // The cancel really was attempted; the guard is what makes its outcome survive the cut.
    assert!(
        !rest.with_method("DELETE").is_empty(),
        "{:?}",
        rest.sequence(),
    );

    let written = journal_at(&journal).expect("the guard checkpointed the ledger on the way out");

    assert!(
        written.unsettled().iter().any(|entry| {
            entry.client_order_id().to_string() == CLIENT_ORDER_ID
                && entry.kind == UncertainKind::Cancel
        }),
        "the unconfirmed cancel is journaled: {:?}",
        written.unsettled(),
    );

    let releases: Vec<String> = private
        .bodies()
        .into_iter()
        .filter(|body| is_a_switch_release(body))
        .collect();

    assert!(
        releases.is_empty(),
        "the switch is not released over an unconfirmed cancel: {releases:?}",
    );
    assert!(
        harness.client.account().dead_mans_switch_permits_orders(),
        "the armed switch is still covering the order",
    );
    assert_eq!(
        harness.client.new_risk_refusal(),
        Some(NewRiskRefusal::UnconfirmedCancels {
            client_order_ids: vec![ClientOrderId::from(CLIENT_ORDER_ID)],
        }),
        "and the account is refused by the cancel rather than reported clean",
    );

    // The transport was kept alive for the acknowledgement the stop was still waiting for.
    assert!(
        harness.client.private_stream_is_running(),
        "the stop had not reached the close step, so the socket is still up",
    );

    harness
        .client
        .stop()
        .expect("the synchronous stop closes what remains");
}

/// A submission whose answer is unknown keeps the switch armed and is journaled when the caller
/// ends the session: an order that may be resting is never left uncovered.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_disconnect_over_an_unknown_submission_stays_armed_and_journaled() {
    let journal = JournalPath::new("disconnect-unknown");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // The create is answered with a body this adapter cannot read, so the submission's outcome is
    // unknown: it may have been applied, and the account must not be reported clean.
    rest.set_create_answer("not-a-payload");

    let mut harness = build_harness(&rest, &private, journal.config());

    converge(&mut harness).await;

    let order = limit_order(CLIENT_ORDER_ID);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    wait_until(&mut harness, "the unknown submission", |client, _events| {
        matches!(
            client.new_risk_refusal(),
            Some(NewRiskRefusal::UnknownSubmissions { .. })
        )
    })
    .await;

    // The order never settles, so the stop is still waiting when the caller cuts it short.
    let cut_short = tokio::time::timeout(Duration::from_secs(1), harness.client.disconnect()).await;

    assert!(
        cut_short.is_err(),
        "the caller's bound is what ended the disconnect",
    );

    let written = journal_at(&journal).expect("the guard checkpointed the ledger on the way out");

    assert!(
        written.unsettled().iter().any(|entry| {
            entry.client_order_id().to_string() == CLIENT_ORDER_ID
                && entry.kind == UncertainKind::Submission
        }),
        "the unknown submission is journaled: {:?}",
        written.unsettled(),
    );

    let releases: Vec<String> = private
        .bodies()
        .into_iter()
        .filter(|body| is_a_switch_release(body))
        .collect();

    assert!(
        releases.is_empty(),
        "the switch is not released over a submission that may be resting: {releases:?}",
    );
    assert_eq!(
        harness.client.new_risk_refusal(),
        Some(NewRiskRefusal::UnknownSubmissions {
            client_order_ids: vec![ClientOrderId::from(CLIENT_ORDER_ID)],
        }),
        "the account is refused by the unknown submission rather than reported clean",
    );

    harness.client.stop().expect("stop");
}

/// The stop cancels only the orders this run placed: an order the venue reports that this client
/// does not own is read, identified and left alone.
#[tokio::test(flavor = "multi_thread")]
async fn test_disconnect_never_cancels_an_order_this_run_does_not_own() {
    const FOREIGN_ORDER_ID: &str = "foreign-venue-order-1";

    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.set_cancel_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "canceled"));

    let mut harness = harness_with_one_order(&rest, &private, sandbox_config()).await;

    // The venue now reports a foreign order beside this run's own. A pass reads it and identifies
    // it; it never enters the order index this client cancels from.
    rest.set_orders(&[
        api_order(FOREIGN_ORDER_ID, "ondo_foreign_1", "open"),
        api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open"),
    ]);

    let _ = harness.client.account().reconcile_account(now()).await;

    assert!(
        !harness
            .client
            .tracks(&ClientOrderId::from("ondo_foreign_1")),
        "a foreign order is identified, never adopted",
    );

    harness.client.disconnect().await.expect("the disconnect");

    let deletes = rest.with_method("DELETE");

    assert_eq!(
        deletes.len(),
        1,
        "only this run's own order is cancelled: {:?}",
        rest.sequence(),
    );
    assert_eq!(
        deletes[0].target,
        format!("/v1/perps/orders/{VENUE_ORDER_ID}"),
    );
    assert!(
        !rest
            .sequence()
            .iter()
            .any(|request| request.contains(FOREIGN_ORDER_ID)),
        "the foreign order is never named by a cancel: {:?}",
        rest.sequence(),
    );
}

/// The client the factory builds is the one the live node drives, and its `disconnect` hook runs
/// the ordered stop: with no orders there is nothing to cancel, and the switch it armed on connect
/// is released.
#[tokio::test(flavor = "multi_thread")]
async fn test_the_factory_built_client_runs_the_ordered_stop_on_disconnect() {
    use nautilus_common::factories::ExecutionClientFactory;
    use nautilus_ondo::factories::OndoExecutionClientFactory;

    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    let (exec_tx, _exec_rx) = mpsc::unbounded_channel::<ExecutionEvent>();
    let (data_tx, _data_rx) = mpsc::unbounded_channel::<DataEvent>();
    replace_exec_event_sender(exec_tx);
    replace_data_event_sender(data_tx);

    let config = OndoExecutionClientConfig {
        base_url_http: Some(rest.http_url()),
        base_url_ws: Some(private.url.clone()),
        ..sandbox_config()
    };
    let cache = Rc::new(RefCell::new(Cache::default()));

    let mut client = OndoExecutionClientFactory::new()
        .create(
            TraderId::from("TESTER-001"),
            CLIENT_ID,
            &config,
            cache.into(),
        )
        .expect("the factory builds the native execution client");

    client.start().expect("start");
    client.connect().await.expect("connect");

    // Wait for the switch to be armed on the wire, so the release step has one to carry out.
    let start = Instant::now();

    while !private.bodies().iter().any(|body| {
        body.contains("cancelAllOrdersAfterPerps") && body.contains(r#""op":"subscribe""#)
    }) {
        assert!(
            start.elapsed() <= WAIT,
            "the private session never armed the switch",
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    client.disconnect().await.expect("the disconnect");

    let releases: Vec<String> = private.frames_with_op("unsubscribe");

    assert!(
        releases.iter().any(|body| is_a_switch_release(body)),
        "the factory-built client's disconnect releases the switch: {releases:?}",
    );
    assert!(
        rest.with_method("DELETE").is_empty(),
        "no orders, so no cancel: {:?}",
        rest.sequence(),
    );
    assert!(!client.is_connected());
}

// ------------------------------------------------------------------------------------------------
// The correction round: a successful query is not a confirmed cancellation, and shutdown is one
// bounded budget over every owned task (review findings 1-6)
// ------------------------------------------------------------------------------------------------

/// A batch submission carrying `orders` as one native list.
fn order_list_command(orders: &[OrderAny]) -> SubmitOrderList {
    let inits: Vec<_> = orders
        .iter()
        .map(|order| order.init_event().clone())
        .collect();

    let list = OrderList::new(
        OrderListId::from("OL-1"),
        InstrumentId::from(NVDA),
        StrategyId::from("S-001"),
        orders.iter().map(|order| order.client_order_id()).collect(),
        UnixNanos::default(),
    );

    SubmitOrderList::new(
        TraderId::from("TESTER-001"),
        Some(ClientId::from(CLIENT_ID)),
        StrategyId::from("S-001"),
        list,
        inits,
        None, // exec_algorithm_id
        None, // position_id
        None, // params
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    )
}

/// Submits `client_order_id` and waits until the venue has named its order.
async fn submit_and_await_venue_id(harness: &mut Harness, client_order_id: &str) -> String {
    let order = limit_order(client_order_id);

    seed_order(harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    wait_until(harness, "the venue order id", |client, _events| {
        client
            .order_state(&ClientOrderId::from(client_order_id))
            .is_some_and(|state| state.venue_order_id.is_some())
    })
    .await;

    harness
        .client
        .order_state(&ClientOrderId::from(client_order_id))
        .and_then(|state| state.venue_order_id)
        .expect("the venue order id")
        .to_string()
}

/// Waits until the mock has seen a request of `method`, or fails.
async fn await_mock_method(rest: &MockRest, method: &str, what: &str) {
    let start = Instant::now();

    while rest.with_method(method).is_empty() {
        assert!(start.elapsed() <= WAIT, "the mock never saw {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The switch frames a private session sent.
fn switch_frames(private: &MockPrivate) -> Vec<String> {
    private
        .bodies()
        .into_iter()
        .filter(|body| body.contains("cancelAllOrdersAfterPerps"))
        .collect()
}

/// A cancel accepted without an order and then confirmed by a query that says the order is still
/// open is **not** a confirmed cancellation. The stop must not release the switch over it.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_working_confirming_get_keeps_the_switch_armed() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.set_cancel_answer("{}");
    rest.set_order_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open"));

    let mut harness = harness_with_one_order(&rest, &private, sandbox_config()).await;

    let error = harness
        .client
        .disconnect()
        .await
        .expect_err("a working order is not a cancelled one");

    assert!(error.to_string().contains("outcome=TimedOut"), "{error}");
    assert!(
        switch_frames(&private)
            .iter()
            .all(|frame| !is_a_switch_release(frame)),
        "the switch is not released over a working order: {:?}",
        switch_frames(&private),
    );
    assert!(
        harness.client.account().dead_mans_switch_permits_orders(),
        "the armed switch is still the one covering the order",
    );
    assert_eq!(
        harness.client.new_risk_refusal(),
        Some(NewRiskRefusal::UnconfirmedCancels {
            client_order_ids: vec![ClientOrderId::from(CLIENT_ORDER_ID)],
        }),
    );
}

/// The background probe that finds a working order keeps the cancel unconfirmed.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_background_probe_that_finds_a_working_order_keeps_the_cancel() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.set_order_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open"));

    let mut harness = harness_with_one_order(&rest, &private, sandbox_config()).await;

    harness.client.account().note_unconfirmed_cancel(
        ClientOrderId::from(CLIENT_ORDER_ID),
        Some(VenueOrderId::from(VENUE_ORDER_ID)),
        "the cancel request was not answered".to_string(),
        now(),
    );

    let reports = harness
        .client
        .account()
        .probe_unknown_submissions(now())
        .await;

    assert_eq!(reports.len(), 1, "{reports:?}");
    assert!(
        matches!(reports[0].disposition, ProbeDisposition::KeepProbing { .. }),
        "a working order keeps the cancel outstanding: {reports:?}",
    );
    assert_eq!(
        harness.client.unconfirmed_cancels().len(),
        1,
        "the probe did not confirm a cancel over a working order",
    );
    assert!(harness.client.refuses_new_risk());

    harness.client.stop().expect("stop");
}

/// A terminal order whose fills do not agree with the venue's own total is not a settled order, and
/// it keeps the switch armed.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_terminal_order_with_missing_fills_keeps_the_switch_armed() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // The venue states a canceled order it also says filled 1.00, but no fill is ever reported.
    rest.set_create_answer(&api_order_filled(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "canceled",
        "1.00",
    ));

    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;

    let order = limit_order(CLIENT_ORDER_ID);

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    wait_until(&mut harness, "the terminal order", |client, _events| {
        client
            .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
            .is_some_and(|state| state.status.is_terminal())
    })
    .await;

    assert!(
        harness
            .client
            .unresolved_orders()
            .contains(&ClientOrderId::from(CLIENT_ORDER_ID)),
        "the terminal order is unresolved because its fills do not add up",
    );

    let error = harness
        .client
        .disconnect()
        .await
        .expect_err("an unreconciled terminal order is not settled");

    assert!(error.to_string().contains("outcome=TimedOut"), "{error}");
    assert!(
        switch_frames(&private)
            .iter()
            .all(|frame| !is_a_switch_release(frame)),
        "the switch is not released over an unreconciled fill: {:?}",
        switch_frames(&private),
    );
}

/// A read-only restart that restored an owned order and an unconfirmed cancel from its journal
/// sends no DELETE and no switch frame, and keeps the inherited work reportable.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_read_only_restart_never_cancels_from_a_restored_journal() {
    let journal = JournalPath::new("read-only-restored");

    // First run: a trading session places one order and stops over a cancel nothing answers, so the
    // journal it leaves holds an owned order and an unconfirmed cancel for it.
    {
        let rest = MockRest::start().await;
        let private = MockPrivate::start(Venue::Ack).await;
        let mut first = harness_with_one_order(&rest, &private, journal.config()).await;

        let _ = first
            .client
            .stop_and_wait(now(), Duration::from_millis(300))
            .await;
    }

    let written = journal_at(&journal).expect("the first run checkpointed");

    assert!(
        written
            .unsettled()
            .iter()
            .any(|entry| entry.client_order_id().to_string() == CLIENT_ORDER_ID),
        "the first run left an unconfirmed cancel: {:?}",
        written.unsettled(),
    );

    // The restart is read-only against the same journal.
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut second = build_harness(
        &rest,
        &private,
        OndoExecutionClientConfig {
            account_read_only: true,
            ..journal.config()
        },
    );

    assert!(
        matches!(
            second.client.account().journal_status(),
            JournalStatus::Restored { .. }
        ),
        "the read-only run restores the journal: {:?}",
        second.client.account().journal_status(),
    );

    second.client.start().expect("start");
    second.client.connect().await.expect("connect");

    let error = second
        .client
        .disconnect()
        .await
        .expect_err("the inherited work is not a clean shutdown");

    assert!(
        error.to_string().contains("UnconfirmedCancels")
            || error.to_string().contains("outstanding"),
        "{error}"
    );
    assert!(
        rest.with_method("DELETE").is_empty(),
        "a read-only session never sends a cancel: {:?}",
        rest.sequence(),
    );
    assert!(
        switch_frames(&private).is_empty(),
        "a read-only session never arms, renews or releases the switch: {:?}",
        switch_frames(&private),
    );
    assert_eq!(
        second.client.new_risk_refusal(),
        Some(NewRiskRefusal::AccountIsReadOnly),
    );

    // The inherited work is still reportable: the read-only stop did not erase it.
    let kept = journal_at(&journal).expect("the read-only run kept the journal");

    assert!(
        kept.unsettled().iter().any(
            |entry| entry.client_order_id().to_string() == CLIENT_ORDER_ID
                && entry.kind == UncertainKind::Cancel
        ),
        "the inherited cancel is still in the journal: {:?}",
        kept.unsettled(),
    );
}

/// A shutdown cut short while the first cancel is blocked still names **every** owned order: the
/// pre-registration and the checkpoint happen before the first request.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_shutdown_cut_short_registers_every_owned_order() {
    let journal = JournalPath::new("cut-short-two-orders");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.set_create_answer("<echo>");

    let mut harness = build_harness(&rest, &private, journal.config());

    converge(&mut harness).await;

    let first = submit_and_await_venue_id(&mut harness, "ondo_two_1").await;
    let second = submit_and_await_venue_id(&mut harness, "ondo_two_2").await;

    assert_ne!(first, second);

    // The first DELETE blocks in the mock, so the second order never gets its own request before
    // the caller's bound drops the shutdown future.
    rest.hold_cancels();

    let cut_short = tokio::time::timeout(Duration::from_secs(1), harness.client.disconnect()).await;

    assert!(
        cut_short.is_err(),
        "the caller's bound is what ended the shutdown",
    );

    let written = journal_at(&journal).expect("the guard checkpointed the ledger");
    let mut unsettled: Vec<String> = written
        .unsettled()
        .iter()
        .map(|entry| entry.client_order_id().to_string())
        .collect();

    unsettled.sort();
    assert!(
        unsettled == vec!["ondo_two_1".to_string(), "ondo_two_2".to_string()],
        "both owned orders are registered before the first request: {unsettled:?}",
    );
    assert!(
        switch_frames(&private)
            .iter()
            .all(|frame| !is_a_switch_release(frame)),
        "the switch is not released by a shutdown that was cut short",
    );

    rest.release_cancels();
    harness.client.stop().expect("stop");
}

/// A blocked in-flight submission is drained within the bound, its order survives in the journal,
/// and the drained generation can be reset and reopened for genuine work.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_blocked_submission_drains_and_the_generation_can_reopen() {
    let journal = JournalPath::new("blocked-submit");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.set_create_answer("<echo>");
    rest.hold_create();

    let mut harness = build_harness(&rest, &private, journal.config());

    converge(&mut harness).await;

    let order = limit_order("ondo_blocked");

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    // The POST is in flight and held by the mock.
    await_mock_method(&rest, "POST", "the held create request").await;

    let error = harness
        .client
        .disconnect()
        .await
        .expect_err("an in-flight submission is not a settled one");

    assert!(
        error.to_string().contains("tasks_drained=true"),
        "the request task generation drained within its bound: {error}",
    );

    let written = journal_at(&journal).expect("the stop checkpointed the in-flight order");

    assert!(
        written
            .unsettled()
            .iter()
            .any(|entry| entry.client_order_id().to_string() == "ondo_blocked"),
        "the in-flight order survives in the journal: {:?}",
        written.unsettled(),
    );

    // The generation drained, so `reset` can open a fresh one; a recovered session can submit.
    // Before that, the venue answers the in-flight order as terminal, which is what settles the
    // pre-registered cancel and lets a genuine recovery reach trading again.
    rest.release_create();
    rest.set_order_answer(&api_order("venue-ondo_blocked", "ondo_blocked", "canceled"));

    let _ = harness
        .client
        .account()
        .probe_unknown_submissions(now())
        .await;

    assert!(
        harness.client.unconfirmed_cancels().is_empty(),
        "the venue's terminal answer settled the pre-registered cancel",
    );

    harness
        .client
        .reset()
        .expect("reset after a drained shutdown");
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    converge(&mut harness).await;

    let before = rest.with_method("POST").len();
    let recovered = submit_and_await_venue_id(&mut harness, "ondo_recovered").await;

    assert!(!recovered.is_empty());
    assert!(
        rest.with_method("POST").len() > before,
        "the reopened generation placed a new order: {:?}",
        rest.sequence(),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_ownership_is_scoped_to_the_current_generation() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;
    harness
        .client
        .disconnect()
        .await
        .expect("the first shutdown");
    harness.client.stop().expect("stop the first lifecycle");
    harness.client.reset().expect("reopen the generation");
    converge(&mut harness).await;

    rest.set_create_answer("<echo>");
    let id = "ondo_second_generation";
    let venue_id = submit_and_await_venue_id(&mut harness, id).await;
    rest.set_cancel_answer(&api_order(&venue_id, id, "canceled"));

    harness.client.stop().expect("stop the second lifecycle");
    let result = harness.client.disconnect().await;

    assert!(
        rest.with_method("DELETE")
            .iter()
            .any(|request| request.target.ends_with(&venue_id)),
        "the second generation's order must be cleaned, even after a previous clean shutdown: \
         result={result:?}, requests={:?}",
        rest.sequence(),
    );
    harness
        .client
        .reset()
        .expect("the second generation drained");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_ownership_cancels_requests_when_the_caller_abandons_drain() {
    let journal = JournalPath::new("abandoned-request-drain");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    rest.set_create_answer("<echo>");
    rest.hold_create();

    let mut harness = build_harness(&rest, &private, journal.config());
    converge(&mut harness).await;
    let id = ClientOrderId::from("ondo_abandoned_drain");
    let order = limit_order(id.as_str());
    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("submit");
    await_mock_method(&rest, "POST", "the blocked submission").await;

    assert!(
        tokio::time::timeout(Duration::from_millis(100), harness.client.disconnect())
            .await
            .is_err(),
    );
    harness
        .client
        .stop()
        .expect("the node's final synchronous stop");
    let persisted = journal_at(&journal).expect("the in-flight work was checkpointed");
    assert!(
        persisted
            .unsettled()
            .iter()
            .any(|entry| entry.client_order_id() == id.as_str())
    );

    // Allow the abandoned request to answer. A stopped generation must never apply that answer.
    rest.release_create();
    let late_acceptance = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if harness
                .client
                .order_state(&id)
                .is_some_and(|state| state.venue_order_id.is_some())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        late_acceptance.is_err(),
        "the request applied its answer after final stop"
    );

    rest.set_order_answer(&api_order(
        "venue-ondo_abandoned_drain",
        id.as_str(),
        "canceled",
    ));
    let _ = harness
        .client
        .account()
        .probe_unknown_submissions(now())
        .await;
    let _ = harness.client.disconnect().await;
    harness
        .client
        .reset()
        .expect("the canceled generation is still drainable");
}

/// A blocked in-flight batch is drained within the bound and every item survives in the journal.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_blocked_batch_submission_drains_and_preserves_every_item() {
    let journal = JournalPath::new("blocked-batch");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.hold_create();

    let mut harness = build_harness(&rest, &private, journal.config());

    converge(&mut harness).await;

    let first = limit_order("ondo_batch_1");
    let second = limit_order("ondo_batch_2");

    seed_order(&harness, &first);
    seed_order(&harness, &second);
    harness
        .client
        .submit_order_list(order_list_command(&[first, second]))
        .expect("the command is handled");

    // The batch POST is in flight and held by the mock.
    await_mock_method(&rest, "POST", "the held batch request").await;

    let error = harness
        .client
        .disconnect()
        .await
        .expect_err("an in-flight batch is not a settled one");

    assert!(
        error.to_string().contains("tasks_drained=true"),
        "the request task generation drained within its bound: {error}",
    );

    let written = journal_at(&journal).expect("the stop checkpointed the batch");
    let mut unsettled: Vec<String> = written
        .unsettled()
        .iter()
        .map(|entry| entry.client_order_id().to_string())
        .collect();

    unsettled.sort();
    assert_eq!(
        unsettled,
        vec!["ondo_batch_1".to_string(), "ondo_batch_2".to_string()],
        "every batch item is registered before the request is dropped",
    );

    rest.release_create();
    harness.client.stop().expect("stop");
}

/// A submission a definitive refusal answers during the drain clears the cancel the shutdown
/// pre-registered for it: an order that never rested must not leave a registration that would lock
/// the account forever.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_definitive_refusal_during_shutdown_clears_its_registration() {
    let journal = JournalPath::new("refused-during-shutdown");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // No create answer is scripted, so the create the submission makes is refused `404`.
    rest.hold_create();

    let mut harness = build_harness(&rest, &private, journal.config());

    converge(&mut harness).await;

    let order = limit_order("ondo_refused");

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    await_mock_method(&rest, "POST", "the held create request").await;

    // Release the create while the shutdown drains, so the task finishes and records the refusal.
    let release = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        rest.release_create();
    };

    let (result, ()) = tokio::join!(harness.client.disconnect(), release);

    result.expect("a refused order leaves nothing outstanding");

    let written = journal_at(&journal).expect("the stop checkpointed");

    assert!(
        !written
            .unsettled()
            .iter()
            .any(|entry| entry.client_order_id().to_string() == "ondo_refused"),
        "no stale cancel registration for a never-rested order: {:?}",
        written.unsettled(),
    );
}

/// A synchronous `stop` before `disconnect` does not cause the async hook to return success by
/// early return: the REST cleanup still runs and the un-releasable switch keeps the result dirty,
/// and a repeated hook preserves that result.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_synchronous_stop_before_disconnect_still_reports_dirty() {
    let journal = JournalPath::new("stop-before-disconnect");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.set_cancel_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "canceled"));

    let mut harness = harness_with_one_order(&rest, &private, journal.config()).await;

    harness.client.stop().expect("the synchronous stop");

    // The later async hook runs the bounded REST cleanup and names the un-releasable switch.
    let error = harness
        .client
        .disconnect()
        .await
        .expect_err("a stop with no transport cannot release the switch");

    assert!(
        error.to_string().contains("switch_released=false"),
        "{error}",
    );
    assert!(
        !rest.with_method("DELETE").is_empty(),
        "the REST cancel still ran after the synchronous stop: {:?}",
        rest.sequence(),
    );

    // A repeated hook preserves the dirty result rather than relabelling it clean.
    let again = harness
        .client
        .disconnect()
        .await
        .expect_err("the dirty result is preserved");

    assert!(
        again.to_string().contains("switch_released=false"),
        "{again}"
    );
}

/// An unknown submission survives real lifecycle shutdown and blocks a restarted client until
/// terminal evidence settles the original identity, without submitting a replacement order.
#[tokio::test(flavor = "multi_thread")]
async fn test_an_unknown_submission_survives_shutdown_and_restart() {
    let journal = JournalPath::new("unknown-restart");

    {
        let rest = MockRest::start().await;
        let private = MockPrivate::start(Venue::Ack).await;

        // The create is answered with a body this adapter cannot read, so the submission is
        // unknown. The lifecycle hook must drain tasks and checkpoint the unresolved identity.
        rest.set_create_answer("not-a-payload");

        let mut first = build_harness(&rest, &private, journal.config());

        converge(&mut first).await;

        let order = limit_order(CLIENT_ORDER_ID);

        seed_order(&first, &order);
        first
            .client
            .submit_order(submit_command(&order))
            .expect("the command is handled");

        wait_until(&mut first, "the unknown submission", |client, _events| {
            matches!(
                client.new_risk_refusal(),
                Some(NewRiskRefusal::UnknownSubmissions { .. })
            )
        })
        .await;

        let error = first
            .client
            .disconnect()
            .await
            .expect_err("an unknown submission prevents clean shutdown");

        assert!(error.to_string().contains(CLIENT_ORDER_ID), "{error}");
        assert!(error.to_string().contains("tasks_drained=true"), "{error}");
        assert!(!first.client.private_stream_is_running());
        assert_eq!(rest.with_method("POST").len(), 1, "no replacement submit");
    }

    let written = journal_at(&journal).expect("the first run checkpointed");

    assert!(
        written.unsettled().iter().any(|entry| {
            entry.client_order_id().to_string() == CLIENT_ORDER_ID
                && entry.kind == UncertainKind::Submission
        }),
        "the unknown submission is in the journal: {:?}",
        written.unsettled(),
    );

    // The restart restores the journal and refuses new risk on the unknown submission.
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut second = build_harness(&rest, &private, journal.config());

    assert!(matches!(
        second.client.account().journal_status(),
        JournalStatus::Restored { .. }
    ));

    second.client.start().expect("start");
    second.client.connect().await.expect("connect");

    for _ in 0..8 {
        let _ = second.client.account().reconcile_account(now()).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_ne!(
        second.client.reconciliation_state(),
        ReconciliationState::Ready,
        "a clean venue read is not enough over a restored unknown submission",
    );
    assert_eq!(
        second.client.new_risk_refusal(),
        Some(NewRiskRefusal::UnknownSubmissions {
            client_order_ids: vec![ClientOrderId::from(CLIENT_ORDER_ID)],
        }),
    );

    rest.set_order_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "canceled"));
    let started = Instant::now();

    while second.client.reconciliation_state() != ReconciliationState::Ready {
        let _ = second
            .client
            .account()
            .probe_unknown_submissions(now())
            .await;
        let _ = second.client.account().reconcile_account(now()).await;
        assert!(
            started.elapsed() <= WAIT,
            "the original order never recovered: {:?}",
            second.client.new_risk_refusal()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let recovered = second
        .client
        .order_state(&ClientOrderId::from(CLIENT_ORDER_ID))
        .expect("recovery keeps the original owned identity");
    assert_eq!(
        recovered.client_order_id,
        ClientOrderId::from(CLIENT_ORDER_ID)
    );
    assert_eq!(
        recovered.venue_order_id,
        Some(VenueOrderId::from(VENUE_ORDER_ID))
    );
    assert!(recovered.is_settled());
    assert!(second.client.unknown_submissions().is_empty());
    assert!(second.client.unconfirmed_cancels().is_empty());
    assert!(!second.client.refuses_new_risk());
    assert!(
        rest.with_method("POST").is_empty(),
        "recovery must not replace the submission"
    );

    second
        .client
        .disconnect()
        .await
        .expect("the recovered client shuts down cleanly");
}

/// Releasing the switch is a step the stop sequence names, and the frame it needs exists.
///
/// This is the wrapper [`nautilus_ondo::reconciliation::DeadMansSwitch::release`] was missing:
/// `stop_sequence()` could return the step and nothing could carry it out.
#[tokio::test(flavor = "multi_thread")]
async fn test_the_release_step_has_a_frame_to_send() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;

    let armed = harness.client.stop_sequence();

    assert!(
        armed.contains(&StopStep::ReleaseDeadMansSwitch),
        "an armed switch is a step of the stop sequence: {armed:?}",
    );
    assert_eq!(
        armed.last(),
        Some(&StopStep::ClosePrivateStream),
        "the stream closes last",
    );

    let frame = harness.client.account().release_dead_mans_switch(now());

    assert_eq!(frame.op, WsOp::Unsubscribe);
    assert_eq!(frame.channel, "cancelAllOrdersAfterPerps");
    assert_eq!(frame.timeout_seconds, 30);

    let json = frame.to_json_text().expect("the frame serializes");

    assert_eq!(
        json,
        r#"{"op":"unsubscribe","channel":"cancelAllOrdersAfterPerps","timeout_seconds":30}"#,
    );

    // Released: the sequence no longer names the step, and nothing is required of the account any
    // more. That the release *permits* orders again is exactly why the stop sequence cancels and
    // confirms this run's own orders before it releases anything.
    assert!(
        !harness
            .client
            .stop_sequence()
            .contains(&StopStep::ReleaseDeadMansSwitch),
    );
    assert!(harness.client.account().dead_mans_switch_permits_orders());
}

/// An armed switch is renewed before the venue's own timer would fire it.
///
/// **The renewal message is unverified.** The frozen material documents the subscribe frame and the
/// timeout and says nothing about which message renews an armed switch, so this adapter re-sends the
/// subscribe frame - the only renewal the documentation admits - and a sandbox session is what
/// settles it (plan §R3.3). What this test pins is the adapter's own half: that it renews, at half
/// the timeout, inside the login-required session that armed it.
#[tokio::test(flavor = "multi_thread")]
async fn test_an_armed_switch_is_renewed_before_its_timeout() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(
        &rest,
        &private,
        OndoExecutionClientConfig {
            // Two seconds, so the renewal lands at one and the test does not wait half a minute.
            dms_timeout_secs: 2,
            ..sandbox_config()
        },
    );

    converge(&mut harness).await;

    let switch_frames = || -> Vec<String> {
        private
            .bodies()
            .into_iter()
            .filter(|body| body.contains("cancelAllOrdersAfterPerps"))
            .collect()
    };

    assert_eq!(
        switch_frames().len(),
        1,
        "arming is one subscribe, and it comes after the login: {:?}",
        private.bodies(),
    );

    let start = Instant::now();

    while switch_frames().len() < 2 {
        assert!(
            start.elapsed() <= WAIT,
            "the switch is renewed at half its timeout",
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        private
            .bodies()
            .iter()
            .position(|body| body.contains("cancelAllOrdersAfterPerps"))
            .is_some_and(|at| at > 0),
        "the switch is never armed before the login frame",
    );

    let diagnostics = harness.client.private_diagnostics().expect("diagnostics");

    assert!(
        diagnostics.records().iter().any(|record| matches!(
            record,
            nautilus_ondo::websocket::private::PrivateRecord::SwitchRenewed { .. }
        )),
        "the renewal is recorded: {:?}",
        diagnostics.records(),
    );
    assert!(
        harness.client.account().dead_mans_switch_permits_orders(),
        "a renewed switch still permits orders",
    );
    assert!(
        harness.client.account().dead_mans_switch_renewals() >= 1,
        "and the renewal that reached the socket is the one that moved the deadline: the count is \
         kept where the deadline moves, not where the frame is built",
    );
}

/// The account runtime and the client are one account: what the runtime moves, the client reads.
#[tokio::test(flavor = "multi_thread")]
async fn test_the_runtime_and_the_client_share_one_account() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let harness = build_harness(&rest, &private, sandbox_config());
    let account = harness.client.account();

    assert_eq!(
        account.reconciliation_state(),
        harness.client.reconciliation_state(),
    );

    account.set_metadata(MetadataValidity::Current);

    assert_eq!(
        harness.client.new_risk_refusal(),
        Some(NewRiskRefusal::AccountState(
            ReconciliationState::Disconnected
        )),
        "the metadata the runtime moved is the metadata the client judges with",
    );
}

/// The account session's socket is not started by a client that is never connected.
#[tokio::test(flavor = "multi_thread")]
async fn test_an_unconnected_client_has_no_private_session() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let harness = build_harness(&rest, &private, sandbox_config());

    assert_eq!(
        harness.client.private_run_state().state,
        PrivateRunState::Disconnected,
    );
    assert!(!harness.client.private_stream_is_running());
    assert_eq!(private.connection_count(), 0);
    assert!(harness.client.private_diagnostics().is_none());
    assert!(harness.client.refuses_new_risk());
}

/// The private session's own state machine, offline and without a socket: the login order, the
/// switch, and the read-only mode.
#[rstest]
fn test_the_session_state_machine_needs_no_socket() {
    let mut session = OndoPrivateSession::new(PrivateStreamMode::Trading);

    assert_eq!(session.phase(), &PrivateSessionPhase::Disconnected);
    assert!(
        session
            .wanted()
            .contains(&PrivateChannel::CancelAllOrdersAfterPerps)
    );

    let connecting = session.on_connected();

    assert_eq!(connecting.actions, vec![PrivateAction::Login]);
    assert!(connecting.actions[0].carries_a_credential());

    let acknowledged = session.handle_frame(r#"{"type":"loggedIn"}"#, now());

    assert!(
        acknowledged.actions.contains(&PrivateAction::ArmSwitch),
        "the switch is armed only after the login: {:?}",
        acknowledged.actions,
    );

    let read_only = OndoPrivateSession::new(PrivateStreamMode::ReadOnly);

    assert!(
        !read_only
            .wanted()
            .contains(&PrivateChannel::CancelAllOrdersAfterPerps),
        "a read-only session subscribes to no channel with a cancelling side effect",
    );
}

// ------------------------------------------------------------------------------------------------
// The journal across a restart: the two crashes (plan §R3.2)
// ------------------------------------------------------------------------------------------------
//
// Two crash semantics, and neither is "the file can be read back":
//
// * a fill that was **reported** and not checkpointed - the process died between the report and the
//   next pass's write - is applied again by the restart, and it must be the *same fill* to
//   everything downstream. The identity that makes it the same is the venue's own fill id, which
//   the fill report carries as its `trade_id`; the engine skips a fill whose trade id is already
//   on the position (`crates/execution/src/engine/mod.rs`, "Duplicate leg fill"), so the replay is
//   one fill and not two;
// * a fill that **is** in the checkpoint is not applied again at all: the restored ledger dedupes
//   it, and no report is emitted.
//
// The two are the reason the journal is written *after* the events it records rather than before:
// the first crash is recoverable because the event carries a stable identity, and the second is
// prevented by the ledger. The opposite ordering would produce a journal claiming a fill no engine
// ever saw, and nothing downstream could tell that from one it had already applied.

/// The journal path one test owns, with its directory.
struct JournalPath {
    directory: std::path::PathBuf,
    path: std::path::PathBuf,
}

impl JournalPath {
    fn new(name: &str) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "ondo-journal-{name}-{}",
            nautilus_core::UUID4::new(),
        ));
        let path = directory.join("ledger.json");

        Self { directory, path }
    }

    fn config(&self) -> OndoExecutionClientConfig {
        OndoExecutionClientConfig {
            journal_path: Some(self.path.display().to_string()),
            ..sandbox_config()
        }
    }
}

impl Drop for JournalPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// The fills the harness reported, whichever envelope carried them.
fn fill_reports(events: &[ExecutionEvent]) -> Vec<nautilus_model::reports::FillReport> {
    events
        .iter()
        .flat_map(|event| match event {
            ExecutionEvent::Report(report) => match report {
                nautilus_common::messages::execution::ExecutionReport::Fill(fill) => {
                    vec![(**fill).clone()]
                }
                nautilus_common::messages::execution::ExecutionReport::OrderWithFills(
                    _report,
                    fills,
                ) => fills.clone(),
                _ => Vec::new(),
            },
            _ => Vec::new(),
        })
        .collect()
}

/// The journal one run left behind, or [`None`] when it wrote none.
fn journal_at(journal: &JournalPath) -> Option<nautilus_ondo::reconciliation::LedgerJournal> {
    if !journal.path.exists() {
        return None;
    }

    Some(
        nautilus_ondo::reconciliation::LedgerJournal::load(
            &journal.path,
            AccountId::from(ACCOUNT_ID),
        )
        .expect("the run's journal reads back"),
    )
}

/// A fill the venue reported and this run had not checkpointed is re-applied by the restart under
/// the **same** identity, so the engine counts one fill.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_fill_reported_before_the_checkpoint_crash_is_replayed_under_one_identity() {
    let journal = JournalPath::new("replay");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    // The venue's order list carries the order; its fill history does not carry the fill yet.
    rest.set_orders(&[api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open")]);
    rest.set_create_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open"));

    let mut first = build_harness(&rest, &private, journal.config());

    converge(&mut first).await;

    // The run's own order: a fill belongs to an order this client placed, and an order nobody
    // placed is identified rather than adopted (plan §6.4).
    let order = limit_order(CLIENT_ORDER_ID);

    seed_order(&first, &order);
    first
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_until(&mut first, "a tracked order", |client, _events| {
        client.tracks(&ClientOrderId::from(CLIENT_ORDER_ID))
    })
    .await;

    // The fill arrives on the private stream, is applied and reported - and the process dies
    // before the next checkpoint, so what the journal holds is the state before it.
    let fill = OndoApiFill::from_raw(
        &serde_json::value::RawValue::from_string(api_fill(
            "fill-before-the-crash",
            VENUE_ORDER_ID,
            CLIENT_ORDER_ID,
            "1.00",
        ))
        .expect("JSON"),
    )
    .expect("the fill payload reads");

    assert_eq!(
        first.client.account().ingest_stream_fill(fill.clone()),
        OndoStreamIngestion::Applied,
    );

    let reported = fill_reports(&drain_events(&mut first));
    let trade_id = reported
        .first()
        .expect("the fill leaves the process as a fill report")
        .trade_id;

    assert!(
        !journal_at(&journal)
            .expect("the run wrote a checkpoint while it was converging")
            .fills()
            .contains(&"fill-before-the-crash".to_string()),
        "the fill was reported and not checkpointed: that is the crash this test is about",
    );

    drop(first);

    // The restart. The venue's fill history now carries the same fill - the read that missed it
    // was the one that ran before it was published - and the order it belongs to has completed.
    rest.set_orders(&[api_order_filled(
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "fullyfilled",
        "1.00",
    )]);
    rest.set_fills(&[api_fill(
        "fill-before-the-crash",
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "1.00",
    )]);
    rest.set_positions(&[api_position("1.00")]);

    let mut second = build_harness(&rest, &private, journal.config());

    assert!(
        matches!(
            second.client.account().journal_status(),
            JournalStatus::Restored { .. }
        ),
        "the restart restores the journal it left: {:?}",
        second.client.account().journal_status(),
    );

    converge(&mut second).await;

    let replayed = fill_reports(&drain_events(&mut second));

    assert_eq!(replayed.len(), 1, "the fill is applied and reported once");
    assert_eq!(
        replayed[0].trade_id, trade_id,
        "and it is the *same* fill: the venue's own fill id travels as the trade id, which is what \
         the engine dedupes a replay against",
    );
    assert_eq!(replayed[0].venue_order_id.as_str(), VENUE_ORDER_ID);
    assert_eq!(replayed[0].last_qty, Quantity::from("1.00"));

    // And this run's checkpoint holds it, so a third run would not replay it again.
    assert!(
        journal_at(&journal)
            .expect("the second run checkpointed")
            .fills()
            .contains(&"fill-before-the-crash".to_string()),
    );

    second.client.stop().expect("stop");
}

/// A fill the checkpoint holds is not applied again: the restored ledger dedupes it and nothing is
/// reported.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_fill_recorded_in_the_checkpoint_is_not_replayed_after_a_restart() {
    let journal = JournalPath::new("checkpointed");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.set_orders(&[api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open")]);
    rest.set_create_answer(&api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open"));
    rest.set_positions(&[api_position("1.00")]);

    let mut first = build_harness(&rest, &private, journal.config());

    converge(&mut first).await;

    let order = limit_order(CLIENT_ORDER_ID);

    seed_order(&first, &order);
    first
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");
    wait_until(&mut first, "a tracked order", |client, _events| {
        client.tracks(&ClientOrderId::from(CLIENT_ORDER_ID))
    })
    .await;

    // The venue's history carries the fill from here, and the pass that reads it applies it and
    // writes a checkpoint that holds it.
    rest.set_fills(&[api_fill(
        "fill-in-the-checkpoint",
        VENUE_ORDER_ID,
        CLIENT_ORDER_ID,
        "1.00",
    )]);
    first
        .client
        .account()
        .reconcile_account(now())
        .await
        .expect("the pass reads the fill");

    let reported = fill_reports(&drain_events(&mut first));

    assert_eq!(
        reported.len(),
        1,
        "the first run applies and reports the fill"
    );
    assert!(
        journal_at(&journal)
            .expect("the run wrote a checkpoint")
            .fills()
            .contains(&"fill-in-the-checkpoint".to_string()),
        "and the checkpoint holds it",
    );

    // The watermark travels with the fills, because it is a fact about them. Captured here so the
    // restart below can be held to it rather than to a literal.
    let watermark_with_the_fill = journal_at(&journal)
        .expect("the run wrote a checkpoint")
        .watermark()
        .expect("a run that applied a fill has an instant to describe its coverage by");

    drop(first);

    // The restart reads the same history. The fill is in the restored ledger, so it is a duplicate
    // rather than a fill - and a duplicate emits nothing.
    let mut second = build_harness(&rest, &private, journal.config());

    converge(&mut second).await;

    assert!(
        fill_reports(&drain_events(&mut second)).is_empty(),
        "a fill the ledger already holds is not reported a second time",
    );
    assert_eq!(
        second.client.applied_fill_count(),
        1,
        "and it was applied once across both runs",
    );
    assert_eq!(
        second.client.reconciliation_state(),
        ReconciliationState::Ready,
        "the account converges: a duplicate is not a disagreement",
    );

    // The restart's own checkpoints are written from a state it restored rather than built. A run
    // that put the ledger back but not the instant it reaches would rewrite the file with no
    // coverage at all, so this is what makes the field's claim - that the watermark survives a
    // restart through the journal - a fact about the code instead of a sentence about it.
    assert_eq!(
        journal_at(&journal)
            .expect("the restart rewrote the checkpoint")
            .watermark(),
        Some(watermark_with_the_fill),
        "the coverage the restored ledger reaches is the coverage this run reports",
    );

    second.client.stop().expect("stop");
}

/// A journal that cannot be restored stops new risk, whatever the account reads.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_run_whose_journal_cannot_be_restored_refuses_new_risk() {
    let journal = JournalPath::new("damaged");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    std::fs::create_dir_all(&journal.directory).expect("the journal directory");

    // A truncated body: the checkpoint still records a fill the file no longer holds.
    let damaged = r#"{"schema_version":2,"account_id":"ONDO-SANDBOX-001","watermark_ns":null,"fills":[],"orders":[],"unsettled":[],"checkpoint":{"written_ns":1,"fills":1,"orders":0,"unsettled":0}}"#;

    std::fs::write(&journal.path, damaged).expect("the damaged journal");

    let mut harness = build_harness(&rest, &private, journal.config());

    assert!(
        matches!(
            harness.client.account().journal_status(),
            JournalStatus::Failed { .. }
        ),
        "a journal that disagrees with itself is not restored: {:?}",
        harness.client.account().journal_status(),
    );

    converge(&mut harness).await;

    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Ready,
        "the account itself reads clean",
    );

    let refusal = harness
        .client
        .new_risk_refusal()
        .expect("and new risk is still refused");

    assert!(
        matches!(refusal, NewRiskRefusal::JournalUnavailable { .. }),
        "by name, and about the journal rather than the account: {refusal:?}",
    );
    assert!(refusal.reason().contains("could not be restored"));

    // A submission never leaves the process.
    let order = limit_order("ondo_without_a_journal");

    seed_order(&harness, &order);
    harness
        .client
        .submit_order(submit_command(&order))
        .expect("the command is handled");

    assert_eq!(rest.writes(), 0, "nothing was sent");

    harness.client.stop().expect("stop");
}

/// With no journal path the run says so, and it still trades: the offline phase's mode is not a
/// failure, and it is not silence either.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_run_with_no_journal_path_says_so_and_keeps_reading_the_account() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;

    let status = harness.client.account().journal_status();

    assert!(
        matches!(status, JournalStatus::NotConfigured { .. }),
        "the run states that it has no journal: {status:?}",
    );
    assert_eq!(status.as_str(), "not_configured");
    assert!(status.reason().contains("in memory"));
    assert!(
        harness.client.can_submit_new_orders(),
        "and a run without a journal is not a run that cannot trade",
    );

    harness.client.stop().expect("stop");
}

/// A journal that stops accepting writes is reported as degraded rather than restored, and trading
/// does not stop over it: the disk stopped taking the checkpoint, the memory did not go anywhere.
///
/// The state the run started in is asserted first, because that is the one it must stop reporting.
/// Nothing here is a mock of a failure: the path's parent directory is replaced with a regular file,
/// which is what `store_atomic` cannot create a directory through.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_journal_that_stops_accepting_writes_is_reported_and_does_not_stop_trading() {
    let journal = JournalPath::new("degraded");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, journal.config());

    converge(&mut harness).await;

    assert_eq!(harness.client.account().journal_write_failures(), 0);
    assert!(journal.path.exists(), "a checkpoint reached the disk");
    assert_eq!(
        harness.client.account().journal_status().as_str(),
        "restored",
        "the run is durable before the disk stops accepting writes",
    );

    std::fs::remove_dir_all(&journal.directory).expect("the journal directory is removed");
    std::fs::write(&journal.directory, b"not a directory").expect("a file takes its place");

    // One more concluded pass, which is one more checkpoint write.
    let _ = harness.client.account().reconcile_account(now()).await;

    let failures = harness.client.account().journal_write_failures();
    let status = harness.client.account().journal_status();

    assert!(failures > 0, "the checkpoint could not be written");
    assert!(
        matches!(status, JournalStatus::Degraded { .. }),
        "a run whose checkpoint never reached the disk is not a run that is durable: {status:?}",
    );
    assert_eq!(status.as_str(), "degraded");
    assert!(
        !matches!(status, JournalStatus::Restored { .. }),
        "{status:?}",
    );
    assert!(
        status
            .reason()
            .contains(&journal.path.display().to_string()),
        "the report names the journal that stopped: {}",
        status.reason(),
    );

    // And it refuses nothing. The account is the one the venue reported, and the run still trades:
    // losing durability is not losing memory, so this is a report and not a refusal.
    assert_eq!(
        harness.client.reconciliation_state(),
        ReconciliationState::Ready,
    );
    assert_eq!(harness.client.new_risk_refusal(), None);
    assert!(harness.client.can_submit_new_orders());

    harness.client.stop().expect("stop");

    let _ = std::fs::remove_file(&journal.directory);
}

/// The account state the engine is told about is the one this adapter verified, and the position
/// report is the venue's own.
#[tokio::test(flavor = "multi_thread")]
async fn test_the_verified_account_and_its_positions_reach_the_engine() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;

    rest.set_orders(&[api_order(VENUE_ORDER_ID, CLIENT_ORDER_ID, "open")]);
    rest.set_positions(&[api_position("0.25")]);

    let mut harness = build_harness(&rest, &private, sandbox_config());

    converge(&mut harness).await;

    let events = drain_events(&mut harness);
    let states: Vec<&nautilus_model::events::AccountState> = events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Account(state) => Some(state),
            _ => None,
        })
        .collect();

    assert!(!states.is_empty(), "a concluded pass publishes the account");

    let state = states.last().expect("an account state");
    let settlement = Currency::from(ONDO_SETTLEMENT_CURRENCY);

    assert!(state.is_reported);
    assert_eq!(state.balances.len(), 1);
    assert_eq!(state.balances[0].currency, settlement);
    assert_eq!(
        state.balances[0].total,
        Money::from_decimal(
            rust_decimal::Decimal::from_str_exact("4950.00").expect("decimal"),
            settlement,
        )
        .expect("money"),
        "the venue's equity, as the venue stated it",
    );
    assert_eq!(state.margins.len(), 1);
    assert_eq!(
        state.margins[0].maintenance,
        Money::from_decimal(
            rust_decimal::Decimal::from_str_exact("112.50").expect("decimal"),
            settlement,
        )
        .expect("money"),
    );

    let reports = harness
        .client
        .generate_position_status_reports(
            &nautilus_common::messages::execution::GeneratePositionStatusReports::new(
                nautilus_core::UUID4::new(),
                UnixNanos::default(),
                None,
                None,
                None,
                None,
                None,
            ),
        )
        .await
        .expect("the positions read");

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].instrument_id, InstrumentId::from(NVDA));
    assert_eq!(reports[0].quantity, Quantity::from("0.25"));
    assert_eq!(reports[0].position_side, PositionSide::Long);
    assert_eq!(
        reports[0].avg_px_open,
        Some(rust_decimal::Decimal::from_str_exact("227.50").expect("decimal")),
    );

    harness.client.stop().expect("stop");
}

/// The account state travels the whole way: the adapter's emitter, the process's execution-event
/// channel, the runner's own dispatcher, and the portfolio's account-update endpoint into the cache.
///
/// The hop this test adds over [`test_the_verified_account_and_its_positions_reach_the_engine`] is
/// the one that is not this adapter's code: `Runner::handle_exec_event` forwards an
/// `ExecutionEvent::Account` to the endpoint the portfolio listens on, and the handler registered
/// there is what updates the cache. Driving the events through that dispatcher is what turns "the
/// adapter emitted an account state" into "the engine's cache holds the account".
#[tokio::test(flavor = "multi_thread")]
async fn test_the_account_state_travels_from_the_emitter_into_the_cache() {
    use nautilus_common::msgbus::{self, MessagingSwitchboard, TypedHandler};
    use nautilus_live::runner::AsyncRunner;
    use nautilus_model::events::AccountState;

    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, sandbox_config());

    // The portfolio's own handler, registered where the live node registers it.
    let cache = Rc::clone(&harness.cache);
    let handler = TypedHandler::from(move |state: &AccountState| {
        cache
            .borrow_mut()
            .update_account_state(state)
            .expect("the account state enters the cache");
    });

    msgbus::register_account_state_endpoint(
        MessagingSwitchboard::portfolio_update_account(),
        handler,
    );

    converge(&mut harness).await;

    let events = drain_events(&mut harness);

    assert!(!events.is_empty(), "the pass published the account");

    for event in events {
        AsyncRunner::handle_exec_event(event);
    }

    let account = harness
        .cache
        .borrow()
        .account_owned(&AccountId::from(ACCOUNT_ID))
        .expect("the account is in the cache");

    let nautilus_model::accounts::AccountAny::Margin(account) = account else {
        panic!("an Ondo account is a margin account");
    };

    let settlement = Currency::from(ONDO_SETTLEMENT_CURRENCY);
    let balance = account
        .balances
        .get(&settlement)
        .expect("the settlement currency is reported");

    assert_eq!(
        balance.total,
        Money::from_decimal(
            rust_decimal::Decimal::from_str_exact("4950.00").expect("decimal"),
            settlement,
        )
        .expect("money"),
        "the account the cache holds is the account the venue stated",
    );
    assert_eq!(
        balance.locked,
        Money::from_decimal(
            rust_decimal::Decimal::from_str_exact("0.00").expect("decimal"),
            settlement,
        )
        .expect("money"),
    );

    harness.client.stop().expect("stop");
}

/// The factory clone used by a node updates the original factory's run-local snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn test_readonly_factory_clones_share_live_telemetry_and_isolate_runs() {
    use nautilus_common::factories::ExecutionClientFactory;
    use nautilus_ondo::factories::OndoExecutionClientFactory;

    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let (exec_tx, _exec_rx) = mpsc::unbounded_channel::<ExecutionEvent>();
    let (data_tx, _data_rx) = mpsc::unbounded_channel::<DataEvent>();
    replace_exec_event_sender(exec_tx);
    replace_data_event_sender(data_tx);
    let factory = OndoExecutionClientFactory::with_budget(OndoRateBudget::with_quota(
        Quota::per_second(NonZeroU32::new(1_000).unwrap()).unwrap(),
    ));
    let node_factory = factory.clone();
    let independent = OndoExecutionClientFactory::new();
    let config = OndoExecutionClientConfig {
        environment: OndoEnvironment::Production,
        base_url_http: Some(rest.http_url()),
        base_url_ws: Some(private.url.clone()),
        account_read_only: true,
        diagnostics_run_id: Some("readonly-first-run".to_string()),
        reconcile_interval_secs: 1,
        ..sandbox_config()
    };
    let mut first = node_factory
        .create(
            TraderId::from("TESTER-001"),
            CLIENT_ID,
            &config,
            Rc::new(RefCell::new(Cache::default())).into(),
        )
        .expect("the cloned factory creates the first client");
    assert_eq!(
        factory.read_only_snapshot().unwrap().run_id,
        "readonly-first-run"
    );
    assert!(!factory.read_only_snapshot().unwrap().logged_in);
    first.start().unwrap();
    first.connect().await.unwrap();
    let start = Instant::now();
    loop {
        let snapshot = factory.read_only_snapshot().unwrap();
        if snapshot.logged_in
            && snapshot.subscriptions_acked.len() == 2
            && snapshot.account_state_events > 0
            && snapshot.run_state == "read_only_synced"
        {
            break;
        }
        assert!(
            start.elapsed() <= WAIT,
            "native telemetry did not converge: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let connected = factory.read_only_snapshot().unwrap();
    assert_eq!(
        connected.reconnects, 0,
        "the initial connection is not a reconnect"
    );
    assert!(connected.recoveries > 0);
    assert_eq!(connected.identity_match, "unknown");
    assert!(independent.read_only_snapshot().is_none());
    first.disconnect().await.unwrap();
    let stopped = factory.read_only_snapshot().unwrap();
    assert_eq!(stopped.shutdown_status, "complete");
    assert!(stopped.logged_in, "accepted login evidence survives stop");
    assert_eq!(
        stopped.subscriptions_acked,
        vec!["ordersPerps", "fillsPerps"]
    );
    assert!(switch_frames(&private).is_empty());
    assert!(rest.with_method("POST").is_empty());
    assert!(rest.with_method("DELETE").is_empty());

    let second_config = OndoExecutionClientConfig {
        diagnostics_run_id: Some("readonly-second-run".to_string()),
        ..config
    };
    let _second = node_factory
        .create(
            TraderId::from("TESTER-001"),
            CLIENT_ID,
            &second_config,
            Rc::new(RefCell::new(Cache::default())).into(),
        )
        .expect("the cloned factory creates the second client");
    drop(first);
    let second = factory.read_only_snapshot().unwrap();
    assert_eq!(second.run_id, "readonly-second-run");
    assert!(!second.logged_in);
    assert!(second.subscriptions_acked.is_empty());
    assert_eq!(second.account_state_events, 0);
    assert_eq!(second.recoveries, 0);
    assert_eq!(second.reconnects, 0);
    assert_eq!(second.shutdown_status, "not_attempted");
    assert_eq!(second.run_state, "disconnected");
    assert!(independent.read_only_snapshot().is_none());
}

#[rstest]
#[case(OndoEnvironment::Sandbox)]
#[case(OndoEnvironment::Production)]
#[tokio::test(flavor = "multi_thread")]
async fn test_readonly_direct_switch_arm_renew_release_never_write(
    #[case] environment: OndoEnvironment,
) {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let harness = build_harness(
        &rest,
        &private,
        OndoExecutionClientConfig {
            environment,
            account_read_only: true,
            ..sandbox_config()
        },
    );
    let credential = Arc::new(
        OndoCredential::new(
            environment,
            TEST_KEY_ID.to_string(),
            TEST_API_SECRET.to_string(),
        )
        .unwrap(),
    );
    let mut stream = nautilus_ondo::websocket::private::OndoPrivateStream::start(
        private.url.clone(),
        harness.client.account(),
        credential,
        PrivateStreamMode::ReadOnly,
        20,
    )
    .unwrap();
    tokio::time::timeout(WAIT, async {
        while private.frames_with_op("subscribe").len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut results = Vec::new();
    for frame in [
        nautilus_ondo::reconciliation::DeadMansSwitchMessage::new(WsOp::Subscribe, 30),
        nautilus_ondo::reconciliation::DeadMansSwitchMessage::new(WsOp::Subscribe, 15),
        nautilus_ondo::reconciliation::DeadMansSwitchMessage::new(WsOp::Unsubscribe, 30),
    ] {
        results.push(stream.send_switch_frame(&frame).await);
    }
    stream.stop().await;
    assert!(
        results.iter().all(|result| result
            .as_ref()
            .is_err_and(|e| e.to_string().contains("read-only"))),
        "every direct DMS operation must be a named readonly refusal: {results:?}"
    );
    assert!(
        switch_frames(&private).is_empty(),
        "readonly direct frames reached the wire"
    );
    assert_eq!(rest.writes(), 0);
}

#[rstest]
#[case(OndoEnvironment::Sandbox)]
#[case(OndoEnvironment::Production)]
#[tokio::test(flavor = "multi_thread")]
async fn test_readonly_unsolicited_switch_ack_cannot_start_renewal(
    #[case] environment: OndoEnvironment,
) {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(
        &rest,
        &private,
        OndoExecutionClientConfig {
            environment,
            account_read_only: true,
            dms_timeout_secs: 2,
            reconcile_interval_secs: 1,
            expected_venue_account_id: Some("unit-account".into()),
            ..sandbox_config()
        },
    );
    connect(&mut harness).await;
    private.push(r#"{"type":"subscribed","channel":"cancelAllOrdersAfterPerps"}"#);
    // This crosses the real one-second renewal interval without changing the process clock
    tokio::time::sleep(Duration::from_millis(1_250)).await;
    let switch_state = harness.client.account().dead_mans_switch_state();
    let renewals = harness.client.account().dead_mans_switch_renewals();
    harness.client.disconnect().await.unwrap();
    assert!(
        switch_frames(&private).is_empty(),
        "unsolicited ACK caused a readonly DMS write"
    );
    assert_eq!(switch_state, DeadMansSwitchState::NotRequired);
    assert_eq!(renewals, 0);
    assert_eq!(rest.writes(), 0);
}

#[rstest]
#[case(OndoEnvironment::Sandbox)]
#[case(OndoEnvironment::Production)]
#[tokio::test(flavor = "multi_thread")]
async fn test_readonly_runtime_cannot_be_armed_or_upgraded(#[case] environment: OndoEnvironment) {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let harness = build_harness(
        &rest,
        &private,
        OndoExecutionClientConfig {
            environment,
            account_read_only: true,
            ..sandbox_config()
        },
    );
    let account = harness.client.account();
    let _ = account.arm_dead_mans_switch(now());
    account.confirm_dead_mans_switch(now());
    account.dead_mans_switch_confirmed_at(now());
    account.note_dead_mans_switch_renewed(now());
    let attempted = nautilus_ondo::websocket::private::OndoPrivateStream::start(
        private.url.clone(),
        account.clone(),
        Arc::new(
            OndoCredential::new(
                environment,
                TEST_KEY_ID.to_string(),
                TEST_API_SECRET.to_string(),
            )
            .unwrap(),
        ),
        PrivateStreamMode::Trading,
        20,
    );
    assert_eq!(
        account.dead_mans_switch_state(),
        DeadMansSwitchState::NotRequired
    );
    assert!(account.dead_mans_switch_renewal_frame().is_none());
    assert_eq!(account.dead_mans_switch_renewals(), 0);
    assert!(attempted.is_err_and(|e| e.to_string().contains("read-only")));
    assert_eq!(private.connection_count(), 0);
    assert!(private.bodies().is_empty());
}

fn production_config(journal: &JournalPath) -> OndoExecutionClientConfig {
    let now = now().as_u64();
    let envelope = serde_json::from_value(serde_json::json!({
        "instrument_id":NVDA,"entry_side":"buy","entry_max_quantity":"0.05",
        "entry_worst_price":"230","entry_max_notional_usd":"15","close_side":"sell",
        "close_max_quantity":"0.05","close_worst_price":"225","max_close_attempts":2,
        "max_notional_per_order_usd":"50","max_gross_exposure_usd":"100","min_available_margin_usdc":"25","max_orders":3,
        "max_new_risk_requests":1,"max_app_requests":6,
        "entry_deadline_unix_nanos":now+60_000_000_000_u64,
        "cleanup_deadline_unix_nanos":now+120_000_000_000_u64,"require_flat_start":true
    }))
    .unwrap();
    OndoExecutionClientConfig {
        environment: OndoEnvironment::Production,
        allow_production_orders: true,
        expected_venue_account_id: Some("unit-account".into()),
        diagnostics_run_id: Some("bounded-test-run".into()),
        execution_envelope: Some(envelope),
        reconcile_interval_secs: 1,
        ..journal.config()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_start_publishes_real_complete_snapshot_after_dms_ack() {
    let journal = JournalPath::new("production-start");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    assert!(harness.client.production_trade_snapshot().is_none());
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    let snapshot = harness.client.production_trade_snapshot().unwrap();
    assert_eq!(snapshot["run_id"], "bounded-test-run");
    assert_eq!(snapshot["phase"], "start");
    for key in [
        "account_flat",
        "coverage_complete",
        "native_ready",
        "metadata_fresh",
        "trading_enabled",
        "dms_verified",
    ] {
        assert_eq!(snapshot[key], true, "{key}");
    }
    assert_eq!(snapshot["underlying_market_closed"], false);
    assert!(snapshot["minimum_notional_usd"].is_null());
    assert!(!switch_frames(&private).is_empty());
    assert_eq!(rest.writes(), 0);
    harness.client.disconnect().await.unwrap();
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test(flavor = "multi_thread")]
async fn test_production_occupied_account_never_connects_or_arms_dms(#[case] position: bool) {
    let journal = JournalPath::new("production-occupied");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    if position {
        rest.state
            .positions
            .lock()
            .unwrap()
            .push(api_position("0.5"));
    } else {
        rest.state
            .orders
            .lock()
            .unwrap()
            .push(api_order("foreign", "foreign", "open"));
    }
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    assert!(harness.client.connect().await.is_err());
    assert_eq!(private.connection_count(), 0);
    assert!(switch_frames(&private).is_empty());
    assert_eq!(rest.writes(), 0);
}

fn production_order(id: &str, close: bool, quantity: &str, price: &str) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(NVDA))
        .client_order_id(ClientOrderId::from(id))
        .side(if close {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        })
        .quantity(Quantity::from(quantity))
        .price(Price::from(price))
        .time_in_force(TimeInForce::Ioc)
        .reduce_only(close)
        .build()
}

fn production_quote(harness: &Harness, at: UnixNanos) {
    harness
        .cache
        .borrow_mut()
        .add_quote(nautilus_model::data::QuoteTick::new(
            InstrumentId::from(NVDA),
            Price::from("227.49"),
            Price::from("227.50"),
            Quantity::from("10.00"),
            Quantity::from("10.00"),
            at,
            at,
        ))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_native_ioc_round_trip_reconciles_before_stop_and_retains_final() {
    let journal = JournalPath::new("production-cycle");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    let start = harness.client.production_trade_snapshot().unwrap();
    production_quote(&harness, now());
    let entry = production_order("prod-entry", false, "0.05", "230.00");
    seed_order(&harness, &entry);
    harness.client.submit_order(submit_command(&entry)).unwrap();
    wait_until(&mut harness, "confirmed entry fill", |client, _| {
        client
            .order_state(&ClientOrderId::from("prod-entry"))
            .is_some_and(|o| o.is_settled() && o.filled == Quantity::from("0.05"))
    })
    .await;
    assert!(harness.client.production_trade_snapshot().is_none());
    production_quote(&harness, now());
    let close = production_order("prod-close", true, "0.05", "225.00");
    seed_order(&harness, &close);
    harness.client.submit_order(submit_command(&close)).unwrap();
    wait_until(&mut harness, "fresh REST flat proof", |client, _| {
        client
            .production_trade_snapshot()
            .is_some_and(|s| s["phase"] == "reconciled")
    })
    .await;
    let proof = harness.client.production_trade_snapshot().unwrap();
    assert!(proof["generation"].as_u64().unwrap() > start["generation"].as_u64().unwrap());
    assert_eq!(proof["position_qty"], "0");
    assert_eq!(
        proof["latest_activity_generation"],
        proof["reconciled_activity_generation"]
    );
    assert_eq!(rest.writes(), 2);
    harness.client.disconnect().await.unwrap();
    let final_proof = harness.client.production_trade_snapshot().unwrap();
    assert_eq!(final_proof["phase"], "final");
    assert_eq!(final_proof["shutdown_status"], "clean");
    assert_eq!(rest.writes(), 2);
}

#[rstest]
#[case("quantity", "0.06", "230.00", false)]
#[case("price", "0.05", "230.01", false)]
#[case("quantity_grid", "0.051", "230.00", false)]
#[case("price_grid", "0.05", "229.999", false)]
#[case("close_without_fill", "0.05", "225.00", true)]
#[case("stale_quote", "0.05", "230.00", false)]
#[case("missing_quote", "0.05", "230.00", false)]
#[case("stale_metadata", "0.05", "230.00", false)]
#[case("gtc", "0.05", "230.00", false)]
#[tokio::test(flavor = "multi_thread")]
async fn test_production_invalid_native_command_never_posts(
    #[case] case: &str,
    #[case] quantity: &str,
    #[case] price: &str,
    #[case] close: bool,
) {
    let journal = JournalPath::new(case);
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    if case != "missing_quote" {
        production_quote(
            &harness,
            if case == "stale_quote" {
                UnixNanos::from(now().as_u64() - 3_000_000_000)
            } else {
                now()
            },
        );
    }
    if case == "stale_metadata" {
        harness
            .client
            .account()
            .set_metadata(MetadataValidity::Stale {
                reason: "synthetic failed metadata read".into(),
            });
    }
    let order = if case == "gtc" {
        limit_order("bad-prod")
    } else {
        production_order("bad-prod", close, quantity, price)
    };
    seed_order(&harness, &order);
    harness.client.submit_order(submit_command(&order)).unwrap();
    wait_until(&mut harness, "a refused production command", |_, events| {
        events.iter().any(|e| {
            matches!(
                e,
                ExecutionEvent::Order(OrderEventAny::Denied(_) | OrderEventAny::Rejected(_))
            )
        })
    })
    .await;
    assert_eq!(rest.writes(), 0, "{case}");
    let _ = harness.client.disconnect().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_two_openings_reserve_only_one_native_send() {
    let journal = JournalPath::new("race-open");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_quote(&harness, now());
    for id in ["race-open-a", "race-open-b"] {
        let order = production_order(id, false, "0.05", "230.00");
        seed_order(&harness, &order);
        harness.client.submit_order(submit_command(&order)).unwrap();
    }
    wait_until(
        &mut harness,
        "one fill and one refusal",
        |client, events| {
            client.applied_fill_count() == 1
                && events
                    .iter()
                    .any(|e| matches!(e, ExecutionEvent::Order(OrderEventAny::Rejected(_))))
        },
    )
    .await;
    assert_eq!(rest.writes(), 1);
    let _ = harness.client.disconnect().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_foreign_and_cancel_all_targets_never_leave_http() {
    let journal = JournalPath::new("foreign-cancel");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    let http = harness.client.http_client();
    for path in [
        "/v1/perps/orders/foreign",
        "/v1/perps/orders?market=NVDA-USD.P",
        "/v1/perps/orders/batch",
    ] {
        assert!(
            http.delete_signed_raw(
                &nautilus_ondo::http::query::OndoRequestTarget::new(path),
                nautilus_ondo::http::rate_limit::OndoRequestPriority::High
            )
            .await
            .is_err()
        );
    }
    assert_eq!(rest.writes(), 0);
    harness.client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_unknown_post_retains_original_id_and_blocks_second_order() {
    let journal = JournalPath::new("unknown-post");
    let rest = MockRest::start().await;
    rest.state.hold_create.store(true, Ordering::SeqCst);
    let private = MockPrivate::start(Venue::Ack).await;
    let mut config = production_config(&journal);
    config.http_timeout_secs = 1;
    let mut harness = build_harness(&rest, &private, config);
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_quote(&harness, now());
    let order = production_order("unknown-original", false, "0.05", "230.00");
    seed_order(&harness, &order);
    harness.client.submit_order(submit_command(&order)).unwrap();
    wait_until(&mut harness, "unknown outcome", |client, _| {
        client.unknown_submissions().len() == 1
    })
    .await;
    production_quote(&harness, now());
    let second = production_order("forbidden-retry", false, "0.05", "230.00");
    seed_order(&harness, &second);
    harness
        .client
        .submit_order(submit_command(&second))
        .unwrap();
    assert_eq!(rest.writes(), 1);
    assert!(
        harness
            .client
            .tracks(&ClientOrderId::from("unknown-original"))
    );
    assert!(harness.client.production_trade_snapshot().is_none());
    rest.state.release_create.notify_waiters();
    let _ = harness.client.disconnect().await;
}

async fn production_entry(harness: &mut Harness) {
    production_quote(harness, now());
    let entry = production_order("prod-entry", false, "0.05", "230.00");
    seed_order(harness, &entry);
    harness.client.submit_order(submit_command(&entry)).unwrap();
    wait_until(harness, "settled own entry", |client, _| {
        client
            .order_state(&ClientOrderId::from("prod-entry"))
            .is_some_and(|o| o.is_settled() && o.filled == Quantity::from("0.05"))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_terminal_partial_close_releases_only_unfilled_reservation() {
    let journal = JournalPath::new("partial-close");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    *rest.state.production_close_fill_limit.lock().unwrap() = Some("0.03".into());
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_entry(&mut harness).await;
    production_quote(&harness, now());
    let first = production_order("close-first", true, "0.05", "225.00");
    seed_order(&harness, &first);
    harness.client.submit_order(submit_command(&first)).unwrap();
    wait_until(&mut harness, "terminal partial close", |client, _| {
        client
            .order_state(&ClientOrderId::from("close-first"))
            .is_some_and(|o| o.is_settled() && o.filled == Quantity::from("0.03"))
    })
    .await;
    production_quote(&harness, now());
    let second = production_order("close-residual", true, "0.02", "225.00");
    seed_order(&harness, &second);
    harness
        .client
        .submit_order(submit_command(&second))
        .unwrap();
    wait_until(&mut harness, "reconciled remaining close", |client, _| {
        client
            .production_trade_snapshot()
            .is_some_and(|s| s["phase"] == "reconciled")
    })
    .await;
    assert_eq!(rest.writes(), 3);
    harness.client.disconnect().await.unwrap();
    assert_eq!(
        harness.client.production_trade_snapshot().unwrap()["phase"],
        "final"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_racing_closes_never_exceed_confirmed_entry() {
    let journal = JournalPath::new("race-close");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_entry(&mut harness).await;
    production_quote(&harness, now());
    for id in ["close-race-a", "close-race-b"] {
        let close = production_order(id, true, "0.05", "225.00");
        seed_order(&harness, &close);
        harness.client.submit_order(submit_command(&close)).unwrap();
    }
    wait_until(&mut harness, "race close flat proof", |client, _| {
        client
            .production_trade_snapshot()
            .is_some_and(|s| s["phase"] == "reconciled")
    })
    .await;
    assert_eq!(rest.writes(), 2);
    harness.client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_close_still_works_after_entry_deadline() {
    let journal = JournalPath::new("close-after-entry-deadline");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    let private = MockPrivate::start(Venue::Ack).await;
    let mut config = production_config(&journal);
    let deadline = now().as_u64() + 3_000_000_000;
    config
        .execution_envelope
        .as_mut()
        .unwrap()
        .entry_deadline_unix_nanos = deadline;
    let mut harness = build_harness(&rest, &private, config);
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_entry(&mut harness).await;
    tokio::time::sleep(Duration::from_nanos(
        deadline.saturating_sub(now().as_u64()) + 20_000_000,
    ))
    .await;
    production_quote(&harness, now());
    let close = production_order("cleanup-close", true, "0.05", "225.00");
    seed_order(&harness, &close);
    harness.client.submit_order(submit_command(&close)).unwrap();
    wait_until(&mut harness, "bounded cleanup proof", |client, _| {
        client
            .production_trade_snapshot()
            .is_some_and(|s| s["phase"] == "reconciled")
    })
    .await;
    assert_eq!(rest.writes(), 2);
    harness.client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_reconnect_rearms_only_owned_residual_then_closes() {
    let journal = JournalPath::new("owned-reconnect");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_entry(&mut harness).await;
    private.disconnect();
    wait_until(&mut harness, "reconnected own residual", |client, _| {
        private.connection_count() >= 2
            && client.private_run_state().state == PrivateRunState::TradingReady
    })
    .await;
    assert!(switch_frames(&private).len() >= 2);
    production_quote(&harness, now());
    let close = production_order("reconnect-close", true, "0.05", "225.00");
    seed_order(&harness, &close);
    harness.client.submit_order(submit_command(&close)).unwrap();
    wait_until(&mut harness, "post reconnect flat proof", |client, _| {
        client
            .production_trade_snapshot()
            .is_some_and(|s| s["phase"] == "reconciled")
    })
    .await;
    assert_eq!(rest.writes(), 2);
    harness.client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_journal_write_failure_blocks_before_post() {
    let journal = JournalPath::new("production-journal-fail");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    std::fs::remove_file(&journal.path).unwrap();
    std::fs::create_dir(&journal.path).unwrap();
    production_quote(&harness, now());
    let order = production_order("journal-refused", false, "0.05", "230.00");
    seed_order(&harness, &order);
    harness.client.submit_order(submit_command(&order)).unwrap();
    wait_until(&mut harness, "journal refusal", |_, events| {
        events
            .iter()
            .any(|e| matches!(e, ExecutionEvent::Order(OrderEventAny::Rejected(_))))
    })
    .await;
    assert_eq!(rest.writes(), 0);
    assert!(harness.client.account().journal_write_failures() > 0);
    let _ = harness.client.disconnect().await;
}

#[rstest]
#[case("deadline")]
#[case("disconnect")]
#[case("quote")]
#[tokio::test(flavor = "multi_thread")]
async fn test_production_rechecks_after_budget_wait(#[case] condition: &str) {
    let journal = JournalPath::new(condition);
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let budget = OndoRateBudget::with_quota(
        Quota::with_period(Duration::from_millis(if condition == "quote" {
            4_000
        } else {
            2_100
        }))
        .unwrap()
        .allow_burst(NonZeroU32::new(64).unwrap()),
    );
    let mut config = production_config(&journal);
    let deadline = now().as_u64() + 2_000_000_000;
    if condition == "deadline" {
        config
            .execution_envelope
            .as_mut()
            .unwrap()
            .entry_deadline_unix_nanos = deadline;
    }
    let mut harness = build_harness_on_budget(&rest, &private, config, budget.clone());
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    while budget
        .limiter()
        .check_key(&ustr::Ustr::from(
            nautilus_ondo::http::rate_limit::ONDO_REST_BUCKET,
        ))
        .is_ok()
    {}
    production_quote(
        &harness,
        if condition == "quote" {
            UnixNanos::from(now().as_u64() - 500_000_000)
        } else {
            now()
        },
    );
    let order = production_order("after-queue", false, "0.05", "230.00");
    seed_order(&harness, &order);
    harness.client.submit_order(submit_command(&order)).unwrap();
    wait_until(&mut harness, "submitted before budget wait", |_, events| {
        events
            .iter()
            .any(|e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))))
    })
    .await;
    assert_eq!(rest.writes(), 0);
    if condition == "disconnect" {
        harness.client.account().note_session_ended(now());
    }
    wait_until(&mut harness, "refused after budget wait", |_, events| {
        events
            .iter()
            .any(|e| matches!(e, ExecutionEvent::Order(OrderEventAny::Rejected(_))))
    })
    .await;
    assert_eq!(rest.writes(), 0);
    if condition == "deadline" {
        assert!(now().as_u64() >= deadline);
    }
    let _ = harness.client.disconnect().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_missing_host_dms_ack_cannot_be_forged_by_runtime_hook() {
    let journal = JournalPath::new("dms-noack");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::NoDmsAck).await;
    let mut config = production_config(&journal);
    config
        .execution_envelope
        .as_mut()
        .unwrap()
        .entry_deadline_unix_nanos = now().as_u64() + 4_000_000_000;
    let mut harness = build_harness(&rest, &private, config);
    harness.client.start().unwrap();
    let account = harness.client.account();
    let (result, ()) = tokio::join!(harness.client.connect(), async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        account.confirm_dead_mans_switch(now());
        account.dead_mans_switch_confirmed_at(now());
    });
    assert!(result.is_err());
    assert!(harness.client.production_trade_snapshot().is_none());
    assert_eq!(rest.writes(), 0);
    assert_eq!(
        switch_frames(&private).len(),
        1,
        "only the unconfirmed initial arm was sent"
    );
    let _ = harness.client.disconnect().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_rest_residual_contradiction_never_publishes_flat_proof() {
    let journal = JournalPath::new("rest-residual");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    *rest.state.production_residual_override.lock().unwrap() = Some("0.01".into());
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_entry(&mut harness).await;
    production_quote(&harness, now());
    let close = production_order("contradicted-close", true, "0.05", "225.00");
    seed_order(&harness, &close);
    harness.client.submit_order(submit_command(&close)).unwrap();
    wait_until(&mut harness, "REST position contradiction", |client, _| {
        client.applied_fill_count() == 2 && client.last_judgment().is_some_and(|j| !j.is_clean())
    })
    .await;
    assert!(harness.client.production_trade_snapshot().is_none());
    let _ = harness.client.disconnect().await;
    assert!(harness.client.production_trade_snapshot().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_reconnect_foreign_position_refuses_any_new_dms_arm() {
    let journal = JournalPath::new("foreign-reconnect");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_entry(&mut harness).await;
    rest.state
        .positions
        .lock()
        .unwrap()
        .push(api_position("0.01").replace(NVDA_MARKET, "TSLA-USD.P"));
    private.disconnect();
    wait_until(&mut harness, "foreign reconnect finding", |client, _| {
        private.connection_count() >= 2 && client.last_judgment().is_some_and(|j| !j.is_clean())
    })
    .await;
    assert_eq!(switch_frames(&private).len(), 1);
    assert!(harness.client.production_trade_snapshot().is_none());
    let _ = harness.client.disconnect().await;
}

#[rstest]
#[case("USD", "12", false)]
#[case("EUR", "1", false)]
#[case("USDC", "1", false)]
#[case("USD", "10", true)]
#[tokio::test(flavor = "multi_thread")]
async fn test_production_real_money_minimum_is_enforced(
    #[case] currency: &str,
    #[case] amount: &str,
    #[case] admitted: bool,
) {
    let journal = JournalPath::new("money-minimum");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    let mut instrument = nautilus_ondo::http::models::parse_instruments(
        MARKETS_BODY,
        &[InstrumentId::from(NVDA)],
        now(),
    )
    .unwrap()
    .remove(0);
    let nautilus_model::instruments::InstrumentAny::CryptoPerpetual(ref mut perpetual) = instrument
    else {
        panic!("fixture instrument");
    };
    perpetual.min_notional = Some(
        Money::from_decimal(
            rust_decimal::Decimal::from_str_exact(amount).unwrap(),
            Currency::from(currency),
        )
        .unwrap(),
    );
    harness
        .cache
        .borrow_mut()
        .add_instrument(instrument)
        .unwrap();
    production_quote(&harness, now());
    let order = production_order("minimum-entry", false, "0.05", "230.00");
    seed_order(&harness, &order);
    harness.client.submit_order(submit_command(&order)).unwrap();
    if admitted {
        wait_until(&mut harness, "minimum admitted", |client, _| {
            client.applied_fill_count() == 1
        })
        .await;
        assert_eq!(rest.writes(), 1);
    } else {
        wait_until(&mut harness, "minimum refused", |_, events| {
            events.iter().any(|e| {
                matches!(
                    e,
                    ExecutionEvent::Order(OrderEventAny::Denied(_) | OrderEventAny::Rejected(_))
                )
            })
        })
        .await;
        assert_eq!(rest.writes(), 0);
    }
    let _ = harness.client.disconnect().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_short_dms_successful_renewal_send_without_ack_stays_expired() {
    let journal = JournalPath::new("short-dms");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::InitialDmsAckOnly).await;
    let mut config = production_config(&journal);
    config.dms_timeout_secs = 4;
    let mut harness = build_harness(&rest, &private, config);
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(3_200)).await;
    assert_eq!(
        harness.client.account().dead_mans_switch_state(),
        DeadMansSwitchState::Armed,
        "legacy socket-send timer still appears armed"
    );
    production_quote(&harness, now());
    let order = production_order("expired-confirmed-dms", false, "0.05", "230.00");
    seed_order(&harness, &order);
    harness.client.submit_order(submit_command(&order)).unwrap();
    wait_until(
        &mut harness,
        "expired confirmed DMS refusal",
        |_, events| {
            events
                .iter()
                .any(|e| matches!(e, ExecutionEvent::Order(OrderEventAny::Rejected(_))))
        },
    )
    .await;
    assert_eq!(rest.writes(), 0);
    assert_eq!(
        switch_frames(&private).len(),
        2,
        "unconfirmed renewal cannot be replaced by newer sends"
    );
    let _ = harness.client.disconnect().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_factory_binds_run_snapshot_and_rejects_journal_reuse() {
    use nautilus_common::factories::ExecutionClientFactory;
    use nautilus_ondo::factories::OndoExecutionClientFactory;
    let journal = JournalPath::new("production-factory");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let config = OndoExecutionClientConfig {
        base_url_http: Some(rest.http_url()),
        base_url_ws: Some(private.url.clone()),
        ..production_config(&journal)
    };
    let (exec_tx, _rx) = mpsc::unbounded_channel::<ExecutionEvent>();
    replace_exec_event_sender(exec_tx);
    let factory = OndoExecutionClientFactory::with_budget(OndoRateBudget::with_quota(
        Quota::per_second(NonZeroU32::new(1_000).unwrap()).unwrap(),
    ));
    let node_factory = factory.clone();
    let mut client = node_factory
        .create(
            TraderId::from("TESTER-001"),
            CLIENT_ID,
            &config,
            Rc::new(RefCell::new(Cache::default())).into(),
        )
        .unwrap();
    assert!(factory.production_trade_snapshot().is_none());
    client.start().unwrap();
    client.connect().await.unwrap();
    let mut detached = factory.production_trade_snapshot().unwrap();
    detached["run_id"] = serde_json::json!("tampered");
    assert_eq!(
        factory.production_trade_snapshot().unwrap()["run_id"],
        "bounded-test-run"
    );
    assert!(
        OndoExecutionClientFactory::new()
            .production_trade_snapshot()
            .is_none()
    );
    client.disconnect().await.unwrap();
    assert!(
        factory
            .create(
                TraderId::from("TESTER-001"),
                CLIENT_ID,
                &config,
                Rc::new(RefCell::new(Cache::default())).into()
            )
            .is_err()
    );
}

#[rstest]
#[case(Venue::NoLoginAck, false, "private_login_not_acknowledged")]
#[case(Venue::RefuseLogin, false, "private_login_not_acknowledged")]
#[case(Venue::NoReportAck, false, "private_subscriptions_not_acknowledged")]
#[case(
    Venue::RefuseSubscribe,
    false,
    "private_subscriptions_not_acknowledged"
)]
#[case(Venue::Ack, true, "private_account_reconciliation_incomplete")]
#[tokio::test(flavor = "multi_thread")]
async fn test_production_readonly_connect_requires_verified_private_readiness(
    #[case] venue: Venue,
    #[case] bad_account: bool,
    #[case] category: &str,
) {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(venue).await;
    if bad_account {
        rest.state.positions.lock().unwrap().push("{}".into());
    }
    let config = OndoExecutionClientConfig {
        environment: OndoEnvironment::Production,
        account_read_only: true,
        expected_venue_account_id: Some("unit-account".into()),
        http_timeout_secs: 2,
        reconcile_interval_secs: 1,
        ..sandbox_config()
    };
    let mut harness = build_harness(&rest, &private, config);
    harness.client.start().unwrap();
    let error = harness.client.connect().await.unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("ondo_readonly_readiness:{category}")
    );
    assert!(!harness.client.is_connected());
    assert!(!harness.client.private_stream_is_running());
    assert_eq!(
        harness
            .client
            .read_only_diagnostics()
            .snapshot()
            .shutdown_status,
        "complete"
    );
    harness.client.stop().unwrap();
    assert_eq!(
        harness
            .client
            .read_only_diagnostics()
            .snapshot()
            .shutdown_status,
        "complete"
    );
    assert!(switch_frames(&private).is_empty());
    assert_eq!(rest.writes(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_readonly_connect_returns_only_after_private_ack_and_account_state() {
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let config = OndoExecutionClientConfig {
        environment: OndoEnvironment::Production,
        account_read_only: true,
        expected_venue_account_id: Some("unit-account".into()),
        http_timeout_secs: 2,
        reconcile_interval_secs: 30,
        ..sandbox_config()
    };
    let mut harness = build_harness(&rest, &private, config);
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    let snapshot = harness.client.read_only_diagnostics().snapshot();
    assert!(snapshot.logged_in);
    assert_eq!(snapshot.subscriptions_acked.len(), 2);
    assert!(snapshot.recoveries > 0);
    assert!(snapshot.account_state_events > 0);
    assert!(harness.client.is_connected());
    harness.client.disconnect().await.unwrap();
    harness.client.stop().unwrap();
    assert_eq!(
        harness
            .client
            .read_only_diagnostics()
            .snapshot()
            .shutdown_status,
        "complete"
    );
    assert!(switch_frames(&private).is_empty());
    assert_eq!(rest.writes(), 0);
}

#[rstest]
#[case(r#"[{"market":"NVDA-USD.P","disabled":true,"isClosed":false}]"#)]
#[case(r#"[{"market":"NVDA-USD.P","isClosed":false}]"#)]
#[case(r#"[{"market":"NVDA-USD.P","disabled":"false","isClosed":false}]"#)]
#[case(r#"[{"market":"NVDA-USD.P","disabled":false,"isClosed":false},{"market":"NVDA-USD.P","disabled":true,"isClosed":false}]"#)]
#[case(r#"[{"market":"TSLA-USD.P","disabled":false,"isClosed":false}]"#)]
#[tokio::test(flavor = "multi_thread")]
async fn test_production_contract_metadata_conflicts_refuse_before_private_transport(
    #[case] contracts: &str,
) {
    let journal = JournalPath::new("contract-conflict");
    let rest = MockRest::start().await;
    *rest.state.contracts.lock().unwrap() = contracts.into();
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    assert!(harness.client.connect().await.is_err());
    assert_eq!(private.connection_count(), 0);
    assert_eq!(rest.writes(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_definitive_zero_close_releases_capacity_for_second_close() {
    let journal = JournalPath::new("known-zero-close");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_entry(&mut harness).await;
    rest.state.reject_next_close.store(true, Ordering::SeqCst);
    production_quote(&harness, now());
    let first = production_order("definite-zero-close", true, "0.05", "225.00");
    seed_order(&harness, &first);
    harness.client.submit_order(submit_command(&first)).unwrap();
    wait_until(&mut harness, "definite close rejection", |_, events| {
        events
            .iter()
            .any(|e| matches!(e, ExecutionEvent::Order(OrderEventAny::Rejected(_))))
    })
    .await;
    production_quote(&harness, now());
    let second = production_order("second-close", true, "0.05", "225.00");
    seed_order(&harness, &second);
    harness
        .client
        .submit_order(submit_command(&second))
        .unwrap();
    wait_until(&mut harness, "second close flat proof", |client, _| {
        client
            .production_trade_snapshot()
            .is_some_and(|s| s["phase"] == "reconciled")
    })
    .await;
    assert_eq!(rest.writes(), 3);
    harness.client.disconnect().await.unwrap();
    assert_eq!(
        harness.client.production_trade_snapshot().unwrap()["phase"],
        "final"
    );
}

#[rstest]
#[case(Venue::NoReleaseAck, false)]
#[case(Venue::WrongReleaseAck, false)]
#[case(Venue::DelayedReleaseAck, true)]
#[case(Venue::AmbiguousReleaseUpdate, false)]
#[case(Venue::MissingReleaseUpdateData, false)]
#[tokio::test(flavor = "multi_thread")]
async fn test_production_final_clean_requires_matching_release_ack(
    #[case] venue: Venue,
    #[case] clean: bool,
) {
    let journal = JournalPath::new("release-ack");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    let private = MockPrivate::start(venue).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    production_entry(&mut harness).await;
    production_quote(&harness, now());
    let close = production_order("ack-close", true, "0.05", "225.00");
    seed_order(&harness, &close);
    harness.client.submit_order(submit_command(&close)).unwrap();
    wait_until(&mut harness, "pre-stop proof", |client, _| {
        client
            .production_trade_snapshot()
            .is_some_and(|s| s["phase"] == "reconciled")
    })
    .await;
    let result = harness.client.disconnect().await;
    assert_eq!(result.is_ok(), clean);
    let proof = harness.client.production_trade_snapshot().unwrap();
    assert_eq!(proof["phase"] == "final", clean);
    assert_eq!(proof["shutdown_status"] == "clean", clean);
    assert_eq!(proof["dms_release"]["attempted"], true);
    assert_eq!(proof["dms_release"]["frame_sent"], true);
    assert_eq!(proof["dms_release"]["acknowledged"], clean);
    assert!(!proof.to_string().contains("SYNTHETIC_PRIVATE"));
    if matches!(
        venue,
        Venue::AmbiguousReleaseUpdate | Venue::MissingReleaseUpdateData
    ) {
        assert_eq!(proof["dms_release"]["updates_after_release"], 1);
        assert_eq!(
            proof["dms_release"]["last_update_data_kind"],
            if venue == Venue::AmbiguousReleaseUpdate {
                "object"
            } else {
                "missing"
            }
        );
    }
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test(flavor = "multi_thread")]
async fn test_production_release_never_rearms_across_renewal_or_connection_loss(
    #[case] lose_connection: bool,
) {
    let journal = JournalPath::new("release-never-rearms");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::NoReleaseAck).await;
    let mut config = production_config(&journal);
    config.dms_timeout_secs = 2;
    let mut harness = build_harness(&rest, &private, config);
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();

    let respond = async {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !switch_frames(&private)
                .iter()
                .any(|body| is_a_switch_release(body))
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the release request reached the loopback venue");
        if lose_connection {
            private.disconnect();
        }
        // Cross the one-second renewal tick while release is awaiting its acknowledgement.
        tokio::time::sleep(Duration::from_millis(1_250)).await;
        if !lose_connection {
            private.push(r#"{"type":"unsubscribed","channel":"cancelAllOrdersAfterPerps"}"#);
        }
    };
    let (result, ()) = tokio::join!(harness.client.disconnect(), respond);
    assert_eq!(result.is_ok(), !lose_connection);
    assert_eq!(
        private.connection_count(),
        1,
        "release must not reconnect and rearm"
    );
    let frames = switch_frames(&private);
    let release = frames
        .iter()
        .position(|body| is_a_switch_release(body))
        .unwrap();
    assert_eq!(
        release + 1,
        frames.len(),
        "release must be the final DMS request"
    );
    let proof = harness.client.production_trade_snapshot().unwrap();
    assert_eq!(proof["dms_release"]["acknowledged"], !lose_connection);
    assert_eq!(proof["shutdown_status"] == "clean", !lose_connection);
    if lose_connection {
        assert_eq!(proof["dms_release"]["outcome"], "no_connection");
    }
    assert_eq!(rest.writes(), 0);
}

/// Production shutdown must leave enough of the approved absolute cleanup window for the same
/// multi-read reconciliation that startup requires. Five seconds is shorter than the observed
/// signed-read sequence and used to prevent the release frame from becoming eligible at all.
#[tokio::test(flavor = "multi_thread")]
async fn test_production_shutdown_waits_for_a_slow_final_reconciliation_before_release() {
    let journal = JournalPath::new("slow-final-reconciliation");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();

    rest.hold_orders();
    let release = async {
        tokio::time::sleep(Duration::from_secs(6)).await;
        rest.release_orders();
    };

    let (result, ()) = tokio::join!(harness.client.disconnect(), release);
    result.expect("the production stop budget covers final reconciliation and DMS release");
    let proof = harness.client.production_trade_snapshot().unwrap();
    assert_eq!(proof["phase"], "final");
    assert_eq!(proof["shutdown_status"], "clean");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_cleanup_deadline_ends_private_task_without_app_stop() {
    let journal = JournalPath::new("automatic-deadline");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut config = production_config(&journal);
    let time = now().as_u64();
    let envelope = config.execution_envelope.as_mut().unwrap();
    envelope.entry_deadline_unix_nanos = time + 3_000_000_000;
    envelope.cleanup_deadline_unix_nanos = time + 4_000_000_000;
    let mut harness = build_harness(&rest, &private, config);
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    wait_until(&mut harness, "native deadline task exit", |client, _| {
        !client.private_stream_is_running()
    })
    .await;
    assert!(!harness.client.is_connected());
    let frames = private.bodies().len();
    let writes = rest.writes();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(private.bodies().len(), frames);
    assert_eq!(rest.writes(), writes);
    assert!(
        harness
            .client
            .production_trade_snapshot()
            .is_none_or(|s| s["phase"] != "final")
    );
    let _ = harness.client.disconnect().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_production_usdc_margin_threshold_is_independent_of_usd_order_notional() {
    let journal = JournalPath::new("usdc-threshold");
    let rest = MockRest::start().await;
    let private = MockPrivate::start(Venue::Ack).await;
    let mut config = production_config(&journal);
    config
        .execution_envelope
        .as_mut()
        .unwrap()
        .min_available_margin_usdc = rust_decimal::Decimal::from(5000);
    let mut harness = build_harness(&rest, &private, config);
    harness.client.start().unwrap();
    assert!(harness.client.connect().await.is_err());
    assert_eq!(private.connection_count(), 0);
    assert_eq!(rest.writes(), 0);
}

#[rstest]
#[case("24.99", false)]
#[case("25.00", true)]
#[case("25.01", true)]
#[tokio::test(flavor = "multi_thread")]
async fn test_production_usdc_send_margin_below_equal_above_threshold(
    #[case] amount: &str,
    #[case] admitted: bool,
) {
    let journal = JournalPath::new("usdc-send-boundary");
    let rest = MockRest::start().await;
    rest.set_create_answer("<production-ioc>");
    *rest.state.available_margin_override.lock().unwrap() = Some("25.00".into());
    let private = MockPrivate::start(Venue::Ack).await;
    let mut harness = build_harness(&rest, &private, production_config(&journal));
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    assert_eq!(
        harness.client.production_trade_snapshot().unwrap()["available_margin_usdc"],
        "25.00"
    );
    *rest.state.available_margin_override.lock().unwrap() = Some(amount.into());
    let desired = rust_decimal::Decimal::from_str_exact(amount).unwrap();
    let started = Instant::now();
    loop {
        let _ = harness.client.account().reconcile_account(now()).await;
        if harness
            .client
            .last_reading()
            .and_then(|r| r.balance)
            .and_then(|b| b.available_margin)
            == Some(desired)
        {
            break;
        }
        assert!(started.elapsed() < WAIT);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    production_quote(&harness, now());
    let order = production_order("usdc-send", false, "0.05", "230.00");
    seed_order(&harness, &order);
    harness.client.submit_order(submit_command(&order)).unwrap();
    if admitted {
        wait_until(&mut harness, "same-unit margin admitted", |client, _| {
            client.applied_fill_count() == 1
        })
        .await;
        assert_eq!(rest.writes(), 1);
    } else {
        wait_until(&mut harness, "same-unit margin refused", |_, events| {
            events.iter().any(|e| {
                matches!(
                    e,
                    ExecutionEvent::Order(OrderEventAny::Rejected(_) | OrderEventAny::Denied(_))
                )
            })
        })
        .await;
        assert_eq!(rest.writes(), 0);
    }
    let _ = harness.client.disconnect().await;
}
