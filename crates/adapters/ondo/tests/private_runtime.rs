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
    messages::{DataEvent, ExecutionEvent, execution::SubmitOrder},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType, OrderSide, OrderType, PositionSide, TimeInForce},
    events::OrderEventAny,
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId},
    orders::{Order, OrderAny, builder::OrderTestBuilder},
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
        ReconciliationState, StopOutcome, StopStep, UncertainKind,
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
}

impl RestState {
    fn new() -> Self {
        Self {
            orders: Mutex::new(Vec::new()),
            fills: Mutex::new(Vec::new()),
            positions: Mutex::new(Vec::new()),
            funding: Mutex::new(Vec::new()),
            create_answer: Mutex::new(None),
            cancel_answer: Mutex::new(None),
            order_answer: Mutex::new(None),
            hold_orders: AtomicBool::new(false),
            held: Notify::new(),
            release: Notify::new(),
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
        ("GET", "/v1/perps/fills") => {
            let fills = state.fills.lock().expect("the fill script").clone();

            write_response(stream, 200, &page(&fills)).await;
        }
        ("GET", "/v1/perps/positions") => {
            let positions = state.positions.lock().expect("the position script").clone();

            write_response(stream, 200, &page(&positions)).await;
        }
        ("GET", "/v1/perps/balance") => {
            write_response(stream, 200, &envelope(&balance_json())).await;
        }
        ("GET", "/v1/perps/funding_fees") => {
            let funding = state.funding.lock().expect("the funding script").clone();

            write_response(stream, 200, &page(&funding)).await;
        }
        ("POST", "/v1/perps/orders") => {
            let answer = state
                .create_answer
                .lock()
                .expect("the create answer")
                .clone();

            match answer {
                Some(answer) => write_response(stream, 200, &envelope(&answer)).await,
                None => write_response(stream, 404, r#"{"success":false}"#).await,
            }
        }
        // One order, by venue order id or by the `client:{id}` form, for a cancel and for the
        // confirming query after one. The market-wide `DELETE /v1/perps/orders` is deliberately not
        // answered here: a stop that sent one would be a defect, and this mock is what proves it
        // never does.
        ("DELETE", path) if path.starts_with("/v1/perps/orders/") => {
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

    Some(Captured { method, target })
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

                                    for reply in responses(venue, &text) {
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
        (_, Some("login")) => vec![r#"{"type":"loggedIn","msg":"Login successful"}"#.to_string()],
        (Venue::RefuseSubscribe, Some("subscribe")) => vec![format!(
            r#"{{"type":"error","channel":"{channel}","msg":"subscription refused"}}"#,
        )],
        (_, Some("subscribe")) => vec![format!(r#"{{"type":"subscribed","channel":"{channel}"}}"#)],
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
        OndoEnvironment::Sandbox,
        TEST_KEY_ID.to_string(),
        TEST_API_SECRET.to_string(),
    )
    .expect("the plan's fake credential is well formed");

    let budget = OndoRateBudget::with_quota(
        Quota::per_second(NonZeroU32::new(1_000).expect("a nonzero quota"))
            .expect("a burst this size replenishes"),
    );

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
        client.private_run_state().state == PrivateRunState::Recovering
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
    wait_until(&mut harness, "a tracked order", |client, _events| {
        client.tracks(&ClientOrderId::from(CLIENT_ORDER_ID))
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
