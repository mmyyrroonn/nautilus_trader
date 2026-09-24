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

//! A scriptable in-process stand-in for the Aster Futures V3 venue.
//!
//! The execution client is exercised against a real HTTP and WebSocket server rather than a
//! stubbed transport, so signing, query construction, pagination cursors and the user data
//! stream lifecycle are all covered by the same tests. Every response is scripted through
//! [`VenueScript`], and every request is recorded so tests can assert on what the client
//! actually sent (and, for the denial paths, on what it did *not* send).

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use nautilus_network::http::HttpClient;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::sync::broadcast;

/// Test-only key published in CCXT's static request fixtures; holds no funds.
pub(crate) const TEST_PRIVATE_KEY: &str =
    "0xff3bdd43534543d421f05aec535965b5050ad6ac15345435345435453495e771";

/// Page size the client is expected to request on the history endpoints.
pub(crate) const PAGE_LIMIT: usize = 1_000;

/// What the venue does with `POST /fapi/v3/order`.
#[derive(Clone, Debug)]
pub(crate) enum SubmitOutcome {
    /// Answer with the standard accepted-order body.
    Accepted,
    /// Answer with an Aster `{code, msg}` body under HTTP 200.
    AsterError { code: i64, msg: String },
    /// Answer with a raw HTTP status and body (no Aster envelope).
    Status { status: u16, body: String },
    /// Answer with a raw HTTP status and body plus a `Retry-After` header, which the client
    /// turns into a bounded cooldown before the next signed request.
    StatusWithRetryAfter {
        status: u16,
        body: String,
        retry_after: String,
    },
    /// Stall for `delay` and then answer as accepted, to drive a client-side timeout.
    Stall { delay: Duration },
}

/// One recorded inbound request.
#[derive(Clone, Debug)]
pub(crate) struct CapturedRequest {
    pub(crate) method: &'static str,
    pub(crate) path: &'static str,
    pub(crate) params: HashMap<String, String>,
}

impl CapturedRequest {
    #[must_use]
    pub(crate) fn param(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(String::as_str)
    }
}

/// The venue's scripted behaviour; every field is swappable mid-test.
#[derive(Debug)]
pub(crate) struct VenueScript {
    /// Aster error body returned by `POST /fapi/v3/listenKey`, if any.
    pub(crate) listen_key_error: Option<Value>,
    /// Number of `POST /fapi/v3/listenKey` calls that stall past the client's timeout.
    ///
    /// Drives the transport-fault path of the initial-connect retry: the client sees a
    /// timeout, not a venue answer.
    pub(crate) listen_key_stalls: usize,
    /// How long a stalled `POST /fapi/v3/listenKey` waits before answering.
    pub(crate) listen_key_stall: Duration,
    /// Per-symbol `{makerCommissionRate, takerCommissionRate}` bodies.
    pub(crate) commission_rates: HashMap<String, Value>,
    /// Aster error body returned by `GET /fapi/v3/commissionRate` for unlisted symbols.
    pub(crate) commission_error: Value,
    /// `GET /fapi/v3/balance` body.
    pub(crate) balances: Value,
    /// Number of `GET /fapi/v3/balance` calls that stall before answering.
    ///
    /// Drives the path where a read was already in flight when a newer stream row landed: the
    /// response carries the account as it was when the read began.
    pub(crate) balance_stalls: usize,
    /// How long a stalled `GET /fapi/v3/balance` waits before answering.
    pub(crate) balance_stall: Duration,
    /// `GET /fapi/v3/openOrders` body.
    pub(crate) open_orders: Value,
    /// Orders addressable by `origClientOrderId` and by `orderId`.
    pub(crate) orders: HashMap<String, Value>,
    /// What `POST /fapi/v3/order` does.
    pub(crate) submit: SubmitOutcome,
    /// Complete per-symbol order history; the server paginates it like the venue does.
    pub(crate) all_orders: HashMap<String, Vec<Value>>,
    /// Per-symbol Aster error body for `GET /fapi/v3/allOrders`.
    pub(crate) all_orders_error: HashMap<String, Value>,
    /// Complete per-symbol trade history; the server paginates it like the venue does.
    pub(crate) user_trades: HashMap<String, Vec<Value>>,
    /// Per-symbol Aster error body for `GET /fapi/v3/userTrades`.
    pub(crate) user_trades_error: HashMap<String, Value>,
    /// Per-key `GET /fapi/v3/order` answers that fail with HTTP 500 before succeeding.
    ///
    /// Keyed by the lookup value the client sends (`orderId` or `origClientOrderId`); each
    /// query for that key consumes one, so a transient outage can be scripted for exactly one
    /// compensation pass.
    pub(crate) order_query_faults: HashMap<String, usize>,
    /// `GET /fapi/v3/positionRisk` body.
    pub(crate) position_risk: Value,
    /// `GET /fapi/v3/positionSide/dual` body, when the venue answers one.
    pub(crate) position_mode: Option<Value>,
    /// Aster error body returned by `GET /fapi/v3/positionSide/dual`, if any.
    pub(crate) position_mode_error: Option<Value>,
    /// Raw status and body returned by `GET /fapi/v3/positionSide/dual`, if any.
    pub(crate) position_mode_status: Option<(u16, String)>,
    /// Aster error body returned by `DELETE /fapi/v3/order`, if any.
    pub(crate) cancel_error: Option<Value>,
    /// Raw status and body returned by `DELETE /fapi/v3/order`, if any.
    ///
    /// Takes precedence over [`VenueScript::cancel_error`], and drives the paths where the
    /// venue never produced an Aster error body at all.
    pub(crate) cancel_status: Option<(u16, String)>,
    /// Whether new user data stream sockets are refused before the upgrade.
    ///
    /// Drives the window where the socket is gone but the shared client keeps retrying, so the
    /// session loop never sees the stream end.
    pub(crate) ws_refuse: bool,
}

impl Default for VenueScript {
    fn default() -> Self {
        Self {
            listen_key_error: None,
            listen_key_stalls: 0,
            listen_key_stall: Duration::from_secs(3),
            commission_rates: HashMap::new(),
            commission_error: json!({"code": -1121, "msg": "Invalid symbol."}),
            balances: json!([]),
            balance_stalls: 0,
            balance_stall: Duration::from_secs(1),
            open_orders: json!([]),
            orders: HashMap::new(),
            submit: SubmitOutcome::Accepted,
            all_orders: HashMap::new(),
            all_orders_error: HashMap::new(),
            user_trades: HashMap::new(),
            user_trades_error: HashMap::new(),
            order_query_faults: HashMap::new(),
            position_risk: json!([]),
            position_mode: None,
            position_mode_error: None,
            position_mode_status: None,
            cancel_error: None,
            cancel_status: None,
            ws_refuse: false,
        }
    }
}

/// Shared handle to the running mock venue.
#[derive(Clone)]
pub(crate) struct MockVenue {
    pub(crate) addr: SocketAddr,
    script: Arc<Mutex<VenueScript>>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    ws_events: broadcast::Sender<WsCommand>,
    ws_connections: Arc<AtomicUsize>,
}

/// A command pushed to every open user data stream socket.
#[derive(Clone, Debug)]
pub(crate) enum WsCommand {
    /// Send this JSON frame.
    Send(String),
    /// Close the socket, which the client must treat as an outage.
    Close,
}

impl MockVenue {
    /// Starts the mock venue on an ephemeral port and waits until it answers.
    pub(crate) async fn start() -> Self {
        let (ws_events, _) = broadcast::channel(64);
        let venue = Self {
            addr: "127.0.0.1:0".parse().expect("placeholder"),
            script: Arc::new(Mutex::new(VenueScript::default())),
            requests: Arc::new(Mutex::new(Vec::new())),
            ws_events,
            ws_connections: Arc::new(AtomicUsize::new(0)),
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let venue = Self { addr, ..venue };
        let router = venue.clone().router();

        tokio::spawn(async move {
            let _ = axum::serve(listener, router.into_make_service()).await;
        });

        let health = format!("http://{addr}/fapi/v1/exchangeInfo");
        let client = HttpClient::builder().build().expect("http client");
        nautilus_common::testing::wait_until_async(
            || {
                let url = health.clone();
                let client = client.clone();
                async move { client.get(url, None, None, Some(1), None).await.is_ok() }
            },
            Duration::from_secs(5),
        )
        .await;

        venue
    }

    #[must_use]
    pub(crate) fn http_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    #[must_use]
    pub(crate) fn ws_url(&self) -> String {
        format!("ws://{}/ws", self.addr)
    }

    /// Mutates the scripted behaviour.
    pub(crate) fn script<F: FnOnce(&mut VenueScript)>(&self, edit: F) {
        edit(&mut self.script.lock());
    }

    /// Returns every request recorded so far.
    #[must_use]
    pub(crate) fn requests(&self) -> Vec<CapturedRequest> {
        self.requests.lock().clone()
    }

    /// Returns the recorded requests for one path and method.
    #[must_use]
    pub(crate) fn requests_for(&self, method: &str, path: &str) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .iter()
            .filter(|request| request.method == method && request.path == path)
            .cloned()
            .collect()
    }

    pub(crate) fn clear_requests(&self) {
        self.requests.lock().clear();
    }

    /// Returns how many user data stream sockets have been opened.
    #[must_use]
    pub(crate) fn ws_connection_count(&self) -> usize {
        self.ws_connections.load(Ordering::SeqCst)
    }

    /// Pushes a frame to every open user data stream socket.
    pub(crate) fn push_ws(&self, frame: &Value) {
        let _ = self.ws_events.send(WsCommand::Send(frame.to_string()));
    }

    /// Drops every open user data stream socket, simulating an outage.
    pub(crate) fn drop_ws(&self) {
        let _ = self.ws_events.send(WsCommand::Close);
    }

    fn router(self) -> Router {
        Router::new()
            .route("/fapi/v1/exchangeInfo", get(handle_exchange_info))
            .route("/fapi/v3/positionSide/dual", get(handle_position_mode))
            .route("/fapi/v3/commissionRate", get(handle_commission_rate))
            .route("/fapi/v3/balance", get(handle_balance))
            .route("/fapi/v3/positionRisk", get(handle_position_risk))
            .route("/fapi/v3/openOrders", get(handle_open_orders))
            .route("/fapi/v3/allOrders", get(handle_all_orders))
            .route("/fapi/v3/userTrades", get(handle_user_trades))
            .route(
                "/fapi/v3/order",
                post(handle_order_submit)
                    .get(handle_order_query)
                    .delete(handle_order_cancel),
            )
            .route("/fapi/v3/allOpenOrders", delete(handle_cancel_all))
            .route(
                "/fapi/v3/listenKey",
                post(handle_listen_key_create)
                    .put(handle_listen_key_keepalive)
                    .delete(handle_listen_key_close),
            )
            .route("/ws/{listen_key}", get(handle_ws))
            .with_state(self)
    }

    fn record(&self, method: &'static str, path: &'static str, params: HashMap<String, String>) {
        self.requests.lock().push(CapturedRequest {
            method,
            path,
            params,
        });
    }
}

fn json_ok(body: &Value) -> Response {
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// Answers with a raw status and body, bypassing the Aster error envelope.
fn raw_status(status: u16, body: &str) -> Response {
    (
        StatusCode::from_u16(status).expect("valid status"),
        [("content-type", "text/plain")],
        body.to_string(),
    )
        .into_response()
}

fn json_error(body: &Value) -> Response {
    (
        StatusCode::BAD_REQUEST,
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// The trimmed Aster `exchangeInfo` body used to load instruments.
#[must_use]
pub(crate) fn exchange_info() -> Value {
    let raw = include_str!("../../test_data/http_exchange_info.json");
    let doc: Value = serde_json::from_str(raw).expect("fixture must be valid JSON");
    doc["response"].clone()
}

async fn handle_exchange_info() -> Response {
    json_ok(&exchange_info())
}

async fn handle_position_mode(State(venue): State<MockVenue>) -> Response {
    venue.record("GET", "positionSide/dual", HashMap::new());
    let script = venue.script.lock();

    if let Some((status, body)) = script.position_mode_status.as_ref() {
        return raw_status(*status, body);
    }

    if let Some(error) = script.position_mode_error.as_ref() {
        return json_error(error);
    }

    match script.position_mode.as_ref() {
        Some(body) => json_ok(body),
        None => json_ok(&json!({"dualSidePosition": false})),
    }
}

async fn handle_commission_rate(
    State(venue): State<MockVenue>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    venue.record("GET", "commissionRate", params.clone());
    let symbol = params.get("symbol").cloned().unwrap_or_default();
    let script = venue.script.lock();

    match script.commission_rates.get(&symbol) {
        Some(body) => json_ok(body),
        None => json_error(&script.commission_error),
    }
}

async fn handle_balance(State(venue): State<MockVenue>) -> Response {
    venue.record("GET", "balance", HashMap::new());

    let (stall, body) = {
        let mut script = venue.script.lock();
        let stall = if script.balance_stalls > 0 {
            script.balance_stalls -= 1;
            Some(script.balance_stall)
        } else {
            None
        };
        // The body is read when the request arrives, not when the response leaves: a stalled
        // response must carry the account as it was when the read began.
        (stall, script.balances.clone())
    };

    if let Some(delay) = stall {
        tokio::time::sleep(delay).await;
    }

    json_ok(&body)
}

async fn handle_position_risk(
    State(venue): State<MockVenue>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    venue.record("GET", "positionRisk", params);
    let body = venue.script.lock().position_risk.clone();
    json_ok(&body)
}

async fn handle_open_orders(
    State(venue): State<MockVenue>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    venue.record("GET", "openOrders", params.clone());
    let all = venue.script.lock().open_orders.clone();

    let Some(symbol) = params.get("symbol") else {
        return json_ok(&all);
    };

    let filtered: Vec<Value> = all
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|order| order["symbol"].as_str() == Some(symbol.as_str()))
        .collect();
    json_ok(&Value::Array(filtered))
}

/// Applies the venue's documented paging rules to a full history.
///
/// `cursor_key` is `orderId` for `allOrders` and `fromId` for `userTrades`; `id_key` names the
/// row's own identifier. The venue refuses the cursor together with a time window, and returns
/// at most `limit` rows in ascending ID order.
fn paginate(
    rows: &[Value],
    params: &HashMap<String, String>,
    cursor_key: &str,
    id_key: &str,
) -> Response {
    let cursor = params.get(cursor_key).and_then(|v| v.parse::<i64>().ok());
    let start = params.get("startTime").and_then(|v| v.parse::<i64>().ok());
    let end = params.get("endTime").and_then(|v| v.parse::<i64>().ok());

    if cursor.is_some() && (start.is_some() || end.is_some()) {
        return json_error(&json!({
            "code": -1128,
            "msg": format!("Combination of optional parameters invalid: {cursor_key} with a time window"),
        }));
    }

    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(500)
        .min(PAGE_LIMIT);

    let mut matched: Vec<Value> = rows
        .iter()
        .filter(|row| {
            let id = row[id_key].as_i64().unwrap_or_default();
            // A row without a `time` is returned whatever the window: the venue cannot filter
            // on a field it did not record, and Aster does omit `time` on some history rows.
            let time = row["time"].as_i64();
            cursor.is_none_or(|cursor| id >= cursor)
                && time.is_none_or(|time| {
                    start.is_none_or(|start| time >= start) && end.is_none_or(|end| time <= end)
                })
        })
        .cloned()
        .collect();

    matched.sort_by_key(|row| row[id_key].as_i64().unwrap_or_default());
    matched.truncate(limit);
    json_ok(&Value::Array(matched))
}

async fn handle_all_orders(
    State(venue): State<MockVenue>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    venue.record("GET", "allOrders", params.clone());
    let symbol = params.get("symbol").cloned().unwrap_or_default();
    let script = venue.script.lock();

    if let Some(error) = script.all_orders_error.get(&symbol) {
        return json_error(error);
    }

    let rows = script.all_orders.get(&symbol).cloned().unwrap_or_default();
    paginate(&rows, &params, "orderId", "orderId")
}

async fn handle_user_trades(
    State(venue): State<MockVenue>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    venue.record("GET", "userTrades", params.clone());
    let symbol = params.get("symbol").cloned().unwrap_or_default();
    let script = venue.script.lock();

    if let Some(error) = script.user_trades_error.get(&symbol) {
        return json_error(error);
    }

    let rows = script.user_trades.get(&symbol).cloned().unwrap_or_default();
    paginate(&rows, &params, "fromId", "id")
}

/// Decodes an `application/x-www-form-urlencoded` signed body into its parameters.
fn form_params(body: &str) -> HashMap<String, String> {
    body.split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| (key.to_string(), percent_decode(value)))
        .collect()
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        index += 3;
                    }
                    Err(_) => {
                        out.push(bytes[index]);
                        index += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8_lossy(&out).to_string()
}

async fn handle_order_submit(State(venue): State<MockVenue>, body: String) -> Response {
    let params = form_params(&body);
    venue.record("POST", "order", params.clone());

    let outcome = venue.script.lock().submit.clone();
    match outcome {
        SubmitOutcome::Accepted => json_ok(&accepted_order(&params)),
        SubmitOutcome::AsterError { code, msg } => json_ok(&json!({"code": code, "msg": msg})),
        SubmitOutcome::Status { status, body } => (
            StatusCode::from_u16(status).expect("valid status"),
            [("content-type", "text/plain")],
            body,
        )
            .into_response(),
        SubmitOutcome::StatusWithRetryAfter {
            status,
            body,
            retry_after,
        } => (
            StatusCode::from_u16(status).expect("valid status"),
            [
                ("content-type", "text/plain"),
                ("retry-after", retry_after.as_str()),
            ],
            body,
        )
            .into_response(),
        SubmitOutcome::Stall { delay } => {
            tokio::time::sleep(delay).await;
            json_ok(&accepted_order(&params))
        }
    }
}

fn accepted_order(params: &HashMap<String, String>) -> Value {
    json!({
        "orderId": 900_001,
        "symbol": params.get("symbol").cloned().unwrap_or_default(),
        "status": "NEW",
        "clientOrderId": params.get("newClientOrderId").cloned().unwrap_or_default(),
        "price": params.get("price").cloned().unwrap_or_else(|| "0".to_string()),
        "avgPrice": "0.00000",
        "origQty": params.get("quantity").cloned().unwrap_or_default(),
        "executedQty": "0",
        "cumQuote": "0",
        "timeInForce": params.get("timeInForce").cloned().unwrap_or_else(|| "GTC".to_string()),
        "type": params.get("type").cloned().unwrap_or_else(|| "LIMIT".to_string()),
        "reduceOnly": false,
        "closePosition": false,
        "side": params.get("side").cloned().unwrap_or_else(|| "BUY".to_string()),
        "positionSide": "BOTH",
        "updateTime": 1_788_571_663_397i64,
    })
}

async fn handle_order_query(
    State(venue): State<MockVenue>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    venue.record("GET", "order", params.clone());
    let mut script = venue.script.lock();

    let key = params
        .get("origClientOrderId")
        .or_else(|| params.get("orderId"))
        .cloned()
        .unwrap_or_default();

    if let Some(remaining) = script.order_query_faults.get_mut(&key)
        && *remaining > 0
    {
        *remaining -= 1;
        return raw_status(500, "internal error");
    }

    match script.orders.get(&key) {
        Some(order) => json_ok(order),
        None => json_error(&json!({"code": -2013, "msg": "Order does not exist."})),
    }
}

async fn handle_order_cancel(State(venue): State<MockVenue>, body: String) -> Response {
    let params = form_params(&body);
    venue.record("DELETE", "order", params.clone());

    let script = venue.script.lock();
    if let Some((status, body)) = script.cancel_status.as_ref() {
        return raw_status(*status, body);
    }

    if let Some(error) = script.cancel_error.as_ref() {
        return json_error(error);
    }

    let key = params
        .get("origClientOrderId")
        .or_else(|| params.get("orderId"))
        .cloned()
        .unwrap_or_default();

    match script.orders.get(&key) {
        Some(order) => {
            let mut canceled = order.clone();
            canceled["status"] = json!("CANCELED");
            json_ok(&canceled)
        }
        None => json_ok(&json!({
            "orderId": key.parse::<i64>().unwrap_or(1),
            "symbol": params.get("symbol").cloned().unwrap_or_default(),
            "status": "CANCELED",
            "clientOrderId": "",
            "price": "0",
            "origQty": "0",
            "executedQty": "0",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
        })),
    }
}

async fn handle_cancel_all(State(venue): State<MockVenue>, body: String) -> Response {
    venue.record("DELETE", "allOpenOrders", form_params(&body));
    json_ok(&json!({"code": 200, "msg": "The operation of cancel all open order is done."}))
}

async fn handle_listen_key_create(State(venue): State<MockVenue>) -> Response {
    venue.record("POST", "listenKey", HashMap::new());

    let (stall, error) = {
        let mut script = venue.script.lock();
        let stall = if script.listen_key_stalls > 0 {
            script.listen_key_stalls -= 1;
            Some(script.listen_key_stall)
        } else {
            None
        };
        (stall, script.listen_key_error.clone())
    };

    if let Some(delay) = stall {
        tokio::time::sleep(delay).await;
    }

    match error {
        Some(error) => json_error(&error),
        None => json_ok(&json!({"listenKey": "aster-test-listen-key"})),
    }
}

async fn handle_listen_key_keepalive(State(venue): State<MockVenue>) -> Response {
    venue.record("PUT", "listenKey", HashMap::new());
    json_ok(&json!({}))
}

async fn handle_listen_key_close(State(venue): State<MockVenue>) -> Response {
    venue.record("DELETE", "listenKey", HashMap::new());
    json_ok(&json!({}))
}

async fn handle_ws(State(venue): State<MockVenue>, ws: WebSocketUpgrade) -> Response {
    if venue.script.lock().ws_refuse {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    ws.on_upgrade(move |socket| serve_ws(socket, venue))
}

async fn serve_ws(mut socket: WebSocket, venue: MockVenue) {
    venue.ws_connections.fetch_add(1, Ordering::SeqCst);
    let mut events = venue.ws_events.subscribe();

    loop {
        tokio::select! {
            command = events.recv() => match command {
                Ok(WsCommand::Send(frame)) => {
                    if socket.send(Message::Text(frame.into())).await.is_err() {
                        break;
                    }
                }
                Ok(WsCommand::Close) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(_)) => {}
                _ => break,
            },
        }
    }

    let _ = socket.send(Message::Close(None)).await;
}
