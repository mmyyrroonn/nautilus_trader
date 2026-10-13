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

//! Offline native continuous execution against a stateful HTTP and private WebSocket venue.
//! All venue evidence travels through the production transport, without ingestion hooks.

use std::{
    cell::RefCell,
    collections::BTreeMap,
    num::NonZeroU32,
    path::PathBuf,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
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
        execution::{CancelOrder, SubmitOrder},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    data::QuoteTick,
    enums::{AccountType, OmsType, OrderSide, OrderType, TimeInForce},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId},
    orders::{Order, OrderAny, builder::OrderTestBuilder},
    types::{Price, Quantity},
};
use nautilus_network::ratelimiter::quota::Quota;
use nautilus_ondo::{
    common::{consts::ONDO_VENUE, credential::OndoCredential, enums::OndoEnvironment},
    config::OndoExecutionClientConfig,
    execution::OndoExecutionClient,
    http::rate_limit::OndoRateBudget,
    reconciliation::ReconciliationState,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;

const NVDA: &str = "NVDA-USD-PERP.ONDO";
const TSLA: &str = "TSLA-USD-PERP.ONDO";
const WAIT: Duration = Duration::from_secs(15);
const KEY: &str = "ondoKeyId_CONTINUOUS_TEST_ONLY";
const SECRET: &str = "ondoApiSecret_CONTINUOUS_TEST_ONLY";
fn now() -> UnixNanos {
    nautilus_core::time::get_atomic_clock_realtime().get_time_ns()
}

#[derive(Clone, Debug)]
struct Request {
    method: String,
    target: String,
    body: Value,
}
#[derive(Default)]
struct State {
    requests: Vec<Request>,
    orders: BTreeMap<String, Value>,
    fills: Vec<Value>,
    positions: BTreeMap<String, Decimal>,
}
struct Venue {
    http: String,
    ws: String,
    state: Arc<Mutex<State>>,
    sockets: Arc<Mutex<Vec<mpsc::UnboundedSender<Message>>>>,
    lost: Arc<AtomicBool>,
    race: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}
impl Drop for Venue {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
impl Venue {
    async fn start() -> Self {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = format!("http://{}", http_listener.local_addr().unwrap());
        let ws = format!("ws://{}/ws", ws_listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State::default()));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let lost = Arc::new(AtomicBool::new(false));
        let race = Arc::new(AtomicBool::new(false));
        let http_task = tokio::spawn({
            let state = Arc::clone(&state);
            let lost = Arc::clone(&lost);
            let race = Arc::clone(&race);
            async move {
                while let Ok((stream, _)) = http_listener.accept().await {
                    let state = Arc::clone(&state);
                    let lost = Arc::clone(&lost);
                    let race = Arc::clone(&race);
                    tokio::spawn(async move {
                        serve(stream, state, lost, race).await;
                    });
                }
            }
        });
        let ws_task = tokio::spawn({
            let sockets = Arc::clone(&sockets);
            async move {
                while let Ok((stream, _)) = ws_listener.accept().await {
                    let sockets = Arc::clone(&sockets);
                    tokio::spawn(async move {
                        let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
                            return;
                        };
                        let (mut sink, mut source) = socket.split();
                        let (sender, mut receiver) = mpsc::unbounded_channel();
                        sockets.lock().unwrap().push(sender);
                        loop {
                            tokio::select! {
                                outgoing = receiver.recv() => match outgoing {
                                    Some(frame) => if sink.send(frame).await.is_err() { return; }, None => return,
                                },
                                incoming = source.next() => match incoming {
                                    Some(Ok(Message::Text(text))) => {
                                        let Ok(value) = serde_json::from_str::<Value>(&text) else { continue; };
                                        let channel = value["channel"].as_str().unwrap_or_default();
                                        let reply = match value["op"].as_str() {
                                            Some("login") => json!({"type":"loggedIn","msg":"Login successful"}),
                                            Some("subscribe") => json!({"type":"subscribed","channel":channel}),
                                            Some("unsubscribe") => json!({"type":"unsubscribed","channel":channel}),
                                            Some("ping") => json!({"type":"pong"}), _ => continue,
                                        };
                                        if sink.send(Message::Text(reply.to_string().into())).await.is_err() { return; }
                                    }, Some(Err(_)) | None => return, _ => {},
                                },
                            }
                        }
                    });
                }
            }
        });
        Self {
            http,
            ws,
            state,
            sockets,
            lost,
            race,
            tasks: vec![http_task, ws_task],
        }
    }
    fn writes(&self) -> Vec<Request> {
        self.state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r.method != "GET")
            .cloned()
            .collect()
    }
    fn push(&self, channel: &str, item: &Value) {
        self.sockets
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .send(Message::Text(
                json!({"type":"update","channel":channel,"data":[item]})
                    .to_string()
                    .into(),
            ))
            .unwrap();
    }
    fn fill(&self, id: &str, quantity: &str) -> Value {
        let (fill, order) = {
            let mut state = self.state.lock().unwrap();
            let fill = apply_fill(&mut state, id, quantity);
            (fill, state.orders[id].clone())
        };
        self.push("ordersPerps", &order);
        self.push("fillsPerps", &fill);
        fill
    }
}
fn apply_fill(state: &mut State, id: &str, quantity: &str) -> Value {
    let market = state.orders[id]["market"].as_str().unwrap();
    let before = state.positions.get(market).copied().unwrap_or_default();
    let order = state.orders.get_mut(id).unwrap();
    let size = Decimal::from_str_exact(quantity).unwrap();
    let filled = Decimal::from_str_exact(order["filledSize"].as_str().unwrap()).unwrap() + size;
    order["filledSize"] = json!(filled.to_string());
    if filled == Decimal::from_str_exact(order["size"].as_str().unwrap()).unwrap() {
        order["status"] = json!("fullyfilled");
    }
    let fill = json!({"id":format!("fill-{id}-{}",state.fills.len()),"orderId":order["orderId"],
        "clientOrderId":id,"market":order["market"],
        "price":order["price"].as_str().unwrap_or("227.50"),"size":quantity,
        "side":order["side"],"direction":if order["side"]=="buy" {
            if before < Decimal::ZERO { "closeShort" } else { "openLong" }
        } else if before > Decimal::ZERO { "closeLong" } else { "openShort" },
        "fee":"0.001","time":"2025-03-05T14:30:01.000000000Z","isMaker":false});
    *state
        .positions
        .entry(order["market"].as_str().unwrap().into())
        .or_default() += if order["side"] == "buy" { size } else { -size };
    state.fills.push(fill.clone());
    fill
}
fn position(market: &str, quantity: Decimal) -> Value {
    json!({"market":market,"direction":if quantity>=Decimal::ZERO {"long"} else {"short"},
        "netQuantity":quantity.abs().to_string(),"averageEntryPrice":"227.50","usedMargin":"5.00",
        "unrealizedPnl":"0.00","markPrice":"227.50","liquidationPrice":"180.00",
        "bankruptcyPrice":"170.00","maintenanceMargin":"1.00","notionalValue":"20.00",
        "leverage":"2.0","netFundingSinceNeutral":"0.00","returnOnEquity":"0.00"})
}
async fn serve(
    mut stream: TcpStream,
    state: Arc<Mutex<State>>,
    lost: Arc<AtomicBool>,
    race: Arc<AtomicBool>,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    let request = loop {
        let Ok(size) = stream.read(&mut chunk).await else {
            return;
        };
        if size == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..size]);
        let text = String::from_utf8_lossy(&buffer);
        let Some((head, body)) = text.split_once("\r\n\r\n") else {
            continue;
        };
        let length = head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if body.len() < length {
            continue;
        }
        let mut parts = head.lines().next().unwrap().split_whitespace();
        break Request {
            method: parts.next().unwrap().into(),
            target: parts.next().unwrap().into(),
            body: serde_json::from_str(&body[..length]).unwrap_or(Value::Null),
        };
    };
    let (status, answer, lose) = {
        let mut state = state.lock().unwrap();
        state.requests.push(request.clone());
        response(&mut state, &request, &lost, &race)
    };
    if lose {
        return;
    }
    let body = answer.to_string();
    let answer = format!(
        "HTTP/1.1 {status} Status\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(answer.as_bytes()).await;
}
fn response(
    state: &mut State,
    request: &Request,
    lost: &AtomicBool,
    race: &AtomicBool,
) -> (u16, Value, bool) {
    let path = request.target.split('?').next().unwrap();
    let mut lose = false;
    let result = match (request.method.as_str(), path) {
        ("GET", "/v1/markets") => {
            return (
                200,
                serde_json::from_str(include_str!("../test_data/rest/markets_synthetic.json"))
                    .unwrap(),
                false,
            );
        }
        ("GET", "/v1/account") => json!({"accountID":"unit-account"}),
        ("GET", "/v1/perps/contracts") => json!([
            {"market":"NVDA-USD.P","isClosed":false,"disabled":false},
            {"market":"TSLA-USD.P","isClosed":false,"disabled":false}]),
        ("GET", "/v1/perps/balance") => json!({"walletBalance":"5000.00","realizedPnl":"0.00",
            "unrealizedPnl":"0.00","marginBalance":"5000.00","usedMargin":"0.00",
            "availableMargin":"5000.00","withdrawableMargin":"5000.00",
            "maintenanceMarginRequirement":"0.00","totalMaintenanceMargin":"0.00",
            "marginRatio":"0.00","leverage":"0.00","underLiquidation":false,
            "totalFundingPayments":"0.00","totalTradingFees":"0.00","totalPnL":"0.00","netInvested":"5000.00"}),
        ("GET", "/v1/perps/orders") => json!(state.orders.values().collect::<Vec<_>>()),
        ("GET", "/v1/perps/fills") => json!(state.fills),
        ("GET", "/v1/perps/positions") => json!(
            state
                .positions
                .iter()
                .filter(|(_, q)| **q != Decimal::ZERO)
                .map(|(market, q)| position(market, *q))
                .collect::<Vec<_>>()
        ),
        ("GET", "/v1/perps/funding_fees") => json!([]),
        ("POST", "/v1/perps/orders") => {
            let body = &request.body;
            let id = body["clientOrderId"].as_str().unwrap();
            state.orders.insert(id.into(),json!({"orderId":format!("venue-{id}"),"clientOrderId":id,
                "side":body["side"],"price":body["price"],"size":body["size"],"market":body["market"],
                "filledSize":"0.00","lastFillSize":"0.00","filledCost":"0.00","fee":"0.00",
                "status":"open","createdAt":"2025-03-05T14:30:00Z","type":body["type"],
                "timeInForce":body["timeInForce"],"reduceOnly":body["reduceOnly"]}));
            if body["type"] == "market" {
                let order = state.orders.get_mut(id).unwrap().as_object_mut().unwrap();
                order.remove("price");
                order.remove("timeInForce");
            }
            if body["timeInForce"] == "IOC" || body["type"] == "market" {
                apply_fill(state, id, body["size"].as_str().unwrap());
            }
            lose = lost.swap(false, Ordering::SeqCst);
            state.orders[id].clone()
        }
        (method, path) if path.starts_with("/v1/perps/orders/") => {
            let lookup = path
                .trim_start_matches("/v1/perps/orders/")
                .replace("%3A", ":")
                .replace("%3a", ":");
            let id = lookup
                .trim_start_matches("client:")
                .trim_start_matches("venue-");
            if !state.orders.contains_key(id) {
                return (404, json!({"success":false}), false);
            }
            if method == "DELETE" {
                if race.swap(false, Ordering::SeqCst) {
                    let order = &state.orders[id];
                    let remaining = Decimal::from_str_exact(order["size"].as_str().unwrap())
                        .unwrap()
                        - Decimal::from_str_exact(order["filledSize"].as_str().unwrap()).unwrap();
                    apply_fill(state, id, &remaining.to_string());
                    return (
                        400,
                        json!({"success":false,"code":"order_already_fully_filled"}),
                        false,
                    );
                }
                state.orders.get_mut(id).unwrap()["status"] = json!("canceled");
            }
            state.orders[id].clone()
        }
        _ => return (404, json!({"success":false}), false),
    };
    (200, json!({"success":true,"result":result}), lose)
}
struct Journal {
    directory: PathBuf,
    path: PathBuf,
}
impl Journal {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("ondo-continuous-{}", UUID4::new()));
        Self {
            path: directory.join("ledger.json"),
            directory,
        }
    }
}
impl Drop for Journal {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
struct Harness {
    client: OndoExecutionClient,
    cache: Rc<RefCell<Cache>>,
    events: Vec<ExecutionEvent>,
    receiver: mpsc::UnboundedReceiver<ExecutionEvent>,
}
fn configuration(journal: &Journal) -> OndoExecutionClientConfig {
    OndoExecutionClientConfig {environment:OndoEnvironment::Production,
        account_id:Some(AccountId::from("ONDO-CONTINUOUS-001")),allow_production_orders:true,
        expected_venue_account_id:Some("unit-account".into()),diagnostics_run_id:Some("continuous-native-tests".into()),
        journal_path:Some(journal.path.display().to_string()),reconcile_interval_secs:1,
        continuous_execution:Some(serde_json::from_value(json!({"instrument_ids":[NVDA,TSLA],
            "max_notional_per_order_usd":"100","max_gross_exposure_usd":"500","min_available_margin_usdc":"25",
            "max_live_orders":8,"max_requests_per_second":1000,"max_unresolved_orders":1})).unwrap()),
        ..Default::default()}
}
fn harness(venue: &Venue, configuration: OndoExecutionClientConfig) -> Harness {
    let cache = Rc::new(RefCell::new(Cache::default()));
    let instruments = nautilus_ondo::http::models::parse_instruments(
        include_str!("../test_data/rest/markets_synthetic.json"),
        &[InstrumentId::from(NVDA), InstrumentId::from(TSLA)],
        now(),
    )
    .unwrap();
    for instrument in instruments {
        cache.borrow_mut().add_instrument(instrument).unwrap();
    }
    let core = ExecutionClientCore::new(
        TraderId::from("TESTER-001"),
        ClientId::from("ONDO-EXEC"),
        *ONDO_VENUE,
        OmsType::Netting,
        AccountId::from("ONDO-CONTINUOUS-001"),
        AccountType::Margin,
        None,
        Rc::clone(&cache),
    );
    let config = OndoExecutionClientConfig {
        base_url_http: Some(venue.http.clone()),
        base_url_ws: Some(venue.ws.clone()),
        ..configuration
    };
    let credential = OndoCredential::new(config.environment, KEY.into(), SECRET.into()).unwrap();
    let (sender, receiver) = mpsc::unbounded_channel();
    let (data_sender, _data_receiver) = mpsc::unbounded_channel::<DataEvent>();
    replace_exec_event_sender(sender);
    replace_data_event_sender(data_sender);
    let budget =
        OndoRateBudget::with_quota(Quota::per_second(NonZeroU32::new(1000).unwrap()).unwrap());
    Harness {
        client: OndoExecutionClient::with_credential(core, config, Some(credential), Some(budget))
            .unwrap(),
        cache,
        events: Vec::new(),
        receiver,
    }
}
async fn wait(harness: &mut Harness, label: &str, done: impl Fn(&OndoExecutionClient) -> bool) {
    let began = Instant::now();
    loop {
        while let Ok(event) = harness.receiver.try_recv() {
            harness.events.push(event);
        }
        if done(&harness.client) {
            return;
        }
        assert!(
            began.elapsed() < WAIT,
            "{label}; state={:?}; refusal={:?}; orders={:?}",
            harness.client.reconciliation_state(),
            harness.client.new_risk_refusal(),
            harness
                .events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::Order(_)))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn connect(harness: &mut Harness) {
    harness.client.start().unwrap();
    harness.client.connect().await.unwrap();
    wait(harness, "continuous account ready", |client| {
        client.can_submit_new_orders()
    })
    .await;
}
fn order(id: &str, instrument: &str, quantity: &str, close: bool, gtc: bool) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(instrument))
        .client_order_id(ClientOrderId::from(id))
        .side(if close {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        })
        .quantity(Quantity::from(quantity))
        .price(Price::from(if close {
            "225.00"
        } else if gtc {
            "227.00"
        } else {
            "230.00"
        }))
        .time_in_force(if gtc {
            TimeInForce::Gtc
        } else {
            TimeInForce::Ioc
        })
        .post_only(gtc)
        .reduce_only(close)
        .build()
}
fn submit(harness: &Harness, order: &OrderAny) {
    let at = now();
    harness
        .cache
        .borrow_mut()
        .add_quote(QuoteTick::new(
            order.instrument_id(),
            Price::from("227.49"),
            Price::from("227.50"),
            Quantity::from("10.00"),
            Quantity::from("10.00"),
            at,
            at,
        ))
        .unwrap();
    harness
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    harness
        .client
        .submit_order(SubmitOrder::new(
            order.trader_id(),
            Some(ClientId::from("ONDO-EXEC")),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            order.init_event().clone(),
            None,
            None,
            None,
            UUID4::new(),
            now(),
            None,
        ))
        .unwrap();
}

fn market_order(id: &str, side: OrderSide, reduce_only: bool) -> OrderAny {
    OrderTestBuilder::new(OrderType::Market)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(NVDA))
        .client_order_id(ClientOrderId::from(id))
        .side(side)
        .quantity(Quantity::from("0.05"))
        .time_in_force(TimeInForce::Ioc)
        .reduce_only(reduce_only)
        .build()
}
fn cancel(harness: &Harness, order: &OrderAny) {
    harness
        .client
        .cancel_order(CancelOrder::new(
            order.trader_id(),
            Some(ClientId::from("ONDO-EXEC")),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            None,
            UUID4::new(),
            now(),
            None,
            None,
        ))
        .unwrap();
}
async fn settled(harness: &mut Harness, id: &str, quantity: &str) {
    wait(harness, "terminal order and exact fills", |client| {
        client
            .order_state(&ClientOrderId::from(id))
            .is_some_and(|state| state.is_settled() && state.filled == Quantity::from(quantity))
    })
    .await;
}
async fn ready(harness: &mut Harness) {
    wait(harness, "fresh account reconciliation", |client| {
        client.reconciliation_state() == ReconciliationState::Ready
            && client.can_submit_new_orders()
            && client.production_trade_snapshot().is_some_and(|snapshot| {
                snapshot["latest_activity_generation"] == snapshot["reconciled_activity_generation"]
                    && snapshot["account_positions_owned"] == true
            })
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_market_long_and_short_round_trips_omit_price_and_time_in_force() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    let mut harness = harness(&venue, configuration(&journal));
    connect(&mut harness).await;
    for (cycle, open, close) in [
        ("long", OrderSide::Buy, OrderSide::Sell),
        ("short", OrderSide::Sell, OrderSide::Buy),
    ] {
        let entry_id = format!("market-{cycle}-entry");
        submit(&harness, &market_order(&entry_id, open, false));
        settled(&mut harness, &entry_id, "0.05").await;
        ready(&mut harness).await;
        let exit_id = format!("market-{cycle}-exit");
        submit(&harness, &market_order(&exit_id, close, true));
        settled(&mut harness, &exit_id, "0.05").await;
        ready(&mut harness).await;
    }
    let writes = venue.writes();
    assert_eq!(writes.len(), 4);
    for write in writes {
        assert_eq!(write.body["type"], "market");
        assert!(write.body.get("price").is_none());
        assert!(write.body.get("timeInForce").is_none());
        assert_eq!(write.body["postOnly"], false);
    }
    assert_eq!(harness.client.applied_fill_count(), 4);
    assert!(
        venue
            .state
            .lock()
            .unwrap()
            .positions
            .values()
            .all(|q| *q == Decimal::ZERO)
    );
    harness.client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_multiple_symbols_repeat_ioc_cycles() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    let mut harness = harness(&venue, configuration(&journal));
    connect(&mut harness).await;
    for cycle in 0..3 {
        for instrument in [NVDA, TSLA] {
            let symbol = if instrument == NVDA { "nvda" } else { "tsla" };
            let entry_id = format!("entry-{cycle}-{symbol}");
            submit(
                &harness,
                &order(&entry_id, instrument, "0.05", false, false),
            );
            settled(&mut harness, &entry_id, "0.05").await;
            ready(&mut harness).await;
            let close_id = format!("close-{cycle}-{symbol}");
            submit(&harness, &order(&close_id, instrument, "0.05", true, false));
            settled(&mut harness, &close_id, "0.05").await;
            ready(&mut harness).await;
        }
    }
    assert_eq!(harness.client.applied_fill_count(), 12);
    assert_eq!(venue.writes().len(), 12);
    assert!(
        venue
            .state
            .lock()
            .unwrap()
            .positions
            .values()
            .all(|q| *q == Decimal::ZERO)
    );
    harness.client.disconnect().await.unwrap();
    assert_eq!(venue.writes().len(), 12);
    assert!(!harness.client.private_stream_is_running());
}
#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_gtc_partial_duplicate_late_fill_cancel_replace() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    let mut harness = harness(&venue, configuration(&journal));
    connect(&mut harness).await;
    let first = order("gtc-first", NVDA, "0.05", false, true);
    submit(&harness, &first);
    wait(&mut harness, "resting GTC acknowledged", |client| {
        client
            .order_state(&first.client_order_id())
            .is_some_and(|state| state.venue_order_id.is_some())
    })
    .await;
    let fill = venue.fill("gtc-first", "0.02");
    wait(&mut harness, "partial fill", |client| {
        client.applied_fill_count() == 1
    })
    .await;
    venue.push("fillsPerps", &fill);
    cancel(&harness, &first);
    settled(&mut harness, "gtc-first", "0.02").await;
    ready(&mut harness).await;
    venue.push("fillsPerps", &fill);
    let replacement = order("gtc-replacement", NVDA, "0.05", false, true);
    submit(&harness, &replacement);
    wait(&mut harness, "replacement acknowledged", |client| {
        client
            .order_state(&replacement.client_order_id())
            .is_some_and(|state| state.venue_order_id.is_some())
    })
    .await;
    cancel(&harness, &replacement);
    settled(&mut harness, "gtc-replacement", "0.00").await;
    ready(&mut harness).await;
    submit(&harness, &order("gtc-close", NVDA, "0.02", true, false));
    settled(&mut harness, "gtc-close", "0.02").await;
    ready(&mut harness).await;
    assert_eq!(harness.client.applied_fill_count(), 2);
    let writes = venue.writes();
    assert_eq!(writes.iter().filter(|r| r.method == "POST").count(), 3);
    assert_eq!(writes.iter().filter(|r| r.method == "DELETE").count(), 2);
    assert_eq!(writes[0].body["timeInForce"], "GTC");
    assert_eq!(writes[0].body["postOnly"], true);
    assert!(
        writes
            .iter()
            .filter(|r| r.method == "DELETE")
            .all(|r| r.target.contains("/orders/"))
    );
    harness.client.disconnect().await.unwrap();
}
#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_cancel_fill_race_converges_before_close() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    let mut harness = harness(&venue, configuration(&journal));
    connect(&mut harness).await;
    let resting = order("cancel-race", TSLA, "0.05", false, true);
    submit(&harness, &resting);
    wait(&mut harness, "resting acknowledged", |client| {
        client
            .order_state(&resting.client_order_id())
            .is_some_and(|state| state.venue_order_id.is_some())
    })
    .await;
    venue.race.store(true, Ordering::SeqCst);
    cancel(&harness, &resting);
    settled(&mut harness, "cancel-race", "0.05").await;
    ready(&mut harness).await;
    submit(&harness, &order("race-close", TSLA, "0.05", true, false));
    settled(&mut harness, "race-close", "0.05").await;
    ready(&mut harness).await;
    assert_eq!(harness.client.applied_fill_count(), 2);
    assert_eq!(
        venue.writes().iter().filter(|r| r.method == "POST").count(),
        2
    );
    harness.client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_lost_response_and_restart_preserve_original_owned_fill() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    {
        let mut first = harness(&venue, configuration(&journal));
        connect(&mut first).await;
        venue.lost.store(true, Ordering::SeqCst);
        submit(&first, &order("lost-original", NVDA, "0.05", false, false));
        settled(&mut first, "lost-original", "0.05").await;
        ready(&mut first).await;
        assert_eq!(first.client.applied_fill_count(), 1);
        assert_eq!(
            venue.writes().len(),
            1,
            "a lost response never resubmits the order"
        );
        assert!(
            first.client.disconnect().await.is_err(),
            "residual position prevents clean shutdown"
        );
        assert!(!first.client.private_stream_is_running());
        assert!(
            first
                .client
                .production_trade_snapshot()
                .is_none_or(|s| s["phase"] != "final")
        );
    }
    let mut second = harness(&venue, configuration(&journal));
    connect(&mut second).await;
    let restored = second
        .client
        .order_state(&ClientOrderId::from("lost-original"))
        .unwrap();
    assert_eq!(
        restored.client_order_id,
        ClientOrderId::from("lost-original")
    );
    assert!(restored.is_settled());
    assert_eq!(second.client.applied_fill_count(), 1);
    let old_fill = venue.state.lock().unwrap().fills[0].clone();
    venue.push("fillsPerps", &old_fill);
    submit(&second, &order("restart-close", NVDA, "0.05", true, false));
    settled(&mut second, "restart-close", "0.05").await;
    ready(&mut second).await;
    assert_eq!(
        second.client.applied_fill_count(),
        2,
        "checkpointed fill is counted once across restart"
    );
    assert_eq!(venue.writes().len(), 2);
    second.client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_external_open_order_is_never_canceled_or_clean() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    let mut harness = harness(&venue, configuration(&journal));
    connect(&mut harness).await;
    let foreign = order("external-order", TSLA, "0.05", false, true);
    venue.state.lock().unwrap().orders.insert(
        "external-order".into(),
        json!({
        "orderId":"venue-external-order","clientOrderId":"external-order","side":"buy",
        "price":"230.00","size":"0.05","market":"TSLA-USD.P","filledSize":"0.00",
        "lastFillSize":"0.00","filledCost":"0.00","fee":"0.00","status":"open",
        "createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC","reduceOnly":false}),
    );
    let _ = harness.client.account().reconcile_account(now()).await;
    wait(
        &mut harness,
        "external order retained in native proof",
        |client| {
            client
                .production_trade_snapshot()
                .is_some_and(|s| s["foreign_open_orders"] == 1)
        },
    )
    .await;
    cancel(&harness, &foreign);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        venue.writes().is_empty(),
        "foreign cancel must be rejected before HTTP"
    );
    assert!(harness.client.disconnect().await.is_err());
    assert!(
        venue.writes().is_empty(),
        "shutdown must not issue market-wide or foreign DELETE"
    );
    assert_eq!(
        venue.state.lock().unwrap().orders["external-order"]["status"],
        "open"
    );
    assert!(
        harness
            .client
            .production_trade_snapshot()
            .is_none_or(|s| s["phase"] != "final")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_external_start_refuses_before_dms_or_http_writes() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    venue.state.lock().unwrap().positions.insert(
        "NVDA-USD.P".into(),
        Decimal::from_str_exact("0.05").unwrap(),
    );
    let mut harness = harness(&venue, configuration(&journal));
    harness.client.start().unwrap();
    assert!(harness.client.connect().await.is_err());
    assert!(venue.writes().is_empty());
    assert!(venue.sockets.lock().unwrap().is_empty());
    assert!(!harness.client.can_submit_new_orders());
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_reused_client_id_cannot_adopt_a_different_venue_order() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    let mut harness = harness(&venue, configuration(&journal));
    connect(&mut harness).await;

    let original = order("collision-original", NVDA, "0.05", false, true);
    submit(&harness, &original);
    wait(
        &mut harness,
        "original venue identity acknowledged",
        |client| {
            client
                .order_state(&original.client_order_id())
                .is_some_and(|state| state.venue_order_id.is_some())
        },
    )
    .await;
    let original_venue_id = harness
        .client
        .order_state(&original.client_order_id())
        .unwrap()
        .venue_order_id;
    cancel(&harness, &original);
    settled(&mut harness, "collision-original", "0.00").await;
    ready(&mut harness).await;

    // An external actor reuses the terminal client ID but receives a new venue identity
    let foreign_order = json!({
        "orderId":"venue-external-collision","clientOrderId":"collision-original",
        "side":"buy","price":"227.00","size":"0.05","market":"NVDA-USD.P",
        "filledSize":"0.02","lastFillSize":"0.02","filledCost":"4.54","fee":"0.001",
        "status":"open","createdAt":"2025-03-05T14:31:00Z","type":"limit",
        "timeInForce":"GTC","reduceOnly":false
    });
    let foreign_fill = json!({
        "id":"external-collision-fill","orderId":"venue-external-collision",
        "clientOrderId":"collision-original","market":"NVDA-USD.P","price":"227.00",
        "size":"0.02","side":"buy","direction":"openLong","fee":"0.001",
        "time":"2025-03-05T14:31:01.000000000Z","isMaker":true
    });
    {
        let mut state = venue.state.lock().unwrap();
        state
            .orders
            .insert("external-collision".into(), foreign_order.clone());
        state.fills.push(foreign_fill.clone());
        state.positions.insert(
            "NVDA-USD.P".into(),
            Decimal::from_str_exact("0.02").unwrap(),
        );
    }
    venue.push("ordersPerps", &foreign_order);
    venue.push("fillsPerps", &foreign_fill);
    let _ = harness.client.account().reconcile_account(now()).await;
    wait(
        &mut harness,
        "colliding external identity is diagnosed",
        |client| {
            client.production_trade_snapshot().is_some_and(|snapshot| {
                snapshot["foreign_open_orders"] == 1
                    && snapshot["account_positions_owned"] == false
                    && snapshot["native_ready"] == false
            })
        },
    )
    .await;

    let retained = harness
        .client
        .order_state(&original.client_order_id())
        .unwrap();
    assert_eq!(retained.venue_order_id, original_venue_id);
    assert_eq!(retained.filled, Quantity::from("0.00"));
    assert!(retained.is_settled());
    assert_eq!(harness.client.applied_fill_count(), 0);
    assert_eq!(venue.writes().len(), 2);

    assert!(harness.client.disconnect().await.is_err());
    assert_eq!(
        venue.writes().len(),
        2,
        "stop cannot cancel the external order"
    );
    assert_eq!(
        venue.state.lock().unwrap().orders["external-collision"]["status"],
        "open"
    );
    assert_eq!(harness.client.applied_fill_count(), 0);
    assert!(
        harness
            .client
            .production_trade_snapshot()
            .is_none_or(|snapshot| snapshot["phase"] != "final")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_stop_cancels_owned_resting_orders_on_both_symbols() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    let mut harness = harness(&venue, configuration(&journal));
    connect(&mut harness).await;
    for (id, instrument) in [("stop-nvda", NVDA), ("stop-tsla", TSLA)] {
        let resting = order(id, instrument, "0.05", false, true);
        submit(&harness, &resting);
        wait(&mut harness, "owned resting order", |client| {
            client
                .order_state(&resting.client_order_id())
                .is_some_and(|state| state.venue_order_id.is_some())
        })
        .await;
        ready(&mut harness).await;
    }
    harness.client.disconnect().await.unwrap();
    let writes = venue.writes();
    assert_eq!(writes.len(), 4);
    assert_eq!(writes.iter().filter(|r| r.method == "DELETE").count(), 2);
    assert!(
        writes
            .iter()
            .filter(|r| r.method == "DELETE")
            .all(|r| r.target.contains("/orders/"))
    );
    assert!(
        venue
            .state
            .lock()
            .unwrap()
            .orders
            .values()
            .all(|o| o["status"] == "canceled")
    );
    assert_eq!(harness.client.applied_fill_count(), 0);
    assert!(!harness.client.private_stream_is_running());
    let proof = harness.client.production_trade_snapshot().unwrap();
    assert_eq!(proof["phase"], "final");
    assert_eq!(proof["shutdown_status"], "clean");
    assert_eq!(proof["foreign_open_orders"], 0);
    assert_eq!(proof["own_open_orders"], 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_native_gross_limit_uses_fresh_marks_across_symbols() {
    let journal = Journal::new();
    let venue = Venue::start().await;
    let mut config = configuration(&journal);
    let limits = config.continuous_execution.as_mut().unwrap();
    limits.max_notional_per_order_usd = Decimal::from(20);
    limits.max_gross_exposure_usd = Decimal::from(20);
    let mut harness = harness(&venue, config);
    connect(&mut harness).await;
    submit(
        &harness,
        &order("gross-held-nvda", NVDA, "0.05", false, false),
    );
    settled(&mut harness, "gross-held-nvda", "0.05").await;
    ready(&mut harness).await;
    let at = now();
    harness
        .cache
        .borrow_mut()
        .add_quote(QuoteTick::new(
            InstrumentId::from(NVDA),
            Price::from("400.00"),
            Price::from("401.00"),
            Quantity::from("10.00"),
            Quantity::from("10.00"),
            at,
            at,
        ))
        .unwrap();
    submit(
        &harness,
        &order("gross-refused-tsla", TSLA, "0.05", false, false),
    );
    let began = Instant::now();
    loop {
        let event = tokio::time::timeout(WAIT, harness.receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let refused = match &event {
            ExecutionEvent::Order(nautilus_model::events::OrderEventAny::Denied(e)) => {
                e.client_order_id == ClientOrderId::from("gross-refused-tsla")
            }
            ExecutionEvent::Order(nautilus_model::events::OrderEventAny::Rejected(e)) => {
                e.client_order_id == ClientOrderId::from("gross-refused-tsla")
            }
            _ => false,
        };
        harness.events.push(event);
        if refused {
            break;
        }
        assert!(began.elapsed() < WAIT, "risk refusal must reach the engine");
    }
    assert_eq!(
        venue.writes().len(),
        1,
        "higher current NVDA mark prevents TSLA risk before POST"
    );
    submit(
        &harness,
        &order("gross-close-nvda", NVDA, "0.05", true, false),
    );
    settled(&mut harness, "gross-close-nvda", "0.05").await;
    ready(&mut harness).await;
    harness.client.disconnect().await.unwrap();
}
