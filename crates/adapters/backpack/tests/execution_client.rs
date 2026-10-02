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

//! Native lifecycle acceptance using signed HTTP and WS loopback peers; all data is synthetic.
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{any, get},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signature, SigningKey};
use nautilus_backpack::{
    account::{
        BackpackAccountError, pagination::BackpackReadBudget, reconciliation::BackpackFillKey,
    },
    common::{credential::BackpackCredential, endpoints::BackpackEndpoints},
    config::BackpackConfig,
    execution_client::{
        BackpackAccountState, BackpackExecutionClient, BackpackExecutionClientConfig,
        BackpackExecutionClientFactory, BackpackExecutionPolicy, BackpackFillAcknowledgement,
    },
    http::quota::BackpackQuota,
    identity::{BackpackClientIdNamespace, BackpackClientIdStore, BackpackSubmissionIntent},
};
use nautilus_common::{
    cache::{Cache, CacheView},
    clients::ExecutionClient,
    factories::ExecutionClientFactory,
    messages::{ExecutionEvent, ExecutionReport, execution::*},
};
use nautilus_core::{UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    enums::{OrderStatus, PositionSide},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId},
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
fn fixture(name: &str) -> Value {
    serde_json::from_str::<Value>(include_str!("../test_data/account/synthetic.json")).unwrap()
        [name]
        .clone()
}
fn trader() -> TraderId {
    TraderId::from("TRADER-001")
}
fn instrument() -> InstrumentId {
    InstrumentId::from("BTC_USDC_PERP.BACKPACK")
}
fn timestamp() -> u64 {
    get_atomic_clock_realtime().get_time_ns().as_u64() / 1000 - 1000
}
fn order_frame(event: &str, trade: i64) -> Value {
    let fill = event == "orderFill";
    let mut data = json!({"e":event,"E":timestamp(),"T":timestamp(),"s":"BTC_USDC_PERP","c":1,"S":"Bid",
        "o":"LIMIT","f":"GTC","q":"0.00002","p":"100.1","r":false,"X":if fill {"PartiallyFilled"}else{"New"},
        "i":"synthetic-order-A","z":if fill {"0.00001"}else{"0"},"Z":if fill {"0.001001"}else{"0"},
        "V":"RejectTaker","O":"USER","y":true,"t":null});
    if fill {
        data["t"] = json!(trade);
        data["l"] = json!("0.00001");
        data["L"] = json!("100.1");
        data["m"] = json!(true);
        data["n"] = json!("-0.000001");
        data["N"] = json!("USDC");
    }
    json!({"stream":"account.orderUpdate.BTC_USDC_PERP","data":data})
}
fn balance_frame(available: &str) -> Value {
    json!({"stream":"account.balanceUpdate","data":{"e":"balanceUpdate","E":timestamp(),"T":timestamp(),"a":"USDC","A":available,"L":"10","S":"5"}})
}
fn position_frame(event: Option<&str>, quantity: &str) -> Value {
    let mut data = json!({"E":timestamp(),"T":timestamp(),"s":"BTC_USDC_PERP","b":"100.1","B":"100.1","f":"0.1","M":"100","m":"0.05","q":quantity,"Q":"0.00001","n":"0.001001","i":"synthetic-position-A","p":"0","P":"0.000001"});
    if let Some(event) = event {
        data["e"] = json!(event);
    }
    json!({"stream":"account.positionUpdate","data":data})
}
#[derive(Clone, Debug)]
enum PeerCommand {
    Text(String),
    Close,
    Ping,
}
#[derive(Debug, Default)]
struct PeerState {
    requests: Mutex<Vec<(String, String, bool)>>,
    subscriptions: Mutex<Vec<(bool, u64)>>,
    senders: Mutex<Vec<mpsc::UnboundedSender<PeerCommand>>>,
    pongs: AtomicUsize,
    capital_requests: AtomicUsize,
    capital_delay_ms: AtomicU64,
    bad_history: AtomicBool,
    public_senders: Mutex<Vec<mpsc::UnboundedSender<PeerCommand>>>,
    guarded: AtomicBool,
    post_hold: AtomicBool,
    post_notify: tokio::sync::Notify,
    post_bodies: Mutex<Vec<Value>>,
    cancel_pending: AtomicBool,
    lost_post: AtomicBool,
    fills: Mutex<Vec<Value>>,
    positions: Mutex<Vec<Value>>,
    resting_order: Mutex<Option<Value>>,
    order_queries: Mutex<Vec<(String, bool)>>,
}
#[derive(Debug)]
struct Peer {
    endpoints: BackpackEndpoints,
    state: Arc<PeerState>,
    task: JoinHandle<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Peer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoints = BackpackEndpoints::loopback_override(
            &format!("http://{addr}"),
            &format!("ws://{addr}"),
        )
        .unwrap();
        let state = Arc::new(PeerState::default());
        let app = Router::new()
            .route("/", get(upgrade))
            .fallback(any(rest))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            endpoints,
            state,
            task,
        }
    }
    fn send(&self, value: &Value) {
        self.raw(value.to_string());
    }
    fn raw(&self, value: String) {
        self.state
            .senders
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .send(PeerCommand::Text(value))
            .unwrap();
    }
    fn close(&self) {
        self.state
            .senders
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .send(PeerCommand::Close)
            .unwrap();
    }
}
async fn upgrade(State(state): State<Arc<PeerState>>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| private_peer(socket, state))
}
async fn private_peer(mut socket: WebSocket, state: Arc<PeerState>) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    state.senders.lock().unwrap().push(tx.clone());
    loop {
        tokio::select! {
            message=socket.recv()=>match message {
                Some(Ok(Message::Text(text)))=>{
                    let Ok(value)=serde_json::from_str::<Value>(&text) else {break;};
                    if value.get("signature").is_none() {
                        state.public_senders.lock().unwrap().push(tx.clone());
                        continue;
                    }
                    let signed=&value["signature"];
                    let ts=signed[2].as_str().unwrap().parse::<u64>().unwrap();let window=signed[3].as_str().unwrap().parse::<u64>().unwrap();
                    let signature=STANDARD.decode(signed[1].as_str().unwrap()).unwrap();let signature=Signature::from_slice(&signature).unwrap();
                    // Public deterministic test seed; never real venue credentials.
                    let key=SigningKey::from_bytes(&[7;32]).verifying_key();
                    let verified=key.verify_strict(format!("instruction=subscribe&timestamp={ts}&window={window}").as_bytes(),&signature).is_ok()
                        && signed[0]==STANDARD.encode(key.as_bytes()) && value["method"]=="SUBSCRIBE"
                        && value["params"]==json!(["account.balanceUpdate","account.orderUpdate","account.positionUpdate"]);
                    state.subscriptions.lock().unwrap().push((verified,ts));
                }
                Some(Ok(Message::Pong(_)))=>{state.pongs.fetch_add(1,Ordering::Relaxed);}
                Some(Ok(Message::Ping(bytes)))=>{let _=socket.send(Message::Pong(bytes)).await;}
                Some(Ok(Message::Close(_)) | Err(_)) | None=>break,
                _=>{},
            },
            command=rx.recv()=>match command {
                Some(PeerCommand::Text(text))=>if socket.send(Message::Text(text.into())).await.is_err(){break;},
                Some(PeerCommand::Close)=>{let _=socket.send(Message::Close(None)).await;break;}
                Some(PeerCommand::Ping)=>{let _=socket.send(Message::Ping(vec![1,2,3].into())).await;}
                None=>break,
            }
        }
    }
}
async fn rest(State(state): State<Arc<PeerState>>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    let authenticated = request.headers().contains_key("x-signature")
        && request.headers().contains_key("x-api-key");
    state.requests.lock().unwrap().push((
        request.method().to_string(),
        path.clone(),
        authenticated,
    ));
    if state.guarded.load(Ordering::Relaxed) && path == "/api/v1/order" {
        if request.method() == "GET" {
            let query = request.uri().query().unwrap_or_default().to_string();
            let headers = request.headers();
            let ts = headers["x-timestamp"].to_str().unwrap();
            let window = headers["x-window"].to_str().unwrap();
            let signature = Signature::from_slice(
                &STANDARD
                    .decode(headers["x-signature"].to_str().unwrap())
                    .unwrap(),
            )
            .unwrap();
            let key = SigningKey::from_bytes(&[7; 32]).verifying_key();
            let verified = key
                .verify_strict(
                    format!("instruction=orderQuery&{query}&timestamp={ts}&window={window}")
                        .as_bytes(),
                    &signature,
                )
                .is_ok()
                && headers["x-api-key"].to_str().unwrap() == STANDARD.encode(key.as_bytes());
            state.order_queries.lock().unwrap().push((query, verified));
            if let Some(order) = state.resting_order.lock().unwrap().clone() {
                return Json(order).into_response();
            }
        }
        if request.method() == "POST" {
            let bytes = axum::body::to_bytes(request.into_body(), 4096)
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            state.post_bodies.lock().unwrap().push(body.clone());
            if state.post_hold.load(Ordering::Relaxed) {
                state.post_notify.notified().await;
            }
            if state.lost_post.load(Ordering::Relaxed) {
                return Json(json!({"unknown":true})).into_response();
            }
            let mut response = body;
            response["id"] = json!(if state.post_bodies.lock().unwrap().len() == 1 {
                "synthetic-order-A"
            } else {
                "synthetic-order-B"
            });
            response["status"] = json!("New");
            response["executedQuantity"] = json!("0");
            response["executedQuoteQuantity"] = json!("0");
            response["createdAt"] = json!(0);
            response["selfTradePrevention"] = json!("RejectTaker");
            return Json(response).into_response();
        }
        if request.method() == "DELETE" && state.cancel_pending.load(Ordering::Relaxed) {
            return StatusCode::ACCEPTED.into_response();
        }
    }
    let body =
        match path.as_str() {
            "/api/v1/markets" => json!([serde_json::from_str::<Value>(include_str!(
                "../test_data/btc_usdc_perp.json"
            ))
            .unwrap()]),
            "/api/v1/account" => fixture("policy"),
            "/api/v1/capital" => {
                state.capital_requests.fetch_add(1, Ordering::Relaxed);
                let delay = state.capital_delay_ms.load(Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                fixture("balances")
            }
            "/api/v1/capital/collateral" => fixture("collateral"),
            "/api/v1/position" => json!(state.positions.lock().unwrap().clone()),
            "/api/v1/orders" => {
                if state.guarded.load(Ordering::Relaxed) {
                    json!([])
                } else {
                    json!([fixture("resting_order")])
                }
            }
            "/api/v1/order" => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"code":"ORDER_NOT_FOUND"})),
                )
                    .into_response();
            }
            "/wapi/v1/history/fills" => json!(state.fills.lock().unwrap().clone()),
            "/wapi/v1/history/orders" => {
                if state.guarded.load(Ordering::Relaxed) {
                    json!([])
                } else {
                    json!([fixture("history_order")])
                }
            }
            _ => return StatusCode::NOT_FOUND.into_response(),
        };
    let count = body.as_array().map_or(0, Vec::len);
    let mut response = Json(body).into_response();
    if path.contains("/history/") && !state.bad_history.load(Ordering::Relaxed) {
        for (name, value) in [
            ("x-page-count", usize::from(count > 0)),
            ("x-current-page", 0),
            ("x-page-size", 10),
            ("x-total", count),
        ] {
            response
                .headers_mut()
                .insert(name, value.to_string().parse().unwrap());
        }
    }
    response
}
fn config(peer: &Peer, directory: &TempDir) -> BackpackExecutionClientConfig {
    BackpackExecutionClientConfig::new_read_only(
        BackpackConfig::with_endpoints_checked(
            vec!["BTC_USDC_PERP".into()],
            peer.endpoints.clone(),
        )
        .unwrap(),
        BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &peer.endpoints).unwrap(),
        AccountId::from("BACKPACK-SYNTHETIC"),
        BackpackClientIdNamespace::new_checked("loopback", "101", Some("2")).unwrap(),
        directory.path().join("identity"),
        BackpackExecutionPolicy {
            connect_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_millis(300),
            recovery_interval: Duration::from_secs(30),
            ..Default::default()
        },
        BackpackReadBudget::new(10, 10, 100, Duration::from_secs(3)).unwrap(),
        BackpackQuota::default(),
    )
    .unwrap()
}
fn cache() -> CacheView {
    CacheView::new(Rc::new(RefCell::new(Cache::default())))
}
fn client(
    config: BackpackExecutionClientConfig,
) -> (
    BackpackExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
) {
    let mut client = BackpackExecutionClient::new(trader(), "BACKPACK", config, cache()).unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    client.set_event_sender(tx);
    (client, rx)
}
async fn until(predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
fn drain(rx: &mut mpsc::UnboundedReceiver<ExecutionEvent>) -> Vec<ExecutionEvent> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}
fn fills(events: Vec<ExecutionEvent>) -> Vec<nautilus_model::reports::FillReport> {
    events
        .into_iter()
        .filter_map(|event| match event {
            ExecutionEvent::Report(ExecutionReport::Fill(fill)) => Some(*fill),
            _ => None,
        })
        .collect()
}
#[tokio::test]
async fn test_native_factory_configuration_claims_and_no_constructor_network() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let config = config(&peer, &directory);
    assert!(!config.identity_directory().exists());
    assert!(peer.state.requests.lock().unwrap().is_empty());
    let telemetry = config.telemetry();
    let factory = BackpackExecutionClientFactory;
    let instance = factory
        .create(trader(), "BACKPACK", &config, cache())
        .unwrap();
    let first = telemetry.snapshot().run_id.unwrap();
    assert_eq!(telemetry.snapshot().schema_version, 1);
    assert!(
        factory
            .create(trader(), "BACKPACK", &config, cache())
            .is_err()
    );
    assert!(peer.state.requests.lock().unwrap().is_empty());
    assert!(!instance.provides_bulk_position_coverage(instrument()));
    drop(instance);
    let second = factory
        .create(trader(), "BACKPACK", &config, cache())
        .unwrap();
    assert!(telemetry.snapshot().run_id.unwrap() > first);
    assert_eq!(telemetry.snapshot().generation, 0);
    assert_eq!(telemetry.snapshot().state, BackpackAccountState::Stopped);
    serde_json::to_value(telemetry.snapshot()).unwrap();
    drop(second);
}
#[tokio::test]
async fn test_production_read_only_configuration_is_explicit_and_performs_no_io() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("unopened");
    let cfg = BackpackExecutionClientConfig::new_read_only(
        BackpackConfig::new_checked(vec!["BTC_USDC_PERP".into()]).unwrap(),
        BackpackCredential::production(&STANDARD.encode([7; 32])).unwrap(),
        AccountId::from("BACKPACK-SYNTHETIC"),
        BackpackClientIdNamespace::new_checked("production", "101", None).unwrap(),
        path.clone(),
        BackpackExecutionPolicy::default(),
        BackpackReadBudget::new(10, 10, 100, Duration::from_secs(3)).unwrap(),
        BackpackQuota::default(),
    )
    .unwrap();
    assert!(!cfg.scope().endpoints().is_loopback());
    assert!(!path.exists());
    assert!(!format!("{cfg:?}").contains(&STANDARD.encode([7; 32])));
}
#[tokio::test]
async fn test_native_connect_rest_reports_signed_private_stream_and_bounded_repeated_stop() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let (mut client, mut rx) = client(config(&peer, &directory));
    client.start().unwrap();
    client.connect().await.unwrap();
    until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
    assert!(peer.state.subscriptions.lock().unwrap()[0].0);
    let health = client.health();
    assert!(health.transport_connected);
    assert!(health.rest_snapshot_observed);
    assert_eq!(health.state, BackpackAccountState::Degraded);
    assert!(!health.private_subscription_confirmed);
    assert!(health.evidence_gaps.contains("AccountIdentityUnverified"));
    let initial = drain(&mut rx);
    assert!(
        initial
            .iter()
            .any(|event| matches!(event, ExecutionEvent::Account(_)))
    );
    assert!(
        !initial
            .iter()
            .any(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Position(_))))
    );
    let open = client
        .generate_order_status_reports(
            &GenerateOrderStatusReportsBuilder::default()
                .ts_init(UnixNanos::from(0))
                .open_only(true)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].client_order_id, None);
    let history = client
        .generate_order_status_reports(
            &GenerateOrderStatusReportsBuilder::default()
                .ts_init(UnixNanos::from(0))
                .open_only(false)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(history.is_empty());
    assert!(client.health().evidence_gaps.contains("UnknownVenueState"));
    let positions = client
        .generate_position_status_reports(
            &GeneratePositionStatusReportsBuilder::default()
                .ts_init(UnixNanos::from(0))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(positions.is_empty());
    let single = GenerateOrderStatusReportBuilder::default()
        .ts_init(UnixNanos::from(0))
        .instrument_id(Some(instrument()))
        .venue_order_id(Some(VenueOrderId::from("missing")))
        .build()
        .unwrap();
    assert!(
        client
            .generate_order_status_report(&single)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        client
            .health()
            .evidence_gaps
            .contains("RestingAbsenceIsUnknown")
    );
    assert!(client.generate_mass_status(None).await.is_err());
    peer.send(&json!({"result":null,"id":1}));
    peer.send(&balance_frame("111"));
    until(|| {
        client
            .health()
            .observed_topics
            .contains("account.balanceUpdate")
    })
    .await;
    let accounts: Vec<_> = drain(&mut rx)
        .into_iter()
        .filter_map(|e| match e {
            ExecutionEvent::Account(account) => Some(account),
            _ => None,
        })
        .collect();
    assert!(
        accounts
            .iter()
            .any(|a| a.balances[0].free.as_decimal() == Decimal::from(111))
    );
    assert!(!client.health().private_subscription_confirmed);
    peer.state
        .senders
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(PeerCommand::Ping)
        .unwrap();
    until(|| peer.state.pongs.load(Ordering::Relaxed) > 0).await;
    client.stop().unwrap();
    client.stop().unwrap();
    tokio::time::timeout(Duration::from_secs(2), client.disconnect())
        .await
        .unwrap()
        .unwrap();
    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
    assert_eq!(client.health().state, BackpackAccountState::Stopped);
    let requests = peer.state.requests.lock().unwrap();
    assert!(requests.iter().all(|(method, _, _)| method == "GET"));
    assert!(
        requests
            .iter()
            .filter(|(_, path, _)| path != "/api/v1/markets")
            .all(|(_, _, signed)| *signed)
    );
}
#[tokio::test]
async fn test_private_true_fills_replay_until_consumer_ack_and_shared_rest_dedup() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let config = config(&peer, &directory);
    {
        let mut store = BackpackClientIdStore::open(
            config.identity_directory(),
            &BackpackClientIdNamespace::new_checked("loopback", "101", Some("2")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            store
                .reserve_intent(
                    BackpackSubmissionIntent::new_checked(
                        ClientOrderId::from("LOCAL-COLLISION"),
                        "{}".into()
                    )
                    .unwrap()
                )
                .unwrap(),
            1
        );
    }
    let (mut client, mut rx) = client(config);
    client.connect().await.unwrap();
    until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
    drain(&mut rx);
    let frame = order_frame("orderFill", 9_007_199_254_740_993);
    peer.send(&frame);
    until(|| client.health().pending_fills == 1).await;
    let first = fills(drain(&mut rx));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].client_order_id, None);
    assert_eq!(
        first[0].commission.as_decimal(),
        Decimal::from_str_exact("-0.000001").unwrap()
    );
    assert_eq!(first[0].trade_id.to_string(), "9007199254740993");
    // Two-phase receipts reject unknown/forged state and never clear pending evidence.
    let mut staged =
        nautilus_backpack::account::reconciliation::BackpackFillReconciler::from_applied(10, [])
            .unwrap();
    staged.stage(first[0].clone()).unwrap();
    let staged_key = BackpackFillKey {
        instrument_id: first[0].instrument_id,
        trade_id: first[0].trade_id,
    };
    let receipt = staged.pending_acknowledgement(staged_key).unwrap();
    let mut forged = receipt.clone();
    forged.fingerprint = "0".repeat(64);
    assert!(matches!(
        staged.acknowledge_committed(&forged),
        Err(BackpackAccountError::Conflict)
    ));
    let mut unknown = receipt.clone();
    unknown.key.trade_id = nautilus_model::identifiers::TradeId::from("unknown-trade");
    assert!(matches!(
        staged.acknowledge_committed(&unknown),
        Err(BackpackAccountError::UnknownAcknowledgement)
    ));
    assert_eq!(staged.pending_in_event_order().len(), 1);
    assert_eq!(staged.applied_records().count(), 0);
    staged.acknowledge_committed(&receipt).unwrap();
    staged.acknowledge_committed(&receipt).unwrap();
    assert_eq!(staged.applied_records().count(), 1);
    assert!(staged.pending_in_event_order().is_empty());
    assert!(matches!(
        staged.acknowledge_committed(&forged),
        Err(BackpackAccountError::Conflict)
    ));
    let key = BackpackFillKey {
        instrument_id: first[0].instrument_id,
        trade_id: first[0].trade_id,
    };
    assert!(
        client
            .acknowledge_fill_with(key, |_| Err(BackpackAccountError::Acknowledgement))
            .is_err()
    );
    peer.send(&frame);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(fills(drain(&mut rx)).len(), 1);
    let mut rest_fill = fixture("fill");
    rest_fill["timestamp"] = json!(
        jiff::Timestamp::from_microsecond(frame["data"]["T"].as_i64().unwrap())
            .unwrap()
            .to_zoned(jiff::tz::TimeZone::UTC)
            .datetime()
            .to_string()
    );
    peer.state.fills.lock().unwrap().push(rest_fill);
    let command = GenerateFillReportsBuilder::default()
        .ts_init(UnixNanos::from(0))
        .build()
        .unwrap();
    assert_eq!(
        client
            .generate_fill_reports(command.clone())
            .await
            .unwrap()
            .len(),
        1
    );
    client.acknowledge_fill_with(key, |_| Ok(())).unwrap();
    assert_eq!(client.health().pending_fills, 0);
    peer.send(&frame);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(fills(drain(&mut rx)).is_empty());
    assert!(
        client
            .generate_fill_reports(command)
            .await
            .unwrap()
            .is_empty()
    );
    let mut conflict = frame;
    conflict["data"]["n"] = json!("0.2");
    peer.send(&conflict);
    until(|| client.health().parse_failures > 0).await;
    assert!(
        client
            .health()
            .evidence_gaps
            .contains("PrivateParseOrDeliveryFailure")
    );
    client.disconnect().await.unwrap();
}
#[tokio::test]
async fn test_private_initial_explicit_positions_and_order_transitions() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let (mut client, mut rx) = client(config(&peer, &directory));
    client.connect().await.unwrap();
    until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
    drain(&mut rx);
    peer.send(&position_frame(None, "-0.00001"));
    peer.send(&position_frame(Some("positionClosed"), "0"));
    peer.send(&order_frame("orderAccepted", 0));
    let mut cancel = order_frame("orderCancelled", 0);
    cancel["data"]["X"] = json!("Cancelled");
    peer.send(&cancel);
    until(|| client.health().observed_topics.len() == 2).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let events = drain(&mut rx);
    let positions: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            ExecutionEvent::Report(ExecutionReport::Position(p)) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(positions.len(), 2);
    assert_eq!(positions[0].position_side, PositionSide::Short);
    assert_eq!(positions[1].position_side, PositionSide::Flat);
    let orders: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            ExecutionEvent::Report(ExecutionReport::Order(o)) => Some(o),
            _ => None,
        })
        .collect();
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[0].order_status, OrderStatus::Accepted);
    assert_eq!(orders[1].order_status, OrderStatus::Canceled);
    let mut invalid = position_frame(Some("positionClosed"), "1");
    invalid["data"]["T"] = json!(u64::MAX);
    peer.send(&invalid);
    until(|| client.health().parse_failures > 0).await;
    assert_eq!(client.health().state, BackpackAccountState::Degraded);
    client.disconnect().await.unwrap();
}
#[tokio::test]
async fn test_reconnect_resigns_replays_pending_and_new_run_does_not_inherit_observed_coverage() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let config = config(&peer, &directory);
    let (mut client, mut rx) = client(config.clone());
    client.connect().await.unwrap();
    until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
    drain(&mut rx);
    peer.send(&balance_frame("99"));
    until(|| !client.health().observed_topics.is_empty()).await;
    let generation = client.health().generation;
    peer.close();
    until(|| peer.state.subscriptions.lock().unwrap().len() >= 2).await;
    until(|| client.health().connection_epoch == Some(1)).await;
    assert_eq!(client.health().generation, generation);
    assert!(
        peer.state
            .subscriptions
            .lock()
            .unwrap()
            .iter()
            .all(|(valid, _)| *valid)
    );
    assert!(client.health().observed_topics.is_empty());
    assert!(!client.health().private_subscription_confirmed);
    assert!(
        client
            .health()
            .evidence_gaps
            .contains("PrivateConnectionLost")
    );
    client.disconnect().await.unwrap();
    client.connect().await.unwrap();
    assert!(client.health().generation > generation);
    assert!(client.health().observed_topics.is_empty());
    client.disconnect().await.unwrap();
}
#[tokio::test]
async fn test_parse_fault_fences_late_rest_bootstrap_and_query_is_owned() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let (mut client, mut rx) = client(config(&peer, &directory));
    client.connect().await.unwrap();
    until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
    drain(&mut rx);
    peer.state.capital_delay_ms.store(200, Ordering::Relaxed);
    client
        .query_account(QueryAccount::new(
            trader(),
            None,
            AccountId::from("BACKPACK-SYNTHETIC"),
            UUID4::new(),
            UnixNanos::from(0),
            None,
            None,
        ))
        .unwrap();
    until(|| peer.state.capital_requests.load(Ordering::Relaxed) >= 2).await;
    peer.raw("{\"data\":\"PRIVATE_SENTINEL\"}".into());
    until(|| client.health().parse_failures > 0).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!client.health().rest_snapshot_observed);
    assert!(
        client
            .health()
            .evidence_gaps
            .contains("AccountQueryIncomplete")
    );
    assert!(
        !drain(&mut rx)
            .iter()
            .any(|e| matches!(e, ExecutionEvent::Account(_)))
    );
    let safe = serde_json::to_string(&client.health()).unwrap();
    assert!(!safe.contains("PRIVATE_SENTINEL"));
    client.disconnect().await.unwrap();
}
#[tokio::test]
async fn test_invalid_history_prevents_successful_connect_and_stop_is_still_bounded() {
    let peer = Peer::start().await;
    peer.state.bad_history.store(true, Ordering::Relaxed);
    let directory = TempDir::new().unwrap();
    let (mut client, _rx) = client(config(&peer, &directory));
    let error = client.connect().await.unwrap_err().to_string();
    assert!(!error.contains("signature"));
    assert!(!client.is_connected());
    assert!(peer.state.subscriptions.lock().unwrap().is_empty());
    client.stop().unwrap();
    client.disconnect().await.unwrap();
}
#[tokio::test]
async fn test_read_only_cancel_modify_and_empty_batch_methods_refuse_without_io() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let (client, _rx) = client(config(&peer, &directory));
    let cancel = CancelOrder::new(
        trader(),
        None,
        StrategyId::from("S-001"),
        instrument(),
        ClientOrderId::from("LOCAL"),
        None,
        UUID4::new(),
        UnixNanos::from(0),
        None,
        None,
    );
    assert!(client.cancel_order(cancel.clone()).is_err());
    assert!(
        client
            .cancel_all_orders(CancelAllOrders::new(
                trader(),
                None,
                StrategyId::from("S-001"),
                instrument(),
                None,
                UUID4::new(),
                UnixNanos::from(0),
                None,
                None
            ))
            .is_err()
    );
    assert!(
        client
            .batch_cancel_orders(BatchCancelOrders::new(
                trader(),
                None,
                StrategyId::from("S-001"),
                instrument(),
                vec![cancel],
                UUID4::new(),
                UnixNanos::from(0),
                None,
                None
            ))
            .is_err()
    );
    assert!(
        client
            .modify_order(ModifyOrder::new(
                trader(),
                None,
                StrategyId::from("S-001"),
                instrument(),
                ClientOrderId::from("LOCAL"),
                None,
                None,
                None,
                None,
                UUID4::new(),
                UnixNanos::from(0),
                None,
                None
            ))
            .is_err()
    );
    assert!(client.batch_modify_orders(serde_json::from_value(json!({"trader_id":"TRADER-001","client_id":null,"strategy_id":"S-001","instrument_id":"BTC_USDC_PERP.BACKPACK","modifies":[],"command_id":UUID4::new(),"ts_init":0,"params":null,"correlation_id":null})).unwrap()).is_err());
    assert!(peer.state.requests.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ack_callback_reentry_health_and_recovery_never_hold_adapter_locks() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let (mut client, mut rx) = client(config(&peer, &directory));
    client.connect().await.unwrap();
    until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
    drain(&mut rx);
    let frame = order_frame("orderFill", 77);
    peer.send(&frame);
    until(|| client.health().pending_fills == 1).await;
    let delivery = client.fill_delivery();
    let callback_delivery = delivery.clone();
    let telemetry = client.telemetry();
    let pending = delivery.pending();
    let key = BackpackFillKey {
        instrument_id: pending[0].instrument_id,
        trade_id: pending[0].trade_id,
    };
    let callback_count = Arc::new(AtomicUsize::new(0));
    let count = callback_count.clone();
    let acknowledged = tokio::task::spawn_blocking(move || {
        delivery.acknowledge_with(key, |_| {
            // Reentry observes pending/applications and health with no adapter lock held.
            assert_eq!(callback_delivery.pending().len(), 1);
            assert!(callback_delivery.applied().is_empty());
            assert_eq!(telemetry.snapshot().schema_version, 1);
            std::thread::sleep(Duration::from_millis(10));
            count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    });
    client
        .query_account(QueryAccount::new(
            trader(),
            None,
            AccountId::from("BACKPACK-SYNTHETIC"),
            UUID4::new(),
            UnixNanos::from(0),
            None,
            None,
        ))
        .unwrap();
    for _ in 0..20 {
        peer.send(&frame);
        tokio::task::yield_now().await;
    }
    tokio::time::timeout(Duration::from_secs(2), acknowledged)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(callback_count.load(Ordering::Relaxed), 1);
    assert_eq!(client.fill_delivery().applied().len(), 1);
    assert_eq!(
        client
            .acknowledge_fill_with(key, |_| {
                callback_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap(),
        BackpackFillAcknowledgement::AlreadyApplied
    );
    assert_eq!(callback_count.load(Ordering::Relaxed), 1);
    client.disconnect().await.unwrap();
}
#[tokio::test]
async fn test_wrong_venue_and_unsupported_instrument_read_requests_are_rejected_before_io() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let (mut client, _rx) = client(config(&peer, &directory));
    client.connect().await.unwrap();
    let before = peer.state.requests.lock().unwrap().len();
    for id in [
        InstrumentId::from("BTC_USDC_PERP.OTHER"),
        InstrumentId::from("ETH_USDC_PERP.BACKPACK"),
    ] {
        let orders = GenerateOrderStatusReportsBuilder::default()
            .ts_init(UnixNanos::from(0))
            .open_only(true)
            .instrument_id(Some(id))
            .build()
            .unwrap();
        assert!(client.generate_order_status_reports(&orders).await.is_err());
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(UnixNanos::from(0))
            .instrument_id(Some(id))
            .build()
            .unwrap();
        assert!(
            client
                .generate_position_status_reports(&positions)
                .await
                .is_err()
        );
        let fills = GenerateFillReportsBuilder::default()
            .ts_init(UnixNanos::from(0))
            .instrument_id(Some(id))
            .build()
            .unwrap();
        assert!(client.generate_fill_reports(fills).await.is_err());
    }
    assert_eq!(peer.state.requests.lock().unwrap().len(), before);
    client.disconnect().await.unwrap();
}
struct ParserScope {
    provider: nautilus_backpack::provider::BackpackInstrumentProvider,
    identities: BackpackClientIdStore,
    _directory: TempDir,
}
impl ParserScope {
    fn new() -> Self {
        let mut provider = nautilus_backpack::provider::BackpackInstrumentProvider::new(
            BackpackConfig::new_checked(vec!["BTC_USDC_PERP".into()]).unwrap(),
        );
        let market = serde_json::from_str(include_str!("../test_data/btc_usdc_perp.json")).unwrap();
        provider
            .replace_markets(&[market], UnixNanos::from(1))
            .unwrap();
        let directory = TempDir::new().unwrap();
        let mut identities = BackpackClientIdStore::open(
            directory.path(),
            &BackpackClientIdNamespace::new_checked("synthetic", "101", None).unwrap(),
        )
        .unwrap();
        identities
            .reserve_intent(
                BackpackSubmissionIntent::new_checked(
                    ClientOrderId::from("COLLIDING-LOCAL-ID"),
                    "{}".into(),
                )
                .unwrap(),
            )
            .unwrap();
        Self {
            provider,
            identities,
            _directory: directory,
        }
    }
    fn context(&self) -> nautilus_backpack::account::reports::BackpackReportContext<'_> {
        nautilus_backpack::account::reports::BackpackReportContext {
            account_id: AccountId::from("BACKPACK-SYNTHETIC"),
            instruments: &self.provider,
            identities: &self.identities,
            confirmed_orders: None,
            ts_init: UnixNanos::from(1),
        }
    }
}
#[rstest::rstest]
#[case("l")]
#[case("n")]
#[case("z")]
#[case("s")]
#[case("p")]
#[case("t")]
#[case("X")]
#[case("q")]
fn test_private_duplicate_economic_or_identity_keys_are_rejected(#[case] key: &str) {
    let scope = ParserScope::new();
    let frame = order_frame("orderFill", 77);
    let duplicate = frame.to_string().replacen(
        "\"data\":{",
        &format!("\"data\":{{\"{key}\":\"PRIVATE_SENTINEL\","),
        1,
    );
    let error = nautilus_backpack::execution_client::private::decode_private(
        duplicate.as_bytes(),
        &scope.context(),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "invalid Backpack account response shape");
    assert!(!error.to_string().contains("PRIVATE_SENTINEL"));
}
#[test]
fn test_private_duplicate_unknown_nested_keys_cannot_hide_conditional_fields() {
    let scope = ParserScope::new();
    let frame = order_frame("orderFill", 77);
    let duplicate =
        frame
            .to_string()
            .replacen("\"data\":{", "\"data\":{\"future\":{\"x\":1,\"x\":2},", 1);
    assert!(
        nautilus_backpack::execution_client::private::decode_private(
            duplicate.as_bytes(),
            &scope.context()
        )
        .is_err()
    );
}
#[rstest::rstest]
#[case("l",json!("0"))]
#[case("l",json!("-0.00001"))]
#[case("l",json!("0.00002"))]
#[case("z",json!("0"))]
#[case("z",json!("0.00003"))]
#[case("X",json!("New"))]
#[case("X",json!("Filled"))]
#[case("q",json!("0.00001"))]
#[case("t", Value::Null)]
#[case("n", Value::Null)]
#[case("T",json!(u64::MAX))]
fn test_private_fill_internal_contradictions_never_produce_native_facts(
    #[case] key: &str,
    #[case] value: Value,
) {
    let scope = ParserScope::new();
    let mut frame = order_frame("orderFill", 77);
    frame["data"][key] = value;
    assert!(
        nautilus_backpack::execution_client::private::decode_private(
            frame.to_string().as_bytes(),
            &scope.context()
        )
        .is_err()
    );
}
#[test]
fn test_private_system_late_fill_is_exact_and_never_owned_by_colliding_client_id() {
    use nautilus_backpack::execution_client::private::{BackpackPrivateFact, decode_private};
    let scope = ParserScope::new();
    let mut frame = order_frame("orderFill", 77);
    frame["data"]["O"] = json!("LIQUIDATION_AUTOCLOSE");
    frame["data"]["X"] = json!("Cancelled");
    let observation = decode_private(frame.to_string().as_bytes(), &scope.context()).unwrap();
    let fills: Vec<_> = observation
        .facts
        .into_iter()
        .filter_map(|fact| match fact {
            BackpackPrivateFact::Fill(fill) => Some(fill),
            _ => None,
        })
        .collect();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].client_order_id, None);
    assert_eq!(
        fills[0].last_qty.as_decimal(),
        Decimal::from_str_exact("0.00001").unwrap()
    );
}
#[test]
fn test_private_conditional_order_and_absent_post_only_are_not_invented_as_plain_limit() {
    use nautilus_backpack::execution_client::private::{BackpackPrivateFact, decode_private};
    let scope = ParserScope::new();
    let mut frame = order_frame("orderAccepted", 0);
    frame["data"]["P"] = json!("101");
    let conditional = decode_private(frame.to_string().as_bytes(), &scope.context()).unwrap();
    assert!(
        !conditional
            .facts
            .iter()
            .any(|f| matches!(f, BackpackPrivateFact::Order(_)))
    );
    frame["data"].as_object_mut().unwrap().remove("P");
    frame["data"].as_object_mut().unwrap().remove("y");
    let unconfirmed = decode_private(frame.to_string().as_bytes(), &scope.context()).unwrap();
    assert!(
        !unconfirmed
            .facts
            .iter()
            .any(|f| matches!(f, BackpackPrivateFact::Order(_)))
    );
}
#[test]
fn test_private_generic_ack_and_valid_empty_control_never_establish_success_or_flat() {
    let scope = ParserScope::new();
    let observation = nautilus_backpack::execution_client::private::decode_private(
        b"{\"id\":1,\"result\":null}",
        &scope.context(),
    )
    .unwrap();
    assert!(observation.topic.is_none());
    assert!(observation.facts.is_empty());
}

#[tokio::test]
async fn test_real_execution_engine_cache_receives_only_true_trade_ids_and_fees() {
    use nautilus_backpack::instruments::{BackpackEconomicsSource, BackpackInstrumentEconomics};
    use nautilus_common::clock::{Clock, TestClock};
    use nautilus_execution::engine::ExecutionEngine;
    use nautilus_model::{
        accounts::{AccountAny, MarginAccount},
        enums::{AccountType, OmsType},
        events::AccountState,
        instruments::InstrumentAny,
        orders::Order,
        types::{AccountBalance, Currency, Money},
    };
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let config = config(&peer, &directory);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let scope = ParserScope::new();
    let economics = BackpackInstrumentEconomics::new_checked(
        Decimal::from_str_exact("0.1").unwrap(),
        Decimal::from_str_exact("0.05").unwrap(),
        Decimal::ZERO,
        Decimal::ZERO,
        BackpackEconomicsSource::Synthetic,
        "offline engine cache regression only".into(),
    )
    .unwrap();
    let native = scope
        .provider
        .get(&instrument())
        .unwrap()
        .to_instrument(Some(&economics))
        .unwrap();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CryptoPerpetual(native))
        .unwrap();
    let account = AccountState::new(
        AccountId::from("BACKPACK-SYNTHETIC"),
        AccountType::Margin,
        vec![AccountBalance::new(
            Money::from("110 USDC"),
            Money::from("10 USDC"),
            Money::from("100 USDC"),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::from(0),
        UnixNanos::from(0),
        None,
    );
    cache
        .borrow_mut()
        .add_account(AccountAny::Margin(MarginAccount::new(account, false)))
        .unwrap();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
    let mut engine = ExecutionEngine::new(clock, cache.clone(), None);
    engine.register_oms_type(StrategyId::from("EXTERNAL"), OmsType::Netting);
    let mut client =
        BackpackExecutionClient::new(trader(), "BACKPACK", config, CacheView::new(cache.clone()))
            .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    client.set_event_sender(tx);
    client.connect().await.unwrap();
    until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
    for event in drain(&mut rx) {
        if let ExecutionEvent::Report(report) = event {
            engine.reconcile_execution_report(&report);
        }
    }
    // The resting partial cumulative snapshot is retained as DTO but cannot synthesize a fill.
    assert!(
        cache
            .borrow()
            .orders(None, None, None, None, None)
            .is_empty()
    );
    assert!(
        client
            .health()
            .evidence_gaps
            .contains("CumulativeOrderReportUnpublished")
    );
    peer.send(&order_frame("orderAccepted", 0));
    until(|| {
        client
            .health()
            .observed_topics
            .contains("account.orderUpdate.BTC_USDC_PERP")
    })
    .await;
    for event in drain(&mut rx) {
        if let ExecutionEvent::Report(report) = event {
            engine.reconcile_execution_report(&report);
        }
    }
    let order_id = cache
        .borrow()
        .client_order_id(&VenueOrderId::from("synthetic-order-A"))
        .copied()
        .unwrap();
    assert!(
        cache
            .borrow()
            .order(&order_id)
            .unwrap()
            .trade_ids()
            .is_empty()
    );
    let first = order_frame("orderFill", 77);
    peer.send(&first);
    until(|| client.health().pending_fills == 1).await;
    for event in drain(&mut rx) {
        if let ExecutionEvent::Report(report) = event {
            engine.reconcile_execution_report(&report);
        }
    }
    let mut second = order_frame("orderFill", 78);
    second["data"]["X"] = json!("Filled");
    second["data"]["z"] = json!("0.00002");
    second["data"]["Z"] = json!("0.002002");
    peer.send(&second);
    until(|| client.health().pending_fills == 2).await;
    for event in drain(&mut rx) {
        if let ExecutionEvent::Report(report) = event {
            engine.reconcile_execution_report(&report);
        }
    }
    {
        let cache = cache.borrow();
        let order = cache.order(&order_id).unwrap();
        assert_eq!(
            order.filled_qty().as_decimal(),
            Decimal::from_str_exact("0.00002").unwrap()
        );
        assert_eq!(
            order
                .trade_ids()
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>(),
            vec!["77", "78"]
        );
        assert_eq!(
            order
                .commissions()
                .get(&Currency::USDC())
                .unwrap()
                .as_decimal(),
            Decimal::from_str_exact("-0.000002").unwrap()
        );
    }
    // Adapter delivery remains pending: cache application is not a durable consumer ACK.
    assert_eq!(client.health().pending_fills, 2);
    peer.send(&second);
    tokio::time::sleep(Duration::from_millis(30)).await;
    for event in drain(&mut rx) {
        if let ExecutionEvent::Report(report) = event {
            engine.reconcile_execution_report(&report);
        }
    }
    assert_eq!(
        cache.borrow().order(&order_id).unwrap().trade_ids().len(),
        2
    );
    client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ack_lease_reentry_error_panic_and_duplicate_do_not_commit_twice() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let (mut client, _rx) = client(config(&peer, &directory));
    client.connect().await.unwrap();
    until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
    peer.send(&order_frame("orderFill", 77));
    until(|| client.health().pending_fills == 1).await;
    let delivery = client.fill_delivery();
    let report = delivery.pending()[0].clone();
    let key = BackpackFillKey {
        instrument_id: report.instrument_id,
        trade_id: report.trade_id,
    };
    assert!(matches!(
        delivery.acknowledge_with(key, |_| {
            assert_eq!(
                delivery
                    .acknowledge_with(key, |_| panic!("reentry invoked callback"))
                    .unwrap(),
                BackpackFillAcknowledgement::InProgress
            );
            Err(BackpackAccountError::Conflict)
        }),
        Err(BackpackAccountError::Conflict)
    ));
    assert_eq!(delivery.pending().len(), 1);
    assert!(delivery.applied().is_empty());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = delivery.acknowledge_with(key, |_| panic!("synthetic interrupted consumer"));
    }));
    assert!(panic.is_err());
    assert_eq!(delivery.pending().len(), 1);
    assert_eq!(
        delivery.acknowledge_with(key, |_| Ok(())).unwrap(),
        BackpackFillAcknowledgement::Applied
    );
    assert_eq!(
        delivery
            .acknowledge_with(key, |_| panic!("applied duplicate invoked callback"))
            .unwrap(),
        BackpackFillAcknowledgement::AlreadyApplied
    );
    assert_eq!(delivery.applied().len(), 1);
    assert_eq!(client.health().pending_fills, 0);
    client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ack_lease_concurrency_counts_new_pending_evidence() {
    let peer = Peer::start().await;
    let directory = TempDir::new().unwrap();
    let (mut client, _rx) = client(config(&peer, &directory));
    client.connect().await.unwrap();
    until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
    peer.send(&order_frame("orderFill", 77));
    until(|| client.health().pending_fills == 1).await;
    let delivery = client.fill_delivery();
    let report = delivery.pending()[0].clone();
    let key = BackpackFillKey {
        instrument_id: report.instrument_id,
        trade_id: report.trade_id,
    };
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let worker = delivery.clone();
    let ack = tokio::task::spawn_blocking(move || {
        worker.acknowledge_with(key, |_| {
            entered.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            Ok(())
        })
    });
    entered_rx.await.unwrap();
    assert_eq!(
        delivery
            .acknowledge_with(key, |_| panic!("concurrent same-key callback"))
            .unwrap(),
        BackpackFillAcknowledgement::InProgress
    );
    let mut second = order_frame("orderFill", 78);
    second["data"]["X"] = json!("Filled");
    second["data"]["z"] = json!("0.00002");
    second["data"]["Z"] = json!("0.002002");
    peer.send(&second);
    until(|| client.health().pending_fills == 2).await;
    release.send(()).unwrap();
    assert_eq!(
        ack.await.unwrap().unwrap(),
        BackpackFillAcknowledgement::Applied
    );
    assert_eq!(client.health().pending_fills, 1);
    assert_eq!(delivery.pending().len(), 1);
    assert_eq!(delivery.applied().len(), 1);
    client.disconnect().await.unwrap();
}

mod guarded {
    use std::collections::BTreeMap;

    use nautilus_backpack::{
        config::BackpackDataClientConfig,
        data::BackpackDataClient,
        execution::{
            guard::{BackpackExecutionAuthority, BackpackLoopbackAccountFacts},
            owner::BackpackMutationPolicy,
        },
        execution_client::restricted::BackpackLoopbackSession,
        instruments::{BackpackEconomicsSource, BackpackInstrumentEconomics},
    };
    use nautilus_common::{
        clients::DataClient,
        clock::{Clock, TestClock},
        live::runner::replace_data_event_sender,
        messages::{DataEvent, data::SubscribeQuotes},
    };
    use nautilus_execution::engine::ExecutionEngine;
    use nautilus_model::{
        accounts::{AccountAny, MarginAccount},
        data::Data,
        enums::{AccountType, OmsType, OrderSide, OrderType},
        events::AccountState,
        identifiers::ClientId,
        orders::{Order, OrderAny, OrderTestBuilder},
        types::{AccountBalance, Currency, Money, Price, Quantity},
    };
    use nautilus_portfolio::portfolio::Portfolio;

    use super::*;

    fn decimal(text: &str) -> Decimal {
        Decimal::from_str_exact(text).unwrap()
    }
    fn now_ns() -> UnixNanos {
        get_atomic_clock_realtime().get_time_ns()
    }
    struct Harness {
        peer: Peer,
        directory: TempDir,
        public: BackpackDataClient,
        public_config: BackpackDataClientConfig,
        client: BackpackExecutionClient,
        cache: Rc<RefCell<Cache>>,
        engine: ExecutionEngine,
        portfolio: Portfolio,
        rx: mpsc::UnboundedReceiver<ExecutionEvent>,
        data_rx: mpsc::UnboundedReceiver<DataEvent>,
        session: BackpackLoopbackSession,
        namespace: BackpackClientIdNamespace,
        config: BackpackExecutionClientConfig,
    }
    #[tokio::test]
    async fn test_attached_control_recovers_after_private_first_public_admission_failure() {
        use nautilus_backpack::execution_client::loopback_config::{
            BackpackLoopbackExecutionClientConfig, BackpackLoopbackExecutionClientFactory,
        };
        use nautilus_common::factories::ExecutionClientFactory;

        let peer = Peer::start().await;
        peer.state.guarded.store(true, Ordering::Relaxed);
        let directory = TempDir::new().unwrap();
        let scope = BackpackConfig::with_endpoints_checked(
            vec!["BTC_USDC_PERP".into()],
            peer.endpoints.clone(),
        )
        .unwrap();
        let economics = BackpackInstrumentEconomics::new_checked(
            decimal("0.1"),
            decimal("0.05"),
            Decimal::ZERO,
            Decimal::ZERO,
            BackpackEconomicsSource::Synthetic,
            "synthetic peer v1".into(),
        )
        .unwrap();
        let public_config = BackpackDataClientConfig::new_checked(
            scope.clone(),
            BTreeMap::from([("BTC_USDC_PERP".into(), economics)]),
        )
        .unwrap();
        let quota = BackpackQuota::default();
        let (data_tx, _data_rx) = mpsc::unbounded_channel();
        replace_data_event_sender(data_tx);
        let (exec_tx, _exec_rx) = mpsc::unbounded_channel();
        nautilus_common::live::runner::replace_exec_event_sender(exec_tx);
        let mut public = BackpackDataClient::with_quota(
            ClientId::from("BP-CONTROL"),
            public_config.clone(),
            quota.clone(),
        )
        .unwrap();
        let namespace =
            BackpackClientIdNamespace::loopback_peer(&peer.endpoints, "101", Some("2")).unwrap();
        let account = BackpackExecutionClientConfig::new_read_only(
            scope,
            BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &peer.endpoints).unwrap(),
            AccountId::from("BACKPACK-SYNTHETIC"),
            namespace.clone(),
            directory.path().join("identity"),
            BackpackExecutionPolicy {
                recovery_interval: Duration::from_secs(30),
                shutdown_timeout: Duration::from_millis(300),
                ..Default::default()
            },
            BackpackReadBudget::new(10, 10, 100, Duration::from_secs(3)).unwrap(),
            quota,
        )
        .unwrap();
        let authority = BackpackExecutionAuthority {
            expires_at_ms: now_ns().as_u64() / 1_000_000 + 60_000,
            max_account_age_ms: 5000,
            max_market_age_ms: 5000,
            max_order_notional: decimal("1"),
            max_reserved_notional: decimal("2"),
            max_reserved_margin: decimal("1"),
            max_unsettled_orders: 1,
            allow_new_risk: true,
            allow_reduction: true,
            allow_owned_cancel: true,
        };
        let plan = BackpackLoopbackExecutionClientConfig::new_checked(
            account,
            public_config.clone(),
            authority,
            BackpackMutationPolicy {
                budget: Duration::from_secs(3),
                window: Default::default(),
            },
        )
        .unwrap();
        let control = plan.control();
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut client = BackpackLoopbackExecutionClientFactory
            .create(trader(), "BACKPACK-CONTROL", &plan, CacheView::new(cache))
            .unwrap();
        client.connect().await.unwrap();
        until(|| !peer.state.subscriptions.lock().unwrap().is_empty()).await;
        assert!(client.is_connected());
        assert!(!public_config.telemetry().snapshot().connected);
        assert!(control.begin_session().is_err());
        assert!(control.begin_session().is_err());
        public.connect().await.unwrap();
        assert!(public_config.telemetry().snapshot().connected);
        let first = control.begin_session().unwrap();
        assert!(first.generation() >= 3);
        let current = control.begin_session().unwrap();
        assert!(current.generation() > first.generation());
        let facts = |generation| BackpackLoopbackAccountFacts {
            namespace: namespace.clone(),
            generation,
            observed_at_ms: now_ns().as_u64() / 1_000_000,
            available_margin: decimal("100"),
            margin_per_notional: decimal("0.1"),
            fee_buffer_per_notional: decimal("0.001"),
            economics_reference: "synthetic peer v1".into(),
            net_positions: BTreeMap::from([(instrument(), Decimal::ZERO)]),
            auto_borrow: false,
            auto_lend: false,
            auto_repay: false,
            liquidating: false,
            complete: true,
        };
        assert!(
            control
                .accept_account(first, facts(first.generation()))
                .is_err()
        );
        assert!(control.refresh_market(first, instrument()).is_err());
        assert!(control.invalidate(first).is_err());
        control
            .accept_account(current, facts(current.generation()))
            .unwrap();
        control.invalidate(current).unwrap();
        assert!(
            control
                .accept_account(current, facts(current.generation()))
                .is_err()
        );
        client.disconnect().await.unwrap();
        public.disconnect().await.unwrap();
    }

    impl Harness {
        async fn new() -> Self {
            let peer = Peer::start().await;
            peer.state.guarded.store(true, Ordering::Relaxed);
            let directory = TempDir::new().unwrap();
            let scope = BackpackConfig::with_endpoints_checked(
                vec!["BTC_USDC_PERP".into()],
                peer.endpoints.clone(),
            )
            .unwrap();
            let economics = BackpackInstrumentEconomics::new_checked(
                decimal("0.1"),
                decimal("0.05"),
                Decimal::ZERO,
                Decimal::ZERO,
                BackpackEconomicsSource::Synthetic,
                "explicit local peer reference model".into(),
            )
            .unwrap();
            let config = BackpackDataClientConfig::new_checked(
                scope.clone(),
                BTreeMap::from([("BTC_USDC_PERP".into(), economics)]),
            )
            .unwrap();
            let public_config = config.clone();
            let public_telemetry = config.telemetry().clone();
            let quota = BackpackQuota::default();
            let (tx, mut data_rx) = mpsc::unbounded_channel();
            replace_data_event_sender(tx);
            let mut public =
                BackpackDataClient::with_quota(ClientId::from("BP"), config, quota.clone())
                    .unwrap();
            public.connect().await.unwrap();
            let cache = Rc::new(RefCell::new(Cache::default()));
            let event = tokio::time::timeout(Duration::from_secs(3), data_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let DataEvent::Instrument(native) = event else {
                panic!("metadata must precede public data")
            };
            cache.borrow_mut().add_instrument(native).unwrap();
            public
                .subscribe_quotes(SubscribeQuotes::new(
                    instrument(),
                    Some(ClientId::from("BP")),
                    None,
                    UUID4::new(),
                    now_ns(),
                    None,
                    None,
                ))
                .unwrap();
            until(|| !peer.state.public_senders.lock().unwrap().is_empty()).await;
            let namespace =
                BackpackClientIdNamespace::loopback_peer(&peer.endpoints, "101", Some("2"))
                    .unwrap();
            let config = BackpackExecutionClientConfig::new_read_only(
                scope,
                BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &peer.endpoints)
                    .unwrap(),
                AccountId::from("BACKPACK-SYNTHETIC"),
                namespace.clone(),
                directory.path().join("identity"),
                BackpackExecutionPolicy {
                    recovery_interval: Duration::from_secs(30),
                    shutdown_timeout: Duration::from_millis(300),
                    ..Default::default()
                },
                BackpackReadBudget::new(10, 10, 100, Duration::from_secs(3)).unwrap(),
                quota.clone(),
            )
            .unwrap()
            .with_loopback_execution(
                BackpackExecutionAuthority {
                    expires_at_ms: now_ns().as_u64() / 1_000_000 + 60_000,
                    max_account_age_ms: 5000,
                    max_market_age_ms: 5000,
                    max_order_notional: decimal("1"),
                    max_reserved_notional: decimal("2"),
                    max_reserved_margin: decimal("1"),
                    max_unsettled_orders: 1,
                    allow_new_risk: true,
                    allow_reduction: true,
                    allow_owned_cancel: true,
                },
                BackpackMutationPolicy {
                    budget: Duration::from_secs(3),
                    window: Default::default(),
                },
                &quota,
                public_telemetry,
            )
            .unwrap();
            let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
            let portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
            let account = AccountState::new(
                AccountId::from("BACKPACK-SYNTHETIC"),
                AccountType::Margin,
                vec![AccountBalance::new(
                    Money::from("100 USDC"),
                    Money::from("0 USDC"),
                    Money::from("100 USDC"),
                )],
                vec![],
                true,
                UUID4::new(),
                now_ns(),
                now_ns(),
                None,
            );
            cache
                .borrow_mut()
                .add_account(AccountAny::Margin(MarginAccount::new(account, true)))
                .unwrap();
            let mut engine = ExecutionEngine::new(clock, cache.clone(), None);
            engine.register_oms_type(StrategyId::from("S-001"), OmsType::Netting);
            let mut client = BackpackExecutionClient::new(
                trader(),
                "BACKPACK",
                config.clone(),
                CacheView::new(cache.clone()),
            )
            .unwrap();
            let (tx, rx) = mpsc::unbounded_channel();
            client.set_event_sender(tx);
            client.connect().await.unwrap();
            until(|| peer.state.subscriptions.lock().unwrap().len() == 1).await;
            let session = client.begin_loopback_session().unwrap();
            let mut harness = Self {
                peer,
                directory,
                public,
                public_config,
                client,
                cache,
                engine,
                portfolio,
                rx,
                data_rx,
                session,
                namespace,
                config,
            };
            harness.quote(1).await;
            harness.account(Decimal::ZERO);
            harness.apply_events();
            harness
        }
        async fn quote(&mut self, sequence: u64) {
            let stamp = timestamp();
            let frame = json!({"stream":"bookTicker.BTC_USDC_PERP","data":{"e":"bookTicker","E":stamp,"T":stamp,"s":"BTC_USDC_PERP","a":"101.0","A":"1.00000","b":"100.0","B":"1.00000","u":sequence}});
            self.peer
                .state
                .public_senders
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .send(PeerCommand::Text(frame.to_string()))
                .unwrap();
            loop {
                let event = tokio::time::timeout(Duration::from_secs(3), self.data_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
                if let DataEvent::Data(Data::Quote(quote)) = event {
                    self.cache.borrow_mut().add_quote(quote).unwrap();
                    self.portfolio.update_quote_tick(&quote);
                    break;
                }
            }
            self.client
                .refresh_loopback_market(self.session, instrument())
                .unwrap();
        }
        fn account(&self, quantity: Decimal) {
            self.client
                .accept_loopback_account(
                    self.session,
                    BackpackLoopbackAccountFacts {
                        namespace: self.namespace.clone(),
                        generation: self.session.generation(),
                        observed_at_ms: now_ns().as_u64() / 1_000_000,
                        available_margin: decimal("100"),
                        margin_per_notional: decimal("0.1"),
                        fee_buffer_per_notional: decimal("0.001"),
                        economics_reference: "synthetic local venue economics v1".into(),
                        net_positions: BTreeMap::from([(instrument(), quantity)]),
                        auto_borrow: false,
                        auto_lend: false,
                        auto_repay: false,
                        liquidating: false,
                        complete: true,
                    },
                )
                .unwrap();
        }
        fn order(&self, id: &str, side: OrderSide, reduce: bool) -> OrderAny {
            OrderTestBuilder::new(OrderType::Limit)
                .trader_id(trader())
                .strategy_id(StrategyId::from("S-001"))
                .instrument_id(instrument())
                .client_order_id(ClientOrderId::from(id))
                .side(side)
                .quantity(Quantity::from("0.00002"))
                .price(Price::from("100.5"))
                .post_only(false)
                .reduce_only(reduce)
                .ts_init(now_ns())
                .build()
        }
        fn submit(&self, order: &OrderAny) -> anyhow::Result<()> {
            self.cache
                .borrow_mut()
                .add_order(order.clone(), None, Some(ClientId::from("BACKPACK")), false)
                .unwrap();
            self.client.submit_order(SubmitOrder::from_order(
                order,
                trader(),
                Some(ClientId::from("BACKPACK")),
                None,
                UUID4::new(),
                now_ns(),
            ))
        }
        fn fill(&self, trade: i64, cumulative: &str, fee: &str) -> Value {
            let mut frame = order_frame("orderFill", trade);
            let bodies = self.peer.state.post_bodies.lock().unwrap();
            frame["data"]["c"] = bodies.last().unwrap()["clientId"].clone();
            frame["data"]["S"] = bodies.last().unwrap()["side"].clone();
            frame["data"]["r"] = bodies.last().unwrap()["reduceOnly"].clone();
            frame["data"]["i"] = json!(if bodies.len() == 1 {
                "synthetic-order-A"
            } else {
                "synthetic-order-B"
            });
            frame["data"]["p"] = json!("100.5");
            frame["data"]["y"] = json!(false);
            frame["data"]["z"] = json!(cumulative);
            frame["data"]["n"] = json!(fee);
            if cumulative == "0.00002" {
                frame["data"]["X"] = json!("Filled");
                frame["data"]["Z"] = json!("0.002002");
            }
            frame
        }
        fn apply_events(&mut self) -> usize {
            let mut fills = 0;
            for event in drain(&mut self.rx) {
                match event {
                    ExecutionEvent::Order(event) => self.engine.process(&event),
                    ExecutionEvent::Report(report) => {
                        if matches!(&report, ExecutionReport::Fill(_)) {
                            fills += 1;
                        }
                        self.engine.reconcile_execution_report(&report);
                    }
                    _ => {}
                }
            }
            fills
        }
        async fn stop(&mut self) {
            self.client.disconnect().await.unwrap();
            self.public.disconnect().await.unwrap();
        }
    }

    // Build through the real factory after a genuine public owner has claimed telemetry/quota.
    // Retain the engine and cache so REST cumulative reports cannot silently infer economics.
    async fn attached_owned_query(cumulative: bool, abnormal: Option<&str>) {
        use nautilus_backpack::execution_client::loopback_config::{
            BackpackLoopbackExecutionClientConfig, BackpackLoopbackExecutionClientFactory,
        };
        let Harness {
            peer,
            directory,
            mut public,
            public_config,
            mut client,
            cache,
            mut engine,
            portfolio,
            rx,
            data_rx,
            namespace,
            config,
            ..
        } = Harness::new().await;
        client.disconnect().await.unwrap();
        drop(client);
        drop(rx);
        let plan = BackpackLoopbackExecutionClientConfig::new_checked(
            config,
            public_config,
            BackpackExecutionAuthority {
                expires_at_ms: now_ns().as_u64() / 1_000_000 + 60_000,
                max_account_age_ms: 5000,
                max_market_age_ms: 5000,
                max_order_notional: decimal("1"),
                max_reserved_notional: decimal("2"),
                max_reserved_margin: decimal("1"),
                max_unsettled_orders: 1,
                allow_new_risk: true,
                allow_reduction: true,
                allow_owned_cancel: true,
            },
            BackpackMutationPolicy {
                budget: Duration::from_secs(3),
                window: Default::default(),
            },
        )
        .unwrap();
        let control = plan.control();
        let (tx, mut rx) = mpsc::unbounded_channel();
        nautilus_common::live::runner::replace_exec_event_sender(tx);
        let mut client = BackpackLoopbackExecutionClientFactory
            .create(trader(), "BACKPACK", &plan, CacheView::new(cache.clone()))
            .unwrap();
        client.connect().await.unwrap();
        until(|| peer.state.subscriptions.lock().unwrap().len() == 2).await;
        let session = control.begin_session().unwrap();
        control.refresh_market(session, instrument()).unwrap();
        control
            .accept_account(
                session,
                BackpackLoopbackAccountFacts {
                    namespace,
                    generation: session.generation(),
                    observed_at_ms: now_ns().as_u64() / 1_000_000,
                    available_margin: decimal("100"),
                    margin_per_notional: decimal("0.1"),
                    fee_buffer_per_notional: decimal("0.001"),
                    economics_reference: "synthetic peer v1".into(),
                    net_positions: BTreeMap::from([(instrument(), Decimal::ZERO)]),
                    auto_borrow: false,
                    auto_lend: false,
                    auto_repay: false,
                    liquidating: false,
                    complete: true,
                },
            )
            .unwrap();
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader())
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(instrument())
            .client_order_id(ClientOrderId::from("OWNED-QUERY"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.00002"))
            .price(Price::from("100.5"))
            .post_only(false)
            .ts_init(now_ns())
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(ClientId::from("BACKPACK")), false)
            .unwrap();
        client
            .submit_order(SubmitOrder::from_order(
                &order,
                trader(),
                Some(ClientId::from("BACKPACK")),
                None,
                UUID4::new(),
                now_ns(),
            ))
            .unwrap();
        until(|| peer.state.post_bodies.lock().unwrap().len() == 1).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                for event in drain(&mut rx) {
                    if let ExecutionEvent::Order(event) = event {
                        engine.process(&event);
                    }
                }
                if cache
                    .borrow()
                    .order(&order.client_order_id())
                    .unwrap()
                    .status()
                    == OrderStatus::Accepted
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let mut resting = peer.state.post_bodies.lock().unwrap()[0].clone();
        resting["id"] = json!("synthetic-order-A");
        resting["status"] = json!(if cumulative { "PartiallyFilled" } else { "New" });
        resting["executedQuantity"] = json!(if cumulative { "0.00001" } else { "0" });
        resting["executedQuoteQuantity"] = json!(if cumulative { "0.001001" } else { "0" });
        resting["createdAt"] = json!(0);
        resting["selfTradePrevention"] = json!("RejectTaker");
        if cumulative {
            let mut frame = order_frame("orderFill", 77);
            frame["data"]["c"] = resting["clientId"].clone();
            frame["data"]["p"] = json!("100.5");
            frame["data"]["y"] = json!(false);
            peer.send(&frame);
            until(|| control.pending().unwrap().len() == 1).await;
            let mut fills = 0;
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    for event in drain(&mut rx) {
                        match event {
                            ExecutionEvent::Order(event) => engine.process(&event),
                            ExecutionEvent::Report(report) => {
                                fills += usize::from(matches!(&report, ExecutionReport::Fill(_)));
                                engine.reconcile_execution_report(&report);
                            }
                            _ => {}
                        }
                    }
                    if fills == 1 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(fills, 1);
        }
        if let Some(kind) = abnormal {
            if kind == "unknown-field" {
                resting["futureOrderPolicy"] = json!(true);
            } else {
                resting["id"] = json!("external-conflicting-order");
            }
        }
        *peer.state.resting_order.lock().unwrap() = Some(resting.clone());
        // Both supported selectors travel through actual signed GETs. An omitted venue ID
        // must resolve the already committed local intent, never adopt a caller-supplied ID.
        for numeric in [false, true] {
            client
                .query_order(QueryOrder::new(
                    trader(),
                    Some(ClientId::from("BACKPACK")),
                    StrategyId::from("S-001"),
                    instrument(),
                    order.client_order_id(),
                    (!numeric).then(|| VenueOrderId::from("synthetic-order-A")),
                    UUID4::new(),
                    now_ns(),
                    None,
                    None,
                ))
                .unwrap();
            if abnormal.is_some() {
                until(|| {
                    control
                        .health()
                        .unwrap()
                        .evidence_gaps
                        .contains("OrderQueryIncomplete")
                        || control
                            .health()
                            .unwrap()
                            .evidence_gaps
                            .contains("UnknownFields")
                })
                .await;
                break;
            }
            until(|| {
                peer.state.order_queries.lock().unwrap().len() == usize::from(numeric) + 1
                    && control
                        .health()
                        .unwrap()
                        .evidence_gaps
                        .contains("OrderTimeUnknown")
            })
            .await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            for event in drain(&mut rx) {
                match event {
                    ExecutionEvent::Order(event) => engine.process(&event),
                    ExecutionEvent::Report(report) => {
                        assert!(!matches!(&report, ExecutionReport::Fill(_)));
                        engine.reconcile_execution_report(&report);
                    }
                    _ => {}
                }
            }
        }
        let queries = peer.state.order_queries.lock().unwrap().clone();
        assert!(queries.iter().all(|(_, verified)| *verified));
        assert_eq!(
            queries[0].0,
            "orderId=synthetic-order-A&symbol=BTC_USDC_PERP"
        );
        if abnormal.is_none() {
            assert_eq!(
                queries[1].0,
                format!("clientId={}&symbol=BTC_USDC_PERP", resting["clientId"])
            );
            let health = control.health().unwrap();
            assert_eq!(health.state, BackpackAccountState::Degraded);
            assert!(health.evidence_gaps.contains("AccountIdentityUnverified"));
            assert!(health.evidence_gaps.contains("OrderTimeUnknown"));
            assert!(health.rest_snapshot_observed);
            assert!(!health.private_subscription_confirmed);
            control.refresh_market(session, instrument()).unwrap();
        } else {
            assert!(control.refresh_market(session, instrument()).is_err());
        }
        peer.state.cancel_pending.store(true, Ordering::Relaxed);
        let cancel = client.cancel_order(CancelOrder::new(
            trader(),
            Some(ClientId::from("BACKPACK")),
            StrategyId::from("S-001"),
            instrument(),
            order.client_order_id(),
            Some(VenueOrderId::from("synthetic-order-A")),
            UUID4::new(),
            now_ns(),
            None,
            None,
        ));
        if abnormal.is_none() {
            cancel.unwrap();
            until(|| {
                control
                    .health()
                    .unwrap()
                    .evidence_gaps
                    .contains("CancelPending")
            })
            .await;
            assert_eq!(
                peer.state
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(method, path, _)| method == "DELETE" && path == "/api/v1/order")
                    .count(),
                1
            );
        } else {
            assert!(cancel.is_err());
            assert!(
                !peer
                    .state
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(method, _, _)| method == "DELETE")
            );
        }
        let cached = cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .clone();
        assert_eq!(
            cached.filled_qty().as_decimal(),
            if cumulative {
                decimal("0.00001")
            } else {
                Decimal::ZERO
            }
        );
        assert_eq!(cached.trade_ids().len(), usize::from(cumulative));
        if cumulative {
            assert_eq!(
                cached.commissions()[&Currency::USDC()].as_decimal(),
                decimal("-0.000001")
            );
            assert_eq!(control.pending().unwrap().len(), 1);
        }
        client.disconnect().await.unwrap();
        assert!(control.shutdown().unwrap().dirty);
        public.disconnect().await.unwrap();
        // The real cache/portfolio and metadata receiver remain owned for the entire run.
        drop((portfolio, data_rx, directory));
    }

    #[tokio::test]
    async fn test_attached_owned_query_static_gaps_preserve_session_and_cancel_202() {
        attached_owned_query(false, None).await;
    }
    #[tokio::test]
    async fn test_attached_owned_query_cumulative_never_infers_fill_or_acknowledges() {
        attached_owned_query(true, None).await;
    }
    #[tokio::test]
    async fn test_attached_owned_query_unknown_field_invalidates_session() {
        attached_owned_query(false, Some("unknown-field")).await;
    }
    #[tokio::test]
    async fn test_attached_owned_query_correlation_conflict_invalidates_session() {
        attached_owned_query(false, Some("correlation-conflict")).await;
    }

    #[tokio::test]
    async fn test_guarded_true_first_fill_waits_for_post_ack_then_real_engine_and_portfolio() {
        let mut h = Harness::new().await;
        h.peer.state.post_hold.store(true, Ordering::Relaxed);
        let order = h.order("LOCAL-FIRST", OrderSide::Buy, false);
        h.submit(&order).unwrap();
        until(|| h.peer.state.post_bodies.lock().unwrap().len() == 1).await;
        let first = h.fill(77, "0.00001", "-0.000001");
        h.peer.send(&first);
        until(|| h.client.health().pending_fills == 1).await;
        assert_eq!(h.apply_events(), 0);
        assert_eq!(
            h.cache.borrow().orders(None, None, None, None, None).len(),
            1
        );
        assert!(
            h.cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .trade_ids()
                .is_empty()
        );
        assert_eq!(h.portfolio.net_position(&instrument()), Decimal::ZERO);
        h.peer.state.post_notify.notify_one();
        until(|| {
            h.client.fill_delivery().pending()[0].client_order_id == Some(order.client_order_id())
        })
        .await;
        assert_eq!(h.apply_events(), 1);
        let actual = h
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .clone();
        assert_eq!(actual.status(), OrderStatus::PartiallyFilled);
        assert_eq!(actual.trade_ids().len(), 1);
        assert_eq!(actual.filled_qty().as_decimal(), decimal("0.00001"));
        assert_eq!(
            actual.commissions()[&Currency::USDC()].as_decimal(),
            decimal("-0.000001")
        );
        assert_eq!(h.portfolio.net_position(&instrument()), decimal("0.00001"));
        assert_eq!(h.client.health().pending_fills, 1);
        h.peer.send(&first);
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.apply_events();
        assert_eq!(
            h.cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .trade_ids()
                .len(),
            1
        );
        assert_eq!(h.portfolio.net_position(&instrument()), decimal("0.00001"));
        assert_eq!(h.peer.state.post_bodies.lock().unwrap().len(), 1);
        h.stop().await;
        assert!(h.client.loopback_shutdown_report().unwrap().dirty);
        // Receipt persistence is intentionally separate from actual framework application.
        assert!(h.client.fill_delivery().applied().is_empty());
        assert!(h.directory.path().exists());
    }
    #[tokio::test]
    async fn test_guarded_pre_ack_conflicting_fill_economics_fault_without_attribution() {
        let mut h = Harness::new().await;
        h.peer.state.post_hold.store(true, Ordering::Relaxed);
        let order = h.order("LOCAL-CONFLICT", OrderSide::Buy, false);
        h.submit(&order).unwrap();
        until(|| h.peer.state.post_bodies.lock().unwrap().len() == 1).await;
        let first = h.fill(77, "0.00001", "-0.000001");
        h.peer.send(&first);
        until(|| h.client.health().pending_fills == 1).await;
        let mut conflict = first.clone();
        conflict["data"]["n"] = json!("0.000002");
        h.peer.send(&conflict);
        until(|| h.client.health().parse_failures > 0).await;
        assert_eq!(h.apply_events(), 0);
        assert_eq!(
            h.client.fill_delivery().pending()[0]
                .commission
                .as_decimal(),
            decimal("-0.000001")
        );
        assert!(
            h.client.fill_delivery().pending()[0]
                .client_order_id
                .is_none()
        );
        assert!(
            h.client
                .refresh_loopback_market(h.session, instrument())
                .is_err()
        );
        h.peer.state.post_notify.notify_one();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(h.apply_events(), 0);
        assert_eq!(h.peer.state.post_bodies.lock().unwrap().len(), 1);
        h.stop().await;
        assert!(h.client.loopback_shutdown_report().unwrap().dirty);
    }

    #[tokio::test]
    async fn test_guarded_cancel_202_terminal_waits_for_durable_true_fill_ack_and_late_fill_reopens_risk()
     {
        use std::io::Write;

        use nautilus_backpack::execution::owner::BackpackLoopbackTerminalEvidence;
        let mut h = Harness::new().await;
        let order = h.order("LOCAL-CANCEL", OrderSide::Buy, false);
        h.submit(&order).unwrap();
        until(|| h.peer.state.post_bodies.lock().unwrap().len() == 1).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.apply_events();
        let first = h.fill(77, "0.00001", "-0.000001");
        h.peer.send(&first);
        until(|| h.client.health().pending_fills == 1).await;
        h.apply_events();
        h.peer.state.cancel_pending.store(true, Ordering::Relaxed);
        h.client
            .cancel_order(CancelOrder::new(
                trader(),
                Some(ClientId::from("BACKPACK")),
                StrategyId::from("S-001"),
                instrument(),
                order.client_order_id(),
                Some(VenueOrderId::from("synthetic-order-A")),
                UUID4::new(),
                now_ns(),
                None,
                None,
            ))
            .unwrap();
        until(|| h.client.health().evidence_gaps.contains("CancelPending")).await;
        h.apply_events();
        assert_eq!(
            h.cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::PartiallyFilled
        );
        let mut terminal = first.clone();
        terminal["data"]["e"] = json!("orderCancelled");
        terminal["data"]["X"] = json!("Cancelled");
        terminal["data"]["t"] = Value::Null;
        for key in ["l", "L", "m", "n", "N"] {
            terminal["data"].as_object_mut().unwrap().remove(key);
        }
        h.peer.send(&terminal);
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.apply_events();
        assert_eq!(
            h.cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::PartiallyFilled
        );
        let evidence = BackpackLoopbackTerminalEvidence {
            client_order_id: order.client_order_id(),
            venue_order_id: VenueOrderId::from("synthetic-order-A"),
            instrument_id: instrument(),
            generation: h.session.generation(),
            cumulative_quantity: decimal("0.00001"),
            applied_fill_quantity: decimal("0.00001"),
            economic_ack_reference: "synthetic-consumer/checkpoint-1".into(),
        };
        assert!(
            h.client
                .accept_loopback_terminal(h.session, &evidence)
                .is_err()
        );
        let delivery = h.client.fill_delivery();
        let report = delivery.pending()[0].clone();
        let key = BackpackFillKey {
            instrument_id: report.instrument_id,
            trade_id: report.trade_id,
        };
        // Persist the ACTUALLY APPLIED native order/positions and immutable receipt together.
        // This local witness demonstrates the consumer boundary; it does not make the
        // framework's asynchronous event routing an atomic production transaction.
        let checkpoint = h.directory.path().join("consumer.json");
        delivery.acknowledge_with(key, |receipt| {
            let cache = h.cache.borrow();
            let state = json!({"order":(*cache.order(&order.client_order_id()).unwrap()).clone(),"positions":cache.positions(None,None,None,None,None).into_iter().map(|position|(*position).clone()).collect::<Vec<_>>(),"receipt":receipt,"reference":{"quantity":"0.00001","fee":"-0.000001"}});
            let mut file = tempfile::NamedTempFile::new_in(h.directory.path()).unwrap();
            file.write_all(&serde_json::to_vec(&state).unwrap()).unwrap(); file.as_file().sync_all().unwrap(); file.persist(&checkpoint).unwrap();
            Ok(())
        }).unwrap();
        h.apply_events();
        assert_eq!(
            h.cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Canceled
        );
        assert_eq!(h.portfolio.net_position(&instrument()), decimal("0.00001"));
        h.client
            .accept_loopback_terminal(h.session, &evidence)
            .unwrap();
        // A duplicated terminal acknowledgement cannot free a second reservation.
        h.client
            .accept_loopback_terminal(h.session, &evidence)
            .unwrap();
        let late = h.fill(78, "0.00002", "0.000001");
        h.peer.send(&late);
        until(|| h.client.health().pending_fills == 1).await;
        h.apply_events();
        assert_eq!(
            h.cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .trade_ids()
                .len(),
            2
        );
        assert_eq!(h.portfolio.net_position(&instrument()), decimal("0.00002"));
        assert_eq!(h.client.fill_delivery().applied().len(), 1);
        h.account(decimal("0.00002"));
        h.quote(2).await;
        let next = h.order("BLOCKED-NEW", OrderSide::Buy, false);
        assert!(h.submit(&next).is_err());
        assert_eq!(h.peer.state.post_bodies.lock().unwrap().len(), 1);
        h.stop().await;
        let stopped = h.client.loopback_shutdown_report().unwrap();
        assert!(stopped.dirty);
        assert_eq!(stopped.unknown, 1);
        h.client.stop().unwrap();
        assert_eq!(h.client.loopback_shutdown_report(), Some(stopped));
        let restored: Value = serde_json::from_slice(&std::fs::read(checkpoint).unwrap()).unwrap();
        assert_eq!(restored["reference"]["quantity"], "0.00001");
        let restored_order: OrderAny = serde_json::from_value(restored["order"].clone()).unwrap();
        assert_eq!(restored_order.trade_ids().len(), 1);
    }

    #[tokio::test]
    async fn test_guarded_exact_reference_roundtrip_native_positions_fees_and_portfolio_pnl() {
        use nautilus_backpack::execution::owner::BackpackLoopbackTerminalEvidence;
        let mut h = Harness::new().await;
        let entry = h.order("ENTRY", OrderSide::Buy, false);
        h.submit(&entry).unwrap();
        until(|| h.peer.state.post_bodies.lock().unwrap().len() == 1).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.apply_events();
        let first = h.fill(77, "0.00001", "0.000001");
        h.peer.send(&first);
        until(|| h.client.health().pending_fills == 1).await;
        h.apply_events();
        let mut second = h.fill(78, "0.00002", "-0.000001");
        second["data"]["L"] = json!("100.2");
        second["data"]["Z"] = json!("0.002003");
        h.peer.send(&second);
        until(|| h.client.health().pending_fills == 2).await;
        h.apply_events();
        assert_eq!(h.portfolio.net_position(&instrument()), decimal("0.00002"));
        let delivery = h.client.fill_delivery();
        for report in delivery.pending() {
            delivery
                .acknowledge_with(
                    BackpackFillKey {
                        instrument_id: report.instrument_id,
                        trade_id: report.trade_id,
                    },
                    |_| Ok(()),
                )
                .unwrap();
        }
        h.client
            .accept_loopback_terminal(
                h.session,
                &BackpackLoopbackTerminalEvidence {
                    client_order_id: entry.client_order_id(),
                    venue_order_id: VenueOrderId::from("synthetic-order-A"),
                    instrument_id: instrument(),
                    generation: h.session.generation(),
                    cumulative_quantity: decimal("0.00002"),
                    applied_fill_quantity: decimal("0.00002"),
                    economic_ack_reference: "explicit synthetic consumer applied entry".into(),
                },
            )
            .unwrap();
        h.account(decimal("0.00002"));
        h.quote(2).await;
        let exit = h.order("EXIT", OrderSide::Sell, true);
        h.submit(&exit).unwrap();
        until(|| h.peer.state.post_bodies.lock().unwrap().len() == 2).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.apply_events();
        let mut close = h.fill(79, "0.00002", "0.000003");
        close["data"]["l"] = json!("0.00002");
        close["data"]["L"] = json!("101.0");
        close["data"]["Z"] = json!("0.002020");
        h.peer.send(&close);
        until(|| h.client.health().pending_fills == 1).await;
        h.apply_events();
        let entry_cost =
            decimal("0.00001") * decimal("100.1") + decimal("0.00001") * decimal("100.2");
        let exit_proceeds = decimal("0.00002") * decimal("101.0");
        let fees = decimal("0.000001") + decimal("-0.000001") + decimal("0.000003");
        let reference_pnl = exit_proceeds - entry_cost - fees;
        assert_eq!(reference_pnl, decimal("0.000014"));
        assert_eq!(h.portfolio.net_position(&instrument()), Decimal::ZERO);
        assert_eq!(
            h.portfolio
                .realized_pnl(&instrument())
                .unwrap()
                .as_decimal(),
            reference_pnl
        );
        assert_eq!(
            h.cache
                .borrow()
                .order(&entry.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Filled
        );
        assert_eq!(
            h.cache
                .borrow()
                .order(&exit.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Filled
        );
        assert_eq!(
            h.cache
                .borrow()
                .order(&exit.client_order_id())
                .unwrap()
                .commissions()[&Currency::USDC()]
                .as_decimal(),
            fees
        );
        assert_eq!(delivery.applied().len(), 2);
        assert_eq!(h.client.health().pending_fills, 1); // Portfolio application alone never ACKs.
        h.peer.send(&close);
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.apply_events();
        assert_eq!(
            h.portfolio
                .realized_pnl(&instrument())
                .unwrap()
                .as_decimal(),
            reference_pnl
        );
        h.stop().await;
    }

    #[tokio::test]
    async fn test_guarded_public_generation_fault_refuses_new_post_but_owned_cancel_uses_exit_authority()
     {
        let mut h = Harness::new().await;
        let order = h.order("OWNED", OrderSide::Buy, false);
        h.submit(&order).unwrap();
        until(|| h.peer.state.post_bodies.lock().unwrap().len() == 1).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.apply_events();
        h.public.disconnect().await.unwrap();
        let new = h.order("NEW-STALE", OrderSide::Buy, false);
        assert!(h.submit(&new).is_err());
        h.peer.state.cancel_pending.store(true, Ordering::Relaxed);
        h.client
            .cancel_order(CancelOrder::new(
                trader(),
                Some(ClientId::from("BACKPACK")),
                StrategyId::from("S-001"),
                instrument(),
                order.client_order_id(),
                Some(VenueOrderId::from("synthetic-order-A")),
                UUID4::new(),
                now_ns(),
                None,
                None,
            ))
            .unwrap();
        until(|| h.client.health().evidence_gaps.contains("CancelPending")).await;
        assert_eq!(h.peer.state.post_bodies.lock().unwrap().len(), 1);
        assert_eq!(
            h.peer
                .state
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(method, _, _)| method == "DELETE")
                .count(),
            1
        );
        h.apply_events();
        assert_eq!(
            h.cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Accepted
        );
        h.stop().await;
    }

    #[tokio::test]
    async fn test_guarded_lost_post_ack_rest_collision_remains_unbound_and_never_retried() {
        let mut h = Harness::new().await;
        h.peer.state.lost_post.store(true, Ordering::Relaxed);
        let order = h.order("UNKNOWN", OrderSide::Buy, false);
        h.submit(&order).unwrap();
        until(|| {
            h.client
                .health()
                .evidence_gaps
                .contains("MutationOutcomeUnknown")
        })
        .await;
        h.apply_events();
        assert_eq!(
            h.cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Submitted
        );
        h.peer.send(&h.fill(77, "0.00001", "0.000001"));
        until(|| h.client.health().pending_fills == 1).await;
        assert_eq!(h.apply_events(), 0);
        assert!(
            h.client.fill_delivery().pending()[0]
                .client_order_id
                .is_none()
        );
        assert!(
            h.client
                .cancel_order(CancelOrder::new(
                    trader(),
                    None,
                    StrategyId::from("S-001"),
                    instrument(),
                    order.client_order_id(),
                    None,
                    UUID4::new(),
                    now_ns(),
                    None,
                    None
                ))
                .is_err()
        );
        assert!(
            h.client
                .submit_order(SubmitOrder::from_order(
                    &order,
                    trader(),
                    None,
                    None,
                    UUID4::new(),
                    now_ns()
                ))
                .is_err()
        );
        assert_eq!(h.peer.state.post_bodies.lock().unwrap().len(), 1);
        assert_eq!(
            h.peer
                .state
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(method, _, _)| method == "DELETE")
                .count(),
            0
        );
        assert_eq!(h.portfolio.net_position(&instrument()), Decimal::ZERO);
        h.stop().await;
        assert_eq!(h.client.loopback_shutdown_report().unwrap().unknown, 1);
    }

    #[tokio::test]
    async fn test_guarded_restart_rest_true_fill_dedup_couples_native_state_and_consumer_receipt() {
        use std::io::Write;

        use nautilus_backpack::account::reconciliation::BackpackAppliedFill;
        use nautilus_model::position::Position;
        let mut h = Harness::new().await;
        let order = h.order("RESTART", OrderSide::Buy, false);
        h.submit(&order).unwrap();
        until(|| h.peer.state.post_bodies.lock().unwrap().len() == 1).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.apply_events();
        let first = h.fill(77, "0.00001", "-0.000001");
        h.peer.send(&first);
        until(|| h.client.health().pending_fills == 1).await;
        h.apply_events();
        let checkpoint = h.directory.path().join("consumer-restart.json");
        let delivery = h.client.fill_delivery();
        let fill = delivery.pending()[0].clone();
        let key = BackpackFillKey {
            instrument_id: fill.instrument_id,
            trade_id: fill.trade_id,
        };
        delivery.acknowledge_with(key,|receipt| {
            let cache = h.cache.borrow();
            let state = json!({"order":(*cache.order(&order.client_order_id()).unwrap()).clone(),
                "positions":cache.positions(None,None,None,None,None).into_iter().map(|position|(*position).clone()).collect::<Vec<_>>(),
                "receipt":receipt,"account":(*cache.account(&AccountId::from("BACKPACK-SYNTHETIC")).unwrap()).clone()});
            let mut file = tempfile::NamedTempFile::new_in(h.directory.path()).unwrap();
            file.write_all(&serde_json::to_vec(&state).unwrap()).unwrap(); file.as_file().sync_all().unwrap(); file.persist(&checkpoint).unwrap();
            Ok(())
        }).unwrap();
        let raw_fill = json!({"clientId":first["data"]["c"].as_u64().unwrap().to_string(),"fee":"-0.000001","feeSymbol":"USDC","isMaker":true,"orderId":"synthetic-order-A","price":"100.1","quantity":"0.00001","side":"Bid","symbol":"BTC_USDC_PERP","systemOrderType":null,"timestamp":jiff::Timestamp::from_microsecond(first["data"]["T"].as_i64().unwrap()).unwrap().to_zoned(jiff::tz::TimeZone::UTC).datetime().to_string(),"tradeId":77});
        h.peer.state.fills.lock().unwrap().push(raw_fill);
        h.client.disconnect().await.unwrap();
        let stopped = h.client.loopback_shutdown_report().unwrap();
        assert!(stopped.dirty);
        let config = h.config.clone();
        let Harness {
            peer,
            directory,
            mut public,
            client,
            cache: old_cache,
            engine,
            portfolio,
            ..
        } = h;
        drop(delivery);
        drop(client);
        drop(engine);
        drop(portfolio);
        let serialized: Value =
            serde_json::from_slice(&std::fs::read(&checkpoint).unwrap()).unwrap();
        let restored_order: OrderAny = serde_json::from_value(serialized["order"].clone()).unwrap();
        let positions: Vec<Position> =
            serde_json::from_value(serialized["positions"].clone()).unwrap();
        let receipt: BackpackAppliedFill =
            serde_json::from_value(serialized["receipt"].clone()).unwrap();
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(
                old_cache
                    .borrow()
                    .instrument(&instrument())
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let account: AccountAny = serde_json::from_value(serialized["account"].clone()).unwrap();
        cache.borrow_mut().add_account(account).unwrap();
        cache
            .borrow_mut()
            .add_order(
                restored_order.clone(),
                None,
                Some(ClientId::from("BACKPACK")),
                false,
            )
            .unwrap();
        for position in &positions {
            cache
                .borrow_mut()
                .add_position(position, OmsType::Netting)
                .unwrap();
        }
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
        let mut portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
        portfolio.initialize_positions();
        let mut engine = ExecutionEngine::new(clock, cache.clone(), None);
        engine.register_oms_type(StrategyId::from("S-001"), OmsType::Netting);
        let mut client = BackpackExecutionClient::new(
            trader(),
            "BACKPACK",
            config,
            CacheView::new(cache.clone()),
        )
        .unwrap();
        client.restore_applied_fills(vec![receipt]).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        client.set_event_sender(tx);
        client.connect().await.unwrap();
        until(|| peer.state.subscriptions.lock().unwrap().len() == 2).await;
        let token = client.begin_loopback_session().unwrap();
        assert!(token.generation() > 1);
        client
            .restore_loopback_order(token, order.client_order_id())
            .unwrap();
        let reports = client
            .generate_fill_reports(GenerateFillReports::new(
                UUID4::new(),
                now_ns(),
                Some(instrument()),
                None,
                None,
                None,
                None,
                None,
            ))
            .await
            .unwrap();
        assert!(reports.is_empty());
        for event in drain(&mut rx) {
            if let ExecutionEvent::Report(report) = event {
                engine.reconcile_execution_report(&report);
            }
        }
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .trade_ids()
                .len(),
            1
        );
        assert_eq!(portfolio.net_position(&instrument()), decimal("0.00001"));
        assert_eq!(client.fill_delivery().applied().len(), 1);
        assert_eq!(client.health().pending_fills, 0);
        assert!(
            client
                .submit_order(SubmitOrder::from_order(
                    &order,
                    trader(),
                    None,
                    None,
                    UUID4::new(),
                    now_ns()
                ))
                .is_err()
        );
        assert_eq!(peer.state.post_bodies.lock().unwrap().len(), 1);
        client.disconnect().await.unwrap();
        assert!(client.loopback_shutdown_report().unwrap().dirty);
        public.disconnect().await.unwrap();
        assert!(directory.path().exists());
    }
    #[tokio::test]
    async fn test_guarded_old_config_and_telemetry_cannot_hold_owner_after_client_drop() {
        let mut h = Harness::new().await;
        let old_config = h.config.clone();
        let old_telemetry = h.client.telemetry();
        h.client.disconnect().await.unwrap();
        let Harness {
            peer,
            directory,
            mut public,
            client,
            namespace,
            ..
        } = h;
        drop(client);
        let fresh = BackpackExecutionClientConfig::new_read_only(
            BackpackConfig::with_endpoints_checked(
                vec!["BTC_USDC_PERP".into()],
                peer.endpoints.clone(),
            )
            .unwrap(),
            BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &peer.endpoints).unwrap(),
            AccountId::from("BACKPACK-SYNTHETIC"),
            namespace,
            directory.path().join("identity"),
            BackpackExecutionPolicy::default(),
            BackpackReadBudget::new(10, 10, 100, Duration::from_secs(3)).unwrap(),
            BackpackQuota::default(),
        )
        .unwrap();
        assert_ne!(
            old_config.telemetry().snapshot().run_id,
            fresh.telemetry().snapshot().run_id
        );
        let fresh_client =
            BackpackExecutionClient::new(trader(), "BACKPACK", fresh, cache()).unwrap();
        assert_eq!(
            old_telemetry.snapshot().state,
            BackpackAccountState::Stopped
        );
        assert!(!old_telemetry.snapshot().transport_connected);
        assert!(fresh_client.begin_loopback_session().is_err());
        drop(fresh_client);
        public.disconnect().await.unwrap();
        assert!(old_config.identity_directory().exists());
    }
}
