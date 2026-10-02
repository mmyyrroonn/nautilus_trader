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
        BackpackExecutionClientFactory, BackpackExecutionPolicy,
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
    fills: Mutex<Vec<Value>>,
    positions: Mutex<Vec<Value>>,
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
    state.senders.lock().unwrap().push(tx);
    loop {
        tokio::select! {
            message=socket.recv()=>match message {
                Some(Ok(Message::Text(text)))=>{
                    let Ok(value)=serde_json::from_str::<Value>(&text) else {break;};
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
            "/api/v1/orders" => json!([fixture("resting_order")]),
            "/api/v1/order" => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"code":"ORDER_NOT_FOUND"})),
                )
                    .into_response();
            }
            "/wapi/v1/history/fills" => json!(state.fills.lock().unwrap().clone()),
            "/wapi/v1/history/orders" => json!([fixture("history_order")]),
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
    assert!(
        client
            .acknowledge_fill_with(key, |_| {
                callback_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .is_err()
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
