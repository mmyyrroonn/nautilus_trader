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

//! Synthetic loopback public runtime; venue metadata is the checked public BTC fixture.
//! No credentials, account payloads, or venue mutation endpoint is involved.
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, VecDeque},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        Request, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use nautilus_backpack::{
    common::endpoints::BackpackEndpoints,
    config::{BackpackConfig, BackpackDataClientConfig, BackpackPublicLifecycleConfig},
    data::BackpackDataClient,
    factories::BackpackDataClientFactory,
    instruments::{BackpackEconomicsSource, BackpackInstrumentEconomics},
};
use nautilus_common::{
    cache::{Cache, CacheView},
    clients::DataClient,
    clock::TestClock,
    factories::{ClientConfig, DataClientFactory},
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent,
        data::{
            SubscribeBookDeltas, SubscribeQuotes, SubscribeTrades, UnsubscribeBookDeltas,
            UnsubscribeQuotes,
        },
    },
    providers::InstrumentProvider,
};
use nautilus_core::{UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::Data,
    enums::BookType,
    identifiers::{ClientId, InstrumentId},
    instruments::Instrument,
};
use parking_lot::Mutex;
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{Notify, broadcast, mpsc},
    task::JoinHandle,
};
const SYMBOL: &str = "BTC_USDC_PERP";
fn instant() -> UnixNanos {
    get_atomic_clock_realtime().get_time_ns()
}
fn micros() -> u64 {
    instant().as_u64() / 1000
}
fn id() -> InstrumentId {
    InstrumentId::from("BTC_USDC_PERP.BACKPACK")
}
#[derive(Clone, Debug)]
enum Command {
    Frame(Value),
    Close,
}
#[derive(Clone)]
struct SnapshotReply {
    id: u64,
    delay_ms: u64,
}
type CapturedRequest = (String, String, HashMap<String, String>);
struct ServerState {
    commands: Mutex<Vec<(usize, Value)>>,
    requests: Mutex<Vec<CapturedRequest>>,
    connections: AtomicUsize,
    active_connections: AtomicUsize,
    market_delay_ms: AtomicU64,
    market_override: Mutex<Option<Value>>,
    control: broadcast::Sender<Command>,
    notify: Notify,
    snapshots: Mutex<VecDeque<SnapshotReply>>,
}
struct Server {
    state: Arc<ServerState>,
    endpoints: BackpackEndpoints,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (control, _) = broadcast::channel(64);
        let state = Arc::new(ServerState {
            commands: Mutex::new(vec![]),
            requests: Mutex::new(vec![]),
            connections: AtomicUsize::new(0),
            active_connections: AtomicUsize::new(0),
            market_delay_ms: AtomicU64::new(0),
            market_override: Mutex::new(None),
            control,
            notify: Notify::new(),
            snapshots: Mutex::new(VecDeque::new()),
        });
        let app = Router::new()
            .route("/", get(ws))
            .route("/api/v1/markets", get(markets))
            .route("/api/v1/depth", get(depth))
            .with_state(state.clone());

        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            state,
            endpoints: BackpackEndpoints::loopback_override(
                &format!("http://{addr}"),
                &format!("ws://{addr}"),
            )
            .unwrap(),
            task,
        }
    }
    fn config(&self) -> BackpackDataClientConfig {
        let economics = BackpackInstrumentEconomics::new_checked(
            Decimal::new(1, 1),
            Decimal::new(5, 2),
            Decimal::ZERO,
            Decimal::ZERO,
            BackpackEconomicsSource::Synthetic,
            "local runtime test economics".into(),
        )
        .unwrap();
        let lifecycle = BackpackPublicLifecycleConfig {
            http_timeout_secs: 5,
            ws_connect_timeout_secs: 2,
            ws_heartbeat_secs: 1,
            ws_idle_timeout_secs: 10,
            reconnect_timeout_secs: 5,
            shutdown_timeout_secs: 1,
            depth_snapshot_limit: 5,
            ..Default::default()
        };
        BackpackDataClientConfig::new_checked(
            BackpackConfig::with_endpoints_checked(vec![SYMBOL.into()], self.endpoints.clone())
                .unwrap(),
            BTreeMap::from([(SYMBOL.into(), economics)]),
        )
        .unwrap()
        .with_lifecycle_checked(lifecycle)
        .unwrap()
    }
    fn send(&self, value: Value) {
        self.state.control.send(Command::Frame(value)).unwrap();
    }
    fn snapshot(&self, id: u64, delay_ms: u64) {
        self.state
            .snapshots
            .lock()
            .push_back(SnapshotReply { id, delay_ms });
    }
    async fn wait_topic(&self, epoch: usize, method: &str, topic: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let wait = self.state.notify.notified();
                let found = self.state.commands.lock().iter().any(|(e, c)| {
                    *e == epoch
                        && c["method"] == method
                        && c["params"].as_array().unwrap().iter().any(|v| v == topic)
                });

                if found {
                    break;
                }
                wait.await;
            }
        })
        .await
        .unwrap();
    }
    async fn wait_depth_requests(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let wait = self.state.notify.notified();
                if self
                    .state
                    .requests
                    .lock()
                    .iter()
                    .filter(|(_, p, _)| p.starts_with("/api/v1/depth"))
                    .count()
                    >= count
                {
                    break;
                }
                wait.await;
            }
        })
        .await
        .unwrap();
    }
}
fn capture(state: &ServerState, request: &Request) {
    state.requests.lock().push((
        request.method().to_string(),
        request.uri().to_string(),
        request
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_owned()))
            .collect(),
    ));
    state.notify.notify_waiters();
}
async fn markets(State(state): State<Arc<ServerState>>, request: Request) -> Json<Value> {
    capture(&state, &request);
    tokio::time::sleep(Duration::from_millis(
        state.market_delay_ms.load(Ordering::Acquire),
    ))
    .await;

    if let Some(value) = state.market_override.lock().clone() {
        return Json(value);
    }
    Json(json!([serde_json::from_str::<Value>(include_str!(
        "../test_data/btc_usdc_perp.json"
    ))
    .unwrap()]))
}
async fn depth(State(state): State<Arc<ServerState>>, request: Request) -> Json<Value> {
    capture(&state, &request);
    let reply = state.snapshots.lock().pop_front().unwrap_or(SnapshotReply {
        id: 100,
        delay_ms: 0,
    });
    let stamp = micros();
    tokio::time::sleep(Duration::from_millis(reply.delay_ms)).await;
    let bid = if reply.id == 999 { "102.0" } else { "100.0" };
    Json(
        json!({"asks":[["101.0","1.00000"]],"bids":[[bid,"1.00000"]],"lastUpdateId":reply.id.to_string(),"timestamp":stamp}),
    )
}
async fn ws(
    State(state): State<Arc<ServerState>>,
    upgrade: WebSocketUpgrade,
    request: Request,
) -> Response {
    capture(&state, &request);
    upgrade.on_upgrade(move |socket| serve_ws(socket, state))
}
async fn serve_ws(socket: WebSocket, state: Arc<ServerState>) {
    let index = state.connections.fetch_add(1, Ordering::AcqRel);
    state.active_connections.fetch_add(1, Ordering::AcqRel);
    let (mut write, mut read) = socket.split();
    let mut control = state.control.subscribe();

    loop {
        tokio::select! {
            command=control.recv()=>match command {
                Ok(Command::Frame(value))=>{
                    if write.send(Message::Text(value.to_string().into())).await.is_err(){break;}
                },
                Ok(Command::Close)=>{let _=write.send(Message::Close(None)).await;break;},
                Err(_)=>break,
            },
            message=read.next()=>match message {
                Some(Ok(Message::Text(value)))=>{
                    state.commands.lock().push((index,serde_json::from_str(&value).unwrap()));
                    state.notify.notify_waiters();
                },
                Some(Ok(Message::Ping(value)))=>{let _=write.send(Message::Pong(value)).await;},
                Some(Ok(Message::Close(_)) | Err(_)) | None=>break,
                _=>{},
            }
        }
    }
    state.active_connections.fetch_sub(1, Ordering::AcqRel);
    state.notify.notify_waiters();
}

fn quote(seq: u64) -> Value {
    let t = micros();
    json!({"stream":format!("bookTicker.{SYMBOL}"),"data":{"e":"bookTicker","E":t,"T":t,"s":SYMBOL,"a":"101.0","A":"1.00000","b":"100.0","B":"1.00000","u":seq}})
}
fn trade(seq: u64) -> Value {
    let t = micros();
    json!({"stream":format!("trade.{SYMBOL}"),"data":{"e":"trade","E":t,"T":t,"s":SYMBOL,"p":"100.0","q":"1.00000","b":"1","a":"2","t":seq,"m":true}})
}
fn update(first: u64, last: u64) -> Value {
    let t = micros();
    json!({"stream":format!("depth.{SYMBOL}"),"data":{"e":"depth","E":t,"T":t,"s":SYMBOL,"a":[["101.0","2.00000"]],"b":[["100.0","2.00000"]],"U":first,"u":last}})
}
fn subscribe(client: &mut dyn DataClient) {
    client
        .subscribe_quotes(SubscribeQuotes::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    client
        .subscribe_trades(SubscribeTrades::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    client
        .subscribe_book_deltas(SubscribeBookDeltas::new(
            id(),
            BookType::L2_MBP,
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            true,
            None,
            None,
        ))
        .unwrap();
}
async fn event(rx: &mut mpsc::UnboundedReceiver<DataEvent>) -> DataEvent {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn no_event(rx: &mut mpsc::UnboundedReceiver<DataEvent>) {
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn loopback_factory_instruments_before_data_receive_before_snapshot_and_unsubscribe() {
    let server = Server::start().await;
    server.snapshot(100, 200);
    let config = server.config();
    assert_eq!(config.telemetry().snapshot().run_id, None);
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let factory = BackpackDataClientFactory::new();
    let cache = CacheView::new(Rc::new(RefCell::new(Cache::default())));
    let clock = Rc::new(RefCell::new(TestClock::new()));
    let mut client = factory.create("BP", &config, cache, clock).unwrap();
    subscribe(client.as_mut());
    client.connect().await.unwrap();
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    server
        .wait_topic(0, "SUBSCRIBE", &format!("depth.{SYMBOL}"))
        .await;
    server.wait_depth_requests(1).await;
    server.send(update(101, 101));
    server.send(quote(1));
    server.send(trade(1));
    let mut kinds = vec![];
    let mut batch = None;

    for _ in 0..3 {
        match event(&mut rx).await {
            DataEvent::Data(Data::Quote(_)) => kinds.push("quote"),
            DataEvent::Data(Data::Trade(_)) => kinds.push("trade"),
            DataEvent::Data(Data::Deltas(d)) => {
                batch = Some(d);
                kinds.push("depth");
            }
            v => panic!("unexpected event {v:?}"),
        }
    }
    assert!(kinds.contains(&"quote") && kinds.contains(&"trade"));
    let batch = batch.unwrap();
    assert_eq!(batch.sequence, 101);
    assert!(
        batch
            .deltas
            .iter()
            .any(|d| d.order.size.to_string() == "2.00000")
    );
    let health = config.telemetry().snapshot();
    assert!(health.connected && health.metadata_ready);
    assert_eq!(health.quotes_fresh.get(SYMBOL), Some(&true));
    assert!(health.books[SYMBOL].is_some());
    assert_eq!(serde_json::to_value(health).unwrap()["schema_version"], 1);
    client
        .unsubscribe_quotes(&UnsubscribeQuotes::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    server.send(quote(2));
    server
        .wait_topic(0, "UNSUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    no_event(&mut rx).await;
    client.disconnect().await.unwrap();
    client.disconnect().await.unwrap();
    assert!(!config.telemetry().snapshot().connected);
    client.dispose().unwrap();
    assert!(client.connect().await.is_err());

    for (method, _, headers) in server.state.requests.lock().iter() {
        assert_eq!(method, "GET");
        assert!(
            !headers
                .keys()
                .any(|h| h.starts_with("x-api") || h == "x-signature")
        );
    }

    for (_, cmd) in server.state.commands.lock().iter() {
        assert!(cmd.get("signature").is_none());
        assert!(
            cmd["params"]
                .as_array()
                .unwrap()
                .iter()
                .all(|v| !v.as_str().unwrap().starts_with("account."))
        );
    }
}

#[tokio::test]
async fn loopback_gap_bootstraps_new_book_and_reconnect_republishes_instruments() {
    let server = Server::start().await;
    let config = server.config();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config.clone()).unwrap();
    subscribe(&mut client);
    client.connect().await.unwrap();
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    server.wait_depth_requests(1).await;
    assert!(matches!(
        event(&mut rx).await,
        DataEvent::Data(Data::Deltas(_))
    ));
    server.snapshot(103, 200);
    server.send(update(103, 103));
    server.wait_depth_requests(2).await;
    assert!(config.telemetry().snapshot().books[SYMBOL].is_none());
    server.send(update(104, 104));
    let DataEvent::Data(Data::Deltas(batch)) = event(&mut rx).await else {
        panic!("missing recovered batch")
    };
    assert_eq!(batch.sequence, 104);
    server.state.control.send(Command::Close).unwrap();
    server
        .wait_topic(1, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    assert!(matches!(
        event(&mut rx).await,
        DataEvent::Data(Data::Deltas(_))
    ));
    server.send(quote(1));
    assert!(matches!(
        event(&mut rx).await,
        DataEvent::Data(Data::Quote(_))
    ));
    assert_eq!(client.health().connection_epoch, 1);
    client.disconnect().await.unwrap();
    client.connect().await.unwrap();
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    server
        .wait_topic(2, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    assert_eq!(client.health().generation, 2);
    client.stop().unwrap();
    assert!(!client.health().connected);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn loopback_unsubscribe_cancels_old_snapshot_and_telemetry_exclusive_owner() {
    let server = Server::start().await;
    server.snapshot(900, 1500);
    server.snapshot(100, 0);
    let config = server.config();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config.clone()).unwrap();
    assert!(BackpackDataClient::new(ClientId::from("OTHER"), config.clone()).is_err());
    subscribe(&mut client);
    client.connect().await.unwrap();
    let _ = event(&mut rx).await;
    server.wait_depth_requests(1).await;
    client
        .unsubscribe_book_deltas(&UnsubscribeBookDeltas::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    server
        .wait_topic(0, "UNSUBSCRIBE", &format!("depth.{SYMBOL}"))
        .await;
    client
        .subscribe_book_deltas(SubscribeBookDeltas::new(
            id(),
            BookType::L2_MBP,
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            true,
            None,
            None,
        ))
        .unwrap();
    server.wait_depth_requests(2).await;
    let DataEvent::Data(Data::Deltas(batch)) = event(&mut rx).await else {
        panic!("missing replacement snapshot")
    };
    assert_eq!(batch.sequence, 100);
    tokio::time::sleep(Duration::from_millis(1600)).await;
    no_event(&mut rx).await;
    assert!(client.health().books[SYMBOL].is_some());
    client.disconnect().await.unwrap();
    let run = client.health().run_id;
    drop(client);
    let replacement = BackpackDataClient::new(ClientId::from("NEW"), config.clone()).unwrap();
    assert_ne!(replacement.health().run_id, run);
}

#[tokio::test]
async fn loopback_quote_freshness_null_side_old_event_and_malformed_duplicate() {
    let server = Server::start().await;
    let config = server
        .config()
        .with_lifecycle_checked(BackpackPublicLifecycleConfig {
            quote_stale_after_ms: 100,
            ..server.config().lifecycle().clone()
        })
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config.clone()).unwrap();
    client
        .subscribe_quotes(SubscribeQuotes::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    client.connect().await.unwrap();
    let _ = event(&mut rx).await;
    server
        .wait_topic(0, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    server.send(quote(1));
    let _ = event(&mut rx).await;
    assert!(client.health().quotes_fresh[SYMBOL]);
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert!(!client.health().quotes_fresh[SYMBOL]);
    server.send(quote(1));
    no_event(&mut rx).await;
    assert!(!client.health().quotes_fresh[SYMBOL]);
    let mut future = quote(2);
    future["data"]["T"] = json!(micros() + 1_000_000_000);
    server.send(future);
    no_event(&mut rx).await;
    let mut old = quote(3);
    old["data"]["T"] = json!(micros() - 1_000_000);
    server.send(old);
    no_event(&mut rx).await;
    let mut unavailable = quote(4);
    unavailable["data"]["a"] = Value::Null;
    unavailable["data"]["A"] = Value::Null;
    server.send(unavailable);
    no_event(&mut rx).await;
    assert!(client.health().quotes_fresh.get(SYMBOL).is_none_or(|v| !*v));
    let mut malformed = quote(4);
    malformed["data"]["b"] = json!("100.05");
    server.send(malformed);
    server
        .wait_topic(1, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    assert!(client.health().quotes_fresh.get(SYMBOL).is_none_or(|v| !*v));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn loopback_provider_complete_allowlist_and_strict_ids_filters() {
    let server = Server::start().await;
    let (tx, _rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), server.config()).unwrap();
    client.load_all(None).await.unwrap();
    assert_eq!(client.store().count(), 1);
    client.load(&id(), None).await.unwrap();
    assert!(client.store().contains(&id()));
    assert!(
        client
            .load_ids(&[InstrumentId::from("SOL_USDC_PERP.BACKPACK")], None)
            .await
            .is_err()
    );
    assert!(client.store().is_empty());
    assert!(
        client
            .load_all(Some(&HashMap::from([("marketType".into(), "PERP".into())])))
            .await
            .is_err()
    );
}
#[derive(Debug)]
struct ForeignConfig;
impl ClientConfig for ForeignConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
#[rstest]
fn factory_rejects_foreign_config_without_network() {
    let cache = CacheView::new(Rc::new(RefCell::new(Cache::default())));
    assert!(
        BackpackDataClientFactory::new()
            .create(
                "BP",
                &ForeignConfig,
                cache,
                Rc::new(RefCell::new(TestClock::new()))
            )
            .is_err()
    );
}

#[tokio::test]
async fn loopback_drop_inflight_old_run_cannot_pollute_reclaimed_telemetry() {
    let server = Server::start().await;
    server.snapshot(900, 800);
    server.snapshot(100, 0);
    let config = server.config();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut first = BackpackDataClient::new(ClientId::from("FIRST"), config.clone()).unwrap();
    subscribe(&mut first);
    first.connect().await.unwrap();
    let _ = event(&mut rx).await;
    server.wait_depth_requests(1).await;
    server.send(quote(1));
    let _ = event(&mut rx).await;
    let old_run = first.health().run_id;
    assert!(first.health().quotes_fresh[SYMBOL]);
    drop(first);
    let mut second = BackpackDataClient::new(ClientId::from("SECOND"), config.clone()).unwrap();
    let health = second.health();
    assert_ne!(health.run_id, old_run);
    assert_eq!(health.generation, 0);
    assert!(!health.metadata_ready && !health.connected);
    assert!(health.quotes_fresh.is_empty() && health.books.is_empty());
    subscribe(&mut second);
    second.connect().await.unwrap();
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    server.wait_depth_requests(2).await;
    let DataEvent::Data(Data::Deltas(batch)) = event(&mut rx).await else {
        panic!("missing second run snapshot")
    };
    assert_eq!(batch.sequence, 100);
    tokio::time::sleep(Duration::from_millis(900)).await;
    no_event(&mut rx).await;
    assert_eq!(second.health().generation, 1);
    assert_eq!(second.health().run_id, health.run_id);
    assert!(second.health().connected);
    second.disconnect().await.unwrap();
}

#[tokio::test]
async fn loopback_partial_connect_cancellation_and_invalid_complete_metadata_roll_back() {
    let server = Server::start().await;
    let config = server.config();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config).unwrap();
    server.state.market_delay_ms.store(500, Ordering::Release);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.connect())
            .await
            .is_err()
    );
    assert!(!client.health().connected && !client.health().metadata_ready);
    client.disconnect().await.unwrap();
    no_event(&mut rx).await;
    server.state.market_delay_ms.store(0, Ordering::Release);
    *server.state.market_override.lock() = Some(json!([]));
    assert!(client.connect().await.is_err());
    assert!(!client.health().connected && !client.health().metadata_ready);
    assert_eq!(server.state.connections.load(Ordering::Acquire), 0);
    assert!(client.store().is_empty());
    *server.state.market_override.lock() = None;
    client.connect().await.unwrap();
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    assert!(client.is_connected());
    client.reset().unwrap();
    client.disconnect().await.unwrap();
    no_event(&mut rx).await;
}

#[tokio::test]
async fn loopback_reconnect_new_tick_reaches_engine_and_instrument_response() {
    let server = Server::start().await;
    let config = server.config();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config).unwrap();
    client.load_all(None).await.unwrap();
    assert_eq!(client.store().count(), 1);
    client
        .subscribe_quotes(SubscribeQuotes::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    client.connect().await.unwrap();
    let _ = event(&mut rx).await;
    assert!(client.store().is_empty());
    server
        .wait_topic(0, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    // This edited public metadata is deliberately synthetic, never a venue tick-change claim.
    let mut market: Value =
        serde_json::from_str(include_str!("../test_data/btc_usdc_perp.json")).unwrap();
    market["filters"]["price"]["tickSize"] = json!("0.5");
    market["filters"]["price"]["minPrice"] = json!("0.5");
    *server.state.market_override.lock() = Some(json!([market]));
    server.state.control.send(Command::Close).unwrap();
    server
        .wait_topic(1, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    let DataEvent::Instrument(instrument) = event(&mut rx).await else {
        panic!("missing fresh instrument")
    };
    assert_eq!(instrument.price_increment().to_string(), "0.5");
    assert!(client.store().is_empty());
    let request = nautilus_common::messages::data::RequestInstrument::new(
        id(),
        None,
        None,
        Some(ClientId::from("BP")),
        UUID4::new(),
        instant(),
        None,
    );
    let correlation = request.request_id;
    client.request_instrument(request).unwrap();
    let DataEvent::Response(nautilus_common::messages::data::DataResponse::Instrument(response)) =
        event(&mut rx).await
    else {
        panic!("missing current metadata response")
    };
    assert_eq!(response.correlation_id, correlation);
    assert_eq!(response.data.price_increment().to_string(), "0.5");
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn loopback_duplicate_depth_does_not_extend_book_freshness_but_empty_progress_does() {
    let server = Server::start().await;
    let config = server
        .config()
        .with_lifecycle_checked(BackpackPublicLifecycleConfig {
            ws_idle_timeout_secs: 2,
            ..server.config().lifecycle().clone()
        })
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config).unwrap();
    client
        .subscribe_book_deltas(SubscribeBookDeltas::new(
            id(),
            BookType::L2_MBP,
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            true,
            None,
            None,
        ))
        .unwrap();
    client.connect().await.unwrap();
    let _ = event(&mut rx).await;
    let _ = event(&mut rx).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    server.send(update(100, 100));
    no_event(&mut rx).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(client.health().connected);
    assert!(!client.health().books_fresh[SYMBOL]);
    assert!(client.health().books_continuous[SYMBOL]);
    let mut empty = update(101, 101);
    empty["data"]["a"] = json!([]);
    empty["data"]["b"] = json!([]);
    server.send(empty);
    no_event(&mut rx).await;
    assert!(client.health().books_fresh[SYMBOL]);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn loopback_queue_overflow_during_metadata_bootstrap_cannot_reopen_that_epoch() {
    let server = Server::start().await;
    let config = server
        .config()
        .with_lifecycle_checked(BackpackPublicLifecycleConfig {
            max_buffer_frames: 1,
            ..server.config().lifecycle().clone()
        })
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config).unwrap();
    client
        .subscribe_quotes(SubscribeQuotes::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    client.connect().await.unwrap();
    let _ = event(&mut rx).await;
    server
        .wait_topic(0, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    server.state.market_delay_ms.store(300, Ordering::Release);
    server.state.control.send(Command::Close).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let wait = server.state.notify.notified();
            if server
                .state
                .requests
                .lock()
                .iter()
                .filter(|(_, p, _)| p == "/api/v1/markets")
                .count()
                >= 2
            {
                break;
            }
            wait.await;
        }
    })
    .await
    .unwrap();

    for seq in 1..=8 {
        server.send(quote(seq));
    }
    server
        .wait_topic(2, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    assert_eq!(client.health().connection_epoch, 2);
    assert!(client.health().quotes_fresh.is_empty());
    server.send(quote(10));
    assert!(matches!(
        event(&mut rx).await,
        DataEvent::Data(Data::Quote(_))
    ));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn loopback_snapshot_failure_reconnects_without_publishing_invalid_book() {
    let server = Server::start().await;
    server.snapshot(999, 0);
    let config = server.config();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config).unwrap();
    client
        .subscribe_book_deltas(SubscribeBookDeltas::new(
            id(),
            BookType::L2_MBP,
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            true,
            None,
            None,
        ))
        .unwrap();
    client.connect().await.unwrap();
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    let DataEvent::Data(Data::Deltas(batch)) = event(&mut rx).await else {
        panic!("missing valid recovery snapshot")
    };
    assert_eq!(batch.sequence, 100);
    assert_eq!(client.health().connection_epoch, 1);
    client.disconnect().await.unwrap();
}
#[tokio::test]
async fn loopback_idle_feed_recovery_does_not_reuse_previous_quote_freshness() {
    let server = Server::start().await;
    let config = server
        .config()
        .with_lifecycle_checked(BackpackPublicLifecycleConfig {
            ws_idle_timeout_secs: 2,
            ..server.config().lifecycle().clone()
        })
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config).unwrap();
    client
        .subscribe_quotes(SubscribeQuotes::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    client.connect().await.unwrap();
    let _ = event(&mut rx).await;
    server
        .wait_topic(0, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    server.send(quote(1));
    let _ = event(&mut rx).await;
    server
        .wait_topic(1, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    assert!(matches!(event(&mut rx).await, DataEvent::Instrument(_)));
    assert!(client.health().quotes_fresh.is_empty());
    server.send(quote(1));
    assert!(matches!(
        event(&mut rx).await,
        DataEvent::Data(Data::Quote(_))
    ));
    client.disconnect().await.unwrap();
}
#[tokio::test]
async fn loopback_recovery_deadline_covers_slow_metadata_and_owned_teardown() {
    let server = Server::start().await;
    let config = server
        .config()
        .with_lifecycle_checked(BackpackPublicLifecycleConfig {
            reconnect_timeout_secs: 2,
            ..server.config().lifecycle().clone()
        })
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), config).unwrap();
    client
        .subscribe_quotes(SubscribeQuotes::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    client.connect().await.unwrap();
    let _ = event(&mut rx).await;
    server
        .wait_topic(0, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    server.state.market_delay_ms.store(5000, Ordering::Release);
    server.state.control.send(Command::Close).unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if client.health().stale_reason == Some("recovery deadline") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!client.health().connected && !client.health().metadata_ready);
    client.disconnect().await.unwrap();
    no_event(&mut rx).await;
}

#[tokio::test]
async fn loopback_closed_data_sink_closes_owning_public_socket() {
    let server = Server::start().await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = BackpackDataClient::new(ClientId::from("BP"), server.config()).unwrap();
    client
        .subscribe_quotes(SubscribeQuotes::new(
            id(),
            Some(ClientId::from("BP")),
            None,
            UUID4::new(),
            instant(),
            None,
            None,
        ))
        .unwrap();
    client.connect().await.unwrap();
    let _ = event(&mut rx).await;
    server
        .wait_topic(0, "SUBSCRIBE", &format!("bookTicker.{SYMBOL}"))
        .await;
    drop(rx);
    server.send(quote(1));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let wait = server.state.notify.notified();
            if server.state.active_connections.load(Ordering::Acquire) == 0 {
                break;
            }
            wait.await;
        }
    })
    .await
    .unwrap();
    assert!(!client.health().connected);
    assert_eq!(
        client.health().stale_reason,
        Some("data engine unavailable")
    );
    client.disconnect().await.unwrap();
}
