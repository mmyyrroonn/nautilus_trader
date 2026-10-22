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

use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use futures_util::{SinkExt, StreamExt};
use nautilus_arcus::{
    config::ArcusExecutionClientConfig,
    execution::ArcusExecutionClient,
    models::{ArcusMarket, ArcusModifyRequest, ArcusOrderSide, ArcusTimeInForce},
    signing::{ArcusSigner, modify_signing_payload},
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::TestClock,
    live::runner::{replace_data_event_sender, replace_exec_event_sender},
    messages::{
        DataEvent, ExecutionEvent,
        execution::{GenerateOrderStatusReport, ModifyOrder},
    },
};
use nautilus_core::{UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    accounts::{AccountAny, MarginAccount},
    enums::{AccountType, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{OrderEventAny, OrderSubmitted},
    identifiers::{
        ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId, Venue, VenueOrderId,
    },
    orders::{Order, OrderAny, OrderTestBuilder},
    types::{Price, Quantity},
};
use parking_lot::Mutex;
use rstest::rstest;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const ADDRESS: &str = "0x1111111111111111111111111111111111111111";
// RFC 8032 section 7.1 public test seed; never an account credential.
const TEST_SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";

fn market() -> ArcusMarket {
    serde_json::from_str(include_str!("../test_data/market.json")).unwrap()
}
fn now() -> UnixNanos {
    get_atomic_clock_realtime().get_time_ns()
}

#[rstest]
fn operation_three_matches_independent_signature_vector_and_preserves_total_units() {
    let request = ArcusModifyRequest {
        address: "0xABCDEF0123456789ABCDEF0123456789ABCDEF01".into(),
        account_index: 3,
        market_id: 1,
        order_id: "AbCd1234".into(),
        quantity: dec!(0.0015),
        price: dec!(50000.1),
        side: ArcusOrderSide::Buy,
        time_in_force: ArcusTimeInForce::Alo,
        reduce_only: false,
        good_til_time: "1795518526400000".into(),
        client_time: "1792667326400000000".into(),
    };
    let timestamp = request.client_time.parse().unwrap();
    assert_eq!(
        modify_signing_payload(&request, &market(), timestamp).unwrap(),
        "{\"ad\":\"0xabcdef0123456789abcdef0123456789abcdef01\",\"ai\":3,\"ct\":1792667326400000000,\"g\":1795518526400000000,\"id\":\"AbCd1234\",\"m\":1,\"op\":3,\"p\":500001,\"q\":150000,\"r\":0,\"s\":0,\"t\":3,\"v\":1}"
    );
    assert_eq!(
        ArcusSigner::from_seed_hex(TEST_SEED)
            .unwrap()
            .sign_modify(&request, &market(), timestamp)
            .unwrap(),
        "8ab949aa95f957f90fd24c337b66a4361d55b64a16999ea65341a830bda05a05b30f28affa6bba72da2136767f0f3e2c3603813a2b1d97637d6b2c203bd9ae05"
    );
    let mut invalid = request.clone();
    invalid.client_time = (timestamp + 1).to_string();
    assert!(modify_signing_payload(&invalid, &market(), timestamp).is_err());
    invalid = request.clone();
    invalid.order_id = "not-hex".into();
    assert!(modify_signing_payload(&invalid, &market(), timestamp).is_err());
    invalid = request;
    invalid.good_til_time = (timestamp / 1000 + 86_400_000_000).to_string();
    assert!(modify_signing_payload(&invalid, &market(), timestamp).is_err());
}

#[derive(Clone)]
struct Mock {
    order: Arc<Mutex<Value>>,
    fills: Arc<Mutex<Vec<Value>>>,
    requests: Arc<Mutex<Vec<ArcusModifyRequest>>>,
    mode: Arc<AtomicUsize>,
    frames: tokio::sync::broadcast::Sender<Value>,
}

struct Fixture {
    client: ArcusExecutionClient,
    engine: ExecutionEngine,
    events: tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    _data_events: tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    cache: Rc<RefCell<Cache>>,
    state: Mock,
    http_task: tokio::task::JoinHandle<()>,
    ws_task: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.client.stop();
        self.http_task.abort();
        self.ws_task.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        let (frames, _) = tokio::sync::broadcast::channel(32);
        let timestamp = now().as_u64();
        let state = Mock {
            order: Arc::new(Mutex::new(
                json!({"orderId":"AbCd1234","clientId":"MODIFY-1","accountIndex":0,
            "marketId":1,"marketDisplayName":"BTC-USD","side":"BUY","type":"LIMIT","status":"OPEN",
            "price":"50000.0","originalSize":"0.01000000","remainingSize":"0.01000000","filledSize":"0",
            "timeInForce":"ALO","reduceOnly":true,"goodTilTime":(timestamp/1000+33*86_400_000_000_u64).to_string(),
            "createdAt":timestamp/1000,"updatedAt":timestamp/1000,"sequenceNumber":1}),
            )),
            fills: Arc::new(Mutex::new(Vec::new())),
            requests: Arc::new(Mutex::new(Vec::new())),
            mode: Arc::new(AtomicUsize::new(0)),
            frames,
        };
        let router = Router::new()
            .route("/v1/markets", get(|| async {Json(json!({"markets":[market()]}))}))
            .route("/v1/account", get(|| async {Json(json!({"address":ADDRESS,"accountIndex":0,"equity":"1000",
                "freeCollateral":"1000","netQuoteBalance":"1000","netDeposits":"1000","positions":{},"sequenceNumber":1}))}))
            .route("/v1/orders", get(|State(s):State<Mock>| async move {Json(json!({"orders":[s.order.lock().clone()]}))}))
            .route("/v1/openOrders", get(|State(s):State<Mock>| async move {Json(json!({"orders":[s.order.lock().clone()]}))}))
            .route("/v1/order/AbCd1234", get(|State(s):State<Mock>| async move {Json(s.order.lock().clone())}))
            .route("/v1/fills", get(|State(s):State<Mock>| async move {Json(json!({"fills":s.fills.lock().clone()}))}))
            .route("/v1/positions", get(|| async {Json(json!({"positions":{}}))}))
            .route("/v1/modifyOrder", post(|State(s):State<Mock>,headers:HeaderMap,Json(value):Json<Value>| async move {
                assert!(value.get("clientId").is_none());
                let request:ArcusModifyRequest=serde_json::from_value(value).unwrap();
                let timestamp=headers["X-Timestamp"].to_str().unwrap().parse::<u64>().unwrap();
                assert_eq!(request.client_time,timestamp.to_string());
                assert!(now().as_u64().saturating_sub(timestamp)<5_000_000_000);
                let key:[u8;32]=nautilus_core::hex::decode(headers["X-API-Key"].to_str().unwrap()).unwrap().try_into().unwrap();
                let signature=Signature::from_slice(&nautilus_core::hex::decode(headers["X-Signature"].to_str().unwrap()).unwrap()).unwrap();
                VerifyingKey::from_bytes(&key).unwrap().verify(modify_signing_payload(&request,&market(),timestamp).unwrap().as_bytes(),&signature).unwrap();
                s.requests.lock().push(request);

                match s.mode.load(Ordering::Acquire) {
                    1 => (StatusCode::BAD_REQUEST,Json(json!({"error":"MODIFY_CHANGED_IMMUTABLE_FIELD"}))),
                    2 => (StatusCode::BAD_GATEWAY,Json(json!({"error":"Uncertain after dispatch"}))),
                    3 => {tokio::time::sleep(Duration::from_secs(3)).await;(StatusCode::ACCEPTED,Json(json!({"status":"ACK","orderId":"AbCd1234"})))},
                    _ => (StatusCode::ACCEPTED,Json(json!({"status":"ACK","orderId":"AbCd1234"}))),
                }
            })).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_url = format!("http://{}", listener.local_addr().unwrap());

        let http_task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", listener.local_addr().unwrap());
        let frames = state.frames.clone();

        let ws_task = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(socket).await.unwrap();
                let mut inbox = frames.subscribe();

                loop {
                    tokio::select! {
                        frame=inbox.recv()=>{let frame=frame.unwrap();if frame["_close"]==true{let _=socket.close(None).await;break;}

                            if socket.send(Message::Text(frame.to_string().into())).await.is_err(){break;}},
                        frame=socket.next()=>match frame {
                            Some(Ok(Message::Text(text)))=>{let mut ack:Value=serde_json::from_str(&text).unwrap();ack["type"]="subscribed".into();ack["contents"]=json!({"isSnapshot":true});

                                if socket.send(Message::Text(ack.to_string().into())).await.is_err(){break;}},
                            Some(Ok(Message::Ping(p)))=>{if socket.send(Message::Pong(p)).await.is_err(){break;}},
                            Some(Ok(Message::Close(_))|Err(_))|None=>break,_=>{}
                        }
                    }
                }
            }
        });
        let cache = Rc::new(RefCell::new(Cache::default()));
        let config = ArcusExecutionClientConfig {
            address: Some(ADDRESS.into()),
            api_secret: Some(TEST_SEED.into()),
            base_url_http: Some(http_url),
            base_url_ws: Some(ws_url),
            http_timeout_secs: 1,
            reconciliation_interval_secs: 60,
            ..Default::default()
        };
        let core = ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            ClientId::from("ARCUS"),
            Venue::from("ARCUS"),
            OmsType::Netting,
            config.account_id,
            AccountType::Margin,
            None,
            cache.clone(),
        );
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(tx);
        let (tx, mut data) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let mut client = ArcusExecutionClient::new(core, config).unwrap();
        client.start().unwrap();
        client.connect().await.unwrap();
        let mut builder = OrderTestBuilder::new(OrderType::Limit);
        let mut order = builder
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(InstrumentId::from("BTC-USD-PERP.ARCUS"))
            .client_order_id(ClientOrderId::from("MODIFY-1"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.01000000"))
            .price(Price::from("50000.0"))
            .time_in_force(TimeInForce::Gtc)
            .post_only(true)
            .reduce_only(true)
            .build();
        order
            .apply(OrderEventAny::Submitted(OrderSubmitted::new(
                order.trader_id(),
                order.strategy_id(),
                order.instrument_id(),
                order.client_order_id(),
                client.account_id(),
                UUID4::new(),
                now(),
                now(),
            )))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(order, None, None, false)
            .unwrap();

        while let Ok(DataEvent::Instrument(instrument)) = data.try_recv() {
            cache.borrow_mut().add_instrument(instrument).unwrap();
        }
        let engine =
            ExecutionEngine::new(Rc::new(RefCell::new(TestClock::new())), cache.clone(), None);
        let mut fixture = Self {
            client,
            engine,
            events,
            _data_events: data,
            cache,
            state,
            http_task,
            ws_task,
        };
        fixture
            .until(
                |f| f.client.is_connected() && f.order().status() == OrderStatus::Accepted,
                15,
            )
            .await;
        fixture
    }

    fn order(&self) -> OrderAny {
        self.cache
            .borrow()
            .order(&ClientOrderId::from("MODIFY-1"))
            .unwrap()
            .clone()
    }
    fn pump(&mut self) {
        while let Ok(event) = self.events.try_recv() {
            match event {
                ExecutionEvent::Order(event) => self.engine.process(&event),
                ExecutionEvent::Report(report) => self.engine.reconcile_execution_report(&report),
                ExecutionEvent::Account(event)
                    if self.cache.borrow().account(&event.account_id).is_none() =>
                {
                    self.cache
                        .borrow_mut()
                        .add_account(AccountAny::Margin(MarginAccount::new(event, false)))
                        .unwrap();
                }
                _ => {}
            }
        }
    }
    async fn until(&mut self, predicate: impl Fn(&Self) -> bool, seconds: u64) {
        let result = tokio::time::timeout(Duration::from_secs(seconds), async {
            loop {
                self.pump();

                if predicate(self) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "Timed out: connected={}, order={:?}, posts={}",
            self.client.is_connected(),
            self.order(),
            self.state.requests.lock().len()
        );
    }
    fn command(&self, quantity: Option<&str>, price: Option<&str>) -> ModifyOrder {
        let order = self.order();
        ModifyOrder::new(
            order.trader_id(),
            None,
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            order.venue_order_id(),
            quantity.map(Quantity::from),
            price.map(Price::from),
            None,
            UUID4::new(),
            now(),
            None,
            None,
        )
    }
    fn publish(&self, mut record: Value) {
        record["updatedAt"] = (now().as_u64() / 1000).into();
        *self.state.order.lock() = record.clone();
        self.state.frames.send(json!({"type":"channel_data","channel":"orders","id":ADDRESS,"accountIndex":0,"contents":record})).unwrap();
    }
    async fn filled(&mut self) {
        let fill = json!({"tradeId":"REAL-FILL-1","orderId":"AbCd1234","clientId":"MODIFY-1","marketId":1,
        "marketDisplayName":"BTC-USD","side":"BUY","originalSize":"0.01000000","size":"0.00300000","price":"50000.0",
        "fee":"0.01","role":"MAKER","createdAt":now().as_u64()/1000,"sequenceNumber":2});
        self.state.fills.lock().push(fill.clone());
        self.state
            .frames
            .send(
                json!({"type":"channel_data","channel":"userFills","id":ADDRESS,
            "accountIndex":0,"contents":{"fills":[fill]}}),
            )
            .unwrap();
        let mut record = self.state.order.lock().clone();
        record["remainingSize"] = "0.00700000".into();
        record["filledSize"] = "0.00300000".into();
        record["status"] = "PARTIALLY_FILLED".into();
        record["sequenceNumber"] = 2.into();
        self.publish(record);
        self.until(|f| f.order().filled_qty().as_decimal() == dec!(0.003), 3)
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_modify_ack_partial_fields_partial_fill_and_same_id_replacement_reach_engine() {
    let mut f = Fixture::new().await;
    f.filled().await;
    f.client
        .modify_order(f.command(None, Some("50001.0")))
        .unwrap();
    f.until(
        |f| f.state.requests.lock().len() == 1 && f.order().is_pending_update(),
        3,
    )
    .await;
    assert_eq!(f.order().price().unwrap().as_decimal(), dec!(50000));
    assert_eq!(
        f.order()
            .events()
            .iter()
            .filter(|event| matches!(event, OrderEventAny::Updated(_)))
            .count(),
        0
    );
    let request = f.state.requests.lock()[0].clone();
    assert_eq!(request.quantity, dec!(0.010));
    assert!(request.reduce_only);
    assert_eq!(request.time_in_force, ArcusTimeInForce::Alo);
    assert_eq!(
        request.good_til_time,
        f.state.order.lock()["goodTilTime"].as_str().unwrap()
    );
    let mut interim = f.state.order.lock().clone();
    interim["status"] = "OPEN".into();
    interim["state"] = "CANCELED".into();
    interim["cancelReason"] = "MODIFY_CANCELED".into();
    interim["sequenceNumber"] = 3.into();
    f.publish(interim.clone());
    f.until(|f| !f.client.is_connected(), 3).await;
    let query = GenerateOrderStatusReport::new(
        UUID4::new(),
        now(),
        None,
        None,
        Some(VenueOrderId::from("AbCd1234")),
        None,
        None,
    );
    assert!(f.client.generate_order_status_report(&query).await.is_err());
    let mut unsequenced = interim.clone();
    unsequenced["cancelReason"] = Value::Null;
    unsequenced
        .as_object_mut()
        .unwrap()
        .remove("sequenceNumber");
    *f.state.order.lock() = unsequenced;
    assert!(f.client.generate_order_status_report(&query).await.is_err());
    assert!(!f.order().is_closed());
    assert!(f.order().is_pending_update());
    let mut replacement = interim.clone();
    replacement["status"] = "PLACED".into();
    replacement["state"] = Value::Null;
    replacement["cancelReason"] = Value::Null;
    replacement["price"] = "50001.0".into();
    replacement["sequenceNumber"] = 4.into();
    f.publish(replacement.clone());
    f.until(
        |f| {
            f.order().price().unwrap().as_decimal() == dec!(50001) && !f.order().is_pending_update()
        },
        3,
    )
    .await;
    assert_eq!(f.order().filled_qty().as_decimal(), dec!(0.003));
    assert_eq!(
        f.order().venue_order_id(),
        Some(VenueOrderId::from("AbCd1234"))
    );
    f.state.frames.send(json!({"type":"channel_data","channel":"orders","id":ADDRESS,"accountIndex":0,"contents":interim})).unwrap();
    f.client
        .modify_order(f.command(Some("0.00800000"), None))
        .unwrap();
    f.until(|f| f.state.requests.lock().len() == 2, 3).await;
    assert_eq!(f.state.requests.lock()[1].price, dec!(50001));
    replacement["originalSize"] = "0.00800000".into();
    replacement["remainingSize"] = "0.00500000".into();
    replacement["sequenceNumber"] = 5.into();
    f.publish(replacement);
    f.until(
        |f| f.order().quantity().as_decimal() == dec!(0.008) && !f.order().is_pending_update(),
        3,
    )
    .await;
    assert_eq!(f.order().filled_qty().as_decimal(), dec!(0.003));
    assert_eq!(f.order().leaves_qty().as_decimal(), dec!(0.005));
    assert_eq!(
        f.order()
            .events()
            .iter()
            .filter(|event| matches!(event, OrderEventAny::Updated(_)))
            .count(),
        2
    );
    assert!(
        f.client
            .modify_order(f.command(Some("0.00300000"), None))
            .is_err()
    );
    let mut trigger = f.command(None, Some("50002.0"));
    trigger.trigger_price = Some(Price::from("50003.0"));
    assert!(f.client.modify_order(trigger).is_err());
    assert_eq!(f.state.requests.lock().len(), 2);
    // Expiry is preserved exactly; an old resting order is not silently renewed.
    let mut expired = f.state.order.lock().clone();
    expired["goodTilTime"] = (now().as_u64() / 1000 + 86_400_000_000_u64)
        .to_string()
        .into();
    expired["sequenceNumber"] = 6.into();
    *f.state.order.lock() = expired;
    let query = GenerateOrderStatusReport::new(
        UUID4::new(),
        now(),
        None,
        None,
        Some(VenueOrderId::from("AbCd1234")),
        None,
        None,
    );
    f.client.generate_order_status_report(&query).await.unwrap();
    assert!(
        f.client
            .modify_order(f.command(None, Some("50003.0")))
            .is_err()
    );
    assert_eq!(f.state.requests.lock().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_modify_operation_rejection_preserves_original_and_fill_reset_fails_closed() {
    let mut f = Fixture::new().await;
    f.filled().await;
    f.client
        .modify_order(f.command(None, Some("50001.0")))
        .unwrap();
    f.until(
        |f| f.state.requests.lock().len() == 1 && f.order().is_pending_update(),
        3,
    )
    .await;
    let mut rejected = f.state.order.lock().clone();
    rejected["status"] = "REJECTED".into();
    rejected["rejectionReason"] = "MODIFY_WOULD_CROSS_OUTSIDE_RTH_TRADING_BOUNDARY".into();
    rejected["sequenceNumber"] = 3.into();
    f.publish(rejected);
    f.until(|f| !f.order().is_pending_update(), 3).await;
    assert!(!f.order().is_closed());
    assert_eq!(f.order().price().unwrap().as_decimal(), dec!(50000));
    let mut resting = f.state.order.lock().clone();
    resting["status"] = "PARTIALLY_FILLED".into();
    resting["rejectionReason"] = Value::Null;
    resting["sequenceNumber"] = 4.into();
    f.publish(resting.clone());
    f.client
        .modify_order(f.command(None, Some("50002.0")))
        .unwrap();
    f.until(|f| f.state.requests.lock().len() == 2, 3).await;
    resting["status"] = "CANCELED".into();
    resting["cancelReason"] = "MODIFY_CANCELED".into();
    resting["sequenceNumber"] = 5.into();
    f.publish(resting.clone());
    f.until(|f| !f.client.is_connected(), 3).await;
    // A nonzero replacement counter must not masquerade as a new total either.
    let mut ambiguous = resting.clone();
    ambiguous["status"] = "PLACED".into();
    ambiguous["cancelReason"] = Value::Null;
    ambiguous["price"] = "50002.0".into();
    ambiguous["originalSize"] = "0.00900000".into();
    ambiguous["remainingSize"] = "0.00600000".into();
    ambiguous["sequenceNumber"] = 6.into();
    f.publish(ambiguous);
    tokio::time::sleep(Duration::from_millis(100)).await;
    f.pump();
    assert_eq!(f.order().quantity().as_decimal(), dec!(0.010));
    assert_eq!(f.order().price().unwrap().as_decimal(), dec!(50000));
    resting["status"] = "PLACED".into();
    resting["cancelReason"] = Value::Null;
    resting["sequenceNumber"] = 7.into();
    resting["price"] = "50002.0".into();
    resting["filledSize"] = "0".into();
    resting["remainingSize"] = "0.01000000".into();
    f.publish(resting);
    tokio::time::sleep(Duration::from_millis(100)).await;
    f.pump();
    assert!(!f.client.is_connected());
    assert!(!f.order().is_closed());
    assert_eq!(f.order().filled_qty().as_decimal(), dec!(0.003));
    assert_eq!(f.order().price().unwrap().as_decimal(), dec!(50000));
    assert_eq!(f.state.requests.lock().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_http_timeout_cannot_clear_a_newer_modify_generation_after_ws_success() {
    let mut f = Fixture::new().await;
    f.state.mode.store(3, Ordering::Release);
    f.client
        .modify_order(f.command(None, Some("50001.0")))
        .unwrap();
    f.until(|f| f.state.requests.lock().len() == 1, 3).await;
    let mut record = f.state.order.lock().clone();
    record["price"] = "50001.0".into();
    record["sequenceNumber"] = 2.into();
    f.publish(record.clone());
    f.until(
        |f| {
            f.order().price().unwrap().as_decimal() == dec!(50001) && !f.order().is_pending_update()
        },
        3,
    )
    .await;
    f.state.mode.store(0, Ordering::Release);
    f.client
        .modify_order(f.command(None, Some("50002.0")))
        .unwrap();
    f.until(
        |f| f.state.requests.lock().len() == 2 && f.order().is_pending_update(),
        4,
    )
    .await;
    assert!(f.client.is_connected());
    record["price"] = "50002.0".into();
    record["sequenceNumber"] = 3.into();
    f.publish(record);
    f.until(
        |f| {
            f.order().price().unwrap().as_decimal() == dec!(50002) && !f.order().is_pending_update()
        },
        3,
    )
    .await;
    assert!(f.client.is_connected());
    assert_eq!(f.state.requests.lock().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_modify_definite_reject_preserves_original_and_unknown_timeout_recovers_without_retry()
{
    let mut f = Fixture::new().await;
    f.filled().await;
    f.state.mode.store(1, Ordering::Release);
    f.client
        .modify_order(f.command(None, Some("50001.0")))
        .unwrap();
    f.until(
        |f| f.state.requests.lock().len() == 1 && !f.order().is_pending_update(),
        3,
    )
    .await;
    assert!(!f.order().is_closed());
    assert!(f.client.is_connected());
    assert_eq!(f.order().price().unwrap().as_decimal(), dec!(50000));
    f.state.mode.store(3, Ordering::Release);
    f.client
        .modify_order(f.command(None, Some("50002.0")))
        .unwrap();
    f.until(
        |f| f.state.requests.lock().len() == 2 && !f.client.is_connected(),
        4,
    )
    .await;
    assert!(f.order().is_pending_update());
    assert!(!f.order().is_closed());
    assert!(
        f.client
            .modify_order(f.command(None, Some("50003.0")))
            .is_err()
    );
    let mut record = f.state.order.lock().clone();
    record["status"] = "CANCELED".into();
    record["cancelReason"] = "MODIFY_CANCELED".into();
    record["sequenceNumber"] = 3.into();
    *f.state.order.lock() = record.clone();
    let query = GenerateOrderStatusReport::new(
        UUID4::new(),
        now(),
        None,
        None,
        Some(VenueOrderId::from("AbCd1234")),
        None,
        None,
    );
    assert!(f.client.generate_order_status_report(&query).await.is_err());
    f.pump();
    assert!(!f.order().is_closed());
    record["status"] = "PLACED".into();
    record["cancelReason"] = Value::Null;
    record["price"] = "50002.0".into();
    record["sequenceNumber"] = 4.into();
    record["updatedAt"] = (now().as_u64() / 1000).into();
    *f.state.order.lock() = record;
    f.state.frames.send(json!({"_close":true})).unwrap();
    f.until(
        |f| f.client.is_connected() && f.order().price().unwrap().as_decimal() == dec!(50002),
        20,
    )
    .await;
    assert!(!f.order().is_pending_update());
    assert_eq!(f.order().filled_qty().as_decimal(), dec!(0.003));
    assert_eq!(f.state.requests.lock().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_removes_queued_unsent_modify_reservation_before_reconnect() {
    let mut f = Fixture::new().await;
    f.state.mode.store(3, Ordering::Release);
    f.client
        .modify_order(f.command(None, Some("50001.0")))
        .unwrap();
    f.until(|f| f.state.requests.lock().len() == 1, 3).await;
    let mut record = f.state.order.lock().clone();
    record["price"] = "50001.0".into();
    record["sequenceNumber"] = 2.into();
    f.publish(record);
    f.until(
        |f| {
            f.order().price().unwrap().as_decimal() == dec!(50001) && !f.order().is_pending_update()
        },
        3,
    )
    .await;
    f.client
        .modify_order(f.command(None, Some("50002.0")))
        .unwrap();
    f.client.disconnect().await.unwrap();
    f.pump();
    assert_eq!(f.state.requests.lock().len(), 1);
    f.state.mode.store(0, Ordering::Release);
    f.client.connect().await.unwrap();
    f.until(|f| f.client.is_connected(), 20).await;
    f.client
        .modify_order(f.command(None, Some("50002.0")))
        .unwrap();
    f.until(|f| f.state.requests.lock().len() == 2, 3).await;
}
