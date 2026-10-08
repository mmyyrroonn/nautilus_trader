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

//! Normal native factory and actual execution engine/portfolio acceptance against owned peers.

use std::collections::BTreeMap;

use nautilus_common::{
    clock::{Clock, TestClock},
    live::runner::replace_exec_event_sender,
    messages::{
        ExecutionReport,
        execution::{CancelOrder, QueryOrder},
    },
};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_hyperliquid::http::client::HyperliquidHttpClient;
use nautilus_model::{
    enums::{OrderSide, OrderStatus, TimeInForce},
    identifiers::{ClientId, StrategyId, VenueOrderId},
    orders::OrderAny,
    types::Currency,
};
use nautilus_portfolio::portfolio::Portfolio;
use rust_decimal::Decimal;
use tempfile::TempDir;

use super::*;

#[derive(Debug)]
pub(super) struct PeerExecution {
    pub posts: Vec<Value>,
    pub orders: BTreeMap<String, Value>,
    pub fills: Vec<Value>,
    pub acknowledge_posts: bool,
    pub observable_orders: bool,
    pub asset_data: Value,
}

impl Default for PeerExecution {
    fn default() -> Self {
        Self {
            posts: Vec::new(),
            orders: BTreeMap::new(),
            fills: Vec::new(),
            acknowledge_posts: true,
            observable_orders: true,
            asset_data: json!({"user":USER,"coin":"io:SNDK","leverage":{"type":"isolated","value":1},
                "maxTradeSzs":["1","1"],"availableToTrade":["100","100"],"markPx":"100"}),
        }
    }
}

pub(super) fn info_response(state: &PeerState, request: &Value) -> Option<Value> {
    if request["type"] == "metaAndAssetCtxs" {
        return Some(
            json!([state.meta(), [{"funding":"0","openInterest":"1","prevDayPx":"100", "dayNtlVlm":"1", "premium":"0", "oraclePx":"100", "markPx":"100", "midPx":"100", "impactPxs":["100","100"]}]]),
        );
    }
    let data = state.data.lock();
    let execution = data.execution.as_ref()?;
    match request["type"].as_str()? {
        "activeAssetData" => Some(execution.asset_data.clone()),
        "frontendOpenOrders" | "openOrders" => Some(json!(
            execution
                .orders
                .values()
                .filter(|order| { execution.observable_orders && order["status"] == "open" })
                .map(|order| order["order"].clone())
                .collect::<Vec<_>>()
        )),
        "orderStatus" => {
            let target = &request["oid"];
            let order = execution.orders.iter().find(|(cloid, order)| {
                target.as_str() == Some(cloid.as_str())
                    || target.as_u64() == order["order"]["oid"].as_u64()
            });
            Some(match order {
                Some((_, order)) if execution.observable_orders => {
                    json!({"status":"order","order":order})
                }
                _ => json!({"status":"unknownOid"}),
            })
        }
        "userFills" | "userFillsByTime" => Some(json!(execution.fills)),
        "historicalOrders" => Some(json!(
            execution.orders.values().cloned().collect::<Vec<_>>()
        )),
        _ => None,
    }
}

pub(super) fn post_response(state: &PeerState, post: &Value) -> Option<Value> {
    let mut data = state.data.lock();
    let execution = data.execution.as_mut()?;
    let action = post["request"]["payload"]["action"].clone();
    execution.posts.push(action.clone());
    let action_type = action["type"].as_str().unwrap_or("");
    let statuses = match action_type {
        "order" => {
            let wire = &action["orders"][0];
            assert_eq!(
                wire["a"], 110000,
                "the signed action must use the explicit io DEX asset"
            );
            let cloid = wire["c"].as_str().expect("owned action requires a CLOID");
            let oid = 70_000 + execution.posts.len() as u64;
            let ioc = wire["t"]["limit"]["tif"] == "Ioc";
            execution.orders.insert(
                cloid.to_string(),
                json!({"order":{
                "coin":"io:SNDK","side":if wire["b"] == true {"B"} else {"A"},
                "limitPx":wire["p"],"sz":if ioc {json!("0")} else {wire["s"].clone()},"origSz":wire["s"],"oid":oid,
                "timestamp":now_ms(),"cloid":cloid,"reduceOnly":wire["r"],
                "orderType":"Limit","tif":wire["t"]["limit"]["tif"]
            },"status":if ioc {"filled"} else {"open"},"statusTimestamp":now_ms()}),
            );
            if ioc {
                json!([{"filled":{"oid":oid,"totalSz":wire["s"],"avgPx":wire["p"]}}])
            } else {
                json!([{"resting":{"oid":oid}}])
            }
        }
        "cancel" | "cancelByCloid" => {
            let cancel = &action["cancels"][0];
            if action_type == "cancelByCloid" {
                assert_eq!(cancel["asset"], 110000);
                assert!(cancel["cloid"].as_str().is_some());
            } else {
                assert_eq!(cancel["a"], 110000);
            }
            for order in execution.orders.values_mut() {
                if (action_type == "cancel" && order["order"]["oid"] == cancel["o"])
                    || (action_type == "cancelByCloid"
                        && order["order"]["cloid"] == cancel["cloid"])
                {
                    order["status"] = json!("canceled");
                    order["statusTimestamp"] = json!(now_ms());
                }
            }
            json!(["success"])
        }
        _ => panic!("bounded execution peer received an unsupported action: {action}"),
    };
    if !execution.acknowledge_posts {
        return None;
    }
    let response_type = if action_type == "cancelByCloid" {
        "cancel"
    } else {
        action_type
    };
    Some(
        json!({"channel":"post","data":{"id":post["id"],"response":{"type":"action","payload":{
            "status":"ok","response":{"type":response_type,"data":{"statuses":statuses}}
        }}}}),
    )
}

fn policy(directory: &TempDir) -> Value {
    json!({"schema_version":1,"strategy_id":"ENTROPY-001",
        "journal_path":directory.path().join("io-intents.jsonl"),
        "max_order_notional":"100","max_gross_notional":"200","max_open_orders":2,"max_actions":16,
        "max_leverage":1,"margin_buffer":"5","fee_buffer_bps":"10", "action_timeout_ms":1000,
        "recovery_timeout_ms":1500,"recovery_max_attempts":2,"recovery_retry_delay_ms":10,
        "metadata_max_age_ms":30000,
        "symbols":[{"instrument_id":"io:SNDK-USD-PERP.HYPERLIQUID","max_quantity":"1","min_price":"95","max_price":"105"}]})
}

struct Harness {
    peer: Peer,
    directory: TempDir,
    factory: HyperliquidExecutionClientFactory,
    client: Box<dyn ExecutionClient>,
    cache: Rc<RefCell<Cache>>,
    engine: ExecutionEngine,
    portfolio: Portfolio,
    receiver: UnboundedReceiver<ExecutionEvent>,
    fill_events: usize,
}

impl Harness {
    async fn new() -> Self {
        Self::with_balance("100", |_| {}).await
    }

    async fn with_balance(balance: &str, change: impl FnOnce(&mut PeerExecution)) -> Self {
        let peer = Peer::start(clearinghouse(balance, balance, "0", balance, false)).await;
        let directory = TempDir::new().unwrap();
        let mut execution = PeerExecution::default();
        change(&mut execution);
        peer.state.data.lock().execution = Some(execution);
        let mut config = execution_config(&peer, 30000);
        config.io_execution_policy_json = Some(policy(&directory).to_string());
        let cache = Rc::new(RefCell::new(Cache::default()));
        // A normal node loads instruments through its provider before applying fills.
        // Seed the real native cache from this peer's public metadata using that HTTP API.
        let mut provider =
            HyperliquidHttpClient::new(HyperliquidEnvironment::Testnet, 2, None).unwrap();
        provider.set_base_info_url(format!("http://{}/info", peer.addr));
        for instrument in provider.request_instruments().await.unwrap() {
            cache.borrow_mut().add_instrument(instrument).unwrap();
        }
        let factory = HyperliquidExecutionClientFactory::new();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        set_exec_event_sender(sender);
        let mut client = factory
            .create(
                TraderId::from("TESTER-001"),
                "HYPERLIQUID",
                &config,
                cache.clone().into(),
            )
            .unwrap();
        client.start().unwrap();
        let (connected, registered) =
            tokio::join!(client.connect(), register_account(&mut receiver, &cache));
        connected.unwrap();
        registered.unwrap();
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
        let portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
        let mut engine = ExecutionEngine::new(clock, cache.clone(), None);
        engine.register_oms_type(StrategyId::from("ENTROPY-001"), OmsType::Netting);
        let mut result = Self {
            peer,
            directory,
            factory,
            client,
            cache,
            engine,
            portfolio,
            receiver,
            fill_events: 0,
        };
        result.wait_ready().await;
        result
    }

    fn scope(&self) -> Value {
        serde_json::from_str(
            &self
                .factory
                .execution_scope_snapshot_json()
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    }

    async fn wait_ready(&mut self) {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                self.apply_events();
                let scope = self.scope();
                if scope["recovery_complete"] == true && scope["account"]["trusted"] == true {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("execution proof did not become complete: {}", self.scope()));
    }

    fn order(
        &self,
        id: &str,
        side: OrderSide,
        quantity: &str,
        price: &str,
        reduce_only: bool,
        tif: TimeInForce,
    ) -> OrderAny {
        OrderTestBuilder::new(OrderType::Limit)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("ENTROPY-001"))
            .instrument_id(InstrumentId::from("io:SNDK-USD-PERP.HYPERLIQUID"))
            .client_order_id(ClientOrderId::from(id))
            .side(side)
            .quantity(Quantity::from(quantity))
            .price(Price::from(price))
            .time_in_force(tif)
            .post_only(false)
            .reduce_only(reduce_only)
            .ts_init(UnixNanos::from(now_ms() * 1_000_000))
            .build()
    }

    fn submit(&self, order: &OrderAny) -> anyhow::Result<()> {
        self.cache
            .borrow_mut()
            .add_order(
                order.clone(),
                None,
                Some(ClientId::from("HYPERLIQUID")),
                false,
            )
            .unwrap();
        self.client.submit_order(SubmitOrder::from_order(
            order,
            order.trader_id(),
            Some(ClientId::from("HYPERLIQUID")),
            None,
            UUID4::new(),
            UnixNanos::from(now_ms() * 1_000_000),
        ))
    }

    fn apply_event(&mut self, event: ExecutionEvent) {
        match event {
            ExecutionEvent::Order(event) => {
                if matches!(&event, OrderEventAny::Filled(_)) {
                    self.fill_events += 1;
                }
                self.engine.process(&event);
            }
            ExecutionEvent::Report(report) => {
                if matches!(&report, ExecutionReport::Fill(_)) {
                    self.fill_events += 1;
                }
                self.engine.reconcile_execution_report(&report);
            }
            ExecutionEvent::Account(account) => self.portfolio.update_account(&account),
            _ => {}
        }
    }

    fn apply_events(&mut self) {
        while let Ok(event) = self.receiver.try_recv() {
            self.apply_event(event);
        }
    }

    async fn wait_status(&mut self, id: ClientOrderId, status: OrderStatus) {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                self.apply_events();
                if self.cache.borrow().order(&id).unwrap().status() == status {
                    break;
                }
                let event = self.receiver.recv().await.unwrap();
                self.apply_event(event);
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "order {id} actual={:?} expected={status:?}, proof={}",
                self.cache.borrow().order(&id).unwrap().status(),
                self.scope()
            )
        });
    }

    async fn wait_posts(&mut self, count: usize) {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                self.apply_events();
                if self.peer.state.writes.load(Ordering::SeqCst) >= count {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {count} actual actions; proof={}", self.scope()));
    }

    fn fill(
        &self,
        post_index: usize,
        tid: u64,
        quantity: &str,
        price: &str,
        fee: &str,
        start: &str,
    ) -> Value {
        let data = self.peer.state.data.lock();
        let execution = data.execution.as_ref().unwrap();
        let wire = &execution.posts[post_index]["orders"][0];
        let cloid = wire["c"].as_str().unwrap();
        let oid = execution.orders[cloid]["order"]["oid"].clone();
        json!({"coin":"io:SNDK","px":price,"sz":quantity,"side":if wire["b"] == true {"B"} else {"A"},
            "time":now_ms(),"startPosition":start,"dir":if wire["r"] == true {"Close Long"} else {"Open Long"},
            "closedPnl":"0","hash":format!("0x{tid:064x}"),"oid":oid,"crossed":true,
            "fee":fee,"builderFee":"0.0002","tid":tid,"feeToken":"USDC","cloid":cloid})
    }

    fn send_fill(&self, fill: Value) {
        self.peer
            .state
            .data
            .lock()
            .execution
            .as_mut()
            .unwrap()
            .fills
            .push(fill.clone());
        self.peer
            .state
            .instructions
            .send(PeerInstruction::Frame(
                json!({"channel":"user","data":{"fills":[fill]}}),
            ))
            .unwrap();
    }

    fn set_position(&self, quantity: &str) {
        let mut data = self.peer.state.data.lock();
        let mut io = clearinghouse("100", "100", "0", "100", quantity != "0");
        if quantity != "0" {
            io["assetPositions"][0]["position"]["szi"] = json!(quantity);
            io["assetPositions"][0]["position"]["leverage"]["value"] = json!(1);
        }
        // This synthetic source version is carried by the actual private WS
        // snapshot; it is not a source timestamp invented by the adapter.
        io["time"] = json!(now_ms());
        data.io = io;
        drop(data);
        self.peer
            .state
            .instructions
            .send(PeerInstruction::Frame(
                self.peer.state.clearinghouse_frame(),
            ))
            .unwrap();
    }

    fn terminal(&self, post_index: usize, status: &str) -> Value {
        let mut data = self.peer.state.data.lock();
        let execution = data.execution.as_mut().unwrap();
        let cloid = execution.posts[post_index]["orders"][0]["c"]
            .as_str()
            .unwrap()
            .to_string();
        let order = execution.orders.get_mut(&cloid).unwrap();
        order["status"] = json!(status);
        order["statusTimestamp"] = json!(now_ms());
        order["order"]["sz"] = json!("0");
        order.clone()
    }

    async fn refresh(&mut self) {
        self.wait_latest_position_snapshot().await;
        self.client
            .query_account(QueryAccount::new(
                TraderId::from("TESTER-001"),
                Some(ClientId::from("HYPERLIQUID")),
                AccountId::from("HYPERLIQUID-ENTROPY"),
                UUID4::new(),
                UnixNanos::from(now_ms() * 1_000_000),
                None,
                None,
            ))
            .unwrap();
        self.wait_ready().await;
    }

    async fn refresh_owned(&mut self, order: &OrderAny) {
        self.wait_latest_position_snapshot().await;
        self.client
            .query_order(QueryOrder::new(
                order.trader_id(),
                Some(ClientId::from("HYPERLIQUID")),
                order.strategy_id(),
                order.instrument_id(),
                order.client_order_id(),
                None,
                UUID4::new(),
                UnixNanos::from(now_ms() * 1_000_000),
                None,
                None,
            ))
            .unwrap();
        self.wait_ready().await;
    }

    async fn wait_latest_position_snapshot(&self) {
        let source_time = self.peer.state.data.lock().io["time"].as_u64();
        if let Some(source_time) = source_time {
            tokio::time::timeout(Duration::from_secs(2), async {
                while self.scope()["account"]["ws_source_time_ms"] != source_time {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "new actual private snapshot was not observed: {}",
                    self.scope()
                )
            });
        }
    }

    async fn stop(&mut self) {
        self.client.disconnect().await.unwrap();
        assert!(!self.client.is_connected());
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.peer.state.active.load(Ordering::SeqCst) > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(self.directory.path().join("io-intents.jsonl").is_file());
    }
}

#[tokio::test]
async fn factory_actual_partial_fills_fees_and_owned_ioc_close_update_engine_and_portfolio() {
    let mut harness = Harness::new().await;
    let order = harness.order(
        "E-ENTRY-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    let first = harness.fill(0, 101, "0.1", "99.75", "-0.001", "0");
    {
        let mut data = harness.peer.state.data.lock();
        let execution = data.execution.as_mut().unwrap();
        let cloid = execution.posts[0]["orders"][0]["c"]
            .as_str()
            .unwrap()
            .to_string();
        execution.orders.get_mut(&cloid).unwrap()["order"]["sz"] = json!("0.1");
    }
    harness.set_position("0.1");
    harness.send_fill(first.clone());
    harness
        .wait_status(order.client_order_id(), OrderStatus::PartiallyFilled)
        .await;
    assert_eq!(harness.fill_events, 1);
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::from_str_exact("0.1").unwrap()
    );
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .commissions()
            .get(&Currency::USDC())
            .copied(),
        Some(Money::from("-0.001 USDC"))
    );
    harness.send_fill(first);
    let status = harness.terminal(0, "filled");
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::Frame(
            json!({"channel":"orderUpdates","data":[status]}),
        ))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    harness.apply_events();
    assert_eq!(
        harness.fill_events, 1,
        "replays and Filled markers must not fabricate fills"
    );
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .filled_qty()
            .as_decimal(),
        Decimal::from_str_exact("0.1").unwrap()
    );
    let second = harness.fill(0, 102, "0.1", "100", "0.002", "0.1");
    harness.set_position("0.2");
    harness.send_fill(second);
    harness
        .wait_status(order.client_order_id(), OrderStatus::Filled)
        .await;
    // Preserve the production 1200/min HTTP quota. Full role/mode/collateral
    // verification costs 182 weight, so explicitly pace this finite workflow.
    tokio::time::sleep(Duration::from_secs(25)).await;
    harness.refresh().await;
    assert_eq!(harness.fill_events, 2);
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .commissions()
            .get(&Currency::USDC())
            .copied(),
        Some(Money::from("0.001 USDC"))
    );
    let close = harness.order(
        "E-CLOSE-001",
        OrderSide::Sell,
        "0.2",
        "99",
        true,
        TimeInForce::Ioc,
    );
    harness.submit(&close).unwrap();
    harness.wait_posts(2).await;
    harness
        .wait_status(close.client_order_id(), OrderStatus::Accepted)
        .await;
    {
        let data = harness.peer.state.data.lock();
        let wire = &data.execution.as_ref().unwrap().posts[1]["orders"][0];
        assert_eq!(wire["r"], true);
        assert_eq!(wire["b"], false);
        assert_eq!(wire["p"], "99");
        assert_eq!(wire["t"]["limit"]["tif"], "Ioc");
    }
    let closing_fill = harness.fill(1, 103, "0.2", "99.5", "0.0015", "0.2");
    harness.set_position("0");
    harness.terminal(1, "filled");
    harness.send_fill(closing_fill);
    harness
        .wait_status(close.client_order_id(), OrderStatus::Filled)
        .await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    harness.refresh_owned(&close).await;
    assert_eq!(harness.fill_events, 3);
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&close.client_order_id())
            .unwrap()
            .commissions()
            .get(&Currency::USDC())
            .copied(),
        Some(Money::from("0.0015 USDC"))
    );
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::ZERO
    );
    assert_eq!(harness.scope()["account"]["flat"], true);
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 2);
    harness.stop().await;
}

#[tokio::test]
async fn exact_price_precision_failure_never_reaches_peer() {
    let mut harness = Harness::new().await;
    let invalid = harness.order(
        "E-PRICE-001",
        OrderSide::Buy,
        "0.2",
        "100.001",
        false,
        TimeInForce::Ioc,
    );
    let _ = harness.submit(&invalid);
    harness
        .wait_status(invalid.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 0);
    assert_eq!(harness.fill_events, 0);
    harness.stop().await;
}

#[rstest]
#[case("1", "100")]
#[case("100", "1")]
#[tokio::test]
async fn newer_private_funds_decline_limits_new_risk_before_http_refresh(
    #[case] equity: &str,
    #[case] withdrawable: &str,
) {
    let mut harness = Harness::new().await;
    let source_time = now_ms();
    let mut private = clearinghouse(equity, equity, "0", withdrawable, false);
    private["time"] = json!(source_time);
    harness.peer.state.data.lock().ws_financial_override = Some(private);
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::Frame(
            harness.peer.state.clearinghouse_frame(),
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while harness.scope()["account"]["ws_source_time_ms"] != source_time {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(harness.scope()["account"]["free"], "100");
    assert_eq!(harness.scope()["account"]["withdrawable"], "100");
    assert_eq!(harness.scope()["account"]["trusted"], true);
    assert_eq!(harness.scope()["recovery_complete"], true);
    let witness = harness.scope()["latest_private_funds"].clone();
    assert_eq!(witness["free"], equity);
    assert_eq!(witness["withdrawable"], withdrawable);
    assert_eq!(witness["source_time_ms"], source_time);
    assert!(witness["received_time_ms"].as_u64().unwrap() >= source_time);
    let order = harness.order(
        "E-PRIVATE-FUNDS",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let denied = harness.submit(&order).unwrap_err();
    assert!(
        denied
            .to_string()
            .contains("Insufficient conservative io funds"),
        "unexpected admission reason: {denied}"
    );
    harness
        .wait_status(order.client_order_id(), OrderStatus::Denied)
        .await;
    let cache = harness.cache.borrow();
    let cached = cache.order(&order.client_order_id()).unwrap();
    let OrderEventAny::Denied(event) = cached.last_event() else {
        panic!("expected native denial event");
    };
    assert!(
        event
            .reason
            .as_str()
            .contains("Insufficient conservative io funds")
    );
    drop(cached);
    drop(cache);
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::ZERO
    );
    harness.stop().await;
}

#[tokio::test]
async fn concurrent_orders_cannot_spend_one_balance_twice() {
    let mut harness = Harness::with_balance("30", |_| {}).await;
    let first = harness.order(
        "E-BUDGET-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let second = harness.order(
        "E-BUDGET-002",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let _ = harness.submit(&first);
    let _ = harness.submit(&second);
    harness.wait_posts(1).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    harness.apply_events();
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    let statuses = [first.client_order_id(), second.client_order_id()]
        .map(|id| harness.cache.borrow().order(&id).unwrap().status());
    assert!(statuses.contains(&OrderStatus::Accepted));
    assert!(statuses.contains(&OrderStatus::Denied) || statuses.contains(&OrderStatus::Rejected));
    harness.stop().await;
}

#[tokio::test]
async fn owned_cancel_uses_exact_cached_order_and_dedicated_io_asset() {
    let mut harness = Harness::new().await;
    let order = harness.order(
        "E-CANCEL-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    let oid = harness
        .cache
        .borrow()
        .order(&order.client_order_id())
        .unwrap()
        .venue_order_id();
    let command = |instrument, venue_order_id| {
        CancelOrder::new(
            order.trader_id(),
            Some(ClientId::from("HYPERLIQUID")),
            order.strategy_id(),
            instrument,
            order.client_order_id(),
            venue_order_id,
            UUID4::new(),
            UnixNanos::from(now_ms() * 1_000_000),
            None,
            None,
        )
    };
    let _ = harness
        .client
        .cancel_order(command(InstrumentId::from("BTC-USD-PERP.HYPERLIQUID"), oid));
    let _ = harness.client.cancel_order(command(
        order.instrument_id(),
        Some(VenueOrderId::from("999999")),
    ));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    harness
        .client
        .cancel_order(command(order.instrument_id(), oid))
        .unwrap();
    harness.wait_posts(2).await;
    harness.wait_ready().await;
    harness
        .wait_status(order.client_order_id(), OrderStatus::Canceled)
        .await;
    assert_eq!(harness.fill_events, 0);
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::ZERO
    );
    harness.stop().await;
}

#[tokio::test]
async fn missing_ack_and_unknown_oid_retain_budget_and_restart_never_resends() {
    let mut harness = Harness::with_balance("100", |peer| {
        peer.acknowledge_posts = false;
        peer.observable_orders = false;
    })
    .await;
    let order = harness.order(
        "E-UNKNOWN-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness.wait_posts(1).await;
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let scope = harness.scope();
            if scope["owned_intents"]["E-UNKNOWN-001"]["phase"] == "unknown"
                && scope["diagnostic"].as_str().is_some_and(|text| {
                    text.contains("RecoveryIncomplete") && text.contains("unknownOid")
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "possibly accepted intent did not remain Unknown: {}",
            harness.scope()
        )
    });
    harness.apply_events();
    assert!(!matches!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Denied | OrderStatus::Rejected | OrderStatus::Canceled | OrderStatus::Filled
    ));
    let reservation = harness.scope()["owned_intents"]["E-UNKNOWN-001"]["reservation"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(Decimal::from_str_exact(&reservation).unwrap() > Decimal::ZERO);
    let another = harness.order(
        "E-UNKNOWN-002",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let _ = harness.submit(&another);
    harness
        .wait_status(another.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    let original_cloid = harness
        .peer
        .state
        .data
        .lock()
        .execution
        .as_ref()
        .unwrap()
        .posts[0]["orders"][0]["c"]
        .clone();
    let requests_before_restart = harness.peer.state.data.lock().requests.len();
    harness.stop().await;
    let Harness {
        peer,
        directory,
        client,
        factory,
        ..
    } = harness;
    drop(client);
    drop(factory);
    let mut config = execution_config(&peer, 30000);
    config.io_execution_policy_json = Some(policy(&directory).to_string());
    let cache = Rc::new(RefCell::new(Cache::default()));
    let factory = HyperliquidExecutionClientFactory::new();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(sender);
    let mut client = factory
        .create(
            TraderId::from("TESTER-001"),
            "HYPERLIQUID",
            &config,
            cache.clone().into(),
        )
        .unwrap();
    client.start().unwrap();
    let (connected, account) =
        tokio::join!(client.connect(), register_account(&mut receiver, &cache));
    connected.unwrap();
    account.unwrap();
    let restored: Value =
        serde_json::from_str(&factory.execution_scope_snapshot_json().unwrap().unwrap()).unwrap();
    assert_eq!(
        restored["owned_intents"]["E-UNKNOWN-001"]["cloid"],
        original_cloid
    );
    assert_eq!(
        restored["owned_intents"]["E-UNKNOWN-001"]["phase"],
        "unknown"
    );
    assert_eq!(
        restored["owned_intents"]["E-UNKNOWN-001"]["reservation"],
        reservation
    );
    assert_eq!(restored["recovery_complete"], false);
    assert_eq!(
        peer.state.writes.load(Ordering::SeqCst),
        1,
        "restart must query, never resubmit"
    );
    let requests = peer.state.data.lock().requests.clone();
    assert!(
        requests[requests_before_restart..]
            .iter()
            .any(|request| request["type"] == "orderStatus"
                && request["user"] == USER
                && request["oid"] == original_cloid)
    );
    assert!(
        requests[requests_before_restart..]
            .iter()
            .any(|request| request["type"] == "frontendOpenOrders" && request["dex"] == "io")
    );
    client.disconnect().await.unwrap();
}

#[rstest]
#[case("px", "100.25")]
#[case("sz", "0.15")]
#[case("fee", "0.005")]
#[tokio::test]
async fn same_raw_trade_identity_with_changed_economics_is_a_conflict_even_after_terminal(
    #[case] field: &str,
    #[case] changed: &str,
) {
    let mut harness = Harness::new().await;
    let order = harness.order(
        "E-CONFLICT-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    let fill = harness.fill(0, 201, "0.2", "100", "0.002", "0");
    harness.set_position("0.2");
    harness.terminal(0, "filled");
    harness.send_fill(fill.clone());
    harness
        .wait_status(order.client_order_id(), OrderStatus::Filled)
        .await;
    harness.refresh_owned(&order).await;
    let mut conflict = fill;
    conflict[field] = json!(changed);
    harness.send_fill(conflict);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            harness.apply_events();
            if harness.scope()["recovery_complete"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "altered raw trade identity did not invalidate proof: {}",
            harness.scope()
        )
    });
    assert_eq!(harness.fill_events, 1);
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::from_str_exact("0.2").unwrap()
    );
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .trade_ids()
            .len(),
        1
    );
    let blocked = harness.order(
        "E-CONFLICT-002",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let _ = harness.submit(&blocked);
    harness
        .wait_status(blocked.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    harness.stop().await;
}

#[tokio::test]
async fn actual_user_leverage_change_invalidates_new_risk_without_mutating_leverage() {
    let mut harness = Harness::new().await;
    let prior_requests = harness.peer.state.data.lock().requests.len();
    harness
        .peer
        .state
        .data
        .lock()
        .execution
        .as_mut()
        .unwrap()
        .asset_data["leverage"]["value"] = json!(2);
    harness
        .client
        .query_account(QueryAccount::new(
            TraderId::from("TESTER-001"),
            Some(ClientId::from("HYPERLIQUID")),
            AccountId::from("HYPERLIQUID-ENTROPY"),
            UUID4::new(),
            UnixNanos::from(now_ms() * 1_000_000),
            None,
            None,
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let scope = harness.scope();
            if scope["diagnostic"].as_str().is_some_and(|text| {
                text.contains("verification failed")
                    && text.contains("out-of-policy actual leverage")
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        harness.peer.state.data.lock().requests[prior_requests..]
            .iter()
            .any(|request| request["type"] == "activeAssetData"
                && request["user"] == USER
                && request["coin"] == "io:SNDK")
    );
    let order = harness.order(
        "E-LEVERAGE-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let _ = harness.submit(&order);
    harness
        .wait_status(order.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 0);
    assert_eq!(harness.scope()["user_asset_source_time_ms"], Value::Null);
    harness.stop().await;
}

#[tokio::test]
async fn foreign_io_open_order_revokes_ownership_without_adoption() {
    let mut harness = Harness::new().await;
    let foreign = json!({"order":{"coin":"io:SNDK","side":"B","limitPx":"100",
        "sz":"0.2","origSz":"0.2","oid":99999,"timestamp":now_ms(),
        "cloid":"0xffffffffffffffffffffffffffffffff","reduceOnly":false,
        "orderType":"Limit","tif":"Gtc"},"status":"open","statusTimestamp":now_ms()});
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::Frame(
            json!({"channel":"orderUpdates","data":[foreign]}),
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while harness.scope()["recovery_complete"] != false {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    harness.apply_events();
    assert!(
        harness.scope()["owned_intents"]
            .as_object()
            .unwrap()
            .is_empty()
    );
    assert_eq!(harness.fill_events, 0);
    let order = harness.order(
        "E-FOREIGN-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let _ = harness.submit(&order);
    harness
        .wait_status(order.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 0);
    harness.stop().await;
}

#[rstest]
#[case("user", false)]
#[case("userEvents", false)]
#[case("user", true)]
#[tokio::test]
async fn unsupported_private_economic_frames_revoke_trust_before_new_risk(
    #[case] channel: &str,
    #[case] liquidation: bool,
) {
    let mut harness = Harness::new().await;
    let data = if liquidation {
        json!({"liquidation":{"lid":123,"liquidator":"0x1111111111111111111111111111111111111111",
            "liquidated_user":USER,"liquidated_ntl_pos":"10","liquidated_account_value":"100"}})
    } else {
        json!({"funding":{"time":now_ms(),"coin":"io:SNDK","usdc":"-0.01","szi":"0.2","fundingRate":"0.0005"}})
    };
    let frame = json!({"channel":channel,"data":data});
    let _: nautilus_hyperliquid::websocket::messages::HyperliquidWsMessage =
        serde_json::from_value(frame.clone()).unwrap();
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::Frame(frame))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            harness.apply_events();
            let snapshot = harness.scope();
            if snapshot["recovery_complete"] == false && snapshot["account"]["trusted"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "unhandled private {channel} did not revoke proof: {}",
            harness.scope()
        )
    });
    assert_eq!(harness.fill_events, 0);
    let order = harness.order(
        "E-ECONOMIC-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let _ = harness.submit(&order);
    harness
        .wait_status(order.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 0);
    harness.stop().await;
}

#[rstest]
#[case("cloid", "0xffffffffffffffffffffffffffffffff")]
#[case("sz", "0.3")]
#[case("fee", "0.000000000000000001")]
#[case("px", "100.25")]
#[tokio::test]
async fn contradictory_or_unprojectable_actual_fill_blocks_without_native_position(
    #[case] field: &str,
    #[case] changed: &str,
) {
    let mut harness = Harness::new().await;
    let order = harness.order(
        "E-RAW-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    let mut fill = harness.fill(0, 401, "0.2", "100", "0.002", "0");
    fill[field] = json!(changed);
    harness.send_fill(fill);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            harness.apply_events();
            if harness.scope()["recovery_complete"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "invalid {field} did not close recovery: {}",
            harness.scope()
        )
    });
    assert_eq!(harness.fill_events, 0);
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::ZERO
    );
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .filled_qty()
            .as_decimal(),
        Decimal::ZERO
    );
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    harness.stop().await;
}

#[tokio::test]
async fn durable_actual_fill_does_not_claim_native_cache_restored_after_fresh_factory_restart() {
    let mut harness = Harness::new().await;
    let order = harness.order(
        "E-PROJECTION-001",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    let fill = harness.fill(0, 301, "0.2", "100", "0.002", "0");
    harness.set_position("0.2");
    harness.terminal(0, "filled");
    harness.send_fill(fill);
    harness
        .wait_status(order.client_order_id(), OrderStatus::Filled)
        .await;
    harness.refresh_owned(&order).await;
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::from_str_exact("0.2").unwrap()
    );
    harness.stop().await;
    let Harness {
        peer,
        directory,
        client,
        factory,
        ..
    } = harness;
    drop(client);
    drop(factory);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let factory = HyperliquidExecutionClientFactory::new();
    let mut config = execution_config(&peer, 30000);
    config.io_execution_policy_json = Some(policy(&directory).to_string());
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(sender);
    let mut client = factory
        .create(
            order.trader_id(),
            "HYPERLIQUID",
            &config,
            cache.clone().into(),
        )
        .unwrap();
    client.start().unwrap();
    let (connected, registered) =
        tokio::join!(client.connect(), register_account(&mut receiver, &cache));
    connected.unwrap();
    registered.unwrap();
    let scope: Value =
        serde_json::from_str(&factory.execution_scope_snapshot_json().unwrap().unwrap()).unwrap();
    assert_eq!(scope["native_projection_recovery_required"], true);
    assert_eq!(scope["recovery_complete"], false);
    assert_eq!(scope["actual_fills"].as_object().unwrap().len(), 1);
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
    let portfolio = Portfolio::new(clock, cache.clone(), None);
    assert_eq!(
        portfolio.net_position(&order.instrument_id()),
        Decimal::ZERO,
        "a fresh native cache has not applied the historical fill"
    );
    let close = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(order.trader_id())
        .strategy_id(order.strategy_id())
        .instrument_id(order.instrument_id())
        .client_order_id(ClientOrderId::from("E-PROJECTION-CLOSE"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.2"))
        .price(Price::from("99"))
        .time_in_force(TimeInForce::Ioc)
        .reduce_only(true)
        .post_only(false)
        .build();
    cache
        .borrow_mut()
        .add_order(
            close.clone(),
            None,
            Some(ClientId::from("HYPERLIQUID")),
            false,
        )
        .unwrap();
    let _ = client.submit_order(SubmitOrder::from_order(
        &close,
        close.trader_id(),
        Some(ClientId::from("HYPERLIQUID")),
        None,
        UUID4::new(),
        UnixNanos::from(now_ms() * 1_000_000),
    ));
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        peer.state.writes.load(Ordering::SeqCst),
        1,
        "do not close from a fabricated restored cache"
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn partial_fill_then_cancel_preserves_actual_position_and_rejects_oversized_close() {
    let mut harness = Harness::new().await;
    let order = harness.order(
        "E-PARTIAL-CANCEL",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    {
        let mut data = harness.peer.state.data.lock();
        let execution = data.execution.as_mut().unwrap();
        let cloid = execution.posts[0]["orders"][0]["c"]
            .as_str()
            .unwrap()
            .to_string();
        execution.orders.get_mut(&cloid).unwrap()["order"]["sz"] = json!("0.1");
    }
    harness.set_position("0.1");
    harness.send_fill(harness.fill(0, 501, "0.1", "100", "0.001", "0"));
    harness
        .wait_status(order.client_order_id(), OrderStatus::PartiallyFilled)
        .await;
    harness.refresh_owned(&order).await;
    // Refill the real HTTP quota before cancel's terminal/account verification.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let oid = harness
        .cache
        .borrow()
        .order(&order.client_order_id())
        .unwrap()
        .venue_order_id();
    harness
        .client
        .cancel_order(CancelOrder::new(
            order.trader_id(),
            Some(ClientId::from("HYPERLIQUID")),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            oid,
            UUID4::new(),
            UnixNanos::from(now_ms() * 1_000_000),
            None,
            None,
        ))
        .unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Canceled)
        .await;
    harness.wait_ready().await;
    assert_eq!(harness.fill_events, 1);
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::from_str_exact("0.1").unwrap()
    );
    let oversized = harness.order(
        "E-OVERSIZED-CLOSE",
        OrderSide::Sell,
        "0.2",
        "99",
        true,
        TimeInForce::Ioc,
    );
    let _ = harness.submit(&oversized);
    harness
        .wait_status(oversized.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 2);
    harness.stop().await;
}

#[tokio::test]
async fn observed_same_size_isolated_leverage_change_revokes_old_asset_proof() {
    let mut harness = Harness::new().await;
    let order = harness.order(
        "E-WS-LEVERAGE",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    harness.set_position("0.2");
    harness.terminal(0, "filled");
    harness.send_fill(harness.fill(0, 502, "0.2", "100", "0.001", "0"));
    harness
        .wait_status(order.client_order_id(), OrderStatus::Filled)
        .await;
    harness.refresh_owned(&order).await;
    {
        let mut data = harness.peer.state.data.lock();
        data.io["assetPositions"][0]["position"]["leverage"]["value"] = json!(2);
    }
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::Frame(
            harness.peer.state.clearinghouse_frame(),
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = harness.scope();
            if snapshot["recovery_complete"] == false && snapshot["account"]["trusted"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "same-size leverage drift retained old proof: {}",
            harness.scope()
        )
    });
    let blocked = harness.order(
        "E-WS-LEVERAGE-NEW",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let _ = harness.submit(&blocked);
    harness
        .wait_status(blocked.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    assert_eq!(harness.fill_events, 1);
    harness.stop().await;
}

#[tokio::test]
async fn query_account_total_deadline_bounds_private_verification_and_preserves_unknown() {
    let mut harness = Harness::new().await;
    harness.peer.state.data.lock().io_response_delay_ms = 5000;
    let started = tokio::time::Instant::now();
    harness
        .client
        .query_account(QueryAccount::new(
            TraderId::from("TESTER-001"),
            Some(ClientId::from("HYPERLIQUID")),
            AccountId::from("HYPERLIQUID-ENTROPY"),
            UUID4::new(),
            UnixNanos::from(now_ms() * 1_000_000),
            None,
            None,
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !harness.scope()["diagnostic"]
            .as_str()
            .is_some_and(|text| text.contains("total deadline exhausted"))
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(harness.scope()["account"]["trusted"], false);
    assert_eq!(harness.scope()["recovery_complete"], false);
    let order = harness.order(
        "E-QUERY-DEADLINE",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    let _ = harness.submit(&order);
    harness
        .wait_status(order.client_order_id(), OrderStatus::Denied)
        .await;
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 0);
    harness.stop().await;
}

#[tokio::test]
async fn unsupported_user_fills_with_native_report_content_cannot_advance_trusted_scope() {
    let mut harness = Harness::new().await;
    let order = harness.order(
        "E-USERFILLS",
        OrderSide::Buy,
        "0.2",
        "100",
        false,
        TimeInForce::Gtc,
    );
    harness.submit(&order).unwrap();
    harness
        .wait_status(order.client_order_id(), OrderStatus::Accepted)
        .await;
    harness.wait_ready().await;
    let frame = json!({"channel":"userFills","data":{"user":USER,"isSnapshot":false,
        "fills":[harness.fill(0, 503, "0.2", "100", "0.001", "0")]}});
    let _: nautilus_hyperliquid::websocket::messages::HyperliquidWsMessage =
        serde_json::from_value(frame.clone()).unwrap();
    harness
        .peer
        .state
        .instructions
        .send(PeerInstruction::Frame(frame))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            harness.apply_events();
            let snapshot = harness.scope();
            if snapshot["recovery_complete"] == false && snapshot["account"]["trusted"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(harness.fill_events, 0);
    assert_eq!(
        harness.portfolio.net_position(&order.instrument_id()),
        Decimal::ZERO
    );
    assert_eq!(harness.peer.state.writes.load(Ordering::SeqCst), 1);
    harness.stop().await;
}
