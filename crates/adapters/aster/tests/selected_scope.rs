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

//! Normal-factory selected-account proofs against actual local HTTP and private sockets.

mod common;

use std::{cell::RefCell, rc::Rc, time::Duration};

use common::{MockVenue, SubmitOutcome, TEST_PRIVATE_KEY};
use nautilus_aster::{
    common::enums::AsterEnvironment, config::AsterExecutionClientConfig,
    factories::AsterExecutionClientFactory, signing::AsterEip712Signer,
};
use nautilus_binance::config::BinanceInstrumentProviderConfig;
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    factories::ExecutionClientFactory,
    live::runner::{replace_data_event_sender, replace_exec_event_sender},
    messages::{
        DataEvent, ExecutionEvent,
        execution::{ExecutionReport, QueryAccount, QueryOrder, SubmitOrder},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    events::OrderEventAny,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId,
    },
    orders::{Order, OrderAny, builder::OrderTestBuilder},
    types::{Money, Price, Quantity},
};
use rstest::rstest;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedReceiver;

const INSTRUMENT: &str = "BTCUSDT-PERP.ASTER";
const ACCOUNT: &str = "ASTER-SCOPE";
const CLIENT: &str = "ASTER-SCOPE";

struct Harness {
    factory: AsterExecutionClientFactory,
    client: Box<dyn ExecutionClient>,
    cache: Rc<RefCell<Cache>>,
    exec_rx: UnboundedReceiver<ExecutionEvent>,
    _data_rx: UnboundedReceiver<DataEvent>,
}

fn position(quantity: &str) -> Value {
    json!({"symbol":"BTCUSDT", "positionAmt":quantity, "entryPrice":"50000.00",
        "positionSide":"BOTH", "updateTime":1})
}

fn script_good(venue: &MockVenue) {
    venue.script(|script| {
        script.position_mode = Some(json!({"dualSidePosition":false}));
        script.balances = json!([{"asset":"USDT", "balance":"1000.0",
            "availableBalance":"900.00000001", "marginAvailable":true, "updateTime":1}]);
        script.position_risk = json!([position("0.000")]);
        script.commission_rates.insert(
            "BTCUSDT".into(),
            json!({"symbol":"BTCUSDT",
            "makerCommissionRate":"0.000050", "takerCommissionRate":"0.000400"}),
        );
    });
}

fn config(venue: &MockVenue, age: u64, refresh: u64) -> AsterExecutionClientConfig {
    let signer = AsterEip712Signer::for_environment(TEST_PRIVATE_KEY, AsterEnvironment::Mainnet)
        .expect("published synthetic signer")
        .address_hex();
    AsterExecutionClientConfig {
        account_id: AccountId::from(ACCOUNT),
        user_address: Some(signer.clone()),
        signer_address: Some(signer),
        signer_private_key: Some(TEST_PRIVATE_KEY.into()),
        base_url_http: Some(venue.http_url()),
        base_url_ws: Some(venue.ws_url()),
        http_timeout_secs: Some(1),
        ws_connect_timeout_secs: Some(1),
        instrument_provider: BinanceInstrumentProviderConfig {
            load_all: false,
            load_ids: Some(vec![INSTRUMENT.into()]),
            ..Default::default()
        },
        selected_scope_policy_json: Some(
            json!({"instrument_ids":[INSTRUMENT],
            "balance_asset":"USDT", "max_age_ms":age, "max_refresh_ms":refresh})
            .to_string(),
        ),
        ..Default::default()
    }
}

fn build(venue: &MockVenue, age: u64, refresh: u64) -> Harness {
    let (exec_tx, exec_rx) = tokio::sync::mpsc::unbounded_channel();
    let (data_tx, data_rx) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(exec_tx);
    replace_data_event_sender(data_tx);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let factory = AsterExecutionClientFactory::new();
    let client = factory
        .create(
            TraderId::from("TESTER-001"),
            CLIENT,
            &config(venue, age, refresh),
            cache.clone().into(),
        )
        .expect("normal factory");
    Harness {
        factory,
        client,
        cache,
        exec_rx,
        _data_rx: data_rx,
    }
}

fn snapshot(factory: &AsterExecutionClientFactory) -> Value {
    serde_json::from_str(
        &factory
            .selected_scope_snapshot_json()
            .expect("snapshot")
            .expect("live configured native client"),
    )
    .expect("valid diagnostic JSON")
}

async fn connected(venue: &MockVenue) -> Harness {
    script_good(venue);
    let mut harness = build(venue, 5_000, 5_000);
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    assert_eq!(snapshot(&harness.factory)["ready"], true);
    harness
}

async fn wait_ready(factory: &AsterExecutionClientFactory, expected: bool) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let value = snapshot(factory);
        if value["ready"] == expected {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness did not become {expected}: {value}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn query_account(client: &dyn ExecutionClient) {
    client
        .query_account(QueryAccount::new(
            TraderId::from("TESTER-001"),
            Some(ClientId::from(CLIENT)),
            AccountId::from(ACCOUNT),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("normal query command");
}

async fn wait_order(harness: &mut Harness, accepted: bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        while let Ok(event) = harness.exec_rx.try_recv() {
            if let ExecutionEvent::Order(event) = event {
                if matches!(event, OrderEventAny::Accepted(_)) && accepted {
                    return;
                }
                if matches!(event, OrderEventAny::Denied(_)) && !accepted {
                    return;
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected native order event"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn submit(harness: &Harness, id: &str) {
    let order: OrderAny = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(InstrumentId::from(INSTRUMENT))
        .client_order_id(ClientOrderId::from(id))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.010"))
        .price(Price::from("50000.00"))
        .time_in_force(TimeInForce::Gtc)
        .build();
    // The only cached write is the user's original order intent, never venue account or fill facts
    harness
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("original intent");
    harness
        .client
        .submit_order(SubmitOrder::new(
            order.trader_id(),
            Some(ClientId::from(CLIENT)),
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
        ))
        .expect("submit command");
}

#[tokio::test]
async fn full_normal_factory_proof_preserves_selected_receive_origin() {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    let proof = snapshot(&harness.factory);
    assert_eq!(proof["account_id"], ACCOUNT);
    assert_eq!(proof["positions"][0]["instrument_id"], INSTRUMENT);
    assert_eq!(proof["positions"][0]["signed_quantity"], "0.000");
    assert_eq!(proof["balance"]["free"], "900.00000001");
    assert_eq!(proof["source_time_ns"], Value::Null);
    assert_eq!(
        proof["source_time_origin"],
        "venue_does_not_supply_snapshot_time"
    );
    for flag in [
        "whole_account_verified",
        "run_ownership_verified",
        "funding_verified",
    ] {
        assert_eq!(proof[flag], false);
    }
    assert!(proof["received_time_ns"].as_u64().is_some());
    assert!(
        venue
            .requests_for("GET", "positionRisk")
            .iter()
            .any(|r| r.param("symbol") == Some("BTCUSDT"))
    );
    assert!(!venue.requests_for("GET", "userTrades").is_empty());
    assert!(!venue.requests_for("GET", "openOrders").is_empty());
    harness.client.disconnect().await.expect("disconnect");
    assert_eq!(snapshot(&harness.factory)["ready"], false);
    harness.client.stop().expect("stop");
    let factory = harness.factory.clone();
    drop(harness);
    assert!(
        factory
            .selected_scope_snapshot_json()
            .expect("dropped snapshot")
            .is_none()
    );
}

#[rstest]
#[case("missing")]
#[case("duplicate")]
#[case("wrong_symbol")]
#[case("side")]
#[case("malformed_quantity")]
#[case("future_time")]
#[tokio::test]
async fn incomplete_selected_positions_never_prove_flat(#[case] fault: &str) {
    let venue = MockVenue::start().await;
    script_good(&venue);
    venue.script(|script| {
        let mut row = position("0.000");
        match fault {
            "missing" => script.position_risk = json!([]),
            "duplicate" => script.position_risk = json!([row.clone(), row]),
            "wrong_symbol" => {
                row["symbol"] = "ETHUSDT".into();
                script.position_risk = json!([row]);
            }
            "side" => {
                row["positionSide"] = "LONG".into();
                script.position_risk = json!([row]);
            }
            "malformed_quantity" => {
                row["positionAmt"] = "unknown".into();
                script.position_risk = json!([row]);
            }
            "future_time" => {
                row["updateTime"] = i64::MAX.into();
                script.position_risk = json!([row]);
            }
            _ => unreachable!(),
        }
    });
    let mut harness = build(&venue, 5_000, 5_000);
    harness.client.start().expect("start");
    let _ = harness.client.connect().await;
    let proof = snapshot(&harness.factory);
    assert_eq!(proof["ready"], false);
    assert_eq!(proof["positions"], json!([]));
    assert_eq!(proof["balance"], Value::Null);
    assert!(venue.requests_for("POST", "order").is_empty());
    harness.client.stop().expect("stop");
}

#[rstest]
#[case("missing_available")]
#[case("not_margin")]
#[case("negative")]
#[case("duplicate")]
#[case("missing_asset")]
#[case("subnative_precision")]
#[tokio::test]
async fn incomplete_balance_never_grants_new_risk(#[case] fault: &str) {
    let venue = MockVenue::start().await;
    script_good(&venue);
    venue.script(|script| match fault {
        "missing_available" => {
            script.balances[0]
                .as_object_mut()
                .unwrap()
                .remove("availableBalance");
        }
        "not_margin" => script.balances[0]["marginAvailable"] = false.into(),
        "negative" => script.balances[0]["balance"] = "-1".into(),
        "duplicate" => {
            let row = script.balances[0].clone();
            script.balances.as_array_mut().unwrap().push(row);
        }
        "missing_asset" => script.balances[0]["asset"] = "USD1".into(),
        "subnative_precision" => script.balances[0]["availableBalance"] = "900.000000001".into(),
        _ => unreachable!(),
    });
    let mut harness = build(&venue, 5_000, 5_000);
    harness.client.start().expect("start");
    let _ = harness.client.connect().await;
    assert_eq!(snapshot(&harness.factory)["ready"], false);
    assert!(venue.requests_for("POST", "order").is_empty());
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn position_only_private_change_revokes_proof_then_full_query_restores_current_scope() {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    let initial = snapshot(&harness.factory);
    venue.push_ws(&json!({"e":"ACCOUNT_UPDATE", "E":1, "T":1,
        "a":{"m":"ORDER", "B":[], "P":[{"s":"BTCUSDT", "pa":"0.010", "ps":"BOTH"}]}}));
    let invalidated = wait_ready(&harness.factory, false).await;
    assert!(invalidated["generation"].as_u64() > initial["generation"].as_u64());
    venue.script(|script| script.position_risk = json!([position("0.010")]));
    query_account(harness.client.as_ref());
    let current = wait_ready(&harness.factory, true).await;
    assert_eq!(current["positions"][0]["signed_quantity"], "0.010");
    assert!(current["generation"].as_u64() > invalidated["generation"].as_u64());
    assert_eq!(current["whole_account_verified"], false);
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn stale_witness_refuses_order_without_network_submission() {
    let venue = MockVenue::start().await;
    script_good(&venue);
    let mut harness = build(&venue, 200, 5_000);
    harness.client.start().expect("start");
    harness.client.connect().await.expect("connect");
    assert_eq!(snapshot(&harness.factory)["ready"], true);
    tokio::time::sleep(Duration::from_millis(210)).await;
    assert_eq!(snapshot(&harness.factory)["ready"], false);
    venue.clear_requests();
    submit(&harness, "STALE-SCOPE");
    wait_order(&mut harness, false).await;
    assert!(venue.requests_for("POST", "order").is_empty());
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn final_dispatch_consumes_one_witness_and_second_order_is_denied() {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    venue.clear_requests();
    submit(&harness, "SCOPE-ENTRY");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while venue.requests_for("POST", "order").is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "native dispatch did not reach the peer"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(venue.requests_for("POST", "order").len(), 1);
    assert_eq!(snapshot(&harness.factory)["ready"], false);
    submit(&harness, "SCOPE-SECOND");
    wait_order(&mut harness, false).await;
    assert_eq!(venue.requests_for("POST", "order").len(), 1);
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn in_flight_refresh_cannot_borrow_newer_private_generation() {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    venue.clear_requests();
    venue.script(|script| {
        script.balance_stalls = 1;
        script.balance_stall = Duration::from_millis(300);
    });
    query_account(harness.client.as_ref());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while venue.requests_for("GET", "balance").is_empty() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    venue.push_ws(&json!({"e":"ACCOUNT_UPDATE", "E":1, "T":1,
        "a":{"m":"ORDER", "B":[], "P":[]}}));
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(snapshot(&harness.factory)["ready"], false);
    query_account(harness.client.as_ref());
    wait_ready(&harness.factory, true).await;
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn timed_out_write_remains_untrusted_and_is_not_resent() {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    venue.script(|script| {
        script.submit = SubmitOutcome::Stall {
            delay: Duration::from_secs(2),
        }
    });
    venue.clear_requests();
    submit(&harness, "SCOPE-UNKNOWN");
    tokio::time::sleep(Duration::from_millis(1_250)).await;
    assert_eq!(venue.requests_for("POST", "order").len(), 1);
    assert_eq!(snapshot(&harness.factory)["ready"], false);
    harness.client.stop().expect("stop");
}

#[rstest]
#[case("venue_rejection")]
#[case("http_auth")]
#[case("rate_limit")]
#[tokio::test]
async fn written_rejection_consumes_scope_and_never_resends(#[case] response: &str) {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    venue.script(|script| {
        script.submit = match response {
            "venue_rejection" => SubmitOutcome::AsterError {
                code: -2010,
                msg: "Synthetic insufficient funds".into(),
            },
            "http_auth" => SubmitOutcome::Status {
                status: 401,
                body: "Synthetic authentication refusal".into(),
            },
            "rate_limit" => SubmitOutcome::StatusWithRetryAfter {
                status: 429,
                body: "Synthetic cooldown".into(),
                retry_after: "1".into(),
            },
            _ => unreachable!(),
        };
    });
    venue.clear_requests();
    submit(&harness, "SCOPE-REJECT");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while venue.requests_for("POST", "order").is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "native submission did not reach the peer"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(venue.requests_for("POST", "order").len(), 1);
    assert_eq!(snapshot(&harness.factory)["ready"], false);
    assert!(!venue.requests().is_empty());
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn actual_private_socket_loss_revokes_selected_scope() {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    assert!(venue.ws_connection_count() >= 1);
    venue.script(|script| script.ws_refuse = true);
    venue.drop_ws();
    let proof = wait_ready(&harness.factory, false).await;
    assert_eq!(proof["positions"], json!([]));
    harness.client.stop().expect("stop");
}

#[rstest]
#[case("{not valid JSON")]
#[case(r#"{"e":"FUTURE_PRIVATE_EVENT","E":1}"#)]
#[case(r#"{"e":"ACCOUNT_UPDATE","E":1,"a":"malformed"}"#)]
#[case(r#"{"e":"ORDER_TRADE_UPDATE","E":1,"o":{"s":"BTCUSDT","q":"malformed"}}"#)]
#[tokio::test]
async fn raw_private_ingress_revokes_scope_even_when_typed_decoder_drops_it(#[case] raw: &str) {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    venue.push_raw_ws(raw);
    let proof = wait_ready(&harness.factory, false).await;
    assert_eq!(proof["balance"], Value::Null);
    assert_eq!(proof["positions"], json!([]));
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn dropping_connected_client_immediately_detaches_diagnostics_without_polling_tasks() {
    let venue = MockVenue::start().await;
    let harness = connected(&venue).await;
    let factory = harness.factory.clone();
    let cache = harness.cache.clone();
    drop(harness);
    assert!(
        factory
            .selected_scope_snapshot_json()
            .expect("dropped client")
            .is_none()
    );
    let replacement = factory
        .create(
            TraderId::from("TESTER-001"),
            CLIENT,
            &config(&venue, 5_000, 5_000),
            cache.into(),
        )
        .expect("immediate replacement after actual client drop");
    assert_eq!(snapshot(&factory)["ready"], false);
    drop(replacement);
    assert!(
        factory
            .selected_scope_snapshot_json()
            .expect("dropped replacement")
            .is_none()
    );
}

#[rstest]
#[case("1e-100")]
#[case("0.00000000000000000000000000001")]
#[case("79228162514264337593543950336")]
#[tokio::test]
async fn unrepresentable_original_position_never_becomes_verified_zero(#[case] quantity: &str) {
    let venue = MockVenue::start().await;
    script_good(&venue);
    venue.script(|script| script.position_risk = json!([position(quantity)]));
    let mut harness = build(&venue, 5_000, 5_000);
    harness.client.start().expect("start");
    let _ = harness.client.connect().await;
    assert_eq!(snapshot(&harness.factory)["ready"], false);
    assert!(venue.requests_for("POST", "order").is_empty());
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn later_selected_partial_order_cannot_certify_unobserved_cumulative_fills() {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    venue.clear_requests();
    venue.script(|script| script.position_stall = Some(Duration::from_millis(150)));
    query_account(harness.client.as_ref());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    while !venue
        .requests_for("GET", "positionRisk")
        .iter()
        .any(|r| r.param("symbol") == Some("BTCUSDT"))
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "selected position phase never began"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    venue.script(|script| {
        script.open_orders = json!([{
            "symbol":"BTCUSDT", "orderId":919, "clientOrderId":"FOREIGN-PARTIAL",
            "price":"50000.00", "avgPrice":"50000.00", "origQty":"0.010",
            "executedQty":"0.005", "cumQuote":"250.00", "status":"PARTIALLY_FILLED",
            "timeInForce":"GTC", "type":"LIMIT", "side":"BUY", "positionSide":"BOTH",
            "reduceOnly":false, "closePosition":false, "time":1, "updateTime":1
        }]);
    });
    tokio::time::sleep(Duration::from_millis(220)).await;
    assert_eq!(snapshot(&harness.factory)["ready"], false);
    assert_eq!(snapshot(&harness.factory)["recovery_complete"], false);
    harness.client.stop().expect("stop");
}

fn chronology_order(
    id: i64,
    client_id: &str,
    quantity: &str,
    first_ms: i64,
    last_ms: i64,
) -> Value {
    json!({"symbol":"BTCUSDT", "orderId":id, "clientOrderId":client_id,
        "price":"50000.00", "avgPrice":"50000.00", "origQty":quantity,
        "executedQty":quantity, "cumQuote":"0", "status":"FILLED", "timeInForce":"GTC",
        "type":"LIMIT", "side":"BUY", "positionSide":"BOTH", "reduceOnly":false,
        "closePosition":false, "time":first_ms, "updateTime":last_ms})
}

fn chronology_trade(id: i64, order_id: i64, time: i64) -> Value {
    json!({"symbol":"BTCUSDT", "id":id, "orderId":order_id, "price":"50000.00",
        "qty":"0.010", "quoteQty":"500.00", "realizedPnl":"0", "side":"BUY",
        "positionSide":"BOTH", "maker":true, "buyer":true, "commission":"0.02",
        "commissionAsset":"USDT", "time":time})
}

/// Places all fixture trades after the successful connection's actual history checkpoint.
async fn chronology_time() -> i64 {
    tokio::time::sleep(Duration::from_millis(5)).await;
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as i64;
    // The latest fixture offset is four milliseconds; no future-time rejection may mask the test
    tokio::time::sleep(Duration::from_millis(10)).await;
    time
}

fn drain_reports(harness: &mut Harness) -> Vec<ExecutionReport> {
    let mut reports = Vec::new();
    while let Ok(event) = harness.exec_rx.try_recv() {
        if let ExecutionEvent::Report(report) = event {
            reports.push(report);
        }
    }
    reports
}

async fn complete_chronology_query(venue: &MockVenue, harness: &Harness, ready: bool) -> Value {
    venue.clear_requests();
    query_account(harness.client.as_ref());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let proof = snapshot(&harness.factory);
        // The compensation pass reaches positionRisk after fills, orders, and balances;
        // the terminal phase also fences response processing, rather than just request ingress.
        if !venue.requests_for("GET", "positionRisk").is_empty()
            && proof["phase"] == if ready { "ready" } else { "degraded" }
        {
            assert_eq!(proof["ready"], ready, "complete normal query: {proof}");
            assert_eq!(
                proof["recovery_complete"], ready,
                "complete normal query: {proof}"
            );
            assert!(!venue.requests_for("GET", "balance").is_empty());
            return proof;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "normal query never completed: {proof}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn assert_no_chronology_bundles(reports: &[ExecutionReport], affected: &[&str]) {
    assert!(!reports.iter().any(|report| matches!(report,
        ExecutionReport::OrderWithFills(order, _) if affected.contains(&order.venue_order_id.as_str())
    )), "ambiguous actual fills must remain unpublished: {reports:?}");
    assert!(
        !reports.iter().any(|report| matches!(report,
            ExecutionReport::Fill(fill) if affected.contains(&fill.venue_order_id.as_str())
        )),
        "a bare fill must not bypass the atomic chronology gate: {reports:?}"
    );
}

#[rstest]
#[case("interleaved", 1)]
#[case("equal_millisecond", 0)]
#[tokio::test]
async fn selected_ambiguous_atomic_history_stays_in_debt_across_normal_queries(
    #[case] name: &str,
    #[case] middle_offset: i64,
) {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    drain_reports(&mut harness);
    let time = chronology_time().await;
    let last_offset = if middle_offset == 0 { 0 } else { 2 };
    venue.script(|script| {
        let a = chronology_order(990_900, "CHRONO-A", "0.020", time, time + last_offset);
        let b = chronology_order(
            990_100,
            "CHRONO-B",
            "0.010",
            time + middle_offset,
            time + middle_offset,
        );
        script.orders.insert("990900".into(), a);
        script.orders.insert("990100".into(), b);
        script.user_trades.insert(
            "BTCUSDT".into(),
            vec![
                chronology_trade(991_001, 990_900, time),
                chronology_trade(991_002, 990_100, time + middle_offset),
                chronology_trade(991_003, 990_900, time + last_offset),
            ],
        );
        script.position_risk = json!([position("0.030")]);
    });
    for attempt in 0..3 {
        let proof = complete_chronology_query(&venue, &harness, false).await;
        assert_eq!(
            proof["positions"],
            json!([]),
            "{name} attempt {attempt}: {proof}"
        );
        assert_eq!(proof["balance"], Value::Null);
        assert_eq!(proof["fill_chronology_debt"], true);
        if attempt == 0 {
            assert!(
                !venue.requests_for("GET", "userTrades").is_empty(),
                "actual fixture history was never read"
            );
        }
        assert_no_chronology_bundles(&drain_reports(&mut harness), &["990900", "990100"]);
        assert!(venue.requests_for("POST", "order").is_empty());
    }
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn late_targeted_actual_order_fill_keeps_selected_chronology_debt_sticky() {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    drain_reports(&mut harness);
    let time = chronology_time().await;
    venue.script(|script| {
        script.orders.insert(
            "990900".into(),
            chronology_order(990_900, "CHRONO-A", "0.020", time, time + 2),
        );
        script.user_trades.insert(
            "BTCUSDT".into(),
            vec![
                chronology_trade(991_001, 990_900, time),
                chronology_trade(991_003, 990_900, time + 2),
            ],
        );
        script.position_risk = json!([position("0.020")]);
    });
    complete_chronology_query(&venue, &harness, true).await;
    let reports = drain_reports(&mut harness);
    let bundles: Vec<_> = reports
        .iter()
        .filter_map(|report| match report {
            ExecutionReport::OrderWithFills(order, fills)
                if order.venue_order_id.as_str() == "990900" =>
            {
                Some(fills)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        bundles.len(),
        1,
        "first successful actual recovery: {reports:?}"
    );
    assert_eq!(bundles[0].len(), 2);
    assert_eq!(bundles[0][0].trade_id.as_str(), "991001");
    assert_eq!(bundles[0][1].trade_id.as_str(), "991003");
    assert!(
        bundles[0]
            .iter()
            .all(|fill| fill.commission == Money::from("0.02 USDT"))
    );
    venue.script(|script| {
        script.orders.insert(
            "990100".into(),
            chronology_order(990_100, "CHRONO-B", "0.010", time + 1, time + 1),
        );
        script
            .user_trades
            .get_mut("BTCUSDT")
            .expect("existing actual history")
            .push(chronology_trade(991_002, 990_100, time + 1));
        script.position_risk = json!([position("0.030")]);
    });
    venue.clear_requests();
    harness
        .client
        .query_order(QueryOrder::new(
            TraderId::from("TESTER-001"),
            Some(ClientId::from(CLIENT)),
            StrategyId::from("S-001"),
            InstrumentId::from(INSTRUMENT),
            ClientOrderId::from("CHRONO-B"),
            Some(VenueOrderId::from("990100")),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("normal targeted order command");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let proof = snapshot(&harness.factory);
        if proof["phase"] == "degraded"
            && proof["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("chronology"))
        {
            assert_eq!(proof["fill_chronology_debt"], true);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "late actual targeted fill did not raise chronology debt: {proof}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        venue
            .requests_for("GET", "order")
            .iter()
            .any(|request| request.param("orderId") == Some("990100"))
    );
    assert!(
        !venue.requests_for("GET", "userTrades").is_empty(),
        "targeted cumulative order must read its actual old trade history"
    );
    assert_no_chronology_bundles(&drain_reports(&mut harness), &["990100"]);
    for _ in 0..2 {
        complete_chronology_query(&venue, &harness, false).await;
        assert_no_chronology_bundles(&drain_reports(&mut harness), &["990100"]);
    }
    harness.client.stop().expect("stop");
}

#[tokio::test]
async fn selected_separated_batches_publish_in_actual_time_order_despite_reverse_order_ids() {
    let venue = MockVenue::start().await;
    let mut harness = connected(&venue).await;
    drain_reports(&mut harness);
    let time = chronology_time().await;
    venue.script(|script| {
        script.orders.insert(
            "990900".into(),
            chronology_order(990_900, "CHRONO-A", "0.020", time, time + 2),
        );
        script.orders.insert(
            "990100".into(),
            chronology_order(990_100, "CHRONO-B", "0.010", time + 4, time + 4),
        );
        script.user_trades.insert(
            "BTCUSDT".into(),
            vec![
                chronology_trade(991_001, 990_900, time),
                chronology_trade(991_003, 990_900, time + 2),
                chronology_trade(991_004, 990_100, time + 4),
            ],
        );
        script.position_risk = json!([position("0.030")]);
    });
    complete_chronology_query(&venue, &harness, true).await;
    let reports = drain_reports(&mut harness);
    let ids: Vec<_> = reports
        .iter()
        .filter_map(|report| match report {
            ExecutionReport::OrderWithFills(order, _) => Some(order.venue_order_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        ids,
        ["990900", "990100"],
        "actual-time ordered bundles: {reports:?}"
    );
    harness.client.stop().expect("stop");
}
