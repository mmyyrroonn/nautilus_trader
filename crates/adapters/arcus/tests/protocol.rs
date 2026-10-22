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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use nautilus_arcus::{
    http::{ArcusHttpClient, ArcusHttpError},
    instruments::parse_instrument,
    models::*,
    signing::{ArcusSigner, cancel_signing_payload, order_signing_payload, validate_address},
};
use nautilus_core::UnixNanos;
use nautilus_model::instruments::Instrument;
use rstest::rstest;
use rust_decimal_macros::dec;

fn market() -> ArcusMarket {
    serde_json::from_str(include_str!("../test_data/market.json")).unwrap()
}

#[rstest]
fn http_construction_initializes_the_shared_tls_provider() {
    let _client = ArcusHttpClient::new("https://api.arcus.xyz".into(), 10, None).unwrap();
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    let _config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
}

fn order() -> ArcusOrderRequest {
    ArcusOrderRequest {
        address: "0xABCDEF0123456789ABCDEF0123456789ABCDEF01".into(),
        account_index: 3,
        market_id: 1,
        order_side: ArcusOrderSide::Buy,
        order_type: ArcusOrderType::Limit,
        time_in_force: ArcusTimeInForce::Alo,
        quantity: dec!(0.001),
        price: dec!(50000.1),
        good_til_time: "1800000000000000".into(),
        reduce_only: false,
        client_id: Some("MixedCase_1".into()),
        timestamp: 1790000000000000000,
    }
}

#[rstest]
fn typed_payload_preserves_case_and_native_integer_units() {
    let order = order();
    assert_eq!(
        order_signing_payload(&order, &market(), order.timestamp).unwrap(),
        "{\"ad\":\"0xabcdef0123456789abcdef0123456789abcdef01\",\"ai\":3,\"c\":\"MixedCase_1\",\"ct\":1790000000000000000,\"g\":1800000000000000000,\"m\":1,\"op\":1,\"p\":500001,\"q\":100000,\"r\":0,\"s\":0,\"t\":3,\"v\":1}"
    );
}

#[rstest]
fn signing_key_matches_rfc8032_public_key() {
    // RFC 8032 section 7.1, public test vector, not an account credential
    let signer = ArcusSigner::from_seed_hex(
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
    )
    .unwrap();
    assert_eq!(
        signer.api_key(),
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
    );
    let order = order();
    let signature = signer
        .sign_order(&order, &market(), order.timestamp)
        .unwrap();
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("../test_data/signature.json")).unwrap();
    assert_eq!(signature, reference["signature"].as_str().unwrap());
    assert!(!format!("{signer:?}").contains("9d61b19d"));
}

#[rstest]
fn signing_rejects_off_grid_prices_and_tier_boundary() {
    let mut order = order();
    order.price = dec!(50000.11);
    assert!(order_signing_payload(&order, &market(), order.timestamp).is_err());
    order.price = dec!(500000.1);
    assert!(order_signing_payload(&order, &market(), order.timestamp).is_err());
    order.price = dec!(500000.2);
    let payload = order_signing_payload(&order, &market(), order.timestamp).unwrap();
    assert!(payload.contains("\"p\":5000002"));
}

#[rstest]
fn signing_rejects_bad_client_id_short_expiry_and_unprotected_market() {
    let mut order = order();
    order.client_id = Some("unicode-\u{4e2d}".into());
    assert!(order_signing_payload(&order, &market(), order.timestamp).is_err());
    order.client_id = None;
    order.good_til_time = (order.timestamp / 1_000 + 86_400_000_000).to_string();
    assert!(order_signing_payload(&order, &market(), order.timestamp).is_err());
    order.good_til_time = "1800000000000000".into();
    order.order_type = ArcusOrderType::Market;
    assert!(order_signing_payload(&order, &market(), order.timestamp).is_err());
    order.time_in_force = ArcusTimeInForce::Ioc;
    order.price = dec!(56000);
    assert!(order_signing_payload(&order, &market(), order.timestamp).is_err());
}

#[rstest]
fn cancel_by_client_id_signs_exactly_one_target() {
    let mut cancel = ArcusCancelRequest {
        address: order().address,
        account_index: 3,
        market_id: 1,
        kind: "clientId".into(),
        order_id: None,
        client_id: Some("MixedCase_1".into()),
        timestamp: order().timestamp,
    };
    let payload = cancel_signing_payload(&cancel, cancel.timestamp).unwrap();
    assert!(payload.contains("\"c\":\"MixedCase_1\""));
    assert!(!payload.contains("\"id\""));
    cancel.order_id = Some("1234".into());
    assert!(cancel_signing_payload(&cancel, cancel.timestamp).is_err());
    assert!(validate_address(&"\u{4e2d}".repeat(14), 0).is_err());
}

#[rstest]
fn instrument_preserves_grid_fees_and_settlement() {
    let instrument = parse_instrument(&market(), UnixNanos::from(100)).unwrap();
    assert_eq!(instrument.id().to_string(), "BTC-USD-PERP.ARCUS");
    assert_eq!(instrument.price_increment().as_decimal(), dec!(0.1));
    assert_eq!(instrument.size_increment().as_decimal(), dec!(0.00000001));
    assert_eq!(instrument.maker_fee(), dec!(-0.000020));
    assert_eq!(instrument.taker_fee(), dec!(0.000225));
    assert_eq!(instrument.settlement_currency().code.as_str(), "USDG.ARCUS");
}

#[tokio::test]
async fn malformed_accepted_write_is_unknown_and_never_retried() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let app = Router::new().route(
        "/v1/placeOrder",
        post(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                (StatusCode::ACCEPTED, "not-json")
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let signer = Arc::new(ArcusSigner::from_seed_hex(&"00".repeat(32)).unwrap());
    let client = ArcusHttpClient::new(format!("http://{address}"), 2, Some(signer)).unwrap();
    let error = client.place_order(&order(), &market()).await.unwrap_err();
    assert!(matches!(error, ArcusHttpError::UnknownOutcome(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn order_wire_contract_includes_query_decimal_strings_and_typed_signature() {
    async fn handle(
        Query(query): Query<std::collections::HashMap<String, String>>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        assert_eq!(query["address"], order().address.to_ascii_lowercase());
        assert_eq!(query["accountIndex"], "3");
        assert_eq!(body["price"], "50000.1");
        assert_eq!(body["quantity"], "0.001");
        assert_eq!(body["clientId"], "MixedCase_1");
        assert_eq!(body["timeInForce"], "ALO");
        let signer = ArcusSigner::from_seed_hex(&"00".repeat(32)).unwrap();
        assert_eq!(headers["X-API-Key"].to_str().unwrap(), signer.api_key());
        assert_eq!(
            headers["X-Timestamp"].to_str().unwrap(),
            order().timestamp.to_string()
        );
        assert_eq!(
            headers["X-Signature"].to_str().unwrap(),
            signer
                .sign_order(&order(), &market(), order().timestamp)
                .unwrap()
        );
        (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({"status":"ACK","orderId":"abcd","clientId":"MixedCase_1"})),
        )
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/v1/placeOrder", post(handle)),
        )
        .await
        .unwrap();
    });
    let signer = Arc::new(ArcusSigner::from_seed_hex(&"00".repeat(32)).unwrap());
    let client = ArcusHttpClient::new(format!("http://{address}"), 2, Some(signer)).unwrap();
    let response = client.place_order(&order(), &market()).await.unwrap();
    assert_eq!(response.status, "ACK");
    assert_eq!(response.order_id.as_deref(), Some("abcd"));
    task.abort();
}

#[tokio::test]
async fn rate_limit_preserves_precise_retry_and_does_not_retry_write() {
    let app = Router::new().route("/v1/placeOrder", post(|| async { (StatusCode::TOO_MANY_REQUESTS, Json(serde_json::json!({"error":"rate limited","reason":"account_empty","retryAfterMs":850}))) }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let signer = Arc::new(ArcusSigner::from_seed_hex(&"00".repeat(32)).unwrap());
    let client = ArcusHttpClient::new(format!("http://{address}"), 2, Some(signer)).unwrap();
    assert!(
        matches!(client.place_order(&order(), &market()).await, Err(ArcusHttpError::RateLimited { retry_after_ms: 850, reason }) if reason == "account_empty")
    );
    task.abort();
}

#[tokio::test]
async fn market_cancel_signs_only_the_requested_market_scope() {
    async fn cancel(
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        assert_eq!(body["marketId"], 7);
        assert_eq!(body["accountIndex"], 3);
        let timestamp = headers["X-Timestamp"]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let signer = ArcusSigner::from_seed_hex(&"00".repeat(32)).unwrap();
        assert_eq!(
            headers["X-Signature"].to_str().unwrap(),
            signer
                .sign_legacy("cancelAllOrders", &body, timestamp)
                .unwrap()
        );
        (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({"status":"CANCEL_ALL_ACKNOWLEDGED","marketId":7})),
        )
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/v1/cancelAllOrders", post(cancel)),
        )
        .await
        .unwrap();
    });
    let signer = Arc::new(ArcusSigner::from_seed_hex(&"00".repeat(32)).unwrap());
    let client = ArcusHttpClient::new(format!("http://{address}"), 2, Some(signer)).unwrap();
    assert_eq!(
        client
            .cancel_market(&order().address, 3, 7)
            .await
            .unwrap()
            .status,
        "CANCEL_ALL_ACKNOWLEDGED"
    );
    task.abort();
}

#[tokio::test]
async fn inclusive_fill_pagination_deduplicates_boundary() {
    async fn handle(
        State(calls): State<Arc<AtomicUsize>>,
        Query(query): Query<std::collections::HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        let mut fills = Vec::new();
        let count = if call == 0 {
            1000
        } else {
            assert_eq!(query["to"], "1790000000000000");
            2
        };

        for i in 0..count {
            let id = if call == 0 { i } else { 999 + i };
            fills.push(serde_json::json!({"tradeId":id.to_string(),"orderId":"abcd","marketId":1,"marketDisplayName":"BTC-USD","side":"BUY","originalSize":"1","size":"0.001","price":"50000","fee":"0.01","role":"MAKER","createdAt":1790000000000000_u64}));
        }
        Json(serde_json::json!({"fills":fills,"total":count}))
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/v1/fills", get(handle))
        .with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = ArcusHttpClient::new(format!("http://{address}"), 2, None).unwrap();
    let fills = client
        .fills(&order().address, 3, None, None, None)
        .await
        .unwrap();
    assert_eq!(fills.len(), 1001);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    task.abort();
}

#[tokio::test]
async fn open_order_pagination_uses_created_at_and_deduplicates_boundary() {
    async fn handle(
        State(calls): State<Arc<AtomicUsize>>,
        Query(query): Query<std::collections::HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(query["accountIndex"], "3");
        assert!(!query.contains_key("from"));
        let count = if call == 0 {
            assert!(!query.contains_key("to"));
            1000
        } else {
            assert_eq!(query["to"], "1790000000000001");
            2
        };
        let orders = (0..count)
            .map(|i| {
                let index = if call == 0 { i } else { 999 + i };
                serde_json::json!({
                "orderId": format!("{index:064x}"), "marketId":1, "accountIndex":3,
                    "marketDisplayName":"BTC-USD", "side":"BUY", "type":"LIMIT",
                    "status":"OPEN", "price":"50000.1", "originalSize":"0.001",
                    "remainingSize":"0.001", "createdAt":1790000000001000_u64 - index,
                    "updatedAt":1800000000000000_u64,
                })
            })
            .collect::<Vec<_>>();
        Json(serde_json::json!({"orders":orders,"total":count}))
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/v1/openOrders", get(handle))
        .with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = ArcusHttpClient::new(format!("http://{address}"), 2, None).unwrap();
    let orders = client.open_orders(&order().address, 3).await.unwrap();
    assert_eq!(orders.len(), 1001);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    task.abort();
}

#[tokio::test]
async fn saturated_fill_boundary_fails_reconciliation_explicitly() {
    let app = Router::new().route("/v1/fills", get(|| async {
        let fills = (0..1000).map(|i| serde_json::json!({"tradeId":i.to_string(),"orderId":"abcd","marketId":1,"marketDisplayName":"BTC-USD","side":"BUY","originalSize":"1","size":"0.001","price":"50000","fee":"0.01","role":"MAKER","createdAt":1790000000000000_u64})).collect::<Vec<_>>();
        Json(serde_json::json!({"fills":fills}))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = ArcusHttpClient::new(format!("http://{address}"), 2, None).unwrap();
    assert!(matches!(
        client.fills(&order().address, 3, None, None, None).await,
        Err(ArcusHttpError::Incomplete(_))
    ));
    task.abort();
}

#[rstest]
fn expiry_overflow_is_rejected_without_truncation() {
    let mut order = order();
    order.good_til_time = u64::MAX.to_string();
    assert!(order_signing_payload(&order, &market(), order.timestamp).is_err());
}

#[rstest]
fn nanodollar_ledger_preserves_money_and_position_pnl_through_serde() {
    use nautilus_model::{
        events::order::spec::OrderFilledSpec,
        identifiers::PositionId,
        position::Position,
        types::{Currency, Money, Price, Quantity},
    };

    let currency = nautilus_arcus::instruments::settlement_currency().unwrap();
    assert_eq!(currency.code.as_str(), "USDG.ARCUS");
    assert_eq!(currency.precision, 9);
    assert_eq!(Currency::USDG().precision, 8);
    let money = Money::from_decimal(dec!(0.000000001), currency).unwrap();
    assert_eq!(money.as_decimal(), dec!(0.000000001));
    let encoded = serde_json::to_string(&money).unwrap();
    let decoded: Money = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.currency.precision, 9);
    assert_eq!(decoded.as_decimal(), money.as_decimal());

    let instrument = parse_instrument(&market(), UnixNanos::from(1)).unwrap();
    let fill = OrderFilledSpec::builder()
        .instrument_id(instrument.id())
        .position_id(PositionId::from("ARCUS-POSITION-1"))
        .last_qty(Quantity::from("0.00100000"))
        .last_px(Price::from("50000.0"))
        .currency(Currency::USD())
        .commission(money)
        .build();
    let position = Position::new_checked(&instrument, fill).unwrap();
    assert_eq!(position.settlement_currency, currency);
    assert_eq!(
        position.realized_pnl.unwrap().as_decimal(),
        dec!(-0.000000001)
    );
    let encoded = serde_json::to_string(&position).unwrap();
    let decoded: Position = serde_json::from_str(&encoded).unwrap();
    assert_eq!(
        decoded.realized_pnl.unwrap().as_decimal(),
        dec!(-0.000000001)
    );
    assert_eq!(decoded.commissions()[0].as_decimal(), dec!(0.000000001));
}
