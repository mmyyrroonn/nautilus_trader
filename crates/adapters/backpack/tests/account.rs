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

//! Synthetic account protocol cases and real signed read-only loopback traversal.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    response::Response,
    routing::any,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use nautilus_backpack::{
    account::{
        BackpackAccountError, BackpackEvidenceGap,
        client::{BackpackAccountReader, BackpackRestingOrder},
        models::*,
        pagination::{BackpackHistoryWindow, BackpackReadBudget},
        reconciliation::{BackpackFillKey, BackpackFillReconciler, BackpackFillStage},
        reports::{
            BackpackFundingObservation, BackpackOrderBindings, BackpackOrderOwnership,
            BackpackReportContext, fill_report, order_report, position_report, wallet_report,
        },
    },
    common::{credential::BackpackCredential, endpoints::BackpackEndpoints},
    config::BackpackConfig,
    http::{
        client::{BackpackHttpClient, BackpackHttpPolicy, BackpackSystemClock},
        quota::BackpackQuota,
    },
    identity::{BackpackClientIdNamespace, BackpackClientIdStore, BackpackSubmissionIntent},
    models::BackpackMarket,
    provider::BackpackInstrumentProvider,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderStatus, PositionSide},
    identifiers::{AccountId, ClientOrderId, InstrumentId, VenueOrderId},
    types::Currency,
};
use rstest::rstest;
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

fn fixture(name: &str) -> Value {
    serde_json::from_str::<Value>(include_str!("../test_data/account/synthetic.json")).unwrap()
        [name]
        .clone()
}
fn model<T: DeserializeOwned>(name: &str) -> T {
    serde_json::from_value(fixture(name)).unwrap()
}
fn dec(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}
fn window() -> BackpackHistoryWindow {
    BackpackHistoryWindow::new(0, 2_000_000_000_000).unwrap()
}
fn budget(size: u64, pages: u64) -> BackpackReadBudget {
    BackpackReadBudget::new(size, pages, 100, Duration::from_secs(3)).unwrap()
}

#[derive(Clone, Debug)]
struct Reply {
    status: u16,
    body: Value,
    headers: Vec<(String, String)>,
    delay: Duration,
}
impl Reply {
    fn json(body: Value) -> Self {
        Self {
            status: 200,
            body,
            headers: vec![],
            delay: Duration::ZERO,
        }
    }
    fn page(body: Value, index: u64, size: u64, total: u64) -> Self {
        let mut result = Self::json(body);
        result.headers = vec![
            ("x-page-count".into(), total.div_ceil(size).to_string()),
            ("x-current-page".into(), index.to_string()),
            ("x-page-size".into(), size.to_string()),
            ("x-total".into(), total.to_string()),
        ];
        result
    }
}
#[derive(Clone, Debug)]
struct Captured {
    method: String,
    uri: String,
    authenticated: bool,
    body_empty: bool,
}
#[derive(Debug)]
struct ServerState {
    replies: Mutex<VecDeque<Reply>>,
    captured: Mutex<Vec<Captured>>,
}
#[derive(Debug)]
struct Server {
    endpoints: BackpackEndpoints,
    state: Arc<ServerState>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoints = BackpackEndpoints::loopback_override(
            &format!("http://{address}"),
            &format!("ws://{address}"),
        )
        .unwrap();
        let state = Arc::new(ServerState {
            replies: Mutex::new(replies.into()),
            captured: Mutex::new(vec![]),
        });
        let app = Router::new()
            .fallback(any(handler))
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
    fn reader(&self, budget: BackpackReadBudget) -> BackpackAccountReader {
        let credential =
            BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &self.endpoints).unwrap();
        let http = BackpackHttpClient::new(
            self.endpoints.clone(),
            Some(credential),
            BackpackQuota::default(),
            BackpackHttpPolicy::new(Default::default(), Duration::from_secs(3), 0).unwrap(),
            Arc::new(BackpackSystemClock),
        )
        .unwrap();
        BackpackAccountReader::new(http, budget)
    }
    fn captured(&self) -> Vec<Captured> {
        self.state.captured.lock().unwrap().clone()
    }
}
async fn handler(State(state): State<Arc<ServerState>>, request: Request) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, 1024).await.unwrap();
    let authenticated = ["x-api-key", "x-signature", "x-timestamp", "x-window"]
        .iter()
        .all(|key| parts.headers.contains_key(*key));
    state.captured.lock().unwrap().push(Captured {
        method: parts.method.to_string(),
        uri: parts.uri.to_string(),
        authenticated,
        body_empty: bytes.is_empty(),
    });
    let reply = state.replies.lock().unwrap().pop_front().unwrap_or(Reply {
        status: 500,
        body: json!({"code":"UnexpectedRequest"}),
        headers: vec![],
        delay: Duration::ZERO,
    });
    tokio::time::sleep(reply.delay).await;
    let mut response = Response::builder()
        .status(reply.status)
        .header("content-type", "application/json");
    for (key, value) in reply.headers {
        response = response.header(key, value);
    }
    response.body(Body::from(reply.body.to_string())).unwrap()
}
fn assert_gets(server: &Server) {
    assert!(
        server
            .captured()
            .iter()
            .all(|r| r.method == "GET" && r.authenticated && r.body_empty)
    );
}
fn snapshot_replies() -> Vec<Reply> {
    vec![
        Reply::json(fixture("policy")),
        Reply::json(fixture("balances")),
        Reply::json(fixture("collateral")),
        Reply::json(json!([fixture("position")])),
        Reply::json(json!([fixture("resting_order")])),
    ]
}

#[derive(Debug)]
struct Scope {
    provider: BackpackInstrumentProvider,
    store: BackpackClientIdStore,
    _directory: TempDir,
}
impl Scope {
    fn new() -> Self {
        let mut provider = BackpackInstrumentProvider::new(
            BackpackConfig::new_checked(vec!["BTC_USDC_PERP".into()]).unwrap(),
        );
        let market: BackpackMarket =
            serde_json::from_str(include_str!("../test_data/btc_usdc_perp.json")).unwrap();
        provider
            .replace_markets(&[market], UnixNanos::from(1))
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let namespace = BackpackClientIdNamespace::new_checked(
            "synthetic-loopback",
            "synthetic-account",
            Some("2"),
        )
        .unwrap();
        let mut store = BackpackClientIdStore::open(directory.path(), &namespace).unwrap();
        let id = store
            .reserve_intent(
                BackpackSubmissionIntent::new_checked(
                    ClientOrderId::from("O-SYNTHETIC-1"),
                    "{}".into(),
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(id, 1);
        Self {
            provider,
            store,
            _directory: directory,
        }
    }
    fn context(&self) -> BackpackReportContext<'_> {
        BackpackReportContext {
            account_id: AccountId::from("BACKPACK-SYNTHETIC"),
            instruments: &self.provider,
            identities: &self.store,
            confirmed_orders: None,
            ts_init: UnixNanos::from(2),
        }
    }
}

#[rstest]
#[case("policy", "AccountSummary")]
#[case("collateral", "MarginAccountSummary")]
#[case("position", "FuturePositionWithMargin")]
#[case("resting_order", "LimitOrder")]
#[case("history_order", "Order")]
#[case("fill", "OrderFill")]
#[case("funding", "FundingPayment")]
fn test_synthetic_fixture_required_fields_match_recorded_official_schema(
    #[case] name: &str,
    #[case] schema: &str,
) {
    let official: Value =
        serde_json::from_str(include_str!("../test_data/account/official_schema.json")).unwrap();
    let body = fixture(name);
    for required in official["components"]["schemas"][schema]["required"]
        .as_array()
        .unwrap()
    {
        assert!(
            body.get(required.as_str().unwrap()).is_some(),
            "missing official required field {required}"
        );
    }
}

#[rstest]
#[case("\"1e-9\"")]
#[case("\" 1\"")]
#[case("\"+1\"")]
#[case("\"NaN\"")]
#[case("null")]
#[case("true")]
#[case("\"0.00000000000000000000000000001\"")]
fn test_invalid_or_unrepresentable_decimal_is_rejected(#[case] wire: &str) {
    assert!(serde_json::from_str::<BackpackDecimal>(wire).is_err());
}

#[rstest]
fn test_decimal_numeric_compatibility_is_exact_not_f64() {
    let raw = "0.1234567890123456789012345678";
    let exact: BackpackDecimal = serde_json::from_str(raw).unwrap();
    assert_eq!(exact.0, dec(raw));
    let raw = fixture("position")
        .to_string()
        .replace("\"-0.00001\"", "-0.00001");
    let position: BackpackPosition = serde_json::from_str(&raw).unwrap();
    assert_eq!(position.net_quantity.0, dec("-0.00001"));
}

#[rstest]
fn test_true_fill_exact_trade_id_fee_rebate_and_original_durable_ownership() {
    let scope = Scope::new();
    let observed = fill_report(model("fill"), &scope.context()).unwrap();
    assert_eq!(
        observed.ownership,
        BackpackOrderOwnership::Reserved(ClientOrderId::from("O-SYNTHETIC-1"))
    );
    let report = observed.report.unwrap();
    assert!(report.client_order_id.is_none());
    assert!(
        observed
            .gaps
            .contains(&BackpackEvidenceGap::OwnershipUnverified)
    );
    assert_eq!(report.trade_id.to_string(), "9007199254740993");
    assert_eq!(report.last_qty.as_decimal(), dec("0.00001"));
    assert_eq!(report.last_px.as_decimal(), dec("100.1"));
    assert_eq!(report.commission.as_decimal(), dec("-0.000001"));
    assert_eq!(report.commission.currency, Currency::USDC());
    assert_eq!(report.ts_event.as_u64() % 1_000_000_000, 123_456_000);
    assert!(
        observed
            .gaps
            .contains(&BackpackEvidenceGap::AccountIdentityUnverified)
    );
}

#[rstest]
fn test_missing_trade_id_preserves_economics_without_fabricating_a_report() {
    let scope = Scope::new();
    let mut raw: BackpackFill = model("fill");
    raw.trade_id = None;
    let observation = fill_report(raw, &scope.context()).unwrap();
    assert!(observation.report.is_none());
    assert_eq!(observation.raw.quantity.0, dec("0.00001"));
    assert!(
        observation
            .gaps
            .contains(&BackpackEvidenceGap::MissingTradeId)
    );
}

#[rstest]
#[case(Some("FutureNewSystem"), Some("1"))]
#[case(Some("LiquidatePositionOnAdl"), Some("1"))]
#[case(None, Some("42"))]
#[case(None, None)]
fn test_external_and_system_fills_are_not_adopted(
    #[case] system: Option<&str>,
    #[case] client: Option<&str>,
) {
    let scope = Scope::new();
    let mut raw: BackpackFill = model("fill");
    raw.system_order_type = system.map(str::to_string);
    raw.client_id = client.map(str::to_string);
    let observed = fill_report(raw, &scope.context()).unwrap();
    assert_eq!(observed.ownership, BackpackOrderOwnership::External);
    assert!(observed.report.unwrap().client_order_id.is_none());
    if system == Some("FutureNewSystem") {
        assert!(
            observed
                .gaps
                .contains(&BackpackEvidenceGap::UnknownVenueState)
        );
    }
}

#[rstest]
#[case("4294967296")]
#[case("-1")]
#[case("1.0")]
#[case("")]
fn test_invalid_fill_client_id_not_truncated_or_hashed(#[case] id: &str) {
    let scope = Scope::new();
    let mut raw: BackpackFill = model("fill");
    raw.client_id = Some(id.into());
    assert!(matches!(
        fill_report(raw, &scope.context()),
        Err(BackpackAccountError::InvalidField("fill clientId"))
    ));
}

#[rstest]
fn test_money_quantity_price_precision_loss_is_not_rounded() {
    let scope = Scope::new();
    for field in ["fee", "quantity", "price"] {
        let mut raw = fixture("fill");
        raw[field] = json!(match field {
            "fee" => "0.000000001",
            "quantity" => "0.000001",
            _ => "100.11",
        });
        let raw: BackpackFill = serde_json::from_value(raw).unwrap();
        assert!(matches!(
            fill_report(raw, &scope.context()),
            Err(BackpackAccountError::InvalidField(_))
        ));
    }
}

#[rstest]
fn test_wallet_staked_equity_and_liability_not_conflated() {
    let balance: BackpackWalletBalance =
        serde_json::from_value(fixture("balances")["USDC"].clone()).unwrap();
    let report = wallet_report("USDC", &balance).unwrap();
    assert_eq!(report.trading_balance.free.as_decimal(), dec("100"));
    assert_eq!(report.trading_balance.locked.as_decimal(), dec("10"));
    assert_eq!(report.trading_balance.total.as_decimal(), dec("110"));
    assert_eq!(report.staked.as_decimal(), dec("5"));
    assert_eq!(report.wallet_total.as_decimal(), dec("115"));
    let collateral: BackpackCollateral = model("collateral");
    assert_eq!(collateral.net_equity.0, dec("120"));
    assert_eq!(collateral.net_equity_available.0, dec("105"));
    assert_eq!(collateral.liabilities_value.0, Decimal::ZERO);
}

#[rstest]
fn test_order_and_explicit_signed_position_domain_reports_have_unknown_event_time() {
    let scope = Scope::new();
    let order = order_report(model("resting_order"), &scope.context()).unwrap();
    let report = order.report.unwrap();
    assert_eq!(report.order_status, OrderStatus::PartiallyFilled);
    assert_eq!(report.ts_accepted, UnixNanos::from(0));
    assert!(order.gaps.contains(&BackpackEvidenceGap::OrderTimeUnknown));
    let position = position_report(&model("position"), &scope.context()).unwrap();
    assert_eq!(position.position_side, PositionSide::Short);
    assert_eq!(position.signed_decimal_qty, dec("-0.00001"));
    assert_eq!(position.ts_last, UnixNanos::from(0));
}

#[rstest]
fn test_unknown_order_state_missing_economics_and_reduce_policy_are_not_defaulted() {
    let scope = Scope::new();
    for field in [
        "status",
        "quantity",
        "executedQuantity",
        "reduceOnly",
        "postOnly",
    ] {
        let mut raw = fixture("resting_order");
        if field == "status" {
            raw[field] = json!("FutureVenueState");
        } else {
            raw.as_object_mut().unwrap().remove(field);
        }
        let observed =
            order_report(serde_json::from_value(raw).unwrap(), &scope.context()).unwrap();
        assert!(observed.report.is_none());
        assert!(
            observed
                .gaps
                .contains(&BackpackEvidenceGap::UnknownVenueState)
        );
    }
}

#[rstest]
fn test_funding_sign_retained_but_currency_and_timezone_are_unknown() {
    let paid = BackpackFundingObservation::from(model::<BackpackFundingPayment>("funding"));
    assert_eq!(paid.raw.quantity.0, dec("-0.000001"));
    assert!(
        paid.gaps
            .contains(&BackpackEvidenceGap::FundingCurrencyUnknown)
    );
    assert!(
        paid.gaps
            .contains(&BackpackEvidenceGap::FundingTimezoneUnknown)
    );
    let mut received = paid.raw;
    received.quantity = BackpackDecimal(dec("0.000002"));
    assert_eq!(
        BackpackFundingObservation::from(received).raw.quantity.0,
        dec("0.000002")
    );
}

#[rstest]
fn test_first_fill_pending_retry_failed_ack_restart_and_rest_ws_dedup() {
    let scope = Scope::new();
    let report = fill_report(model("fill"), &scope.context())
        .unwrap()
        .report
        .unwrap();
    let key = BackpackFillKey {
        instrument_id: report.instrument_id,
        trade_id: report.trade_id,
    };
    let mut reconciler = BackpackFillReconciler::from_applied(10, []).unwrap();
    assert!(matches!(
        reconciler.stage(report.clone()).unwrap(),
        BackpackFillStage::Pending(_)
    ));
    assert_eq!(reconciler.applied_records().count(), 0);
    assert!(
        reconciler
            .acknowledge_with(key, |_| Err(BackpackAccountError::Acknowledgement))
            .is_err()
    );
    let mut duplicate = report.clone();
    duplicate.ts_init = UnixNanos::from(50);
    duplicate.report_id = Default::default();
    assert!(matches!(
        reconciler.stage(duplicate.clone()).unwrap(),
        BackpackFillStage::Pending(_)
    ));
    assert_eq!(reconciler.pending_in_event_order().len(), 1);
    // Restart before a durable consumer ACK: no applied records, therefore redelivery.
    let mut before_ack =
        BackpackFillReconciler::from_applied(10, reconciler.applied_records().cloned()).unwrap();
    assert!(matches!(
        before_ack.stage(report.clone()).unwrap(),
        BackpackFillStage::Pending(_)
    ));
    let mut durable_record = None;
    reconciler
        .acknowledge_with(key, |record| {
            durable_record = Some(record.clone());
            Ok(())
        })
        .unwrap();
    assert!(reconciler.pending_in_event_order().is_empty());
    let mut restored = BackpackFillReconciler::from_applied(10, [durable_record.unwrap()]).unwrap();
    assert!(matches!(
        restored.stage(duplicate).unwrap(),
        BackpackFillStage::AlreadyApplied
    ));
    let mut conflict = report;
    conflict.commission =
        nautilus_model::types::Money::from_decimal(dec("0.000001"), Currency::USDC()).unwrap();
    assert!(matches!(
        restored.stage(conflict),
        Err(BackpackAccountError::Conflict)
    ));
}

#[rstest]
fn test_out_of_order_and_terminal_late_fills_sorted_by_event_time_without_loss() {
    let scope = Scope::new();
    let mut reconciler = BackpackFillReconciler::from_applied(3, []).unwrap();
    for (id, timestamp) in [
        (3, "2026-10-02T00:00:03"),
        (1, "2026-10-02T00:00:01"),
        (2, "2026-10-02T00:00:02"),
    ] {
        let mut raw: BackpackFill = model("fill");
        raw.trade_id = Some(id);
        raw.timestamp = timestamp.into();
        reconciler
            .stage(fill_report(raw, &scope.context()).unwrap().report.unwrap())
            .unwrap();
    }
    let ids: Vec<_> = reconciler
        .pending_in_event_order()
        .iter()
        .map(|r| r.trade_id.to_string())
        .collect();
    assert_eq!(ids, ["1", "2", "3"]);
    let mut late: BackpackFill = model("fill");
    late.trade_id = Some(4);
    assert!(matches!(
        reconciler.stage(fill_report(late, &scope.context()).unwrap().report.unwrap()),
        Err(BackpackAccountError::Budget)
    ));
}

#[rstest]
#[tokio::test]
async fn test_actual_signed_snapshot_with_explicit_unknown_identity_and_non_atomic_coverage() {
    let server = Server::start(snapshot_replies()).await;
    let snapshot = server
        .reader(budget(2, 5))
        .snapshot(&CancellationToken::new())
        .await
        .unwrap();
    assert!(
        snapshot
            .gaps
            .contains(&BackpackEvidenceGap::AccountIdentityUnverified)
    );
    assert!(
        snapshot
            .gaps
            .contains(&BackpackEvidenceGap::NonAtomicSnapshot)
    );
    assert!(snapshot.position("SOL_USDC_PERP").is_none());
    assert_eq!(
        snapshot.position("BTC_USDC_PERP").unwrap().net_quantity.0,
        dec("-0.00001")
    );
    let paths: Vec<_> = server.captured().iter().map(|r| r.uri.clone()).collect();
    assert_eq!(
        paths,
        [
            "/api/v1/account",
            "/api/v1/capital",
            "/api/v1/capital/collateral",
            "/api/v1/position",
            "/api/v1/orders"
        ]
    );
    assert_gets(&server);
}

#[rstest]
#[tokio::test]
async fn test_empty_snapshots_are_observed_empty_not_missing_or_flat() {
    let mut replies = snapshot_replies();
    replies[1].body = json!({});
    replies[3].body = json!([]);
    replies[4].body = json!([]);
    let server = Server::start(replies).await;
    let snapshot = server
        .reader(budget(2, 5))
        .snapshot(&CancellationToken::new())
        .await
        .unwrap();
    assert!(
        snapshot.balances.is_empty()
            && snapshot.positions.is_empty()
            && snapshot.open_orders.is_empty()
    );
    assert!(snapshot.position("BTC_USDC_PERP").is_none());
    assert!(
        snapshot
            .gaps
            .contains(&BackpackEvidenceGap::NonAtomicSnapshot)
    );
    assert_gets(&server);
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[tokio::test]
async fn test_missing_response_is_failure_never_successful_empty(#[case] endpoint: usize) {
    let mut replies = snapshot_replies();
    replies[endpoint].body = Value::Null;
    let server = Server::start(replies).await;
    assert!(matches!(
        server
            .reader(budget(2, 5))
            .snapshot(&CancellationToken::new())
            .await,
        Err(BackpackAccountError::Decode)
    ));
    assert_eq!(server.captured().len(), endpoint + 1);
}

#[rstest]
#[tokio::test]
async fn test_unsupported_policy_liability_and_collateral_degrade_without_mutation() {
    let mut replies = snapshot_replies();
    replies[0].body["autoLend"] = json!(true);
    replies[0].body["unknownSetting"] = json!(true);
    replies[2].body["borrowLiability"] = json!("1");
    replies[2].body["collateral"][0]["lendQuantity"] = json!("2");
    let server = Server::start(replies).await;
    let snapshot = server
        .reader(budget(2, 5))
        .snapshot(&CancellationToken::new())
        .await
        .unwrap();
    assert!(
        snapshot
            .gaps
            .contains(&BackpackEvidenceGap::UnsupportedAccountPolicy)
    );
    assert!(
        snapshot
            .gaps
            .contains(&BackpackEvidenceGap::UnsupportedCollateral)
    );
    assert!(snapshot.gaps.contains(&BackpackEvidenceGap::UnknownFields));
    assert_gets(&server);
}

#[rstest]
#[tokio::test]
async fn test_full_fill_pagination_has_fixed_cutoff_exact_headers_and_no_first_page_completion() {
    let first = fixture("fill");
    let mut second = first.clone();
    second["tradeId"] = json!(9007199254740994_i64);
    let server = Server::start(vec![
        Reply::page(json!([first]), 0, 1, 2),
        Reply::page(json!([second]), 1, 1, 2),
    ])
    .await;
    let history = server
        .reader(budget(1, 3))
        .fill_history(window(), Some("BTC_USDC_PERP"), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(history.records.len(), 2);
    assert_eq!(history.evidence.pages, 2);
    assert_eq!(history.evidence.rows, 2);
    assert!(
        history
            .evidence
            .gaps
            .contains(&BackpackEvidenceGap::RetentionOrReplicationUnknown)
    );
    assert!(
        !history
            .evidence
            .gaps
            .contains(&BackpackEvidenceGap::CutoffNotServerEnforced)
    );
    let captured = server.captured();
    for (index, request) in captured.iter().enumerate() {
        let query: HashMap<_, _> =
            url::form_urlencoded::parse(request.uri.split_once('?').unwrap().1.as_bytes())
                .into_owned()
                .collect();
        assert_eq!(query["from"], "0");
        assert_eq!(query["to"], "2000000000000");
        assert_eq!(query["offset"], index.to_string());
        assert_eq!(query["limit"], "1");
        assert_eq!(query["sortDirection"], "Asc");
        assert!(!query.contains_key("fillType"));
    }
    assert_gets(&server);
}

#[rstest]
#[case("orders")]
#[case("funding")]
#[tokio::test]
async fn test_order_funding_history_has_no_invented_cutoff_wire_filter(#[case] endpoint: &str) {
    let body = if endpoint == "orders" {
        fixture("history_order")
    } else {
        fixture("funding")
    };
    let server = Server::start(vec![Reply::page(json!([body]), 1, 2, 1)]).await;
    let reader = server.reader(budget(2, 2));
    let cancel = CancellationToken::new();
    let evidence = if endpoint == "orders" {
        reader
            .order_history(window(), None, &cancel)
            .await
            .unwrap()
            .evidence
    } else {
        reader
            .funding_history(window(), None, &cancel)
            .await
            .unwrap()
            .evidence
    };
    assert!(
        evidence
            .gaps
            .contains(&BackpackEvidenceGap::CutoffNotServerEnforced)
    );
    assert!(!server.captured()[0].uri.contains("from="));
    assert!(!server.captured()[0].uri.contains("to="));
    assert_gets(&server);
}

#[rstest]
#[tokio::test]
async fn test_resting_404_never_proves_never_executed_and_selector_is_exclusive() {
    let mut reply = Reply::json(json!({"code":"OrderNotFound"}));
    reply.status = 404;
    let server = Server::start(vec![reply]).await;
    let reader = server.reader(budget(1, 2));
    let cancel = CancellationToken::new();
    assert!(matches!(
        reader
            .resting_order("BTC_USDC_PERP", None, Some(1), &cancel)
            .await
            .unwrap(),
        BackpackRestingOrder::UnknownNotResting
    ));
    assert!(
        reader
            .resting_order("BTC_USDC_PERP", Some("synthetic-order-A"), Some(1), &cancel)
            .await
            .is_err()
    );
    assert_eq!(server.captured().len(), 1);
    assert_gets(&server);
}

#[rstest]
#[tokio::test]
async fn test_resting_observed_uses_actual_original_client_id() {
    let server = Server::start(vec![Reply::json(fixture("resting_order"))]).await;
    let order = server
        .reader(budget(1, 2))
        .resting_order("BTC_USDC_PERP", None, Some(1), &CancellationToken::new())
        .await
        .unwrap();
    let BackpackRestingOrder::Observed(raw) = order else {
        panic!("expected observed order")
    };
    let scope = Scope::new();
    assert!(matches!(
        order_report(*raw, &scope.context()).unwrap().ownership,
        BackpackOrderOwnership::Reserved(_)
    ));
}

#[rstest]
#[case("repeat")]
#[case("header_change")]
#[case("missing_header")]
#[case("truncated")]
#[case("failed")]
#[tokio::test]
async fn test_history_faults_fail_closed_instead_of_partial_success(#[case] fault: &str) {
    let first = fixture("fill");
    let mut second = first.clone();
    second["tradeId"] = json!(9007199254740994_i64);
    let mut reply = Reply::page(json!([second]), 1, 1, 2);
    match fault {
        "repeat" => reply.body = json!([first.clone()]),
        "header_change" => reply.headers[3].1 = "3".into(),
        "missing_header" => {
            reply.headers.pop();
        }
        "truncated" => reply.body = json!([]),
        "failed" => {
            reply.status = 500;
            reply.body = json!({"code":"ReadFailed"});
        }
        _ => unreachable!(),
    }
    let server = Server::start(vec![Reply::page(json!([first]), 0, 1, 2), reply]).await;
    let result = server
        .reader(budget(1, 3))
        .fill_history(window(), None, &CancellationToken::new())
        .await;
    assert!(result.is_err());
    assert_eq!(server.captured().len(), 2);
    assert_gets(&server);
}

#[rstest]
#[tokio::test]
async fn test_finite_page_budget_stops_before_extra_request() {
    let server = Server::start(vec![Reply::page(json!([fixture("fill")]), 0, 1, 2)]).await;
    let result = server
        .reader(budget(1, 1))
        .fill_history(window(), None, &CancellationToken::new())
        .await;
    assert!(matches!(result, Err(BackpackAccountError::Budget)));
    assert_eq!(server.captured().len(), 1);
}

#[rstest]
#[tokio::test]
async fn test_empty_history_requires_all_headers_and_remains_replication_unknown() {
    let server = Server::start(vec![Reply::page(json!([]), 0, 1, 0)]).await;
    let result = server
        .reader(budget(1, 1))
        .fill_history(window(), None, &CancellationToken::new())
        .await
        .unwrap();
    assert!(result.records.is_empty());
    assert_eq!(result.evidence.pages, 1);
    assert!(
        result
            .evidence
            .gaps
            .contains(&BackpackEvidenceGap::RetentionOrReplicationUnknown)
    );
}

#[rstest]
#[tokio::test]
async fn test_deadline_is_total_across_pages_and_cancellation_sends_no_request() {
    let mut first = Reply::page(json!([fixture("fill")]), 0, 1, 2);
    first.delay = Duration::from_millis(150);
    let mut second_fill = fixture("fill");
    second_fill["tradeId"] = json!(2);
    let mut second = Reply::page(json!([second_fill]), 1, 1, 2);
    second.delay = Duration::from_millis(150);
    let server = Server::start(vec![first, second]).await;
    let short = BackpackReadBudget::new(1, 3, 100, Duration::from_millis(250)).unwrap();
    assert!(
        server
            .reader(short)
            .fill_history(window(), None, &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(server.captured().len(), 2);
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(server.reader(budget(1, 2)).snapshot(&cancel).await.is_err());
    assert_eq!(server.captured().len(), 2);
}

#[rstest]
#[tokio::test]
async fn test_outside_fixed_fill_window_rejected() {
    let server = Server::start(vec![Reply::page(json!([fixture("fill")]), 0, 1, 1)]).await;
    let early = BackpackHistoryWindow::new(0, 1).unwrap();
    assert!(matches!(
        server
            .reader(budget(1, 2))
            .fill_history(early, None, &CancellationToken::new())
            .await,
        Err(BackpackAccountError::InvalidField(
            "fill outside history window"
        ))
    ));
}

#[rstest]
#[case("symbol")]
#[case("clientId")]
#[case("id")]
#[tokio::test]
async fn test_resting_response_must_match_requested_selector(#[case] field: &str) {
    let mut body = fixture("resting_order");
    body[field] = if field == "clientId" {
        json!(42)
    } else {
        json!("mismatched-identity")
    };
    let server = Server::start(vec![Reply::json(body)]).await;
    let by_order = field == "id";
    let result = server
        .reader(budget(1, 2))
        .resting_order(
            "BTC_USDC_PERP",
            by_order.then_some("synthetic-order-A"),
            (!by_order).then_some(1),
            &CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        result,
        Err(BackpackAccountError::InvalidField(
            "resting order correlation"
        ))
    ));
    assert_gets(&server);
}

#[rstest]
#[case("orders")]
#[case("fills")]
#[case("funding")]
#[tokio::test]
async fn test_history_symbol_scope_cannot_silently_expand(#[case] endpoint: &str) {
    let mut body = fixture(match endpoint {
        "orders" => "history_order",
        "fills" => "fill",
        _ => "funding",
    });
    body["symbol"] = json!("SOL_USDC_PERP");
    let server = Server::start(vec![Reply::page(json!([body]), 0, 1, 1)]).await;
    let reader = server.reader(budget(1, 2));
    let cancel = CancellationToken::new();
    let failed = match endpoint {
        "orders" => matches!(
            reader
                .order_history(window(), Some("BTC_USDC_PERP"), &cancel)
                .await,
            Err(BackpackAccountError::InvalidField("history symbol"))
        ),
        "fills" => matches!(
            reader
                .fill_history(window(), Some("BTC_USDC_PERP"), &cancel)
                .await,
            Err(BackpackAccountError::InvalidField("history symbol"))
        ),
        _ => matches!(
            reader
                .funding_history(window(), Some("BTC_USDC_PERP"), &cancel)
                .await,
            Err(BackpackAccountError::InvalidField("history symbol"))
        ),
    };
    assert!(failed);
    assert_gets(&server);
}

#[rstest]
fn test_budget_and_window_boundaries_rejected_before_transport() {
    assert!(BackpackReadBudget::new(0, 1, 1, Duration::from_secs(1)).is_err());
    assert!(BackpackReadBudget::new(1001, 1, 1001, Duration::from_secs(1)).is_err());
    assert!(BackpackReadBudget::new(1, 0, 1, Duration::from_secs(1)).is_err());
    assert!(BackpackReadBudget::new(1, 1, 0, Duration::from_secs(1)).is_err());
    assert!(BackpackReadBudget::new(1, 1, 1, Duration::ZERO).is_err());
    assert!(BackpackHistoryWindow::new(1, 1).is_err());
    assert!(BackpackHistoryWindow::new(2, 1).is_err());
    assert!(BackpackHistoryWindow::new(0, u64::MAX).is_err());
}

#[rstest]
#[tokio::test]
async fn test_missing_trade_identity_history_preserves_raw_and_explicit_gap() {
    let mut body = fixture("fill");
    body.as_object_mut().unwrap().remove("tradeId");
    let server = Server::start(vec![Reply::page(json!([body]), 0, 1, 1)]).await;
    let result = server
        .reader(budget(1, 2))
        .fill_history(window(), None, &CancellationToken::new())
        .await
        .unwrap();
    assert!(result.records[0].trade_id.is_none());
    assert_eq!(result.records[0].quantity.0, dec("0.00001"));
    assert!(
        result
            .evidence
            .gaps
            .contains(&BackpackEvidenceGap::MissingTradeId)
    );
}

#[rstest]
fn test_conditional_and_contradictory_orders_are_not_simplified() {
    let scope = Scope::new();
    let mut trigger = fixture("resting_order");
    trigger["triggerPrice"] = json!("100");
    assert!(
        order_report(serde_json::from_value(trigger).unwrap(), &scope.context())
            .unwrap()
            .report
            .is_none()
    );
    let mut contradictory = fixture("resting_order");
    contradictory["status"] = json!("Filled");
    assert!(matches!(
        order_report(
            serde_json::from_value(contradictory).unwrap(),
            &scope.context()
        ),
        Err(BackpackAccountError::InvalidField("executedQuantity"))
    ));
}

#[rstest]
fn test_colliding_external_client_id_never_attaches_a_native_order_without_binding() {
    let scope = Scope::new();
    let mut raw: BackpackFill = model("fill");
    raw.order_id = "unrelated-external-order".into();
    let observed = fill_report(raw, &scope.context()).unwrap();
    assert!(matches!(
        observed.ownership,
        BackpackOrderOwnership::Reserved(_)
    ));
    assert!(observed.report.unwrap().client_order_id.is_none());
    assert!(
        observed
            .gaps
            .contains(&BackpackEvidenceGap::OwnershipUnverified)
    );
    let order = order_report(model("resting_order"), &scope.context()).unwrap();
    assert!(order.report.unwrap().client_order_id.is_none());
}

#[rstest]
fn test_native_order_attribution_requires_independent_matching_command_binding() {
    let scope = Scope::new();
    let mut bindings = BackpackOrderBindings::default();
    bindings
        .confirm_acknowledged(
            ClientOrderId::from("O-SYNTHETIC-1"),
            VenueOrderId::from("synthetic-order-A"),
            InstrumentId::from("BTC_USDC_PERP.BACKPACK"),
            &scope.store,
        )
        .unwrap();
    let mut context = scope.context();
    context.confirmed_orders = Some(&bindings);
    let fill = fill_report(model("fill"), &context).unwrap();
    assert_eq!(
        fill.ownership,
        BackpackOrderOwnership::Confirmed(ClientOrderId::from("O-SYNTHETIC-1"))
    );
    assert_eq!(
        fill.report.unwrap().client_order_id,
        Some(ClientOrderId::from("O-SYNTHETIC-1"))
    );
    assert!(
        !fill
            .gaps
            .contains(&BackpackEvidenceGap::OwnershipUnverified)
    );
    let order = order_report(model("resting_order"), &context).unwrap();
    assert_eq!(
        order.report.unwrap().client_order_id,
        Some(ClientOrderId::from("O-SYNTHETIC-1"))
    );
    let mut colliding: BackpackFill = model("fill");
    colliding.order_id = "external-order-with-colliding-id".into();
    assert!(
        fill_report(colliding, &context)
            .unwrap()
            .report
            .unwrap()
            .client_order_id
            .is_none()
    );
    let mut mismatched: BackpackFill = model("fill");
    mismatched.client_id = Some("42".into());
    assert!(matches!(
        fill_report(mismatched, &context),
        Err(BackpackAccountError::Conflict)
    ));
}

#[rstest]
fn test_command_binding_rejects_unreserved_and_conflicting_venue_native_links() {
    let scope = Scope::new();
    let mut bindings = BackpackOrderBindings::default();
    let instrument = InstrumentId::from("BTC_USDC_PERP.BACKPACK");
    assert!(
        bindings
            .confirm_acknowledged(
                ClientOrderId::from("UNRESERVED"),
                VenueOrderId::from("unknown"),
                instrument,
                &scope.store
            )
            .is_err()
    );
    bindings
        .confirm_acknowledged(
            ClientOrderId::from("O-SYNTHETIC-1"),
            VenueOrderId::from("synthetic-order-A"),
            instrument,
            &scope.store,
        )
        .unwrap();
    assert!(matches!(
        bindings.confirm_acknowledged(
            ClientOrderId::from("O-SYNTHETIC-1"),
            VenueOrderId::from("other-order"),
            instrument,
            &scope.store
        ),
        Err(BackpackAccountError::Conflict)
    ));
    assert!(matches!(
        bindings.confirm_acknowledged(
            ClientOrderId::from("O-SYNTHETIC-1"),
            VenueOrderId::from("synthetic-order-A"),
            InstrumentId::from("SOL_USDC_PERP.BACKPACK"),
            &scope.store
        ),
        Err(BackpackAccountError::Conflict)
    ));
}

#[rstest]
fn test_first_fill_pending_attribution_can_be_qualified_by_later_command_ack() {
    let scope = Scope::new();
    let first = fill_report(model("fill"), &scope.context())
        .unwrap()
        .report
        .unwrap();
    let mut reconciler = BackpackFillReconciler::from_applied(10, []).unwrap();
    reconciler.stage(first).unwrap();
    assert!(
        reconciler.pending_in_event_order()[0]
            .client_order_id
            .is_none()
    );
    let mut bindings = BackpackOrderBindings::default();
    bindings
        .confirm_acknowledged(
            ClientOrderId::from("O-SYNTHETIC-1"),
            VenueOrderId::from("synthetic-order-A"),
            InstrumentId::from("BTC_USDC_PERP.BACKPACK"),
            &scope.store,
        )
        .unwrap();
    let mut context = scope.context();
    context.confirmed_orders = Some(&bindings);
    let confirmed = fill_report(model("fill"), &context)
        .unwrap()
        .report
        .unwrap();
    let BackpackFillStage::Pending(delivery) = reconciler.stage(confirmed).unwrap() else {
        panic!("unacknowledged economics must stay pending")
    };
    assert_eq!(
        delivery.client_order_id,
        Some(ClientOrderId::from("O-SYNTHETIC-1"))
    );
    assert_eq!(reconciler.applied_records().count(), 0);
}

#[rstest]
#[case("New", "0.00001")]
#[case("PartiallyFilled", "0")]
#[case("PartiallyFilled", "0.00002")]
fn test_known_order_status_and_filled_quantity_must_agree(
    #[case] status: &str,
    #[case] filled: &str,
) {
    let scope = Scope::new();
    let mut raw = fixture("resting_order");
    raw["status"] = json!(status);
    raw["executedQuantity"] = json!(filled);
    assert!(matches!(
        order_report(serde_json::from_value(raw).unwrap(), &scope.context()),
        Err(BackpackAccountError::InvalidField("executedQuantity"))
    ));
}
