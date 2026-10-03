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

//! Explicit synthetic loopback peer facts and real HTTP mutation fault probes.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
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
use ed25519_dalek::{Signature, SigningKey};
use nautilus_backpack::{
    common::{credential::BackpackCredential, endpoints::BackpackEndpoints},
    config::BackpackConfig,
    execution::{
        BackpackExecutionError, BackpackExecutionErrorKind, BackpackMutationStatus,
        command::BackpackOrderSpec,
        guard::{
            BackpackExecutionAuthority, BackpackLoopbackAccountFacts, BackpackLoopbackMarketFacts,
        },
        owner::{
            BackpackLoopbackTerminalEvidence, BackpackMutationPolicy, BackpackOrderOwner,
            BackpackOrderOwnerConfig,
        },
    },
    http::{
        client::{BackpackClock, BackpackHttpClient, BackpackHttpPolicy},
        error::{BackpackHttpError, BackpackRequestOutcome},
        quota::BackpackQuota,
        request::{BackpackReadOperation, BackpackReadRequest},
    },
    identity::{BackpackClientIdNamespace, BackpackClientIdStore},
    models::BackpackMarket,
    parsing::parse_market,
    signing::{BackpackParameters, BackpackReceiveWindow},
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::QuoteTick,
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    types::{Price, Quantity},
};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
struct Captured {
    method: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
    uri: String,
}
#[derive(Clone, Debug)]
struct Reply {
    status: u16,
    body: Option<String>,
    delay: Duration,
}
impl Reply {
    fn status(status: u16, body: &str) -> Self {
        Self {
            status,
            body: Some(body.into()),
            delay: Duration::ZERO,
        }
    }
    fn delayed(delay: Duration) -> Self {
        Self {
            status: 200,
            body: None,
            delay,
        }
    }
}
#[derive(Debug)]
struct ServerState {
    captured: Mutex<Vec<Captured>>,
    replies: Mutex<VecDeque<Reply>>,
    last_order: Mutex<Option<Value>>,
    notify: Notify,
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
            captured: Mutex::new(vec![]),
            replies: Mutex::new(replies.into()),
            last_order: Mutex::new(None),
            notify: Notify::new(),
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
    fn captured(&self) -> Vec<Captured> {
        self.state.captured.lock().unwrap().clone()
    }
    async fn wait_requests(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let wait = self.state.notify.notified();
                if self.state.captured.lock().unwrap().len() >= count {
                    break;
                }
                wait.await;
            }
        })
        .await
        .unwrap();
    }
}
async fn handler(State(state): State<Arc<ServerState>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, 8192).await.unwrap();
    let method = parts.method.to_string();
    let mut response = json!({});
    if method == "POST" {
        response = serde_json::from_slice(&bytes).unwrap();
        response["id"] = json!(format!("venue-{}", response["clientId"]));
        response["executedQuantity"] = json!("0");
        response["status"] = json!("New");
        *state.last_order.lock().unwrap() = Some(response.clone());
    } else if method == "DELETE" {
        response = state.last_order.lock().unwrap().clone().unwrap();
        response["status"] = json!("Cancelled");
    }
    state.captured.lock().unwrap().push(Captured {
        method,
        uri: parts.uri.to_string(),
        headers: parts
            .headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_owned()))
            .collect(),
        body: bytes.to_vec(),
    });
    state.notify.notify_one();
    let reply = state.replies.lock().unwrap().pop_front().unwrap_or(Reply {
        status: 200,
        body: None,
        delay: Duration::ZERO,
    });
    tokio::time::sleep(reply.delay).await;
    Response::builder()
        .status(reply.status)
        .header("content-type", "application/json")
        .body(Body::from(
            reply.body.unwrap_or_else(|| response.to_string()),
        ))
        .unwrap()
}
#[derive(Debug)]
struct Clock(AtomicU64);
impl BackpackClock for Clock {
    fn timestamp_ms(&self) -> Result<u64, BackpackHttpError> {
        Ok(self.0.load(Ordering::Acquire))
    }
}
fn decimal(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap()
}
fn namespace(endpoints: &BackpackEndpoints) -> BackpackClientIdNamespace {
    BackpackClientIdNamespace::loopback_peer(endpoints, "peer-account", None).unwrap()
}
fn instrument() -> InstrumentId {
    InstrumentId::from("BTC_USDC_PERP.BACKPACK")
}
fn authority() -> BackpackExecutionAuthority {
    BackpackExecutionAuthority {
        expires_at_ms: 100_000,
        max_account_age_ms: 10_000,
        max_market_age_ms: 10_000,
        max_order_notional: decimal("1000"),
        max_reserved_notional: decimal("2000"),
        max_reserved_margin: decimal("500"),
        max_unsettled_orders: 4,
        allow_new_risk: true,
        allow_reduction: true,
        allow_owned_cancel: true,
    }
}
fn account(
    endpoints: &BackpackEndpoints,
    generation: u64,
    now: u64,
) -> BackpackLoopbackAccountFacts {
    BackpackLoopbackAccountFacts {
        namespace: namespace(endpoints),
        generation,
        observed_at_ms: now,
        available_margin: decimal("500"),
        margin_per_notional: decimal("0.1"),
        fee_buffer_per_notional: decimal("0.001"),
        economics_reference: "explicit synthetic local margin/fee model".into(),
        net_positions: BTreeMap::from([(instrument(), decimal("1"))]),
        auto_borrow: false,
        auto_lend: false,
        auto_repay: false,
        liquidating: false,
        complete: true,
    }
}
fn market(generation: u64, now: u64) -> BackpackLoopbackMarketFacts {
    let raw: BackpackMarket =
        serde_json::from_str(include_str!("../test_data/btc_usdc_perp.json")).unwrap();
    let metadata = parse_market(
        &raw,
        &BackpackConfig::new_checked(vec!["BTC_USDC_PERP".into()]).unwrap(),
        UnixNanos::from(now * 1_000_000),
    )
    .unwrap();
    let quote = QuoteTick::new_checked(
        instrument(),
        Price::from_decimal(decimal("99.9")).unwrap(),
        Price::from_decimal(decimal("100.1")).unwrap(),
        Quantity::from_decimal(decimal("1")).unwrap(),
        Quantity::from_decimal(decimal("1")).unwrap(),
        UnixNanos::from(now * 1_000_000),
        UnixNanos::from(now * 1_000_000),
    )
    .unwrap();
    BackpackLoopbackMarketFacts {
        generation,
        metadata,
        quote,
    }
}
fn spec(id: &str) -> BackpackOrderSpec {
    BackpackOrderSpec {
        client_order_id: ClientOrderId::from(id),
        instrument_id: instrument(),
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Gtc,
        quantity: decimal("0.1"),
        price: Some(decimal("100")),
        post_only: false,
        reduce_only: false,
    }
}
fn build_owner(
    server: &Server,
    directory: &TempDir,
    clock: Arc<Clock>,
    quota: BackpackQuota,
    budget: Duration,
) -> BackpackOrderOwner {
    let config = BackpackOrderOwnerConfig {
        config: BackpackConfig::with_endpoints_checked(
            vec!["BTC_USDC_PERP".into()],
            server.endpoints.clone(),
        )
        .unwrap(),
        endpoints: server.endpoints.clone(),
        credential: BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &server.endpoints)
            .unwrap(),
        quota,
        clock,
        policy: BackpackMutationPolicy {
            window: BackpackReceiveWindow::default(),
            budget,
        },
        identities: BackpackClientIdStore::open(directory.path(), &namespace(&server.endpoints))
            .unwrap(),
        namespace: namespace(&server.endpoints),
        authority: authority(),
    };
    BackpackOrderOwner::new_checked(config).unwrap()
}
fn prepare(owner: &BackpackOrderOwner, server: &Server, generation: u64, now: u64) {
    let guard = owner.guard();
    guard
        .begin_session(&namespace(&server.endpoints), generation)
        .unwrap();
    guard
        .update_account(account(&server.endpoints, generation, now), now)
        .unwrap();
    guard.update_market(market(generation, now), now).unwrap();
}
fn local_kind(error: BackpackExecutionError, kind: BackpackExecutionErrorKind) {
    match error {
        BackpackExecutionError::Local(found) => assert_eq!(found, kind),
        BackpackExecutionError::Http(error) => panic!("unexpected HTTP failure: {error}"),
    }
}

#[rstest]
#[tokio::test]
async fn test_true_post_ack_is_bound_signed_and_intent_precedes_first_byte() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(1000)));
    let owner = build_owner(
        &server,
        &directory,
        clock,
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let receipt = owner
        .submit(spec("original"), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(receipt.status(), BackpackMutationStatus::ResponseObserved);
    assert_eq!(
        owner.confirmed_binding(ClientOrderId::from("original")),
        Some((VenueOrderId::from("venue-1"), instrument()))
    );
    let captured = server.captured();
    assert_eq!(captured.len(), 1);
    let request = &captured[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.uri, "/api/v1/order");
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["clientId"], json!(1));
    assert_eq!(body["quantity"], json!("0.1"));
    assert!(body.get("autoBorrow").is_none());
    verify_signature(request, "orderExecute");
    let checkpoint = std::fs::read_to_string(directory.path().join("identity.json")).unwrap();
    assert!(checkpoint.contains("original"));
    assert!(checkpoint.contains("notional"));
    local_kind(
        owner
            .submit(spec("original"), &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::Duplicate,
    );
    assert_eq!(server.captured().len(), 1);
}
fn verify_signature(request: &Captured, instruction: &str) {
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    let mut parameters: Vec<_> = body
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                if let Some(value) = value.as_str() {
                    value.to_owned()
                } else {
                    value.to_string()
                },
            )
        })
        .collect();
    parameters.sort();
    let parameters = parameters
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical = format!(
        "instruction={instruction}&{parameters}&timestamp={}&window={}",
        request.headers["x-timestamp"], request.headers["x-window"]
    );
    let signature =
        Signature::from_slice(&STANDARD.decode(&request.headers["x-signature"]).unwrap()).unwrap();
    SigningKey::from_bytes(&[7; 32])
        .verifying_key()
        .verify_strict(canonical.as_bytes(), &signature)
        .unwrap();
}

#[rstest]
#[case(
    OrderType::StopLimit,
    TimeInForce::Gtc,
    false,
    false,
    Some(decimal("100"))
)]
#[case(OrderType::Limit, TimeInForce::Gtd, false, false, Some(decimal("100")))]
#[case(OrderType::Limit, TimeInForce::Ioc, true, false, Some(decimal("100")))]
#[case(OrderType::Market, TimeInForce::Ioc, false, false, None)]
#[case(OrderType::Market, TimeInForce::Gtc, false, true, None)]
#[tokio::test]
async fn test_unsupported_matrix_sends_zero(
    #[case] order_type: OrderType,
    #[case] tif: TimeInForce,
    #[case] post: bool,
    #[case] reduce: bool,
    #[case] price: Option<Decimal>,
) {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let mut command = spec("unsupported");
    command.order_type = order_type;
    command.time_in_force = tif;
    command.post_only = post;
    command.reduce_only = reduce;
    command.price = price;
    local_kind(
        owner
            .submit(command, &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::UnsupportedCommand,
    );
    assert!(server.captured().is_empty());
}

#[rstest]
#[case("0.100001", "100")]
#[case("0.000001", "100")]
#[case("0.1", "100.01")]
#[case("0.1", "1000000.1")]
#[case("0.1", "0")]
#[tokio::test]
async fn test_exact_grid_and_bounds_refuse_before_intent(
    #[case] quantity: &str,
    #[case] price: &str,
) {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let mut command = spec("offgrid");
    command.quantity = decimal(quantity);
    command.price = Some(decimal(price));
    assert!(
        owner
            .submit(command, &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(server.captured().is_empty());
    assert!(
        !std::fs::read_to_string(directory.path().join("identity.json"))
            .unwrap()
            .contains("offgrid")
    );
}

#[rstest]
#[case(TimeInForce::Gtc, false)]
#[case(TimeInForce::Gtc, true)]
#[case(TimeInForce::Ioc, false)]
#[case(TimeInForce::Fok, false)]
#[tokio::test]
async fn test_supported_limit_matrix(#[case] tif: TimeInForce, #[case] post: bool) {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let mut command = spec("matrix");
    command.time_in_force = tif;
    command.post_only = post;
    owner
        .submit(command, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(server.captured().len(), 1);
}

#[rstest]
#[tokio::test]
async fn test_reduce_only_market_requires_direction_and_unreserved_position() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let mut command = spec("reduce");
    command.order_type = OrderType::Market;
    command.price = None;
    command.time_in_force = TimeInForce::Ioc;
    command.reduce_only = true;
    command.quantity = decimal("0.6");
    local_kind(
        owner
            .submit(command.clone(), &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::Capacity,
    );
    command.side = OrderSide::Sell;
    owner
        .submit(command.clone(), &CancellationToken::new())
        .await
        .unwrap();
    command.client_order_id = ClientOrderId::from("reduce2");
    local_kind(
        owner
            .submit(command, &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::Capacity,
    );
    assert_eq!(server.captured().len(), 1);
}

#[rstest]
#[case(429, "{}")]
#[case(500, "{}")]
#[case(200, "unreadable")]
#[case(404, "{}")]
#[tokio::test]
async fn test_unknown_response_never_retries_or_changes_original_id(
    #[case] status: u16,
    #[case] body: &str,
) {
    let server = Server::start(vec![Reply::status(status, body)]).await;
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(1000)));
    let owner = build_owner(
        &server,
        &directory,
        clock.clone(),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    assert_eq!(
        owner
            .submit(spec("unknown"), &CancellationToken::new())
            .await
            .unwrap_err()
            .outcome(),
        BackpackRequestOutcome::Unknown
    );
    owner
        .guard()
        .update_account(account(&server.endpoints, 1, 1000), 1000)
        .unwrap();
    assert!(
        owner
            .submit(spec("new-id"), &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(
        owner
            .cancel_owned(ClientOrderId::from("unknown"), &CancellationToken::new())
            .await
            .is_err()
    );
    let first = owner.stop().unwrap();
    assert!(first.dirty);
    assert_eq!(first.unknown, 1);
    assert_eq!(owner.stop().unwrap(), first);
    drop(owner);
    let restored = build_owner(
        &server,
        &directory,
        clock,
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&restored, &server, 2, 1000);
    local_kind(
        restored
            .submit(spec("unknown"), &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::Duplicate,
    );
    assert_eq!(server.captured().len(), 1);
}

#[rstest]
#[tokio::test]
async fn test_accept_then_response_loss_keeps_one_post_and_unknown_capacity() {
    let server = Server::start(vec![Reply::delayed(Duration::from_secs(2))]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_millis(100),
    );
    prepare(&owner, &server, 1, 1000);
    let error = owner
        .submit(spec("lost"), &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(server.captured().len(), 1);
    assert!(server.state.last_order.lock().unwrap().is_some());
    local_kind(
        owner
            .submit(spec("lost"), &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::Duplicate,
    );
    assert_eq!(owner.stop().unwrap().unknown, 1);
}

#[rstest]
#[tokio::test]
async fn test_cancel_202_pending_owned_only_and_true_fills_required_to_release() {
    let server = Server::start(vec![
        Reply {
            status: 200,
            body: None,
            delay: Duration::ZERO,
        },
        Reply::status(202, ""),
    ])
    .await;
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(1000)));
    let owner = build_owner(
        &server,
        &directory,
        clock.clone(),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let id = ClientOrderId::from("owned");
    owner
        .submit(spec("owned"), &CancellationToken::new())
        .await
        .unwrap();
    assert!(
        owner
            .cancel_owned(ClientOrderId::from("external"), &CancellationToken::new())
            .await
            .is_err()
    );
    let receipt = owner
        .cancel_owned(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(receipt.status(), BackpackMutationStatus::CancelPending);
    let request = &server.captured()[1];
    assert_eq!(request.method, "DELETE");
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["orderId"], json!("venue-1"));
    assert!(body.get("clientId").is_none());
    verify_signature(request, "orderCancel");
    assert!(
        owner
            .cancel_owned(id, &CancellationToken::new())
            .await
            .is_err()
    );
    let mut evidence = BackpackLoopbackTerminalEvidence {
        client_order_id: id,
        venue_order_id: VenueOrderId::from("venue-1"),
        instrument_id: instrument(),
        generation: 1,
        cumulative_quantity: decimal("0.05"),
        applied_fill_quantity: Decimal::ZERO,
        economic_ack_reference: "durable true fill+fee application ACK".into(),
    };
    assert!(owner.acknowledge_reconciled_terminal(&evidence).is_err());
    evidence.applied_fill_quantity = decimal("0.05");
    owner.acknowledge_reconciled_terminal(&evidence).unwrap();
    owner
        .guard()
        .update_account(account(&server.endpoints, 1, 1000), 1000)
        .unwrap();
    assert_eq!(owner.stop().unwrap().pending_cancellations, 0);
    drop(owner);
    let restored = build_owner(
        &server,
        &directory,
        clock,
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&restored, &server, 2, 1000);
    assert!(
        restored
            .cancel_owned(id, &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(server.captured().len(), 2);
}

#[rstest]
#[case("invalidate")]
#[case("stop")]
#[case("expire")]
#[case("limit")]
#[tokio::test]
async fn test_queued_request_rechecks_session_stop_freshness_and_authority(#[case] change: &str) {
    let server = Server::start(vec![]).await;
    let quota =
        BackpackQuota::with_periods(Duration::from_millis(300), Duration::from_secs(2)).unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(1000)));
    let read = BackpackHttpClient::new(
        server.endpoints.clone(),
        None,
        quota.clone(),
        BackpackHttpPolicy::default(),
        clock.clone(),
    )
    .unwrap();
    read.read(
        &BackpackReadRequest::new(
            BackpackReadOperation::Markets,
            BackpackParameters::default(),
        )
        .unwrap(),
        None,
        None,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let directory = TempDir::new().unwrap();
    let owner = Arc::new(build_owner(
        &server,
        &directory,
        clock.clone(),
        quota,
        Duration::from_secs(2),
    ));
    prepare(&owner, &server, 1, 1000);
    let sending = owner.clone();
    let task = tokio::spawn(async move {
        sending
            .submit(spec("queued"), &CancellationToken::new())
            .await
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    match change {
        "invalidate" => owner.guard().invalidate(),
        "stop" => {
            owner.stop().unwrap();
        }
        "expire" => clock.0.store(50_000, Ordering::Release),
        _ => {
            let mut limits = authority();
            limits.allow_new_risk = false;
            owner.guard().update_authority(limits).unwrap();
        }
    }
    assert_eq!(
        task.await.unwrap().unwrap_err().outcome(),
        BackpackRequestOutcome::NotSent
    );
    assert_eq!(server.captured().len(), 1);
}

#[rstest]
#[tokio::test]
async fn test_shared_quota_longer_than_receive_window_signs_fresh_after_wait() {
    let server = Server::start(vec![]).await;
    let quota =
        BackpackQuota::with_periods(Duration::from_millis(5100), Duration::from_secs(2)).unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(1000)));
    let read = BackpackHttpClient::new(
        server.endpoints.clone(),
        None,
        quota.clone(),
        BackpackHttpPolicy::default(),
        clock.clone(),
    )
    .unwrap();
    read.read(
        &BackpackReadRequest::new(
            BackpackReadOperation::Markets,
            BackpackParameters::default(),
        )
        .unwrap(),
        None,
        None,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let directory = TempDir::new().unwrap();
    let owner = Arc::new(build_owner(
        &server,
        &directory,
        clock.clone(),
        quota.clone(),
        Duration::from_secs(7),
    ));
    prepare(&owner, &server, 1, 1000);
    let sending = owner.clone();
    let task = tokio::spawn(async move {
        sending
            .submit(spec("fresh-sign"), &CancellationToken::new())
            .await
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    clock.0.store(7000, Ordering::Release);
    owner
        .guard()
        .update_account(account(&server.endpoints, 1, 7000), 7000)
        .unwrap();
    owner.guard().update_market(market(1, 7000), 7000).unwrap();
    task.await.unwrap().unwrap();
    server.wait_requests(2).await;
    let request = &server.captured()[1];
    assert_eq!(request.headers["x-timestamp"], "7000");
    verify_signature(request, "orderExecute");
    let diagnostic = quota.diagnostics();
    assert_eq!(diagnostic.schema_version, 1);
    assert_eq!(diagnostic.observed_waits, 2);
    assert_eq!(diagnostic.admitted_waits, 2);
    assert_eq!(diagnostic.refused_waits, 0);
    assert!(diagnostic.last_queue_wait_ns >= 4_900_000_000);
    assert_eq!(diagnostic.last_admitted, Some(true));
}

#[rstest]
#[tokio::test]
async fn test_intent_storage_failure_sends_zero_and_poison_is_sticky() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    std::fs::remove_file(directory.path().join("identity.initialized")).unwrap();
    assert!(
        owner
            .submit(spec("failed"), &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(
        owner
            .guard()
            .begin_session(&namespace(&server.endpoints), 2)
            .is_err()
    );
    assert!(server.captured().is_empty());
}

#[rstest]
#[tokio::test]
async fn test_freshness_future_unknown_policy_and_exact_capacity_have_no_defaults() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let mut facts = account(&server.endpoints, 1, 1001);
    assert!(owner.guard().update_account(facts.clone(), 1000).is_err());
    assert!(
        owner
            .submit(spec("future"), &CancellationToken::new())
            .await
            .is_err()
    );
    facts.observed_at_ms = 1000;
    facts.auto_borrow = true;
    assert!(owner.guard().update_account(facts.clone(), 1000).is_err());
    facts.auto_borrow = false;
    facts.available_margin = decimal("1");
    owner.guard().update_account(facts, 1000).unwrap();
    local_kind(
        owner
            .submit(spec("margin"), &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::Capacity,
    );
    let mut facts = account(&server.endpoints, 1, 1000);
    facts.margin_per_notional = decimal("0.3333333333333333333333333333");
    owner.guard().update_account(facts, 1000).unwrap();
    let mut command = spec("precision");
    command.quantity = decimal("0.00001");
    local_kind(
        owner
            .submit(command, &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::Capacity,
    );
    assert!(server.captured().is_empty());
}

#[rstest]
#[tokio::test]
async fn test_definitive_rejection_releases_capacity_but_identity_is_permanent() {
    let server = Server::start(vec![Reply::status(
        400,
        r#"{"code":"INVALID_CLIENT_REQUEST"}"#,
    )])
    .await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    assert_eq!(
        owner
            .submit(spec("rejected"), &CancellationToken::new())
            .await
            .unwrap_err()
            .outcome(),
        BackpackRequestOutcome::VenueRejected
    );
    owner
        .submit(spec("next"), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&server.captured()[1].body).unwrap()["clientId"],
        json!(2)
    );
}

#[rstest]
#[tokio::test]
async fn test_production_write_and_wrong_credential_audience_are_explicitly_refused() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let make =
        |endpoints: BackpackEndpoints, credential: BackpackCredential| BackpackOrderOwnerConfig {
            config: BackpackConfig::with_endpoints_checked(
                vec!["BTC_USDC_PERP".into()],
                endpoints.clone(),
            )
            .unwrap(),
            endpoints,
            credential,
            quota: BackpackQuota::default(),
            clock: Arc::new(Clock(AtomicU64::new(1000))),
            policy: BackpackMutationPolicy {
                window: BackpackReceiveWindow::default(),
                budget: Duration::from_secs(1),
            },
            identities: BackpackClientIdStore::open(
                directory.path(),
                &namespace(&server.endpoints),
            )
            .unwrap(),
            namespace: namespace(&server.endpoints),
            authority: authority(),
        };
    local_kind(
        BackpackOrderOwner::new_checked(make(
            BackpackEndpoints::production(),
            BackpackCredential::production(&STANDARD.encode([7; 32])).unwrap(),
        ))
        .unwrap_err(),
        BackpackExecutionErrorKind::UnsupportedProduction,
    );
    assert!(
        BackpackOrderOwner::new_checked(make(
            server.endpoints.clone(),
            BackpackCredential::production(&STANDARD.encode([7; 32])).unwrap()
        ))
        .is_err()
    );
    assert!(server.captured().is_empty());
}

#[rstest]
#[case(0, 20_000)]
#[case(20_001, 20_000)]
#[tokio::test]
async fn test_old_engine_event_with_fresh_receipt_and_future_event_send_zero(
    #[case] event_ms: u64,
    #[case] now: u64,
) {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(now))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, now);
    let mut facts = market(1, now);
    facts.quote.ts_event = UnixNanos::from(event_ms * 1_000_000);
    assert!(owner.guard().update_market(facts, now).is_err());
    assert!(
        owner
            .submit(spec("stale-event"), &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(server.captured().is_empty());
}

#[rstest]
#[tokio::test]
async fn test_exact_authority_expiry_millisecond_has_zero_dispatch() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(1000)));
    let owner = build_owner(
        &server,
        &directory,
        clock.clone(),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    clock.0.store(authority().expires_at_ms, Ordering::Release);
    assert!(
        owner
            .submit(spec("expired"), &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(server.captured().is_empty());
}

#[rstest]
#[tokio::test]
async fn test_true_terminal_fill_ack_wins_over_late_202_cancel_response() {
    let server = Server::start(vec![
        Reply {
            status: 200,
            body: None,
            delay: Duration::ZERO,
        },
        Reply {
            status: 202,
            body: Some(String::new()),
            delay: Duration::from_millis(100),
        },
    ])
    .await;
    let directory = TempDir::new().unwrap();
    let owner = Arc::new(build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    ));
    prepare(&owner, &server, 1, 1000);
    owner
        .submit(spec("racing"), &CancellationToken::new())
        .await
        .unwrap();
    let deleting = owner.clone();
    let task = tokio::spawn(async move {
        deleting
            .cancel_owned(ClientOrderId::from("racing"), &CancellationToken::new())
            .await
    });
    server.wait_requests(2).await;
    owner
        .acknowledge_reconciled_terminal(&BackpackLoopbackTerminalEvidence {
            client_order_id: ClientOrderId::from("racing"),
            venue_order_id: VenueOrderId::from("venue-1"),
            instrument_id: instrument(),
            generation: 1,
            cumulative_quantity: decimal("0.1"),
            applied_fill_quantity: decimal("0.1"),
            economic_ack_reference: "durable synthetic true fill and fee ACK before response"
                .into(),
        })
        .unwrap();
    assert_eq!(
        task.await.unwrap().unwrap().status(),
        BackpackMutationStatus::CancelPending
    );
    assert_eq!(owner.stop().unwrap().pending_cancellations, 0);
}

#[rstest]
#[tokio::test]
async fn test_cancel_attempt_survives_restart_without_resend() {
    let server = Server::start(vec![
        Reply {
            status: 200,
            body: None,
            delay: Duration::ZERO,
        },
        Reply::status(202, ""),
    ])
    .await;
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(1000)));
    let owner = build_owner(
        &server,
        &directory,
        clock.clone(),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    owner
        .submit(spec("pending"), &CancellationToken::new())
        .await
        .unwrap();
    owner
        .cancel_owned(ClientOrderId::from("pending"), &CancellationToken::new())
        .await
        .unwrap();
    drop(owner);
    let restored = build_owner(
        &server,
        &directory,
        clock,
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&restored, &server, 2, 1000);
    assert!(
        restored
            .confirmed_binding(ClientOrderId::from("pending"))
            .is_some()
    );
    local_kind(
        restored
            .cancel_owned(ClientOrderId::from("pending"), &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::Duplicate,
    );
    assert_eq!(server.captured().len(), 2);
}

#[rstest]
#[tokio::test]
async fn test_checkpoint_failure_after_identity_commit_sends_zero_and_restores_unknown() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(1000)));
    let owner = build_owner(
        &server,
        &directory,
        clock.clone(),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    std::fs::remove_file(directory.path().join("execution.json")).unwrap();
    assert!(
        owner
            .submit(spec("committed"), &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(server.captured().is_empty());
    drop(owner);
    let restored = build_owner(
        &server,
        &directory,
        clock,
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&restored, &server, 2, 1000);
    local_kind(
        restored
            .submit(spec("committed"), &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::Duplicate,
    );
    assert_eq!(restored.stop().unwrap().unknown, 1);
}

#[rstest]
fn test_advanced_flags_have_no_native_wire_path() {
    let mut value = serde_json::to_value(spec("advanced")).unwrap();
    value["autoBorrow"] = json!(true);
    assert!(serde_json::from_value::<BackpackOrderSpec>(value).is_err());
}

#[rstest]
#[tokio::test]
async fn test_final_quota_check_uses_engine_age_even_with_fresh_receipt() {
    let server = Server::start(vec![]).await;
    let quota =
        BackpackQuota::with_periods(Duration::from_millis(300), Duration::from_secs(2)).unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(20_000)));
    let read = BackpackHttpClient::new(
        server.endpoints.clone(),
        None,
        quota.clone(),
        BackpackHttpPolicy::default(),
        clock.clone(),
    )
    .unwrap();
    read.read(
        &BackpackReadRequest::new(
            BackpackReadOperation::Markets,
            BackpackParameters::default(),
        )
        .unwrap(),
        None,
        None,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let directory = TempDir::new().unwrap();
    let owner = Arc::new(build_owner(
        &server,
        &directory,
        clock.clone(),
        quota,
        Duration::from_secs(2),
    ));
    prepare(&owner, &server, 1, 20_000);
    let mut facts = market(1, 20_000);
    facts.quote.ts_event = UnixNanos::from(10_001_000_000);
    owner.guard().update_market(facts, 20_000).unwrap();
    let sending = owner.clone();
    let task = tokio::spawn(async move {
        sending
            .submit(spec("aged-engine"), &CancellationToken::new())
            .await
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    clock.0.store(20_002, Ordering::Release);
    owner
        .guard()
        .update_account(account(&server.endpoints, 1, 20_002), 20_002)
        .unwrap();
    assert_eq!(
        task.await.unwrap().unwrap_err().outcome(),
        BackpackRequestOutcome::NotSent
    );
    assert_eq!(server.captured().len(), 1);
}

#[rstest]
#[tokio::test]
async fn test_old_flat_snapshot_does_not_make_shutdown_clean() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(Clock(AtomicU64::new(1000)));
    let owner = build_owner(
        &server,
        &directory,
        clock.clone(),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let mut facts = account(&server.endpoints, 1, 1000);
    facts.net_positions.insert(instrument(), Decimal::ZERO);
    owner.guard().update_account(facts, 1000).unwrap();
    clock.0.store(11_001, Ordering::Release);
    let first = owner.stop().unwrap();
    assert!(first.dirty && first.positions_unknown_or_nonzero);
    clock.0.store(1000, Ordering::Release);
    assert!(owner.stop().unwrap().dirty);
}

#[rstest]
#[tokio::test]
async fn test_cancel_permission_is_distinct_from_new_risk_and_data_freshness() {
    let server = Server::start(vec![
        Reply {
            status: 200,
            body: None,
            delay: Duration::ZERO,
        },
        Reply::status(202, ""),
    ])
    .await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    owner
        .submit(spec("cancel-separate"), &CancellationToken::new())
        .await
        .unwrap();
    owner.guard().invalidate();
    owner
        .guard()
        .begin_session(&namespace(&server.endpoints), 2)
        .unwrap();
    let mut policy = authority();
    policy.allow_new_risk = false;
    policy.allow_reduction = false;
    owner.guard().update_authority(policy).unwrap();
    assert_eq!(
        owner
            .cancel_owned(
                ClientOrderId::from("cancel-separate"),
                &CancellationToken::new()
            )
            .await
            .unwrap()
            .status(),
        BackpackMutationStatus::CancelPending
    );
}

#[rstest]
#[tokio::test]
async fn test_new_risk_sell_limit_cannot_claim_limit_price_notional_upper_bound() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let mut command = spec("sell-hardcap");
    command.side = OrderSide::Sell;
    local_kind(
        owner
            .submit(command, &CancellationToken::new())
            .await
            .unwrap_err(),
        BackpackExecutionErrorKind::UnsupportedCommand,
    );
    assert!(server.captured().is_empty());
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn test_config_and_durable_namespace_cannot_use_another_origin(
    #[case] config_mismatch: bool,
) {
    let server = Server::start(vec![]).await;
    let other = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let origin = if config_mismatch {
        &server.endpoints
    } else {
        &other.endpoints
    };
    let scope = namespace(origin);
    let input = BackpackOrderOwnerConfig {
        config: BackpackConfig::with_endpoints_checked(
            vec!["BTC_USDC_PERP".into()],
            other.endpoints.clone(),
        )
        .unwrap(),
        endpoints: server.endpoints.clone(),
        credential: BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &server.endpoints)
            .unwrap(),
        quota: BackpackQuota::default(),
        clock: Arc::new(Clock(AtomicU64::new(1000))),
        policy: BackpackMutationPolicy {
            window: BackpackReceiveWindow::default(),
            budget: Duration::from_secs(1),
        },
        identities: BackpackClientIdStore::open(directory.path(), &scope).unwrap(),
        namespace: scope,
        authority: authority(),
    };
    local_kind(
        BackpackOrderOwner::new_checked(input).unwrap_err(),
        BackpackExecutionErrorKind::Identity,
    );
    assert!(server.captured().is_empty());
}

#[rstest]
#[case("systemOrderType",json!("Liquidation"))]
#[case("triggerPrice",json!("100"))]
#[case("quoteQuantity",json!("10"))]
#[case("strategyId",json!("foreign-strategy"))]
#[case("autoBorrow",json!(true))]
#[tokio::test]
async fn test_nonstandard_post_ack_never_binds_or_cancels(
    #[case] field: &str,
    #[case] extra: Value,
) {
    let mut body = json!({"id":"external","clientId":1,"symbol":"BTC_USDC_PERP","side":"Bid","orderType":"Limit","timeInForce":"GTC","quantity":"0.1","price":"100","postOnly":false,"reduceOnly":false,"executedQuantity":"0","status":"New"});
    body[field] = extra;
    let server = Server::start(vec![Reply::status(200, &body.to_string())]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    assert_eq!(
        owner
            .submit(spec("nonstandard"), &CancellationToken::new())
            .await
            .unwrap_err()
            .outcome(),
        BackpackRequestOutcome::Unknown
    );
    assert!(
        owner
            .confirmed_binding(ClientOrderId::from("nonstandard"))
            .is_none()
    );
    assert!(
        owner
            .cancel_owned(
                ClientOrderId::from("nonstandard"),
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert_eq!(server.captured().len(), 1);
}

#[rstest]
#[tokio::test]
async fn test_unrelated_future_ack_field_is_preserved_without_rejecting_binding() {
    let body = json!({"id":"future-ok","clientId":1,"symbol":"BTC_USDC_PERP","side":"Bid","orderType":"Limit","timeInForce":"GTC","quantity":"0.1","price":"100","postOnly":false,"reduceOnly":false,"executedQuantity":"0","status":"New","futureUnrelatedField":{"value":123}});
    let server = Server::start(vec![Reply::status(200, &body.to_string())]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let receipt = owner
        .submit(spec("future-field"), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(receipt.body()).unwrap()["futureUnrelatedField"],
        body["futureUnrelatedField"]
    );
    assert!(
        owner
            .confirmed_binding(ClientOrderId::from("future-field"))
            .is_some()
    );
}

#[rstest]
#[tokio::test]
async fn test_native_quote_container_does_not_bypass_executable_grid_checks() {
    let server = Server::start(vec![]).await;
    let directory = TempDir::new().unwrap();
    let owner = build_owner(
        &server,
        &directory,
        Arc::new(Clock(AtomicU64::new(1000))),
        BackpackQuota::default(),
        Duration::from_secs(1),
    );
    prepare(&owner, &server, 1, 1000);
    let mut facts = market(1, 1000);
    facts.quote.bid_price = Price::from_decimal(decimal("99.91")).unwrap();
    assert!(owner.guard().update_market(facts, 1000).is_err());
    assert!(
        owner
            .submit(spec("bad-native-quote"), &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(server.captured().is_empty());
}
