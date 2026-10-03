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

//! Synthetic local transport probes, not captured venue account payloads.
//!
//! The seed [7;32] is generated public test material. Successful `{}` responses
//! exercise the raw GET boundary only; no domain parser or live capability is asserted.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
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
    http::{
        client::{BackpackClock, BackpackHttpClient, BackpackHttpPolicy, BackpackSystemClock},
        error::{BackpackHttpErrorKind, BackpackRequestOutcome},
        quota::BackpackQuota,
        request::{BackpackReadOperation, BackpackReadRequest},
    },
    signing::{BackpackParameters, BackpackReceiveWindow, BackpackScalar},
};
use nautilus_network::dst::time::Instant;
use rstest::rstest;
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
struct Reply {
    status: u16,
    body: &'static str,
    headers: Vec<(&'static str, String)>,
    delay: Duration,
}
impl Reply {
    fn json(status: u16, body: &'static str) -> Self {
        Self {
            status,
            body,
            headers: vec![],
            delay: Duration::ZERO,
        }
    }
}
#[derive(Clone, Debug)]
struct Captured {
    method: String,
    uri: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}
#[derive(Debug)]
struct ServerState {
    replies: Mutex<VecDeque<Reply>>,
    captured: Mutex<Vec<Captured>>,
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
            replies: Mutex::new(replies.into()),
            captured: Mutex::new(vec![]),
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
            while self.state.captured.lock().unwrap().len() < count {
                self.state.notify.notified().await;
            }
        })
        .await
        .unwrap();
    }
    fn client(
        &self,
        quota: BackpackQuota,
        budget: Duration,
        retries: u32,
        authenticated: bool,
    ) -> BackpackHttpClient {
        let credential = authenticated.then(|| {
            BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &self.endpoints).unwrap()
        });
        BackpackHttpClient::new(
            self.endpoints.clone(),
            credential,
            quota,
            BackpackHttpPolicy::new(BackpackReceiveWindow::default(), budget, retries).unwrap(),
            Arc::new(BackpackSystemClock),
        )
        .unwrap()
    }
}
async fn handler(State(state): State<Arc<ServerState>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 8192).await.unwrap();
    state.captured.lock().unwrap().push(Captured {
        method: parts.method.to_string(),
        uri: parts.uri.to_string(),
        headers: parts
            .headers
            .iter()
            .map(|(key, value)| (key.as_str().into(), value.to_str().unwrap().into()))
            .collect(),
        body: body.to_vec(),
    });
    state.notify.notify_one();
    let reply = state
        .replies
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or(Reply::json(200, "{}"));
    tokio::time::sleep(reply.delay).await;
    let mut builder = Response::builder()
        .status(reply.status)
        .header("content-type", "application/json");
    for (key, value) in reply.headers {
        builder = builder.header(key, value);
    }
    builder.body(Body::from(reply.body)).unwrap()
}
fn balances() -> BackpackReadRequest {
    BackpackReadRequest::new(
        BackpackReadOperation::Balances,
        BackpackParameters::default(),
    )
    .unwrap()
}
fn open_order() -> BackpackReadRequest {
    let mut p = BackpackParameters::default();
    p.insert(
        "symbol",
        Some(BackpackScalar::Token("BTC_USDC_PERP".into())),
    )
    .unwrap();
    p.insert("clientId", Some(BackpackScalar::Unsigned(u32::MAX.into())))
        .unwrap();
    BackpackReadRequest::new(BackpackReadOperation::OpenOrder, p).unwrap()
}
fn verify_auth(request: &Captured, canonical_prefix: &str) {
    let timestamp = &request.headers["x-timestamp"];
    assert_eq!(request.headers["x-window"], "5000");
    let canonical = format!("{canonical_prefix}&timestamp={timestamp}&window=5000");
    let public = SigningKey::from_bytes(&[7; 32]).verifying_key();
    assert_eq!(
        request.headers["x-api-key"],
        STANDARD.encode(public.as_bytes())
    );
    let signature =
        Signature::from_slice(&STANDARD.decode(&request.headers["x-signature"]).unwrap()).unwrap();
    public
        .verify_strict(canonical.as_bytes(), &signature)
        .unwrap();
}

#[tokio::test]
async fn test_signed_get_query_empty_body_and_pagination_headers() {
    let mut reply = Reply::json(200, "{}");
    reply.headers = vec![
        ("x-total", "10".into()),
        ("x-page-count", "2".into()),
        ("x-current-page", "1".into()),
        ("x-page-size", "5".into()),
        ("x-api-key", "echo-secret".into()),
    ];
    let server = Server::start(vec![reply]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 0, true);
    let result = client
        .read(&open_order(), None, None, &CancellationToken::new())
        .await
        .unwrap();
    let request = server.captured().pop().unwrap();
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.uri,
        "/api/v1/order?clientId=4294967295&symbol=BTC_USDC_PERP"
    );
    assert!(request.body.is_empty());
    verify_auth(
        &request,
        "instruction=orderQuery&clientId=4294967295&symbol=BTC_USDC_PERP",
    );
    assert_eq!(result.headers().len(), 4);
    assert_eq!(result.headers()["x-total"], "10");
    assert_eq!(result.body(), b"{}");
    assert!(!format!("{result:?} {client:?}").contains("echo-secret"));
    assert!(!format!("{client:?}").contains(&request.headers["x-api-key"]));
}

#[tokio::test]
async fn test_public_get_never_contains_credentials() {
    let server = Server::start(vec![Reply::json(200, "[]")]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 0, true);
    let request = BackpackReadRequest::new(
        BackpackReadOperation::Markets,
        BackpackParameters::default(),
    )
    .unwrap();
    client
        .read(&request, None, None, &CancellationToken::new())
        .await
        .unwrap();
    let captured = server.captured().pop().unwrap();
    assert_eq!(captured.uri, "/api/v1/markets");
    for name in ["x-api-key", "x-signature", "x-timestamp", "x-window"] {
        assert!(!captured.headers.contains_key(name));
    }
}

#[tokio::test]
async fn test_shared_quota_longer_than_window_signs_after_admission() {
    let server = Server::start(vec![]).await;
    let quota =
        BackpackQuota::with_periods(Duration::from_millis(5100), Duration::from_secs(2)).unwrap();
    let first = server.client(quota.clone(), Duration::from_secs(8), 0, true);
    let second = server.client(quota, Duration::from_secs(8), 0, true);
    first
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap();
    second
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap();
    let captured = server.captured();
    assert_eq!(captured.len(), 2);
    let before: u64 = captured[0].headers["x-timestamp"].parse().unwrap();
    let after: u64 = captured[1].headers["x-timestamp"].parse().unwrap();
    assert!(
        after.saturating_sub(before) >= 5000,
        "signing occurred before quota wait"
    );
    verify_auth(&captured[1], "instruction=balanceQuery");
}

#[tokio::test]
async fn test_expired_deadline_and_missing_credentials_send_nothing() {
    let server = Server::start(vec![]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 2, true);
    let e = client
        .read(
            &balances(),
            Some(Instant::now()),
            None,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::NotSent);
    assert_eq!(e.kind(), BackpackHttpErrorKind::Admission);
    let public = server.client(BackpackQuota::default(), Duration::from_secs(2), 2, false);
    let e = public
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.kind(), BackpackHttpErrorKind::Credentials);
    assert!(server.captured().is_empty());
}

#[tokio::test]
async fn test_stale_admission_after_shared_quota_sends_nothing() {
    let server = Server::start(vec![]).await;
    let client = server.client(
        BackpackQuota::with_periods(Duration::from_millis(150), Duration::from_secs(2)).unwrap(),
        Duration::from_secs(2),
        0,
        true,
    );
    client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap();
    let allowed = AtomicBool::new(true);
    let checked = Notify::new();
    let admission = || {
        checked.notify_one();
        allowed.load(Ordering::Acquire)
    };
    let request = balances();
    let cancel = CancellationToken::new();
    let pending = client.read(&request, None, Some(&admission), &cancel);
    tokio::pin!(pending);
    tokio::select! { result = &mut pending => panic!("unexpected dispatch: {result:?}"),
    () = checked.notified() => {} }
    allowed.store(false, Ordering::Release);
    let e = pending.await.unwrap_err();
    assert_eq!(e.kind(), BackpackHttpErrorKind::Admission);
    assert_eq!(e.outcome(), BackpackRequestOutcome::NotSent);
    assert_eq!(server.captured().len(), 1);
}

#[tokio::test]
async fn test_read_retry_preserves_unknown_on_later_not_found() {
    let server = Server::start(vec![
        Reply::json(503, "sensitive-body"),
        Reply::json(
            404,
            r#"{"code":"ORDER_NOT_FOUND","message":"sensitive-body"}"#,
        ),
    ])
    .await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 2, true);
    let e = client
        .read(&open_order(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.status(), Some(404));
    assert_eq!(e.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(server.captured().len(), 2);
    assert!(!format!("{e:?} {e}").contains("sensitive-body"));
}

#[tokio::test]
async fn test_429_read_retry_and_total_budget() {
    let mut limited = Reply::json(429, r#"{"code":"TOO_MANY_REQUESTS"}"#);
    limited.headers = vec![("retry-after", "0".into())];
    let server = Server::start(vec![limited.clone(), Reply::json(200, "{}")]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 2, true);
    client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(server.captured().len(), 2);
    let server = Server::start(vec![limited]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_millis(30), 2, true);
    let e = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(server.captured().len(), 1);
}

#[tokio::test]
async fn test_no_retries_when_disabled_and_uncertain_success() {
    let server = Server::start(vec![Reply::json(500, "secret-body")]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 0, true);
    let e = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(server.captured().len(), 1);
    let server = Server::start(vec![Reply::json(200, "not-json-sensitive")]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 2, true);
    let e = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.kind(), BackpackHttpErrorKind::Decode);
    assert_eq!(e.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(server.captured().len(), 1);
    assert!(!format!("{e:?} {e}").contains("not-json-sensitive"));
}

#[tokio::test]
async fn test_redirect_does_not_forward_authentication() {
    let target = Server::start(vec![]).await;
    let mut redirect = Reply::json(302, "{}");
    redirect
        .headers
        .push(("location", target.endpoints.rest_url().into()));
    let server = Server::start(vec![redirect]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 2, true);
    let e = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.status(), Some(302));
    assert_eq!(server.captured().len(), 1);
    assert!(target.captured().is_empty());
}

#[tokio::test]
async fn test_cancellation_before_and_after_dispatch() {
    let server = Server::start(vec![Reply {
        delay: Duration::from_secs(5),
        ..Reply::json(200, "{}")
    }])
    .await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 2, true);
    let token = CancellationToken::new();
    token.cancel();
    let e = client
        .read(&balances(), None, None, &token)
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::NotSent);
    assert!(server.captured().is_empty());
    let token = CancellationToken::new();
    let request = balances();
    let pending = client.read(&request, None, None, &token);
    tokio::pin!(pending);
    tokio::select! { result = &mut pending => panic!("unexpected completion: {result:?}"),
    () = server.wait_requests(1) => {} }
    token.cancel();
    let e = pending.await.unwrap_err();
    assert_eq!(e.kind(), BackpackHttpErrorKind::Cancelled);
    assert_eq!(e.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(server.captured().len(), 1);
}

#[tokio::test]
async fn test_transport_timeout_is_unknown_and_budget_is_bounded() {
    let server = Server::start(vec![Reply {
        delay: Duration::from_secs(5),
        ..Reply::json(200, "{}")
    }])
    .await;
    let client = server.client(
        BackpackQuota::default(),
        Duration::from_millis(100),
        2,
        true,
    );
    let e = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(server.captured().len(), 1);
}

#[rstest]
fn test_production_credentials_refuse_local_peer() {
    let endpoints =
        BackpackEndpoints::loopback_override("http://127.0.0.1:8080", "ws://127.0.0.1:8081")
            .unwrap();
    let e = BackpackHttpClient::new(
        endpoints,
        Some(BackpackCredential::production(&STANDARD.encode([7; 32])).unwrap()),
        BackpackQuota::default(),
        BackpackHttpPolicy::default(),
        Arc::new(BackpackSystemClock),
    )
    .unwrap_err();
    assert_eq!(e.kind(), BackpackHttpErrorKind::Credentials);
}

#[derive(Debug)]
struct InvalidClock;
impl BackpackClock for InvalidClock {
    fn timestamp_ms(&self) -> Result<u64, nautilus_backpack::http::error::BackpackHttpError> {
        Ok(u64::MAX)
    }
}
#[tokio::test]
async fn test_invalid_clock_is_not_sent() {
    let server = Server::start(vec![]).await;
    let credential =
        BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &server.endpoints).unwrap();
    let client = BackpackHttpClient::new(
        server.endpoints.clone(),
        Some(credential),
        BackpackQuota::default(),
        BackpackHttpPolicy::default(),
        Arc::new(InvalidClock),
    )
    .unwrap();
    let e = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::NotSent);
    assert!(server.captured().is_empty());
}

#[tokio::test]
async fn test_caller_deadline_includes_retry_after_sleep() {
    let mut limited = Reply::json(429, "{}");
    limited.headers.push(("retry-after", "1".into()));
    let server = Server::start(vec![limited]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(10), 2, true);
    let started = Instant::now();
    let e = client
        .read(
            &balances(),
            Some(started + Duration::from_millis(100)),
            None,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(e.kind(), BackpackHttpErrorKind::Budget);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(server.captured().len(), 1);
}

#[tokio::test]
async fn test_structured_rejection_preserves_prior_unknown() {
    let rejection = Reply::json(
        400,
        r#"{"code":"INVALID_CLIENT_REQUEST","message":"sensitive"}"#,
    );
    let server = Server::start(vec![rejection.clone()]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 2, true);
    let e = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::VenueRejected);
    assert_eq!(e.venue_code(), Some("INVALID_CLIENT_REQUEST"));
    assert_eq!(server.captured().len(), 1);
    assert!(!format!("{e:?} {e}").contains("sensitive"));
    let server = Server::start(vec![Reply::json(503, "{}"), rejection]).await;
    let client = server.client(BackpackQuota::default(), Duration::from_secs(2), 2, true);
    let e = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(e.status(), Some(400));
    assert_eq!(server.captured().len(), 2);
}

#[derive(Debug)]
struct SlowClock;
impl BackpackClock for SlowClock {
    fn timestamp_ms(&self) -> Result<u64, nautilus_backpack::http::error::BackpackHttpError> {
        // A caller clock can take time. Shared transport must refuse a now-expired preparation.
        std::thread::sleep(Duration::from_millis(60));
        Ok(1_700_000_000_000)
    }
}
#[tokio::test]
async fn test_deadline_expiring_during_preparation_is_not_sent() {
    let server = Server::start(vec![]).await;
    let credential =
        BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &server.endpoints).unwrap();
    let client = BackpackHttpClient::new(
        server.endpoints.clone(),
        Some(credential),
        BackpackQuota::default(),
        BackpackHttpPolicy::new(
            BackpackReceiveWindow::default(),
            Duration::from_millis(20),
            0,
        )
        .unwrap(),
        Arc::new(SlowClock),
    )
    .unwrap();
    let e = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.outcome(), BackpackRequestOutcome::NotSent);
    assert!(server.captured().is_empty());
}

#[tokio::test]
async fn test_public_and_private_reads_share_quota_and_queued_cancellation_is_not_sent() {
    let server = Server::start(vec![]).await;
    let quota =
        BackpackQuota::with_periods(Duration::from_millis(150), Duration::from_secs(2)).unwrap();
    let client = server.client(quota.clone(), Duration::from_secs(2), 0, true);
    let public = BackpackReadRequest::new(
        BackpackReadOperation::Markets,
        BackpackParameters::default(),
    )
    .unwrap();
    client
        .read(&public, None, None, &CancellationToken::new())
        .await
        .unwrap();
    let started = Instant::now();
    client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(100));
    let captured = server.captured();
    assert!(!captured[0].headers.contains_key("x-api-key"));
    assert!(captured[1].headers.contains_key("x-api-key"));
    let queued = Notify::new();
    let admission = || {
        queued.notify_one();
        true
    };
    let request = balances();
    let cancel = CancellationToken::new();
    let pending = client.read(&request, None, Some(&admission), &cancel);
    tokio::pin!(pending);
    tokio::select! { result = &mut pending => panic!("unexpected completion: {result:?}"),
    () = queued.notified() => {} }
    cancel.cancel();
    let e = pending.await.unwrap_err();
    assert_eq!(e.kind(), BackpackHttpErrorKind::Cancelled);
    assert_eq!(e.outcome(), BackpackRequestOutcome::NotSent);
    assert_eq!(server.captured().len(), 2);
    let measured = quota.diagnostics();
    assert_eq!(measured.observed_waits, 3);
    assert_eq!(measured.admitted_waits, 2);
    assert_eq!(measured.refused_waits, 1);
    assert_eq!(measured.last_admitted, Some(false));
    assert!(measured.total_queue_wait_ns >= 100_000_000);
}

#[tokio::test]
async fn test_quota_diagnostics_measure_refused_wait_without_dispatch() {
    let server = Server::start(vec![]).await;
    let quota =
        BackpackQuota::with_periods(Duration::from_millis(500), Duration::from_secs(2)).unwrap();
    let first = server.client(quota.clone(), Duration::from_secs(2), 0, true);
    first
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap();
    let queued = server.client(quota.clone(), Duration::from_millis(100), 0, true);
    let started = Instant::now();
    let error = queued
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.outcome(), BackpackRequestOutcome::NotSent);
    assert_eq!(server.captured().len(), 1);
    let measured = quota.diagnostics();
    assert_eq!(measured.schema_version, 1);
    assert_eq!(measured.observed_waits, 2);
    assert_eq!(measured.admitted_waits, 1);
    assert_eq!(measured.refused_waits, 1);
    assert_eq!(measured.last_admitted, Some(false));
    assert!(measured.last_queue_wait_ns >= 80_000_000);
    assert!(Duration::from_nanos(measured.last_queue_wait_ns) <= started.elapsed());
}

#[tokio::test]
async fn test_quota_diagnostics_exclude_slow_signing_and_transport_unknown() {
    let server = Server::start(vec![Reply {
        delay: Duration::from_secs(2),
        ..Reply::json(200, "{}")
    }])
    .await;
    let quota = BackpackQuota::default();
    let client = BackpackHttpClient::new(
        server.endpoints.clone(),
        Some(
            BackpackCredential::loopback_peer(&STANDARD.encode([7; 32]), &server.endpoints)
                .unwrap(),
        ),
        quota.clone(),
        BackpackHttpPolicy::new(
            BackpackReceiveWindow::default(),
            Duration::from_millis(250),
            0,
        )
        .unwrap(),
        Arc::new(SlowClock),
    )
    .unwrap();
    let started = Instant::now();
    let error = client
        .read(&balances(), None, None, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.outcome(), BackpackRequestOutcome::Unknown);
    assert_eq!(server.captured().len(), 1);
    let measured = quota.diagnostics();
    assert_eq!(measured.observed_waits, 1);
    assert_eq!(measured.admitted_waits, 1);
    assert_eq!(measured.refused_waits, 0);
    assert_eq!(measured.last_admitted, Some(true));
    assert!(
        started
            .elapsed()
            .saturating_sub(Duration::from_nanos(measured.last_queue_wait_ns))
            >= Duration::from_millis(200)
    );
}
