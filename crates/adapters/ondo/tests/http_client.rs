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

//! Offline transport tests for the Ondo Perps REST client, its shared rate budget and its
//! request-target serialization.
//!
//! Nothing here reaches the venue. Every response is either an inline body or the synthetic
//! `test_data/rest/markets_synthetic.json` fixture, and every request goes to a scripted HTTP/1.1
//! server bound to `127.0.0.1:0` inside the test process. The clients are created with a shortened
//! retry policy so a bounded backoff is exercised in milliseconds instead of the production
//! seconds, and the only tests that take a whole real second are the two that must observe a real
//! request timeout and the one-second `Retry-After` wait the venue actually asked for.
//!
//! This file is separate from `http_contract.rs` because that file is the synchronous schema
//! contract for `GET /v1/markets` parsing, while everything here needs a tokio runtime and a
//! transport.

use std::{
    collections::VecDeque,
    net::SocketAddr,
    num::NonZeroU32,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use nautilus_core::string::secret::REDACTED;
use nautilus_network::{http::StatusCode, ratelimiter::quota::Quota, retry::RetryConfig};
use nautilus_ondo::{
    common::{
        credential::{OndoCredential, OndoEnvironmentError},
        endpoint::OndoEndpoint,
        enums::{OndoAuthenticationScope, OndoEnvironment},
    },
    http::{
        client::{
            NewRiskPermit, OndoCancelAnswer, OndoHttpClient, OndoNewRiskGuard, OndoNewRiskSendError,
        },
        error::{OndoAuthFailure, OndoHttpError},
        orders::{OndoOrderStatus, market_cancel_target},
        private::{ACCOUNT_PATH, OndoPrivateReadQuery},
        query::{
            CONTRACTS_PATH, CursorWalk, MARKETS_PATH, ORDERS_PATH, OndoRequestTarget, STATUS_PATH,
            client_order_lookup,
        },
        rate_limit::{OndoRateBudget, OndoRequestPriority},
    },
    signing::{ONDO_KEY_ID_HEADER, ONDO_SIGN_HEADER, ONDO_TIMESTAMP_HEADER, sign_rest},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

/// The synthetic `GET /v1/markets` body: the same fixture the contract tests parse.
const MARKETS_BODY: &str = include_str!("../test_data/rest/markets_synthetic.json");

/// A `GET /status` body in the `GenericResponse` envelope. The status schema is UNVERIFIED (the
/// live host answered HTTP 403 during the protocol freeze), so this only pins that the envelope's
/// `result` is unwrapped when a body carries one.
const STATUS_BODY: &str =
    r#"{"success":true,"result":{"status":"ok","time":"2026-09-14T11:50:18Z"}}"#;

/// A `GET /v1/perps/contracts` body in the `GenericResponse` envelope. The contract schema is
/// documented but has never been sampled, so only the documented field names and the exact decimal
/// lexemes are asserted.
const CONTRACTS_BODY: &str = r#"{"success":true,"result":[{"market":"NVDA-USD.P","productType":"perps","contractType":"linear","baseCurrency":"NVDA","quoteCurrency":"USD","disabled":false,"isClosed":false,"makerFee":"0.0001","takerFee":"0.00025","fundingRate":"0.0000063","lastPrice":"212.22","bid":"212.20","ask":"212.25","indexPrice":"212.21"}]}"#;

// ------------------------------------------------------------------------------------------------
// Scripted mock HTTP server
// ------------------------------------------------------------------------------------------------

/// One request as the mock server saw it.
#[derive(Debug, Clone)]
struct CapturedRequest {
    method: String,
    target: String,
    body: String,
    /// The request head exactly as it arrived, so a test can assert which headers were *not* sent.
    head: String,
    at: Instant,
}

/// One scripted reply. The last reply of a script is sticky, so a script of one reply answers every
/// connection the same way.
#[derive(Debug, Clone)]
enum Reply {
    /// Answer with this status, headers and body.
    Answer {
        status: u16,
        headers: Vec<(&'static str, String)>,
        body: String,
    },
    /// Wait, then answer `200 OK` with this body: drives the client-side request timeout.
    Slow { delay: Duration, body: String },
}

impl Reply {
    fn answer(status: u16, body: &str) -> Self {
        Self::Answer {
            status,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    fn ok(body: &str) -> Self {
        Self::answer(200, body)
    }

    fn with_header(status: u16, name: &'static str, value: &str, body: &str) -> Self {
        Self::Answer {
            status,
            headers: vec![(name, value.to_string())],
            body: body.to_string(),
        }
    }
}

/// A scripted HTTP/1.1 server bound inside the test process.
#[derive(Debug)]
struct MockServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    handle: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl MockServer {
    async fn start(script: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the mock server");
        let addr = listener.local_addr().expect("read the mock server address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let script = Arc::new(Mutex::new(VecDeque::from(script)));

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let script = Arc::clone(&script);
                let seen = Arc::clone(&seen);

                tokio::spawn(async move {
                    serve_connection(stream, script, seen).await;
                });
            }
        });

        Self {
            addr,
            requests,
            handle,
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn captured(&self) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .expect("mock server request log")
            .clone()
    }

    fn targets(&self) -> Vec<String> {
        self.captured()
            .into_iter()
            .map(|request| request.target)
            .collect()
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    script: Arc<Mutex<VecDeque<Reply>>>,
    seen: Arc<Mutex<Vec<CapturedRequest>>>,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];

    loop {
        let read = match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        buffer.extend_from_slice(&chunk[..read]);

        if let Some(request) = parse_request(&buffer) {
            seen.lock().expect("mock server request log").push(request);
            break;
        }
    }

    let reply = {
        let mut script = script.lock().expect("mock server script");
        if script.len() > 1 {
            script.pop_front()
        } else {
            script.front().cloned()
        }
    };

    match reply {
        Some(Reply::Answer {
            status,
            headers,
            body,
        }) => write_response(&mut stream, status, &headers, &body).await,
        Some(Reply::Slow { delay, body }) => {
            tokio::time::sleep(delay).await;
            write_response(&mut stream, 200, &[], &body).await;
        }
        None => drop(stream),
    }
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    headers: &[(&'static str, String)],
    body: &str,
) {
    let reason = StatusCode::from_u16(status)
        .map(|code| code.canonical_reason().unwrap_or("").to_string())
        .unwrap_or_default();
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len(),
    );

    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");

    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(body.as_bytes()).await;
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}

/// Parses a complete HTTP request out of `buffer`, if it holds one.
fn parse_request(buffer: &[u8]) -> Option<CapturedRequest> {
    let head_end = buffer.windows(4).position(|window| window == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);

    let body_start = head_end + 4;
    if buffer.len() < body_start + content_length {
        return None;
    }

    Some(CapturedRequest {
        method,
        target,
        head,
        body: String::from_utf8_lossy(&buffer[body_start..body_start + content_length]).to_string(),
        at: Instant::now(),
    })
}

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

/// A retry policy with test-sized delays. `operation_timeout_ms` is disabled so the only timeout
/// in play is the HTTP client's own.
fn retry_policy(
    max_retries: u32,
    initial_delay_ms: u64,
    max_delay_ms: u64,
    max_elapsed_ms: Option<u64>,
) -> RetryConfig {
    RetryConfig {
        max_retries,
        initial_delay_ms,
        max_delay_ms,
        backoff_factor: 2.0,
        jitter_ms: 0,
        operation_timeout_ms: None,
        immediate_first: false,
        max_elapsed_ms,
    }
}

/// The budget a transport test client is given.
///
/// These tests are about the transport, so their budget must not be the thing a failure can be
/// blamed on, and it must not pace a multi-request test for real seconds. The production
/// one-request-per-second default is asserted in `nautilus_ondo::http::rate_limit`'s own tests.
fn test_budget() -> OndoRateBudget {
    OndoRateBudget::with_quota(Quota::per_second(NonZeroU32::new(1_000).unwrap()).unwrap())
}

fn client_for(server: &MockServer, retry: RetryConfig) -> OndoHttpClient {
    OndoHttpClient::builder()
        .base_url(server.url())
        .budget(test_budget())
        .retry_config(retry)
        .build()
        .expect("the mock HTTP client builds")
}

fn test_client(server: &MockServer) -> OndoHttpClient {
    client_for(server, retry_policy(2, 1, 4, Some(5_000)))
}

// ------------------------------------------------------------------------------------------------
// 429, Retry-After, and the bounded backoff
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_a_429_without_retry_after_backs_off_exponentially_and_stays_bounded() {
    // Every reply is a 429 that carries no `Retry-After`, so the retry delay can only come from
    // the policy's own exponential backoff. The bound is the policy: one attempt plus
    // `max_retries`, and the observed gaps must grow.
    let server = MockServer::start(vec![Reply::answer(429, "too many requests")]).await;
    let client = client_for(&server, retry_policy(2, 50, 200, Some(5_000)));

    let started = Instant::now();
    let error = client
        .get_markets()
        .await
        .expect_err("a 429 without a body of market data is an error");
    let elapsed = started.elapsed();

    assert!(
        matches!(
            error,
            OndoHttpError::RateLimited {
                retry_after_secs: None,
                ..
            }
        ),
        "expected a rate-limit refusal without a Retry-After, was {error:?}",
    );

    let requests = server.captured();
    assert_eq!(
        requests.len(),
        3,
        "one attempt plus the policy's two bounded retries",
    );
    assert!(elapsed < Duration::from_secs(1), "bounded: {elapsed:?}");

    // The policy's 50 ms initial delay doubling to 100 ms is the spacing the two observed attempts
    // must show: the lower bounds are what makes this an exponential backoff and not merely "some
    // delay" (a scheduler under load can only lengthen a gap, never shorten it below the timer).
    let first_gap = requests[1].at.duration_since(requests[0].at);
    let second_gap = requests[2].at.duration_since(requests[1].at);
    assert!(
        first_gap >= Duration::from_millis(45),
        "the first retry waits the policy's initial delay: {first_gap:?}",
    );
    assert!(
        second_gap >= Duration::from_millis(90),
        "the second retry waits twice the initial delay: {second_gap:?}",
    );
    assert!(
        second_gap > first_gap,
        "the delay between attempts must grow (exponential backoff): {first_gap:?} then {second_gap:?}",
    );
}

#[tokio::test]
async fn test_a_429_retry_after_wait_is_the_header_value_not_the_policy_backoff() {
    // Both legs run on the *same* policy, pinned at a 1 ms initial delay and a 2 ms cap, so the
    // policy's own backoff can never produce a gap of more than a few milliseconds. The only thing
    // that differs between the legs is the `Retry-After` the venue sent, so any difference in the
    // observed request spacing can only have come from the header - not from a fixed constant.
    let policy = || retry_policy(2, 1, 2, Some(30_000));

    let no_wait = MockServer::start(vec![
        Reply::with_header(429, "Retry-After", "0", "slow down"),
        Reply::ok(MARKETS_BODY),
    ])
    .await;
    let no_wait_client = client_for(&no_wait, policy());
    let response = no_wait_client
        .get_markets()
        .await
        .expect("the zero-second retry succeeds");
    assert_eq!(response.trading_pairs().len(), 4);

    let requests = no_wait.captured();
    assert_eq!(requests.len(), 2, "the 429 and its retry are both seen");
    let zero_gap = requests[1].at.duration_since(requests[0].at);

    let one_second = MockServer::start(vec![
        Reply::with_header(429, "Retry-After", "1", "slow down"),
        Reply::ok(MARKETS_BODY),
    ])
    .await;
    let one_second_client = client_for(&one_second, policy());
    let response = one_second_client
        .get_markets()
        .await
        .expect("the honoured retry succeeds");
    assert_eq!(response.trading_pairs().len(), 4);

    let requests = one_second.captured();
    assert_eq!(requests.len(), 2, "the 429 and its retry are both seen");
    let one_gap = requests[1].at.duration_since(requests[0].at);

    // `Retry-After: 0` asks for no wait, and only the pinned backoff is left: milliseconds.
    assert!(
        zero_gap < Duration::from_millis(500),
        "a zero-second Retry-After costs only the policy's own backoff: {zero_gap:?}",
    );
    // `Retry-After: 1` asks for a second, and a second is the wait the client takes.
    assert!(
        one_gap >= Duration::from_millis(900),
        "the header's second must be waited out, was {one_gap:?}",
    );
    // ... and it is that second, not a longer wait of the client's own. The upper bound is loose on
    // purpose: a scheduler delay can only stretch a real sleep, and the property this leg proves is
    // that the header is honoured - which the two lower bounds above and the contrast with the
    // zero-second leg already detect, on a machine that is arbitrarily loaded. A wait of the policy's
    // own making (1-2 ms) or of a wrong constant (tens of seconds) still fails here.
    assert!(
        one_gap < Duration::from_secs(5),
        "the wait is the header's second, not a longer constant: {one_gap:?}",
    );
    assert!(
        one_gap >= zero_gap + Duration::from_millis(900),
        "the header is the only difference between the legs: {zero_gap:?} then {one_gap:?}",
    );
}

#[tokio::test]
async fn test_a_429_retry_after_that_cannot_fit_the_retry_budget_is_returned_not_waited_out() {
    // The venue asks for a one-second wait. The retry budget is 200 ms, so honouring the header
    // means refusing to retry inside the budget and surfacing the 429 immediately - the proof that
    // the header was read is that the client returns in well under the wait it was asked for.
    let server = MockServer::start(vec![Reply::with_header(
        429,
        "Retry-After",
        "1",
        "slow down",
    )])
    .await;
    let client = client_for(&server, retry_policy(3, 1, 2, Some(200)));

    let started = Instant::now();
    let error = client
        .get_markets()
        .await
        .expect_err("a 429 is a rate-limit refusal");
    let elapsed = started.elapsed();

    assert!(
        matches!(
            error,
            OndoHttpError::RateLimited {
                retry_after_secs: Some(1),
                ..
            }
        ),
        "expected the parsed Retry-After on the error, was {error:?}",
    );
    assert_eq!(
        server.captured().len(),
        1,
        "the venue's wait did not fit the retry budget, so no early retry was sent",
    );
    assert!(
        elapsed < Duration::from_millis(600),
        "the client must not sleep out a wait it cannot afford: {elapsed:?}",
    );
}

#[tokio::test]
async fn test_a_429_with_a_zero_second_retry_after_is_retried_and_can_succeed() {
    // A zero-second `Retry-After` is the header's honoured form that needs no real wait: the
    // retry happens and the second reply succeeds.
    let server = MockServer::start(vec![
        Reply::with_header(429, "Retry-After", "0", "slow down"),
        Reply::ok(MARKETS_BODY),
    ])
    .await;
    let client = test_client(&server);

    let response = client
        .get_markets()
        .await
        .expect("the retry succeeds on the second attempt");

    assert_eq!(response.trading_pairs().len(), 4);
    assert_eq!(server.captured().len(), 2);
}

// ------------------------------------------------------------------------------------------------
// 401 / 403 are terminal, and the error carries the body's code
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_a_403_is_terminal_and_carries_the_status_and_the_observed_error_code() {
    // The observed live body: Cloudflare's plain-text `error code: 1010` behind HTTP 403. A 200 is
    // scripted behind it, so a retry would succeed - the assertion is that none is attempted.
    let server = MockServer::start(vec![
        Reply::answer(403, "error code: 1010"),
        Reply::ok(MARKETS_BODY),
    ])
    .await;
    let client = client_for(&server, retry_policy(3, 1, 2, Some(5_000)));

    let error = client.get_markets().await.expect_err("403 is terminal");

    assert!(
        matches!(
            &error,
            OndoHttpError::RequestRejected {
                status: 403,
                code: Some(code),
                ..
            } if code == "1010"
        ),
        "expected a terminal rejection carrying the body's error code, was {error:?}",
    );
    assert!(error.to_string().contains("1010"));
    assert_eq!(
        server.captured().len(),
        1,
        "a 403 is never retried in a loop",
    );
}

#[tokio::test]
async fn test_a_401_is_terminal_and_carries_a_json_error_code() {
    let body = r#"{"success":false,"errorCode":"UNAUTHORIZED","message":"login required"}"#;
    let server = MockServer::start(vec![Reply::answer(401, body), Reply::ok(MARKETS_BODY)]).await;
    let client = client_for(&server, retry_policy(3, 1, 2, Some(5_000)));

    let error = client.get_markets().await.expect_err("401 is terminal");

    assert!(
        matches!(
            &error,
            OndoHttpError::RequestRejected {
                status: 401,
                code: Some(code),
                ..
            } if code == "UNAUTHORIZED"
        ),
        "expected a terminal rejection carrying the JSON error code, was {error:?}",
    );
    assert_eq!(server.captured().len(), 1);
}

#[tokio::test]
async fn test_a_read_that_fails_with_a_server_error_is_retried_within_the_bound() {
    let server = MockServer::start(vec![Reply::answer(503, "busy"), Reply::ok(MARKETS_BODY)]).await;
    let client = test_client(&server);

    let response = client
        .get_markets()
        .await
        .expect("the second reply succeeds");

    assert_eq!(response.trading_pairs().len(), 4);
    assert_eq!(server.captured().len(), 2);
}

#[tokio::test]
async fn test_a_body_that_is_not_the_schema_is_a_decode_error_and_is_not_retried() {
    let server = MockServer::start(vec![Reply::ok("not json"), Reply::ok(MARKETS_BODY)]).await;
    let client = client_for(&server, retry_policy(3, 1, 2, Some(5_000)));

    let error = client
        .get_markets()
        .await
        .expect_err("the body is not the schema");

    assert!(matches!(error, OndoHttpError::Decode(_)), "was {error:?}");
    assert_eq!(
        server.captured().len(),
        1,
        "a deterministic decode failure must not be retried",
    );
}

// ------------------------------------------------------------------------------------------------
// A POST is never replayed
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_a_post_body_is_sent_once_and_never_replayed() {
    // A 503 is retryable for a read; the POST path must still send exactly one request, and the
    // captured body must be the exact bytes handed to the client.
    let server = MockServer::start(vec![Reply::answer(503, "busy"), Reply::ok("{}")]).await;
    let client = client_for(&server, retry_policy(3, 1, 2, Some(5_000)));
    let request = r#"{"market":"NVDA-USD.P","side":"buy","type":"limit","size":"0.01","price":"100.00","clientOrderId":"ondo_probe_example_1"}"#;
    let target = OndoRequestTarget::new(ORDERS_PATH);

    let error = client
        .post_raw(&target, request.as_bytes().to_vec())
        .await
        .expect_err("a 503 is an error on the single-shot POST path");

    assert!(
        matches!(error, OndoHttpError::Http { status: 503, .. }),
        "was {error:?}",
    );

    let captured = server.captured();
    assert_eq!(
        captured.len(),
        1,
        "a POST must never be replayed automatically: the result would be unknown",
    );
    assert_eq!(captured[0].method, "POST");
    assert_eq!(captured[0].target, ORDERS_PATH);
    assert_eq!(captured[0].body, request);
}

// ------------------------------------------------------------------------------------------------
// A timed-out read is retried
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_a_read_that_times_out_is_retried_within_the_bound() {
    // The first reply is slower than the client's one-second timeout; the retry is answered
    // immediately. This is the only test that must observe a real timeout.
    let server = MockServer::start(vec![
        Reply::Slow {
            delay: Duration::from_millis(1_200),
            body: MARKETS_BODY.to_string(),
        },
        Reply::ok(MARKETS_BODY),
    ])
    .await;
    let client = OndoHttpClient::builder()
        .base_url(server.url())
        .timeout_secs(1)
        .budget(test_budget())
        .retry_config(retry_policy(1, 1, 2, Some(5_000)))
        .build()
        .expect("the mock HTTP client builds");

    let started = Instant::now();
    let response = client
        .get_markets()
        .await
        .expect("the retry is answered before the timeout");
    let elapsed = started.elapsed();

    assert_eq!(response.trading_pairs().len(), 4);
    assert_eq!(server.captured().len(), 2);
    assert!(
        elapsed >= Duration::from_secs(1),
        "the first attempt must have run into the one-second timeout: {elapsed:?}",
    );
}

// ------------------------------------------------------------------------------------------------
// The documented endpoints and the decimals on the wire
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_the_documented_endpoints_use_their_exact_paths_and_keep_the_wire_decimals() {
    let server = MockServer::start(vec![
        Reply::ok(STATUS_BODY),
        Reply::ok(MARKETS_BODY),
        Reply::ok(CONTRACTS_BODY),
    ])
    .await;
    let client = test_client(&server);

    let status = client.get_status().await.expect("the status body decodes");
    assert_eq!(status["status"], "ok");
    assert_eq!(status["time"], "2026-09-14T11:50:18Z");

    let markets = client
        .get_markets()
        .await
        .expect("the markets body decodes");
    assert_eq!(markets.trading_pairs().len(), 4);
    // The wire lexeme, not a re-formatted number.
    assert_eq!(
        markets.trading_pairs()[0].base_increment.as_deref(),
        Some("0.01"),
    );
    assert_eq!(
        markets.trading_pairs()[0].quote_increment.as_deref(),
        Some("0.01"),
    );
    assert_eq!(markets.token_config()[0].id, "USDC");

    let contracts = client
        .get_contracts()
        .await
        .expect("the contracts body decodes");
    assert_eq!(contracts.len(), 1);
    // Each contract is the exact JSON text the venue sent, so a decimal lexeme is never routed
    // through an `f64`.
    let contract = contracts[0].get();
    assert!(contract.contains(r#""makerFee":"0.0001""#), "{contract}");
    assert!(contract.contains(r#""takerFee":"0.00025""#), "{contract}");
    assert!(contract.contains(r#""lastPrice":"212.22""#), "{contract}");

    assert_eq!(
        server.targets(),
        vec![
            STATUS_PATH.to_string(),
            MARKETS_PATH.to_string(),
            CONTRACTS_PATH.to_string(),
        ],
    );
}

#[tokio::test]
async fn test_an_empty_contract_list_fails_closed_instead_of_reporting_no_contracts() {
    let server = MockServer::start(vec![Reply::ok(r#"{"success":true,"result":[]}"#)]).await;
    let client = test_client(&server);

    let error = client
        .get_contracts()
        .await
        .expect_err("no contracts is a failure");

    assert!(matches!(error, OndoHttpError::EmptyResult), "was {error:?}");
}

#[tokio::test]
async fn test_the_status_endpoint_fails_closed_when_its_envelope_reports_failure() {
    let server = MockServer::start(vec![Reply::ok(r#"{"success":false,"result":null}"#)]).await;
    let client = test_client(&server);

    let error = client.get_status().await.expect_err("the envelope failed");

    assert!(
        matches!(error, OndoHttpError::Unsuccessful),
        "was {error:?}",
    );
}

// ------------------------------------------------------------------------------------------------
// The public client puts no credential on the wire
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_the_public_client_puts_no_credential_on_the_wire() {
    // `OndoHttpClient` takes no credentials: its builder has no key member, the HTTP layer reads no
    // `.env` file and no environment variable at all, and the request the transport sends carries no
    // auth header and no auth query parameter. A key could only reach the venue as a header or a
    // parameter, and neither is present.
    let server = MockServer::start(vec![
        Reply::ok(STATUS_BODY),
        Reply::ok(MARKETS_BODY),
        Reply::ok(CONTRACTS_BODY),
    ])
    .await;
    let client = test_client(&server);

    client.get_status().await.expect("the status body decodes");
    client
        .get_markets()
        .await
        .expect("the markets body decodes");
    client
        .get_contracts()
        .await
        .expect("the contracts body decodes");

    let requests = server.captured();
    assert_eq!(requests.len(), 3);

    // The real credential surface is `ONDO-KEY-ID`/`ONDO-TIMESTAMP`/`ONDO-SIGN` (Task 7); none of
    // it, and no generic auth header, is sent by this public client.
    const CREDENTIAL_MARKERS: [&str; 8] = [
        "authorization",
        "cookie",
        "api-key",
        "apikey",
        "key-id",
        "secret",
        "sign",
        "token",
    ];

    for request in &requests {
        let head = request.head.to_ascii_lowercase();
        let target = request.target.to_ascii_lowercase();

        for marker in CREDENTIAL_MARKERS {
            assert!(
                !head.contains(marker),
                "a `{marker}` header reached the wire: {}",
                request.head,
            );
            assert!(
                !target.contains(marker),
                "a `{marker}` parameter reached the request target: {}",
                request.target,
            );
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Sharing one budget between two clients
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_two_clients_built_on_one_budget_share_the_same_limiter_instance() {
    let server = MockServer::start(vec![Reply::ok(MARKETS_BODY)]).await;
    let budget = OndoRateBudget::new();

    let data_client = OndoHttpClient::builder()
        .base_url(server.url())
        .budget(budget.clone())
        .build()
        .expect("the data client builds");
    let exec_client = OndoHttpClient::builder()
        .base_url(server.url())
        .budget(budget.clone())
        .build()
        .expect("the execution client builds");
    let other = client_for(&server, retry_policy(2, 1, 4, Some(5_000)));

    assert!(
        Arc::ptr_eq(
            data_client.budget().limiter(),
            exec_client.budget().limiter()
        ),
        "one injected budget is one budget, not one per client",
    );
    assert!(
        !Arc::ptr_eq(data_client.budget().limiter(), other.budget().limiter()),
        "a client that was not given the budget keeps its own",
    );
}

// ------------------------------------------------------------------------------------------------
// Request-target byte strings
// ------------------------------------------------------------------------------------------------

#[test]
fn test_the_request_target_is_the_exact_path_and_query_a_signature_will_cover() {
    assert_eq!(OndoRequestTarget::new(STATUS_PATH).as_str(), "/status");
    assert_eq!(OndoRequestTarget::new(MARKETS_PATH).as_str(), "/v1/markets");
    assert_eq!(
        OndoRequestTarget::new(CONTRACTS_PATH).as_str(),
        "/v1/perps/contracts",
    );

    // The §6.2 lookup forms: the `client:{clientOrderId}` value and a market whose symbol carries a
    // dot. The unreserved set is left alone; the colon is encoded.
    let target = OndoRequestTarget::new(ORDERS_PATH)
        .with_query_param("clientOrderId", &client_order_lookup("abc-123"))
        .with_query_param("market", "NVDA-USD.P");

    assert_eq!(
        target.as_str(),
        "/v1/perps/orders?clientOrderId=client%3Aabc-123&market=NVDA-USD.P",
    );
    assert_eq!(client_order_lookup("abc-123"), "client:abc-123");
}

#[test]
fn test_the_query_encoder_preserves_unreserved_bytes_and_encodes_the_rest() {
    let target = OndoRequestTarget::new(ORDERS_PATH)
        .with_query_param("keep", "AZaz09-._~")
        .with_query_param("space", "a b")
        .with_query_param("reserved", "+&=%#?/")
        .with_query_param("utf8", "é");

    assert_eq!(
        target.as_str(),
        "/v1/perps/orders?keep=AZaz09-._~&space=a%20b&reserved=%2B%26%3D%25%23%3F%2F&utf8=%C3%A9",
    );
}

#[test]
fn test_the_query_parameters_keep_their_insertion_order() {
    let target = OndoRequestTarget::new(ORDERS_PATH)
        .with_query_param("market", "TSLA-USD.P")
        .with_query_param("market", "NVDA-USD.P");

    // Serialized once, in the caller's order: the signature and the wire bytes are the same string.
    assert_eq!(
        target.as_str(),
        "/v1/perps/orders?market=TSLA-USD.P&market=NVDA-USD.P",
    );
}

// ------------------------------------------------------------------------------------------------
// Cursor pagination cannot loop forever
// ------------------------------------------------------------------------------------------------

#[test]
fn test_a_cursor_walk_stops_on_a_repeated_cursor_and_at_the_page_cap() {
    let mut walk = CursorWalk::new(4);
    assert_eq!(walk.advance(Some("c1")).unwrap(), Some("c1".to_string()));
    assert_eq!(walk.advance(Some("c2")).unwrap(), Some("c2".to_string()));
    assert_eq!(walk.pages(), 2);

    let repeated = walk.advance(Some("c1")).unwrap_err();
    assert!(
        matches!(repeated, OndoHttpError::Pagination { pages: 2, .. }),
        "a repeated cursor must stop the walk, was {repeated:?}",
    );

    // An endpoint that never stops handing out fresh cursors is bounded by the page cap.
    let mut endless = CursorWalk::new(2);
    assert_eq!(endless.advance(Some("a")).unwrap(), Some("a".to_string()));
    assert_eq!(endless.advance(Some("b")).unwrap(), Some("b".to_string()));

    let capped = endless.advance(Some("c")).unwrap_err();
    assert!(
        matches!(capped, OndoHttpError::Pagination { pages: 2, .. }),
        "the page cap must stop the walk, was {capped:?}",
    );
}

#[test]
fn test_a_cursor_walk_ends_when_the_endpoint_reports_no_next_cursor() {
    let mut walk = CursorWalk::new(4);

    assert_eq!(walk.advance(None).unwrap(), None);
    assert_eq!(walk.pages(), 0);

    // A zero page cap is a walk that never continues, not an unbounded one.
    let mut none_allowed = CursorWalk::new(0);
    assert!(
        matches!(
            none_allowed.advance(Some("a")),
            Err(OndoHttpError::Pagination { pages: 0, .. })
        ),
        "a zero page cap stops before the first page",
    );
}

// ------------------------------------------------------------------------------------------------
// The authenticated surface
//
// Fake key material only: nothing here is, or ever was, a venue credential, and no request leaves
// the test process. What is asserted is the *contract* - which headers go on the wire, that the
// signature covers the bytes that were sent, that a credential never reaches a log or a URL, and
// that a refused clock or environment stops the request before the socket.
// ------------------------------------------------------------------------------------------------

const FAKE_KEY_ID: &str = "ondoKeyId_UNIT_TEST_ONLY";
const FAKE_API_SECRET: &str = "ondoApiSecret_UNIT_TEST_ONLY";

/// A `GET /v1/account` body in the `GenericResponse` envelope. The account schema is documented but
/// unverified (no authenticated response has ever been observed), so only the envelope, the cursor
/// and the exact decimal lexemes are asserted.
const ACCOUNT_BODY: &str =
    r#"{"success":true,"result":{"walletBalance":"1000.000000","availableMargin":"1000.000000"}}"#;

/// A `POST /v1/perps/orders` body in the `GenericResponse` envelope. The order schema is UNVERIFIED,
/// so only the envelope shape is asserted; this reply is never reached, because a POST is not
/// replayed.
const ORDERS_BODY: &str = r#"{"success":true,"result":{"orderId":"order-1"}}"#;

/// The frozen REST spec's own 200 for `DELETE /v1/perps/orders` (cancel all orders), verbatim: a
/// bare `GenericResponse`, whose `required` list is `["success"]` and which has no `result` member.
const BARE_SUCCESS_BODY: &str = r#"{"success":true}"#;

/// A `DELETE /v1/perps/orders/{orderID}` body: the documented `ApiOrder` payload, post-cancel.
const CANCELLED_ORDER_BODY: &str = r#"{"success":true,"result":{"orderId":"order-1","clientOrderId":"ondo_probe_example_1","side":"buy","market":"NVDA-USD.P","size":"10.00","price":"227.50","filledSize":"5.00","status":"canceled","createdAt":"2025-03-05T14:30:00Z","type":"limit","timeInForce":"GTC"}}"#;

fn fake_credential() -> OndoCredential {
    OndoCredential::new(
        OndoEnvironment::Sandbox,
        FAKE_KEY_ID.to_string(),
        FAKE_API_SECRET.to_string(),
    )
    .expect("the fake unit-test credential builds")
}

/// A production credential, for the read-only dispatch tests. It authenticates against the
/// production environment, and the loopback mock is an admitted test service for it too.
fn fake_production_credential() -> OndoCredential {
    OndoCredential::new(
        OndoEnvironment::Production,
        FAKE_KEY_ID.to_string(),
        FAKE_API_SECRET.to_string(),
    )
    .expect("the fake unit-test credential builds")
}

/// The authenticated mock client, with a new-risk guard that admits every write.
///
/// A signed `POST` is an order creation, so the transport consults a guard before it sends one, and
/// an authenticated client without a guard refuses rather than sends. These tests are about the
/// transport, so theirs is one that always admits; the guard's own behaviour is tested where it
/// belongs, in the section at the end of this file.
fn signed_client_for(server: &MockServer, retry: RetryConfig) -> OndoHttpClient {
    signed_client_with_guard(server, retry, Arc::new(TestGuard::admitting()))
}

/// [`signed_client_for`] with the guard the caller keeps, so a test can drive it.
fn signed_client_with_guard(
    server: &MockServer,
    retry: RetryConfig,
    guard: Arc<TestGuard>,
) -> OndoHttpClient {
    OndoHttpClient::builder()
        .base_url(server.url())
        .budget(test_budget())
        .retry_config(retry)
        .credential(Arc::new(fake_credential()))
        .new_risk_guard(guard as Arc<dyn OndoNewRiskGuard>)
        .build()
        .expect("the authenticated mock client builds")
}

/// Returns one header of a captured request head, case-insensitively.
fn request_header(request: &CapturedRequest, name: &str) -> Option<String> {
    request
        .head
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _value)| key.eq_ignore_ascii_case(name))
        .map(|(_key, value)| value.trim().to_string())
}

/// Signs a captured request the way the venue will: over the method, the target and the body the
/// server actually received, keyed with the fake secret. This is what makes "the bytes signed are
/// the bytes sent" an assertion rather than a claim.
fn resign_captured(request: &CapturedRequest) -> String {
    let timestamp_ms: u64 = request_header(request, ONDO_TIMESTAMP_HEADER)
        .expect("the timestamp header is present")
        .parse()
        .expect("the timestamp header is a millisecond count");

    sign_rest(
        &fake_credential(),
        timestamp_ms,
        &request.method,
        &request.target,
        request.body.as_bytes(),
    )
}

#[tokio::test]
async fn test_a_signed_read_sends_the_three_headers_and_signs_the_bytes_it_sends() {
    let server = MockServer::start(vec![Reply::ok(ACCOUNT_BODY)]).await;
    let client = signed_client_for(&server, retry_policy(2, 1, 4, Some(5_000)));

    assert!(client.is_authenticated());

    let response = client
        .get_account()
        .await
        .expect("the account body decodes");

    assert_eq!(response.http_status(), 200);
    assert_eq!(response.success(), Some(true));
    // The documented balance members survive as the venue's own decimal strings.
    assert!(
        response
            .raw_result()
            .contains(r#""walletBalance":"1000.000000""#),
        "{}",
        response.raw_result(),
    );

    let requests = server.captured();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];

    assert_eq!(request.method, "GET");
    assert_eq!(request.target, ACCOUNT_PATH);
    assert_eq!(
        request_header(request, ONDO_KEY_ID_HEADER).as_deref(),
        Some(FAKE_KEY_ID),
        "the key id travels with its `ondoKeyId_` prefix intact",
    );
    // Conflict 1: the API-key page's three headers, and the REST spec's single alternative never.
    assert!(
        !request.head.to_ascii_lowercase().contains("x-api-key-id"),
        "the REST spec's alternative header must never be emitted: {}",
        request.head,
    );
    assert_eq!(
        request
            .head
            .lines()
            .filter(|line| line.to_ascii_uppercase().starts_with("ONDO-"))
            .count(),
        3,
        "exactly the three documented headers: {}",
        request.head,
    );

    // The signature the venue will verify is the signature over the request it received.
    assert_eq!(
        request_header(request, ONDO_SIGN_HEADER).as_deref(),
        Some(resign_captured(request).as_str()),
        "the signature must cover exactly the bytes that were sent",
    );

    // Nothing about the credential belongs in the request line or query.
    assert!(!request.target.contains(FAKE_KEY_ID), "{}", request.target);
    assert!(
        !request.target.contains(FAKE_API_SECRET),
        "{}",
        request.target,
    );
}

#[tokio::test]
async fn test_a_signed_read_returns_the_cursor_and_the_raw_decimal_lexemes() {
    let body = r#"{"success":true,"cursor":"page-2","result":[{"id":"fill-1","orderId":"order-1","market":"NVDA-USD.P","price":"212.22994961526383480047124429696","size":"0.01","direction":"openLong","fee":"0.00025"}]}"#;
    let server = MockServer::start(vec![Reply::ok(body)]).await;
    let client = signed_client_for(&server, retry_policy(2, 1, 4, Some(5_000)));
    let query = OndoPrivateReadQuery::new()
        .with_market("NVDA-USD.P")
        .with_limit(2);

    let response = client
        .get_fills(&query)
        .await
        .expect("the fills body decodes");

    assert_eq!(
        server.targets(),
        vec!["/v1/perps/fills?market=NVDA-USD.P&limit=2".to_string()],
        "the query is serialized once, in the documented order",
    );
    assert_eq!(response.cursor(), Some("page-2"));
    assert_eq!(response.cursor_field(), Some("cursor"));

    let fills = response.fills().expect("the fill schema decodes");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].id(), "fill-1");
    assert_eq!(fills[0].order_id(), "order-1");
    assert_eq!(
        fills[0].price(),
        Some("212.22994961526383480047124429696"),
        "the decimal lexeme is the venue's, digit for digit",
    );
    assert_eq!(fills[0].direction().as_str(), "open_long");
}

#[tokio::test]
async fn test_a_signed_read_that_fails_with_a_server_error_is_retried_within_the_bound() {
    // A 5xx is transport-level, so the signed read is retried inside the policy exactly like a
    // public read - and each attempt is re-signed over the bytes of that attempt.
    let server = MockServer::start(vec![Reply::answer(503, "busy"), Reply::ok(ACCOUNT_BODY)]).await;
    let client = signed_client_for(&server, retry_policy(2, 1, 4, Some(5_000)));

    let response = client
        .get_account()
        .await
        .expect("the retry is answered before the bound");

    assert_eq!(response.http_status(), 200);

    let requests = server.captured();
    assert_eq!(
        requests.len(),
        2,
        "one attempt plus the policy's bounded retry",
    );

    for request in &requests {
        assert_eq!(request.target, ACCOUNT_PATH);
        assert_eq!(
            request_header(request, ONDO_KEY_ID_HEADER).as_deref(),
            Some(FAKE_KEY_ID),
            "every attempt carries the key id",
        );
        assert_eq!(
            request_header(request, ONDO_SIGN_HEADER).as_deref(),
            Some(resign_captured(request).as_str()),
            "each attempt's signature covers the bytes that attempt sent",
        );
    }
}

#[tokio::test]
async fn test_the_venue_s_named_auth_failures_are_distinct_on_the_wire() {
    // Each case is driven through the real transport, so the classification is asserted where it is
    // used rather than only in isolation.
    let cases: [(u16, &str, OndoAuthFailure); 5] = [
        (
            401,
            r#"{"code":"api_key_not_found","message":"unknown key"}"#,
            OndoAuthFailure::ApiKeyNotFound,
        ),
        (
            401,
            r#"{"code":"timestamp_too_far"}"#,
            OndoAuthFailure::TimestampTooFar,
        ),
        (
            401,
            r#"{"code":"signature_mismatch"}"#,
            OndoAuthFailure::SignatureMismatch,
        ),
        (
            403,
            r#"{"code":"key_doesnt_have_scope"}"#,
            OndoAuthFailure::KeyDoesntHaveScope,
        ),
        (403, "forbidden", OndoAuthFailure::Forbidden),
    ];

    for (status, body, expected) in cases {
        let server =
            MockServer::start(vec![Reply::answer(status, body), Reply::ok(ACCOUNT_BODY)]).await;
        let client = signed_client_for(&server, retry_policy(3, 1, 2, Some(5_000)));

        let error = client
            .get_account()
            .await
            .expect_err(&format!("HTTP {status} is a rejection"));

        match &error {
            OndoHttpError::AuthRejected {
                failure,
                status: seen,
                ..
            } => {
                assert_eq!(*failure, expected, "was {error:?}");
                assert_eq!(*seen, status);
            }
            other => panic!("expected a named auth rejection, was {other:?}"),
        }

        assert!(error.to_string().contains(expected.name()), "{error}");
        assert_eq!(
            server.captured().len(),
            1,
            "an auth rejection is terminal: it is never retried in a loop",
        );
    }
}

#[tokio::test]
async fn test_a_rejection_that_echoes_the_key_id_comes_back_redacted() {
    // The venue's own `ip_not_permitted` message echoes the key id it was sent, so the body is the
    // last place a credential could reach a log.
    let body = format!(
        r#"{{"code":"ip_not_permitted","message":"IP addr 203.0.113.1 is not allowed for key {FAKE_KEY_ID} (secret {FAKE_API_SECRET})"}}"#,
    );
    let server = MockServer::start(vec![Reply::answer(401, &body), Reply::ok(ACCOUNT_BODY)]).await;
    let client = signed_client_for(&server, retry_policy(3, 1, 2, Some(5_000)));

    let error = client.get_account().await.expect_err("401 is terminal");
    let rendered = format!("{error:?} {error}");

    assert!(
        matches!(
            &error,
            OndoHttpError::AuthRejected {
                failure: OndoAuthFailure::IpNotPermitted,
                ..
            }
        ),
        "was {error:?}",
    );
    assert!(!rendered.contains(FAKE_KEY_ID), "{rendered}");
    assert!(!rendered.contains(FAKE_API_SECRET), "{rendered}");
    assert!(rendered.contains(REDACTED), "{rendered}");
    // The venue's own diagnosis survives: redaction is not censoring.
    assert!(rendered.contains("203.0.113.1"), "{rendered}");
    assert_eq!(server.captured().len(), 1);
}

#[tokio::test]
async fn test_a_refused_clock_stops_the_next_request_before_the_socket() {
    // The first request is answered with a `Date` header decades behind, which is the same evidence
    // a real skew produces: the venue's clock is far outside the ±30 s signature tolerance. The
    // *next* request must therefore refuse to sign, and the server must see exactly one request.
    let server = MockServer::start(vec![Reply::with_header(
        200,
        "date",
        "Thu, 01 Jan 1970 00:00:00 GMT",
        ACCOUNT_BODY,
    )])
    .await;
    let client = signed_client_for(&server, retry_policy(3, 1, 2, Some(5_000)));

    client
        .get_account()
        .await
        .expect("the first request is signed: no clock evidence exists yet");

    let error = client
        .get_account()
        .await
        .expect_err("the observed offset is far outside the tolerance");

    assert!(
        matches!(error, OndoHttpError::Signing(_)),
        "expected a signing refusal, was {error:?}",
    );
    assert!(error.to_string().contains("refusing to sign"), "{error}");
    assert_eq!(
        server.captured().len(),
        1,
        "a refused signature is never sent: no second request may reach the socket",
    );
}

#[tokio::test]
async fn test_a_signed_post_is_single_shot_and_signed_over_its_body() {
    // A 503 is retryable for a read; the signed POST path must still send exactly one request, and
    // its signature must cover the body the server received.
    let server = MockServer::start(vec![Reply::answer(503, "busy"), Reply::ok(ORDERS_BODY)]).await;
    let client = signed_client_for(&server, retry_policy(3, 1, 2, Some(5_000)));
    let request = ORDER_REQUEST_BODY;
    let target = OndoRequestTarget::new(ORDERS_PATH);

    let error = client
        .post_signed_raw(
            &target,
            request.as_bytes().to_vec(),
            OndoRequestPriority::Normal,
            NewRiskPermit::new(1),
        )
        .await
        .expect_err("a 503 is an error on the signed POST path");

    assert!(
        matches!(
            error,
            OndoNewRiskSendError::Http(OndoHttpError::Http { status: 503, .. })
        ),
        "a 5xx is not an auth failure, was {error:?}",
    );

    let captured = server.captured();
    assert_eq!(
        captured.len(),
        1,
        "a signed POST must never be replayed: the result would be unknown",
    );
    assert_eq!(captured[0].body, request);
    assert_eq!(
        request_header(&captured[0], ONDO_SIGN_HEADER).as_deref(),
        Some(resign_captured(&captured[0]).as_str()),
        "the signature must cover the exact body bytes that were sent",
    );
}

/// The venue has no `POST .../cancel`: all three cancels are `DELETE` (plan §6.2), so this is the
/// request a cancel actually makes. It is single-shot for the same reason a submission is - a replay
/// could cancel twice - and its signature must cover the target and the **empty** body that were
/// sent.
#[tokio::test]
async fn test_a_signed_delete_is_single_shot_and_signs_the_bytes_it_sends() {
    // A 503 is retryable for a read; the signed DELETE path must still send exactly one request.
    let server = MockServer::start(vec![
        Reply::answer(503, "busy"),
        Reply::ok(BARE_SUCCESS_BODY),
    ])
    .await;
    let client = signed_client_for(&server, retry_policy(3, 1, 2, Some(5_000)));

    let error = client
        .delete_signed_raw(
            &market_cancel_target("NVDA-USD.P"),
            OndoRequestPriority::High,
        )
        .await
        .expect_err("a 503 is an error on the signed DELETE path");

    assert!(
        matches!(error, OndoHttpError::Http { status: 503, .. }),
        "a 5xx is not an auth failure, was {error:?}",
    );

    let captured = server.captured();
    assert_eq!(
        captured.len(),
        1,
        "a signed DELETE must never be replayed: the result would be unknown",
    );
    assert_eq!(captured[0].method, "DELETE");
    assert_eq!(captured[0].target, "/v1/perps/orders?market=NVDA-USD.P");
    assert_eq!(captured[0].body, "", "a cancel carries no body");
    assert_eq!(
        request_header(&captured[0], ONDO_SIGN_HEADER).as_deref(),
        Some(resign_captured(&captured[0]).as_str()),
        "the signature must cover the method, the target and the empty body that were sent",
    );
}

/// The same bytes, read two ways - and the difference is what a cancel is allowed to conclude.
///
/// `DELETE /v1/perps/orders` documents a bare `GenericResponse`, so the strict seam (which every
/// *read* needs: a missing `result` is a truncated answer) reports a schema failure, while the
/// cancel-all path reads the documented success as [`OndoCancelAnswer::Unconfirmed`]. That variant
/// is what obliges the caller to confirm by query; an error on this path is what let a cancel the
/// venue had performed look like a cancel that never happened.
#[tokio::test]
async fn test_a_market_cancel_reads_the_documented_bare_success_as_unconfirmed() {
    let server = MockServer::start(vec![Reply::ok(BARE_SUCCESS_BODY)]).await;
    let client = signed_client_for(&server, retry_policy(2, 1, 2, Some(5_000)));

    let strict = client
        .delete_signed_raw(
            &market_cancel_target("NVDA-USD.P"),
            OndoRequestPriority::High,
        )
        .await
        .expect_err("a read whose result went missing is a schema failure");
    assert!(
        matches!(
            strict,
            OndoHttpError::MissingField {
                field: "result",
                ..
            }
        ),
        "was {strict:?}",
    );

    match client
        .cancel_market_orders("NVDA-USD.P", OndoRequestPriority::High)
        .await
        .expect("the cancel-all answer is documented without a result")
    {
        OndoCancelAnswer::Unconfirmed { raw } => {
            assert_eq!(
                raw, BARE_SUCCESS_BODY,
                "the envelope's own text is kept, not a summary of it",
            );
        }
        other => panic!("a success carrying no order confirms nothing, was {other:?}"),
    }

    let captured = server.captured();
    assert_eq!(
        captured.len(),
        2,
        "each call is one request, and neither was retried"
    );
    assert!(captured.iter().all(|request| request.method == "DELETE"));
}

/// A cancel that carries the order's own post-cancel payload is read as that order.
#[tokio::test]
async fn test_a_cancel_that_carries_the_order_is_read_as_that_order() {
    let server = MockServer::start(vec![Reply::ok(CANCELLED_ORDER_BODY)]).await;
    let client = signed_client_for(&server, retry_policy(2, 1, 2, Some(5_000)));

    match client
        .cancel_order("order-1", OndoRequestPriority::High)
        .await
        .expect("the cancel answers with the order")
    {
        OndoCancelAnswer::Order(order) => {
            assert_eq!(order.order_id(), "order-1");
            assert_eq!(order.status(), &OndoOrderStatus::Canceled);
            assert_eq!(order.client_order_id(), Some("ondo_probe_example_1"));
        }
        other => panic!("expected the order the venue sent, was {other:?}"),
    }

    let captured = server.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].method, "DELETE");
    assert_eq!(captured[0].target, "/v1/perps/orders/order-1");
}

#[tokio::test]
async fn test_a_client_without_a_credential_refuses_to_sign_a_private_read() {
    let server = MockServer::start(vec![Reply::ok(ACCOUNT_BODY)]).await;
    let client = test_client(&server);

    assert!(!client.is_authenticated());

    let error = client
        .get_account()
        .await
        .expect_err("a public client has no credential to sign with");

    assert!(
        matches!(
            &error,
            OndoHttpError::NotAuthenticated { target } if target == ACCOUNT_PATH
        ),
        "was {error:?}",
    );
    assert!(
        server.captured().is_empty(),
        "the client must not reach the venue without a signature",
    );
}

// ------------------------------------------------------------------------------------------------
// A signed request is never carried to another authority
// ------------------------------------------------------------------------------------------------

/// A signed request carries `ONDO-KEY-ID`, `ONDO-SIGNATURE` and `ONDO-TIMESTAMP`. The underlying
/// transport follows up to ten redirects by default and re-sends the caller's headers on every hop;
/// the names above are this adapter's own, so the HTTP client's sensitive-header stripping (which
/// covers `Authorization`, `Cookie` and `Proxy-Authorization`) does not reach them. A redirect is
/// therefore the one path by which a signature could be replayed against another authority, and the
/// authenticated transport refuses the hop instead.
#[tokio::test]
async fn test_a_signed_request_does_not_follow_a_redirect_to_another_authority() {
    let elsewhere = MockServer::start(vec![Reply::ok(ACCOUNT_BODY)]).await;
    let redirecting = MockServer::start(vec![Reply::with_header(
        302,
        "Location",
        &format!("{}{}", elsewhere.url(), ACCOUNT_PATH),
        "",
    )])
    .await;

    let client = signed_client_for(&redirecting, retry_policy(0, 1, 2, Some(5_000)));

    let error = match client.get_account().await {
        Ok(response) => {
            panic!("a redirect is not a signed answer and must not be read as one: {response:?}",)
        }
        Err(error) => error,
    };

    assert!(
        matches!(error, OndoHttpError::Http { status: 302, .. }),
        "the redirect is answered as the status it is, was {error:?}",
    );
    assert_eq!(
        redirecting.captured().len(),
        1,
        "the request is sent once, and a redirect is never replayed",
    );
    assert!(
        elsewhere.captured().is_empty(),
        "the signed request must not be carried to the authority the redirect names",
    );
}

/// The mock server is the explicit local test service, and the client says which authority class it
/// signs for: a signed session on a loopback mock cannot reach the venue, and the public client
/// signs for nothing at all.
#[tokio::test]
async fn test_an_authenticated_client_reports_the_endpoint_class_it_signs_for() {
    let server = MockServer::start(vec![Reply::ok(MARKETS_BODY)]).await;

    assert_eq!(
        signed_client_for(&server, retry_policy(0, 1, 2, Some(5_000))).endpoint_kind(),
        Some(OndoEndpoint::LoopbackTestService),
    );
    assert_eq!(
        test_client(&server).endpoint_kind(),
        None,
        "the public transport carries no credential and is not gated",
    );
}

/// The refusal is a construction-time decision, so a client that would send to a refused authority
/// is never built. The mock server's own listener is used under a name the policy refuses
/// (`0.0.0.0`, which is not a loopback address), so "nothing was sent" is an observation about a
/// reachable listener rather than about a host that was never up.
#[tokio::test]
async fn test_an_endpoint_outside_the_allowlist_is_refused_before_a_client_exists() {
    let server = MockServer::start(vec![Reply::ok(ACCOUNT_BODY)]).await;
    let refused = server.url().replace("127.0.0.1", "0.0.0.0");

    let error = OndoHttpClient::builder()
        .base_url(refused)
        .credential(Arc::new(fake_credential()))
        .build()
        .expect_err("a host that is neither the sandbox authority nor loopback is refused");

    assert!(
        matches!(
            error,
            OndoHttpError::Environment(OndoEnvironmentError::HostNotAllowed { ref host })
                if host == "0.0.0.0"
        ),
        "the refusal is the endpoint gate's, was {error:?}",
    );
    assert!(
        server.captured().is_empty(),
        "no request is made: the refusal happens before the client exists",
    );
}

/// §4.4: a signed request draws on the *same* in-process budget as a public read, so it cannot
/// exceed it. The budget's only slot is spent before the read is spawned and the clock is paused, so
/// a signed read that bypassed the budget would reach the (closed) port and finish at once.
#[tokio::test(start_paused = true)]
async fn test_a_signed_read_draws_on_the_same_budget_as_a_public_read() {
    let budget =
        OndoRateBudget::with_quota(Quota::per_second(NonZeroU32::new(1).unwrap()).unwrap());
    let client = OndoHttpClient::builder()
        .base_url("http://127.0.0.1:1".to_string())
        .budget(budget.clone())
        .credential(Arc::new(fake_credential()))
        .build()
        .expect("the client builds");

    budget.acquire(OndoRequestPriority::Normal).await;

    let handle = tokio::spawn(async move {
        client
            .get_signed(
                &OndoRequestTarget::new(ACCOUNT_PATH),
                OndoRequestPriority::Normal,
            )
            .await
    });

    for _ in 0..64 {
        tokio::task::yield_now().await;
    }

    assert!(
        !handle.is_finished(),
        "the signed read waits for the shared slot instead of taking its own",
    );

    handle.abort();
}

// ------------------------------------------------------------------------------------------------
// The new-risk guard (plan §6.4)
// ------------------------------------------------------------------------------------------------

/// A `POST /v1/perps/orders` body: the documented create schema.
const ORDER_REQUEST_BODY: &str = r#"{"market":"NVDA-USD.P","side":"buy","type":"limit","size":"0.01","price":"100.00","clientOrderId":"ondo_probe_example_1"}"#;

/// What a test guard refuses a write with. The transport only carries it; what the account's own
/// guard renders is tested where that guard lives.
const TEST_REFUSAL: &str = "order-denied: reconciliation: the account stopped admitting new risk";

/// A new-risk guard a transport test drives by hand.
///
/// It admits until it is told not to, and it keeps every permit it was asked about - so a test can
/// assert *when* the send point asks (after the wait for the budget, never before it) and *what* it
/// asks with (the generation the caller was admitted under, not a fresh reading).
#[derive(Debug, Default)]
struct TestGuard {
    refusing: AtomicBool,
    seen: Mutex<Vec<u64>>,
}

impl TestGuard {
    fn admitting() -> Self {
        Self::default()
    }

    fn refusing() -> Self {
        Self {
            refusing: AtomicBool::new(true),
            seen: Mutex::new(Vec::new()),
        }
    }

    /// Stops admitting: the account's state changed while the request waited.
    fn refuse(&self) {
        self.refusing.store(true, Ordering::SeqCst);
    }

    fn asked(&self) -> usize {
        self.seen.lock().expect("the guard's log").len()
    }

    fn permits(&self) -> Vec<u64> {
        self.seen.lock().expect("the guard's log").clone()
    }
}

impl OndoNewRiskGuard for TestGuard {
    fn revalidate(&self, permit: NewRiskPermit) -> Result<(), String> {
        self.seen
            .lock()
            .expect("the guard's log")
            .push(permit.generation());

        if self.refusing.load(Ordering::SeqCst) {
            Err(TEST_REFUSAL.to_string())
        } else {
            Ok(())
        }
    }
}

/// The other half of the guard's contract: an admitted write goes out, unchanged and signed.
#[tokio::test]
async fn test_a_new_risk_write_whose_guard_admits_reaches_the_venue() {
    let server = MockServer::start(vec![Reply::ok(ORDERS_BODY)]).await;
    let guard = Arc::new(TestGuard::admitting());
    let client =
        signed_client_with_guard(&server, retry_policy(2, 1, 2, Some(5_000)), guard.clone());

    let response = client
        .post_signed_raw(
            &OndoRequestTarget::new(ORDERS_PATH),
            ORDER_REQUEST_BODY.as_bytes().to_vec(),
            OndoRequestPriority::Normal,
            NewRiskPermit::new(11),
        )
        .await
        .expect("an admitted write is sent");

    assert_eq!(response.raw_result(), r#"{"orderId":"order-1"}"#);

    let captured = server.captured();
    assert_eq!(captured.len(), 1, "the write is sent once");
    assert_eq!(captured[0].target, ORDERS_PATH);
    assert_eq!(captured[0].body, ORDER_REQUEST_BODY);
    assert_eq!(
        request_header(&captured[0], ONDO_SIGN_HEADER).as_deref(),
        Some(resign_captured(&captured[0]).as_str()),
        "the gate does not change what is signed or what is sent",
    );
    assert_eq!(
        guard.permits(),
        vec![11],
        "the send point re-checks the permit the caller was admitted under",
    );
}

/// R0.1's window at the transport: the guard is consulted **after** the request has waited for the
/// shared budget and **before** it exists. The clock is stopped and the budget's only slot is spent
/// before the write is spawned, so the wait is this test's to end - and the base URL is a closed
/// port, which is what makes "nothing was sent" observable rather than assumed: a request that went
/// out anyway could only come back as a transport error.
#[tokio::test(start_paused = true)]
async fn test_a_new_risk_write_is_refused_by_its_guard_after_the_budget_wait() {
    let budget =
        OndoRateBudget::with_quota(Quota::per_second(NonZeroU32::new(1).unwrap()).unwrap());
    let guard = Arc::new(TestGuard::admitting());
    let client = OndoHttpClient::builder()
        .base_url("http://127.0.0.1:1".to_string())
        .budget(budget.clone())
        .credential(Arc::new(fake_credential()))
        .new_risk_guard(Arc::clone(&guard) as Arc<dyn OndoNewRiskGuard>)
        .build()
        .expect("the client builds");

    budget.acquire(OndoRequestPriority::Normal).await;

    let handle = tokio::spawn(async move {
        client
            .post_signed_raw(
                &OndoRequestTarget::new(ORDERS_PATH),
                ORDER_REQUEST_BODY.as_bytes().to_vec(),
                OndoRequestPriority::Normal,
                NewRiskPermit::new(7),
            )
            .await
    });

    for _ in 0..64 {
        tokio::task::yield_now().await;
    }

    assert!(
        !handle.is_finished(),
        "the write waits for the shared slot instead of taking its own",
    );
    assert_eq!(
        guard.asked(),
        0,
        "the guard is asked at the send point, not while the request queues",
    );

    // The account stops admitting new risk while the write is queued.
    guard.refuse();

    // Release the budget: the write resumes and reaches the send point.
    tokio::time::advance(Duration::from_secs(1)).await;

    for _ in 0..64 {
        tokio::task::yield_now().await;
    }

    let outcome = handle.await.expect("the write task completes");

    assert!(
        matches!(outcome, Err(OndoNewRiskSendError::Refused { ref reason }) if reason == TEST_REFUSAL),
        "the account's refusal is the outcome, not a request that went out: {outcome:?}",
    );
    assert_eq!(
        guard.permits(),
        vec![7],
        "the permit is re-checked as the one the caller held, after the wait",
    );
}

/// A client that cannot answer "does the account admit new risk?" does not send. The transport is
/// the last gate before the wire, so the failure it must not have is the quiet one.
#[tokio::test]
async fn test_an_authenticated_client_without_a_new_risk_guard_refuses_a_signed_post() {
    let server = MockServer::start(vec![Reply::ok(ORDERS_BODY)]).await;
    let client = OndoHttpClient::builder()
        .base_url(server.url())
        .budget(test_budget())
        .retry_config(retry_policy(2, 1, 2, Some(5_000)))
        .credential(Arc::new(fake_credential()))
        .build()
        .expect("the authenticated mock client builds");

    let outcome = client
        .post_signed_raw(
            &OndoRequestTarget::new(ORDERS_PATH),
            ORDER_REQUEST_BODY.as_bytes().to_vec(),
            OndoRequestPriority::Normal,
            NewRiskPermit::new(1),
        )
        .await;

    assert!(
        matches!(outcome, Err(OndoNewRiskSendError::Refused { .. })),
        "a write that cannot be admitted is not sent: {outcome:?}",
    );
    assert!(
        server.captured().is_empty(),
        "the refusal happens before a request exists",
    );
}

/// The cancel path carries no new-risk gate, and that is deliberate: a cancel reduces risk, and an
/// account that has stopped admitting new risk is exactly an account whose resting orders have to
/// be cleaned up. Gating it would refuse the cancels an unknown outcome makes necessary.
#[tokio::test]
async fn test_a_refusing_new_risk_guard_does_not_stop_a_cancel() {
    let server = MockServer::start(vec![Reply::ok(CANCELLED_ORDER_BODY)]).await;
    let guard = Arc::new(TestGuard::refusing());
    let client =
        signed_client_with_guard(&server, retry_policy(2, 1, 2, Some(5_000)), guard.clone());

    let refused = client
        .post_signed_raw(
            &OndoRequestTarget::new(ORDERS_PATH),
            ORDER_REQUEST_BODY.as_bytes().to_vec(),
            OndoRequestPriority::Normal,
            NewRiskPermit::new(3),
        )
        .await;
    assert!(
        matches!(refused, Err(OndoNewRiskSendError::Refused { .. })),
        "new risk is refused: {refused:?}",
    );

    match client
        .cancel_order("order-1", OndoRequestPriority::High)
        .await
        .expect("the cancel is sent")
    {
        OndoCancelAnswer::Order(order) => assert_eq!(order.order_id(), "order-1"),
        other => panic!("expected the order the venue sent, was {other:?}"),
    }

    let captured = server.captured();
    assert_eq!(captured.len(), 1, "only the cancel became a request");
    assert_eq!(captured[0].method, "DELETE");
    assert_eq!(captured[0].target, "/v1/perps/orders/order-1");
    assert_eq!(
        guard.asked(),
        1,
        "the cancel path does not consult the new-risk guard",
    );
}

// ------------------------------------------------------------------------------------------------
// The read-only dispatch guard
// ------------------------------------------------------------------------------------------------

/// A read-only scope refuses a signed write at the dispatch, before the shared budget is acquired
/// and before the new-risk guard is consulted, so no request exists and no admission decision is
/// wasted. This is the write refusal the account's own read-only flag does not by itself provide:
/// `DELETE` is deliberately not new-risk guarded, and this guard is what stops a cancel too.
#[tokio::test]
async fn test_a_read_only_client_refuses_a_signed_post_and_delete_before_any_request() {
    let server = MockServer::start(vec![Reply::ok(ORDERS_BODY)]).await;
    let guard = Arc::new(TestGuard::admitting());
    let client = OndoHttpClient::builder()
        .base_url(server.url())
        .budget(test_budget())
        .retry_config(retry_policy(2, 1, 2, Some(5_000)))
        .credential(Arc::new(fake_credential()))
        .authentication_scope(OndoAuthenticationScope::SandboxReadOnly)
        .new_risk_guard(guard.clone() as Arc<dyn OndoNewRiskGuard>)
        .build()
        .expect("the read-only mock client builds");

    assert_eq!(
        client.authentication_scope(),
        Some(OndoAuthenticationScope::SandboxReadOnly),
    );

    let posted = client
        .post_signed_raw(
            &OndoRequestTarget::new(ORDERS_PATH),
            ORDER_REQUEST_BODY.as_bytes().to_vec(),
            OndoRequestPriority::Normal,
            NewRiskPermit::new(1),
        )
        .await;
    assert!(
        matches!(
            posted,
            Err(OndoNewRiskSendError::Http(
                OndoHttpError::WriteNotPermitted {
                    scope: OndoAuthenticationScope::SandboxReadOnly,
                    method: "POST",
                }
            ))
        ),
        "a read-only session refuses a submission at the dispatch: {posted:?}",
    );

    let deleted = client
        .cancel_order("order-1", OndoRequestPriority::High)
        .await;
    assert!(
        matches!(
            deleted,
            Err(OndoHttpError::WriteNotPermitted {
                scope: OndoAuthenticationScope::SandboxReadOnly,
                method: "DELETE",
            })
        ),
        "a read-only session refuses a cancel at the dispatch: {deleted:?}",
    );

    assert!(
        server.captured().is_empty(),
        "neither refusal became a request",
    );
    assert_eq!(
        guard.asked(),
        0,
        "the dispatch refuses before the new-risk guard is consulted",
    );
}

/// The production read-only scope is refused the same way, and a production credential built
/// without an explicit scope defaults to read-only rather than to a write-capable transport.
#[tokio::test]
async fn test_a_production_read_only_client_refuses_writes_and_defaults_to_read_only() {
    let server = MockServer::start(vec![Reply::ok(ORDERS_BODY)]).await;

    let defaulted = OndoHttpClient::builder()
        .base_url(server.url())
        .budget(test_budget())
        .credential(Arc::new(fake_production_credential()))
        .new_risk_guard(Arc::new(TestGuard::admitting()) as Arc<dyn OndoNewRiskGuard>)
        .build()
        .expect("the production mock client builds");

    assert_eq!(
        defaulted.authentication_scope(),
        Some(OndoAuthenticationScope::ProductionReadOnly),
        "a production credential without a scope is never write-capable by omission",
    );

    let posted = defaulted
        .post_signed_raw(
            &OndoRequestTarget::new(ORDERS_PATH),
            ORDER_REQUEST_BODY.as_bytes().to_vec(),
            OndoRequestPriority::Normal,
            NewRiskPermit::new(1),
        )
        .await;
    assert!(
        matches!(
            posted,
            Err(OndoNewRiskSendError::Http(
                OndoHttpError::WriteNotPermitted {
                    scope: OndoAuthenticationScope::ProductionReadOnly,
                    ..
                }
            ))
        ),
        "a production read-only session refuses a submission: {posted:?}",
    );

    assert!(
        server.captured().is_empty(),
        "the production refusal became no request",
    );
}

/// A sandbox credential built without an explicit scope keeps the sandbox's normal trading
/// capability, so the read-only dispatch guard does not weaken ordinary sandbox behaviour.
#[tokio::test]
async fn test_a_sandbox_credential_without_a_scope_still_trades() {
    let server = MockServer::start(vec![Reply::ok(ORDERS_BODY)]).await;
    let client = signed_client_for(&server, retry_policy(2, 1, 2, Some(5_000)));

    assert_eq!(
        client.authentication_scope(),
        Some(OndoAuthenticationScope::SandboxTrading),
    );

    client
        .post_signed_raw(
            &OndoRequestTarget::new(ORDERS_PATH),
            ORDER_REQUEST_BODY.as_bytes().to_vec(),
            OndoRequestPriority::Normal,
            NewRiskPermit::new(1),
        )
        .await
        .expect("a sandbox trading session still sends an admitted write");

    assert_eq!(server.captured().len(), 1);
}

/// A public client carries no scope and no credential, so it cannot sign at all - and that is still
/// the answer, not a write refusal, for a client that never had a write surface.
#[tokio::test]
async fn test_a_public_client_never_permits_a_write() {
    let server = MockServer::start(vec![Reply::ok(ORDERS_BODY)]).await;
    let client = test_client(&server);

    assert_eq!(client.authentication_scope(), None);
    assert!(
        !client.is_authenticated(),
        "a public client holds no credential",
    );
    assert!(server.captured().is_empty());
}

/// A credential is never sent under another environment's scope. The pair is refused at
/// construction, before a socket exists, in both directions - including the finding's exact shape,
/// a production credential with a sandbox-trading scope pointed at the official sandbox URL.
#[tokio::test]
async fn test_a_credential_from_another_environment_is_refused_for_the_scope() {
    let server = MockServer::start(Vec::new()).await;

    for (credential, scope, base_url) in [
        (
            fake_credential(),
            OndoAuthenticationScope::ProductionReadOnly,
            server.url(),
        ),
        (
            fake_production_credential(),
            OndoAuthenticationScope::SandboxTrading,
            server.url(),
        ),
        // The exact shape the review named: a production key scoped as sandbox trading against
        // the official sandbox authority. The refusal is the credential/scope pair, not the URL.
        (
            fake_production_credential(),
            OndoAuthenticationScope::SandboxTrading,
            "https://api.ondoperps-sandbox.xyz".to_string(),
        ),
        (
            fake_credential(),
            OndoAuthenticationScope::ProductionReadOnly,
            "https://api.ondoperps.xyz".to_string(),
        ),
    ] {
        let built = OndoHttpClient::builder()
            .base_url(base_url)
            .budget(test_budget())
            .credential(Arc::new(credential))
            .authentication_scope(scope)
            .build();

        let error = built.expect_err("a cross-environment credential is refused");
        assert!(
            matches!(
                error,
                OndoHttpError::Environment(
                    OndoEnvironmentError::CredentialEnvironmentMismatch { .. }
                )
            ),
            "was {error:?}",
        );
    }

    assert!(
        server.captured().is_empty(),
        "the refusal happens before a socket is opened",
    );
}
