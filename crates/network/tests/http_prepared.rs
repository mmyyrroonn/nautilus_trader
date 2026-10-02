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

//! Complete request preparation at the HTTP quota and transport boundary.

#![cfg(not(all(feature = "simulation", madsim)))]

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{Router, http::HeaderMap, routing::post};
use nautilus_network::{
    http::{HttpClient, HttpClientError, Method, PreparedHttpRequest},
    ratelimiter::{RateLimiter, quota::Quota},
};
use rstest::rstest;
use tokio::{sync::oneshot, time::Instant};
use ustr::Ustr;

#[tokio::test(start_paused = true)]
async fn test_complete_request_is_prepared_after_quota_and_sent_once() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let server_requests = Arc::clone(&requests);
    let app = Router::new().route(
        "/prepared",
        post(move |headers: HeaderMap, body: String| {
            let requests = Arc::clone(&server_requests);
            async move {
                requests.fetch_add(1, Ordering::AcqRel);
                format!("{}\n{body}", headers["x-signature"].to_str().unwrap())
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let key = Ustr::from("prepared-headers");
    let limiter = Arc::new(RateLimiter::new_with_quota(
        None,
        vec![(key, Quota::with_period(Duration::from_secs(10)).unwrap())],
    ));
    limiter.check_key(&key).unwrap();
    let client = HttpClient::builder()
        .rate_limiters(vec![limiter])
        .use_system_proxy(false)
        .build()
        .unwrap();
    let version = Arc::new(AtomicU64::new(1));
    let prepared = Arc::new(AtomicBool::new(false));
    let request_version = Arc::clone(&version);
    let request_prepared = Arc::clone(&prepared);
    let request = tokio::spawn(async move {
        client
            .request_with_url_redacted_prepared_request(
                Method::POST,
                Some(vec![key.to_string()]),
                None,
                || -> Result<PreparedHttpRequest, HttpClientError> {
                    assert!(!request_prepared.swap(true, Ordering::AcqRel));
                    let version = request_version.load(Ordering::Acquire);
                    Ok(PreparedHttpRequest {
                        url: format!("http://{address}/prepared"),
                        headers: Some(HashMap::from([(
                            "x-signature".to_string(),
                            format!("signed-{version}"),
                        )])),
                        body: Some(format!("body-{version}").into_bytes()),
                    })
                },
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(!prepared.load(Ordering::Acquire));
    assert_eq!(requests.load(Ordering::Acquire), 0);
    version.store(2, Ordering::Release);
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::time::resume();
    let response = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status.as_u16(), 200);
    assert_eq!(response.body.as_ref(), b"signed-2\nbody-2");
    assert_eq!(requests.load(Ordering::Acquire), 1);
    assert!(prepared.load(Ordering::Acquire));
    server.abort();
}

#[tokio::test(start_paused = true)]
async fn test_complete_request_deadline_expires_without_preparation() {
    let key = Ustr::from("prepared-headers-deadline");
    let limiter = Arc::new(RateLimiter::new_with_quota(
        None,
        vec![(key, Quota::with_period(Duration::from_secs(10)).unwrap())],
    ));
    limiter.check_key(&key).unwrap();
    let client = HttpClient::builder()
        .rate_limiters(vec![limiter])
        .use_system_proxy(false)
        .build()
        .unwrap();
    let prepared = Arc::new(AtomicBool::new(false));
    let request_prepared = Arc::clone(&prepared);
    let deadline = Instant::now() + Duration::from_secs(2);
    let request = tokio::spawn(async move {
        client
            .request_with_url_redacted_prepared_request(
                Method::POST,
                Some(vec![key.to_string()]),
                Some(deadline),
                || -> Result<PreparedHttpRequest, HttpClientError> {
                    request_prepared.store(true, Ordering::Release);
                    Err(HttpClientError::Error("must not prepare".to_string()))
                },
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(!prepared.load(Ordering::Acquire));
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(
        request.await.unwrap(),
        Err(HttpClientError::AdmissionDenied(_))
    ));
    assert!(!prepared.load(Ordering::Acquire));
}

#[tokio::test]
async fn test_complete_request_preserves_preparation_refusal() {
    let client = HttpClient::builder()
        .use_system_proxy(false)
        .build()
        .unwrap();
    let result = client
        .request_with_url_redacted_prepared_request(
            Method::POST,
            None,
            None,
            || -> Result<PreparedHttpRequest, HttpClientError> {
                Err(HttpClientError::AdmissionDenied(
                    "stale session".to_string(),
                ))
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(HttpClientError::AdmissionDenied(reason)) if reason == "stale session"
    ));
}

#[tokio::test]
async fn test_complete_request_deadline_after_send_is_transport_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, received) = oneshot::channel();
    let signal = Arc::new(std::sync::Mutex::new(Some(sent)));
    let app = Router::new().route(
        "/pending",
        post(move || {
            let signal = Arc::clone(&signal);
            async move {
                signal.lock().unwrap().take().unwrap().send(()).unwrap();
                std::future::pending::<String>().await
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = HttpClient::builder()
        .use_system_proxy(false)
        .build()
        .unwrap();
    let request = tokio::spawn(async move {
        client
            .request_with_url_redacted_prepared_request(
                Method::POST,
                None,
                Some(Instant::now() + Duration::from_secs(10)),
                || -> Result<PreparedHttpRequest, HttpClientError> {
                    Ok(PreparedHttpRequest {
                        url: format!("http://{address}/pending"),
                        headers: None,
                        body: Some(b"one-attempt".to_vec()),
                    })
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), received)
        .await
        .unwrap()
        .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(matches!(
        request.await.unwrap(),
        Err(HttpClientError::TimeoutError(_))
    ));
    server.abort();
}

#[rstest]
fn test_prepared_request_debug_omits_authentication_values() {
    let request = PreparedHttpRequest {
        url: "https://example.invalid/?signature=synthetic-query".to_string(),
        headers: Some(HashMap::from([(
            "x-signature".to_string(),
            "synthetic-header".to_string(),
        )])),
        body: Some(b"synthetic-body".to_vec()),
    };
    assert_eq!(format!("{request:?}"), "PreparedHttpRequest { .. }");
}
