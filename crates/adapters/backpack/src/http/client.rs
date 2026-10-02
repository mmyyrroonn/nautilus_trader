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

//! Restricted GET transport with fresh post-quota authentication and bounded retries.

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use nautilus_network::{
    dst::time::{Instant, timeout},
    http::{HttpClient, HttpRedirectPolicy, HttpResponse, Method, PreparedHttpRequest},
    retry::{RetryConfig, RetryError, RetryManager},
};
use tokio_util::sync::CancellationToken;

use super::{
    error::{BackpackHttpError, BackpackHttpErrorKind, BackpackRequestOutcome},
    quota::BackpackQuota,
    request::BackpackReadRequest,
};
use crate::{
    common::{credential::BackpackCredential, endpoints::BackpackEndpoints},
    signing::{BackpackReceiveWindow, canonical_rest},
};

const RESPONSE_HEADERS: &[&str] = &[
    "x-page-count",
    "x-current-page",
    "x-page-size",
    "x-total",
    "retry-after",
];

/// An injectable Unix clock for expiring protocol authentication.
pub trait BackpackClock: fmt::Debug + Send + Sync {
    /// Returns Unix milliseconds, within the signed timestamp range.
    ///
    /// # Errors
    ///
    /// Returns an error when a usable timestamp is unavailable.
    fn timestamp_ms(&self) -> Result<u64, BackpackHttpError>;
}

/// System clock used for live protocol requests.
#[derive(Debug, Default)]
pub struct BackpackSystemClock;
impl BackpackClock for BackpackSystemClock {
    fn timestamp_ms(&self) -> Result<u64, BackpackHttpError> {
        let value = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Validation))?
            .as_millis();
        u64::try_from(value)
            .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Validation))
    }
}

/// A finite request budget covering quota waits, retry delays and all transport attempts.
#[derive(Clone, Copy, Debug)]
pub struct BackpackHttpPolicy {
    window: BackpackReceiveWindow,
    budget: Duration,
    max_retries: u32,
}
impl Default for BackpackHttpPolicy {
    fn default() -> Self {
        Self {
            window: BackpackReceiveWindow::default(),
            budget: Duration::from_secs(15),
            max_retries: 2,
        }
    }
}
impl BackpackHttpPolicy {
    /// Validates a finite budget and a maximum of ten read retries.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero/over60s budget or more than ten retries.
    pub fn new(
        window: BackpackReceiveWindow,
        budget: Duration,
        max_retries: u32,
    ) -> Result<Self, BackpackHttpError> {
        if budget.is_zero() || budget > Duration::from_secs(60) || max_retries > 10 {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Validation));
        }
        Ok(Self {
            window,
            budget,
            max_retries,
        })
    }
}

/// Raw read bytes and allowlisted pagination headers, with a redacted Debug surface.
#[derive(Clone)]
pub struct BackpackHttpResponse {
    body: Bytes,
    headers: HashMap<String, String>,
}
impl fmt::Debug for BackpackHttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpackHttpResponse")
            .field("payload", &"[REDACTED]")
            .finish()
    }
}
impl BackpackHttpResponse {
    /// Returns exact response bytes for typed parsing at the next boundary.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    /// Returns allowlisted pagination and backoff headers.
    #[must_use]
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }
}

/// Fixed-origin GET client; credentials never accompany public requests.
#[derive(Clone)]
pub struct BackpackHttpClient {
    endpoints: BackpackEndpoints,
    credential: Option<BackpackCredential>,
    transport: HttpClient,
    policy: BackpackHttpPolicy,
    clock: Arc<dyn BackpackClock>,
}
impl fmt::Debug for BackpackHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpackHttpClient")
            .field("authentication", &"[REDACTED]")
            .field("policy", &self.policy)
            .finish()
    }
}
impl BackpackHttpClient {
    /// Creates a restricted client sharing the caller's quota scope.
    ///
    /// # Errors
    ///
    /// Returns an error for credential audience mismatch or transport configuration failure.
    pub fn new(
        endpoints: BackpackEndpoints,
        credential: Option<BackpackCredential>,
        quota: BackpackQuota,
        policy: BackpackHttpPolicy,
        clock: Arc<dyn BackpackClock>,
    ) -> Result<Self, BackpackHttpError> {
        if let Some(credential) = &credential {
            credential.check_audience(&endpoints)?;
        }
        let transport = HttpClient::builder()
            .header_keys(
                RESPONSE_HEADERS
                    .iter()
                    .map(|key| (*key).to_string())
                    .collect(),
            )
            .rate_limiters(vec![quota.into_limiter()])
            .redirect_policy(HttpRedirectPolicy::Reject)
            .use_system_proxy(false)
            .build()?;
        Ok(Self {
            endpoints,
            credential,
            transport,
            policy,
            clock,
        })
    }

    /// Performs a GET with one finite budget including all quota waits and read retries.
    ///
    /// Admission is checked before queuing and after quota acquisition. Cancellation
    /// after possible dispatch remains Unknown, even when a later attempt is refused.
    /// This API cannot place or cancel an order or mutate account settings.
    ///
    /// # Errors
    ///
    /// Returns typed local, venue, uncertain transport, decoding, budget or cancellation failure.
    pub async fn read(
        &self,
        request: &BackpackReadRequest,
        deadline: Option<Instant>,
        admission: Option<&(dyn Fn() -> bool + Send + Sync)>,
        cancel: &CancellationToken,
    ) -> Result<BackpackHttpResponse, BackpackHttpError> {
        if request.is_authenticated() && self.credential.is_none() {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Credentials));
        }
        if admission.is_some_and(|admission| !admission()) {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission));
        }
        let budget_deadline = Instant::now()
            .checked_add(self.policy.budget)
            .ok_or_else(|| BackpackHttpError::local(BackpackHttpErrorKind::Validation))?;
        let deadline = deadline.map_or(budget_deadline, |d| d.min(budget_deadline));
        if deadline <= Instant::now() {
            return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission));
        }
        let transmitted = AtomicBool::new(false);
        let prior_unknown = AtomicBool::new(false);
        let retry = RetryManager::new(RetryConfig {
            max_retries: self.policy.max_retries,
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_factor: 2.0,
            jitter_ms: 0,
            operation_timeout_ms: None,
            immediate_first: false,
            max_elapsed_ms: Some(self.policy.budget.as_millis() as u64),
        });
        let operation = retry.execute_with_retry_with_delay(
            "Backpack read",
            || async {
                let result = self
                    .read_once(request, deadline, admission, &transmitted)
                    .await;
                result.map_err(|mut e| {
                    if prior_unknown.load(Ordering::Acquire) {
                        e.outcome = BackpackRequestOutcome::Unknown;
                    }
                    if e.outcome == BackpackRequestOutcome::Unknown {
                        prior_unknown.store(true, Ordering::Release);
                    }
                    e
                })
            },
            BackpackHttpError::is_retryable_read,
            |e| {
                if e.status == Some(429) {
                    Some(e.retry_after.unwrap_or(Duration::from_secs(1)))
                } else {
                    None
                }
            },
            |e| {
                let kind = match e {
                    RetryError::Canceled => BackpackHttpErrorKind::Cancelled,
                    RetryError::InvalidConfiguration { .. } => BackpackHttpErrorKind::Validation,
                    _ => BackpackHttpErrorKind::Budget,
                };
                BackpackHttpError::local(kind)
                    .preserve_transmission(transmitted.load(Ordering::Acquire))
            },
        );
        let operation = async {
            timeout(
                deadline.saturating_duration_since(Instant::now()),
                operation,
            )
            .await
            .map_err(|_| BackpackHttpError::local(BackpackHttpErrorKind::Budget))?
        };
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(BackpackHttpError::local(BackpackHttpErrorKind::Cancelled)),
            result = operation => result,
        };
        result.map_err(|e| e.preserve_transmission(transmitted.load(Ordering::Acquire)))
    }

    async fn read_once(
        &self,
        request: &BackpackReadRequest,
        deadline: Instant,
        admission: Option<&(dyn Fn() -> bool + Send + Sync)>,
        transmitted: &AtomicBool,
    ) -> Result<BackpackHttpResponse, BackpackHttpError> {
        let previously_transmitted = transmitted.load(Ordering::Acquire);
        let response = self
            .transport
            .request_with_url_redacted_prepared_request(
                Method::GET,
                Some(BackpackQuota::keys(request.operation.historical_market())),
                Some(deadline),
                || {
                    if admission.is_some_and(|admission| !admission()) {
                        return Err(BackpackHttpError::local(BackpackHttpErrorKind::Admission));
                    }
                    let mut url = format!(
                        "{}{}",
                        self.endpoints.rest_url().trim_end_matches('/'),
                        request.operation.path()
                    );
                    let query = request.parameters.query_string();
                    if !query.is_empty() {
                        url.push('?');
                        url.push_str(&query);
                    }
                    let headers = if let Some(instruction) = request.operation.instruction() {
                        let credential = self.credential.as_ref().ok_or_else(|| {
                            BackpackHttpError::local(BackpackHttpErrorKind::Credentials)
                        })?;
                        credential.check_audience(&self.endpoints)?;
                        let timestamp = self.clock.timestamp_ms()?;
                        if timestamp > i64::MAX as u64 {
                            return Err(BackpackHttpError::local(
                                BackpackHttpErrorKind::Validation,
                            ));
                        }
                        let canonical = canonical_rest(
                            instruction,
                            &request.parameters,
                            timestamp,
                            self.policy.window,
                        );
                        Some(credential.headers(&canonical, timestamp, self.policy.window)?)
                    } else {
                        None
                    };
                    transmitted.store(true, Ordering::Release);
                    Ok(PreparedHttpRequest {
                        url,
                        headers,
                        body: None,
                    })
                },
            )
            .await;
        if response
            .as_ref()
            .is_err_and(|e| e.outcome == BackpackRequestOutcome::NotSent)
        {
            // The shared transport proves that this attempt never entered transport,
            // including a deadline expiring during synchronous preparation.
            transmitted.store(previously_transmitted, Ordering::Release);
        }
        Self::decode_response(response?)
    }

    fn decode_response(response: HttpResponse) -> Result<BackpackHttpResponse, BackpackHttpError> {
        let status = response.status.as_u16();
        if status != 200 {
            let code = serde_json::from_slice::<serde_json::Value>(&response.body)
                .ok()
                .and_then(|value| {
                    value
                        .get("code")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .filter(|code| {
                    !code.is_empty()
                        && code.len() <= 64
                        && code
                            .bytes()
                            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                });
            let outcome = if [400, 401, 403].contains(&status)
                && matches!(
                    code.as_deref(),
                    Some("INVALID_CLIENT_REQUEST" | "ACCOUNT_DEACTIVATED")
                ) {
                BackpackRequestOutcome::VenueRejected
            } else {
                BackpackRequestOutcome::Unknown
            };
            let retry_after = response
                .headers
                .get("retry-after")
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs);
            return Err(BackpackHttpError {
                kind: BackpackHttpErrorKind::Venue,
                outcome,
                status: Some(status),
                code,
                retry_after,
            });
        }
        serde_json::from_slice::<serde_json::Value>(&response.body)
            .map_err(|_| BackpackHttpError::unknown(BackpackHttpErrorKind::Decode))?;
        let headers = response
            .headers
            .into_iter()
            .filter(|(key, _)| RESPONSE_HEADERS.contains(&key.as_str()))
            .collect();
        Ok(BackpackHttpResponse {
            body: response.body,
            headers,
        })
    }
}
