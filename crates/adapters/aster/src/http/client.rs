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

//! HTTP client for the signed endpoints of the Aster Futures V3 API.
//!
//! Market data is served by the Binance USD-M data client (see [`crate::factories`]); this
//! client covers only the EIP-712 signed `TRADE` / `USER_DATA` / `USER_STREAM` surface, which
//! has no Binance equivalent because Aster replaced HMAC authentication with typed-data
//! signatures in V3.
//!
//! Every signed request is built as an ordered parameter string prefixed with the
//! authentication triple `nonce`, `user`, `signer`, signed, and then sent as the query string
//! for `GET` or as an `application/x-www-form-urlencoded` body otherwise.

use std::{collections::HashMap, num::NonZeroU32, sync::Arc, time::Duration};

use nautilus_core::consts::NAUTILUS_USER_AGENT;
use nautilus_network::{
    http::{HttpClient, HttpResponse, Method, USER_AGENT},
    ratelimiter::quota::Quota,
    retry::{RetryConfig, RetryManager},
};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use tokio::time::{Instant, sleep_until};

use crate::{
    common::{
        consts::{
            ASTER_ALL_OPEN_ORDERS_PATH, ASTER_ALL_ORDERS_PATH, ASTER_BALANCE_PATH,
            ASTER_COMMISSION_RATE_PATH, ASTER_GLOBAL_RATE_KEY, ASTER_LISTEN_KEY_PATH,
            ASTER_OPEN_ORDERS_PATH, ASTER_ORDER_PATH, ASTER_ORDER_RATE_KEY,
            ASTER_ORDERS_PER_MINUTE, ASTER_POSITION_RISK_PATH, ASTER_POSITION_SIDE_DUAL_PATH,
            ASTER_REQUEST_WEIGHT_PER_MINUTE, ASTER_USER_TRADES_PATH, ASTER_WEIGHT_ALL_OPEN_ORDERS,
            ASTER_WEIGHT_ALL_ORDERS, ASTER_WEIGHT_BALANCE, ASTER_WEIGHT_COMMISSION_RATE,
            ASTER_WEIGHT_DEFAULT, ASTER_WEIGHT_LISTEN_KEY, ASTER_WEIGHT_OPEN_ORDERS_ALL,
            ASTER_WEIGHT_OPEN_ORDERS_SYMBOL, ASTER_WEIGHT_ORDER, ASTER_WEIGHT_POSITION_RISK,
            ASTER_WEIGHT_POSITION_SIDE_DUAL, ASTER_WEIGHT_USER_TRADES,
        },
        credential::AsterCredential,
    },
    http::{
        error::{AsterHttpError, AsterHttpResult},
        models::{
            AsterBalance, AsterCancelAllOrdersResponse, AsterCommissionRate, AsterErrorResponse,
            AsterListenKeyResponse, AsterOrder, AsterPositionModeResponse, AsterPositionRisk,
            AsterUserTrade,
        },
        query::AsterParams,
    },
    signing::NonceGenerator,
};

const FORM_CONTENT_TYPE: &str = "application/x-www-form-urlencoded";

/// Retries attempted after the first failed `GET`, capping total attempts at four.
const GET_MAX_RETRIES: u32 = 3;

/// Delay before the first `GET` retry; doubled per attempt up to [`GET_RETRY_MAX_DELAY_MS`].
const GET_RETRY_INITIAL_DELAY_MS: u64 = 500;

/// Cap on the `GET` retry backoff, giving a 500 ms / 1 s / 2 s schedule.
const GET_RETRY_MAX_DELAY_MS: u64 = 2_000;

/// Response header carrying the wait the venue asks for after a `429` or `418`.
const RETRY_AFTER_HEADER: &str = "retry-after";

/// HTTP status Aster returns when a rate-limit budget is exceeded.
const STATUS_TOO_MANY_REQUESTS: u16 = 429;

/// HTTP status Aster returns when repeated violations earn the IP an outright ban.
const STATUS_IP_BANNED: u16 = 418;

/// Cooldown applied after a `429` that carries no usable `Retry-After`.
const RATE_LIMIT_COOLDOWN_SECS: u64 = 10;

/// Cooldown applied after a `418` that carries no usable `Retry-After`.
const IP_BAN_COOLDOWN_SECS: u64 = 120;

/// Upper bound on a cooldown, however long the venue asks for.
///
/// Aster bans run from two minutes to three days. Sleeping out a multi-day ban inside the
/// client would strand the execution engine with no way to observe it, so the wait is capped
/// and a still-banned venue simply re-arms the cooldown on the next attempt.
const MAX_COOLDOWN_SECS: u64 = 300;

/// Characters of a response body retained in an error message.
const MAX_ERROR_BODY_CHARS: usize = 512;

/// Maximum page size Aster accepts on `allOrders` and `userTrades` (the default is 500).
pub const ASTER_HISTORY_PAGE_LIMIT: u32 = 1_000;

/// Widest window Aster accepts between `startTime` and `endTime` on the history endpoints.
pub const ASTER_HISTORY_MAX_INTERVAL_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

/// HTTP client for Aster's signed Futures V3 endpoints.
///
/// Cheap to clone; clones share the connection pool, rate limiters, and nonce sequence. The
/// shared nonce sequence matters: Aster tracks nonces per signer address and rejects
/// duplicates, so every request made with the same credentials must draw from one counter.
#[derive(Debug, Clone)]
pub struct AsterHttpClient {
    inner: Arc<AsterHttpClientInner>,
}

#[derive(Debug)]
struct AsterHttpClientInner {
    client: HttpClient,
    base_url: String,
    credential: Option<AsterCredential>,
    nonce: NonceGenerator,
    /// Instant before which no further request may be sent, armed by a `429` or `418`.
    cooldown_until: Mutex<Option<Instant>>,
}

impl AsterHttpClient {
    /// Creates a new [`AsterHttpClient`].
    ///
    /// Passing `credential: None` yields a client that can only reach public endpoints; every
    /// signed call then fails with [`AsterHttpError::MissingCredentials`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be built (for example when the
    /// proxy URL is malformed).
    pub fn new(
        base_url: &str,
        credential: Option<AsterCredential>,
        timeout_secs: Option<u64>,
        proxy_url: Option<String>,
    ) -> AsterHttpResult<Self> {
        let mut headers = HashMap::new();
        headers.insert(USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string());

        let client = HttpClient::builder()
            .headers(headers)
            // Retained from every response so a `429` or `418` can honour the venue's own wait.
            .header_keys(vec![RETRY_AFTER_HEADER.to_string()])
            .keyed_quotas(Self::rate_limit_quotas())
            .maybe_timeout_secs(timeout_secs)
            .maybe_proxy_url(proxy_url)
            .build()?;

        Ok(Self {
            inner: Arc::new(AsterHttpClientInner {
                client,
                base_url: base_url.trim_end_matches('/').to_string(),
                credential,
                nonce: NonceGenerator::new(),
                cooldown_until: Mutex::new(None),
            }),
        })
    }

    /// Returns the keyed quotas enforced by the client.
    ///
    /// Aster publishes a 2400 request-weight/minute and a 1200 order/minute budget. Each
    /// request spends its documented weight from the global key (see
    /// [`Self::rate_limit_keys`]), so these quotas are the venue budgets themselves rather
    /// than a per-request approximation of them.
    fn rate_limit_quotas() -> Vec<(String, Quota)> {
        vec![
            (
                ASTER_GLOBAL_RATE_KEY.to_string(),
                Quota::per_minute(
                    NonZeroU32::new(ASTER_REQUEST_WEIGHT_PER_MINUTE).expect("non-zero quota"),
                ),
            ),
            (
                ASTER_ORDER_RATE_KEY.to_string(),
                Quota::per_minute(
                    NonZeroU32::new(ASTER_ORDERS_PER_MINUTE).expect("non-zero quota"),
                ),
            ),
        ]
    }

    /// Returns the request weight `path` spends from the shared budget.
    ///
    /// The values are the venue's published weights. `openOrders` is the one endpoint whose
    /// weight depends on the request: a single symbol costs 1, while the account-wide listing
    /// costs 40, which is one sixtieth of the whole minute budget.
    fn request_weight(path: &str, params: &AsterParams) -> u32 {
        match path {
            ASTER_ORDER_PATH => ASTER_WEIGHT_ORDER,
            ASTER_ALL_OPEN_ORDERS_PATH => ASTER_WEIGHT_ALL_OPEN_ORDERS,
            ASTER_LISTEN_KEY_PATH => ASTER_WEIGHT_LISTEN_KEY,
            ASTER_OPEN_ORDERS_PATH => {
                if params.get("symbol").is_some() {
                    ASTER_WEIGHT_OPEN_ORDERS_SYMBOL
                } else {
                    ASTER_WEIGHT_OPEN_ORDERS_ALL
                }
            }
            ASTER_ALL_ORDERS_PATH => ASTER_WEIGHT_ALL_ORDERS,
            ASTER_USER_TRADES_PATH => ASTER_WEIGHT_USER_TRADES,
            ASTER_BALANCE_PATH => ASTER_WEIGHT_BALANCE,
            ASTER_POSITION_RISK_PATH => ASTER_WEIGHT_POSITION_RISK,
            ASTER_COMMISSION_RATE_PATH => ASTER_WEIGHT_COMMISSION_RATE,
            ASTER_POSITION_SIDE_DUAL_PATH => ASTER_WEIGHT_POSITION_SIDE_DUAL,
            _ => ASTER_WEIGHT_DEFAULT,
        }
    }

    /// Returns the rate-limit keys charged for one request.
    ///
    /// The global key is repeated once per unit of request weight. The rate limiter plans all
    /// the keys of a request together and commits them atomically, so N repeats reserve N
    /// cells of the 2400/minute budget, or the request waits until all N are available.
    /// Charging one unit per request instead would let 120 `commissionRate` calls (weight 20)
    /// through for the price of 120, spending the venue's entire minute budget.
    fn rate_limit_keys(
        path: &str,
        params: &AsterParams,
        counts_against_order_quota: bool,
    ) -> Vec<String> {
        let weight = Self::request_weight(path, params) as usize;
        let mut keys = Vec::with_capacity(weight + usize::from(counts_against_order_quota));
        keys.extend(std::iter::repeat_n(
            ASTER_GLOBAL_RATE_KEY.to_string(),
            weight,
        ));

        if counts_against_order_quota {
            keys.push(ASTER_ORDER_RATE_KEY.to_string());
        }

        keys
    }

    /// Returns the cooldown a response status demands, if any.
    ///
    /// Only the delta-seconds form of `Retry-After` is honoured; the HTTP-date form, which
    /// Aster has not been observed to send, falls back to the default for the status.
    fn cooldown_for_status(status: u16, retry_after: Option<&str>) -> Option<Duration> {
        let default_secs = match status {
            STATUS_TOO_MANY_REQUESTS => RATE_LIMIT_COOLDOWN_SECS,
            STATUS_IP_BANNED => IP_BAN_COOLDOWN_SECS,
            _ => return None,
        };

        let secs = retry_after
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(default_secs)
            .clamp(1, MAX_COOLDOWN_SECS);

        Some(Duration::from_secs(secs))
    }

    /// Arms the shared cooldown when the venue rate-limited or banned the client.
    ///
    /// Nothing is resent: the failed request is still surfaced to its caller, and only
    /// *subsequent* requests wait. A longer cooldown already armed is never shortened.
    fn arm_cooldown(&self, response: &HttpResponse) {
        let status = response.status.as_u16();
        let retry_after = response.headers.get(RETRY_AFTER_HEADER).map(String::as_str);

        let Some(cooldown) = Self::cooldown_for_status(status, retry_after) else {
            return;
        };

        let deadline = Instant::now() + cooldown;
        let mut guard = self.inner.cooldown_until.lock();

        if guard.is_none_or(|current| deadline > current) {
            *guard = Some(deadline);
            log::warn!(
                "Aster rate limited the client (HTTP {status}), holding requests for {cooldown:?}"
            );
        }
    }

    /// Returns the remaining cooldown, when the venue has asked the client to back off.
    #[must_use]
    pub fn cooldown_remaining(&self) -> Option<Duration> {
        let deadline = (*self.inner.cooldown_until.lock())?;
        let now = Instant::now();

        (deadline > now).then(|| deadline - now)
    }

    /// Waits out any armed cooldown.
    ///
    /// The deadline is copied out and the lock released before awaiting, so the mutex is never
    /// held across a suspension point; a cooldown extended while this task sleeps is picked up
    /// on the next pass.
    async fn await_cooldown(&self) {
        loop {
            let deadline = *self.inner.cooldown_until.lock();

            let Some(deadline) = deadline else {
                return;
            };

            if deadline <= Instant::now() {
                return;
            }

            sleep_until(deadline).await;
        }
    }

    /// Returns the resolved REST base URL.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.inner.base_url
    }

    /// Returns whether signing credentials are configured.
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        self.inner.credential.is_some()
    }

    /// Returns the master account (user) address, when credentials are configured.
    #[must_use]
    pub fn user_address(&self) -> Option<&str> {
        self.inner
            .credential
            .as_ref()
            .map(AsterCredential::user_address)
    }

    /// Returns the API wallet (signer) address, when credentials are configured.
    #[must_use]
    pub fn signer_address(&self) -> Option<&str> {
        self.inner
            .credential
            .as_ref()
            .map(AsterCredential::signer_address)
    }

    /// Builds the signed payload for `params`.
    ///
    /// Prepends the authentication triple in the order Aster's own examples use
    /// (`nonce`, `user`, `signer`) and appends `&signature=0x...`.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials are missing or signing fails.
    pub fn build_signed_payload(&self, params: &AsterParams) -> AsterHttpResult<String> {
        let credential = self
            .inner
            .credential
            .as_ref()
            .ok_or(AsterHttpError::MissingCredentials)?;

        let mut signed = Vec::with_capacity(params.len() + 3);
        signed.push(("nonce".to_string(), self.inner.nonce.next().to_string()));
        signed.push(("user".to_string(), credential.user_address().to_string()));
        signed.push((
            "signer".to_string(),
            credential.signer_address().to_string(),
        ));
        signed.extend(params.entries().iter().cloned());

        credential
            .signer()
            .sign_params(&signed)
            .map_err(|e| AsterHttpError::SigningError(e.to_string()))
    }

    /// Sends a signed `GET` request and deserializes the response.
    ///
    /// # Errors
    ///
    /// Returns an error if signing, transport, or deserialization fails, or if Aster returns
    /// an error payload.
    pub async fn signed_get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: AsterParams,
    ) -> AsterHttpResult<T> {
        self.signed_request(Method::GET, path, params, false).await
    }

    /// Sends a signed `POST` request and deserializes the response.
    ///
    /// # Errors
    ///
    /// Returns an error if signing, transport, or deserialization fails, or if Aster returns
    /// an error payload.
    pub async fn signed_post<T: DeserializeOwned>(
        &self,
        path: &str,
        params: AsterParams,
        counts_against_order_quota: bool,
    ) -> AsterHttpResult<T> {
        self.signed_request(Method::POST, path, params, counts_against_order_quota)
            .await
    }

    /// Sends a signed `PUT` request and deserializes the response.
    ///
    /// # Errors
    ///
    /// Returns an error if signing, transport, or deserialization fails, or if Aster returns
    /// an error payload.
    pub async fn signed_put<T: DeserializeOwned>(
        &self,
        path: &str,
        params: AsterParams,
    ) -> AsterHttpResult<T> {
        self.signed_request(Method::PUT, path, params, false).await
    }

    /// Sends a signed `DELETE` request and deserializes the response.
    ///
    /// # Errors
    ///
    /// Returns an error if signing, transport, or deserialization fails, or if Aster returns
    /// an error payload.
    pub async fn signed_delete<T: DeserializeOwned>(
        &self,
        path: &str,
        params: AsterParams,
        counts_against_order_quota: bool,
    ) -> AsterHttpResult<T> {
        self.signed_request(Method::DELETE, path, params, counts_against_order_quota)
            .await
    }

    /// Dispatches a signed request, retrying idempotent `GET`s on transport faults.
    ///
    /// `GET` is the only method repeated. Order submission and cancellation are not idempotent:
    /// a transport failure there is ambiguous (the order may already be on the book), so it is
    /// surfaced to the execution client, which resolves it through reconciliation.
    async fn signed_request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: AsterParams,
        counts_against_order_quota: bool,
    ) -> AsterHttpResult<T> {
        if method != Method::GET {
            return self
                .send_signed(method, path, params, counts_against_order_quota, None)
                .await;
        }

        // Each attempt rebuilds and re-signs the payload, so it draws a fresh nonce; Aster
        // rejects a replayed nonce for a signer address.
        let operation = || {
            let params = params.clone();
            async move {
                self.send_signed(Method::GET, path, params, false, None)
                    .await
            }
        };

        Self::get_retry_manager()
            .execute_with_retry(
                path,
                operation,
                AsterHttpError::is_retryable_transport,
                // Only reached for retry-control failures; an exhausted budget returns the last
                // transport error verbatim, which is what the caller needs to see.
                |e| AsterHttpError::NetworkError(format!("GET {path} retry failed: {e}")),
            )
            .await
    }

    /// Returns the retry policy applied to idempotent `GET` requests.
    ///
    /// Up to `GET_MAX_RETRIES` repeats after the first attempt, with a fixed 500 ms / 1 s / 2 s
    /// backoff (no jitter: this is a single client, not a fleet that needs de-synchronising).
    /// The per-attempt timeout is left to the HTTP client's own configured request timeout.
    fn get_retry_manager() -> RetryManager<AsterHttpError> {
        RetryManager::new(RetryConfig {
            max_retries: GET_MAX_RETRIES,
            initial_delay_ms: GET_RETRY_INITIAL_DELAY_MS,
            max_delay_ms: GET_RETRY_MAX_DELAY_MS,
            backoff_factor: 2.0,
            jitter_ms: 0,
            operation_timeout_ms: None,
            immediate_first: false,
            max_elapsed_ms: None,
        })
    }

    async fn send_signed<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: AsterParams,
        counts_against_order_quota: bool,
        admission: Option<&(dyn Fn() -> Result<(), String> + Send + Sync)>,
    ) -> AsterHttpResult<T> {
        // Waited out before signing: a nonce drawn now and sent minutes later would be stale,
        // and Aster rejects a stale nonce.
        self.await_cooldown().await;

        // The send boundary: a request that queued behind a cooldown is re-checked here, after
        // the wait and before anything is signed, so a session that lost its execution
        // readiness while it waited does not reach the venue.
        if let Some(admission) = admission {
            admission().map_err(AsterHttpError::ValidationError)?;
        }

        let payload = self.build_signed_payload(&params)?;
        let is_get = method == Method::GET;

        let url = if is_get {
            format!("{}{path}?{payload}", self.inner.base_url)
        } else {
            format!("{}{path}", self.inner.base_url)
        };

        let mut headers = HashMap::new();
        let body = if is_get {
            None
        } else {
            headers.insert("Content-Type".to_string(), FORM_CONTENT_TYPE.to_string());
            Some(payload.into_bytes())
        };

        let keys = Self::rate_limit_keys(path, &params, counts_against_order_quota);

        // The signed payload carries the signature, so the URL is redacted from logs and
        // transport errors for GET requests.
        let response = self
            .inner
            .client
            .request_with_url_redacted(method, url, None, Some(headers), body, None, Some(keys))
            .await?;

        self.arm_cooldown(&response);

        Self::deserialize_response(&response)
    }

    fn deserialize_response<T: DeserializeOwned>(response: &HttpResponse) -> AsterHttpResult<T> {
        if !response.status.is_success() {
            return Err(Self::parse_error_response(response));
        }

        // Aster answers some failures with HTTP 200 and an error body.
        if let Ok(error) = serde_json::from_slice::<AsterErrorResponse>(&response.body)
            && error.code < 0
        {
            return Err(AsterHttpError::AsterError {
                code: error.code,
                message: error.msg,
                // A `2xx` carrying an error body: the status adds nothing, the code decides.
                status: None,
            });
        }

        serde_json::from_slice::<T>(&response.body).map_err(|e| {
            AsterHttpError::JsonError(format!("{e}: {}", truncate_body(&response.body)))
        })
    }

    fn parse_error_response(response: &HttpResponse) -> AsterHttpError {
        if let Ok(error) = serde_json::from_slice::<AsterErrorResponse>(&response.body) {
            // The status travels with the code: a parseable body under `503` still means the
            // execution status is unknown, and classifying on the code alone would terminalise
            // an order that may be live.
            return AsterHttpError::AsterError {
                code: error.code,
                message: error.msg,
                status: Some(response.status.as_u16()),
            };
        }

        AsterHttpError::UnexpectedStatus {
            status: response.status.as_u16(),
            body: truncate_body(&response.body),
        }
    }

    // ----------------------------------------------------------------------------------------
    // Trading
    // ----------------------------------------------------------------------------------------

    /// Submits a new order (`POST /fapi/v3/order`).
    ///
    /// # Errors
    ///
    /// Returns an error if the venue rejects the order or the request fails.
    pub async fn submit_order(&self, params: AsterParams) -> AsterHttpResult<AsterOrder> {
        self.signed_post(ASTER_ORDER_PATH, params, true).await
    }

    /// Submits an order, re-checking `admission` at the send boundary.
    ///
    /// The check runs after any venue cooldown has been waited out and before the request is
    /// signed, so a session that lost its execution readiness while the request was queued
    /// cannot reach the venue. A refusal is a local [`AsterHttpError::ValidationError`]: the
    /// order provably never left this process and must not be reconciled as a venue outcome.
    ///
    /// # Errors
    ///
    /// Returns an error if the admission check refuses the request, signing fails, or the
    /// request fails.
    pub async fn submit_order_admitted<F>(
        &self,
        params: AsterParams,
        admission: F,
    ) -> AsterHttpResult<AsterOrder>
    where
        F: Fn() -> Result<(), String> + Send + Sync,
    {
        self.send_signed(
            Method::POST,
            ASTER_ORDER_PATH,
            params,
            true,
            Some(&admission),
        )
        .await
    }

    /// Cancels a single order (`DELETE /fapi/v3/order`).
    ///
    /// Exactly one of `order_id` / `orig_client_order_id` must be supplied.
    ///
    /// # Errors
    ///
    /// Returns an error if the order is unknown or the request fails.
    pub async fn cancel_order(
        &self,
        symbol: &str,
        order_id: Option<i64>,
        orig_client_order_id: Option<&str>,
    ) -> AsterHttpResult<AsterOrder> {
        if order_id.is_none() && orig_client_order_id.is_none() {
            return Err(AsterHttpError::ValidationError(
                "cancel_order requires either order_id or orig_client_order_id".to_string(),
            ));
        }

        let params = AsterParams::new()
            .with("symbol", symbol)
            .with_opt("orderId", order_id)
            .with_opt("origClientOrderId", orig_client_order_id);

        self.signed_delete(ASTER_ORDER_PATH, params, true).await
    }

    /// Cancels every open order for a symbol (`DELETE /fapi/v3/allOpenOrders`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn cancel_all_orders(
        &self,
        symbol: &str,
    ) -> AsterHttpResult<AsterCancelAllOrdersResponse> {
        let params = AsterParams::new().with("symbol", symbol);
        self.signed_delete(ASTER_ALL_OPEN_ORDERS_PATH, params, true)
            .await
    }

    // ----------------------------------------------------------------------------------------
    // Account and order queries
    // ----------------------------------------------------------------------------------------

    /// Queries a single order (`GET /fapi/v3/order`).
    ///
    /// # Errors
    ///
    /// Returns an error if the order is unknown or the request fails.
    pub async fn query_order(
        &self,
        symbol: &str,
        order_id: Option<i64>,
        orig_client_order_id: Option<&str>,
    ) -> AsterHttpResult<AsterOrder> {
        if order_id.is_none() && orig_client_order_id.is_none() {
            return Err(AsterHttpError::ValidationError(
                "query_order requires either order_id or orig_client_order_id".to_string(),
            ));
        }

        let params = AsterParams::new()
            .with("symbol", symbol)
            .with_opt("orderId", order_id)
            .with_opt("origClientOrderId", orig_client_order_id);

        self.signed_get(ASTER_ORDER_PATH, params).await
    }

    /// Queries open orders (`GET /fapi/v3/openOrders`), optionally for one symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn query_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> AsterHttpResult<Vec<AsterOrder>> {
        let params = AsterParams::new().with_opt("symbol", symbol);
        self.signed_get(ASTER_OPEN_ORDERS_PATH, params).await
    }

    /// Queries historical orders for a symbol (`GET /fapi/v3/allOrders`).
    ///
    /// `order_id` is the venue's pagination cursor: the response then starts at that order ID.
    /// Aster does not accept it together with a time window, so passing both is rejected
    /// locally rather than sent.
    ///
    /// # Errors
    ///
    /// Returns an error if `order_id` is combined with `start_time_ms` or `end_time_ms`, or if
    /// the request fails.
    pub async fn query_all_orders(
        &self,
        symbol: &str,
        start_time_ms: Option<i64>,
        end_time_ms: Option<i64>,
        order_id: Option<i64>,
        limit: Option<u32>,
    ) -> AsterHttpResult<Vec<AsterOrder>> {
        if order_id.is_some() && (start_time_ms.is_some() || end_time_ms.is_some()) {
            return Err(AsterHttpError::ValidationError(
                "allOrders does not accept `orderId` together with `startTime`/`endTime`"
                    .to_string(),
            ));
        }

        let params = AsterParams::new()
            .with("symbol", symbol)
            .with_opt("orderId", order_id)
            .with_opt("startTime", start_time_ms)
            .with_opt("endTime", end_time_ms)
            .with_opt("limit", limit);

        self.signed_get(ASTER_ALL_ORDERS_PATH, params).await
    }

    /// Queries per-asset futures balances (`GET /fapi/v3/balance`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn query_balances(&self) -> AsterHttpResult<Vec<AsterBalance>> {
        self.signed_get(ASTER_BALANCE_PATH, AsterParams::new())
            .await
    }

    /// Queries position risk (`GET /fapi/v3/positionRisk`), optionally for one symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn query_position_risk(
        &self,
        symbol: Option<&str>,
    ) -> AsterHttpResult<Vec<AsterPositionRisk>> {
        let params = AsterParams::new().with_opt("symbol", symbol);
        self.signed_get(ASTER_POSITION_RISK_PATH, params).await
    }

    /// Queries the account's own trades (`GET /fapi/v3/userTrades`).
    ///
    /// `from_id` is the venue's pagination cursor: the response then starts at that trade ID.
    /// Aster does not accept it together with a time window, so passing both is rejected
    /// locally rather than sent.
    ///
    /// # Errors
    ///
    /// Returns an error if `from_id` is combined with `start_time_ms` or `end_time_ms`, or if
    /// the request fails.
    pub async fn query_user_trades(
        &self,
        symbol: &str,
        start_time_ms: Option<i64>,
        end_time_ms: Option<i64>,
        from_id: Option<i64>,
        limit: Option<u32>,
    ) -> AsterHttpResult<Vec<AsterUserTrade>> {
        if from_id.is_some() && (start_time_ms.is_some() || end_time_ms.is_some()) {
            return Err(AsterHttpError::ValidationError(
                "userTrades does not accept `fromId` together with `startTime`/`endTime`"
                    .to_string(),
            ));
        }

        let params = AsterParams::new()
            .with("symbol", symbol)
            .with_opt("startTime", start_time_ms)
            .with_opt("endTime", end_time_ms)
            .with_opt("fromId", from_id)
            .with_opt("limit", limit);

        self.signed_get(ASTER_USER_TRADES_PATH, params).await
    }

    /// Queries the account's commission rate for a symbol (`GET /fapi/v3/commissionRate`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn query_commission_rate(
        &self,
        symbol: &str,
    ) -> AsterHttpResult<AsterCommissionRate> {
        let params = AsterParams::new().with("symbol", symbol);
        self.signed_get(ASTER_COMMISSION_RATE_PATH, params).await
    }

    /// Queries whether the account runs in hedge (dual-side) mode.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn query_position_mode(&self) -> AsterHttpResult<AsterPositionModeResponse> {
        self.signed_get(ASTER_POSITION_SIDE_DUAL_PATH, AsterParams::new())
            .await
    }

    // ----------------------------------------------------------------------------------------
    // User data stream
    // ----------------------------------------------------------------------------------------

    /// Creates a user data stream listen key (`POST /fapi/v3/listenKey`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn create_listen_key(&self) -> AsterHttpResult<String> {
        let response: AsterListenKeyResponse = self
            .signed_post(ASTER_LISTEN_KEY_PATH, AsterParams::new(), false)
            .await?;
        Ok(response.listen_key)
    }

    /// Renews the current listen key (`PUT /fapi/v3/listenKey`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn keepalive_listen_key(&self) -> AsterHttpResult<()> {
        let _: serde_json::Value = self
            .signed_put(ASTER_LISTEN_KEY_PATH, AsterParams::new())
            .await?;
        Ok(())
    }

    /// Closes the current listen key (`DELETE /fapi/v3/listenKey`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn close_listen_key(&self) -> AsterHttpResult<()> {
        let _: serde_json::Value = self
            .signed_delete(ASTER_LISTEN_KEY_PATH, AsterParams::new(), false)
            .await?;
        Ok(())
    }
}

/// Renders a response body for an error message, bounded to [`MAX_ERROR_BODY_CHARS`].
///
/// An error body is foreign input of unbounded size (an edge proxy can answer with an entire
/// HTML page), and the rendered error reaches the logs and the engine's error events.
fn truncate_body(body: &[u8]) -> String {
    String::from_utf8_lossy(body)
        .chars()
        .take(MAX_ERROR_BODY_CHARS)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Router,
        http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
        response::IntoResponse,
    };
    use nautilus_network::ratelimiter::{RateLimiter, clock::MonotonicClock};
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::common::enums::AsterEnvironment;

    /// One scripted response from the stand-in venue.
    #[derive(Clone, Debug)]
    struct ScriptedResponse {
        status: u16,
        retry_after: Option<String>,
        body: String,
    }

    /// A stand-in venue answering scripted responses, counting every request it received.
    ///
    /// Requests beyond the script are answered with an empty `200` body.
    #[derive(Clone)]
    struct MockVenue {
        addr: SocketAddr,
        hits: Arc<AtomicUsize>,
    }

    impl MockVenue {
        async fn start(responses: Vec<ScriptedResponse>) -> Self {
            let hits = Arc::new(AtomicUsize::new(0));
            let state = (hits.clone(), Arc::new(responses));

            let router = Router::new().fallback(move || {
                let (hits, script) = state.clone();

                async move {
                    let index = hits.fetch_add(1, Ordering::SeqCst);

                    match script.get(index) {
                        Some(response) => {
                            let mut headers = HeaderMap::new();
                            if let Some(retry_after) = response.retry_after.as_deref() {
                                headers.insert(
                                    HeaderName::from_static(RETRY_AFTER_HEADER),
                                    HeaderValue::from_str(retry_after).expect("header value"),
                                );
                            }
                            (
                                StatusCode::from_u16(response.status).expect("status"),
                                headers,
                                response.body.clone(),
                            )
                                .into_response()
                        }
                        None => (StatusCode::OK, "{}").into_response(),
                    }
                }
            });

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("local addr");

            tokio::spawn(async move {
                let _ = axum::serve(listener, router.into_make_service()).await;
            });

            Self { addr, hits }
        }

        fn url(&self) -> String {
            format!("http://{}", self.addr)
        }

        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
    }

    /// Returns a rate limiter carrying the client's own quotas.
    fn venue_rate_limiter() -> RateLimiter<Ustr, MonotonicClock> {
        RateLimiter::new_with_quota(
            None,
            AsterHttpClient::rate_limit_quotas()
                .into_iter()
                .map(|(key, quota)| (Ustr::from(&key), quota))
                .collect(),
        )
    }

    fn interned_keys(
        path: &str,
        params: &AsterParams,
        counts_against_order_quota: bool,
    ) -> Vec<Ustr> {
        AsterHttpClient::rate_limit_keys(path, params, counts_against_order_quota)
            .iter()
            .map(|key| Ustr::from(key))
            .collect()
    }

    /// Test-only key published in CCXT's static request fixtures; holds no funds.
    const TEST_PRIVATE_KEY: &str =
        "0xff3bdd43534543d421f05aec535965b5050ad6ac15345435345435453495e771";
    const TEST_ADDRESS: &str = "0xb67f9a782d3678a0bac50c22eacbb4924fe9d4cf";

    fn credential() -> AsterCredential {
        AsterCredential::resolve_with_env(
            Some(TEST_PRIVATE_KEY),
            None,
            None,
            AsterEnvironment::Mainnet,
            |_| None,
        )
        .unwrap()
    }

    fn client() -> AsterHttpClient {
        AsterHttpClient::new(
            "https://fapi.asterdex.com/",
            Some(credential()),
            Some(30),
            None,
        )
        .unwrap()
    }

    /// Drives the `GET` retry policy over a scripted sequence of outcomes, returning the
    /// final result and the number of attempts made.
    async fn run_get_retry(mut outcomes: Vec<AsterHttpResult<u8>>) -> (AsterHttpResult<u8>, usize) {
        outcomes.reverse();
        let outcomes = std::cell::RefCell::new(outcomes);
        let attempts = std::cell::Cell::new(0usize);

        let result = AsterHttpClient::get_retry_manager()
            .execute_with_retry(
                "test",
                || async {
                    attempts.set(attempts.get() + 1);
                    outcomes
                        .borrow_mut()
                        .pop()
                        .unwrap_or(Err(AsterHttpError::NetworkError(
                            "tls handshake eof".into(),
                        )))
                },
                AsterHttpError::is_retryable_transport,
                |e| AsterHttpError::NetworkError(format!("retry failed: {e}")),
            )
            .await;

        (result, attempts.get())
    }

    #[tokio::test(start_paused = true)]
    async fn test_get_retries_transport_faults_then_succeeds() {
        // The first live testnet run saw exactly this: a TLS handshake EOF followed by a TCP
        // connect timeout through the host proxy, then a good response.
        let (result, attempts) = run_get_retry(vec![
            Err(AsterHttpError::NetworkError("tls handshake eof".into())),
            Err(AsterHttpError::Timeout("tcp connect error 10060".into())),
            Ok(7),
        ])
        .await;

        assert_eq!(result.unwrap(), 7);
        assert_eq!(attempts, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_get_retry_is_bounded() {
        let (result, attempts) = run_get_retry(Vec::new()).await;

        // One initial attempt plus GET_MAX_RETRIES repeats, then the last transport error is
        // surfaced verbatim rather than being wrapped.
        assert_eq!(attempts, GET_MAX_RETRIES as usize + 1);
        let error = result.unwrap_err();
        assert!(error.is_retryable_transport());
        assert!(error.to_string().contains("tls handshake eof"), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn test_get_does_not_retry_venue_error_bodies() {
        // `-1121 Invalid symbol` is a definitive answer: testnet does not list NVDAUSDT.
        let (result, attempts) = run_get_retry(vec![Err(AsterHttpError::AsterError {
            code: -1121,
            message: "Invalid symbol.".to_string(),
            status: Some(400),
        })])
        .await;

        assert_eq!(attempts, 1);
        assert_eq!(result.unwrap_err().code(), Some(-1121));
    }

    #[rstest]
    fn test_new_trims_trailing_slash_from_base_url() {
        assert_eq!(client().base_url(), "https://fapi.asterdex.com");
    }

    #[rstest]
    fn test_credential_accessors() {
        let client = client();

        assert!(client.has_credentials());
        assert_eq!(client.user_address(), Some(TEST_ADDRESS));
        assert_eq!(client.signer_address(), Some(TEST_ADDRESS));
    }

    #[rstest]
    fn test_unauthenticated_client_cannot_sign() {
        let client = AsterHttpClient::new("https://fapi.asterdex.com", None, None, None).unwrap();

        assert!(!client.has_credentials());
        assert_eq!(client.user_address(), None);

        let error = client
            .build_signed_payload(&AsterParams::new())
            .unwrap_err();
        assert!(matches!(error, AsterHttpError::MissingCredentials));
    }

    #[rstest]
    fn test_signed_payload_prefixes_auth_triple_in_order() {
        let params = AsterParams::new()
            .with("symbol", "BTCUSDT")
            .with("side", "BUY");

        let payload = client().build_signed_payload(&params).unwrap();

        let (prefix, signature) = payload.rsplit_once("&signature=").unwrap();
        let keys: Vec<&str> = prefix
            .split('&')
            .map(|pair| pair.split_once('=').unwrap().0)
            .collect();

        assert_eq!(keys, ["nonce", "user", "signer", "symbol", "side"]);
        assert!(signature.starts_with("0x"));
        assert_eq!(signature.len(), 132);
        assert!(prefix.contains(&format!("user={TEST_ADDRESS}")));
        assert!(prefix.contains(&format!("signer={TEST_ADDRESS}")));
    }

    #[rstest]
    fn test_signed_payload_signature_verifies_against_the_signed_prefix() {
        let payload = client()
            .build_signed_payload(&AsterParams::new().with("symbol", "BTCUSDT"))
            .unwrap();
        let (prefix, signature) = payload.rsplit_once("&signature=").unwrap();

        let expected = credential().signer().sign_param_string(prefix).unwrap();

        assert_eq!(signature, expected);
    }

    #[rstest]
    fn test_signed_payload_nonce_is_strictly_increasing_across_clones() {
        let client = client();
        let clone = client.clone();

        let nonce_of = |payload: &str| -> u64 {
            payload
                .split('&')
                .find_map(|pair| pair.strip_prefix("nonce="))
                .unwrap()
                .parse()
                .unwrap()
        };

        let first = nonce_of(&client.build_signed_payload(&AsterParams::new()).unwrap());
        let second = nonce_of(&clone.build_signed_payload(&AsterParams::new()).unwrap());
        let third = nonce_of(&client.build_signed_payload(&AsterParams::new()).unwrap());

        assert!(second > first, "{second} !> {first}");
        assert!(third > second, "{third} !> {second}");
    }

    #[rstest]
    #[tokio::test]
    async fn test_cancel_order_requires_an_identifier() {
        let error = client()
            .cancel_order("BTCUSDT", None, None)
            .await
            .unwrap_err();

        assert!(matches!(error, AsterHttpError::ValidationError(_)));
        assert!(error.to_string().contains("order_id"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_order_requires_an_identifier() {
        let error = client()
            .query_order("BTCUSDT", None, None)
            .await
            .unwrap_err();

        assert!(matches!(error, AsterHttpError::ValidationError(_)));
    }

    #[rstest]
    #[tokio::test]
    async fn test_all_orders_rejects_cursor_combined_with_a_time_window() {
        // Aster documents `orderId` as mutually exclusive with `startTime`/`endTime`.
        let error = client()
            .query_all_orders("BTCUSDT", Some(1), None, Some(7), None)
            .await
            .unwrap_err();

        assert!(matches!(error, AsterHttpError::ValidationError(_)));
        assert!(error.to_string().contains("orderId"), "{error}");
    }

    #[rstest]
    #[tokio::test]
    async fn test_user_trades_rejects_cursor_combined_with_a_time_window() {
        let error = client()
            .query_user_trades("BTCUSDT", None, Some(2), Some(7), None)
            .await
            .unwrap_err();

        assert!(matches!(error, AsterHttpError::ValidationError(_)));
        assert!(error.to_string().contains("fromId"), "{error}");
    }

    #[rstest]
    fn test_history_page_limits_match_the_venue_documentation() {
        assert_eq!(ASTER_HISTORY_PAGE_LIMIT, 1_000);
        assert_eq!(ASTER_HISTORY_MAX_INTERVAL_MS, 604_800_000);
    }

    #[rstest]
    #[case(ASTER_ORDER_PATH, ASTER_WEIGHT_ORDER)]
    #[case(ASTER_ALL_OPEN_ORDERS_PATH, ASTER_WEIGHT_ALL_OPEN_ORDERS)]
    #[case(ASTER_LISTEN_KEY_PATH, ASTER_WEIGHT_LISTEN_KEY)]
    #[case(ASTER_ALL_ORDERS_PATH, ASTER_WEIGHT_ALL_ORDERS)]
    #[case(ASTER_USER_TRADES_PATH, ASTER_WEIGHT_USER_TRADES)]
    #[case(ASTER_BALANCE_PATH, ASTER_WEIGHT_BALANCE)]
    #[case(ASTER_POSITION_RISK_PATH, ASTER_WEIGHT_POSITION_RISK)]
    #[case(ASTER_COMMISSION_RATE_PATH, ASTER_WEIGHT_COMMISSION_RATE)]
    #[case(ASTER_POSITION_SIDE_DUAL_PATH, ASTER_WEIGHT_POSITION_SIDE_DUAL)]
    #[case("/fapi/v3/somethingNew", ASTER_WEIGHT_DEFAULT)]
    fn test_request_weight_matches_the_documented_table(#[case] path: &str, #[case] expected: u32) {
        let params = AsterParams::new().with("symbol", "BTCUSDT");

        assert_eq!(AsterHttpClient::request_weight(path, &params), expected);
    }

    #[rstest]
    fn test_open_orders_weight_depends_on_the_symbol_filter() {
        // Aster charges 40 for the account-wide listing and 1 when a symbol narrows it.
        let all = AsterParams::new();
        let one = AsterParams::new().with("symbol", "BTCUSDT");

        assert_eq!(
            AsterHttpClient::request_weight(ASTER_OPEN_ORDERS_PATH, &all),
            ASTER_WEIGHT_OPEN_ORDERS_ALL
        );
        assert_eq!(
            AsterHttpClient::request_weight(ASTER_OPEN_ORDERS_PATH, &one),
            ASTER_WEIGHT_OPEN_ORDERS_SYMBOL
        );
    }

    #[rstest]
    fn test_rate_limit_keys_repeat_the_global_key_once_per_weight_unit() {
        let params = AsterParams::new().with("symbol", "BTCUSDT");
        let heavy = AsterHttpClient::rate_limit_keys(ASTER_COMMISSION_RATE_PATH, &params, false);

        assert_eq!(heavy.len(), ASTER_WEIGHT_COMMISSION_RATE as usize);
        assert!(heavy.iter().all(|key| key == ASTER_GLOBAL_RATE_KEY));

        let order = AsterHttpClient::rate_limit_keys(ASTER_ORDER_PATH, &params, true);

        assert_eq!(
            order,
            vec![
                ASTER_GLOBAL_RATE_KEY.to_string(),
                ASTER_ORDER_RATE_KEY.to_string()
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_heavy_endpoint_exhausts_the_weight_budget_sooner_than_a_light_one() {
        // 2400 weight per minute: 120 `commissionRate` calls (weight 20) spend the whole
        // budget, so the 121st has to wait, while 121 order queries (weight 1) do not.
        let params = AsterParams::new().with("symbol", "BTCUSDT");
        let heavy = interned_keys(ASTER_COMMISSION_RATE_PATH, &params, false);
        let light = interned_keys(ASTER_ORDER_PATH, &params, false);

        let limiter = venue_rate_limiter();
        let start = Instant::now();
        for _ in 0..120 {
            limiter.await_keys_ready(Some(&heavy)).await;
        }

        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "the budget covers 120 calls"
        );

        limiter.await_keys_ready(Some(&heavy)).await;

        assert!(start.elapsed() > Duration::ZERO, "the 121st call must wait");

        let limiter = venue_rate_limiter();
        let start = Instant::now();
        for _ in 0..121 {
            limiter.await_keys_ready(Some(&light)).await;
        }

        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[rstest]
    #[case(429, None, Some(Duration::from_secs(RATE_LIMIT_COOLDOWN_SECS)))]
    #[case(418, None, Some(Duration::from_secs(IP_BAN_COOLDOWN_SECS)))]
    #[case(429, Some("3"), Some(Duration::from_secs(3)))]
    #[case(418, Some(" 45 "), Some(Duration::from_secs(45)))]
    // An HTTP-date `Retry-After` is not parsed; the default for the status applies.
    #[case(
        429,
        Some("Wed, 21 Oct 2026 07:28:00 GMT"),
        Some(Duration::from_secs(RATE_LIMIT_COOLDOWN_SECS))
    )]
    // A multi-day ban is capped so the client cannot silently sleep out the session.
    #[case(418, Some("259200"), Some(Duration::from_secs(MAX_COOLDOWN_SECS)))]
    #[case(429, Some("0"), Some(Duration::from_secs(1)))]
    #[case(200, None, None)]
    #[case(400, Some("5"), None)]
    #[case(503, None, None)]
    fn test_cooldown_for_status(
        #[case] status: u16,
        #[case] retry_after: Option<&str>,
        #[case] expected: Option<Duration>,
    ) {
        assert_eq!(
            AsterHttpClient::cooldown_for_status(status, retry_after),
            expected
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_await_cooldown_waits_for_the_armed_deadline() {
        let client = client();

        assert_eq!(client.cooldown_remaining(), None);

        let deadline = Instant::now() + Duration::from_secs(30);
        *client.inner.cooldown_until.lock() = Some(deadline);

        let start = Instant::now();
        client.await_cooldown().await;

        assert!(start.elapsed() >= Duration::from_secs(30));
        assert_eq!(client.cooldown_remaining(), None);
    }

    #[tokio::test]
    async fn test_rate_limited_response_holds_later_requests_without_resending() {
        let venue = MockVenue::start(vec![ScriptedResponse {
            status: 429,
            retry_after: Some("2".to_string()),
            body: r#"{"code":-1003,"msg":"Too many requests."}"#.to_string(),
        }])
        .await;
        let client = AsterHttpClient::new(&venue.url(), Some(credential()), Some(5), None).unwrap();

        let error = client
            .submit_order(AsterParams::new().with("symbol", "BTCUSDT"))
            .await
            .unwrap_err();

        assert!(error.is_rate_limited(), "{error}");
        assert_eq!(venue.hits(), 1, "a rate-limited POST must never be resent");

        let remaining = client.cooldown_remaining().expect("cooldown armed");
        assert!(remaining > Duration::from_millis(1_500), "{remaining:?}");

        // The next request waits out the cooldown rather than being sent into the ban.
        let outcome =
            tokio::time::timeout(Duration::from_millis(250), client.query_balances()).await;

        assert!(outcome.is_err(), "a request was sent during the cooldown");
        assert_eq!(venue.hits(), 1);
    }

    #[tokio::test]
    async fn test_unexpected_status_body_is_truncated() {
        let venue = MockVenue::start(vec![ScriptedResponse {
            status: 502,
            retry_after: None,
            body: "x".repeat(2_000),
        }])
        .await;
        let client = AsterHttpClient::new(&venue.url(), Some(credential()), Some(5), None).unwrap();

        let error = client.query_balances().await.unwrap_err();

        match error {
            AsterHttpError::UnexpectedStatus { status, body } => {
                assert_eq!(status, 502);
                assert_eq!(body.chars().count(), MAX_ERROR_BODY_CHARS);
            }
            other => panic!("expected an unexpected-status error, was {other}"),
        }
        assert_eq!(venue.hits(), 1, "a 502 is not a transport fault to retry");
    }

    #[rstest]
    fn test_rate_limit_quotas_cover_requests_and_orders() {
        let quotas = AsterHttpClient::rate_limit_quotas();
        let keys: Vec<&str> = quotas.iter().map(|(k, _)| k.as_str()).collect();

        assert_eq!(keys, [ASTER_GLOBAL_RATE_KEY, ASTER_ORDER_RATE_KEY]);
        assert_eq!(
            quotas[0].1.burst_size().get(),
            ASTER_REQUEST_WEIGHT_PER_MINUTE
        );
        assert_eq!(quotas[1].1.burst_size().get(), ASTER_ORDERS_PER_MINUTE);
    }
}
