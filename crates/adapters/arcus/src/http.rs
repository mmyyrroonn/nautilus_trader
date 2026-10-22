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

//! Bounded asynchronous REST transport. Mutating requests are never retried.

use std::{
    collections::HashSet,
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, Method, Url};
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::{
    models::*,
    signing::{ArcusSigner, validate_address},
};

const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PAGES: usize = 64;
const PAGE_SIZE: usize = 1_000;

#[derive(Debug, thiserror::Error)]
/// Distinguishes definite rejections from writes whose outcome requires reconciliation.
pub enum ArcusHttpError {
    #[error("Arcus request rejected ({status}): {message}")]
    Rejected { status: u16, message: String },
    #[error("Arcus rate limited; retry after {retry_after_ms} ms ({reason})")]
    RateLimited { retry_after_ms: u64, reason: String },
    #[error("Arcus write outcome is unknown: {0}")]
    UnknownOutcome(String),
    #[error("Arcus transport failed: {0}")]
    Transport(String),
    #[error("Arcus response decoding failed: {0}")]
    Decode(String),
    #[error("Arcus request validation failed: {0}")]
    Validation(String),
    #[error("Arcus reconciliation incomplete: {0}")]
    Incomplete(String),
}

/// Result returned by the Arcus REST transport.
pub type ArcusHttpResult<T> = Result<T, ArcusHttpError>;

#[derive(Debug)]
struct ReadBudget {
    tokens: f64,
    updated: tokio::time::Instant,
}

// Arcus keys read budgets by source IP, not by account or client instance
static PROCESS_READ_BUDGET: LazyLock<Arc<tokio::sync::Mutex<ReadBudget>>> = LazyLock::new(|| {
    Arc::new(tokio::sync::Mutex::new(ReadBudget {
        tokens: 1_500.0,
        updated: tokio::time::Instant::now(),
    }))
});

#[derive(Clone, Debug)]
/// Shares a connection pool and process-wide read budget across cloned clients.
pub struct ArcusHttpClient {
    client: Client,
    base_url: Url,
    signer: Option<Arc<ArcusSigner>>,
    read_budget: Arc<tokio::sync::Mutex<ReadBudget>>,
}

impl ArcusHttpClient {
    /// Creates a bounded HTTP client with redirects and automatic retries disabled.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid URLs, nonpositive timeouts, or transport initialization.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Adapter factories pass owned resolved URL configuration"
    )]
    pub fn new(
        base_url: String,
        timeout_secs: u64,
        signer: Option<Arc<ArcusSigner>>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(timeout_secs > 0, "Arcus HTTP timeout must be positive");
        let base_url = Url::parse(base_url.trim_end_matches('/'))?;
        anyhow::ensure!(
            matches!(base_url.scheme(), "https" | "http"),
            "Invalid Arcus HTTP URL scheme"
        );
        anyhow::ensure!(
            base_url.username().is_empty() && base_url.password().is_none(),
            "Arcus URL must not contain credentials"
        );
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .connect_timeout(Duration::from_secs(timeout_secs.min(10)))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        Ok(Self {
            client,
            base_url,
            signer,
            read_budget: Arc::clone(&PROCESS_READ_BUDGET),
        })
    }

    async fn reserve_read_weight(&self, mut weight: u32) {
        loop {
            let portion = weight.min(1_500);
            let delay = {
                let mut budget = self.read_budget.lock().await;
                let now = tokio::time::Instant::now();
                budget.tokens = (budget.tokens
                    + now.duration_since(budget.updated).as_secs_f64() * 25.0)
                    .min(1_500.0);
                budget.updated = now;

                if budget.tokens >= f64::from(portion) {
                    budget.tokens -= f64::from(portion);
                    weight -= portion;

                    if weight == 0 {
                        return;
                    }
                    continue;
                }
                Duration::from_secs_f64((f64::from(portion) - budget.tokens) / 25.0)
            };
            tokio::time::sleep(delay).await;
        }
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        body: Option<serde_json::Value>,
        signature: Option<(u64, String)>,
        weight: u32,
    ) -> ArcusHttpResult<T> {
        let is_write = method != Method::GET;

        if !is_write {
            self.reserve_read_weight(weight).await;
        }
        let url = self
            .base_url
            .join(path)
            .map_err(|e| ArcusHttpError::Validation(e.to_string()))?;
        let mut request = self.client.request(method, url).query(query);

        if let Some(body) = body {
            request = request.json(&body);
        }

        if let Some((timestamp, signature)) = signature {
            let signer = self
                .signer
                .as_ref()
                .ok_or_else(|| ArcusHttpError::Validation("Signing key is required".into()))?;
            request = request
                .header("X-API-Key", signer.api_key())
                .header("X-Timestamp", timestamp.to_string())
                .header("X-Signature", signature);
        }
        let response = request.send().await.map_err(|e| {
            if is_write && !e.is_builder() && !e.is_connect() {
                ArcusHttpError::UnknownOutcome(e.without_url().to_string())
            } else {
                ArcusHttpError::Transport(e.without_url().to_string())
            }
        })?;
        let status = response.status();
        let retry_after_ms = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1)
            .saturating_mul(1_000);
        let mut response = response;
        let mut bytes = Vec::new();

        while let Some(chunk) = response.chunk().await.map_err(|e| {
            if is_write {
                ArcusHttpError::UnknownOutcome(e.without_url().to_string())
            } else {
                ArcusHttpError::Transport(e.without_url().to_string())
            }
        })? {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(if is_write {
                    ArcusHttpError::UnknownOutcome("Response exceeded bounded size".into())
                } else {
                    ArcusHttpError::Decode("Response exceeded bounded size".into())
                });
            }
            bytes.extend_from_slice(&chunk);
        }

        if !status.is_success() {
            let error = serde_json::from_slice::<ArcusApiError>(&bytes).ok();

            if status.as_u16() == 429 {
                return Err(ArcusHttpError::RateLimited {
                    retry_after_ms: error
                        .as_ref()
                        .and_then(|e| e.retry_after_ms)
                        .unwrap_or(retry_after_ms),
                    reason: error
                        .as_ref()
                        .and_then(|e| e.reason.clone())
                        .unwrap_or_else(|| "ip".into()),
                });
            }
            let message = error.as_ref().map_or_else(
                || format!("HTTP {}", status.as_u16()),
                |e| {
                    format!(
                        "{} ({})",
                        e.error,
                        e.error_type.as_deref().unwrap_or("unspecified")
                    )
                },
            );

            if is_write
                && (status.is_server_error() || status.as_u16() == 408)
                && !error
                    .as_ref()
                    .is_some_and(|e| e.error_type.as_deref() == Some("Transmission"))
            {
                return Err(ArcusHttpError::UnknownOutcome(message));
            }
            return Err(ArcusHttpError::Rejected {
                status: status.as_u16(),
                message,
            });
        }
        serde_json::from_slice(&bytes).map_err(|e| {
            if is_write {
                ArcusHttpError::UnknownOutcome(format!(
                    "Accepted response could not be decoded: {e}"
                ))
            } else {
                ArcusHttpError::Decode(e.to_string())
            }
        })
    }

    fn account_query(address: &str, account_index: u8) -> ArcusHttpResult<Vec<(String, String)>> {
        validate_address(address, account_index)
            .map_err(|e| ArcusHttpError::Validation(e.to_string()))?;
        Ok(vec![
            ("address".into(), address.to_ascii_lowercase()),
            ("accountIndex".into(), account_index.to_string()),
        ])
    }

    /// Retrieves all perpetual market definitions with exact decimal fields.
    ///
    /// # Errors
    ///
    /// Returns a transport, rate-limit, rejection, or response-decoding error.
    pub async fn markets(&self) -> ArcusHttpResult<Vec<ArcusMarket>> {
        let markets = self
            .request::<ArcusMarketsResponse>(Method::GET, "/v1/markets", &[], None, None, 20)
            .await?
            .markets;
        self.reserve_read_weight(u32::try_from(markets.len() / 20).unwrap_or(u32::MAX))
            .await;
        Ok(markets)
    }

    /// Retrieves the account state for one master address and subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, an inactive account, or failed response retrieval.
    pub async fn account(&self, address: &str, account_index: u8) -> ArcusHttpResult<ArcusAccount> {
        self.request(
            Method::GET,
            "/v1/account",
            &Self::account_query(address, account_index)?,
            None,
            None,
            2,
        )
        .await
    }

    /// Retrieves a complete open-position snapshot for one subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity or failed response retrieval.
    pub async fn positions(
        &self,
        address: &str,
        account_index: u8,
    ) -> ArcusHttpResult<Vec<ArcusPosition>> {
        Ok(self
            .request::<ArcusPositionsResponse>(
                Method::GET,
                "/v1/positions",
                &Self::account_query(address, account_index)?,
                None,
                None,
                2,
            )
            .await?
            .positions
            .into_values()
            .collect())
    }

    /// Retrieves an order using its server identifier and owning subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, an unknown order, or failed response retrieval.
    pub async fn order_status(
        &self,
        address: &str,
        account_index: u8,
        order_id: &str,
    ) -> ArcusHttpResult<ArcusOrder> {
        let digits = order_id
            .strip_prefix("0x")
            .or_else(|| order_id.strip_prefix("0X"))
            .unwrap_or(order_id);

        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ArcusHttpError::Validation(
                "Server order ID must be hexadecimal".into(),
            ));
        }
        self.request(
            Method::GET,
            &format!("/v1/order/{order_id}"),
            &Self::account_query(address, account_index)?,
            None,
            None,
            2,
        )
        .await
    }

    fn history_query(
        address: &str,
        account_index: u8,
        market: Option<&str>,
        from: Option<u64>,
        to: Option<u64>,
    ) -> ArcusHttpResult<Vec<(String, String)>> {
        let mut query = Self::account_query(address, account_index)?;
        query.push(("limit".into(), PAGE_SIZE.to_string()));

        if let Some(market) = market {
            query.push(("market".into(), market.into()));
        }

        if let Some(from) = from {
            query.push(("from".into(), from.to_string()));
        }

        if let Some(to) = to {
            query.push(("to".into(), to.to_string()));
        }
        Ok(query)
    }

    /// Retrieves bounded history, deduplicating overlapping microsecond page boundaries.
    ///
    /// # Errors
    ///
    /// Returns an error if retrieval fails, pagination stalls, or history exceeds 64 pages.
    pub async fn orders(
        &self,
        address: &str,
        account_index: u8,
        market: Option<&str>,
        from: Option<u64>,
        mut to: Option<u64>,
    ) -> ArcusHttpResult<Vec<ArcusOrder>> {
        let mut result = Vec::new();
        let mut seen = HashSet::new();

        for _ in 0..MAX_PAGES {
            let query = Self::history_query(address, account_index, market, from, to)?;
            let page = self
                .request::<ArcusOrdersResponse>(Method::GET, "/v1/orders", &query, None, None, 70)
                .await?
                .orders;
            let complete = page.len() < PAGE_SIZE;
            let oldest = page.iter().map(|o| o.updated_at).min();

            for order in page {
                if seen.insert(order.order_id.clone()) {
                    result.push(order);
                }
            }

            if complete {
                return Ok(result);
            }
            let oldest =
                oldest.ok_or_else(|| ArcusHttpError::Incomplete("Empty full order page".into()))?;

            if to.is_some_and(|bound| oldest >= bound) {
                return Err(ArcusHttpError::Incomplete(
                    "Order pagination saturated a microsecond boundary".into(),
                ));
            }
            to = Some(oldest);
        }
        Err(ArcusHttpError::Incomplete(
            "Order history exceeded 64 pages; narrow the time range".into(),
        ))
    }

    /// Retrieves every live order, paging by placement time rather than update time.
    ///
    /// # Errors
    ///
    /// Returns an error for retrieval failures, missing cursor timestamps, or incomplete pagination.
    pub async fn open_orders(
        &self,
        address: &str,
        account_index: u8,
    ) -> ArcusHttpResult<Vec<ArcusOrder>> {
        let mut result = Vec::new();
        let mut seen = HashSet::new();
        let mut to = None;

        for _ in 0..MAX_PAGES {
            let query = Self::history_query(address, account_index, None, None, to)?;
            let page = self
                .request::<ArcusOrdersResponse>(
                    Method::GET,
                    "/v1/openOrders",
                    &query,
                    None,
                    None,
                    70,
                )
                .await?
                .orders;
            let complete = page.len() < PAGE_SIZE;
            let mut oldest = None;

            for order in page {
                let created_at = order.created_at.ok_or_else(|| {
                    ArcusHttpError::Incomplete(
                        "Open order omitted the createdAt pagination cursor".into(),
                    )
                })?;
                oldest = Some(oldest.map_or(created_at, |previous: u64| previous.min(created_at)));

                if seen.insert(order.order_id.clone()) {
                    result.push(order);
                }
            }

            if complete {
                return Ok(result);
            }
            let oldest = oldest
                .ok_or_else(|| ArcusHttpError::Incomplete("Empty full open-order page".into()))?;

            if to.is_some_and(|bound| oldest >= bound) {
                return Err(ArcusHttpError::Incomplete(
                    "Open-order pagination saturated a microsecond boundary".into(),
                ));
            }
            to = Some(oldest);
        }
        Err(ArcusHttpError::Incomplete(
            "Open orders exceeded 64 pages".into(),
        ))
    }

    /// Retrieves fills with bounded pagination and stable trade/order identity deduplication.
    ///
    /// # Errors
    ///
    /// Returns an error if retrieval fails, pagination stalls, or history exceeds 64 pages.
    pub async fn fills(
        &self,
        address: &str,
        account_index: u8,
        market: Option<&str>,
        from: Option<u64>,
        mut to: Option<u64>,
    ) -> ArcusHttpResult<Vec<ArcusFill>> {
        let mut result = Vec::new();
        let mut seen = HashSet::new();

        for _ in 0..MAX_PAGES {
            let query = Self::history_query(address, account_index, market, from, to)?;
            let page = self
                .request::<ArcusFillsResponse>(Method::GET, "/v1/fills", &query, None, None, 70)
                .await?
                .fills;
            let complete = page.len() < PAGE_SIZE;
            let oldest = page.iter().map(|f| f.created_at).min();

            for fill in page {
                if seen.insert((fill.trade_id.clone(), fill.order_id.clone())) {
                    result.push(fill);
                }
            }

            if complete {
                return Ok(result);
            }
            let oldest =
                oldest.ok_or_else(|| ArcusHttpError::Incomplete("Empty full fill page".into()))?;

            if to.is_some_and(|bound| oldest >= bound) {
                return Err(ArcusHttpError::Incomplete(
                    "Fill pagination saturated a microsecond boundary".into(),
                ));
            }
            to = Some(oldest);
        }
        Err(ArcusHttpError::Incomplete(
            "Fill history exceeded 64 pages; narrow the time range".into(),
        ))
    }

    fn signer(&self) -> ArcusHttpResult<&ArcusSigner> {
        self.signer
            .as_deref()
            .ok_or_else(|| ArcusHttpError::Validation("Signing key is required".into()))
    }

    /// Validates, signs, and transmits an order exactly once.
    ///
    /// # Errors
    ///
    /// Returns validation or definite rejection errors, or an uncertain-outcome error.
    pub async fn place_order(
        &self,
        order: &ArcusOrderRequest,
        market: &ArcusMarket,
    ) -> ArcusHttpResult<ArcusOrderResponse> {
        let signature = self
            .signer()?
            .sign_order(order, market, order.timestamp)
            .map_err(|e| ArcusHttpError::Validation(e.to_string()))?;
        let body =
            serde_json::to_value(order).map_err(|e| ArcusHttpError::Validation(e.to_string()))?;
        self.request(
            Method::POST,
            "/v1/placeOrder",
            &Self::account_query(&order.address, order.account_index)?,
            Some(body),
            Some((order.timestamp, signature)),
            0,
        )
        .await
    }

    /// Signs and transmits a single cancellation without retrying it.
    ///
    /// # Errors
    ///
    /// Returns validation or definite rejection errors, or an uncertain-outcome error.
    pub async fn cancel_order(
        &self,
        order: &ArcusCancelRequest,
    ) -> ArcusHttpResult<ArcusOrderResponse> {
        let signature = self
            .signer()?
            .sign_cancel(order, order.timestamp)
            .map_err(|e| ArcusHttpError::Validation(e.to_string()))?;
        let body =
            serde_json::to_value(order).map_err(|e| ArcusHttpError::Validation(e.to_string()))?;
        self.request(
            Method::POST,
            "/v1/cancelOrder",
            &Self::account_query(&order.address, order.account_index)?,
            Some(body),
            Some((order.timestamp, signature)),
            0,
        )
        .await
    }

    /// Requests account-wide cancellation; its acknowledgment is not terminal order state.
    ///
    /// # Errors
    ///
    /// Returns validation or definite rejection errors, or an uncertain-outcome error.
    pub async fn cancel_all(
        &self,
        address: &str,
        account_index: u8,
    ) -> ArcusHttpResult<ArcusOrderResponse> {
        self.cancel_scope(address, account_index, None).await
    }

    /// Requests cancellation of every order on a single market without scanning history.
    ///
    /// # Errors
    ///
    /// Returns validation or definite rejection errors, or an uncertain-outcome error.
    pub async fn cancel_market(
        &self,
        address: &str,
        account_index: u8,
        market_id: u16,
    ) -> ArcusHttpResult<ArcusOrderResponse> {
        self.cancel_scope(address, account_index, Some(market_id))
            .await
    }

    async fn cancel_scope(
        &self,
        address: &str,
        account_index: u8,
        market_id: Option<u16>,
    ) -> ArcusHttpResult<ArcusOrderResponse> {
        validate_address(address, account_index)
            .map_err(|e| ArcusHttpError::Validation(e.to_string()))?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ArcusHttpError::Validation(e.to_string()))?
            .as_nanos()
            .try_into()
            .map_err(|_| ArcusHttpError::Validation("Clock overflows nanoseconds".into()))?;
        let mut body = json!({ "address": address, "accountIndex": account_index });

        if let Some(market_id) = market_id {
            body["marketId"] = market_id.into();
        }
        let signature = self
            .signer()?
            .sign_legacy("cancelAllOrders", &body, timestamp)
            .map_err(|e| ArcusHttpError::Validation(e.to_string()))?;
        self.request(
            Method::POST,
            "/v1/cancelAllOrders",
            &Self::account_query(address, account_index)?,
            Some(body),
            Some((timestamp, signature)),
            0,
        )
        .await
    }
}
