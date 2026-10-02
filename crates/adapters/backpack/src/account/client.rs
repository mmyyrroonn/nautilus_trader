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

//! Fixed authenticated GET reads; one deadline covers the complete traversal or snapshot.

use std::collections::{BTreeMap, BTreeSet};

use nautilus_network::dst::time::Instant;
use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;

use super::{
    BackpackAccountError, BackpackEvidenceGap,
    models::*,
    pagination::{
        BackpackHistory, BackpackHistoryWindow, BackpackPageHeaders, BackpackReadBudget,
        PageTracker,
    },
    reports::fill_time,
};
use crate::{
    http::{
        client::BackpackHttpClient,
        request::{BackpackReadOperation, BackpackReadRequest},
    },
    signing::{BackpackParameters, BackpackScalar},
};

/// Independent endpoint observations, not an atomic account snapshot or identity proof.
#[derive(Clone, Debug)]
pub struct BackpackAccountSnapshot {
    pub policy: BackpackAccountPolicy,
    pub balances: BTreeMap<String, BackpackWalletBalance>,
    pub collateral: BackpackCollateral,
    pub positions: Vec<BackpackPosition>,
    pub open_orders: Vec<BackpackOrder>,
    pub gaps: BTreeSet<BackpackEvidenceGap>,
}
impl BackpackAccountSnapshot {
    /// Returns only explicitly reported positions. None means unobserved, never flat.
    #[must_use]
    pub fn position(&self, symbol: &str) -> Option<&BackpackPosition> {
        self.positions.iter().find(|p| p.symbol == symbol)
    }
}

/// Resting-only absence retains unknown prior execution/cancellation/submission status.
#[derive(Clone, Debug)]
pub enum BackpackRestingOrder {
    Observed(Box<BackpackOrder>),
    UnknownNotResting,
}

/// Typed read-only client sharing the caller's HTTP quota and explicit finite budgets.
#[derive(Clone, Debug)]
pub struct BackpackAccountReader {
    http: BackpackHttpClient,
    budget: BackpackReadBudget,
}
impl BackpackAccountReader {
    /// Creates a reader without reading credentials, resolving accounts or issuing I/O.
    #[must_use]
    pub const fn new(http: BackpackHttpClient, budget: BackpackReadBudget) -> Self {
        Self { http, budget }
    }

    /// Collects settings, balances, collateral, positions and all resting orders.
    ///
    /// Every operation uses one total deadline. Missing/null responses and request
    /// failures return errors; `{}` balances and `[]` positions are valid observations
    /// but do not prove flatness or account identity. Unsupported policy never mutates
    /// settings. No cross-endpoint watermark is documented, so coverage stays degraded.
    ///
    /// # Errors
    ///
    /// Returns an error for transport/deadline/decoding failure or duplicate row identities.
    pub async fn snapshot(
        &self,
        cancel: &CancellationToken,
    ) -> Result<BackpackAccountSnapshot, BackpackAccountError> {
        let deadline = self.deadline()?;
        let policy: BackpackAccountPolicy = self
            .one(
                BackpackReadOperation::Account,
                BackpackParameters::default(),
                deadline,
                cancel,
            )
            .await?;
        let balances: BTreeMap<String, BackpackWalletBalance> = self
            .one(
                BackpackReadOperation::Balances,
                BackpackParameters::default(),
                deadline,
                cancel,
            )
            .await?;
        let collateral: BackpackCollateral = self
            .one(
                BackpackReadOperation::Collateral,
                BackpackParameters::default(),
                deadline,
                cancel,
            )
            .await?;
        let positions: Vec<BackpackPosition> = self
            .one(
                BackpackReadOperation::Positions,
                BackpackParameters::default(),
                deadline,
                cancel,
            )
            .await?;
        let open_orders: Vec<BackpackOrder> = self
            .one(
                BackpackReadOperation::OpenOrders,
                BackpackParameters::default(),
                deadline,
                cancel,
            )
            .await?;
        if positions.len() as u64 > self.budget.max_items
            || open_orders.len() as u64 > self.budget.max_items
            || balances.len() as u64 > self.budget.max_items
            || collateral.collateral.len() as u64 > self.budget.max_items
        {
            return Err(BackpackAccountError::Budget);
        }
        unique(positions.iter().map(|p| p.symbol.as_str()))?;
        unique(open_orders.iter().map(|o| o.id.as_str()))?;
        unique(collateral.collateral.iter().map(|a| a.symbol.as_str()))?;
        // Returned positions/funding may identify a user, but empty results and account
        // settings do not attest the configured credential/store namespace.
        let mut gaps = BTreeSet::from([
            BackpackEvidenceGap::AccountIdentityUnverified,
            BackpackEvidenceGap::NonAtomicSnapshot,
            BackpackEvidenceGap::PositionTimeUnknown,
            BackpackEvidenceGap::OrderTimeUnknown,
        ]);
        if policy.auto_borrow_settlements
            || policy.auto_lend
            || policy.auto_repay_borrows
            || policy.liquidating
            || collateral.borrow_liability.0 != rust_decimal::Decimal::ZERO
            || collateral.liabilities_value.0 != rust_decimal::Decimal::ZERO
        {
            gaps.insert(BackpackEvidenceGap::UnsupportedAccountPolicy);
        }
        if collateral.collateral.iter().any(|a| {
            a.symbol != "USDC"
                || a.lend_quantity.0 != rust_decimal::Decimal::ZERO
                || a.collateral_weight.0 != rust_decimal::Decimal::ONE
        }) || collateral.unsettled_equity.0 != rust_decimal::Decimal::ZERO
        {
            gaps.insert(BackpackEvidenceGap::UnsupportedCollateral);
        }
        if !policy.extra.is_empty()
            || !collateral.extra.is_empty()
            || positions.iter().any(|p| !p.extra.is_empty())
            || balances.values().any(|b| !b.extra.is_empty())
            || open_orders.iter().any(|o| !o.extra.is_empty())
            || collateral.collateral.iter().any(|a| !a.extra.is_empty())
        {
            gaps.insert(BackpackEvidenceGap::UnknownFields);
        }
        check_budget(deadline, cancel)?;
        Ok(BackpackAccountSnapshot {
            policy,
            balances,
            collateral,
            positions,
            open_orders,
            gaps,
        })
    }

    /// Reads one exclusively addressed resting order. HTTP404 means only not resting.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid selectors, transport failure or malformed response.
    pub async fn resting_order(
        &self,
        symbol: &str,
        order_id: Option<&str>,
        client_id: Option<u32>,
        cancel: &CancellationToken,
    ) -> Result<BackpackRestingOrder, BackpackAccountError> {
        let mut params = symbol_parameters(Some(symbol))?;
        params.insert(
            "orderId",
            order_id.map(|id| BackpackScalar::Token(id.to_string())),
        )?;
        params.insert(
            "clientId",
            client_id.map(|id| BackpackScalar::Unsigned(id.into())),
        )?;
        let request = BackpackReadRequest::new(BackpackReadOperation::OpenOrder, params)?;
        match self
            .http
            .read(&request, Some(self.deadline()?), None, cancel)
            .await
        {
            Ok(response) => {
                let raw: BackpackOrder = decode(response.body())?;
                if raw.symbol != symbol
                    || order_id.is_some_and(|id| raw.id != id)
                    || client_id.is_some_and(|id| raw.client_id != Some(id))
                {
                    return Err(BackpackAccountError::InvalidField(
                        "resting order correlation",
                    ));
                }
                Ok(BackpackRestingOrder::Observed(Box::new(raw)))
            }
            Err(e) if e.status() == Some(404) => Ok(BackpackRestingOrder::UnknownNotResting),
            Err(e) => Err(e.into()),
        }
    }

    /// Traverses every reported order page; the venue has no server time-cutoff filter.
    ///
    /// # Errors
    ///
    /// Returns an error for failed pages, unstable headers, repeats or finite budget exhaustion.
    pub async fn order_history(
        &self,
        window: BackpackHistoryWindow,
        symbol: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<BackpackHistory<BackpackOrder>, BackpackAccountError> {
        let mut history = self
            .history(
                BackpackReadOperation::OrderHistory,
                window,
                symbol,
                cancel,
                |o: &BackpackOrder| {
                    matching_symbol(symbol, &o.symbol)?;
                    Ok(o.id.clone())
                },
            )
            .await?;
        history
            .evidence
            .gaps
            .insert(BackpackEvidenceGap::OrderTimeUnknown);
        Ok(history)
    }
    /// Traverses a fixed inclusive/exclusive UTC fill window including system fills.
    ///
    /// # Errors
    ///
    /// Returns an error for failed pages, unstable headers, repeated IDs or out-of-window fills.
    pub async fn fill_history(
        &self,
        window: BackpackHistoryWindow,
        symbol: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<BackpackHistory<BackpackFill>, BackpackAccountError> {
        let mut history = self
            .history(
                BackpackReadOperation::FillHistory,
                window,
                symbol,
                cancel,
                |f: &BackpackFill| {
                    matching_symbol(symbol, &f.symbol)?;
                    let timestamp = fill_time(&f.timestamp)?.as_u64();
                    if timestamp < window.from_ms * 1_000_000
                        || timestamp >= window.to_ms * 1_000_000
                    {
                        return Err(BackpackAccountError::InvalidField(
                            "fill outside history window",
                        ));
                    }
                    if let Some(id) = f.trade_id {
                        serde_json::to_string(&(&f.symbol, id))
                            .map_err(|_| BackpackAccountError::Decode)
                    } else {
                        fingerprint(f)
                    }
                },
            )
            .await?;
        if history.records.iter().any(|fill| fill.trade_id.is_none()) {
            history
                .evidence
                .gaps
                .insert(BackpackEvidenceGap::MissingTradeId);
        }
        Ok(history)
    }
    /// Traverses raw signed funding payments; missing currency/timezone stays explicit.
    ///
    /// # Errors
    ///
    /// Returns an error for failed pages, unstable headers, repeats or finite budget exhaustion.
    pub async fn funding_history(
        &self,
        window: BackpackHistoryWindow,
        symbol: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<BackpackHistory<BackpackFundingPayment>, BackpackAccountError> {
        let mut history = self
            .history(
                BackpackReadOperation::FundingHistory,
                window,
                symbol,
                cancel,
                |f: &BackpackFundingPayment| {
                    matching_symbol(symbol, &f.symbol)?;
                    f.interval_end_timestamp
                        .parse::<jiff::civil::DateTime>()
                        .map_err(|_| BackpackAccountError::InvalidField("funding interval"))?;
                    serde_json::to_string(&(
                        f.user_id,
                        f.subaccount_id,
                        &f.symbol,
                        &f.interval_end_timestamp,
                    ))
                    .map_err(|_| BackpackAccountError::Decode)
                },
            )
            .await?;
        history
            .evidence
            .gaps
            .insert(BackpackEvidenceGap::FundingCurrencyUnknown);
        history
            .evidence
            .gaps
            .insert(BackpackEvidenceGap::FundingTimezoneUnknown);
        Ok(history)
    }

    fn deadline(&self) -> Result<Instant, BackpackAccountError> {
        Instant::now()
            .checked_add(self.budget.timeout)
            .ok_or(BackpackAccountError::Budget)
    }
    async fn one<T: DeserializeOwned>(
        &self,
        operation: BackpackReadOperation,
        params: BackpackParameters,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<T, BackpackAccountError> {
        let request = BackpackReadRequest::new(operation, params)?;
        let response = self
            .http
            .read(&request, Some(deadline), None, cancel)
            .await?;
        decode(response.body())
    }
    async fn history<T: DeserializeOwned + Serialize>(
        &self,
        operation: BackpackReadOperation,
        window: BackpackHistoryWindow,
        symbol: Option<&str>,
        cancel: &CancellationToken,
        key: impl Fn(&T) -> Result<String, BackpackAccountError>,
    ) -> Result<BackpackHistory<T>, BackpackAccountError> {
        // Validate public window fields again before any arithmetic or transport.
        let window = BackpackHistoryWindow::new(window.from_ms, window.to_ms)?;
        let deadline = self.deadline()?;
        let mut tracker = PageTracker::new(self.budget);
        let mut records = Vec::new();
        loop {
            let mut params = symbol_parameters(symbol)?;
            params.insert(
                "limit",
                Some(BackpackScalar::Unsigned(self.budget.page_size)),
            )?;
            params.insert(
                "offset",
                Some(BackpackScalar::Unsigned(tracker.next_offset()?)),
            )?;
            params.insert(
                "sortDirection",
                Some(BackpackScalar::Token("Asc".to_string())),
            )?;
            if operation == BackpackReadOperation::FillHistory {
                params.insert(
                    "from",
                    Some(BackpackScalar::Signed(
                        i64::try_from(window.from_ms)
                            .map_err(|_| BackpackAccountError::InvalidField("history from"))?,
                    )),
                )?;
                params.insert(
                    "to",
                    Some(BackpackScalar::Signed(
                        i64::try_from(window.to_ms)
                            .map_err(|_| BackpackAccountError::InvalidField("history to"))?,
                    )),
                )?;
            }
            let request = BackpackReadRequest::new(operation, params)?;
            let response = self
                .http
                .read(&request, Some(deadline), None, cancel)
                .await?;
            let page: Vec<T> = decode(response.body())?;
            let headers = BackpackPageHeaders::parse(response.headers())?;
            let keys: Vec<String> = page.iter().map(&key).collect::<Result<_, _>>()?;
            let complete = tracker.accept(headers, &keys)?;
            records.extend(page);
            if complete {
                break;
            }
        }
        check_budget(deadline, cancel)?;
        let evidence = tracker.finish(window, operation == BackpackReadOperation::FillHistory)?;
        Ok(BackpackHistory { records, evidence })
    }
}
fn symbol_parameters(symbol: Option<&str>) -> Result<BackpackParameters, BackpackAccountError> {
    let mut parameters = BackpackParameters::default();
    parameters.insert(
        "symbol",
        symbol.map(|s| BackpackScalar::Token(s.to_string())),
    )?;
    Ok(parameters)
}
fn decode<T: DeserializeOwned>(raw: &[u8]) -> Result<T, BackpackAccountError> {
    serde_json::from_slice(raw).map_err(|_| BackpackAccountError::Decode)
}
fn unique<'a>(values: impl IntoIterator<Item = &'a str>) -> Result<(), BackpackAccountError> {
    let mut keys = BTreeSet::new();
    for value in values {
        if value.is_empty() || !keys.insert(value) {
            return Err(BackpackAccountError::Conflict);
        }
    }
    Ok(())
}
fn fingerprint<T: Serialize>(value: &T) -> Result<String, BackpackAccountError> {
    let bytes = serde_json::to_vec(value).map_err(|_| BackpackAccountError::Decode)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn check_budget(deadline: Instant, cancel: &CancellationToken) -> Result<(), BackpackAccountError> {
    if cancel.is_cancelled() || Instant::now() >= deadline {
        Err(BackpackAccountError::Budget)
    } else {
        Ok(())
    }
}

fn matching_symbol(requested: Option<&str>, received: &str) -> Result<(), BackpackAccountError> {
    if requested.is_some_and(|symbol| symbol != received) {
        Err(BackpackAccountError::InvalidField("history symbol"))
    } else {
        Ok(())
    }
}
