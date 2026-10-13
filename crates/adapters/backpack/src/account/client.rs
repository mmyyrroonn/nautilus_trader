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
    authenticated_namespace: Option<crate::identity::BackpackClientIdNamespace>,
    read_started_at_ms: u64,
}
impl BackpackAccountSnapshot {
    /// Derives conservative facts only from this reader's authenticated credential-scoped response.
    ///
    /// The caller must fence private updates around this non-atomic REST traversal.
    /// Missing rows mean no current open position in this complete endpoint response;
    /// they never prove historic flatness or absence of concurrent venue mutations.
    pub(crate) fn production_facts(
        &self,
        namespace: &crate::identity::BackpackClientIdNamespace,
        generation: u64,
        observed_at_ms: u64,
        symbols: &BTreeSet<String>,
    ) -> anyhow::Result<crate::execution::guard::BackpackLoopbackAccountFacts> {
        use rust_decimal::Decimal;

        anyhow::ensure!(
            self.authenticated_namespace
                .as_ref()
                .is_some_and(|authenticated| namespace.credential_authority_matches(authenticated)),
            "production account response is not bound to this credential authority"
        );
        anyhow::ensure!(
            generation != 0 && !symbols.is_empty(),
            "invalid production observation scope"
        );
        anyhow::ensure!(
            !self.gaps.contains(&BackpackEvidenceGap::UnknownFields),
            "unknown production account response fields"
        );
        let collateral = &self.collateral;
        anyhow::ensure!(
            !self.policy.auto_lend
                && !self.policy.auto_repay_borrows
                && !self.policy.liquidating
                && self.policy.leverage_limit.0 >= Decimal::ONE,
            "unsupported production account policy"
        );
        anyhow::ensure!(
            collateral.borrow_liability.0 == Decimal::ZERO,
            "borrowed production collateral is unsupported"
        );
        anyhow::ensure!(
            collateral.unsettled_equity.0 == Decimal::ZERO,
            "unsettled production collateral is unsupported"
        );
        anyhow::ensure!(
            liabilities_explained_by_pnl(collateral),
            "production liabilities are not fully explained by negative unrealized PnL"
        );
        anyhow::ensure!(
            !self
                .gaps
                .contains(&BackpackEvidenceGap::UnsupportedCollateral),
            "unsupported production collateral evidence"
        );
        anyhow::ensure!(
            collateral.net_equity_available.0 >= Decimal::ZERO
                && collateral.net_equity_locked.0 >= Decimal::ZERO
                && collateral.net_equity.0 >= collateral.net_equity_available.0
                && collateral.imf.0 >= Decimal::ZERO
                && collateral.imf.0 <= Decimal::ONE
                && collateral.mmf.0 >= Decimal::ZERO
                && collateral.mmf.0 <= collateral.imf.0,
            "invalid production margin observation"
        );
        anyhow::ensure!(
            collateral.collateral.len() <= 1,
            "multi-asset collateral is unsupported"
        );
        for asset in &collateral.collateral {
            anyhow::ensure!(
                asset.symbol == "USDC"
                    && asset.collateral_weight.0 == Decimal::ONE
                    && asset.asset_mark_price.0 == Decimal::ONE
                    && asset.lend_quantity.0 == Decimal::ZERO
                    && asset.available_quantity.0 >= Decimal::ZERO
                    && asset.total_quantity.0 >= Decimal::ZERO
                    && asset.collateral_value.0 >= Decimal::ZERO,
                "only unborrowed, unlent, par-valued USDC collateral is supported"
            );
        }
        anyhow::ensure!(
            !collateral.collateral.is_empty() || collateral.net_equity_available.0 == Decimal::ZERO,
            "available margin lacks USDC collateral evidence"
        );
        let collateral_value = collateral
            .collateral
            .first()
            .map_or(Decimal::ZERO, |asset| asset.collateral_value.0);

        // Positive PnL is included in assetsValue, negative PnL in liabilitiesValue,
        // validate each component without counting either PnL twice.
        anyhow::ensure!(
            collateral_value.checked_add(collateral.pnl_unrealized.0.max(Decimal::ZERO))
                == Some(collateral.assets_value.0),
            "contradictory production assets and unrealized PnL observations"
        );
        anyhow::ensure!(
            collateral
                .assets_value
                .0
                .checked_sub(collateral.liabilities_value.0)
                == Some(collateral.net_equity.0)
                && collateral_value.checked_add(collateral.pnl_unrealized.0)
                    == Some(collateral.net_equity.0),
            "contradictory production equity and collateral observations"
        );

        // Checked addition rounds only to Decimal's representable precision, so
        // sub-precision venue rounding does not require a monetary tolerance.
        anyhow::ensure!(
            collateral
                .net_equity_available
                .0
                .checked_add(collateral.net_equity_locked.0)
                == Some(collateral.net_equity.0),
            "contradictory production available and locked margin observations"
        );
        for (currency, balance) in &self.balances {
            anyhow::ensure!(
                balance.available.0 >= Decimal::ZERO
                    && balance.locked.0 >= Decimal::ZERO
                    && balance.staked.0 == Decimal::ZERO
                    && (currency == "USDC"
                        || (balance.available.0 == Decimal::ZERO
                            && balance.locked.0 == Decimal::ZERO)),
                "non-USDC, staked, or negative wallet balances are unsupported"
            );
        }
        let mut net_positions = BTreeMap::new();
        for symbol in symbols {
            anyhow::ensure!(
                symbol
                    .strip_suffix("_USDC_PERP")
                    .is_some_and(|base| !base.is_empty()
                        && base
                            .bytes()
                            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())),
                "unsupported production symbol"
            );
            net_positions.insert(
                nautilus_model::identifiers::InstrumentId::from(format!("{symbol}.BACKPACK")),
                Decimal::ZERO,
            );
        }
        let mut identity = None;
        let mut seen = BTreeSet::new();
        for position in &self.positions {
            anyhow::ensure!(
                symbols.contains(&position.symbol) && seen.insert(&position.symbol),
                "outside-scope or duplicate production position"
            );
            let current = (position.user_id, position.subaccount_id);
            anyhow::ensure!(
                identity.is_none_or(|previous| previous == current),
                "conflicting venue position identities"
            );
            identity = Some(current);
            if let Some(expected) = namespace.asserted_subaccount() {
                anyhow::ensure!(
                    position.subaccount_id == Some(expected),
                    "venue position subaccount does not match assertion"
                );
            }
            anyhow::ensure!(
                position.imf.0 >= Decimal::ZERO
                    && position.imf.0 <= Decimal::ONE
                    && position.mmf.0 >= Decimal::ZERO
                    && position.mmf.0 <= position.imf.0,
                "invalid production position margin observation"
            );
            net_positions.insert(
                nautilus_model::identifiers::InstrumentId::from(format!(
                    "{}.BACKPACK",
                    position.symbol
                )),
                position.net_quantity.0,
            );
        }
        if self.positions.is_empty() && self.open_orders.is_empty() {
            anyhow::ensure!(
                collateral.net_exposure_futures.0 == Decimal::ZERO
                    && collateral.pnl_unrealized.0 == Decimal::ZERO
                    && collateral.net_equity_locked.0 == Decimal::ZERO,
                "empty positions/orders contradict collateral exposure, PnL, or locked margin"
            );
        }
        anyhow::ensure!(
            namespace.asserted_subaccount().is_none() || identity.is_some(),
            "numeric subaccount assertion cannot be verified from empty positions"
        );
        let fee_bps = self
            .policy
            .futures_maker_fee
            .0
            .abs()
            .max(self.policy.futures_taker_fee.0.abs())
            .normalize();
        let fee = Decimal::try_from_i128_with_scale(fee_bps.mantissa(), fee_bps.scale() + 4)
            .map_err(|_| anyhow::anyhow!("fee fraction cannot be represented exactly"))?;
        anyhow::ensure!(
            fee <= Decimal::ONE && self.policy.futures_taker_fee.0 >= Decimal::ZERO,
            "invalid production fee observation"
        );
        Ok(crate::execution::guard::BackpackLoopbackAccountFacts {
            namespace: namespace.clone(), generation,
            observed_at_ms: observed_at_ms.min(self.read_started_at_ms),
            available_margin: collateral.net_equity_available.0,
            // Reserve the full notional instead of estimating dynamic venue margin tiers
            margin_per_notional: Decimal::ONE,
            fee_buffer_per_notional: fee,
            economics_reference: "authenticated account/collateral; conservative full-notional reserve; futures fees in bps".into(),
            net_positions,
            // Native order borrowing is disabled; the observed settlement policy is
            // retained on the snapshot and is not changed or represented as false.
            auto_borrow: false,
            auto_lend: self.policy.auto_lend,
            auto_repay: self.policy.auto_repay_borrows,
            liquidating: self.policy.liquidating,
            complete: true,
        })
    }

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
        let read_started_at_ms = self.http.timestamp_ms()?;
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
        if unsupported_account_policy(&policy, &collateral) {
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
            || open_orders.iter().any(|order| {
                order.extra.iter().any(|(key, value)| {
                    !disabled_spot_order_flag(key, value) && !absent_order_option(key, value)
                })
            })
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
            authenticated_namespace: self.http.credential_namespace(),
            read_started_at_ms,
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
fn liabilities_explained_by_pnl(collateral: &BackpackCollateral) -> bool {
    use rust_decimal::Decimal;

    collateral.liabilities_value.0 == collateral.pnl_unrealized.0.min(Decimal::ZERO).abs()
}

fn unsupported_account_policy(
    policy: &BackpackAccountPolicy,
    collateral: &BackpackCollateral,
) -> bool {
    policy.auto_borrow_settlements
        || policy.auto_lend
        || policy.auto_repay_borrows
        || policy.liquidating
        || collateral.borrow_liability.0 != rust_decimal::Decimal::ZERO
        || !liabilities_explained_by_pnl(collateral)
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

#[cfg(test)]
mod tests {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{common::credential::BackpackCredential, identity::BackpackClientIdNamespace};

    fn snapshot() -> (BackpackAccountSnapshot, BackpackClientIdNamespace) {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../test_data/account/synthetic.json")).unwrap();
        let key = BackpackCredential::production(&STANDARD.encode([19; 32])).unwrap();
        let namespace =
            BackpackClientIdNamespace::production_credential(key.verifying_key(), None).unwrap();
        let mut collateral: BackpackCollateral =
            serde_json::from_value(fixture["collateral"].clone()).unwrap();
        // Empty position/order observations require compatible collateral observations
        collateral.net_exposure_futures.0 = Decimal::ZERO;
        collateral.pnl_unrealized.0 = Decimal::ZERO;
        collateral.net_equity_locked.0 = Decimal::ZERO;
        collateral.net_equity.0 = collateral.net_equity_available.0;
        collateral.assets_value.0 = collateral.net_equity.0;
        let asset = &mut collateral.collateral[0];
        asset.collateral_value.0 = collateral.net_equity.0;
        asset.total_quantity.0 = collateral.net_equity.0;
        asset.balance_notional.0 = collateral.net_equity.0;
        asset.available_quantity.0 = collateral.net_equity.0;
        asset.open_order_quantity.0 = Decimal::ZERO;
        (
            BackpackAccountSnapshot {
                policy: serde_json::from_value(fixture["policy"].clone()).unwrap(),
                balances: BTreeMap::new(),
                collateral,
                positions: Vec::new(),
                open_orders: Vec::new(),
                gaps: BTreeSet::new(),
                authenticated_namespace: Some(namespace.clone()),
                read_started_at_ms: 1000,
            },
            namespace,
        )
    }

    fn equity_snapshot(
        collateral_value: &str,
        pnl: &str,
        equity: &str,
        available: &str,
        locked: &str,
    ) -> (BackpackAccountSnapshot, BackpackClientIdNamespace) {
        let (mut snapshot, namespace) = snapshot();
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../test_data/account/synthetic.json")).unwrap();
        let mut position: BackpackPosition =
            serde_json::from_value(fixture["position"].clone()).unwrap();
        position.net_quantity.0 = Decimal::from_str_exact("0.00005").unwrap();
        position.pnl_unrealized.0 = Decimal::from_str_exact(pnl).unwrap();
        snapshot.positions.push(position);
        let value = Decimal::from_str_exact(collateral_value).unwrap();
        let asset = &mut snapshot.collateral.collateral[0];
        asset.collateral_value.0 = value;
        asset.total_quantity.0 = value;
        asset.balance_notional.0 = value;
        asset.available_quantity.0 = value;
        let pnl = Decimal::from_str_exact(pnl).unwrap();
        snapshot.collateral.assets_value.0 = value.checked_add(pnl.max(Decimal::ZERO)).unwrap();
        snapshot.collateral.liabilities_value.0 = pnl.min(Decimal::ZERO).abs();
        snapshot.collateral.pnl_unrealized.0 = pnl;
        snapshot.collateral.net_equity.0 = Decimal::from_str_exact(equity).unwrap();
        snapshot.collateral.net_equity_available.0 = Decimal::from_str_exact(available).unwrap();
        snapshot.collateral.net_equity_locked.0 = Decimal::from_str_exact(locked).unwrap();
        (snapshot, namespace)
    }

    #[rstest]
    #[case("5", "120", "105")]
    #[case("-5", "110", "95")]
    #[case("0", "115", "100")]
    fn production_facts_derive_equity_from_collateral_and_signed_pnl(
        #[case] pnl: &str,
        #[case] equity: &str,
        #[case] available: &str,
    ) {
        let (snapshot, namespace) = equity_snapshot("115", pnl, equity, available, "15");
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);

        let facts = snapshot
            .production_facts(&namespace, 1, 5000, &symbols)
            .unwrap();

        assert_eq!(
            facts.available_margin,
            Decimal::from_str_exact(available).unwrap()
        );
        assert_eq!(
            snapshot
                .collateral
                .assets_value
                .0
                .checked_sub(snapshot.collateral.liabilities_value.0),
            Some(snapshot.collateral.net_equity.0)
        );
    }

    #[rstest]
    #[case("0.2786106666666666666666666668")]
    #[case("0.2786106666666666666666666732")]
    fn production_facts_accept_only_representable_margin_rounding(#[case] locked: &str) {
        // The first case reproduces the 2026-10-13 private read-only observation,
        // its exact available + locked residual is -3.2e-27, the second is +3.2e-27.
        let available = "99.73143433333333333333333333";
        let (snapshot, namespace) =
            equity_snapshot("100.0091", "0.000945", "100.010045", available, locked);
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);

        let facts = snapshot
            .production_facts(&namespace, 1, 5000, &symbols)
            .unwrap();

        assert_eq!(
            facts.available_margin,
            Decimal::from_str_exact(available).unwrap()
        );
    }

    #[rstest]
    #[case("0.2786106666666666666666666568")]
    #[case("0.2786106666666666666666666768")]
    #[case("0.278611")]
    fn production_facts_reject_margin_difference_at_representable_precision(#[case] locked: &str) {
        let (snapshot, namespace) = equity_snapshot(
            "100.0091",
            "0.000945",
            "100.010045",
            "99.73143433333333333333333333",
            locked,
        );
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);

        let result = snapshot.production_facts(&namespace, 1, 5000, &symbols);

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("contradictory production available and locked margin")
        );
    }

    #[rstest]
    #[case("115", "5", "115", "100")]
    #[case("115", "-5", "115", "100")]
    #[case("114", "5", "120", "105")]
    fn production_facts_reject_contradictory_collateral_equity(
        #[case] collateral_value: &str,
        #[case] pnl: &str,
        #[case] equity: &str,
        #[case] available: &str,
    ) {
        let (snapshot, namespace) = equity_snapshot(collateral_value, pnl, equity, available, "15");
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);

        let result = snapshot.production_facts(&namespace, 1, 5000, &symbols);

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("contradictory production equity")
        );
    }

    #[rstest]
    #[case("multi-asset")]
    #[case("non-USDC")]
    #[case("borrow")]
    #[case("liabilities")]
    #[case("lend")]
    #[case("positive unsettled")]
    #[case("negative unsettled")]
    fn production_facts_keep_unsupported_collateral_rejected(#[case] unsupported: &str) {
        let (mut snapshot, namespace) = equity_snapshot("115", "5", "120", "105", "15");
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);
        match unsupported {
            "multi-asset" => {
                let mut second = snapshot.collateral.collateral[0].clone();
                second.symbol = "BTC".into();
                snapshot.collateral.collateral.push(second);
            }
            "non-USDC" => snapshot.collateral.collateral[0].symbol = "BTC".into(),
            "borrow" => snapshot.collateral.borrow_liability.0 = Decimal::ONE,
            "liabilities" => snapshot.collateral.liabilities_value.0 = Decimal::ONE,
            "lend" => snapshot.collateral.collateral[0].lend_quantity.0 = Decimal::ONE,
            "positive unsettled" => snapshot.collateral.unsettled_equity.0 = Decimal::ONE,
            "negative unsettled" => snapshot.collateral.unsettled_equity.0 = -Decimal::ONE,
            _ => unreachable!(),
        }

        let result = snapshot.production_facts(&namespace, 1, 5000, &symbols);

        assert!(result.is_err());
    }

    #[rstest]
    fn production_facts_reject_equity_overflow() {
        let (mut snapshot, namespace) = equity_snapshot("115", "5", "120", "105", "15");
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);
        snapshot.collateral.collateral[0].collateral_value.0 = Decimal::MAX;
        snapshot.collateral.pnl_unrealized.0 = Decimal::ONE;
        snapshot.collateral.net_equity.0 = Decimal::MAX;
        snapshot.collateral.net_equity_available.0 = Decimal::MAX;
        snapshot.collateral.net_equity_locked.0 = Decimal::ZERO;

        let result = snapshot.production_facts(&namespace, 1, 5000, &symbols);

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("contradictory production assets")
        );
    }

    #[rstest]
    fn production_facts_accept_real_negative_pnl_liability_without_unsupported_policy_gap() {
        let available = "99.72676766666666666666666667";
        let (snapshot, namespace) = equity_snapshot(
            "100.00524",
            "-0.000195",
            "100.005045",
            available,
            "0.2782773333333333333333333335",
        );
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);

        let facts = snapshot
            .production_facts(&namespace, 1, 5000, &symbols)
            .unwrap();

        assert_eq!(
            snapshot.collateral.assets_value.0,
            Decimal::from_str_exact("100.00524").unwrap()
        );
        assert_eq!(
            snapshot.collateral.liabilities_value.0,
            Decimal::from_str_exact("0.000195").unwrap()
        );
        assert_eq!(
            facts.available_margin,
            Decimal::from_str_exact(available).unwrap()
        );
        assert!(!unsupported_account_policy(
            &snapshot.policy,
            &snapshot.collateral
        ));
    }

    #[rstest]
    #[case("-0.000195", "0")]
    #[case("-0.000195", "0.000194")]
    #[case("-0.000195", "0.000196")]
    #[case("-0.000195", "-0.000195")]
    #[case("0.000195", "0.000195")]
    #[case("0", "0.000195")]
    fn production_facts_reject_unexplained_liabilities(#[case] pnl: &str, #[case] liability: &str) {
        let (mut snapshot, namespace) = equity_snapshot("115", pnl, "115", "100", "15");
        snapshot.collateral.liabilities_value.0 = Decimal::from_str_exact(liability).unwrap();
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);

        let result = snapshot.production_facts(&namespace, 1, 5000, &symbols);

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("liabilities are not fully explained by negative unrealized PnL")
        );
        assert!(unsupported_account_policy(
            &snapshot.policy,
            &snapshot.collateral
        ));
    }

    #[rstest]
    #[case("5", "120", "105", "115")]
    #[case("-5", "110", "95", "110")]
    fn production_facts_reject_assets_that_double_count_or_omit_pnl(
        #[case] pnl: &str,
        #[case] equity: &str,
        #[case] available: &str,
        #[case] assets: &str,
    ) {
        let (mut snapshot, namespace) = equity_snapshot("115", pnl, equity, available, "15");
        snapshot.collateral.assets_value.0 = Decimal::from_str_exact(assets).unwrap();
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);

        let result = snapshot.production_facts(&namespace, 1, 5000, &symbols);

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("contradictory production assets")
        );
    }

    #[rstest]
    fn production_facts_distinguish_borrowed_and_unsettled_collateral_from_pnl_liabilities() {
        let (mut snapshot, namespace) = equity_snapshot("115", "-5", "110", "95", "15");
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);
        snapshot.collateral.borrow_liability.0 = Decimal::ONE;

        let borrowed = snapshot.production_facts(&namespace, 1, 5000, &symbols);

        assert!(
            borrowed
                .unwrap_err()
                .to_string()
                .contains("borrowed production collateral")
        );
        assert!(unsupported_account_policy(
            &snapshot.policy,
            &snapshot.collateral
        ));

        snapshot.collateral.borrow_liability.0 = Decimal::ZERO;
        snapshot.collateral.unsettled_equity.0 = Decimal::ONE;

        let unsettled = snapshot.production_facts(&namespace, 1, 5000, &symbols);

        assert!(
            unsettled
                .unwrap_err()
                .to_string()
                .contains("unsettled production collateral")
        );
    }

    #[rstest]
    fn production_facts_reserve_full_notional_and_preserve_snapshot_start_time() {
        let (snapshot, namespace) = snapshot();
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);
        let facts = snapshot
            .production_facts(&namespace, 1, 5000, &symbols)
            .unwrap();
        assert_eq!(facts.observed_at_ms, 1000);
        assert_eq!(facts.available_margin, Decimal::from(105));
        assert_eq!(facts.margin_per_notional, Decimal::ONE);
        assert_eq!(facts.fee_buffer_per_notional, Decimal::new(2, 4));
        assert_eq!(
            facts.net_positions.values().copied().collect::<Vec<_>>(),
            [Decimal::ZERO]
        );
        assert!(snapshot.gaps.is_empty());
    }

    #[rstest]
    fn production_facts_reject_unbound_policy_collateral_and_numeric_identity_gaps() {
        let (mut snapshot, namespace) = snapshot();
        let symbols = BTreeSet::from(["BTC_USDC_PERP".into()]);
        snapshot.authenticated_namespace = None;
        assert!(
            snapshot
                .production_facts(&namespace, 1, 1000, &symbols)
                .is_err()
        );
        snapshot.authenticated_namespace = Some(namespace.clone());
        snapshot.policy.auto_lend = true;
        assert!(
            snapshot
                .production_facts(&namespace, 1, 1000, &symbols)
                .is_err()
        );
        snapshot.policy.auto_lend = false;
        snapshot.collateral.borrow_liability.0 = Decimal::ONE;
        assert!(
            snapshot
                .production_facts(&namespace, 1, 1000, &symbols)
                .is_err()
        );
        snapshot.collateral.borrow_liability.0 = Decimal::ZERO;
        snapshot.collateral.collateral[0].collateral_weight.0 = Decimal::new(99, 2);
        assert!(
            snapshot
                .production_facts(&namespace, 1, 1000, &symbols)
                .is_err()
        );
        snapshot.collateral.collateral[0].collateral_weight.0 = Decimal::ONE;
        snapshot.collateral.net_exposure_futures.0 = Decimal::ONE;
        assert!(
            snapshot
                .production_facts(&namespace, 1, 1000, &symbols)
                .is_err()
        );
        snapshot.collateral.net_exposure_futures.0 = Decimal::ZERO;
        snapshot.policy.futures_maker_fee.0 = Decimal::new(1, 28);
        snapshot.policy.futures_taker_fee.0 = Decimal::ZERO;
        assert!(
            snapshot
                .production_facts(&namespace, 1, 1000, &symbols)
                .is_err()
        );
        snapshot.policy.futures_taker_fee.0 = Decimal::from(2);
        let key = BackpackCredential::production(&STANDARD.encode([19; 32])).unwrap();
        let asserted =
            BackpackClientIdNamespace::production_credential(key.verifying_key(), Some("2"))
                .unwrap();
        assert!(
            snapshot
                .production_facts(&asserted, 1, 1000, &symbols)
                .is_err()
        );
        snapshot.gaps.insert(BackpackEvidenceGap::UnknownFields);
        assert!(
            snapshot
                .production_facts(&namespace, 1, 1000, &symbols)
                .is_err()
        );
    }
}
