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

//! Explicit finite authority and loopback peer observations; no production readiness proof.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
};

use nautilus_model::{
    data::QuoteTick,
    enums::{OrderSide, OrderType},
    identifiers::InstrumentId,
};
use rust_decimal::Decimal;

use super::{
    BackpackExecutionError, BackpackExecutionErrorKind,
    command::BackpackOrderSpec,
    owner::{OrderRecord, OwnerState},
};
use crate::{identity::BackpackClientIdNamespace, instruments::BackpackInstrumentMetadata};

/// Caller-authorized finite limits and permissions, with no permissive defaults.
#[derive(Clone, Debug)]
pub struct BackpackExecutionAuthority {
    pub expires_at_ms: u64,
    pub max_account_age_ms: u64,
    pub max_market_age_ms: u64,
    pub max_order_notional: Decimal,
    pub max_reserved_notional: Decimal,
    pub max_reserved_margin: Decimal,
    pub max_unsettled_orders: usize,
    pub allow_new_risk: bool,
    pub allow_reduction: bool,
    pub allow_owned_cancel: bool,
}
impl BackpackExecutionAuthority {
    pub(crate) fn validate(&self) -> Result<(), BackpackExecutionError> {
        if self.expires_at_ms == 0
            || self.expires_at_ms > i64::MAX as u64
            || self.max_account_age_ms == 0
            || self.max_market_age_ms == 0
            || self.max_order_notional <= Decimal::ZERO
            || self.max_reserved_notional < self.max_order_notional
            || self.max_reserved_margin <= Decimal::ZERO
            || self.max_unsettled_orders == 0
        {
            return Err(BackpackExecutionErrorKind::Validation.into());
        }
        Ok(())
    }
}

/// Accepted facts from the explicit local protocol peer, not a production account attestation.
#[derive(Clone, Debug)]
pub struct BackpackLoopbackAccountFacts {
    pub namespace: BackpackClientIdNamespace,
    pub generation: u64,
    pub observed_at_ms: u64,
    /// Peer-confirmed available USDC margin, separate from wallet balances.
    pub available_margin: Decimal,
    /// Explicit local peer economic model; not inferred from public venue metadata.
    pub margin_per_notional: Decimal,
    pub fee_buffer_per_notional: Decimal,
    pub economics_reference: String,
    /// Complete signed net positions for the allowlist; absent entries are unknown.
    pub net_positions: BTreeMap<InstrumentId, Decimal>,
    /// Each must be explicitly false for the restricted account policy.
    pub auto_borrow: bool,
    pub auto_lend: bool,
    pub auto_repay: bool,
    pub liquidating: bool,
    /// The owner has accepted complete, non-conflicting local peer evidence.
    pub complete: bool,
}

/// Validated metadata and a current two-sided quote from the same loopback generation.
#[derive(Clone, Debug)]
pub struct BackpackLoopbackMarketFacts {
    pub generation: u64,
    pub metadata: BackpackInstrumentMetadata,
    pub quote: QuoteTick,
}

/// Control handle for invalidating queued writes and accepting new peer observations.
#[derive(Clone)]
pub struct BackpackExecutionGuard {
    pub(crate) state: Arc<Mutex<OwnerState>>,
}
impl fmt::Debug for BackpackExecutionGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpackExecutionGuard")
            .finish_non_exhaustive()
    }
}
impl BackpackExecutionGuard {
    /// Starts an explicitly authenticated local peer session and clears prior observations.
    ///
    /// # Errors
    /// Returns an error for stopped/poisoned state, wrong namespace, or a reused generation.
    pub fn begin_session(
        &self,
        namespace: &BackpackClientIdNamespace,
        generation: u64,
    ) -> Result<(), BackpackExecutionError> {
        let mut state = self.lock()?;
        if state.stopped || state.poisoned {
            return Err(BackpackExecutionErrorKind::Stopped.into());
        }
        if namespace != &state.namespace || generation <= state.generation {
            return Err(BackpackExecutionErrorKind::Identity.into());
        }
        state.generation = generation;
        state.session_active = true;
        state.account = None;
        state.markets.clear();
        state.public = None;
        Ok(())
    }
    pub(crate) fn bind_public(
        &self,
        telemetry: crate::telemetry::BackpackPublicTelemetry,
    ) -> Result<(), BackpackExecutionError> {
        let (observed, token) = telemetry.admission_snapshot();
        if observed.run_id.is_none()
            || !observed.connected
            || !observed.metadata_ready
            || observed.stale_reason.is_some()
        {
            return Err(BackpackExecutionErrorKind::Readiness.into());
        }
        let mut state = self.lock()?;
        state.public = Some(PublicAdmission { telemetry, token });
        Ok(())
    }

    /// Accepts explicitly complete local peer account facts. This never verifies a real account.
    ///
    /// # Errors
    /// Returns an error and invalidates account trust for wrong scope, unsupported policy,
    /// missing positions, unknown economics, or stale/future observations.
    pub fn update_account(
        &self,
        facts: BackpackLoopbackAccountFacts,
        now_ms: u64,
    ) -> Result<(), BackpackExecutionError> {
        let mut state = self.lock()?;
        state.account = None;
        state.check_session(now_ms)?;
        if facts.namespace != state.namespace
            || facts.generation != state.generation
            || !facts.complete
            || facts.auto_borrow
            || facts.auto_lend
            || facts.auto_repay
            || facts.liquidating
            || facts.available_margin < Decimal::ZERO
            || facts.margin_per_notional < Decimal::ZERO
            || facts.margin_per_notional > Decimal::ONE
            || facts.fee_buffer_per_notional < Decimal::ZERO
            || facts.fee_buffer_per_notional > Decimal::ONE
            || facts.economics_reference.trim().is_empty()
            || !fresh(
                facts.observed_at_ms,
                now_ms,
                state.authority.max_account_age_ms,
            )
            || state.config.symbols().iter().any(|symbol| {
                !facts
                    .net_positions
                    .contains_key(&InstrumentId::from(format!("{symbol}.BACKPACK")))
            })
        {
            return Err(BackpackExecutionErrorKind::Readiness.into());
        }
        state.account = Some(facts);
        Ok(())
    }
    /// Accepts a validated current local observation; bad replacement invalidates that market.
    ///
    /// # Errors
    /// Returns an error for wrong generation, crossed/empty quotes, invalid metadata or freshness.
    pub fn update_market(
        &self,
        facts: BackpackLoopbackMarketFacts,
        now_ms: u64,
    ) -> Result<(), BackpackExecutionError> {
        let mut state = self.lock()?;
        state.markets.remove(&facts.metadata.instrument_id);
        state.check_session(now_ms)?;
        let quote = &facts.quote;
        if facts.generation != state.generation
            || quote.instrument_id != facts.metadata.instrument_id
            || quote.bid_price.raw <= 0
            || quote.ask_price.raw <= 0
            || quote.bid_price >= quote.ask_price
            || quote.bid_size.is_zero()
            || quote.ask_size.is_zero()
            || [quote.bid_price, quote.ask_price].iter().any(|price| {
                price
                    .as_decimal()
                    .checked_rem(facts.metadata.price_increment.as_decimal())
                    != Some(Decimal::ZERO)
            })
            || [quote.bid_size, quote.ask_size].iter().any(|size| {
                size.as_decimal()
                    .checked_rem(facts.metadata.size_increment.as_decimal())
                    != Some(Decimal::ZERO)
            })
            || !fresh(
                quote.ts_event.as_u64() / 1_000_000,
                now_ms,
                state.authority.max_market_age_ms,
            )
            || !fresh(
                quote.ts_init.as_u64() / 1_000_000,
                now_ms,
                state.authority.max_market_age_ms,
            )
            || !fresh(
                facts.metadata.received_at.as_u64() / 1_000_000,
                now_ms,
                state.authority.max_market_age_ms,
            )
        {
            return Err(BackpackExecutionErrorKind::Readiness.into());
        }
        state.markets.insert(facts.metadata.instrument_id, facts);
        Ok(())
    }
    pub(crate) fn invalidate_account(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.account = None;
        }
    }
    /// Replaces finite authority; outstanding reservations remain held.
    ///
    /// # Errors
    /// Returns an error for invalid limits or a stopped/poisoned owner.
    pub fn update_authority(
        &self,
        authority: BackpackExecutionAuthority,
    ) -> Result<(), BackpackExecutionError> {
        authority.validate()?;
        let mut state = self.lock()?;
        if state.stopped || state.poisoned {
            return Err(BackpackExecutionErrorKind::Stopped.into());
        }
        state.authority = authority;
        Ok(())
    }

    /// Freezes session trust before reconnect/reconciliation. Queued final checks then refuse.
    pub fn invalidate(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.session_active = false;
            state.account = None;
            state.markets.clear();
        }
    }
    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, OwnerState>, BackpackExecutionError> {
        self.state
            .lock()
            .map_err(|_| BackpackExecutionErrorKind::Stopped.into())
    }
}

impl OwnerState {
    pub(crate) fn check_session(&self, now_ms: u64) -> Result<(), BackpackExecutionError> {
        if self.stopped || self.poisoned {
            return Err(BackpackExecutionErrorKind::Stopped.into());
        }
        if !self.session_active
            || now_ms >= self.authority.expires_at_ms
            || now_ms > i64::MAX as u64
        {
            return Err(BackpackExecutionErrorKind::Readiness.into());
        }
        Ok(())
    }
    pub(crate) fn reservation(
        &self,
        spec: &BackpackOrderSpec,
        now_ms: u64,
        exclude: Option<&OrderRecord>,
    ) -> Result<(Decimal, Decimal), BackpackExecutionError> {
        self.check_session(now_ms)?;
        let account = self
            .account
            .as_ref()
            .ok_or(BackpackExecutionErrorKind::Readiness)?;
        let market = self
            .markets
            .get(&spec.instrument_id)
            .ok_or(BackpackExecutionErrorKind::Readiness)?;
        if !fresh(
            account.observed_at_ms,
            now_ms,
            self.authority.max_account_age_ms,
        ) || !fresh(
            market.quote.ts_event.as_u64() / 1_000_000,
            now_ms,
            self.authority.max_market_age_ms,
        ) || !fresh(
            market.quote.ts_init.as_u64() / 1_000_000,
            now_ms,
            self.authority.max_market_age_ms,
        ) || !fresh(
            market.metadata.received_at.as_u64() / 1_000_000,
            now_ms,
            self.authority.max_market_age_ms,
        ) {
            return Err(BackpackExecutionErrorKind::Readiness.into());
        }
        if let Some(public) = &self.public {
            public.validate(market)?;
        }
        spec.validate(&market.metadata, &self.config)?;
        let records = self.records.values().filter(|record| {
            record.holds_capacity()
                && exclude.is_none_or(|excluded| {
                    excluded.spec.client_order_id != record.spec.client_order_id
                })
        });
        let mut notional = Decimal::ZERO;
        let mut margin = Decimal::ZERO;
        let mut reductions = Decimal::ZERO;
        let mut count = 0;
        for record in records {
            count += 1;
            notional = add(notional, record.notional)?;
            margin = add(margin, record.margin)?;
            if record.spec.reduce_only
                && record.spec.instrument_id == spec.instrument_id
                && record.spec.side == spec.side
            {
                reductions = add(reductions, record.spec.quantity)?;
            }
            if !spec.reduce_only && record.is_unknown() {
                return Err(BackpackExecutionErrorKind::Readiness.into());
            }
        }
        if count >= self.authority.max_unsettled_orders {
            return Err(BackpackExecutionErrorKind::Capacity.into());
        }
        if spec.reduce_only {
            if !self.authority.allow_reduction {
                return Err(BackpackExecutionErrorKind::Readiness.into());
            }
            let position = *account
                .net_positions
                .get(&spec.instrument_id)
                .ok_or(BackpackExecutionErrorKind::Readiness)?;
            if !((position > Decimal::ZERO && spec.side == OrderSide::Sell)
                || (position < Decimal::ZERO && spec.side == OrderSide::Buy))
                || add(reductions, spec.quantity)? > position.abs()
            {
                return Err(BackpackExecutionErrorKind::Capacity.into());
            }
            return Ok((Decimal::ZERO, Decimal::ZERO));
        }
        if !self.authority.allow_new_risk
            || spec.order_type != OrderType::Limit
            || spec.side != OrderSide::Buy
        {
            return Err(BackpackExecutionErrorKind::UnsupportedCommand.into());
        }
        let cost = multiply(
            spec.quantity,
            spec.price.ok_or(BackpackExecutionErrorKind::Validation)?,
        )?;
        let required_margin = multiply(
            cost,
            add(account.margin_per_notional, account.fee_buffer_per_notional)?,
        )?;
        if cost > self.authority.max_order_notional
            || add(notional, cost)? > self.authority.max_reserved_notional
            || add(margin, required_margin)? > self.authority.max_reserved_margin
            || add(margin, required_margin)? > account.available_margin
        {
            return Err(BackpackExecutionErrorKind::Capacity.into());
        }
        Ok((cost, required_margin))
    }
}
fn fresh(observed_ms: u64, now_ms: u64, max_age_ms: u64) -> bool {
    now_ms
        .checked_sub(observed_ms)
        .is_some_and(|age| age <= max_age_ms)
}
pub(crate) fn add(left: Decimal, right: Decimal) -> Result<Decimal, BackpackExecutionError> {
    let left = left.normalize();
    let right = right.normalize();
    let scale = left.scale().max(right.scale());
    let left = left
        .mantissa()
        .checked_mul(
            10_i128
                .checked_pow(scale - left.scale())
                .ok_or(BackpackExecutionErrorKind::Capacity)?,
        )
        .ok_or(BackpackExecutionErrorKind::Capacity)?;
    let right = right
        .mantissa()
        .checked_mul(
            10_i128
                .checked_pow(scale - right.scale())
                .ok_or(BackpackExecutionErrorKind::Capacity)?,
        )
        .ok_or(BackpackExecutionErrorKind::Capacity)?;
    Decimal::try_from_i128_with_scale(
        left.checked_add(right)
            .ok_or(BackpackExecutionErrorKind::Capacity)?,
        scale,
    )
    .map_err(|_| BackpackExecutionErrorKind::Capacity.into())
}
fn multiply(left: Decimal, right: Decimal) -> Result<Decimal, BackpackExecutionError> {
    let left = left.normalize();
    let right = right.normalize();
    Decimal::try_from_i128_with_scale(
        left.mantissa()
            .checked_mul(right.mantissa())
            .ok_or(BackpackExecutionErrorKind::Capacity)?,
        left.scale() + right.scale(),
    )
    .map_err(|_| BackpackExecutionErrorKind::Capacity.into())
}

#[derive(Clone, Debug)]
pub(crate) struct PublicAdmission {
    telemetry: crate::telemetry::BackpackPublicTelemetry,
    token: crate::telemetry::BackpackPublicAdmissionToken,
}
impl PublicAdmission {
    fn validate(&self, market: &BackpackLoopbackMarketFacts) -> Result<(), BackpackExecutionError> {
        let (current, token) = self.telemetry.admission_snapshot();
        let symbol = market.metadata.raw_symbol.as_str();
        if token != self.token
            || !current.connected
            || !current.metadata_ready
            || current.stale_reason.is_some()
            || current.quotes_fresh.get(symbol) != Some(&true)
            || current
                .quote_event_ns
                .get(symbol)
                .and_then(|value| value.parse::<u64>().ok())
                != Some(market.quote.ts_event.as_u64())
            || current
                .quote_received_ns
                .get(symbol)
                .and_then(|value| value.parse::<u64>().ok())
                != Some(market.quote.ts_init.as_u64())
        {
            return Err(BackpackExecutionErrorKind::Readiness.into());
        }
        Ok(())
    }
}
