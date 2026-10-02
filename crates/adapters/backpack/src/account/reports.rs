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

//! Exact native domain reports with retained raw venue evidence and local correlation.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce},
    identifiers::{AccountId, ClientOrderId, InstrumentId, PositionId, TradeId, VenueOrderId},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use super::{BackpackAccountError, BackpackEvidenceGap, models::*};
use crate::{
    identity::BackpackClientIdStore, instruments::BackpackInstrumentMetadata,
    provider::BackpackInstrumentProvider,
};

/// Caller-configured account labels and already namespace-validated local reservations.
/// The caller must use the same credential scope as the store. Account endpoints do
/// not attest this relationship; supplied account IDs never become verified identity.
#[derive(Debug)]
pub struct BackpackReportContext<'a> {
    pub account_id: AccountId,
    pub instruments: &'a BackpackInstrumentProvider,
    pub identities: &'a BackpackClientIdStore,
    /// Independent command-owner bindings; None never assigns a local native order ID.
    pub confirmed_orders: Option<&'a BackpackOrderBindings>,
    pub ts_init: UnixNanos,
}

/// Local durable correlation, not a venue idempotency or collision guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpackOrderOwnership {
    /// Candidate numeric correlation only, with no native-engine attribution.
    Reserved(ClientOrderId),
    /// Independently confirmed venue order binding with exact instrument and durable ID.
    Confirmed(ClientOrderId),
    External,
}
impl BackpackOrderOwnership {
    pub(crate) const fn client_id(self) -> Option<ClientOrderId> {
        match self {
            Self::Confirmed(id) => Some(id),
            Self::Reserved(_) | Self::External => None,
        }
    }
}
/// Independently confirmed command-owner bindings; account reads never populate this map.
/// The owner must persist/restore these bindings with its actual submission evidence.
/// Losing bindings conservatively returns local reservations to unverified candidates.
#[derive(Debug, Default)]
pub struct BackpackOrderBindings {
    orders: BTreeMap<VenueOrderId, (InstrumentId, ClientOrderId)>,
}
impl BackpackOrderBindings {
    /// Records the owner's authoritative venue order acknowledgement or independently
    /// verified full immutable-intent evidence, never a numeric clientId coincidence.
    /// The caller must establish that evidence before invoking this method. This
    /// library validates consistency but cannot attest arbitrary caller claims.
    ///
    /// # Errors
    ///
    /// Returns an error for an unreserved native ID or conflicting venue/native binding.
    pub fn confirm_acknowledged(
        &mut self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        identities: &BackpackClientIdStore,
    ) -> Result<(), BackpackAccountError> {
        if identities.venue_id(&client_order_id).is_none() {
            return Err(BackpackAccountError::InvalidField(
                "unreserved order binding",
            ));
        }
        let value = (instrument_id, client_order_id);
        if self
            .orders
            .get(&venue_order_id)
            .is_some_and(|existing| existing != &value)
            || self
                .orders
                .iter()
                .any(|(venue, (_, client))| *client == client_order_id && *venue != venue_order_id)
        {
            return Err(BackpackAccountError::Conflict);
        }
        self.orders.insert(venue_order_id, value);
        Ok(())
    }
}

impl BackpackReportContext<'_> {
    /// Looks up only original persisted numeric clientId. No unknown order is adopted.
    #[must_use]
    pub fn ownership(&self, client_id: Option<u32>) -> BackpackOrderOwnership {
        client_id
            .and_then(|id| self.identities.client_order_id(id))
            .map_or(
                BackpackOrderOwnership::External,
                BackpackOrderOwnership::Reserved,
            )
    }
    fn confirmed_ownership(
        &self,
        candidate: BackpackOrderOwnership,
        instrument_id: InstrumentId,
        venue_order_id: VenueOrderId,
        observed_client: Option<u32>,
        system_order: bool,
    ) -> Result<BackpackOrderOwnership, BackpackAccountError> {
        if system_order {
            return Ok(BackpackOrderOwnership::External);
        }
        let Some((bound_instrument, native_id)) = self
            .confirmed_orders
            .and_then(|bindings| bindings.orders.get(&venue_order_id))
        else {
            return Ok(candidate);
        };
        let numeric_id = self
            .identities
            .venue_id(native_id)
            .ok_or(BackpackAccountError::Conflict)?;
        if *bound_instrument != instrument_id || observed_client.is_some_and(|id| id != numeric_id)
        {
            return Err(BackpackAccountError::Conflict);
        }
        Ok(BackpackOrderOwnership::Confirmed(*native_id))
    }
    fn instrument(
        &self,
        symbol: &str,
    ) -> Result<&BackpackInstrumentMetadata, BackpackAccountError> {
        self.instruments
            .all()
            .find(|m| m.raw_symbol.as_str() == symbol)
            .ok_or(BackpackAccountError::Unsupported("instrument"))
    }
}

/// Wallet totals include staked funds; native trading balance deliberately excludes them.
#[derive(Clone, Debug)]
pub struct BackpackWalletReport {
    pub trading_balance: AccountBalance,
    pub staked: Money,
    pub wallet_total: Money,
}

/// Converts free/locked/staked independently without conflating wallet and equity.
///
/// # Errors
///
/// Returns an error for unknown currency, negative amounts, overflow or precision loss.
pub fn wallet_report(
    symbol: &str,
    balance: &BackpackWalletBalance,
) -> Result<BackpackWalletReport, BackpackAccountError> {
    let currency = currency(symbol)?;
    let free = nonnegative(balance.available.0, "wallet available")?;
    let locked = nonnegative(balance.locked.0, "wallet locked")?;
    let staked = nonnegative(balance.staked.0, "wallet staked")?;
    let trading = free
        .checked_add(locked)
        .ok_or(BackpackAccountError::InvalidField("wallet total"))?;
    let total = trading
        .checked_add(staked)
        .ok_or(BackpackAccountError::InvalidField("wallet total"))?;
    let trading_balance = AccountBalance::new_checked(
        money(trading, currency)?,
        money(locked, currency)?,
        money(free, currency)?,
    )
    .map_err(|_| BackpackAccountError::InvalidField("wallet balance"))?;
    Ok(BackpackWalletReport {
        trading_balance,
        staked: money(staked, currency)?,
        wallet_total: money(total, currency)?,
    })
}

/// Fill observations retain unknown system events and economics even without a tradeId.
#[derive(Clone, Debug)]
pub struct BackpackFillObservation {
    pub raw: BackpackFill,
    pub ownership: BackpackOrderOwnership,
    pub report: Option<FillReport>,
    pub gaps: BTreeSet<BackpackEvidenceGap>,
}

/// Converts only true trade identities and exact quantity/price/fee values.
///
/// Unknown system-order labels do not discard a valid economic fill. A missing
/// tradeId or unsupported side remains observable without fabricating a native report.
///
/// # Errors
///
/// Returns an error for invalid IDs, timestamps, clientId, economics or precision loss.
pub fn fill_report(
    raw: BackpackFill,
    context: &BackpackReportContext<'_>,
) -> Result<BackpackFillObservation, BackpackAccountError> {
    let numeric_client = raw
        .client_id
        .as_deref()
        .map(|id| {
            if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
                return Err(BackpackAccountError::InvalidField("fill clientId"));
            }
            id.parse::<u32>()
                .map_err(|_| BackpackAccountError::InvalidField("fill clientId"))
        })
        .transpose()?;
    // System orders are external even when a colliding clientId is present.
    let ownership = if raw.system_order_type.is_some() {
        BackpackOrderOwnership::External
    } else {
        context.ownership(numeric_client)
    };
    let mut gaps = BTreeSet::from([BackpackEvidenceGap::AccountIdentityUnverified]);
    if !raw.extra.is_empty() {
        gaps.insert(BackpackEvidenceGap::UnknownFields);
    }
    if raw
        .system_order_type
        .as_deref()
        .is_some_and(|s| !known_system(s))
    {
        gaps.insert(BackpackEvidenceGap::UnknownVenueState);
    }
    let side = order_side(&raw.side);
    if side.is_none() {
        gaps.insert(BackpackEvidenceGap::UnknownVenueState);
    }
    let metadata = context.instrument(&raw.symbol)?;
    let qty = quantity(raw.quantity.0, metadata, true)?;
    let px = price(raw.price.0, metadata)?;
    let commission = money(raw.fee.0, currency(&raw.fee_symbol)?)?;
    let event = fill_time(&raw.timestamp)?;
    let venue_order_id = VenueOrderId::new_checked(&raw.order_id)
        .map_err(|_| BackpackAccountError::InvalidField("fill orderId"))?;
    let ownership = context.confirmed_ownership(
        ownership,
        metadata.instrument_id,
        venue_order_id,
        numeric_client,
        raw.system_order_type.is_some(),
    )?;
    if matches!(ownership, BackpackOrderOwnership::Reserved(_)) {
        gaps.insert(BackpackEvidenceGap::OwnershipUnverified);
    }
    let report = match (raw.trade_id, side) {
        (Some(id), Some(side)) => Some(FillReport::new(
            context.account_id,
            metadata.instrument_id,
            venue_order_id,
            TradeId::new_checked(id.to_string())
                .map_err(|_| BackpackAccountError::InvalidField("tradeId"))?,
            side,
            qty,
            px,
            commission,
            if raw.is_maker {
                LiquiditySide::Maker
            } else {
                LiquiditySide::Taker
            },
            ownership.client_id(),
            None,
            event,
            context.ts_init,
            None,
        )),
        _ => {
            if raw.trade_id.is_none() {
                gaps.insert(BackpackEvidenceGap::MissingTradeId);
            }
            None
        }
    };
    Ok(BackpackFillObservation {
        raw,
        ownership,
        report,
        gaps,
    })
}

/// Venue lifecycle observation; missing/unknown states never become terminal by guess.
#[derive(Clone, Debug)]
pub struct BackpackOrderObservation {
    pub raw: BackpackOrder,
    pub ownership: BackpackOrderOwnership,
    pub report: Option<OrderStatusReport>,
    pub gaps: BTreeSet<BackpackEvidenceGap>,
}

/// Maps supported lifecycle fields while retaining unrepresentable orders for review.
/// Creation/last engine time remain zero because documented units/timezone are absent.
///
/// # Errors
///
/// Returns an error for malformed known economics, IDs or precision loss.
pub fn order_report(
    raw: BackpackOrder,
    context: &BackpackReportContext<'_>,
) -> Result<BackpackOrderObservation, BackpackAccountError> {
    let ownership = if raw.system_order_type.is_some() {
        BackpackOrderOwnership::External
    } else {
        context.ownership(raw.client_id)
    };
    let mut gaps = BTreeSet::from([
        BackpackEvidenceGap::AccountIdentityUnverified,
        BackpackEvidenceGap::OrderTimeUnknown,
    ]);
    if !raw.extra.is_empty() {
        gaps.insert(BackpackEvidenceGap::UnknownFields);
    }
    if raw
        .system_order_type
        .as_deref()
        .is_some_and(|s| !known_system(s))
    {
        gaps.insert(BackpackEvidenceGap::UnknownVenueState);
    }
    let kind = match raw.order_type.as_str() {
        "Market" => Some(OrderType::Market),
        "Limit" => Some(OrderType::Limit),
        _ => None,
    };
    let status = match raw.status.as_str() {
        "New" => Some(OrderStatus::Accepted),
        "PartiallyFilled" => Some(OrderStatus::PartiallyFilled),
        "Filled" => Some(OrderStatus::Filled),
        "Cancelled" => Some(OrderStatus::Canceled),
        "Expired" => Some(OrderStatus::Expired),
        _ => None,
    };
    let tif = match raw.time_in_force.as_str() {
        "GTC" => Some(TimeInForce::Gtc),
        "IOC" => Some(TimeInForce::Ioc),
        "FOK" => Some(TimeInForce::Fok),
        _ => None,
    };
    let side = order_side(&raw.side);
    let metadata = context.instrument(&raw.symbol)?;
    let venue_id = VenueOrderId::new_checked(&raw.id)
        .map_err(|_| BackpackAccountError::InvalidField("order id"))?;
    let ownership = context.confirmed_ownership(
        ownership,
        metadata.instrument_id,
        venue_id,
        raw.client_id,
        raw.system_order_type.is_some(),
    )?;
    if matches!(ownership, BackpackOrderOwnership::Reserved(_)) {
        gaps.insert(BackpackEvidenceGap::OwnershipUnverified);
    }
    let report = match (kind, status, tif, side, raw.quantity, raw.executed_quantity) {
        (Some(kind), Some(status), Some(tif), Some(side), Some(qty), Some(filled))
            if raw.quote_quantity.is_none()
                && raw.reduce_only.is_some()
                && (kind == OrderType::Market || raw.post_only.is_some())
                && !raw.extra.iter().any(|(key, value)| {
                    !value.is_null()
                        && (key.starts_with("trigger")
                            || key.starts_with("stopLoss")
                            || key.starts_with("takeProfit")
                            || key == "strategyId")
                }) =>
        {
            let qty = quantity(qty.0, metadata, true)?;
            let filled = quantity(filled.0, metadata, false)?;
            if filled > qty || (status == OrderStatus::Filled && filled != qty) {
                return Err(BackpackAccountError::InvalidField("executedQuantity"));
            }
            let limit = raw.price.map(|px| price(px.0, metadata)).transpose()?;
            if kind == OrderType::Limit && limit.is_none() {
                return Err(BackpackAccountError::InvalidField("limit price"));
            }
            let mut report = OrderStatusReport::new(
                context.account_id,
                metadata.instrument_id,
                ownership.client_id(),
                venue_id,
                Some(side),
                kind,
                tif,
                status,
                qty,
                filled,
                UnixNanos::from(0),
                UnixNanos::from(0),
                context.ts_init,
                None,
            );
            report.price = limit;
            report.post_only = raw.post_only.unwrap_or(false);
            report.reduce_only = raw.reduce_only.unwrap_or(false);
            report.cancel_reason = raw.expiry_reason.clone();
            Some(report)
        }
        _ => {
            gaps.insert(BackpackEvidenceGap::UnknownVenueState);
            None
        }
    };
    Ok(BackpackOrderObservation {
        raw,
        ownership,
        report,
        gaps,
    })
}

/// Converts an explicitly returned signed position; omitted symbols remain unobserved.
///
/// # Errors
///
/// Returns an error for unsupported symbols, invalid IDs, or quantity precision loss.
pub fn position_report(
    raw: &BackpackPosition,
    context: &BackpackReportContext<'_>,
) -> Result<PositionStatusReport, BackpackAccountError> {
    let metadata = context.instrument(&raw.symbol)?;
    let signed = raw.net_quantity.0;
    let side = if signed > Decimal::ZERO {
        PositionSide::Long
    } else if signed < Decimal::ZERO {
        PositionSide::Short
    } else {
        PositionSide::Flat
    };
    let qty = quantity(signed.abs(), metadata, false)?;
    let position_id = PositionId::new_checked(&raw.position_id)
        .map_err(|_| BackpackAccountError::InvalidField("positionId"))?;
    Ok(PositionStatusReport::new(
        context.account_id,
        metadata.instrument_id,
        side,
        qty,
        UnixNanos::from(0),
        context.ts_init,
        None,
        Some(position_id),
        Some(raw.entry_price.0),
    ))
}

/// Funding remains a signed exact raw cashflow until denomination/timezone are proved.
#[derive(Clone, Debug)]
pub struct BackpackFundingObservation {
    pub raw: BackpackFundingPayment,
    pub gaps: BTreeSet<BackpackEvidenceGap>,
}
impl From<BackpackFundingPayment> for BackpackFundingObservation {
    fn from(raw: BackpackFundingPayment) -> Self {
        let mut gaps = BTreeSet::from([
            BackpackEvidenceGap::AccountIdentityUnverified,
            BackpackEvidenceGap::FundingCurrencyUnknown,
            BackpackEvidenceGap::FundingTimezoneUnknown,
        ]);
        if !raw.extra.is_empty() {
            gaps.insert(BackpackEvidenceGap::UnknownFields);
        }
        Self { raw, gaps }
    }
}

pub(crate) fn fill_time(raw: &str) -> Result<UnixNanos, BackpackAccountError> {
    // Only fills explicitly document UTC. Do not reuse this for naive funding/order dates.
    let datetime = jiff::civil::DateTime::from_str(raw)
        .map_err(|_| BackpackAccountError::InvalidField("fill timestamp"))?;
    let timestamp = datetime
        .to_zoned(jiff::tz::TimeZone::UTC)
        .map_err(|_| BackpackAccountError::InvalidField("fill timestamp"))?
        .timestamp();
    let nanos = u64::try_from(timestamp.as_nanosecond())
        .map_err(|_| BackpackAccountError::InvalidField("fill timestamp"))?;
    Ok(UnixNanos::from(nanos))
}
fn order_side(raw: &str) -> Option<OrderSide> {
    match raw {
        "Bid" => Some(OrderSide::Buy),
        "Ask" => Some(OrderSide::Sell),
        _ => None,
    }
}
fn known_system(raw: &str) -> bool {
    [
        "CollateralConversion",
        "FutureExpiry",
        "LiquidatePositionOnAdl",
        "LiquidatePositionOnBook",
        "LiquidatePositionOnBackstop",
        "OrderBookClosed",
    ]
    .contains(&raw)
}
fn currency(raw: &str) -> Result<Currency, BackpackAccountError> {
    Currency::from_str(raw).map_err(|_| BackpackAccountError::Unsupported("currency"))
}
fn nonnegative(value: Decimal, field: &'static str) -> Result<Decimal, BackpackAccountError> {
    if value < Decimal::ZERO {
        Err(BackpackAccountError::InvalidField(field))
    } else {
        Ok(value)
    }
}
fn money(value: Decimal, currency: Currency) -> Result<Money, BackpackAccountError> {
    let result = Money::from_decimal(value, currency)
        .map_err(|_| BackpackAccountError::InvalidField("money"))?;
    if result.as_decimal() != value {
        return Err(BackpackAccountError::InvalidField("money precision loss"));
    }
    Ok(result)
}
fn quantity(
    value: Decimal,
    metadata: &BackpackInstrumentMetadata,
    positive: bool,
) -> Result<Quantity, BackpackAccountError> {
    if value < Decimal::ZERO || (positive && value == Decimal::ZERO) {
        return Err(BackpackAccountError::InvalidField("quantity"));
    }
    let qty = Quantity::from_decimal_dp(value, metadata.size_increment.precision)
        .map_err(|_| BackpackAccountError::InvalidField("quantity"))?;
    if qty.as_decimal() != value {
        return Err(BackpackAccountError::InvalidField(
            "quantity precision loss",
        ));
    }
    Ok(qty)
}
fn price(
    value: Decimal,
    metadata: &BackpackInstrumentMetadata,
) -> Result<Price, BackpackAccountError> {
    if value <= Decimal::ZERO {
        return Err(BackpackAccountError::InvalidField("price"));
    }
    let px = Price::from_decimal_dp(value, metadata.price_increment.precision)
        .map_err(|_| BackpackAccountError::InvalidField("price"))?;
    if px.as_decimal() != value {
        return Err(BackpackAccountError::InvalidField("price precision loss"));
    }
    Ok(px)
}
