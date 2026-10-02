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

//! Exact public metadata parsing and checked time-unit conversion.

use std::str::FromStr;

use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    types::{Currency, Price, Quantity, fixed::FIXED_PRECISION},
};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    config::{BackpackConfig, BackpackConfigError},
    instruments::BackpackInstrumentMetadata,
    models::BackpackMarket,
};

/// Validates official market metadata and parses exact Nautilus field values.
///
/// # Errors
///
/// Returns an error for unlisted or unsupported products, inactive/hidden markets,
/// inconsistent identity, unknown currency, missing/invalid filters, off-grid bounds,
/// representational precision loss, malformed funding metadata, or domain failures.
pub fn parse_market(
    market: &BackpackMarket,
    config: &BackpackConfig,
    received_at: UnixNanos,
) -> Result<BackpackInstrumentMetadata, BackpackInstrumentError> {
    config.validate_market(&market.symbol, &market.market_type, &market.quote_symbol)?;

    if market.order_book_state != "Open" || !market.visible {
        return Err(BackpackInstrumentError::InactiveMarket {
            state: market.order_book_state.clone(),
            visible: market.visible,
        });
    }

    if market.symbol != format!("{}_{}_PERP", market.base_symbol, market.quote_symbol) {
        return Err(BackpackInstrumentError::InconsistentIdentity);
    }

    let base_currency = Currency::from_str(&market.base_symbol)
        .map_err(|_| BackpackInstrumentError::UnknownCurrency(market.base_symbol.clone()))?;
    let quote_currency = Currency::from_str(&market.quote_symbol)
        .map_err(|_| BackpackInstrumentError::UnknownCurrency(market.quote_symbol.clone()))?;
    let tick = positive_decimal(&market.filters.price.tick_size, "tickSize")?;
    let step = positive_decimal(&market.filters.quantity.step_size, "stepSize")?;
    let min_price = positive_decimal(&market.filters.price.min_price, "minPrice")?;
    let min_quantity = positive_decimal(&market.filters.quantity.min_quantity, "minQuantity")?;
    let max_price = market
        .filters
        .price
        .max_price
        .as_deref()
        .map(|s| positive_decimal(s, "maxPrice"))
        .transpose()?;
    let max_quantity = market
        .filters
        .quantity
        .max_quantity
        .as_deref()
        .map(|s| positive_decimal(s, "maxQuantity"))
        .transpose()?;
    validate_bounds(min_price, max_price, tick, "price")?;
    validate_bounds(min_quantity, max_quantity, step, "quantity")?;
    let price_precision = tick.normalize().scale() as u8;
    let size_precision = step.normalize().scale() as u8;
    let price_increment = exact_price(tick, price_precision)?;
    let size_increment = exact_quantity(step, size_precision)?;
    let min_price = exact_price(min_price, price_precision)?;
    let min_quantity = exact_quantity(min_quantity, size_precision)?;
    let max_price = max_price
        .map(|value| exact_price(value, price_precision))
        .transpose()?;
    let max_quantity = max_quantity
        .map(|value| exact_quantity(value, size_precision))
        .transpose()?;

    if market.funding_interval == Some(0) {
        return Err(BackpackInstrumentError::InvalidField("fundingInterval"));
    }

    let lower = market
        .funding_rate_lower_bound
        .as_deref()
        .map(|s| decimal(s, "fundingRateLowerBound"))
        .transpose()?;
    let upper = market
        .funding_rate_upper_bound
        .as_deref()
        .map(|s| decimal(s, "fundingRateUpperBound"))
        .transpose()?;

    if let (Some(lower), Some(upper)) = (lower, upper)
        && lower > upper
    {
        return Err(BackpackInstrumentError::InvalidField("funding rate bounds"));
    }

    let instrument_id = InstrumentId::from_str(&format!("{}.BACKPACK", market.symbol))
        .map_err(|e| BackpackInstrumentError::Domain(e.to_string()))?;
    let raw_symbol = Symbol::new_checked(&market.symbol)
        .map_err(|e| BackpackInstrumentError::Domain(e.to_string()))?;
    Ok(BackpackInstrumentMetadata {
        instrument_id,
        raw_symbol,
        base_currency,
        quote_currency,
        price_increment,
        size_increment,
        min_quantity,
        max_quantity,
        min_price,
        max_price,
        funding_interval_ms: market.funding_interval,
        funding_rate_lower_bound_bps: lower,
        funding_rate_upper_bound_bps: upper,
        received_at,
        raw_metadata: serde_json::to_value(market).map_err(BackpackInstrumentError::Json)?,
    })
}

/// Converts a documented Unix microsecond timestamp into nanoseconds exactly.
///
/// # Errors
///
/// Returns an error if multiplication exceeds the `UnixNanos` range.
pub fn unix_microseconds_to_nanos(value: u64) -> Result<UnixNanos, BackpackInstrumentError> {
    value
        .checked_mul(1_000)
        .map(UnixNanos::from)
        .ok_or(BackpackInstrumentError::InvalidField(
            "microsecond timestamp overflow",
        ))
}

/// Converts basis points into a fractional rate without silent rounding.
///
/// # Errors
///
/// Returns an error if the result cannot be represented exactly by `Decimal`.
pub fn basis_points_to_ratio(value: Decimal) -> Result<Decimal, BackpackInstrumentError> {
    let denominator = Decimal::from(10_000);
    let ratio = value
        .checked_div(denominator)
        .ok_or(BackpackInstrumentError::InvalidField(
            "funding rate ratio overflow",
        ))?;

    if ratio.checked_mul(denominator) != Some(value) {
        return Err(BackpackInstrumentError::InvalidField(
            "funding rate ratio precision loss",
        ));
    }

    Ok(ratio)
}

/// A market, field, or construction outside the verified metadata contract.
#[derive(Debug, Error)]
pub enum BackpackInstrumentError {
    /// Invalid configuration/product scope.
    #[error(transparent)]
    Config(#[from] BackpackConfigError),
    /// A malformed public JSON response.
    #[error("invalid Backpack market JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Market trading state is outside the conservative active scope.
    #[error("inactive Backpack market: state={state}, visible={visible}")]
    InactiveMarket {
        /// Venue state, preserved even when unknown.
        state: String,
        /// Venue visibility.
        visible: bool,
    },
    /// Native symbol and the actual currency fields disagree.
    #[error("Backpack native symbol and currency metadata disagree")]
    InconsistentIdentity,
    /// Currency facts have not been explicitly registered with Nautilus.
    #[error("unknown Backpack currency: {0}")]
    UnknownCurrency(String),
    /// A required field is malformed, non-positive, inconsistent, or not representable.
    #[error("invalid Backpack metadata field: {0}")]
    InvalidField(&'static str),
    /// Invalid domain construction.
    #[error("Backpack Nautilus construction failed: {0}")]
    Domain(String),
    /// Explicit economics are required; unknown values must not become zero defaults.
    #[error("Backpack instrument construction requires explicitly sourced margin and fee inputs")]
    MissingEconomics,
    /// Economic inputs lack a valid range or source.
    #[error("invalid Backpack instrument economics or provenance")]
    InvalidEconomics,
    /// An allowlisted symbol occurs more than once in a refresh.
    #[error("duplicate Backpack market: {0}")]
    DuplicateMarket(String),
    /// A complete refresh omitted an allowlisted symbol.
    #[error("Backpack refresh omitted allowlisted market: {0}")]
    MissingMarket(String),
}

fn decimal(value: &str, field: &'static str) -> Result<Decimal, BackpackInstrumentError> {
    // The venue declares decimal strings, not scientific notation, whitespace, or JSON numbers.
    let digits = value.strip_prefix('-').unwrap_or(value);
    let valid = !digits.is_empty()
        && digits.bytes().filter(|b| *b == b'.').count() <= 1
        && digits.bytes().all(|b| b.is_ascii_digit() || b == b'.')
        && !digits.starts_with('.')
        && !digits.ends_with('.');

    if !valid {
        return Err(BackpackInstrumentError::InvalidField(field));
    }

    Decimal::from_str_exact(value).map_err(|_| BackpackInstrumentError::InvalidField(field))
}

fn positive_decimal(value: &str, field: &'static str) -> Result<Decimal, BackpackInstrumentError> {
    let value = decimal(value, field)?;

    if value <= Decimal::ZERO {
        return Err(BackpackInstrumentError::InvalidField(field));
    }

    Ok(value)
}

fn validate_bounds(
    min: Decimal,
    max: Option<Decimal>,
    increment: Decimal,
    field: &'static str,
) -> Result<(), BackpackInstrumentError> {
    let min_remainder = min
        .checked_rem(increment)
        .ok_or(BackpackInstrumentError::InvalidField(field))?;

    if min_remainder != Decimal::ZERO {
        return Err(BackpackInstrumentError::InvalidField(field));
    }

    if let Some(max) = max
        && (max < min || max.checked_rem(increment) != Some(Decimal::ZERO))
    {
        return Err(BackpackInstrumentError::InvalidField(field));
    }

    Ok(())
}

fn exact_price(value: Decimal, precision: u8) -> Result<Price, BackpackInstrumentError> {
    if precision > FIXED_PRECISION {
        return Err(BackpackInstrumentError::InvalidField("price precision"));
    }

    let price = Price::from_decimal_dp(value, precision)
        .map_err(|e| BackpackInstrumentError::Domain(e.to_string()))?;

    if price.as_decimal() != value {
        return Err(BackpackInstrumentError::InvalidField(
            "price precision loss",
        ));
    }

    Ok(price)
}

fn exact_quantity(value: Decimal, precision: u8) -> Result<Quantity, BackpackInstrumentError> {
    if precision > FIXED_PRECISION {
        return Err(BackpackInstrumentError::InvalidField("quantity precision"));
    }

    let quantity = Quantity::from_decimal_dp(value, precision)
        .map_err(|e| BackpackInstrumentError::Domain(e.to_string()))?;

    if quantity.as_decimal() != value {
        return Err(BackpackInstrumentError::InvalidField(
            "quantity precision loss",
        ));
    }

    Ok(quantity)
}
