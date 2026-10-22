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

//! Exact Arcus market metadata conversion.

use std::{str::FromStr, sync::LazyLock};

use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    enums::CurrencyType,
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::models::ArcusMarket;

static SETTLEMENT_CURRENCY: LazyLock<Result<Currency, String>> = LazyLock::new(|| {
    (|| -> anyhow::Result<Currency> {
        let currency = Currency::new_checked(
            "USDG.ARCUS",
            9,
            0,
            "Arcus USDG ledger",
            CurrencyType::Crypto,
        )?;
        Currency::register(currency, false)?;
        let registered = Currency::try_from_str("USDG.ARCUS")
            .ok_or_else(|| anyhow::anyhow!("Arcus settlement currency registration failed"))?;
        anyhow::ensure!(
            registered.precision == 9 && registered.currency_type == CurrencyType::Crypto,
            "Existing USDG.ARCUS currency conflicts with Arcus nanodollar ledger"
        );
        Ok(registered)
    })()
    .map_err(|e| e.to_string())
});

/// Returns the registered nine-decimal Arcus USDG ledger denomination.
///
/// The alias represents USDG one to one; it preserves the venue's nanodollar accounting
/// through Money serialization without overriding the framework's global USDG definition.
///
/// # Errors
///
/// Returns an error if the existing alias has conflicting precision or registration fails.
pub fn settlement_currency() -> anyhow::Result<Currency> {
    match &*SETTLEMENT_CURRENCY {
        Ok(currency) => Ok(*currency),
        Err(message) => anyhow::bail!("{message}"),
    }
}

/// Converts a venue perpetual into its canonical native instrument.
///
/// # Errors
///
/// Returns an error for unsupported market identity, invalid increments, or domain conversion.
pub fn parse_instrument(
    market: &ArcusMarket,
    received_at: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    anyhow::ensure!(
        market.market_type == "PERPETUAL",
        "Unsupported Arcus market type"
    );
    anyhow::ensure!(
        market.quote_asset == "USD",
        "Unsupported Arcus quote currency"
    );
    anyhow::ensure!(
        market.tick_size > Decimal::ZERO && market.step_size > Decimal::ZERO,
        "Arcus increments must be positive"
    );
    anyhow::ensure!(
        !market.market_display_name.is_empty()
            && market
                .market_display_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "Invalid Arcus market symbol"
    );
    anyhow::ensure!(
        !market.base_asset.is_empty()
            && market
                .base_asset
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "Invalid Arcus base asset"
    );
    let price_increment = Price::from_decimal(market.tick_size.normalize())?;
    let size_increment = Quantity::from_decimal(market.step_size.normalize())?;
    let base_currency = Currency::get_or_create_crypto(&market.base_asset);
    let quote_currency = Currency::USD();
    let mut info = Params::new();
    info.insert("arcus_market".into(), serde_json::to_value(market)?);
    info.insert("arcus_market_id".into(), market.market_id.into());
    let instrument = CryptoPerpetual::builder()
        .instrument_id(InstrumentId::from_str(&format!(
            "{}-PERP.ARCUS",
            market.market_display_name
        ))?)
        .raw_symbol(Symbol::from(market.market_display_name.as_str()))
        .base_currency(base_currency)
        .quote_currency(quote_currency)
        .settlement_currency(settlement_currency()?)
        .is_inverse(false)
        .price_precision(price_increment.precision)
        .size_precision(size_increment.precision)
        .price_increment(price_increment)
        .size_increment(size_increment)
        .lot_size(size_increment)
        .min_quantity(Quantity::from_decimal(market.min_order_size.normalize())?)
        .max_quantity(Quantity::from_decimal(market.max_order_size.normalize())?)
        .min_notional(Money::from_decimal(
            market.min_order_notional,
            quote_currency,
        )?)
        .margin_init(market.initial_margin_fraction)
        .margin_maint(market.maintenance_margin_fraction)
        .maker_fee(Decimal::from(market.effective_maker_fee_ppm) / Decimal::from(1_000_000))
        .taker_fee(Decimal::from(market.effective_taker_fee_ppm) / Decimal::from(1_000_000))
        .info(info)
        .ts_event(UnixNanos::from(0))
        .ts_init(received_at)
        .build()?;
    Ok(InstrumentAny::CryptoPerpetual(instrument))
}
