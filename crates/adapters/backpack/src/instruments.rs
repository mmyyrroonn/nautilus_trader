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

//! Validated metadata and explicitly sourced economic inputs for Nautilus construction.

use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::CryptoPerpetual,
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::parsing::BackpackInstrumentError;

/// Provenance class of caller-supplied margin and fee inputs.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BackpackEconomicsSource {
    /// Explicit user or strategy configuration, not account verification.
    Configured,
    /// A separately captured venue/account observation, not verified by this parser.
    VenueObserved,
    /// A deliberately synthetic value for offline framework validation.
    Synthetic,
}

/// Economic fields required by `CryptoPerpetual`, with explicit provenance.
///
/// Supplying these values never establishes execution readiness. The venue's public
/// margin functions cannot safely be replaced with a universal constant.
#[derive(Clone, Debug)]
pub struct BackpackInstrumentEconomics {
    pub(crate) margin_init: Decimal,
    pub(crate) margin_maint: Decimal,
    pub(crate) maker_fee: Decimal,
    pub(crate) taker_fee: Decimal,
    pub(crate) source: BackpackEconomicsSource,
    pub(crate) source_reference: String,
}

impl BackpackInstrumentEconomics {
    /// Validates explicit margin fractions, fee fractions, and their source reference.
    ///
    /// # Errors
    ///
    /// Returns an error for margins outside `[0, 1]`, maintenance above initial margin,
    /// absolute fees above one, or an empty provenance reference. Maker rebates are allowed.
    pub fn new_checked(
        margin_init: Decimal,
        margin_maint: Decimal,
        maker_fee: Decimal,
        taker_fee: Decimal,
        source: BackpackEconomicsSource,
        source_reference: String,
    ) -> Result<Self, BackpackInstrumentError> {
        if margin_init < Decimal::ZERO
            || margin_init > Decimal::ONE
            || margin_maint < Decimal::ZERO
            || margin_maint > margin_init
            || maker_fee.abs() > Decimal::ONE
            || taker_fee.abs() > Decimal::ONE
            || source_reference.trim().is_empty()
        {
            return Err(BackpackInstrumentError::InvalidEconomics);
        }

        Ok(Self {
            margin_init,
            margin_maint,
            maker_fee,
            taker_fee,
            source,
            source_reference,
        })
    }
}

/// Validated USDC linear perpetual metadata, without inferred margin or fee readiness.
#[derive(Clone, Debug)]
pub struct BackpackInstrumentMetadata {
    /// Unique native-symbol identity at the BACKPACK venue.
    pub instrument_id: InstrumentId,
    /// Native symbol, preserved without renaming.
    pub raw_symbol: Symbol,
    /// Known Nautilus base currency; unknown currencies require explicit registration.
    pub base_currency: Currency,
    /// Venue quote currency, always USDC for this scope.
    pub quote_currency: Currency,
    /// Positive exact price increment.
    pub price_increment: Price,
    /// Positive exact size increment.
    pub size_increment: Quantity,
    /// Exact minimum order quantity on the increment grid.
    pub min_quantity: Quantity,
    /// Exact optional maximum order quantity.
    pub max_quantity: Option<Quantity>,
    /// Exact minimum quoted price on the increment grid.
    pub min_price: Price,
    /// Exact optional maximum quoted price.
    pub max_price: Option<Price>,
    /// Venue funding interval in milliseconds, or unknown.
    pub funding_interval_ms: Option<u64>,
    /// Venue lower bound in basis points, or unknown.
    pub funding_rate_lower_bound_bps: Option<Decimal>,
    /// Venue upper bound in basis points, or unknown.
    pub funding_rate_upper_bound_bps: Option<Decimal>,
    /// Caller-supplied receipt timestamp, not the market creation or engine event time.
    pub received_at: UnixNanos,
    /// Complete venue metadata, including dynamic bands and margin functions.
    pub raw_metadata: Value,
}

impl BackpackInstrumentMetadata {
    /// Constructs a Nautilus instrument only with explicitly supplied economic inputs.
    ///
    /// Unknown event time is represented by zero; initialization time is the supplied
    /// receipt timestamp. `info` retains provenance and an explicit false readiness flag.
    ///
    /// # Errors
    ///
    /// Returns an error if economics are absent or Nautilus construction fails.
    pub fn to_instrument(
        &self,
        economics: Option<&BackpackInstrumentEconomics>,
    ) -> Result<CryptoPerpetual, BackpackInstrumentError> {
        let economics = economics.ok_or(BackpackInstrumentError::MissingEconomics)?;
        let mut info = Params::new();
        info.insert("backpack_metadata".to_string(), self.raw_metadata.clone());
        info.insert("backpack_execution_ready".to_string(), Value::Bool(false));
        info.insert(
            "backpack_economics".to_string(),
            serde_json::json!({
                "source": economics.source,
                "reference": economics.source_reference,
                "event_timestamp": "unknown",
                "initialization_timestamp": "caller_receipt",
            }),
        );
        CryptoPerpetual::builder()
            .instrument_id(self.instrument_id)
            .raw_symbol(self.raw_symbol)
            .base_currency(self.base_currency)
            .quote_currency(self.quote_currency)
            .settlement_currency(self.quote_currency)
            .is_inverse(false)
            .price_precision(self.price_increment.precision)
            .size_precision(self.size_increment.precision)
            .price_increment(self.price_increment)
            .size_increment(self.size_increment)
            .multiplier(Quantity::from(1))
            .lot_size(self.size_increment)
            .min_quantity(self.min_quantity)
            .maybe_max_quantity(self.max_quantity)
            .min_price(self.min_price)
            .maybe_max_price(self.max_price)
            .margin_init(economics.margin_init)
            .margin_maint(economics.margin_maint)
            .maker_fee(economics.maker_fee)
            .taker_fee(economics.taker_fee)
            .info(info)
            .ts_event(UnixNanos::from(0))
            .ts_init(self.received_at)
            .build()
            .map_err(|e| BackpackInstrumentError::Domain(e.to_string()))
    }
}
